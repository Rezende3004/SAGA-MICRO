# Decisões Arquiteturais — Saga CreateOrder

Documento técnico complementar ao [README](../README.md). Registra as decisões no formato ADR (*Architecture Decision Record*), os riscos mitigados, o tratamento de dead-letter queues e as estratégias de resiliência.

---

## Sumário

1. [Registro de decisões (ADRs)](#1-registro-de-decisões-adrs)
2. [Riscos mitigados](#2-riscos-mitigados)
3. [Contratos de mensagem](#3-contratos-de-mensagem)
4. [Dead-letter queues](#4-dead-letter-queues)
5. [Retry](#5-retry)
6. [Circuit Breaker](#6-circuit-breaker)
7. [Orçamento de timeouts](#7-orçamento-de-timeouts)
8. [Falta de isolamento e contramedidas](#8-falta-de-isolamento-e-contramedidas)
9. [Observabilidade](#9-observabilidade)

---

## 1. Registro de decisões (ADRs)

### ADR-001 — Saga orquestrada

| | |
|---|---|
| **Status** | Aceita |
| **Contexto** | Criar pedido envolve Order, Inventory e Payment, cada um com banco próprio. Não há transação ACID entre eles. O fluxo de falha tem ramificações (recusa, timeout ambíguo, resposta tardia) e exige auditoria. |
| **Decisão** | Implementar a saga `CreateOrderSaga` como **orquestração**: um orquestrador no Order Service mantém a máquina de estados em `saga_instance` e envia comandos assíncronos aos participantes. |
| **Alternativas** | **Coreografia** — descartada: lógica espalhada, acoplamento implícito por eventos, difícil saber "onde está" uma saga, compensação de timeout ambíguo fica sem dono. **2PC/XA** — descartada: gateway externo não participa, bloqueio distribuído reduz disponibilidade. **Orquestrador como serviço separado** (ou engine tipo Temporal/Camunda) — adiada: aumenta a infra para um único fluxo; reavaliar quando houver ≥ 3 sagas. |
| **Consequências** | (+) Fluxo explícito, testável e auditável; participantes simples. (−) Orquestrador vira componente crítico — mitigado com réplicas *stateless* e estado em banco. (−) Risco de o orquestrador acumular regra de negócio — regra: ele só **coordena**, a regra fica nos participantes. |

### ADR-002 — RabbitMQ com quorum queues como barramento

| | |
|---|---|
| **Status** | Aceita |
| **Contexto** | A comunicação é comando/resposta ponto a ponto, não *stream* de eventos para muitos leitores. Precisamos de DLQ, atraso de reentrega e ACK por mensagem. |
| **Decisão** | RabbitMQ 3.13+ com **quorum queues** (replicadas via Raft), *dead-letter exchanges* nativos e confirmação de publicação (*publisher confirms*). |
| **Alternativas** | **Kafka** — ótimo para eventos de domínio e reprocessamento, mas DLQ e retry por mensagem precisam ser construídos à mão (tópicos de retry), e o modelo de partição dificulta *redelivery* individual. Fica como opção para os eventos `OrderApproved`/`OrderRejected` consumidos por analytics. |
| **Consequências** | Entrega **at-least-once** ⇒ idempotência obrigatória em todos os consumidores (ADR-004). |

### ADR-003 — Transactional Outbox

| | |
|---|---|
| **Status** | Aceita |
| **Contexto** | "Gravar no banco" e "publicar no broker" são duas operações em sistemas diferentes. Se o processo cair entre elas, o estado diverge (pedido criado sem comando enviado, ou comando enviado sem pedido). |
| **Decisão** | Cada serviço grava a mensagem de saída numa tabela `outbox` **na mesma transação local** da mudança de estado. Um *Outbox Relay* lê a tabela e publica (polling a cada 200 ms, `FOR UPDATE SKIP LOCKED`); evoluível para CDC com Debezium. |
| **Consequências** | (+) Atomicidade entre estado e mensagem. (−) Publicação pode duplicar (relay cai depois de publicar e antes de marcar como enviada) — absorvido pela idempotência. (−) Latência extra de até ~200 ms por passo. |

```sql
CREATE TABLE outbox (
    id             UUID PRIMARY KEY,          -- = messageId do envelope
    aggregate_id   UUID NOT NULL,             -- sagaId / orderId
    destination    VARCHAR(100) NOT NULL,     -- 'inventory.commands'
    message_type   VARCHAR(60)  NOT NULL,     -- 'ReserveStock'
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ                -- NULL = pendente
);
CREATE INDEX ix_outbox_pending ON outbox (created_at) WHERE published_at IS NULL;
```

### ADR-004 — Idempotent Consumer em todos os participantes

| | |
|---|---|
| **Status** | Aceita |
| **Contexto** | Entrega at-least-once, outbox que pode duplicar e reenvio por timeout fazem com que **toda** mensagem possa chegar mais de uma vez. |
| **Decisão** | Todo handler grava `(consumer, message_id)` em `processed_messages` na mesma transação do efeito de negócio, junto com a resposta produzida. Em duplicata, não reaplica o efeito e **republica a resposta armazenada**. Complementado por chaves naturais únicas (`reservations.saga_id`, `payments.saga_id`). |
| **Consequências** | Efeito de negócio exatamente-uma-vez sobre entrega pelo-menos-uma-vez. Tabela cresce — expurgo de registros com mais de 7 dias (maior que o maior TTL de retry). |

```sql
CREATE TABLE processed_messages (
    consumer      VARCHAR(60) NOT NULL,
    message_id    UUID        NOT NULL,
    saga_id       UUID        NOT NULL,
    response      JSONB,                       -- resposta enviada, para replay
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
```

Pseudocódigo do handler (vale para todos os participantes):

```text
on message(msg):
  BEGIN
    inserted = INSERT INTO processed_messages(consumer, msg.messageId, msg.sagaId)
               ON CONFLICT DO NOTHING
    if not inserted:
        stored = SELECT response FROM processed_messages WHERE ...
        INSERT INTO outbox(stored)            -- republica a mesma resposta
        COMMIT; ACK; return
    reply = executarRegraDeNegocio(msg)       -- ex.: reservar estoque
    UPDATE processed_messages SET response = reply
    INSERT INTO outbox(reply)
  COMMIT
  ACK
```

### ADR-005 — Pagamento como transação pivô e reconciliação antes de compensar

| | |
|---|---|
| **Status** | Aceita |
| **Contexto** | Timeout na chamada ao gateway não significa "não cobrado". Compensar direto pode cancelar um pedido já pago. |
| **Decisão** | `ProcessPayment` é o pivô. Em timeout, o orquestrador entra em `VERIFYING_PAYMENT` e consulta o status pela chave de idempotência; só compensa com resposta conclusiva ou após `VoidPayment`. Passos pós-pivô são apenas retentáveis. |
| **Consequências** | Garante o requisito "nunca cobrar sem pedido aprovado ou reembolso". Requer que o gateway suporte idempotência e consulta por chave (requisito de seleção de fornecedor). |

---

## 2. Riscos mitigados

| # | Risco | Prob. | Impacto | Mitigação | Risco residual |
|---|---|:---:|:---:|---|---|
| R1 | Cobrança duplicada do cliente | Média | **Alto** | `Idempotency-Key = sagaId` no gateway; `UNIQUE payments(saga_id)`; Idempotent Consumer | Gateway sem suporte a idempotência (critério de contratação) |
| R2 | Cliente cobrado e pedido cancelado | Baixa | **Alto** | Reconciliação em `VERIFYING_PAYMENT`; `VoidPayment`; `RefundPayment` para respostas tardias | Janela de minutos até o reembolso |
| R3 | Estoque "preso" em reservas de pedidos mortos | Média | Médio | Compensação `ReleaseStock`; **TTL de 15 min** na reserva; job de expiração | Item indisponível por até 15 min |
| R4 | Overselling (vender mais do que existe) | Média | Alto | Reserva decrementa o disponível de forma atômica (`UPDATE ... WHERE disponivel >= qtd`) | — |
| R5 | Mensagem perdida entre banco e broker | Baixa | Alto | Transactional Outbox; publisher confirms; quorum queues | — |
| R6 | Saga parada para sempre | Média | Médio | `step_deadline` + Timeout Sweeper; métrica de sagas paradas | Saga em `COMPENSATION_FAILED` aguardando humano |
| R7 | Lentidão do gateway derrubando o sistema | Alta (picos) | Alto | Circuit breaker; bulkhead (pool dedicado); API responde `202` sem esperar o pagamento | Pedidos acumulam em `PENDING` durante a indisponibilidade |
| R8 | Mensagem venenosa travando a fila | Baixa | Médio | Validação de schema; erros não-retentáveis vão direto para DLQ | — |
| R9 | Leituras inconsistentes durante a saga (falta de isolamento) | Alta | Baixo | *Semantic lock* (`PENDING`), UI mostra "processando" | Esperado pela consistência eventual |
| R10 | Orquestrador fora do ar | Baixa | Alto | Réplicas stateless; estado em banco; retomada automática | Atraso, não perda |

---

## 3. Contratos de mensagem

Envelope comum (JSON, versionado):

```json
{
  "messageId": "6f1c2a7e-...",        // único por mensagem; reutilizado em reenvios
  "messageType": "ReserveStock",
  "schemaVersion": 1,
  "sagaId": "a3b9...",
  "correlationId": "a3b9...",          // = sagaId; propagado para tracing
  "replyTo": "saga.replies",
  "occurredAt": "2026-10-22T20:15:03Z",
  "payload": { }
}
```

| Comando (→ participante) | Respostas possíveis (→ `saga.replies`) | Retentável? | Idempotência |
|---|---|:---:|---|
| `ReserveStock {orderId, itens[]}` | `StockReserved {reservationId}` · `StockRejected {motivo}` | Sim | `UNIQUE reservations(saga_id)` + tombstone |
| `ReleaseStock {reservationId}` | `StockReleased` | Sim (precisa terminar) | No-op se já liberada/inexistente |
| `ConfirmReservation {reservationId}` | `ReservationConfirmed` | Sim (ilimitado) | No-op se já confirmada |
| `ProcessPayment {orderId, valor, meio}` | `PaymentApproved {transactionId}` · `PaymentRejected {motivo}` | Só falhas técnicas | `UNIQUE payments(saga_id)` + chave no gateway |
| `GetPaymentStatus` | `PaymentApproved` · `PaymentRejected` · `PaymentStatusUnknown` | Sim | Leitura pura |
| `VoidPayment` | `PaymentVoided` | Sim | Chave de idempotência no gateway |
| `RefundPayment {transactionId}` | `PaymentRefunded` | Sim | `UNIQUE refunds(saga_id)` |

**Evolução de contrato**: campos novos são opcionais; remoção/renomeação exige `schemaVersion` novo e convivência de duas versões por pelo menos um ciclo de deploy.

---

## 4. Dead-letter queues

### Topologia

```
inventory.commands ──(x-dead-letter-exchange)──► dlx ──► inventory.commands.dlq
payment.commands   ──(x-dead-letter-exchange)──► dlx ──► payment.commands.dlq
saga.replies       ──(x-dead-letter-exchange)──► dlx ──► saga.replies.dlq
```

Cada fila principal é uma quorum queue com `x-delivery-limit` (limite de reentregas) e `x-dead-letter-exchange` configurados. Mensagens chegam na DLQ com cabeçalhos `x-death` (motivo, contagem, fila de origem) e o `last_error` gravado pelo consumidor.

### O que vai para a DLQ

| Tipo de erro | Exemplo | Caminho |
|---|---|---|
| **Não-retentável** | JSON inválido, schema desconhecido, `sagaId` ausente | Direto para DLQ (`reject` sem requeue) |
| **Retentável esgotado** | Banco do Inventory fora do ar por > 10 tentativas | Retry com backoff → DLQ |
| **Recusa de negócio** | Sem estoque, cartão recusado | **Nunca** vai para DLQ — vira resposta normal (`StockRejected`, `PaymentRejected`) |

### Tratamento operacional

1. **Alerta** imediato: `dlq_depth > 0` em qualquer DLQ dispara alerta (PagerDuty/Slack). Mensagem de compensação na DLQ é severidade **alta** — significa dado inconsistente.
2. **Saga marcada**: quando um comando de compensação vai para a DLQ, o orquestrador (via timeout) leva a saga para `COMPENSATION_FAILED` — nada fica inconsistente em silêncio.
3. **Diagnóstico**: o runbook pede `sagaId` → consulta `saga_instance`, `processed_messages` e o trace distribuído.
4. **Reprocessamento**: após corrigir a causa (deploy, banco restaurado), um operador move as mensagens da DLQ de volta para a fila original com uma ferramenta de *replay*. Como todos os consumidores são idempotentes, reprocessar é seguro.
5. **Mensagens irrecuperáveis** (schema inválido) são arquivadas com motivo e geram correção manual do pedido, registrada em auditoria.
6. **Retenção**: DLQs mantêm mensagens por 14 dias.

---

## 5. Retry

### Onde há retry

| Ponto | Mecanismo | Política |
|---|---|---|
| Consumidor → próprio banco | Requeue com atraso (fila de retry com TTL) | Backoff exponencial + jitter: 1 s, 2 s, 4 s, 8 s… teto 60 s; máx. 10 reentregas → DLQ |
| Payment → gateway externo (HTTP) | Retry dentro do adapter | Máx. **2** tentativas extras, 200 ms / 800 ms com jitter, **só** para erros técnicos (5xx, timeout de conexão, 429 respeitando `Retry-After`) |
| Orquestrador → participante (sem resposta) | Timeout Sweeper reenvia comando | Ver tabela de timeouts no README (seção 7) |
| Outbox Relay → broker | Loop do relay | Retry contínuo; a mensagem continua no outbox até ser confirmada |

### Regras

- **Só se retenta o que é idempotente.** Todos os comandos da saga são, por desenho (ADR-004).
- **Nunca retentar recusa de negócio** (`402`, sem estoque). Retentar só gera carga e o mesmo "não".
- **Jitter obrigatório**: sem ele, milhares de pedidos que falharam juntos voltam juntos e derrubam o serviço que estava se recuperando (*thundering herd*).
- **Retry curto dentro, retry longo fora**: o retry HTTP dentro do adapter é rápido e limitado (para não segurar a mensagem); o retry longo é feito pelo broker/sweeper, liberando a thread.
- Reenvios usam **o mesmo `messageId`**, para a deduplicação funcionar.

---

## 6. Circuit Breaker

Aplicado no **Gateway Adapter** do Payment Service (dependência externa, fora do nosso controle e a mais lenta do fluxo). Implementação de referência: Resilience4j (Java) ou Polly (.NET).

### Configuração

| Parâmetro | Valor | Motivo |
|---|---|---|
| Janela | Deslizante, últimas 50 chamadas | Reage rápido sem oscilar com poucas amostras |
| Mínimo de chamadas | 20 | Evita abrir com 2 erros em horário vazio |
| Limite de falha | 50% | Erros técnicos (5xx, timeout) — recusas de negócio **não** contam |
| Limite de lentidão | 50% das chamadas > 3 s | Gateway lento é tão danoso quanto fora do ar |
| Tempo em `OPEN` | 30 s | Dá tempo ao fornecedor se recuperar |
| Chamadas em `HALF_OPEN` | 5 | Sonda antes de liberar todo o tráfego |
| Timeout por chamada | 5 s | Dentro do orçamento da seção 7 |

### Comportamento com o circuito aberto

```
CLOSED ──(≥50% falhas/lentas)──► OPEN ──(30 s)──► HALF_OPEN ──(5 OK)──► CLOSED
                                    ▲                    │
                                    └──── (falhou) ──────┘
```

- Com o circuito **aberto**, o adapter falha imediatamente com `CircuitOpenException` — sem chamar o gateway.
- O handler trata isso como **erro técnico retentável**: a mensagem `ProcessPayment` volta para a fila com atraso (≥ 30 s). **Não** responde `PaymentRejected` — o cartão do cliente não foi recusado, o fornecedor é que está fora.
- O pedido permanece `PENDING`. Se o circuito ficar aberto por mais tempo que o `step_deadline` do passo, o sweeper leva a saga para `VERIFYING_PAYMENT` normalmente.
- **Bulkhead**: as chamadas ao gateway usam um pool de conexões/threads dedicado (máx. 50 concorrentes), para que lentidão no pagamento não esgote recursos de outros fluxos do Payment Service.
- Mudança de estado do circuito gera métrica e alerta.

**Por que não colocar circuit breaker entre orquestrador e participantes?** A comunicação interna é assíncrona por fila: se o Inventory cair, as mensagens simplesmente esperam na fila. A fila já faz o papel de amortecedor; o mecanismo de proteção ali é o timeout da saga.

---

## 7. Orçamento de timeouts

Os timeouts são aninhados: o de dentro sempre menor que o de fora, senão a camada externa desiste enquanto a interna ainda trabalha.

```
Chamada HTTP ao gateway ............ 5 s
  × (1 + 2 retries) + backoff ...... ≈ 16 s   (pior caso no adapter)
step_deadline PROCESSING_PAYMENT ... 30 s     (> 16 s + latência de fila)
TTL da reserva de estoque .......... 15 min   (> duração máxima esperada da saga)
SLA para o cliente ver resultado ... 2 min    (p99 esperado < 10 s)
```

---

## 8. Falta de isolamento e contramedidas

Sagas são ACD — não têm o "I" de ACID: outras transações enxergam estados intermediários. Contramedidas adotadas (Richardson, *Microservices Patterns*, cap. 4):

| Anomalia | Exemplo | Contramedida |
|---|---|---|
| **Lost update** | Cliente cancela o pedido enquanto a saga ainda roda | *Semantic lock*: pedido `PENDING` não aceita cancelamento pelo cliente — a requisição é recusada com `409 Conflict` ou enfileirada para depois do término |
| **Dirty read** | Relatório conta como venda um pedido que será compensado | Consultas de negócio filtram `status = APPROVED`; `PENDING` é exibido como "processando" |
| **Leitura do estoque intermediário** | Outro cliente vê o item indisponível por uma reserva que vai ser liberada | Aceito pelo negócio (é o comportamento desejado de uma reserva); limitado pelo TTL |
| **Reordenação de compensação** | `ReleaseStock` chega antes de `ReserveStock` | *Tombstone*: `ReleaseStock` sem reserva grava `CANCELLED` para o `sagaId`; `ReserveStock` posterior é recusado |

---

## 9. Observabilidade

- **Correlação**: `correlationId = sagaId` em todo log, mensagem e span. OpenTelemetry propaga o contexto nos cabeçalhos AMQP, permitindo um trace único do `POST /orders` até o `COMPLETED`.
- **Métricas**:
  - `saga_duration_seconds` (histograma por estado final)
  - `saga_state_total{state}` — quantidade de sagas por estado
  - `saga_stuck_total` — sagas com `step_deadline` vencido há mais de 5 min
  - `saga_compensation_total{reason}` — taxa de compensação por motivo
  - `dlq_depth{queue}`, `circuit_breaker_state{target}`, `outbox_lag_seconds`
- **Alertas**: qualquer mensagem em DLQ; qualquer saga em `COMPENSATION_FAILED`; `outbox_lag_seconds > 10`; circuito aberto por > 2 min; taxa de compensação acima do normal (indicador de problema no gateway ou de fraude).
- **Auditoria**: `order_history` registra cada transição de status do pedido com timestamp, `sagaId` e motivo.
