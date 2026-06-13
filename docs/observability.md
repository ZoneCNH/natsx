# natsx Observability

## Metrics

`pkg/natsx` records the metrics defined in `contracts/metrics.md`. The canonical metric namespace is `foundationx_nats_`:

- `foundationx_nats_client_created_total`
- `foundationx_nats_client_closed_total`
- `foundationx_nats_client_errors_total`
- `foundationx_nats_client_health_status`
- `foundationx_nats_client_health_latency_ms`
- `foundationx_nats_core_messages_total`
- `foundationx_nats_core_request_duration_seconds`
- `foundationx_nats_jetstream_messages_total`
- `foundationx_nats_connection_reconnects_total`
- `foundationx_nats_connection_disconnects_total`

Lifecycle metrics are emitted by `New`, `Close`, connection callbacks, and `HealthCheck`. Core NATS and JetStream metrics are emitted by the corresponding publish/request/subscribe/admin paths. Metric labels must stay bounded and must not include payloads, credentials, tokens, connection strings with embedded secrets, or arbitrary headers.

## Health checks

Clients expose `HealthCheck(context.Context)`. Returned fields are:

- `name`
- `status`
- `message`
- `checked_at`
- `latency_ms`
- `metadata`

`status` is one of `healthy`, `degraded`, or `unhealthy`. Nil context, canceled context, uninitialized clients, and closed clients return `unhealthy`. A connected client with an insufficient health-check deadline returns `degraded`. Health checks record `foundationx_nats_client_health_status` and `foundationx_nats_client_health_latency_ms` with bounded labels.

## Logs and evidence

Errors, logs, health messages, docs evidence, and test output must not print raw credentials or production connection material. Use redacted placeholders such as `<redacted>` for URLs, tokens, passwords, nkey seeds, usernames, and credentials paths when those values come from secret stores or live dev configuration.
