# Metrics Contract

`pkg/natsx` exposes a small FoundationX NATS observability surface. Metrics backends are pluggable, but metric names, types, and label semantics are part of the 1.0 contract.

All NATS module metrics use the canonical `foundationx_nats_` prefix. Legacy `natsx_*` names are not part of the 1.0 contract.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `foundationx_nats_client_created_total` | counter | `name` | Successfully created NATS clients. |
| `foundationx_nats_client_closed_total` | counter | `name` | Successfully closed NATS clients; repeated close attempts are not double counted. |
| `foundationx_nats_client_errors_total` | counter | `op`, `kind` | Client lifecycle and operation errors; `kind` must come from the natsx error contract. |
| `foundationx_nats_client_health_status` | gauge | `name`, `status` | Health status gauge; the current status is `1`, other statuses are `0`. |
| `foundationx_nats_client_health_latency_ms` | histogram | `name`, `status` | Health check latency in milliseconds. |
| `foundationx_nats_core_messages_total` | counter | `operation`, `status` | Core NATS publish, subscribe, and request/reply message outcomes. |
| `foundationx_nats_core_request_duration_seconds` | histogram | `operation`, `status` | Core request/reply latency in seconds. |
| `foundationx_nats_jetstream_messages_total` | counter | `operation`, `status` | JetStream publish, pull, ack/nack, and stream/consumer operation outcomes. |
| `foundationx_nats_connection_reconnects_total` | counter | `name` | NATS reconnect notifications observed by the client. |
| `foundationx_nats_connection_disconnects_total` | counter | `name` | NATS disconnect notifications observed by the client. |

## Templatex compatibility metrics

The repository still carries `pkg/templatex` compatibility contracts used by inherited contract tests. These names are outside the `pkg/natsx` 1.0 metrics namespace, but remain documented until the templatex compatibility surface is retired.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `client_created_total` | counter | `name` | Templatex clients created. |
| `client_closed_total` | counter | `name` | Templatex clients closed. |
| `client_errors_total` | counter | `op`, `kind` | Templatex lifecycle and operation errors. |
| `client_health_status` | gauge | `name`, `status` | Templatex health status gauge. |
| `client_health_latency_ms` | histogram | `name`, `status` | Templatex health latency in milliseconds. |
| `client_requests_total` | counter | `operation`, `status` | Templatex request outcomes. |
| `client_request_duration_seconds` | histogram | `operation`, `status` | Templatex request latency in seconds. |
| `client_retries_total` | counter | `operation` | Templatex retry attempts. |
| `client_inflight` | gauge | `operation` | Templatex in-flight operations. |
