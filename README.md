# natsx

`natsx` is the Go NATS integration module for ZoneCNH services. Its 1.0 contract is a small, explicit wrapper around [NATS](https://nats.io/) that standardizes Core NATS publish/subscribe, request/reply, JetStream persistence, subject naming, message envelopes, connection lifecycle, health, metrics, and credential redaction.

This repository is being repaired from an old base-library template into the real NATS module. The public target package is `github.com/ZoneCNH/natsx/pkg/natsx`; legacy `pkg/templatex` code is not part of the natsx 1.0 API and must not be documented as the module identity.


## Current truth (2026-06-12 repair)

| Area | Current state | Release meaning |
| --- | --- | --- |
| Spec intent | `module/natsx/SPEC.md` and `goal.md` define the NATS 1.0 contract. | Source of target API and acceptance criteria. |
| Implemented state | Active implementation work is expected under `pkg/natsx`; legacy `pkg/templatex` remains in this checkout. | Do not count `pkg/templatex` as NATS 1.0 evidence. |
| Examples | Existing Go examples still exercise legacy template behavior; `examples/README.md` defines the replacement set. | Existing examples are compile smoke only until migrated to `pkg/natsx`. |
| Traceability | `module/natsx/TRACEABILITY.md` remains Draft / Pending Evidence until Core NATS and JetStream tests exist. | Do not mark 100/100 on documentation-only evidence. |

The inherited base-library release governance metadata remains on `v0.4.6` while this repository is repaired; that version marker is retained for existing release/version gates and is not a NATS 1.0 approval.

## 1.0 scope

`natsx` owns these stable responsibilities:

- **Core NATS**: publish, subscribe, queue subscription, unsubscribe, drain, and request/reply with `context.Context` cancellation and explicit timeouts.
- **JetStream**: stream and consumer admin, publish acknowledgements, durable consumption, ack/nack/term behavior, and redelivery handling.
- **Envelope and headers**: bidirectional mapping between `NatsMessageEnvelope` fields and NATS headers for `traceId`, `messageId`, `schemaVersion`, subject, and custom headers.
- **Subject builder**: canonical `domain.resource.action.v{version}` construction and parsing with validation.
- **Configuration/options**: explicit servers, client name, auth/TLS, reconnect policy, request timeout, serializer, JetStream enablement, and safe defaults.
- **Lifecycle**: connect, flush, drain, close, subscription cleanup, and idempotent shutdown.
- **Health and observability**: readiness/liveness, connection state, operation counters/durations, consume/redelivery metrics, structured diagnostic fields, and sensitive-value redaction.

## Public API baseline

The stable 1.0 API is documented in `/home/ZoneCNH/module/natsx/goal.md` and `/home/ZoneCNH/module/natsx/SPEC.md`. Implementations should expose these logical contracts from `pkg/natsx`:

```go
type NatsPubSubClient interface {
    Publish(ctx context.Context, subject string, msg NatsMessageEnvelope) (PublishResult, error)
    Subscribe(ctx context.Context, subject string, handler NatsMessageHandler, opts ...SubscribeOption) (Subscription, error)
}

type NatsRequestClient interface {
    Request(ctx context.Context, subject string, msg NatsMessageEnvelope, timeout time.Duration) (NatsMessageEnvelope, error)
    Reply(ctx context.Context, subject string, handler NatsMessageHandler) (Subscription, error)
}

type JetStreamClientX interface {
    Publish(ctx context.Context, stream string, subject string, msg NatsMessageEnvelope) (*PublishAck, error)
    Consume(ctx context.Context, stream string, consumer string, handler NatsMessageHandler) (ConsumerHandle, error)
    AddStream(ctx context.Context, cfg *StreamConfig) error
    AddConsumer(ctx context.Context, stream string, cfg *ConsumerConfig) error
}
```

## Installation

```bash
go get github.com/ZoneCNH/natsx
```

The implementation depends on the official Go NATS client and must be testable with an embedded or locally launched `nats-server`; application tests must not require a shared external NATS service.

## Minimal usage

```go
ctx := context.Background()
client, err := natsx.Connect(ctx,
    natsx.WithServers([]string{"nats://127.0.0.1:4222"}),
    natsx.WithClientName("orders-api"),
    natsx.WithRequestTimeout(time.Second),
)
if err != nil {
    return err
}
defer client.Close(ctx)

subject, err := natsx.Subject().Build("orders", "created", "publish", 1)
if err != nil {
    return err
}

_, err = client.Publish(ctx, subject, natsx.NatsMessageEnvelope{
    EventID:       "evt-123",
    MessageID:     "msg-123",
    SchemaVersion: "orders.created.v1",
    TraceID:       "trace-abc",
    Payload:       []byte(`{"orderId":"o-1"}`),
})
return err
```

See [`examples/README.md`](examples/README.md) for the intended example set and current repair status.

## Subject convention

Subjects use the stable pattern:

```text
{domain}.{resource}.{action}.v{version}
```

Examples:

- `orders.created.publish.v1`
- `device.telemetry.ingest.v1`
- `billing.invoice.request.v1`

Do not place secrets, personal data, credentials, tokens, or unbounded cardinality identifiers in subject tokens.

## Configuration keys

The cross-module configuration namespace is stable:

| Key | Meaning | Default |
| --- | --- | --- |
| `foundationx.nats.enabled` | enables this module | `false` |
| `foundationx.nats.servers` | NATS server URLs | required when enabled |
| `foundationx.nats.client-name` | client identity | application name |
| `foundationx.nats.request.timeout` | request/reply timeout | `1s` |
| `foundationx.nats.reconnect.max-attempts` | reconnect attempts | `-1` continuous |
| `foundationx.nats.jetstream.enabled` | enables JetStream APIs | `false` |
| `foundationx.nats.serializer` | payload codec | `json` |

Credentials, tokens, passwords, and connection URLs with embedded secrets must be redacted in errors, logs, health messages, and evidence artifacts.

## Verification

Required local checks for this repository:

```bash
GOWORK=off go test ./...
git diff --check
```

Required module-evidence check from `/home/ZoneCNH` after updating `module/natsx` docs:

```bash
git diff --check -- module/natsx
```

A 100/100 release also requires embedded NATS integration evidence for Core NATS and JetStream paths, plus synchronized `module/natsx/SPEC.md` and `module/natsx/TRACEABILITY.md` status.

## Current repair status

- Target branch: `natsx`.
- Primary package target: `pkg/natsx`.
- Known legacy residue: `pkg/templatex`, goal-runtime generator assets, and old examples remain until the implementation slice replaces them.
- `/home/natsx/docs/l2/` is intentionally excluded from this repair unless the leader explicitly expands scope.
