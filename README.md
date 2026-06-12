# natsx

`natsx` is the Go NATS integration module for ZoneCNH services. Its 1.0 contract is a small, explicit wrapper around [NATS](https://nats.io/) that standardizes Core NATS publish/subscribe, request/reply, JetStream persistence, subject naming, message envelopes, connection lifecycle, health, metrics, and credential redaction.

This repository is being repaired from an old base-library template into the real NATS module. The public target package is `github.com/ZoneCNH/natsx/pkg/natsx`; legacy `pkg/templatex` code is not part of the natsx 1.0 API and must not be documented as the module identity.


## Current truth (2026-06-12 repair)

| Area | Current state | Release meaning |
| --- | --- | --- |
| Spec intent | `module/natsx/SPEC.md` and `goal.md` define the NATS 1.0 contract. | Source of target API and acceptance criteria. |
| Implemented state | `pkg/natsx` now exposes a working repair baseline for config, lifecycle, Core NATS publish/subscribe/request/queue, JetStream admin/publish/pull, envelopes, subjects, health, errors, and metrics. Legacy `pkg/templatex` remains in this checkout. | Count only `pkg/natsx` executable evidence toward NATS 1.0; do not count `pkg/templatex`. |
| Examples | Existing Go examples still exercise legacy template behavior; `examples/README.md` defines the replacement set. | Existing examples are compile smoke only until migrated to `pkg/natsx`. |
| Traceability | Embedded NATS tests now cover the Core NATS and JetStream repair baseline; `module/natsx/TRACEABILITY.md` remains partial until every 1.0 acceptance group is proven. | Do not mark 100/100 while redelivery/DLQ, reconnect policy, examples, and benchmark evidence remain open. |

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

The stable 1.0 API is documented in `/home/ZoneCNH/module/natsx/goal.md` and `/home/ZoneCNH/module/natsx/SPEC.md`. The current repair baseline exposes these concrete contracts from `pkg/natsx`:

```go
type Handler func(context.Context, Envelope) (Envelope, error)

type Client struct {
    // created by New(ctx, Config)
}

func New(ctx context.Context, cfg Config) (*Client, error)
func (c *Client) Publish(ctx context.Context, env Envelope) error
func (c *Client) Request(ctx context.Context, env Envelope) (Envelope, error)
func (c *Client) Subscribe(subject string, handler Handler) (*nats.Subscription, error)
func (c *Client) QueueSubscribe(subject, queue string, handler Handler) (*nats.Subscription, error)
func (c *Client) JetStream() (*JetStreamClient, error)

type JetStreamClient struct {
    // wraps nats.JetStreamContext
}
```

Target 1.0 gaps still include higher-level consumer handles, explicit redelivery/DLQ policy helpers, reconnect/backoff evidence, migrated examples, and benchmark evidence.

## Installation

```bash
go get github.com/ZoneCNH/natsx
```

The implementation depends on the official Go NATS client and must be testable with an embedded or locally launched `nats-server`; application tests must not require a shared external NATS service.

## Minimal usage

```go
ctx := context.Background()
client, err := natsx.New(ctx, natsx.Config{
    Name:    "orders-api",
    URL:     "nats://127.0.0.1:4222",
    Timeout: time.Second,
})
if err != nil {
    return err
}
defer client.Close(ctx)

subject, err := natsx.Subject().Build("orders", "created", "publish", 1)
if err != nil {
    return err
}

err = client.Publish(ctx, natsx.Envelope{
    Subject:       subject,
    EventID:       "evt-123",
    MessageID:     "msg-123",
    SchemaVersion: "orders.created.v1",
    TraceID:       "trace-abc",
    Data:          []byte(`{"orderId":"o-1"}`),
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
GOWORK=off go test ./pkg/natsx -count=1
GOWORK=off go test -race ./pkg/natsx -count=1
GOWORK=off go test ./... -count=1
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
- Embedded NATS evidence now covers Core NATS publish/subscribe/request/queue and JetStream stream admin, publish, pull, envelope metadata, and ack paths.
- Known legacy residue: `pkg/templatex`, goal-runtime generator assets, and old examples remain until the implementation slice replaces them.
- Remaining release gaps: migrated examples, explicit redelivery/DLQ policy evidence, reconnect/backoff evidence, benchmark/SLO evidence, and full consumer lifecycle API polish.
- `/home/natsx/docs/l2/` is intentionally excluded from this repair unless the leader explicitly expands scope.
