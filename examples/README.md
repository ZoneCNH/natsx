# natsx examples

These examples exercise `github.com/ZoneCNH/natsx/pkg/natsx` against embedded or caller-provided NATS servers. Tests start an embedded broker, so release evidence does not depend on a shared external endpoint.

| Example | Behavior | Validation |
| --- | --- | --- |
| `basic` | Connect, subscribe, publish one envelope, and print the delivered subject. | `GOWORK=off go test ./examples/basic` |
| `config` | Print a sanitized config with credentials, token, NKey seed, and credential path redacted. | `GOWORK=off go test ./examples/config` |
| `health` | Connect and print a healthy status from `HealthCheck`. | `GOWORK=off go test ./examples/health` |
| `jetstream` | Create a stream and durable consumer, publish, fetch, ack, and print the stream name. | `GOWORK=off go test ./examples/jetstream` |

Runnable examples read `NATS_URL` from the environment. Example tests use `examples/internal/embeddednats` and do not require a live service.
