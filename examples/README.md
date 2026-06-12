# natsx examples

This directory is reserved for executable examples of the `pkg/natsx` public API. During the 2026-06-12 repair, examples must demonstrate the real NATS module rather than the legacy `pkg/templatex` template package.

## Required example set

| Example | Purpose | Expected evidence |
| --- | --- | --- |
| `basic` | Connect to an embedded/local NATS server and publish/subscribe one envelope | `GOWORK=off go test ./examples/basic` |
| `config` | Build config/options, show redacted credential output, and validate defaults | `GOWORK=off go test ./examples/config` |
| `health` | Connect, read liveness/readiness, drain/close, and verify health state changes | `GOWORK=off go test ./examples/health` |
| `jetstream` | Create stream/consumer, publish, consume, ack, and close | `GOWORK=off go test ./examples/jetstream` |

## Example rules

- Examples must import `github.com/ZoneCNH/natsx/pkg/natsx` once the implementation slice lands.
- Tests must use an embedded or locally launched `nats-server`; they must not require a shared external NATS endpoint.
- Output must not include credentials, tokens, full authenticated URLs, message payload secrets, or private endpoints.
- Examples must stay short and copy-pasteable; advanced policy belongs in `module/natsx/SPEC.md` and traceability evidence.

## Current status

The Go files currently present under `examples/` still compile against legacy `pkg/templatex` and are intentionally treated as repair debt, not 1.0 natsx API evidence. They should be replaced when `pkg/natsx` implementation and embedded NATS tests are available.

| Example | Current file state | Target owner/slice | Evidence status |
| --- | --- | --- | --- |
| `basic` | `examples/basic` imports `pkg/templatex` | Core NATS implementation + example migration | `legacy-templatex`; not 1.0 evidence |
| `config` | `examples/config` imports `pkg/templatex` | Config/options + redaction migration | `legacy-templatex`; not 1.0 evidence |
| `health` | `examples/health` imports `pkg/templatex` | Lifecycle/health migration | `legacy-templatex`; not 1.0 evidence |
| `jetstream` | directory not present yet | JetStream implementation + embedded NATS test slice | `blocked`; create when API exists |

Replacement examples must only be promoted to release evidence after their tests import `pkg/natsx`, run against embedded/local NATS, and are linked from `/home/ZoneCNH/module/natsx/TRACEABILITY.md`.
