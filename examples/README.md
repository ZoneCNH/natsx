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

The Go files currently present under `examples/` still compile against legacy `pkg/templatex` and are intentionally treated as repair debt, not 1.0 natsx API evidence. `pkg/natsx` now has embedded NATS tests for the Core NATS and JetStream baseline, but examples must still be migrated before they can be promoted to release evidence.

| Example | Current file state | Target owner/slice | Evidence status |
| --- | --- | --- | --- |
| `basic` | `examples/basic` imports `pkg/templatex` | Core NATS example migration | `legacy-templatex`; embedded baseline covered by `pkg/natsx` tests only |
| `config` | `examples/config` imports `pkg/templatex` | Config/options + redaction example migration | `legacy-templatex`; package tests cover config/redaction only |
| `health` | `examples/health` imports `pkg/templatex` | Lifecycle/health example migration | `legacy-templatex`; package tests cover lifecycle/health only |
| `jetstream` | directory not present yet | JetStream example migration | embedded baseline covered by `pkg/natsx` tests only |

Replacement examples must only be promoted to release evidence after their tests import `pkg/natsx`, run against embedded/local NATS, and are linked from `/home/ZoneCNH/module/natsx/TRACEABILITY.md`.
