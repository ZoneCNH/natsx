package natsx

// Metric name constants use the "foundationx_nats_" prefix to match existing
// dashboards. They intentionally differ from the observex.MetricClient*
// generic names (which use no prefix), so cannot be aliased to observex.
// See: github.com/ZoneCNH/observex/pkg/observex/metrics.go
const (
MetricClientCreatedTotal         = "foundationx_nats_client_created_total"
MetricClientClosedTotal          = "foundationx_nats_client_closed_total"
MetricClientErrorsTotal          = "foundationx_nats_client_errors_total"
MetricClientHealthStatus         = "foundationx_nats_client_health_status"
MetricClientHealthLatencyMS      = "foundationx_nats_client_health_latency_ms"
MetricCoreMessagesTotal          = "foundationx_nats_core_messages_total"
MetricCoreRequestDurationSeconds = "foundationx_nats_core_request_duration_seconds"
MetricJetStreamMessagesTotal     = "foundationx_nats_jetstream_messages_total"
MetricConnectionReconnectsTotal  = "foundationx_nats_connection_reconnects_total"
MetricConnectionDisconnectsTotal = "foundationx_nats_connection_disconnects_total"
)

// Metrics is the observability hook interface for natsx clients.
// It is a 3-method subset of observex.Metrics; any observex.Metrics
// implementation satisfies this interface.
type Metrics interface {
IncCounter(name string, labels map[string]string)
ObserveHistogram(name string, value float64, labels map[string]string)
SetGauge(name string, value float64, labels map[string]string)
}

// NoopMetrics discards all observations. observex.NoopMetrics also satisfies
// this interface and may be used directly if observex is already a dependency.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, map[string]string)                {}
func (NoopMetrics) ObserveHistogram(string, float64, map[string]string) {}
func (NoopMetrics) SetGauge(string, float64, map[string]string)         {}
