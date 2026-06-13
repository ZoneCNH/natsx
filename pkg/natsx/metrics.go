package natsx

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

type Metrics interface {
	IncCounter(name string, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
}

type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, map[string]string)                {}
func (NoopMetrics) ObserveHistogram(string, float64, map[string]string) {}
func (NoopMetrics) SetGauge(string, float64, map[string]string)         {}
