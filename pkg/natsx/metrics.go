package natsx

const (
	MetricPublishTotal               = "foundationx_nats_publish_total"
	MetricPublishDurationMS          = "foundationx_nats_publish_duration_ms"
	MetricRequestTotal               = "foundationx_nats_request_total"
	MetricRequestDurationMS          = "foundationx_nats_request_duration_ms"
	MetricConsumeTotal               = "foundationx_nats_consume_total"
	MetricConsumeDurationMS          = "foundationx_nats_consume_duration_ms"
	MetricRedeliveryTotal            = "foundationx_nats_redelivery_total"
	MetricConnectionState            = "foundationx_nats_connection_state"
	MetricCoreMessagesTotal          = "foundationx_nats_messages_total"
	MetricJetStreamMessagesTotal     = "foundationx_nats_jetstream_messages_total"
	MetricClientCreatedTotal         = "foundationx_nats_client_created_total"
	MetricClientClosedTotal          = "foundationx_nats_client_closed_total"
	MetricClientErrorsTotal          = "foundationx_nats_client_errors_total"
	MetricClientHealthStatus         = "foundationx_nats_client_health_status"
	MetricClientHealthLatencyMS      = "foundationx_nats_client_health_latency_ms"
	MetricConnectionReconnectsTotal  = "foundationx_nats_connection_reconnects_total"
	MetricConnectionDisconnectsTotal = "foundationx_nats_connection_disconnects_total"

	// Deprecated aliases kept for downstream source compatibility.
	MetricCoreRequestDurationSeconds = MetricRequestDurationMS
	MetricCoreHandlerDurationSeconds = MetricConsumeDurationMS
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
