package natsx

const (
	MetricClientCreatedTotal         = "natsx_client_created_total"
	MetricClientClosedTotal          = "natsx_client_closed_total"
	MetricClientErrorsTotal          = "natsx_client_errors_total"
	MetricClientHealthStatus         = "natsx_client_health_status"
	MetricClientHealthLatencyMS      = "natsx_client_health_latency_ms"
	MetricCoreMessagesTotal          = "natsx_core_messages_total"
	MetricCoreRequestDurationSeconds = "natsx_core_request_duration_seconds"
	MetricJetStreamMessagesTotal     = "natsx_jetstream_messages_total"
	MetricConnectionReconnectsTotal  = "natsx_connection_reconnects_total"
	MetricConnectionDisconnectsTotal = "natsx_connection_disconnects_total"
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
