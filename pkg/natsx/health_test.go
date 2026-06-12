package natsx

import (
	"context"
	"testing"
)

type metricEvent struct {
	name   string
	value  float64
	labels map[string]string
}

type recordingMetrics struct {
	counters   []metricEvent
	gauges     []metricEvent
	histograms []metricEvent
}

func (m *recordingMetrics) IncCounter(name string, labels map[string]string) {
	m.counters = append(m.counters, metricEvent{name: name, labels: labels})
}
func (m *recordingMetrics) ObserveHistogram(name string, value float64, labels map[string]string) {
	m.histograms = append(m.histograms, metricEvent{name: name, value: value, labels: labels})
}
func (m *recordingMetrics) SetGauge(name string, value float64, labels map[string]string) {
	m.gauges = append(m.gauges, metricEvent{name: name, value: value, labels: labels})
}
func (m *recordingMetrics) counterValue(name, op string, kind ErrorKind) int {
	total := 0
	for _, event := range m.counters {
		if event.name == name && event.labels["op"] == op && event.labels["kind"] == string(kind) {
			total++
		}
	}
	return total
}

func TestHealthCheckDisconnectedRecordsMetrics(t *testing.T) {
	metrics := &recordingMetrics{}
	client := &Client{cfg: Config{Name: "svc"}.withDefaults(), metrics: metrics}

	status := client.HealthCheck(context.Background())
	if status.Name != "svc" || status.Status != HealthUnhealthy || status.Message == "" {
		t.Fatalf("unexpected health status: %+v", status)
	}
	if len(metrics.gauges) != 1 || metrics.gauges[0].name != MetricClientHealthStatus || metrics.gauges[0].value != 0 {
		t.Fatalf("unexpected gauge metrics: %+v", metrics.gauges)
	}
	if len(metrics.histograms) != 1 || metrics.histograms[0].name != MetricClientHealthLatencyMS {
		t.Fatalf("unexpected histogram metrics: %+v", metrics.histograms)
	}
}
