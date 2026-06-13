package natsx

import (
	"context"
	"sync"
	"testing"
)

type metricEvent struct {
	name   string
	value  float64
	labels map[string]string
}

type recordingMetrics struct {
	mu         sync.Mutex
	counters   []metricEvent
	gauges     []metricEvent
	histograms []metricEvent
}

func (m *recordingMetrics) IncCounter(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters = append(m.counters, metricEvent{name: name, labels: labels})
}
func (m *recordingMetrics) ObserveHistogram(name string, value float64, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histograms = append(m.histograms, metricEvent{name: name, value: value, labels: labels})
}
func (m *recordingMetrics) SetGauge(name string, value float64, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges = append(m.gauges, metricEvent{name: name, value: value, labels: labels})
}
func (m *recordingMetrics) counterValue(name, op string, kind ErrorKind) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, event := range m.counters {
		if event.name == name && event.labels["op"] == op && event.labels["kind"] == string(kind) {
			total++
		}
	}
	return total
}
func (m *recordingMetrics) counterWithLabel(name, key, value string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, event := range m.counters {
		if event.name == name && event.labels[key] == value {
			total++
		}
	}
	return total
}

func (m *recordingMetrics) histogramWithLabels(name string, labels map[string]string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, event := range m.histograms {
		if event.name != name {
			continue
		}
		match := true
		for key, value := range labels {
			if event.labels[key] != value {
				match = false
				break
			}
		}
		if match {
			total++
		}
	}
	return total
}

func (m *recordingMetrics) gaugeWithLabels(name string, value float64, labels map[string]string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, event := range m.gauges {
		if event.name != name || event.value != value {
			continue
		}
		match := true
		for key, want := range labels {
			if event.labels[key] != want {
				match = false
				break
			}
		}
		if match {
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
	if got := metrics.gaugeWithLabels(MetricClientHealthStatus, 0, map[string]string{"name": "svc", "status": string(HealthUnhealthy)}); got != 1 {
		t.Fatalf("client health gauge count = %d, want 1: %+v", got, metrics.gauges)
	}
	if got := metrics.gaugeWithLabels(MetricConnectionState, 0, map[string]string{"name": "svc", "status": string(HealthUnhealthy)}); got != 1 {
		t.Fatalf("unexpected gauge metrics: %+v", metrics.gauges)
	}
	if len(metrics.histograms) != 1 || metrics.histograms[0].name != MetricClientHealthLatencyMS {
		t.Fatalf("unexpected histogram metrics: %+v", metrics.histograms)
	}
}
