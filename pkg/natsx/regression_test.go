package natsx

import (
	"context"
	"strings"
	"testing"
)

func TestNewRejectsNilContextAndRecordsMetric(t *testing.T) {
	metrics := &recordingMetrics{}
	//nolint:staticcheck // verifies nil context validation.
	client, err := New(nil, Config{}, WithMetrics(metrics))
	if client != nil {
		t.Fatalf("New(nil) client = %v, want nil", client)
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("New(nil) error = %v, want validation", err)
	}
	if got := metrics.counterValue(MetricClientErrorsTotal, "new", ErrorKindValidation); got != 1 {
		t.Fatalf("error metric count = %d, want 1", got)
	}
}

func TestNewRejectsCanceledContextAndRecordsMetric(t *testing.T) {
	metrics := &recordingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client, err := New(ctx, Config{}, WithMetrics(metrics))
	if client != nil {
		t.Fatalf("New(canceled) client = %v, want nil", client)
	}
	if !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("New(canceled) error = %v, want unavailable", err)
	}
	if got := metrics.counterValue(MetricClientErrorsTotal, "new", ErrorKindUnavailable); got != 1 {
		t.Fatalf("error metric count = %d, want 1", got)
	}
}

func TestNewRejectsInvalidNKeySeed(t *testing.T) {
	_, err := New(context.Background(), Config{NKeySeed: "not-a-seed"})
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("New(invalid nkey) error = %v, want validation", err)
	}
}

func TestWrapRejectsNilConn(t *testing.T) {
	client, err := Wrap(nil)
	if client != nil {
		t.Fatalf("Wrap(nil) client = %v, want nil", client)
	}
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Wrap(nil) error = %v, want validation", err)
	}
}

func TestCoreOperationsRejectInvalidPreconditions(t *testing.T) {
	var client *Client
	if err := client.Publish(context.Background(), NewEnvelope("subject", nil)); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Publish(nil client) error = %v, want validation", err)
	}
	if _, err := client.Request(context.Background(), NewEnvelope("subject", nil)); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Request(nil client) error = %v, want validation", err)
	}
	if _, err := client.Subscribe("subject", func(context.Context, Envelope) (Envelope, error) { return Envelope{}, nil }); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Subscribe(nil client) error = %v, want validation", err)
	}

	client = &Client{metrics: NoopMetrics{}}
	//nolint:staticcheck // verifies nil context validation.
	if err := client.Publish(nil, NewEnvelope("subject", nil)); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Publish(nil ctx) error = %v, want validation", err)
	}
	if _, err := client.Subscribe("subject", nil); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Subscribe(nil handler) error = %v, want validation", err)
	}
}

func TestJetStreamGuards(t *testing.T) {
	var client *Client
	if _, err := client.JetStreamClient(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("JetStreamClient(nil) error = %v, want validation", err)
	}

	var js *JetStreamClient
	if _, err := js.AddStream(&StreamConfig{Name: "ORDERS"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("AddStream(nil js) error = %v, want validation", err)
	}
	if err := js.DeleteStream("ORDERS"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("DeleteStream(nil js) error = %v, want validation", err)
	}
	if _, err := js.StreamInfo("ORDERS"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("StreamInfo(nil js) error = %v, want validation", err)
	}
	if _, err := js.AddConsumer("ORDERS", &ConsumerConfig{Durable: "worker"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("AddConsumer(nil js) error = %v, want validation", err)
	}
	if _, err := js.Publish(NewEnvelope("orders.created", nil)); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Publish(nil js) error = %v, want validation", err)
	}
	if _, err := js.PullSubscribe("orders.created", "worker"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("PullSubscribe(nil js) error = %v, want validation", err)
	}
}

func TestHealthCheckNilAndCanceledContext(t *testing.T) {
	client := &Client{cfg: Config{Name: "svc"}.withDefaults(), metrics: NoopMetrics{}}
	//nolint:staticcheck // verifies nil context validation.
	if status := client.HealthCheck(nil); status.Status != HealthUnhealthy || status.Message == "" {
		t.Fatalf("HealthCheck(nil ctx) = %+v, want unhealthy with message", status)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if status := client.HealthCheck(ctx); status.Status != HealthUnhealthy || status.Message == "" {
		t.Fatalf("HealthCheck(canceled ctx) = %+v, want unhealthy with message", status)
	}
}

func TestNoopMetricsMethodsAreSafe(t *testing.T) {
	var metrics Metrics = NoopMetrics{}
	metrics.IncCounter("counter", map[string]string{"k": "v"})
	metrics.ObserveHistogram("hist", 1, nil)
	metrics.SetGauge("gauge", 1, nil)
}

func TestMetricNamesUseFoundationNATSPrefix(t *testing.T) {
	metrics := []string{
		MetricClientCreatedTotal,
		MetricClientClosedTotal,
		MetricClientErrorsTotal,
		MetricClientHealthStatus,
		MetricClientHealthLatencyMS,
		MetricCoreMessagesTotal,
		MetricCoreRequestDurationSeconds,
		MetricJetStreamMessagesTotal,
		MetricConnectionReconnectsTotal,
		MetricConnectionDisconnectsTotal,
	}
	for _, metric := range metrics {
		if !strings.HasPrefix(metric, "foundationx_nats_") {
			t.Fatalf("metric %q does not use foundationx_nats_ prefix", metric)
		}
	}
}
