package natsx

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type logRecord struct {
	ctx   context.Context
	event LogEvent
}

type recordingLogger struct {
	mu      sync.Mutex
	records []logRecord
}

func (l *recordingLogger) LogNATSEvent(ctx context.Context, event LogEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, logRecord{ctx: ctx, event: event})
}

func (l *recordingLogger) hasEvent(name string, want LogEvent) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, record := range l.records {
		event := record.event
		if event.Name != name {
			continue
		}
		if want.Operation != "" && event.Operation != want.Operation {
			continue
		}
		if want.Subject != "" && event.Subject != want.Subject {
			continue
		}
		if want.Status != "" && event.Status != want.Status {
			continue
		}
		if want.EventID != "" && event.EventID != want.EventID {
			continue
		}
		if want.MessageID != "" && event.MessageID != want.MessageID {
			continue
		}
		if want.SchemaVersion != "" && event.SchemaVersion != want.SchemaVersion {
			continue
		}
		if want.TraceID != "" && event.TraceID != want.TraceID {
			continue
		}
		return true
	}
	return false
}

func (l *recordingLogger) snapshot() []LogEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LogEvent, 0, len(l.records))
	for _, record := range l.records {
		out = append(out, record.event)
	}
	return out
}

func TestEmbeddedNATSStructuredLogFields(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	logger := &recordingLogger{}
	client := newEmbeddedClientWithOptions(t, srv, false, WithLogger(logger))

	subject := mustSubject(t, "orders", "lookup", "request", 1)
	sub, err := client.Subscribe(subject, func(_ context.Context, env Envelope) (Envelope, error) {
		return Envelope{
			Subject:       env.Subject,
			EventID:       env.EventID,
			MessageID:     env.MessageID,
			SchemaVersion: env.SchemaVersion,
			TraceID:       env.TraceID,
			Data:          []byte("reply"),
		}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() after subscribe error = %v", err)
	}

	env := Envelope{
		Subject:       subject,
		EventID:       "event-log-1",
		MessageID:     "message-log-1",
		SchemaVersion: "orders.lookup.v1",
		TraceID:       "trace-log-1",
		Data:          []byte("secret-payload"),
	}
	if _, err := client.Request(context.Background(), env); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	want := LogEvent{
		Operation:     "request",
		Subject:       subject,
		Status:        "ok",
		EventID:       env.EventID,
		MessageID:     env.MessageID,
		SchemaVersion: env.SchemaVersion,
		TraceID:       env.TraceID,
	}
	waitForCondition(t, 2*time.Second, func() bool {
		return logger.hasEvent(LogRequest, want) &&
			logger.hasEvent(LogConsume, LogEvent{
				Operation:     "consume",
				Subject:       subject,
				Status:        "ok",
				EventID:       env.EventID,
				MessageID:     env.MessageID,
				SchemaVersion: env.SchemaVersion,
				TraceID:       env.TraceID,
			})
	}, "structured log metadata was not recorded")

	for _, event := range logger.snapshot() {
		if strings.Contains(fmt.Sprintf("%+v", event), "secret-payload") {
			t.Fatalf("log event leaked payload data: %+v", event)
		}
	}
}
