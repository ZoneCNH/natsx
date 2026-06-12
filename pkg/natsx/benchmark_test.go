package natsx

import (
	"context"
	"testing"
	"time"
)

func BenchmarkEmbeddedNATSPublish(b *testing.B) {
	srv := runEmbeddedNATSServer(b, false)
	client := newEmbeddedClient(b, srv, false)
	subject := mustSubject(b, "orders", "benchmark", "publish", 1)
	received := make(chan struct{}, 1)

	sub, err := client.Subscribe(subject, func(_ context.Context, env Envelope) (Envelope, error) {
		if env.Subject != subject {
			b.Errorf("handler subject = %q, want %q", env.Subject, subject)
		}
		select {
		case received <- struct{}{}:
		default:
		}
		return Envelope{}, nil
	})
	if err != nil {
		b.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		b.Fatalf("Flush() error = %v", err)
	}

	env := Envelope{Subject: subject, EventID: "event-benchmark-1", Data: []byte("benchmark")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		publishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := client.Publish(publishCtx, env)
		cancel()
		if err != nil {
			b.Fatalf("Publish() error = %v", err)
		}
		select {
		case <-received:
		case <-time.After(2 * time.Second):
			b.Fatal("timed out waiting for published message")
		}
	}
}
