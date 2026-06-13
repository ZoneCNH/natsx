package natsx

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
	defer func() { _ = sub.Unsubscribe() }()
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

func BenchmarkEmbeddedNATSRequest(b *testing.B) {
	srv := runEmbeddedNATSServer(b, false)
	client := newEmbeddedClient(b, srv, false)
	subject := mustSubject(b, "orders", "benchmark", "request", 1)

	sub, err := client.Subscribe(subject, func(_ context.Context, env Envelope) (Envelope, error) {
		if env.Subject != subject {
			b.Errorf("handler subject = %q, want %q", env.Subject, subject)
		}
		return Envelope{Data: []byte("pong")}, nil
	})
	if err != nil {
		b.Fatalf("Subscribe() error = %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := client.Conn().Flush(); err != nil {
		b.Fatalf("Flush() error = %v", err)
	}

	env := Envelope{Subject: subject, EventID: "event-benchmark-request-1", Data: []byte("ping")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := client.Request(requestCtx, env)
		cancel()
		if err != nil {
			b.Fatalf("Request() error = %v", err)
		}
		if string(reply.Data) != "pong" {
			b.Fatalf("reply data = %q, want pong", reply.Data)
		}
	}
}

func BenchmarkEmbeddedNATSJetStreamPublish(b *testing.B) {
	srv := runEmbeddedNATSServer(b, true)
	client := newEmbeddedClient(b, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		b.Fatalf("JetStreamClient() error = %v", err)
	}

	subject := mustSubject(b, "orders", "benchmark", "publish", 1)
	if _, err := jsClient.AddStream(&StreamConfig{
		Name:     "BENCHMARKS",
		Subjects: []string{subject},
	}); err != nil {
		b.Fatalf("AddStream() error = %v", err)
	}

	env := Envelope{Subject: subject, EventID: "event-benchmark-js-1", Data: []byte("benchmark")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ack, err := jsClient.Publish(env, nats.MsgId("benchmark-js-"+strconv.Itoa(i)))
		if err != nil {
			b.Fatalf("JetStream Publish() error = %v", err)
		}
		if ack.Stream != "BENCHMARKS" {
			b.Fatalf("ack stream = %q, want BENCHMARKS", ack.Stream)
		}
	}
}
