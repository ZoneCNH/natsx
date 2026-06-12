package natsx

import (
	"bytes"
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func runEmbeddedNATSServer(t *testing.T, jetStream bool) *natsserver.Server {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: jetStream,
	}
	if jetStream {
		opts.StoreDir = t.TempDir()
	}

	srv := natsserver.New(opts)
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("embedded NATS server did not become ready")
	}

	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return srv
}

func newEmbeddedClient(t *testing.T, srv *natsserver.Server, enableJetStream bool) *Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	client, err := New(ctx, Config{
		Name:            "natsx-test",
		URL:             srv.ClientURL(),
		Timeout:         2 * time.Second,
		DrainTimeout:    2 * time.Second,
		EnableJetStream: enableJetStream,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		if err := client.Close(closeCtx); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return client
}

func mustSubject(t *testing.T, domain, resource, action string, version int) string {
	t.Helper()

	subject, err := Subject().Build(domain, resource, action, version)
	if err != nil {
		t.Fatalf("BuildSubject() error = %v", err)
	}
	return subject
}

func headerValue(headers map[string][]string, key string) string {
	values := headers[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func TestEmbeddedNATSCorePublishRequestAndQueue(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	publishSubject := mustSubject(t, "orders", "created", "publish", 1)
	received := make(chan Envelope, 1)
	sub, err := client.Subscribe(publishSubject, func(_ context.Context, env Envelope) (Envelope, error) {
		received <- env
		return Envelope{}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() after subscribe error = %v", err)
	}

	publishCtx, publishCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer publishCancel()
	sent := Envelope{
		Subject:       publishSubject,
		EventID:       "event-core-1",
		MessageID:     "message-core-1",
		SchemaVersion: "orders.created.v1",
		TraceID:       "trace-core-1",
		Headers: map[string][]string{
			"X-Test": {"worker-b"},
		},
		Data: []byte("created"),
	}
	if err := client.Publish(publishCtx, sent); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case got := <-received:
		if got.Subject != publishSubject {
			t.Fatalf("published subject = %q, want %q", got.Subject, publishSubject)
		}
		if !bytes.Equal(got.Data, sent.Data) {
			t.Fatalf("published data = %q, want %q", got.Data, sent.Data)
		}
		if headerValue(got.Headers, "X-Test") != "worker-b" {
			t.Fatalf("published X-Test header = %q, want worker-b", headerValue(got.Headers, "X-Test"))
		}
		if got.EventID != sent.EventID {
			t.Fatalf("published EventID = %q, want %q", got.EventID, sent.EventID)
		}
		if got.MessageID != sent.MessageID {
			t.Fatalf("published MessageID = %q, want %q", got.MessageID, sent.MessageID)
		}
		if got.SchemaVersion != sent.SchemaVersion {
			t.Fatalf("published SchemaVersion = %q, want %q", got.SchemaVersion, sent.SchemaVersion)
		}
		if got.TraceID != sent.TraceID {
			t.Fatalf("published TraceID = %q, want %q", got.TraceID, sent.TraceID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published message")
	}

	requestSubject := mustSubject(t, "orders", "lookup", "request", 1)
	requestSub, err := client.Subscribe(requestSubject, func(_ context.Context, env Envelope) (Envelope, error) {
		if !bytes.Equal(env.Data, []byte("lookup")) {
			t.Errorf("request data = %q, want lookup", env.Data)
		}
		if headerValue(env.Headers, "X-Request") != "ping" {
			t.Errorf("request X-Request header = %q, want ping", headerValue(env.Headers, "X-Request"))
		}
		return Envelope{
			EventID: "event-reply-1",
			TraceID: env.TraceID,
			Headers: map[string][]string{
				"X-Reply": {"ok"},
			},
			Data: []byte("found"),
		}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe() request error = %v", err)
	}
	defer requestSub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() after request subscribe error = %v", err)
	}

	requestCtx, requestCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer requestCancel()
	reply, err := client.Request(requestCtx, Envelope{
		Subject: requestSubject,
		TraceID: "trace-request-1",
		Headers: map[string][]string{
			"X-Request": {"ping"},
		},
		Data: []byte("lookup"),
	})
	if err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if !bytes.Equal(reply.Data, []byte("found")) {
		t.Fatalf("reply data = %q, want found", reply.Data)
	}
	if headerValue(reply.Headers, "X-Reply") != "ok" {
		t.Fatalf("reply X-Reply header = %q, want ok", headerValue(reply.Headers, "X-Reply"))
	}
	if reply.EventID != "event-reply-1" {
		t.Fatalf("reply EventID = %q, want event-reply-1", reply.EventID)
	}
	if reply.TraceID != "trace-request-1" {
		t.Fatalf("reply TraceID = %q, want trace-request-1", reply.TraceID)
	}

	queueSubject := mustSubject(t, "orders", "created", "work", 1)
	queueReceived := make(chan Envelope, 2)
	for i := 0; i < 2; i++ {
		queueSub, err := client.QueueSubscribe(queueSubject, "order-workers", func(_ context.Context, env Envelope) (Envelope, error) {
			queueReceived <- env
			return Envelope{}, nil
		})
		if err != nil {
			t.Fatalf("QueueSubscribe() error = %v", err)
		}
		defer queueSub.Unsubscribe()
	}
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() after queue subscribe error = %v", err)
	}
	queueCtx, queueCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer queueCancel()
	if err := client.Publish(queueCtx, Envelope{Subject: queueSubject, EventID: "event-queue-1", Data: []byte("queued")}); err != nil {
		t.Fatalf("Publish(queue) error = %v", err)
	}
	select {
	case got := <-queueReceived:
		if got.EventID != "event-queue-1" {
			t.Fatalf("queue EventID = %q, want event-queue-1", got.EventID)
		}
		if !bytes.Equal(got.Data, []byte("queued")) {
			t.Fatalf("queue data = %q, want queued", got.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queue message")
	}
	select {
	case got := <-queueReceived:
		t.Fatalf("queue group delivered one message to more than one subscriber: %+v", got)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestEmbeddedNATSRequestNoResponder(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := client.Request(ctx, Envelope{Subject: mustSubject(t, "orders", "missing", "request", 1), Data: []byte("missing")}); !IsKind(err, ErrorKindConnection) {
		t.Fatalf("Request(no responders) error = %v, want connection kind", err)
	}
}

func TestEmbeddedNATSJetStreamPublishAndPull(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)

	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}

	stream, err := jsClient.AddStream(&StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	})
	if err != nil {
		t.Fatalf("AddStream() error = %v", err)
	}
	if stream.Config.Name != "ORDERS" {
		t.Fatalf("stream name = %q, want ORDERS", stream.Config.Name)
	}
	streamInfo, err := jsClient.StreamInfo("ORDERS")
	if err != nil {
		t.Fatalf("StreamInfo() error = %v", err)
	}
	if streamInfo.Config.Name != "ORDERS" {
		t.Fatalf("StreamInfo() stream name = %q, want ORDERS", streamInfo.Config.Name)
	}

	jetStreamSubject := mustSubject(t, "orders", "created", "publish", 1)
	sub, err := jsClient.PullSubscribe(jetStreamSubject, "worker-b", nats.BindStream("ORDERS"))
	if err != nil {
		t.Fatalf("PullSubscribe() error = %v", err)
	}
	defer sub.Unsubscribe()

	ack, err := jsClient.Publish(Envelope{
		Subject:       jetStreamSubject,
		EventID:       "event-js-1",
		MessageID:     "message-js-1",
		SchemaVersion: "orders.created.v1",
		TraceID:       "trace-js-1",
		Headers: map[string][]string{
			"X-Test": {"jetstream"},
		},
		Data: []byte("stored"),
	}, nats.MsgId("worker-b-1"))
	if err != nil {
		t.Fatalf("JetStream Publish() error = %v", err)
	}
	if ack.Stream != "ORDERS" {
		t.Fatalf("ack stream = %q, want ORDERS", ack.Stream)
	}
	if ack.Sequence != 1 {
		t.Fatalf("ack sequence = %d, want 1", ack.Sequence)
	}

	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Fetch() returned %d messages, want 1", len(msgs))
	}
	msg := msgs[0]
	if msg.Subject != jetStreamSubject {
		t.Fatalf("JetStream subject = %q, want %q", msg.Subject, jetStreamSubject)
	}
	if !bytes.Equal(msg.Data, []byte("stored")) {
		t.Fatalf("JetStream data = %q, want stored", msg.Data)
	}
	if msg.Header.Get("X-Test") != "jetstream" {
		t.Fatalf("JetStream X-Test header = %q, want jetstream", msg.Header.Get("X-Test"))
	}
	env := EnvelopeFromMsg(msg)
	if env.EventID != "event-js-1" {
		t.Fatalf("JetStream EventID = %q, want event-js-1", env.EventID)
	}
	if env.MessageID != "message-js-1" {
		t.Fatalf("JetStream MessageID = %q, want message-js-1", env.MessageID)
	}
	if env.SchemaVersion != "orders.created.v1" {
		t.Fatalf("JetStream SchemaVersion = %q, want orders.created.v1", env.SchemaVersion)
	}
	if env.TraceID != "trace-js-1" {
		t.Fatalf("JetStream TraceID = %q, want trace-js-1", env.TraceID)
	}
	if err := msg.Ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}
