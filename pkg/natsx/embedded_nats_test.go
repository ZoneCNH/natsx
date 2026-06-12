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

func headerValue(headers map[string][]string, key string) string {
	values := headers[key]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func TestEmbeddedNATSCorePublishAndRequest(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	publishSubject := Subject("orders", "created", "v1").String()
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
		Subject: publishSubject,
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
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published message")
	}

	requestSubject := Subject("orders", "lookup", "v1").String()
	requestSub, err := client.Subscribe(requestSubject, func(_ context.Context, env Envelope) (Envelope, error) {
		if !bytes.Equal(env.Data, []byte("lookup")) {
			t.Errorf("request data = %q, want lookup", env.Data)
		}
		if headerValue(env.Headers, "X-Request") != "ping" {
			t.Errorf("request X-Request header = %q, want ping", headerValue(env.Headers, "X-Request"))
		}
		return Envelope{
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

	sub, err := jsClient.PullSubscribe("orders.created.v1", "worker-b", nats.BindStream("ORDERS"))
	if err != nil {
		t.Fatalf("PullSubscribe() error = %v", err)
	}
	defer sub.Unsubscribe()

	ack, err := jsClient.Publish(Envelope{
		Subject: "orders.created.v1",
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
	if msg.Subject != "orders.created.v1" {
		t.Fatalf("JetStream subject = %q, want orders.created.v1", msg.Subject)
	}
	if !bytes.Equal(msg.Data, []byte("stored")) {
		t.Fatalf("JetStream data = %q, want stored", msg.Data)
	}
	if msg.Header.Get("X-Test") != "jetstream" {
		t.Fatalf("JetStream X-Test header = %q, want jetstream", msg.Header.Get("X-Test"))
	}
	if err := msg.Ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}
