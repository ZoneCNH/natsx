package natsx

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func runEmbeddedNATSServer(t testing.TB, jetStream bool) *natsserver.Server {
	t.Helper()
	return runEmbeddedNATSServerOnPort(t, jetStream, -1)
}

func runEmbeddedNATSServerOnPort(t testing.TB, jetStream bool, port int) *natsserver.Server {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      port,
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

func newEmbeddedClient(t testing.TB, srv *natsserver.Server, enableJetStream bool) *Client {
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

func mustSubject(t testing.TB, domain, resource, action string, version int) string {
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

func waitForCondition(t testing.TB, timeout time.Duration, condition func() bool, format string, args ...interface{}) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if condition() {
		return
	}
	t.Fatalf(format, args...)
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

func TestEmbeddedNATSCoreTimeoutUnsubscribeDrainAndHealth(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	health := client.HealthCheck(context.Background())
	if health.Status != HealthHealthy {
		t.Fatalf("HealthCheck() status = %q, want healthy: %s", health.Status, health.Message)
	}
	if health.Metadata["server_url"] == "" {
		t.Fatalf("HealthCheck() server_url metadata is empty: %+v", health.Metadata)
	}

	timeoutSubject := mustSubject(t, "orders", "slow", "request", 1)
	timeoutSub, err := client.Subscribe(timeoutSubject, func(_ context.Context, _ Envelope) (Envelope, error) {
		time.Sleep(200 * time.Millisecond)
		return Envelope{Data: []byte("late")}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe(timeout) error = %v", err)
	}
	defer timeoutSub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() after timeout subscribe error = %v", err)
	}

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer timeoutCancel()
	if _, err := client.Request(timeoutCtx, Envelope{Subject: timeoutSubject, Data: []byte("slow")}); !IsKind(err, ErrorKindTimeout) {
		t.Fatalf("Request(timeout) error = %v, want timeout kind", err)
	}

	canceledCtx, canceledCancel := context.WithCancel(context.Background())
	canceledCancel()
	if _, err := client.Request(canceledCtx, Envelope{Subject: timeoutSubject, Data: []byte("cancel")}); !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("Request(canceled) error = %v, want unavailable kind", err)
	}

	unsubscribeSubject := mustSubject(t, "orders", "gone", "publish", 1)
	delivered := make(chan Envelope, 1)
	unsubscribeSub, err := client.Subscribe(unsubscribeSubject, func(_ context.Context, env Envelope) (Envelope, error) {
		delivered <- env
		return Envelope{}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe(unsubscribe) error = %v", err)
	}
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() before unsubscribe error = %v", err)
	}
	if err := unsubscribeSub.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe() error = %v", err)
	}
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() after unsubscribe error = %v", err)
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer publishCancel()
	if err := client.Publish(publishCtx, Envelope{Subject: unsubscribeSubject, Data: []byte("gone")}); err != nil {
		t.Fatalf("Publish(after unsubscribe) error = %v", err)
	}
	select {
	case env := <-delivered:
		t.Fatalf("received message after unsubscribe: %+v", env)
	case <-time.After(200 * time.Millisecond):
	}

	drainSubject := mustSubject(t, "orders", "drain", "publish", 1)
	drained := make(chan Envelope, 2)
	drainSub, err := client.Subscribe(drainSubject, func(_ context.Context, env Envelope) (Envelope, error) {
		drained <- env
		return Envelope{}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe(drain) error = %v", err)
	}
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() before drain error = %v", err)
	}
	drainPublishCtx, drainPublishCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := client.Publish(drainPublishCtx, Envelope{Subject: drainSubject, Data: []byte("before-drain")}); err != nil {
		drainPublishCancel()
		t.Fatalf("Publish(before drain) error = %v", err)
	}
	drainPublishCancel()
	select {
	case env := <-drained:
		if !bytes.Equal(env.Data, []byte("before-drain")) {
			t.Fatalf("drain pre-message data = %q, want before-drain", env.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message before drain")
	}
	if err := drainSub.Drain(); err != nil {
		t.Fatalf("Subscription.Drain() error = %v", err)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		return !drainSub.IsValid()
	}, "Subscription.Drain() left subscription valid")
	afterDrainCtx, afterDrainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := client.Publish(afterDrainCtx, Envelope{Subject: drainSubject, Data: []byte("after-drain")}); err != nil {
		afterDrainCancel()
		t.Fatalf("Publish(after drain) error = %v", err)
	}
	afterDrainCancel()
	select {
	case env := <-drained:
		t.Fatalf("received message after subscription drain: %+v", env)
	case <-time.After(200 * time.Millisecond):
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	closeClient, err := New(closeCtx, Config{
		Name:         "natsx-close-test",
		URL:          srv.ClientURL(),
		Timeout:      2 * time.Second,
		DrainTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New(close client) error = %v", err)
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	if err := closeClient.Close(drainCtx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	waitForCondition(t, 500*time.Millisecond, func() bool {
		return closeClient.Conn().IsClosed()
	}, "Close() left NATS connection open")
	closedHealth := closeClient.HealthCheck(context.Background())
	if closedHealth.Status != HealthUnhealthy {
		t.Fatalf("HealthCheck(closed) status = %q, want unhealthy", closedHealth.Status)
	}
}

func TestEmbeddedNATSReconnectBackoffAndDegradedHealth(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	tcpAddr, ok := srv.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("embedded server addr = %T, want *net.TCPAddr", srv.Addr())
	}

	metrics := &recordingMetrics{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, Config{
		Name:          "natsx-reconnect-test",
		URL:           srv.ClientURL(),
		Timeout:       time.Second,
		DrainTimeout:  time.Second,
		MaxReconnects: 100,
		ReconnectWait: 20 * time.Millisecond,
	}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New(reconnect client) error = %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		if err := client.Close(closeCtx); err != nil {
			t.Fatalf("Close(reconnect client) error = %v", err)
		}
	}()

	srv.Shutdown()
	srv.WaitForShutdown()
	waitForCondition(t, 3*time.Second, func() bool {
		return !client.Conn().IsConnected() && !client.Conn().IsClosed()
	}, "client did not enter reconnecting state after server shutdown")

	degraded := client.HealthCheck(context.Background())
	if degraded.Status != HealthDegraded {
		t.Fatalf("HealthCheck(reconnecting) status = %q, want degraded: %s", degraded.Status, degraded.Message)
	}

	restarted := runEmbeddedNATSServerOnPort(t, false, tcpAddr.Port)
	waitForCondition(t, 5*time.Second, func() bool {
		return restarted.ReadyForConnections(10*time.Millisecond) && client.Conn().IsConnected()
	}, "client did not reconnect to restarted server")

	healthy := client.HealthCheck(context.Background())
	if healthy.Status != HealthHealthy {
		t.Fatalf("HealthCheck(reconnected) status = %q, want healthy: %s", healthy.Status, healthy.Message)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		return metrics.counterWithLabel(MetricConnectionReconnectsTotal, "name", "natsx-reconnect-test") > 0
	}, "reconnect metric %s was not recorded", MetricConnectionReconnectsTotal)
}

func TestEmbeddedNATSJetStreamPublishAndPull(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)

	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}

	streamConfig := &StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	}
	stream, err := jsClient.AddStream(streamConfig)
	if err != nil {
		t.Fatalf("AddStream() error = %v", err)
	}
	if stream.Config.Name != "ORDERS" {
		t.Fatalf("stream name = %q, want ORDERS", stream.Config.Name)
	}
	sameStream, err := jsClient.AddStream(streamConfig)
	if err != nil {
		t.Fatalf("AddStream(same config) error = %v", err)
	}
	if sameStream.Config.Name != "ORDERS" {
		t.Fatalf("same stream name = %q, want ORDERS", sameStream.Config.Name)
	}
	_, err = jsClient.AddStream(&StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.conflict.>"},
	})
	if !IsKind(err, ErrorKindConflict) {
		t.Fatalf("AddStream(conflicting config) error = %v, want conflict kind", err)
	}
	streamInfo, err := jsClient.StreamInfo("ORDERS")
	if err != nil {
		t.Fatalf("StreamInfo() error = %v", err)
	}
	if streamInfo.Config.Name != "ORDERS" {
		t.Fatalf("StreamInfo() stream name = %q, want ORDERS", streamInfo.Config.Name)
	}
	if _, err := jsClient.StreamInfo("MISSING"); !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("StreamInfo(missing) error = %v, want unavailable kind", err)
	}
	if err := jsClient.DeleteStream(" "); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("DeleteStream(blank) error = %v, want validation kind", err)
	}

	jetStreamSubject := mustSubject(t, "orders", "created", "publish", 1)
	missingStreamSubject := mustSubject(t, "unknown", "created", "publish", 1)
	if _, err := jsClient.Publish(Envelope{Subject: missingStreamSubject, Data: []byte("missing")}); !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("JetStream Publish(missing stream) error = %v, want unavailable kind", err)
	}

	consumerConfig := &ConsumerConfig{
		Durable:       "worker-b",
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       200 * time.Millisecond,
		MaxDeliver:    2,
		FilterSubject: jetStreamSubject,
	}
	consumer, err := jsClient.AddConsumer("ORDERS", consumerConfig)
	if err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}
	if consumer.Config.Durable != "worker-b" {
		t.Fatalf("consumer durable = %q, want worker-b", consumer.Config.Durable)
	}
	sameConsumer, err := jsClient.AddConsumer("ORDERS", consumerConfig)
	if err != nil {
		t.Fatalf("AddConsumer(same config) error = %v", err)
	}
	if sameConsumer.Config.Durable != "worker-b" {
		t.Fatalf("same consumer durable = %q, want worker-b", sameConsumer.Config.Durable)
	}
	_, err = jsClient.AddConsumer("ORDERS", &ConsumerConfig{
		Durable:       "worker-b",
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       200 * time.Millisecond,
		MaxDeliver:    2,
		FilterSubject: mustSubject(t, "orders", "updated", "publish", 1),
	})
	if !IsKind(err, ErrorKindConflict) {
		t.Fatalf("AddConsumer(conflicting config) error = %v, want conflict kind", err)
	}

	sub, err := jsClient.PullSubscribe(jetStreamSubject, "worker-b", nats.Bind("ORDERS", "worker-b"))
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
	if err := msg.Nak(); err != nil {
		t.Fatalf("Nak() error = %v", err)
	}

	redeliveredMsgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch(redelivery) error = %v", err)
	}
	if len(redeliveredMsgs) != 1 {
		t.Fatalf("Fetch(redelivery) returned %d messages, want 1", len(redeliveredMsgs))
	}
	redelivered := redeliveredMsgs[0]
	if !bytes.Equal(redelivered.Data, []byte("stored")) {
		t.Fatalf("redelivered data = %q, want stored", redelivered.Data)
	}
	metadata, err := redelivered.Metadata()
	if err != nil {
		t.Fatalf("Metadata(redelivery) error = %v", err)
	}
	if metadata.NumDelivered < 2 {
		t.Fatalf("redelivery count = %d, want at least 2", metadata.NumDelivered)
	}
	if err := redelivered.Ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestEmbeddedNATSJetStreamMaxDeliverAdvisory(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)

	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}

	advisorySub, err := client.Conn().SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.DLQ.dlq-worker")
	if err != nil {
		t.Fatalf("Subscribe(max deliveries advisory) error = %v", err)
	}
	defer advisorySub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() after advisory subscribe error = %v", err)
	}

	subject := mustSubject(t, "orders", "deadletter", "publish", 1)
	if _, err := jsClient.AddStream(&StreamConfig{
		Name:     "DLQ",
		Subjects: []string{subject},
	}); err != nil {
		t.Fatalf("AddStream(DLQ) error = %v", err)
	}
	if _, err := jsClient.AddConsumer("DLQ", &ConsumerConfig{
		Durable:       "dlq-worker",
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       50 * time.Millisecond,
		MaxDeliver:    2,
		FilterSubject: subject,
	}); err != nil {
		t.Fatalf("AddConsumer(DLQ) error = %v", err)
	}

	sub, err := jsClient.PullSubscribe(subject, "dlq-worker", nats.Bind("DLQ", "dlq-worker"))
	if err != nil {
		t.Fatalf("PullSubscribe(DLQ) error = %v", err)
	}
	defer sub.Unsubscribe()
	if _, err := jsClient.Publish(Envelope{Subject: subject, EventID: "event-dlq-1", Data: []byte("poison")}); err != nil {
		t.Fatalf("JetStream Publish(DLQ) error = %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		msgs, err := sub.Fetch(1, nats.MaxWait(3*time.Second))
		if err != nil {
			t.Fatalf("Fetch(DLQ attempt %d) error = %v", attempt, err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Fetch(DLQ attempt %d) returned %d messages, want 1", attempt, len(msgs))
		}
		metadata, err := msgs[0].Metadata()
		if err != nil {
			t.Fatalf("Metadata(DLQ attempt %d) error = %v", attempt, err)
		}
		if metadata.NumDelivered < uint64(attempt) {
			t.Fatalf("delivery attempt = %d, want at least %d", metadata.NumDelivered, attempt)
		}
		if attempt < 2 {
			if err := msgs[0].Nak(); err != nil {
				t.Fatalf("Nak(DLQ attempt %d) error = %v", attempt, err)
			}
		}
	}

	time.Sleep(150 * time.Millisecond)
	msgs, err := sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if err == nil {
		t.Fatalf("Fetch(DLQ after MaxDeliver) returned %d messages, want timeout", len(msgs))
	}
	if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("Fetch(DLQ after MaxDeliver) error = %v, want %v", err, nats.ErrTimeout)
	}

	advisory, err := advisorySub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg(max deliveries advisory) error = %v", err)
	}
	if advisory.Subject == "" || len(advisory.Data) == 0 {
		t.Fatalf("empty max-deliveries advisory: subject=%q data=%q", advisory.Subject, advisory.Data)
	}
}
