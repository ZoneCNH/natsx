package natsx

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestLiveNATSIntegration(t *testing.T) {
	if os.Getenv("NATSX_LIVE_INTEGRATION") != "1" {
		t.Skip("set NATSX_LIVE_INTEGRATION=1 with FOUNDATIONX_NATS_* or NATS_* to run live NATS integration")
	}

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	cfg.EnableJetStream = true
	cfg.Name = "natsx-live-integration"

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New() live connection failed with kind %q", errorKind(err))
	}
	defer func() { _ = client.Close(context.Background()) }()

	unique := time.Now().UnixNano()
	publishSubject := fmt.Sprintf("natsx.live.publish%d.v1", unique)
	requestSubject := fmt.Sprintf("natsx.live.request%d.v1", unique)
	jetStreamSubject := fmt.Sprintf("natsx.live.jetstream%d.v1", unique)

	received := make(chan Envelope, 1)
	sub, err := client.Subscribe(publishSubject, func(_ context.Context, env Envelope) (Envelope, error) {
		received <- env
		return Envelope{}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error kind = %q", errorKind(err))
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := sub.SetPendingLimits(1024, 1024*1024); err != nil {
		t.Fatalf("SetPendingLimits() error = %v", err)
	}

	payload := []byte("live-publish")
	if err := client.Publish(ctx, NewEnvelope(publishSubject, payload)); err != nil {
		t.Fatalf("Publish() error kind = %q", errorKind(err))
	}
	select {
	case env := <-received:
		if !bytes.Equal(env.Data, payload) {
			t.Fatalf("published payload = %q, want %q", env.Data, payload)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for live publish")
	}

	replySub, err := client.Subscribe(requestSubject, func(_ context.Context, env Envelope) (Envelope, error) {
		return NewEnvelope(env.Subject, []byte("live-reply")), nil
	})
	if err != nil {
		t.Fatalf("request Subscribe() error kind = %q", errorKind(err))
	}
	defer func() { _ = replySub.Unsubscribe() }()
	reply, err := client.Request(ctx, NewEnvelope(requestSubject, []byte("live-request")))
	if err != nil {
		t.Fatalf("Request() error kind = %q", errorKind(err))
	}
	if string(reply.Data) != "live-reply" {
		t.Fatalf("reply payload = %q, want live-reply", reply.Data)
	}

	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error kind = %q", errorKind(err))
	}
	streamName := fmt.Sprintf("NATSX_LIVE_%d", unique)
	if _, err := jsClient.AddStream(&StreamConfig{Name: streamName, Subjects: []string{jetStreamSubject}, Storage: nats.MemoryStorage}); err != nil {
		t.Fatalf("AddStream() error kind = %q", errorKind(err))
	}
	defer func() { _ = jsClient.DeleteStream(streamName) }()

	ack, err := jsClient.Publish(NewEnvelope(jetStreamSubject, []byte("live-jetstream")))
	if err != nil {
		t.Fatalf("JetStream Publish() error kind = %q", errorKind(err))
	}
	if ack == nil || ack.Stream != streamName {
		t.Fatalf("JetStream ack stream = %v, want %s", ack, streamName)
	}
	if _, err := jsClient.StreamInfo(streamName); err != nil {
		t.Fatalf("StreamInfo() error kind = %q", errorKind(err))
	}
}
