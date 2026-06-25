package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/ZoneCNH/natsx/pkg/natsx"
	nats "github.com/nats-io/nats.go"
)

// mockPublisher 实现 JetStreamPublisher，记录调用并按预设返回。
type mockPublisher struct {
	lastEnv  natsx.Envelope
	lastOptN int
	ack      *natsx.PubAck
	err      error
}

func (m *mockPublisher) Publish(env natsx.Envelope, opts ...nats.PubOpt) (*natsx.PubAck, error) {
	m.lastEnv = env
	m.lastOptN = len(opts)
	if m.err != nil {
		return nil, m.err
	}
	return m.ack, nil
}

func TestPublisherPubAckReturnsDurableAck(t *testing.T) {
	mp := &mockPublisher{ack: &natsx.PubAck{Stream: "BINANCE_MARKET", Sequence: 42, Duplicate: false}}
	p := NewPublisher(mp, PublisherConfig{})

	res := p.Publish(context.Background(), PublishRequest{
		Subject:        "binance.market.um_perp.tick",
		Payload:        []byte(`{"k":"v"}`),
		IdempotencyKey: "req-001",
	})

	if !res.IsAck() {
		t.Fatalf("want ack, got reject %+v", res.Reject)
	}
	if res.Ack.Stream != "BINANCE_MARKET" {
		t.Errorf("Stream = %q, want BINANCE_MARKET", res.Ack.Stream)
	}
	if res.Ack.Sequence != 42 {
		t.Errorf("Sequence = %d, want 42", res.Ack.Sequence)
	}
	if res.Ack.Duplicate {
		t.Error("Duplicate = true, want false")
	}
	if mp.lastEnv.Subject != "binance.market.um_perp.tick" {
		t.Errorf("env subject = %q", mp.lastEnv.Subject)
	}
	if mp.lastOptN != 1 {
		t.Errorf("want 1 PubOpt (MsgId), got %d", mp.lastOptN)
	}
}

func TestPublisherEmptyIdempotencyKeyRejectedNonRetryable(t *testing.T) {
	p := NewPublisher(&mockPublisher{ack: &natsx.PubAck{}}, PublisherConfig{})
	res := p.Publish(context.Background(), PublishRequest{Subject: "s", Payload: []byte("x")})
	if res.IsAck() {
		t.Fatal("want reject for empty key")
	}
	if res.Reject.Code != "INVALID_REQUEST" {
		t.Errorf("Code = %q, want INVALID_REQUEST", res.Reject.Code)
	}
	if res.Reject.Retryable {
		t.Error("Retryable = true, want false")
	}
}

func TestPublisherEmptySubjectRejected(t *testing.T) {
	p := NewPublisher(&mockPublisher{ack: &natsx.PubAck{}}, PublisherConfig{})
	res := p.Publish(context.Background(), PublishRequest{IdempotencyKey: "k", Payload: []byte("x")})
	if res.IsAck() {
		t.Fatal("want reject for empty subject")
	}
	if res.Reject.Code != "INVALID_REQUEST" {
		t.Errorf("Code = %q, want INVALID_REQUEST", res.Reject.Code)
	}
}

func TestPublisherDuplicateFlagPropagated(t *testing.T) {
	mp := &mockPublisher{ack: &natsx.PubAck{Stream: "S", Sequence: 1, Duplicate: true}}
	p := NewPublisher(mp, PublisherConfig{})
	res := p.Publish(context.Background(), PublishRequest{Subject: "s", Payload: []byte("x"), IdempotencyKey: "k"})
	if !res.IsAck() {
		t.Fatal("want ack")
	}
	if !res.Ack.Duplicate {
		t.Error("Duplicate = false, want true")
	}
}

func TestPublisherPublishErrorRetryableForNonNatsxErr(t *testing.T) {
	mp := &mockPublisher{err: errors.New("connection dropped")}
	p := NewPublisher(mp, PublisherConfig{})
	res := p.Publish(context.Background(), PublishRequest{Subject: "s", Payload: []byte("x"), IdempotencyKey: "k"})
	if res.IsAck() {
		t.Fatal("want reject on publish error")
	}
	if res.Reject.Code != "JETSTREAM_PUBLISH_FAILED" {
		t.Errorf("Code = %q", res.Reject.Code)
	}
	if !res.Reject.Retryable {
		t.Error("Retryable = false, want true for non-natsx connection error")
	}
}

func TestPublisherPublishErrorNonRetryableForNatsxValidationErr(t *testing.T) {
	mp := &mockPublisher{err: &natsx.Error{Kind: natsx.ErrorKindValidation, Retryable: false}}
	p := NewPublisher(mp, PublisherConfig{})
	res := p.Publish(context.Background(), PublishRequest{Subject: "s", Payload: []byte("x"), IdempotencyKey: "k"})
	if res.IsAck() {
		t.Fatal("want reject")
	}
	if res.Reject.Retryable {
		t.Error("Retryable = true, want false for natsx validation error")
	}
}

func TestPublisherNilPublisherRejected(t *testing.T) {
	var p *Publisher
	res := p.Publish(context.Background(), PublishRequest{Subject: "s", Payload: []byte("x"), IdempotencyKey: "k"})
	if res.IsAck() {
		t.Fatal("want reject for nil publisher")
	}
	if res.Reject.Code != "NOT_INITIALIZED" {
		t.Errorf("Code = %q, want NOT_INITIALIZED", res.Reject.Code)
	}
}

// mockPullSubscription 实现 PullSubscription。
type mockPullSubscription struct {
	msgs []*nats.Msg
	err  error
	ack  int
	nak  int
	term int
}

func (m *mockPullSubscription) Fetch(max int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.msgs, nil
}
func (m *mockPullSubscription) Close() error { return nil }

type mockPullSubscriber struct {
	sub *mockPullSubscription
	err error
}

func (m *mockPullSubscriber) PullSubscribe(stream, subject, durable string) (PullSubscription, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.sub, nil
}

func TestConsumerFetchReturnsMessagesWithAckNakTerm(t *testing.T) {
	msg1 := &nats.Msg{Subject: "binance.market.um_perp.tick", Data: []byte("payload-1"), Header: nats.Header{"Nats-Msg-Id": {"k1"}}}
	sub := &mockPullSubscription{msgs: []*nats.Msg{msg1}}

	c, err := NewConsumer(&mockPullSubscriber{sub: sub}, ConsumerConfig{
		Stream: "BINANCE_MARKET", Subject: "binance.market.um_perp.>", Durable: "binance-server",
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	msgs, err := c.Fetch(context.Background(), 10)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 msg, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "payload-1" {
		t.Errorf("Payload = %q", msgs[0].Payload)
	}
	if msgs[0].Headers["Nats-Msg-Id"][0] != "k1" {
		t.Errorf("Header missing Nats-Msg-Id")
	}
	// Ack/Nak/Term 是 nats.Msg 方法，需真实 JetStream 才能验证语义；
	// 这里仅断言 FetchMessage 暴露了它们（非 nil），durable+ManualAck 语义由集成测试覆盖。
	if msgs[0].Ack == nil || msgs[0].Nak == nil || msgs[0].NakWithDelay == nil || msgs[0].Term == nil {
		t.Error("Ack/Nak/NakWithDelay/Term must be non-nil")
	}
}

func TestConsumerTermInvokesOnDeadLetterAtMaxDeliver(t *testing.T) {
	msg := &nats.Msg{
		Subject: "binance.market.um_perp.tick",
		Data:    []byte("poison"),
		Reply:   "$JS.ACK.BINANCE_MARKET.binance-server.2.10.1.0.0",
	}
	sub := &mockPullSubscription{msgs: []*nats.Msg{msg}}
	called := 0
	c, err := NewConsumer(&mockPullSubscriber{sub: sub}, ConsumerConfig{
		Stream: "BINANCE_MARKET", Subject: "binance.market.um_perp.>", Durable: "binance-server", MaxDeliver: 2,
		OnDeadLetter: func(msg FetchMessage) error {
			called++
			if string(msg.Payload) != "poison" {
				t.Fatalf("dead-letter payload = %q", msg.Payload)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	msgs, err := c.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 msg, got %d", len(msgs))
	}
	_ = msgs[0].Term() // nats.Msg.Term needs a live connection; hook execution is the contract tested here.
	if called != 1 {
		t.Fatalf("OnDeadLetter calls = %d, want 1", called)
	}
}

func TestConsumerTermSkipsOnDeadLetterBeforeMaxDeliver(t *testing.T) {
	msg := &nats.Msg{Subject: "s", Data: []byte("retry"), Reply: "$JS.ACK.S.d.1.10.1.0.0"}
	sub := &mockPullSubscription{msgs: []*nats.Msg{msg}}
	called := 0
	c, err := NewConsumer(&mockPullSubscriber{sub: sub}, ConsumerConfig{
		Stream: "S", Subject: "s", Durable: "d", MaxDeliver: 2,
		OnDeadLetter: func(FetchMessage) error { called++; return nil },
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	msgs, err := c.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	_ = msgs[0].Term()
	if called != 0 {
		t.Fatalf("OnDeadLetter calls = %d, want 0", called)
	}
}

func TestConsumerFetchNoMessagesReturnsNilNoError(t *testing.T) {
	sub := &mockPullSubscription{err: nats.ErrTimeout}
	c, _ := NewConsumer(&mockPullSubscriber{sub: sub}, ConsumerConfig{
		Stream: "S", Subject: "s", Durable: "d",
	})
	msgs, err := c.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("timeout should not be error, got %v", err)
	}
	if msgs != nil {
		t.Errorf("want nil msgs on timeout, got %d", len(msgs))
	}
}

func TestConsumerFetchSubscribeErrorReturnsFetchError(t *testing.T) {
	c, _ := NewConsumer(&mockPullSubscriber{err: errors.New("stream not found")}, ConsumerConfig{
		Stream: "S", Subject: "s", Durable: "d",
	})
	_, err := c.Fetch(context.Background(), 1)
	if err == nil {
		t.Fatal("want error")
	}
	var fe *FetchError
	if !errors.As(err, &fe) {
		t.Errorf("want *FetchError, got %T", err)
	}
}

func TestNewConsumerRequiresStreamSubjectDurable(t *testing.T) {
	if _, err := NewConsumer(&mockPullSubscriber{}, ConsumerConfig{}); err == nil {
		t.Error("want error for empty config")
	}
}
