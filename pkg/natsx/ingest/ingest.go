// Package ingest 提供 JetStream 域适配器，把 domain 模块的归一化事件经 JetStream
// 发布/消费，使 natsx 承担 JetStream 适配职责（FR-009/FR-010）。
//
// 本包不依赖任何 domain wire 类型——它是通用契约：调用方提供 subject、payload、
// idempotency key，本包负责 JetStream PubAck/durable consumer/ManualAck 语义。
// domain 模块（如 binance）在自己的 wire.IngestEndpoint 实现里调用本包。
package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ZoneCNH/natsx/pkg/natsx"
	nats "github.com/nats-io/nats.go"
)

// PublishRequest 是通用发布请求，不绑 domain wire。
type PublishRequest struct {
	// Subject 是 JetStream subject，如 binance.market.um_perp.tick。
	Subject string
	// Payload 是序列化后的事件字节。
	Payload []byte
	// IdempotencyKey 用作 Nats-Msg-Id，让 JetStream 服务器侧幂等去重。
	IdempotencyKey string
}

// PublishAck 是发布结果。Accepted=true 表示 JetStream 已持久化。
type PublishAck struct {
	// Stream 是接受消息的 stream 名。
	Stream string
	// Sequence 是 stream 内分配的序列号。
	Sequence uint64
	// Duplicate 报告是否为 JetStream 判定的重复消息。
	Duplicate bool
}

// PublishReject 是发布拒绝。
type PublishReject struct {
	Code      string
	Reason    string
	Retryable bool
}

// PublishResult 是 Publish 的终端结果，Ack 与 Reject 恰一非 nil。
type PublishResult struct {
	Ack    *PublishAck
	Reject *PublishReject
}

// IsAck 报告是否为接受。
func (r PublishResult) IsAck() bool { return r.Ack != nil }

// JetStreamPublisher 是 FR-009 依赖的 JetStream 发布契约。
// 与 natsx.JetStreamClient.Publish 签名对齐，便于直接传入生产实现。
type JetStreamPublisher interface {
	Publish(env natsx.Envelope, opts ...nats.PubOpt) (*natsx.PubAck, error)
}

// Publisher 实现 FR-009 IngestPublisher 域适配器。
type Publisher struct {
	js       JetStreamPublisher
	now      func() time.Time
	streamID string
}

// PublisherConfig 是 Publisher 配置。
type PublisherConfig struct {
	// StreamID 写入 PublishAck.Stream，便于调用方对账（默认 "BINANCE_MARKET"）。
	StreamID string
}

// NewPublisher 构造 Publisher。js 不可为 nil。
func NewPublisher(js JetStreamPublisher, cfg PublisherConfig) *Publisher {
	if strings.TrimSpace(cfg.StreamID) == "" {
		cfg.StreamID = "BINANCE_MARKET"
	}
	return &Publisher{js: js, now: time.Now, streamID: cfg.StreamID}
}

// Publish 把 req 经 JetStream 发布。
//
//   - PubAck 成功 → PublishAck（JetStream 服务器侧已持久化）；
//   - 可重试错误 → PublishReject{Retryable: true}；
//   - 不可重试错误（校验失败）→ PublishReject{Retryable: false}。
func (p *Publisher) Publish(ctx context.Context, req PublishRequest) PublishResult {
	if p == nil || p.js == nil {
		return PublishResult{Reject: &PublishReject{Code: "NOT_INITIALIZED", Reason: "jetstream publisher not initialized", Retryable: false}}
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return PublishResult{Reject: &PublishReject{Code: "INVALID_REQUEST", Reason: "idempotency key empty", Retryable: false}}
	}
	if strings.TrimSpace(req.Subject) == "" {
		return PublishResult{Reject: &PublishReject{Code: "INVALID_REQUEST", Reason: "subject empty", Retryable: false}}
	}

	ack, err := p.js.Publish(
		natsx.NewEnvelope(req.Subject, req.Payload),
		nats.MsgId(req.IdempotencyKey),
	)
	if err != nil {
		return PublishResult{Reject: &PublishReject{
			Code:      "JETSTREAM_PUBLISH_FAILED",
			Reason:    err.Error(),
			Retryable: isRetryable(err),
		}}
	}

	duplicate := false
	stream := p.streamID
	sequence := uint64(0)
	if ack != nil {
		duplicate = ack.Duplicate
		if ack.Stream != "" {
			stream = ack.Stream
		}
		sequence = ack.Sequence
	}
	return PublishResult{Ack: &PublishAck{
		Stream:    stream,
		Sequence:  sequence,
		Duplicate: duplicate,
	}}
}

// isRetryable 判断 JetStream 错误是否值得重试。
func isRetryable(err error) bool {
	var nerr *natsx.Error
	if errors.As(err, &nerr) {
		return nerr.Retryable
	}
	return true
}

// FetchMessage 是 IngestConsumer.Fetch 返回的单条消息。
type FetchMessage struct {
	// Payload 是消息字节。
	Payload []byte
	// Subject 是消息 subject。
	Subject string
	// Headers 是消息头（含 Nats-Msg-Id 等）。
	Headers map[string][]string
	// Ack 显式确认，调用后 offset 推进。未调用且超 AckWait 则 JetStream 重投递。
	Ack func() error
	// Nak 显式否定确认，触发立即重投递。
	Nak func() error
	// NakWithDelay 否定确认并指定重投递延迟。delay 为 0 时等同于 Nak()。
	NakWithDelay func(delay time.Duration) error
	// Term 终止消息（不重投递），用于 poison message。
	Term func() error
}

// FetchError 是消费侧可审计错误。
type FetchError struct {
	Subject    string
	Stream     string
	Consumer   string
	Deliveries int
	Err        error
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("ingest: fetch error (subject=%s stream=%s consumer=%s deliveries=%d): %v", e.Subject, e.Stream, e.Consumer, e.Deliveries, e.Err)
}

func (e *FetchError) Unwrap() error { return e.Err }

// JetStreamPullSubscriber 是 FR-010 依赖的 pull-subscribe 契约。
type JetStreamPullSubscriber interface {
	// PullSubscribe 在给定 stream/subject 上创建 durable consumer，返回 Subscription。
	PullSubscribe(stream, subject, durable string) (PullSubscription, error)
}

// PullSubscription 是 pull 消费句柄。
type PullSubscription interface {
	Fetch(max int, opts ...nats.PullOpt) ([]*nats.Msg, error)
	Close() error
}

// Consumer 实现 FR-010 IngestConsumer 域适配器。
type Consumer struct {
	sub          JetStreamPullSubscriber
	stream       string
	subject      string
	durable      string
	maxWait      time.Duration
	maxDeliver   int
	onDeadLetter func(FetchMessage) error
}

// ConsumerConfig 是 Consumer 配置。
type ConsumerConfig struct {
	Stream  string
	Subject string
	Durable string
	MaxWait time.Duration

	// MaxDeliver mirrors the JetStream consumer MaxDeliver setting. When it is
	// positive and a fetched message has reached that delivery count, Term calls
	// OnDeadLetter before terminating the JetStream message.
	MaxDeliver int
	// OnDeadLetter is invoked by FetchMessage.Term for messages whose JetStream
	// delivery count is greater than or equal to MaxDeliver. The hook lets callers
	// persist/republish poison messages before Term prevents another delivery.
	OnDeadLetter func(FetchMessage) error
}

// NewConsumer 构造 Consumer。
func NewConsumer(sub JetStreamPullSubscriber, cfg ConsumerConfig) (*Consumer, error) {
	if strings.TrimSpace(cfg.Stream) == "" || strings.TrimSpace(cfg.Subject) == "" || strings.TrimSpace(cfg.Durable) == "" {
		return nil, fmt.Errorf("ingest: stream/subject/durable required")
	}
	if cfg.MaxWait == 0 {
		cfg.MaxWait = 5 * time.Second
	}
	if cfg.MaxDeliver < 0 {
		return nil, fmt.Errorf("ingest: max deliver must be non-negative")
	}
	return &Consumer{
		sub:          sub,
		stream:       cfg.Stream,
		subject:      cfg.Subject,
		durable:      cfg.Durable,
		maxWait:      cfg.MaxWait,
		maxDeliver:   cfg.MaxDeliver,
		onDeadLetter: cfg.OnDeadLetter,
	}, nil
}

// Fetch 拉取最多 max 条消息，返回 FetchMessage 列表。
// 调用方显式 Ack/Nak/Term；未 Ack 且超 AckWait 则 JetStream 重投递（at-least-once）。
func (c *Consumer) Fetch(ctx context.Context, max int) ([]FetchMessage, error) {
	if c == nil || c.sub == nil {
		return nil, errors.New("ingest: consumer not initialized")
	}
	sub, err := c.sub.PullSubscribe(c.stream, c.subject, c.durable)
	if err != nil {
		return nil, &FetchError{Stream: c.stream, Subject: c.subject, Consumer: c.durable, Err: err}
	}
	defer sub.Close()

	msgs, err := sub.Fetch(max, nats.MaxWait(c.maxWait))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil // 无消息，非错误
		}
		return nil, &FetchError{Stream: c.stream, Subject: c.subject, Consumer: c.durable, Err: err}
	}

	out := make([]FetchMessage, 0, len(msgs))
	for _, msg := range msgs {
		msg := msg // capture
		fetched := FetchMessage{
			Payload:      append([]byte(nil), msg.Data...),
			Subject:      msg.Subject,
			Headers:      fromNatsHeader(msg.Header),
			Ack:          func() error { return msg.Ack() },
			Nak:          func() error { return msg.Nak() },
			NakWithDelay: func(d time.Duration) error { return msg.NakWithDelay(d) },
			Term:         func() error { return msg.Term() },
		}
		if c.shouldDeadLetter(msg) {
			baseTerm := fetched.Term
			hookMessage := fetched
			hookMessage.Term = baseTerm
			fetched.Term = func() error {
				if err := c.onDeadLetter(hookMessage); err != nil {
					return err
				}
				return baseTerm()
			}
		}
		out = append(out, fetched)
	}
	return out, nil
}

func (c *Consumer) shouldDeadLetter(msg *nats.Msg) bool {
	if c.onDeadLetter == nil || c.maxDeliver <= 0 || msg == nil {
		return false
	}
	deliveries, ok := jetStreamDeliveries(msg)
	return ok && deliveries >= uint64(c.maxDeliver)
}

func jetStreamDeliveries(msg *nats.Msg) (uint64, bool) {
	metadata, err := msg.Metadata()
	if err == nil {
		return metadata.NumDelivered, true
	}
	// nats.Msg.Metadata requires a live subscription/connection. Keep the hook
	// unit-testable by parsing the documented v1 ACK reply shape:
	// $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.
	parts := strings.Split(msg.Reply, ".")
	if len(parts) == 9 && parts[0] == "$JS" && parts[1] == "ACK" {
		var deliveries uint64
		for _, ch := range parts[4] {
			if ch < '0' || ch > '9' {
				return 0, false
			}
			deliveries = deliveries*10 + uint64(ch-'0')
		}
		return deliveries, true
	}
	return 0, false
}

// fromNatsHeader 把 nats.Header 转为 map[string][]string。
func fromNatsHeader(h nats.Header) map[string][]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, v := range h {
		out[k] = append([]string(nil), v...)
	}
	return out
}
