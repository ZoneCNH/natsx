package natsx

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

func (c *Client) Publish(ctx context.Context, env Envelope) error {
	const op = "natsx.Publish"
	if err := c.ready(op, ctx); err != nil {
		return err
	}
	if err := ValidateSubject(op, env.Subject); err != nil {
		return err
	}
	if err := c.conn.PublishMsg(env.ToMsg()); err != nil {
		wrapped := connectionError(op, err)
		recordErrorMetric(c.metrics, "publish", wrapped)
		return wrapped
	}
	if err := c.conn.FlushWithContext(ctx); err != nil {
		wrapped := contextError(op, err)
		recordErrorMetric(c.metrics, "publish", wrapped)
		return wrapped
	}
	c.metrics.IncCounter(MetricCoreMessagesTotal, map[string]string{"op": "publish", "subject": env.Subject})
	return nil
}

func (c *Client) Request(ctx context.Context, env Envelope) (Envelope, error) {
	const op = "natsx.Request"
	start := time.Now()
	if err := c.ready(op, ctx); err != nil {
		return Envelope{}, err
	}
	if err := ValidateSubject(op, env.Subject); err != nil {
		return Envelope{}, err
	}
	msg, err := c.conn.RequestMsgWithContext(ctx, env.ToMsg())
	if err != nil {
		wrapped := connectionError(op, err)
		recordErrorMetric(c.metrics, "request", wrapped)
		return Envelope{}, wrapped
	}
	c.metrics.IncCounter(MetricCoreMessagesTotal, map[string]string{"op": "request", "subject": env.Subject})
	c.metrics.ObserveHistogram(MetricCoreRequestDurationSeconds, time.Since(start).Seconds(), map[string]string{"subject": env.Subject})
	return EnvelopeFromMsg(msg), nil
}

func (c *Client) Subscribe(subject string, handler func(context.Context, Envelope) (Envelope, error)) (*nats.Subscription, error) {
	const op = "natsx.Subscribe"
	if c == nil || c.conn == nil {
		return nil, validationError(op, "client is not connected", nil)
	}
	if err := ValidateSubject(op, subject); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, validationError(op, "handler is required", nil)
	}
	sub, err := c.conn.Subscribe(subject, func(msg *nats.Msg) {
		reply, err := handler(context.Background(), EnvelopeFromMsg(msg))
		if msg.Reply != "" && err == nil {
			_ = msg.RespondMsg(reply.ToMsg())
		}
		c.metrics.IncCounter(MetricCoreMessagesTotal, map[string]string{"op": "subscribe", "subject": subject})
	})
	if err != nil {
		wrapped := connectionError(op, err)
		recordErrorMetric(c.metrics, "subscribe", wrapped)
		return nil, wrapped
	}
	return sub, nil
}

func (c *Client) ready(op string, ctx context.Context) error {
	if c == nil || c.conn == nil {
		return validationError(op, "client is not connected", nil)
	}
	if ctx == nil {
		return validationError(op, "context is required", nil)
	}
	if err := ctx.Err(); err != nil {
		return contextError(op, err)
	}
	if !c.conn.IsConnected() {
		return WrapError(ErrorKindUnavailable, op, "NATS connection is not connected", true, nil)
	}
	return nil
}
