package natsx

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type Handler func(context.Context, Envelope) (Envelope, error)

func (c *Client) Publish(ctx context.Context, env Envelope) error {
	const op = "natsx.Publish"
	start := time.Now()
	metrics := clientMetrics(c)
	if err := c.ready(op, ctx); err != nil {
		recordCoreCounter(metrics, MetricPublishTotal, "publish", env.Subject, "error", err)
		recordCoreDuration(metrics, MetricPublishDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
		c.logEnvelopeEvent(ctx, LogPublish, "publish", env.Subject, "", env, err)
		return err
	}
	if err := ValidateSubject(op, env.Subject); err != nil {
		recordCoreCounter(metrics, MetricPublishTotal, "publish", env.Subject, "error", err)
		recordCoreDuration(metrics, MetricPublishDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
		c.logEnvelopeEvent(ctx, LogPublish, "publish", env.Subject, "", env, err)
		return err
	}
	if err := c.conn.PublishMsg(env.ToMsg()); err != nil {
		wrapped := connectionError(op, err)
		recordErrorMetric(metrics, "publish", wrapped)
		recordCoreCounter(metrics, MetricPublishTotal, "publish", env.Subject, "error", wrapped)
		recordCoreDuration(metrics, MetricPublishDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
		c.logEnvelopeEvent(ctx, LogPublish, "publish", env.Subject, "", env, wrapped)
		return wrapped
	}
	if err := c.conn.FlushWithContext(ctx); err != nil {
		wrapped := contextError(op, err)
		recordErrorMetric(metrics, "publish", wrapped)
		recordCoreCounter(metrics, MetricPublishTotal, "publish", env.Subject, "error", wrapped)
		recordCoreDuration(metrics, MetricPublishDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
		c.logEnvelopeEvent(ctx, LogPublish, "publish", env.Subject, "", env, wrapped)
		return wrapped
	}
	recordCoreCounter(metrics, MetricPublishTotal, "publish", env.Subject, "ok", nil)
	recordCoreDuration(metrics, MetricPublishDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "ok"})
	metrics.IncCounter(MetricCoreMessagesTotal, map[string]string{"op": "publish", "subject": env.Subject})
	c.logEnvelopeEvent(ctx, LogPublish, "publish", env.Subject, "", env, nil)
	return nil
}

func (c *Client) Request(ctx context.Context, env Envelope) (Envelope, error) {
	const op = "natsx.Request"
	start := time.Now()
	metrics := clientMetrics(c)
	if err := c.ready(op, ctx); err != nil {
		recordCoreCounter(metrics, MetricRequestTotal, "request", env.Subject, "error", err)
		recordCoreDuration(metrics, MetricRequestDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
		c.logEnvelopeEvent(ctx, LogRequest, "request", env.Subject, "", env, err)
		return Envelope{}, err
	}
	if err := ValidateSubject(op, env.Subject); err != nil {
		recordCoreCounter(metrics, MetricRequestTotal, "request", env.Subject, "error", err)
		recordCoreDuration(metrics, MetricRequestDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
		c.logEnvelopeEvent(ctx, LogRequest, "request", env.Subject, "", env, err)
		return Envelope{}, err
	}
	msg, err := c.conn.RequestMsgWithContext(ctx, env.ToMsg())
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			wrapped := contextError(op, ctxErr)
			recordErrorMetric(metrics, "request", wrapped)
			recordCoreCounter(metrics, MetricRequestTotal, "request", env.Subject, "error", wrapped)
			recordCoreDuration(metrics, MetricRequestDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
			c.logEnvelopeEvent(ctx, LogRequest, "request", env.Subject, "", env, wrapped)
			return Envelope{}, wrapped
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			wrapped := contextError(op, err)
			recordErrorMetric(metrics, "request", wrapped)
			recordCoreCounter(metrics, MetricRequestTotal, "request", env.Subject, "error", wrapped)
			recordCoreDuration(metrics, MetricRequestDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
			c.logEnvelopeEvent(ctx, LogRequest, "request", env.Subject, "", env, wrapped)
			return Envelope{}, wrapped
		}
		wrapped := connectionError(op, err)
		recordErrorMetric(metrics, "request", wrapped)
		recordCoreCounter(metrics, MetricRequestTotal, "request", env.Subject, "error", wrapped)
		recordCoreDuration(metrics, MetricRequestDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "error"})
		c.logEnvelopeEvent(ctx, LogRequest, "request", env.Subject, "", env, wrapped)
		return Envelope{}, wrapped
	}
	recordCoreCounter(metrics, MetricRequestTotal, "request", env.Subject, "ok", nil)
	metrics.IncCounter(MetricCoreMessagesTotal, map[string]string{"op": "request", "subject": env.Subject})
	recordCoreDuration(metrics, MetricRequestDurationMS, time.Since(start), map[string]string{"subject": env.Subject, "status": "ok"})
	c.logEnvelopeEvent(ctx, LogRequest, "request", env.Subject, "", env, nil)
	return EnvelopeFromMsg(msg), nil
}

func (c *Client) Subscribe(subject string, handler Handler) (*nats.Subscription, error) {
	return c.subscribe("natsx.Subscribe", subject, "", handler)
}

func (c *Client) QueueSubscribe(subject, queue string, handler Handler) (*nats.Subscription, error) {
	const op = "natsx.QueueSubscribe"
	if strings.TrimSpace(queue) == "" {
		return nil, validationError(op, "queue is required", nil)
	}
	return c.subscribe(op, subject, queue, handler)
}

func (c *Client) subscribe(op, subject, queue string, handler Handler) (*nats.Subscription, error) {
	if c == nil || c.conn == nil {
		return nil, validationError(op, "client is not connected", nil)
	}
	if err := ValidateSubject(op, subject); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, validationError(op, "handler is required", nil)
	}
	callback := func(msg *nats.Msg) {
		start := time.Now()
		incoming := EnvelopeFromMsg(msg)
		reply, err := handler(context.Background(), incoming)
		status := "ok"
		if err != nil {
			status = "error"
		}
		recordCoreDuration(c.metrics, MetricConsumeDurationMS, time.Since(start), map[string]string{"subject": subject, "queue": queue, "status": status})
		if err != nil {
			wrapped := WrapError(ErrorKindInternal, op, "handler returned error", false, err)
			recordErrorMetric(c.metrics, "subscribe", wrapped)
			recordCoreCounter(c.metrics, MetricConsumeTotal, "consume", subject, "error", wrapped)
			c.logEnvelopeEvent(context.Background(), LogConsume, "consume", subject, queue, incoming, wrapped)
			return
		}
		if msg.Reply != "" {
			replyMsg := reply.ToMsg()
			replyMsg.Subject = msg.Reply
			if err := msg.RespondMsg(replyMsg); err != nil {
				wrapped := connectionError(op, err)
				recordErrorMetric(c.metrics, "subscribe", wrapped)
				recordCoreCounter(c.metrics, MetricConsumeTotal, "consume", subject, "error", wrapped)
				c.logEnvelopeEvent(context.Background(), LogConsume, "consume", subject, queue, incoming, wrapped)
				return
			}
		}
		recordCoreCounter(c.metrics, MetricConsumeTotal, "consume", subject, "ok", nil)
		c.metrics.IncCounter(MetricCoreMessagesTotal, map[string]string{"op": "subscribe", "subject": subject})
		c.logEnvelopeEvent(context.Background(), LogConsume, "consume", subject, queue, incoming, nil)
	}
	var (
		sub *nats.Subscription
		err error
	)
	if queue == "" {
		sub, err = c.conn.Subscribe(subject, callback)
	} else {
		sub, err = c.conn.QueueSubscribe(subject, queue, callback)
	}
	if err != nil {
		wrapped := connectionError(op, err)
		recordErrorMetric(c.metrics, "subscribe", wrapped)
		return nil, wrapped
	}
	return sub, nil
}

func recordCoreCounter(metrics Metrics, name, op, subject, status string, err error) {
	if metrics == nil {
		return
	}
	labels := map[string]string{"op": op, "subject": subject, "status": status}
	if err != nil {
		labels["kind"] = string(errorKind(err))
	}
	metrics.IncCounter(name, labels)
}

func recordCoreDuration(metrics Metrics, name string, elapsed time.Duration, labels map[string]string) {
	if metrics == nil {
		return
	}
	metrics.ObserveHistogram(name, durationMS(elapsed), labels)
}

func clientMetrics(c *Client) Metrics {
	if c == nil || c.metrics == nil {
		return NoopMetrics{}
	}
	return c.metrics
}

func durationMS(elapsed time.Duration) float64 {
	return float64(elapsed) / float64(time.Millisecond)
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
