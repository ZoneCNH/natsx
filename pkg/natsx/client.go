package natsx

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

type Client struct {
	cfg     Config
	metrics Metrics
	conn    *nats.Conn
	js      nats.JetStreamContext
}

func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	const op = "natsx.New"
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}
	if ctx == nil {
		err := validationError(op, "context is required", nil)
		recordErrorMetric(options.metrics, "new", err)
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		wrapped := contextError(op, err)
		recordErrorMetric(options.metrics, "new", wrapped)
		return nil, wrapped
	}
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		recordErrorMetric(options.metrics, "new", err)
		return nil, err
	}

	nopts := []nats.Option{nats.Name(cfg.Name), nats.Timeout(cfg.Timeout), nats.MaxReconnects(cfg.MaxReconnects), nats.ReconnectWait(cfg.ReconnectWait), nats.DrainTimeout(cfg.DrainTimeout), nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
		options.metrics.IncCounter(MetricConnectionDisconnectsTotal, map[string]string{"name": cfg.Name})
	}), nats.ReconnectHandler(func(_ *nats.Conn) {
		options.metrics.IncCounter(MetricConnectionReconnectsTotal, map[string]string{"name": cfg.Name})
	})}
	if cfg.Token != "" {
		nopts = append(nopts, nats.Token(cfg.Token))
	}
	if cfg.Username != "" || cfg.Password != "" {
		nopts = append(nopts, nats.UserInfo(cfg.Username, cfg.Password))
	}
	if cfg.CredentialsFile != "" {
		nopts = append(nopts, nats.UserCredentials(cfg.CredentialsFile))
	}
	if cfg.NKeySeed != "" {
		nkeyOpt, err := nats.NkeyOptionFromSeed(cfg.NKeySeed)
		if err != nil {
			wrapped := validationError(op, "invalid nkey seed", err)
			recordErrorMetric(options.metrics, "new", wrapped)
			return nil, wrapped
		}
		nopts = append(nopts, nkeyOpt)
	}
	if cfg.TLS {
		nopts = append(nopts, nats.Secure(cfg.BuildTLSConfig()))
	}
	nopts = append(nopts, options.natsOptions...)

	conn, err := nats.Connect(stringsJoin(cfg.endpoints()), nopts...)
	if err != nil {
		wrapped := connectionError(op, err)
		recordErrorMetric(options.metrics, "new", wrapped)
		return nil, wrapped
	}
	client := &Client{cfg: cfg, metrics: options.metrics, conn: conn}
	if cfg.EnableJetStream {
		js, err := conn.JetStream()
		if err != nil {
			conn.Close()
			wrapped := WrapError(ErrorKindUnavailable, op, "jetstream is unavailable", true, err)
			recordErrorMetric(options.metrics, "new", wrapped)
			return nil, wrapped
		}
		client.js = js
	}
	options.metrics.IncCounter(MetricClientCreatedTotal, map[string]string{"name": cfg.Name})
	return client, nil
}

func Wrap(conn *nats.Conn, opts ...Option) (*Client, error) {
	if conn == nil {
		return nil, validationError("natsx.Wrap", "connection is required", nil)
	}
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}
	js, _ := conn.JetStream()
	return &Client{cfg: Config{Name: "natsx"}.withDefaults(), metrics: options.metrics, conn: conn, js: js}, nil
}

func (c *Client) Conn() *nats.Conn {
	if c == nil {
		return nil
	}
	return c.conn
}
func (c *Client) JetStream() (nats.JetStreamContext, error) {
	if c == nil || c.conn == nil {
		return nil, validationError("natsx.JetStream", "client is not connected", nil)
	}
	if c.js != nil {
		return c.js, nil
	}
	js, err := c.conn.JetStream()
	if err != nil {
		return nil, WrapError(ErrorKindUnavailable, "natsx.JetStream", "jetstream is unavailable", true, err)
	}
	c.js = js
	return js, nil
}
func (c *Client) Close(ctx context.Context) error {
	const op = "natsx.Close"
	if c == nil || c.conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return contextError(op, err)
	}
	if c.conn.IsClosed() {
		return nil
	}
	if err := c.conn.Drain(); err != nil {
		if errors.Is(err, nats.ErrConnectionClosed) {
			return nil
		}
		if errors.Is(err, nats.ErrConnectionReconnecting) {
			c.conn.Close()
		}
		wrapped := connectionError(op, err)
		recordErrorMetric(c.metrics, "close", wrapped)
		return wrapped
	}

	timeout := c.cfg.withDefaults().DrainTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if c.conn.IsClosed() {
			c.metrics.IncCounter(MetricClientClosedTotal, map[string]string{"name": c.cfg.Name})
			return nil
		}
		select {
		case <-ctx.Done():
			c.conn.Close()
			err := contextError(op, ctx.Err())
			recordErrorMetric(c.metrics, "close", err)
			return err
		case <-timer.C:
			c.conn.Close()
			err := WrapError(ErrorKindTimeout, op, "drain timeout exceeded", true, nil)
			recordErrorMetric(c.metrics, "close", err)
			return err
		case <-ticker.C:
		}
	}
}

func stringsJoin(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}
func recordErrorMetric(metrics Metrics, op string, err error) {
	if metrics != nil {
		metrics.IncCounter(MetricClientErrorsTotal, map[string]string{"op": op, "kind": string(errorKind(err))})
	}
}
