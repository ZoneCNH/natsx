package natsx

import (
	"context"
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

	nopts := []nats.Option{nats.Name(cfg.Name), nats.Timeout(cfg.Timeout), nats.MaxReconnects(cfg.MaxReconnects), nats.ReconnectWait(cfg.ReconnectWait), nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
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
	if c == nil || c.conn == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return contextError("natsx.Close", ctx.Err())
	}
	done := make(chan struct{})
	go func() { _ = c.conn.Drain(); close(done) }()
	timeout := c.cfg.withDefaults().DrainTimeout
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		c.metrics.IncCounter(MetricClientClosedTotal, map[string]string{"name": c.cfg.Name})
		return nil
	case <-ctx.Done():
		c.conn.Close()
		err := contextError("natsx.Close", ctx.Err())
		recordErrorMetric(c.metrics, "close", err)
		return err
	case <-timer.C:
		c.conn.Close()
		err := WrapError(ErrorKindTimeout, "natsx.Close", "drain timeout exceeded", true, nil)
		recordErrorMetric(c.metrics, "close", err)
		return err
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
