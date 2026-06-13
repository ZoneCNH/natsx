package natsx

import "context"

const (
	LogClientConnected    = "natsx.connected"
	LogClientDisconnected = "natsx.disconnected"
	LogClientReconnected  = "natsx.reconnected"
	LogPublish            = "natsx.publish"
	LogRequest            = "natsx.request"
	LogConsume            = "natsx.consume"
)

type LogEvent struct {
	Name          string
	Operation     string
	ClientName    string
	Subject       string
	Queue         string
	Status        string
	ErrorKind     ErrorKind
	EventID       string
	MessageID     string
	SchemaVersion string
	TraceID       string
}

type Logger interface {
	LogNATSEvent(ctx context.Context, event LogEvent)
}

type NoopLogger struct{}

func (NoopLogger) LogNATSEvent(context.Context, LogEvent) {}

func logConnectionEvent(ctx context.Context, logger Logger, name, operation, clientName string, err error) {
	if logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	event := LogEvent{
		Name:       name,
		Operation:  operation,
		ClientName: clientName,
		Status:     "ok",
	}
	if err != nil {
		event.Status = "error"
		event.ErrorKind = errorKind(err)
	}
	logger.LogNATSEvent(ctx, event)
}

func (c *Client) logNATSEvent(ctx context.Context, event LogEvent) {
	if c == nil || c.logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if event.ClientName == "" {
		event.ClientName = c.cfg.withDefaults().Name
	}
	c.logger.LogNATSEvent(ctx, event)
}

func (c *Client) logEnvelopeEvent(ctx context.Context, name, operation, subject, queue string, env Envelope, err error) {
	event := LogEvent{
		Name:          name,
		Operation:     operation,
		Subject:       subject,
		Queue:         queue,
		Status:        "ok",
		EventID:       env.EventID,
		MessageID:     env.MessageID,
		SchemaVersion: env.SchemaVersion,
		TraceID:       env.TraceID,
	}
	if err != nil {
		event.Status = "error"
		event.ErrorKind = errorKind(err)
	}
	c.logNATSEvent(ctx, event)
}
