package natsx

import (
	"strings"

	"github.com/nats-io/nats.go"
)

const (
	HeaderEventID       = "eventId"
	HeaderMessageID     = "messageId"
	HeaderSchemaVersion = "schemaVersion"
	HeaderTraceID       = "traceId"
)

type Envelope struct {
	Subject       string
	Reply         string
	EventID       string
	MessageID     string
	SchemaVersion string
	TraceID       string
	Headers       map[string][]string
	Data          []byte
}

func NewEnvelope(subject string, data []byte) Envelope {
	return Envelope{Subject: subject, Data: append([]byte(nil), data...)}
}
func (e Envelope) ToMsg() *nats.Msg {
	header := toHeader(e.Headers)
	setHeader(header, HeaderEventID, e.EventID)
	setHeader(header, HeaderMessageID, e.MessageID)
	setHeader(header, HeaderSchemaVersion, e.SchemaVersion)
	setHeader(header, HeaderTraceID, e.TraceID)
	return &nats.Msg{Subject: e.Subject, Reply: e.Reply, Header: header, Data: append([]byte(nil), e.Data...)}
}
func EnvelopeFromMsg(msg *nats.Msg) Envelope {
	if msg == nil {
		return Envelope{}
	}
	return Envelope{
		Subject:       msg.Subject,
		Reply:         msg.Reply,
		EventID:       firstHeader(msg.Header, HeaderEventID),
		MessageID:     firstHeader(msg.Header, HeaderMessageID),
		SchemaVersion: firstHeader(msg.Header, HeaderSchemaVersion),
		TraceID:       firstHeader(msg.Header, HeaderTraceID),
		Headers:       fromHeader(msg.Header),
		Data:          append([]byte(nil), msg.Data...),
	}
}

func setHeader(header nats.Header, key, value string) {
	if value != "" {
		header.Set(key, value)
	}
}

func firstHeader(header nats.Header, key string) string {
	if value := header.Get(key); value != "" {
		return value
	}
	for candidate, values := range header {
		if strings.EqualFold(candidate, key) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func toHeader(in map[string][]string) nats.Header {
	h := nats.Header{}
	for k, vals := range in {
		for _, v := range vals {
			h.Add(k, v)
		}
	}
	return h
}
func fromHeader(in nats.Header) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, vals := range in {
		out[k] = append([]string(nil), vals...)
	}
	return out
}
