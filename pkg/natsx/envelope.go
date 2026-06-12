package natsx

import "github.com/nats-io/nats.go"

type Envelope struct {
	Subject string
	Reply   string
	Headers map[string][]string
	Data    []byte
}

func NewEnvelope(subject string, data []byte) Envelope {
	return Envelope{Subject: subject, Data: append([]byte(nil), data...)}
}
func (e Envelope) ToMsg() *nats.Msg {
	return &nats.Msg{Subject: e.Subject, Reply: e.Reply, Header: toHeader(e.Headers), Data: append([]byte(nil), e.Data...)}
}
func EnvelopeFromMsg(msg *nats.Msg) Envelope {
	if msg == nil {
		return Envelope{}
	}
	return Envelope{Subject: msg.Subject, Reply: msg.Reply, Headers: fromHeader(msg.Header), Data: append([]byte(nil), msg.Data...)}
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
