package natsx

import "testing"

func TestPublicAPIAliases(t *testing.T) {
	var pubsub *NatsPubSubClient = (*Client)(nil)
	var requester *NatsRequestClient = (*Client)(nil)
	var jetstream *JetStreamClientX = (*JetStreamClient)(nil)
	var envelope NatsMessageEnvelope = Envelope{Subject: "orders.created.publish.v1"}
	if pubsub != nil || requester != nil || jetstream != nil {
		t.Fatalf("nil alias assignments changed pointer values")
	}
	if envelope.Subject != "orders.created.publish.v1" {
		t.Fatalf("alias envelope subject = %q", envelope.Subject)
	}
}
