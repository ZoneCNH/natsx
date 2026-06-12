package natsx

import "testing"

func TestEnvelopeRoundTripCopiesDataAndHeaders(t *testing.T) {
	original := NewEnvelope("events.created", []byte("payload"))
	original.Reply = "reply.inbox"
	original.Headers = map[string][]string{"X-Test": {"a", "b"}}

	msg := original.ToMsg()
	msg.Data[0] = 'P'
	msg.Header.Add("X-Test", "c")

	if string(original.Data) != "payload" {
		t.Fatalf("ToMsg mutated original data: %q", original.Data)
	}
	if got := len(original.Headers["X-Test"]); got != 2 {
		t.Fatalf("ToMsg mutated original headers len = %d", got)
	}

	roundTrip := EnvelopeFromMsg(msg)
	msg.Data[0] = 'x'
	msg.Header.Add("X-Test", "d")
	if string(roundTrip.Data) != "Payload" {
		t.Fatalf("EnvelopeFromMsg data = %q, want Payload", roundTrip.Data)
	}
	if got := len(roundTrip.Headers["X-Test"]); got != 3 {
		t.Fatalf("EnvelopeFromMsg headers len = %d, want 3", got)
	}
}
