package natsx

import "testing"

func TestEnvelopeRoundTripCopiesDataAndHeaders(t *testing.T) {
	original := NewEnvelope("events.created", []byte("payload"))
	original.Reply = "reply.inbox"
	original.EventID = "event-1"
	original.MessageID = "message-1"
	original.SchemaVersion = "schema-v1"
	original.TraceID = "trace-1"
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
	if got := msg.Header.Get(HeaderEventID); got != "event-1" {
		t.Fatalf("ToMsg %s header = %q, want event-1", HeaderEventID, got)
	}
	if got := msg.Header.Get(HeaderMessageID); got != "message-1" {
		t.Fatalf("ToMsg %s header = %q, want message-1", HeaderMessageID, got)
	}
	if got := msg.Header.Get(HeaderSchemaVersion); got != "schema-v1" {
		t.Fatalf("ToMsg %s header = %q, want schema-v1", HeaderSchemaVersion, got)
	}
	if got := msg.Header.Get(HeaderTraceID); got != "trace-1" {
		t.Fatalf("ToMsg %s header = %q, want trace-1", HeaderTraceID, got)
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
	if roundTrip.EventID != original.EventID {
		t.Fatalf("EnvelopeFromMsg EventID = %q, want %q", roundTrip.EventID, original.EventID)
	}
	if roundTrip.MessageID != original.MessageID {
		t.Fatalf("EnvelopeFromMsg MessageID = %q, want %q", roundTrip.MessageID, original.MessageID)
	}
	if roundTrip.SchemaVersion != original.SchemaVersion {
		t.Fatalf("EnvelopeFromMsg SchemaVersion = %q, want %q", roundTrip.SchemaVersion, original.SchemaVersion)
	}
	if roundTrip.TraceID != original.TraceID {
		t.Fatalf("EnvelopeFromMsg TraceID = %q, want %q", roundTrip.TraceID, original.TraceID)
	}
}
