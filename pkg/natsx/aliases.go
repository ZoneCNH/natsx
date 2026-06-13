package natsx

// Compatibility aliases preserve the historical/public names expected by
// downstream FoundationX integrations while sharing the existing implementation.
type NatsPubSubClient = Client
type NatsRequestClient = Client
type JetStreamClientX = JetStreamClient
type NatsMessageEnvelope = Envelope
