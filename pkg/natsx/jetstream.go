package natsx

import "github.com/nats-io/nats.go"

type StreamConfig = nats.StreamConfig
type ConsumerConfig = nats.ConsumerConfig
type PubAck = nats.PubAck

type JetStreamClient struct {
	client *Client
	js     nats.JetStreamContext
}

func (c *Client) JetStreamClient() (*JetStreamClient, error) {
	js, err := c.JetStream()
	if err != nil {
		return nil, err
	}
	return &JetStreamClient{client: c, js: js}, nil
}
func (j *JetStreamClient) AddStream(cfg *StreamConfig) (*nats.StreamInfo, error) {
	if j == nil || j.js == nil {
		return nil, validationError("natsx.AddStream", "jetstream is not initialized", nil)
	}
	return j.js.AddStream(cfg)
}
func (j *JetStreamClient) DeleteStream(name string) error {
	if j == nil || j.js == nil {
		return validationError("natsx.DeleteStream", "jetstream is not initialized", nil)
	}
	return j.js.DeleteStream(name)
}
func (j *JetStreamClient) StreamInfo(name string) (*nats.StreamInfo, error) {
	if j == nil || j.js == nil {
		return nil, validationError("natsx.StreamInfo", "jetstream is not initialized", nil)
	}
	return j.js.StreamInfo(name)
}
func (j *JetStreamClient) AddConsumer(stream string, cfg *ConsumerConfig) (*nats.ConsumerInfo, error) {
	if j == nil || j.js == nil {
		return nil, validationError("natsx.AddConsumer", "jetstream is not initialized", nil)
	}
	return j.js.AddConsumer(stream, cfg)
}
func (j *JetStreamClient) Publish(env Envelope, opts ...nats.PubOpt) (*PubAck, error) {
	if j == nil || j.js == nil {
		return nil, validationError("natsx.JetStreamPublish", "jetstream is not initialized", nil)
	}
	if err := ValidateSubject("natsx.JetStreamPublish", env.Subject); err != nil {
		return nil, err
	}
	ack, err := j.js.PublishMsg(env.ToMsg(), opts...)
	if err != nil {
		return nil, connectionError("natsx.JetStreamPublish", err)
	}
	if j.client != nil {
		j.client.metrics.IncCounter(MetricJetStreamMessagesTotal, map[string]string{"op": "publish", "subject": env.Subject})
	}
	return ack, nil
}
func (j *JetStreamClient) PullSubscribe(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	if j == nil || j.js == nil {
		return nil, validationError("natsx.PullSubscribe", "jetstream is not initialized", nil)
	}
	if err := ValidateSubject("natsx.PullSubscribe", subject); err != nil {
		return nil, err
	}
	return j.js.PullSubscribe(subject, durable, opts...)
}
