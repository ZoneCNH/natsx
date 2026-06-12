package natsx

import (
	"errors"
	"reflect"
	"strings"

	"github.com/nats-io/nats.go"
)

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
	if cfg == nil {
		return nil, validationError("natsx.AddStream", "stream config is required", nil)
	}
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		return nil, validationError("natsx.AddStream", "stream name is required", nil)
	}
	stream, err := j.js.AddStream(cfg)
	if err == nil {
		return stream, nil
	}
	if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		existing, infoErr := j.js.StreamInfo(name)
		if infoErr == nil && existing != nil && streamConfigMatches(cfg, &existing.Config) {
			return existing, nil
		}
		return nil, conflictError("natsx.AddStream", err)
	}
	return nil, jetStreamError("natsx.AddStream", err)
}
func (j *JetStreamClient) DeleteStream(name string) error {
	if j == nil || j.js == nil {
		return validationError("natsx.DeleteStream", "jetstream is not initialized", nil)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return validationError("natsx.DeleteStream", "stream name is required", nil)
	}
	if err := j.js.DeleteStream(name); err != nil {
		return jetStreamError("natsx.DeleteStream", err)
	}
	return nil
}
func (j *JetStreamClient) StreamInfo(name string) (*nats.StreamInfo, error) {
	if j == nil || j.js == nil {
		return nil, validationError("natsx.StreamInfo", "jetstream is not initialized", nil)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, validationError("natsx.StreamInfo", "stream name is required", nil)
	}
	stream, err := j.js.StreamInfo(name)
	if err != nil {
		return nil, jetStreamError("natsx.StreamInfo", err)
	}
	return stream, nil
}
func (j *JetStreamClient) AddConsumer(stream string, cfg *ConsumerConfig) (*nats.ConsumerInfo, error) {
	if j == nil || j.js == nil {
		return nil, validationError("natsx.AddConsumer", "jetstream is not initialized", nil)
	}
	streamName := strings.TrimSpace(stream)
	if streamName == "" {
		return nil, validationError("natsx.AddConsumer", "stream name is required", nil)
	}
	if cfg == nil {
		return nil, validationError("natsx.AddConsumer", "consumer config is required", nil)
	}
	if name := consumerConfigName(cfg); name != "" {
		existing, infoErr := j.js.ConsumerInfo(streamName, name)
		if infoErr == nil && existing != nil {
			if consumerConfigMatches(cfg, &existing.Config) {
				return existing, nil
			}
			return nil, conflictError("natsx.AddConsumer", nats.ErrConsumerNameAlreadyInUse)
		}
		if infoErr != nil && !errors.Is(infoErr, nats.ErrConsumerNotFound) {
			return nil, jetStreamError("natsx.AddConsumer", infoErr)
		}
	}
	consumer, err := j.js.AddConsumer(streamName, cfg)
	if err == nil {
		return consumer, nil
	}
	if errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		if name := consumerConfigName(cfg); name != "" {
			existing, infoErr := j.js.ConsumerInfo(streamName, name)
			if infoErr == nil && existing != nil && consumerConfigMatches(cfg, &existing.Config) {
				return existing, nil
			}
		}
		return nil, conflictError("natsx.AddConsumer", err)
	}
	return nil, jetStreamError("natsx.AddConsumer", err)
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
		return nil, jetStreamError("natsx.JetStreamPublish", err)
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
	subject = strings.TrimSpace(subject)
	if err := ValidateSubject("natsx.PullSubscribe", subject); err != nil {
		return nil, err
	}
	if durable != "" {
		durable = strings.TrimSpace(durable)
		if durable == "" {
			return nil, validationError("natsx.PullSubscribe", "consumer durable is invalid", nil)
		}
	}
	sub, err := j.js.PullSubscribe(subject, durable, opts...)
	if err != nil {
		return nil, jetStreamError("natsx.PullSubscribe", err)
	}
	return sub, nil
}

func conflictError(op string, cause error) *Error {
	return WrapError(ErrorKindConflict, op, "", false, cause)
}

func jetStreamError(op string, cause error) error {
	if cause == nil {
		return nil
	}
	if errors.Is(cause, nats.ErrStreamNameAlreadyInUse) || errors.Is(cause, nats.ErrConsumerNameAlreadyInUse) {
		return conflictError(op, cause)
	}
	if errors.Is(cause, nats.ErrStreamNotFound) ||
		errors.Is(cause, nats.ErrConsumerNotFound) ||
		errors.Is(cause, nats.ErrNoStreamResponse) ||
		errors.Is(cause, nats.ErrNoMatchingStream) ||
		errors.Is(cause, nats.ErrJetStreamNotEnabled) ||
		errors.Is(cause, nats.ErrJetStreamNotEnabledForAccount) ||
		errors.Is(cause, nats.ErrConsumerDeleted) ||
		errors.Is(cause, nats.ErrSubjectMismatch) {
		return WrapError(ErrorKindUnavailable, op, "", true, cause)
	}
	return connectionError(op, cause)
}

func consumerConfigName(cfg *ConsumerConfig) string {
	if cfg == nil {
		return ""
	}
	if name := strings.TrimSpace(cfg.Name); name != "" {
		return name
	}
	return strings.TrimSpace(cfg.Durable)
}

func streamConfigMatches(requested, existing *StreamConfig) bool {
	if requested == nil || existing == nil {
		return false
	}
	if requested.Name != existing.Name {
		return false
	}
	if requested.Description != "" && requested.Description != existing.Description {
		return false
	}
	if len(requested.Subjects) > 0 && !reflect.DeepEqual(requested.Subjects, existing.Subjects) {
		return false
	}
	if requested.Retention != existing.Retention {
		return false
	}
	if requested.MaxConsumers != 0 && requested.MaxConsumers != existing.MaxConsumers {
		return false
	}
	if requested.MaxMsgs != 0 && requested.MaxMsgs != existing.MaxMsgs {
		return false
	}
	if requested.MaxBytes != 0 && requested.MaxBytes != existing.MaxBytes {
		return false
	}
	if requested.Discard != existing.Discard {
		return false
	}
	if requested.DiscardNewPerSubject != existing.DiscardNewPerSubject {
		return false
	}
	if requested.MaxAge != 0 && requested.MaxAge != existing.MaxAge {
		return false
	}
	if requested.MaxMsgsPerSubject != 0 && requested.MaxMsgsPerSubject != existing.MaxMsgsPerSubject {
		return false
	}
	if requested.MaxMsgSize != 0 && requested.MaxMsgSize != existing.MaxMsgSize {
		return false
	}
	if requested.Storage != existing.Storage {
		return false
	}
	if requested.Replicas != 0 && requested.Replicas != existing.Replicas {
		return false
	}
	if requested.NoAck != existing.NoAck {
		return false
	}
	if requested.Duplicates != 0 && requested.Duplicates != existing.Duplicates {
		return false
	}
	if requested.Placement != nil && !reflect.DeepEqual(requested.Placement, existing.Placement) {
		return false
	}
	if requested.Mirror != nil && !reflect.DeepEqual(requested.Mirror, existing.Mirror) {
		return false
	}
	if len(requested.Sources) > 0 && !reflect.DeepEqual(requested.Sources, existing.Sources) {
		return false
	}
	if requested.Sealed != existing.Sealed {
		return false
	}
	if requested.DenyDelete != existing.DenyDelete {
		return false
	}
	if requested.DenyPurge != existing.DenyPurge {
		return false
	}
	if requested.AllowRollup != existing.AllowRollup {
		return false
	}
	if requested.Compression != existing.Compression {
		return false
	}
	if requested.FirstSeq != 0 && requested.FirstSeq != existing.FirstSeq {
		return false
	}
	if requested.SubjectTransform != nil && !reflect.DeepEqual(requested.SubjectTransform, existing.SubjectTransform) {
		return false
	}
	if requested.RePublish != nil && !reflect.DeepEqual(requested.RePublish, existing.RePublish) {
		return false
	}
	if requested.AllowDirect != existing.AllowDirect {
		return false
	}
	if requested.MirrorDirect != existing.MirrorDirect {
		return false
	}
	if !reflect.DeepEqual(requested.ConsumerLimits, nats.StreamConsumerLimits{}) && !reflect.DeepEqual(requested.ConsumerLimits, existing.ConsumerLimits) {
		return false
	}
	if len(requested.Metadata) > 0 && !reflect.DeepEqual(requested.Metadata, existing.Metadata) {
		return false
	}
	if requested.AllowMsgTTL != existing.AllowMsgTTL {
		return false
	}
	if requested.SubjectDeleteMarkerTTL != 0 && requested.SubjectDeleteMarkerTTL != existing.SubjectDeleteMarkerTTL {
		return false
	}
	return true
}

func consumerConfigMatches(requested, existing *ConsumerConfig) bool {
	if requested == nil || existing == nil {
		return false
	}
	if requested.Durable != "" && requested.Durable != existing.Durable {
		return false
	}
	if requested.Name != "" && requested.Name != existing.Name {
		return false
	}
	if requested.Description != "" && requested.Description != existing.Description {
		return false
	}
	if requested.DeliverPolicy != existing.DeliverPolicy {
		return false
	}
	if requested.OptStartSeq != 0 && requested.OptStartSeq != existing.OptStartSeq {
		return false
	}
	if requested.OptStartTime != nil && !reflect.DeepEqual(requested.OptStartTime, existing.OptStartTime) {
		return false
	}
	if requested.AckPolicy != existing.AckPolicy {
		return false
	}
	if requested.AckWait != 0 && requested.AckWait != existing.AckWait {
		return false
	}
	if requested.MaxDeliver != 0 && requested.MaxDeliver != existing.MaxDeliver {
		return false
	}
	if len(requested.BackOff) > 0 && !reflect.DeepEqual(requested.BackOff, existing.BackOff) {
		return false
	}
	if requested.FilterSubject != "" && requested.FilterSubject != existing.FilterSubject {
		return false
	}
	if len(requested.FilterSubjects) > 0 && !reflect.DeepEqual(requested.FilterSubjects, existing.FilterSubjects) {
		return false
	}
	if requested.ReplayPolicy != existing.ReplayPolicy {
		return false
	}
	if requested.RateLimit != 0 && requested.RateLimit != existing.RateLimit {
		return false
	}
	if requested.SampleFrequency != "" && requested.SampleFrequency != existing.SampleFrequency {
		return false
	}
	if requested.MaxWaiting != 0 && requested.MaxWaiting != existing.MaxWaiting {
		return false
	}
	if requested.MaxAckPending != 0 && requested.MaxAckPending != existing.MaxAckPending {
		return false
	}
	if requested.FlowControl != existing.FlowControl {
		return false
	}
	if requested.Heartbeat != 0 && requested.Heartbeat != existing.Heartbeat {
		return false
	}
	if requested.HeadersOnly != existing.HeadersOnly {
		return false
	}
	if requested.MaxRequestBatch != 0 && requested.MaxRequestBatch != existing.MaxRequestBatch {
		return false
	}
	if requested.MaxRequestExpires != 0 && requested.MaxRequestExpires != existing.MaxRequestExpires {
		return false
	}
	if requested.MaxRequestMaxBytes != 0 && requested.MaxRequestMaxBytes != existing.MaxRequestMaxBytes {
		return false
	}
	if requested.DeliverSubject != "" && requested.DeliverSubject != existing.DeliverSubject {
		return false
	}
	if requested.DeliverGroup != "" && requested.DeliverGroup != existing.DeliverGroup {
		return false
	}
	if requested.InactiveThreshold != 0 && requested.InactiveThreshold != existing.InactiveThreshold {
		return false
	}
	if requested.Replicas != 0 && requested.Replicas != existing.Replicas {
		return false
	}
	if requested.MemoryStorage != existing.MemoryStorage {
		return false
	}
	if len(requested.Metadata) > 0 && !reflect.DeepEqual(requested.Metadata, existing.Metadata) {
		return false
	}
	return true
}
