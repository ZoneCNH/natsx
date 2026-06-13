package natsx

import "github.com/nats-io/nats.go"

type Option func(*options)
type options struct {
	metrics     Metrics
	logger      Logger
	natsOptions []nats.Option
}

func defaultOptions() options { return options{metrics: NoopMetrics{}, logger: NoopLogger{}} }
func WithMetrics(metrics Metrics) Option {
	return func(o *options) {
		if metrics != nil {
			o.metrics = metrics
		}
	}
}
func WithLogger(logger Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}
func WithNATSOptions(opts ...nats.Option) Option {
	return func(o *options) { o.natsOptions = append(o.natsOptions, opts...) }
}
