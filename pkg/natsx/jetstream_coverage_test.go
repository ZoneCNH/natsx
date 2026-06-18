package natsx

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

// TestJetStreamError covers jetStreamError's nil cause, conflict mapping,
// unavailable mapping, and fallback connection mapping.
func TestJetStreamError(t *testing.T) {
	tests := []struct {
		name      string
		cause     error
		wantKind  ErrorKind
		wantMatch bool // when true, the result must match wantKind
		wantNil   bool
	}{
		{
			name:    "nil cause returns nil",
			cause:   nil,
			wantNil: true,
		},
		{
			name:      "stream name already in use maps to conflict",
			cause:     nats.ErrStreamNameAlreadyInUse,
			wantKind:  ErrorKindConflict,
			wantMatch: true,
		},
		{
			name:      "stream not found maps to unavailable",
			cause:     nats.ErrStreamNotFound,
			wantKind:  ErrorKindUnavailable,
			wantMatch: true,
		},
		{
			name:      "generic error maps to connection",
			cause:     errors.New("boom"),
			wantKind:  ErrorKindConnection,
			wantMatch: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange / Act
			err := jetStreamError("natsx.test", tt.cause)

			// Assert
			if tt.wantNil {
				if err != nil {
					t.Fatalf("jetStreamError(nil) = %v, want nil", err)
				}
				return
			}
			if !IsKind(err, tt.wantKind) {
				t.Fatalf("jetStreamError(%v) kind = %v, want %v", tt.cause, errorKind(err), tt.wantKind)
			}
		})
	}
}

// TestConsumerConfigName covers nil cfg, Name set, Name empty with Durable set,
// and both empty.
func TestConsumerConfigName(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ConsumerConfig
		want string
	}{
		{
			name: "nil cfg returns empty",
			cfg:  nil,
			want: "",
		},
		{
			name: "name set returns name",
			cfg:  &ConsumerConfig{Name: "worker-x"},
			want: "worker-x",
		},
		{
			name: "name empty durable set returns durable",
			cfg:  &ConsumerConfig{Durable: "durable-x"},
			want: "durable-x",
		},
		{
			name: "both empty returns empty",
			cfg:  &ConsumerConfig{},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange / Act
			got := consumerConfigName(tt.cfg)

			// Assert
			if got != tt.want {
				t.Fatalf("consumerConfigName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestConsumerConfigMatches covers nil guards, equal configs, and field-level
// mismatches.
func TestConsumerConfigMatches(t *testing.T) {
	base := &ConsumerConfig{
		Durable:       "worker-b",
		Name:          "worker-b",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    3,
		FilterSubject: "orders.created.publish.v1",
		ReplayPolicy:  nats.ReplayInstantPolicy,
	}

	clone := func() *ConsumerConfig {
		c := *base
		return &c
	}

	tests := []struct {
		name     string
		req      *ConsumerConfig
		existing *ConsumerConfig
		want     bool
	}{
		{
			name:     "nil requested returns false",
			req:      nil,
			existing: clone(),
			want:     false,
		},
		{
			name:     "nil existing returns false",
			req:      clone(),
			existing: nil,
			want:     false,
		},
		{
			name:     "equal returns true",
			req:      clone(),
			existing: clone(),
			want:     true,
		},
		{
			name:     "durable mismatch returns false",
			req:      &ConsumerConfig{Name: "worker-b", Durable: "worker-a"},
			existing: &ConsumerConfig{Name: "worker-b", Durable: "worker-b"},
			want:     false,
		},
		{
			name:     "name mismatch returns false",
			req:      &ConsumerConfig{Name: "worker-a"},
			existing: &ConsumerConfig{Name: "worker-b"},
			want:     false,
		},
		{
			name:     "ack policy mismatch returns false",
			req:      mutate(clone(), func(c *ConsumerConfig) { c.AckPolicy = nats.AckAllPolicy }),
			existing: clone(),
			want:     false,
		},
		{
			name:     "max deliver mismatch returns false",
			req:      mutate(clone(), func(c *ConsumerConfig) { c.MaxDeliver = 99 }),
			existing: clone(),
			want:     false,
		},
		{
			name:     "filter subject mismatch returns false",
			req:      mutate(clone(), func(c *ConsumerConfig) { c.FilterSubject = "orders.updated.publish.v1" }),
			existing: clone(),
			want:     false,
		},
		{
			name:     "replay policy mismatch returns false",
			req:      mutate(clone(), func(c *ConsumerConfig) { c.ReplayPolicy = nats.ReplayOriginalPolicy }),
			existing: clone(),
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange / Act
			got := consumerConfigMatches(tt.req, tt.existing)

			// Assert
			if got != tt.want {
				t.Fatalf("consumerConfigMatches() = %v, want %v", got, tt.want)
			}
		})
	}
}

// mutate applies fn to cfg and returns cfg for table-driven brevity.
func mutate(cfg *ConsumerConfig, fn func(*ConsumerConfig)) *ConsumerConfig {
	fn(cfg)
	return cfg
}

// TestJetStreamClientAddStream covers validation branches and the success path.
func TestJetStreamClientAddStream(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}

	t.Run("nil cfg returns validation error", func(t *testing.T) {
		// Arrange / Act
		_, err := jsClient.AddStream(nil)

		// Assert
		if !IsKind(err, ErrorKindValidation) {
			t.Fatalf("AddStream(nil) error = %v, want validation kind", err)
		}
	})

	t.Run("empty name returns validation error", func(t *testing.T) {
		// Arrange
		cfg := &StreamConfig{Name: "   "}

		// Act
		_, err := jsClient.AddStream(cfg)

		// Assert
		if !IsKind(err, ErrorKindValidation) {
			t.Fatalf("AddStream(empty name) error = %v, want validation kind", err)
		}
	})

	t.Run("success returns stream info", func(t *testing.T) {
		// Arrange
		cfg := &StreamConfig{
			Name:     "ORDERS-COV",
			Subjects: []string{"orders.cov.>"},
		}

		// Act
		stream, err := jsClient.AddStream(cfg)

		// Assert
		if err != nil {
			t.Fatalf("AddStream() error = %v", err)
		}
		if stream.Config.Name != "ORDERS-COV" {
			t.Fatalf("AddStream() name = %q, want ORDERS-COV", stream.Config.Name)
		}
	})
}

// TestJetStreamClientAddConsumer covers the pre-check match branch (returns
// existing) and the conflict branch when a same-named consumer has a different
// config.
func TestJetStreamClientAddConsumer(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}

	streamCfg := &StreamConfig{
		Name:     "ORDERS-CONS",
		Subjects: []string{"orders.cons.>"},
	}
	if _, err := jsClient.AddStream(streamCfg); err != nil {
		t.Fatalf("AddStream() error = %v", err)
	}

	subject := mustSubject(t, "orders", "cons", "publish", 1)
	cfg := &ConsumerConfig{
		Durable:       "cov-worker",
		Name:          "cov-worker",
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    2,
		FilterSubject: subject,
	}

	t.Run("same config returns existing", func(t *testing.T) {
		// Arrange - first add creates the consumer
		first, err := jsClient.AddConsumer("ORDERS-CONS", cfg)
		if err != nil {
			t.Fatalf("first AddConsumer() error = %v", err)
		}
		if first.Config.Durable != "cov-worker" {
			t.Fatalf("first AddConsumer() durable = %q, want cov-worker", first.Config.Durable)
		}

		// Act - second add with same config hits pre-check match branch
		second, err := jsClient.AddConsumer("ORDERS-CONS", cfg)

		// Assert
		if err != nil {
			t.Fatalf("second AddConsumer() error = %v", err)
		}
		if second == nil {
			t.Fatal("second AddConsumer() returned nil, want existing consumer info")
		}
	})

	t.Run("conflicting config returns conflict", func(t *testing.T) {
		// Arrange - same name, different MaxDeliver triggers mismatch.
		conflict := &ConsumerConfig{
			Durable:       "cov-worker",
			Name:          "cov-worker",
			AckPolicy:     nats.AckExplicitPolicy,
			MaxDeliver:    99,
			FilterSubject: subject,
		}

		// Act
		_, err := jsClient.AddConsumer("ORDERS-CONS", conflict)

		// Assert
		if !IsKind(err, ErrorKindConflict) {
			t.Fatalf("AddConsumer(conflict) error = %v, want conflict kind", err)
		}
	})
}

// TestJetStreamClientAddConsumerValidation covers the stream-name and cfg-nil
// validation branches separately so they don't depend on a live server.
func TestJetStreamClientAddConsumerValidation(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}

	t.Run("empty stream name returns validation error", func(t *testing.T) {
		// Arrange / Act
		_, err := jsClient.AddConsumer("   ", &ConsumerConfig{Durable: "x"})

		// Assert
		if !IsKind(err, ErrorKindValidation) {
			t.Fatalf("AddConsumer(blank stream) error = %v, want validation kind", err)
		}
	})

	t.Run("nil cfg returns validation error", func(t *testing.T) {
		// Arrange / Act
		_, err := jsClient.AddConsumer("ORDERS-CONS", nil)

		// Assert
		if !IsKind(err, ErrorKindValidation) {
			t.Fatalf("AddConsumer(nil cfg) error = %v, want validation kind", err)
		}
	})
}
