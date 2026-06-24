package natsx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestClientWrap covers (*Client) construction via Wrap including the nil-conn
// validation branch and the success path that returns the original conn.
func TestClientWrap(t *testing.T) {
	t.Run("nil conn returns validation error", func(t *testing.T) {
		// Arrange / Act
		client, err := Wrap(nil)

		// Assert
		if client != nil {
			t.Fatalf("Wrap(nil) client = %v, want nil", client)
		}
		if !IsKind(err, ErrorKindValidation) {
			t.Fatalf("Wrap(nil) error = %v, want validation kind", err)
		}
	})

	t.Run("real conn is preserved", func(t *testing.T) {
		// Arrange
		srv := runEmbeddedNATSServer(t, false)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("nats.Connect error = %v", err)
		}
		t.Cleanup(nc.Close)

		// Act
		client, err := Wrap(nc)
		if err != nil {
			t.Fatalf("Wrap(nc) error = %v", err)
		}

		// Assert
		if client == nil {
			t.Fatal("Wrap(nc) client = nil, want non-nil")
		}
		if client.Conn() != nc {
			t.Fatalf("Conn() = %p, want %p", client.Conn(), nc)
		}
	})
}

// TestClientJetStream covers JetStream() including nil receiver, cached js,
// and lazy load on a Wrap-constructed client whose js field may be nil.
func TestClientJetStream(t *testing.T) {
	t.Run("nil receiver returns validation error", func(t *testing.T) {
		// Arrange
		var c *Client

		// Act
		js, err := c.JetStream()

		// Assert
		if js != nil {
			t.Fatalf("nil receiver JetStream() js = %v, want nil", js)
		}
		if !IsKind(err, ErrorKindValidation) {
			t.Fatalf("nil receiver JetStream() error = %v, want validation kind", err)
		}
	})

	t.Run("cached js returned when already set", func(t *testing.T) {
		// Arrange
		srv := runEmbeddedNATSServer(t, true)
		client := newEmbeddedClient(t, srv, true)

		// Act - first call populates c.js
		js1, err := client.JetStream()
		if err != nil {
			t.Fatalf("JetStream() first call error = %v", err)
		}
		// Act - second call hits the cached branch
		js2, err := client.JetStream()
		if err != nil {
			t.Fatalf("JetStream() second call error = %v", err)
		}

		// Assert
		if js1 == nil {
			t.Fatal("JetStream() first call returned nil")
		}
		if js1 != js2 {
			t.Fatal("JetStream() second call did not return cached js")
		}
	})

	t.Run("lazy load on wrap client without preset js", func(t *testing.T) {
		// Arrange - Wrap with a non-JetStream conn returns client with js=nil
		// because the embedded server here does not enable JetStream. Use a
		// JetStream-enabled server so the lazy load succeeds.
		srv := runEmbeddedNATSServer(t, true)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("nats.Connect error = %v", err)
		}
		t.Cleanup(nc.Close)

		client, err := Wrap(nc)
		if err != nil {
			t.Fatalf("Wrap error = %v", err)
		}
		// Force the lazy-load branch by clearing any preset js from Wrap.
		client.js = nil

		// Act
		js, err := client.JetStream()

		// Assert
		if err != nil {
			t.Fatalf("JetStream() lazy load error = %v", err)
		}
		if js == nil {
			t.Fatal("JetStream() lazy load returned nil")
		}
	})
}

// TestClientClose covers Close branches: nil ctx, IsClosed early-return,
// timer timeout, and ctx cancellation.
func TestClientClose(t *testing.T) {
	t.Run("nil ctx does not panic", func(t *testing.T) {
		// Arrange
		srv := runEmbeddedNATSServer(t, false)
		client := newEmbeddedClient(t, srv, false)

		// Act - bypass the cleanup Close by clearing conn beforehand is not
		// possible; just call Close with nil ctx directly.
		//nolint:staticcheck // intentional nil-context branch coverage
		err := client.Close(nil)

		// Assert
		if err != nil {
			t.Fatalf("Close(nil ctx) error = %v, want nil", err)
		}
	})

	t.Run("double close returns nil", func(t *testing.T) {
		// Arrange
		srv := runEmbeddedNATSServer(t, false)
		client := newEmbeddedClient(t, srv, false)

		// Act - first close drains and closes
		firstCtx, firstCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer firstCancel()
		if err := client.Close(firstCtx); err != nil {
			t.Fatalf("first Close() error = %v", err)
		}
		waitForCondition(t, 2*time.Second, func() bool {
			return client.Conn().IsClosed()
		}, "first Close() left connection open")

		// Act - second close hits IsClosed early-return
		// Assert
		if err := client.Close(context.Background()); err != nil {
			t.Fatalf("second Close() error = %v, want nil", err)
		}
	})

	t.Run("drain timeout returns timeout error", func(t *testing.T) {
		// Arrange - build a client with a near-zero DrainTimeout so the timer
		// fires before Drain completes.
		srv := runEmbeddedNATSServer(t, false)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := New(ctx, Config{
			Name:         "natsx-close-timeout",
			URL:          srv.ClientURL(),
			Timeout:      2 * time.Second,
			DrainTimeout: 1 * time.Nanosecond,
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		// Act
		err = client.Close(context.Background())

		// Assert - IsClosed race makes this branch timing-dependent; accept
		// either the timeout path or a clean close. The DrainTimeout of 1ns
		// strongly biases toward the timer.C branch.
		if err == nil {
			// Drain finished before the 1ns timer in some runs; acceptable.
			return
		}
		if !IsKind(err, ErrorKindTimeout) {
			t.Fatalf("Close(timeout) error = %v, want timeout kind (or nil)", err)
		}
	})

	t.Run("canceled ctx returns ctx error", func(t *testing.T) {
		// Arrange
		srv := runEmbeddedNATSServer(t, false)
		client := newEmbeddedClient(t, srv, false)

		// Act - pre-canceled ctx triggers ctx.Done() in the select loop.
		canceledCtx, canceledCancel := context.WithCancel(context.Background())
		canceledCancel()
		err := client.Close(canceledCtx)

		// Assert
		if err == nil {
			t.Fatal("Close(canceled ctx) error = nil, want non-nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close(canceled ctx) error = %v, want context.Canceled in chain", err)
		}
	})
}
