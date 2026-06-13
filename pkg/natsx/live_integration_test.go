package natsx

import (
	"bytes"
	"context"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveNATSIntegration(t *testing.T) {
	if os.Getenv("NATSX_LIVE_INTEGRATION") != "1" {
		t.Skip("set NATSX_LIVE_INTEGRATION=1 to run live local NATS integration")
	}

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal("ConfigFromEnv() failed for live integration")
	}
	if !configUsesOnlyLocalNATSEndpoints(cfg) {
		t.Fatal("live integration requires local NATS endpoints")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, cfg)
	if err != nil {
		t.Fatal("New() failed for live local NATS integration")
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		if err := client.Close(closeCtx); err != nil {
			t.Fatal("Close() failed for live local NATS integration")
		}
	})

	subject := mustSubject(t, "live", "integration", "request", 1)
	sub, err := client.Subscribe(subject, func(_ context.Context, env Envelope) (Envelope, error) {
		if !bytes.Equal(env.Data, []byte("ping")) {
			t.Errorf("request payload = %q, want ping", env.Data)
		}
		return Envelope{Data: []byte("pong"), TraceID: env.TraceID}, nil
	})
	if err != nil {
		t.Fatal("Subscribe() failed for live local NATS integration")
	}
	defer sub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		t.Fatal("Flush() failed for live local NATS integration")
	}

	requestCtx, requestCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer requestCancel()
	reply, err := client.Request(requestCtx, Envelope{Subject: subject, TraceID: "trace-live-1", Data: []byte("ping")})
	if err != nil {
		t.Fatal("Request() failed for live local NATS integration")
	}
	if !bytes.Equal(reply.Data, []byte("pong")) {
		t.Fatalf("reply data = %q, want pong", reply.Data)
	}
	if reply.TraceID != "trace-live-1" {
		t.Fatalf("reply TraceID = %q, want trace-live-1", reply.TraceID)
	}
}

func configUsesOnlyLocalNATSEndpoints(cfg Config) bool {
	for _, endpoint := range cfg.withDefaults().endpoints() {
		if !isLocalNATSEndpoint(endpoint) {
			return false
		}
	}
	return true
}

func isLocalNATSEndpoint(endpoint string) bool {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Host == "" {
		return false
	}
	host := parsed.Hostname()
	if host == "" {
		if splitHost, _, splitErr := net.SplitHostPort(parsed.Host); splitErr == nil {
			host = splitHost
		}
	}
	return isLocalNATSHost(host)
}

func isLocalNATSHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
