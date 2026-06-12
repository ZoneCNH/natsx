package main

import (
	"bytes"
	"testing"

	"github.com/ZoneCNH/natsx/examples/internal/embeddednats"
	"github.com/ZoneCNH/natsx/pkg/natsx"
)

func TestRunPublishesAndAcksJetStreamMessage(t *testing.T) {
	var stdout bytes.Buffer

	err := run(&stdout, natsx.Config{
		Name:            "natsx-jetstream-example-test",
		URL:             embeddednats.Run(t, true),
		EnableJetStream: true,
	})
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}

	if stdout.String() != "EXAMPLE_EVENTS\n" {
		t.Fatalf("stdout = %q, want stream name", stdout.String())
	}
}

func TestConfigFromEnvRequiresURL(t *testing.T) {
	t.Setenv("NATS_URL", "")

	if _, err := configFromEnv(); err == nil {
		t.Fatal("configFromEnv() error = nil, want error")
	}
}

func TestConfigFromEnvEnablesJetStream(t *testing.T) {
	const url = "nats://127.0.0.1:4222"
	t.Setenv("NATS_URL", url)

	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("configFromEnv() error = %v", err)
	}
	if cfg.URL != url {
		t.Fatalf("URL = %q, want %q", cfg.URL, url)
	}
	if !cfg.EnableJetStream {
		t.Fatal("EnableJetStream = false, want true")
	}
}
