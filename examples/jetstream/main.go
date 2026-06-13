package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ZoneCNH/natsx/pkg/natsx"
	"github.com/nats-io/nats.go"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	if err := run(os.Stdout, cfg); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(1)
	}
}

func configFromEnv() (natsx.Config, error) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		return natsx.Config{}, errors.New("NATS_URL is required")
	}
	return natsx.Config{
		Name:            "natsx-jetstream-example",
		URL:             url,
		Timeout:         2 * time.Second,
		DrainTimeout:    2 * time.Second,
		EnableJetStream: true,
	}, nil
}

func run(stdout io.Writer, cfg natsx.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := natsx.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	js, err := client.JetStreamClient()
	if err != nil {
		return err
	}
	subject, err := natsx.Subject().Build("orders", "created", "events", 1)
	if err != nil {
		return err
	}
	streamName := "EXAMPLE_EVENTS"
	if _, err := js.AddStream(&natsx.StreamConfig{Name: streamName, Subjects: []string{subject}}); err != nil {
		return fmt.Errorf("add stream: %w", err)
	}
	if _, err := js.AddConsumer(streamName, &natsx.ConsumerConfig{
		Durable:       "example-worker",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subject,
	}); err != nil {
		return fmt.Errorf("add consumer: %w", err)
	}
	if _, err := js.Publish(natsx.Envelope{Subject: subject, EventID: "example-jetstream-1", Data: []byte("created")}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	sub, err := js.PullSubscribe(subject, "example-worker", nats.Bind(streamName, "example-worker"))
	if err != nil {
		return fmt.Errorf("pull subscribe: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if len(msgs) != 1 {
		return fmt.Errorf("fetched %d messages, want 1", len(msgs))
	}
	if err := msgs[0].Ack(); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	_, err = fmt.Fprintln(stdout, streamName)
	return err
}
