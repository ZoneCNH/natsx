package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ZoneCNH/natsx/pkg/natsx"
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
		Name:         "natsx-basic-example",
		URL:          url,
		Timeout:      2 * time.Second,
		DrainTimeout: 2 * time.Second,
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

	subject, err := natsx.Subject().Build("orders", "created", "publish", 1)
	if err != nil {
		return err
	}
	received := make(chan natsx.Envelope, 1)
	sub, err := client.Subscribe(subject, func(_ context.Context, env natsx.Envelope) (natsx.Envelope, error) {
		received <- env
		return natsx.Envelope{}, nil
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer sub.Unsubscribe()
	if err := client.Conn().Flush(); err != nil {
		return fmt.Errorf("flush subscription: %w", err)
	}

	publishCtx, publishCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer publishCancel()
	if err := client.Publish(publishCtx, natsx.Envelope{
		Subject: subject,
		EventID: "example-basic-1",
		Data:    []byte("created"),
	}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	select {
	case env := <-received:
		_, err = fmt.Fprintln(stdout, env.Subject)
		return err
	case <-time.After(2 * time.Second):
		return errors.New("timed out waiting for message")
	}
}
