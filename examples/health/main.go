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
		Name:         "natsx-health-example",
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

	status := client.HealthCheck(ctx)
	if status.Status != natsx.HealthHealthy {
		return fmt.Errorf("health status: %s: %s", status.Status, status.Message)
	}
	_, err = fmt.Fprintln(stdout, status.Status)
	return err
}
