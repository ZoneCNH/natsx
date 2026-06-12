package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ZoneCNH/natsx/pkg/natsx"
)

func main() {
	if err := run(os.Stdout); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	cfg := natsx.Config{
		Name:            "natsx-config-example",
		URL:             "nats://user:password-value@127.0.0.1:4222",
		Servers:         []string{"nats://user:password-value@127.0.0.1:4222"},
		Token:           "token-value",
		Username:        "user",
		Password:        "password-value",
		NKeySeed:        "seed-value",
		CredentialsFile: "/secure/nats.creds",
		Timeout:         time.Second,
		DrainTimeout:    time.Second,
		ReconnectWait:   time.Second,
		EnableJetStream: true,
	}
	sanitized := cfg.Sanitize()

	_, err := fmt.Fprintf(stdout,
		"url=%s\nserver=%s\ncredential=%s\nusername=%s\npassword_secret=%s\nnkey=%s\ncreds=%s\n",
		sanitized.URL,
		sanitized.Servers[0],
		sanitized.Token,
		sanitized.Username,
		sanitized.Password,
		sanitized.NKeySeed,
		sanitized.CredentialsFile,
	)
	return err
}
