package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunPrintsSanitizedConfig(t *testing.T) {
	var stdout bytes.Buffer

	if err := run(&stdout); err != nil {
		t.Fatalf("run() error = %v", err)
	}

	output := stdout.String()
	for _, secret := range []string{"password-value", "token-value", "seed-value", "nats.creds"} {
		if strings.Contains(output, secret) {
			t.Fatalf("output leaked %q: %q", secret, output)
		}
	}
	for _, redacted := range []string{
		"credential=***",
		"password_secret=***",
		"nkey=***",
		"creds=***",
	} {
		if !strings.Contains(output, redacted) {
			t.Fatalf("output = %q, want %q", output, redacted)
		}
	}
	if !strings.Contains(output, "username=user") {
		t.Fatalf("output = %q, want non-secret username", output)
	}
}
