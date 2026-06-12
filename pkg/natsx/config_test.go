package natsx

import (
	"strings"
	"testing"
	"time"
)

func TestConfigValidateDefaultsAndSanitize(t *testing.T) {
	cfg := Config{URL: "nats://alice:secret@example.test:4222", Token: "tok", Password: "pw", NKeySeed: "seed", CredentialsFile: "/tmp/creds.jwt"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	sanitized := cfg.Sanitize()
	if sanitized.Name != "natsx" {
		t.Fatalf("default name = %q, want natsx", sanitized.Name)
	}
	if sanitized.Timeout != (5*time.Second).String() || sanitized.DrainTimeout != (30*time.Second).String() {
		t.Fatalf("unexpected default timeouts: %+v", sanitized)
	}
	for _, raw := range []string{"secret", "tok", "pw", "seed", "/tmp/creds.jwt"} {
		if strings.Contains(sanitized.URL+sanitized.Token+sanitized.Password+sanitized.NKeySeed+sanitized.CredentialsFile, raw) {
			t.Fatalf("sanitized config leaked %q: %+v", raw, sanitized)
		}
	}
}

func TestConfigValidateRejectsInvalidEndpoint(t *testing.T) {
	err := (Config{URL: "not a url"}).Validate()
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Validate() error kind = %v, want validation", err)
	}
}
