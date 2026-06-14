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

func TestConfigValidateInvalidEndpointDoesNotEchoEndpoint(t *testing.T) {
	secretEndpoint := "nats://user:secret@example.test:bad-port"
	err := (Config{URL: secretEndpoint}).Validate()
	if err == nil {
		t.Fatalf("Validate() error = nil, want invalid endpoint")
	}
	if strings.Contains(err.Error(), secretEndpoint) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("Validate() leaked endpoint in error %q", err.Error())
	}
}

func TestConfigValidateTLS(t *testing.T) {
	t.Run("tls_insecure_requires_tls", func(t *testing.T) {
		cfg := Config{URL: "nats://127.0.0.1:4222", TLSInsecure: true}
		err := cfg.Validate()
		if !IsKind(err, ErrorKindValidation) {
			t.Fatalf("Validate() error kind = %v, want validation", err)
		}
		if !strings.Contains(err.Error(), "tls_insecure") {
			t.Fatalf("Validate() error = %q, want tls_insecure mention", err.Error())
		}
	})

	t.Run("tls_with_insecure_passes", func(t *testing.T) {
		cfg := Config{URL: "nats://127.0.0.1:4222", TLS: true, TLSInsecure: true}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("tls_without_insecure_passes", func(t *testing.T) {
		cfg := Config{URL: "nats://127.0.0.1:4222", TLS: true}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})
}

func TestConfigSanitizeWithTLS(t *testing.T) {
	cfg := Config{URL: "nats://127.0.0.1:4222", TLS: true, TLSInsecure: true}
	sanitized := cfg.Sanitize()
	if !sanitized.TLS {
		t.Fatalf("sanitized TLS = false, want true")
	}
	if !sanitized.TLSInsecure {
		t.Fatalf("sanitized TLSInsecure = false, want true")
	}
}

func TestConfigBuildTLSConfig(t *testing.T) {
	t.Run("returns_nil_when_tls_disabled", func(t *testing.T) {
		cfg := Config{TLS: false, TLSInsecure: true}
		if tc := cfg.BuildTLSConfig(); tc != nil {
			t.Fatalf("BuildTLSConfig() = %+v, want nil", tc)
		}
	})

	t.Run("returns_nil_when_tls_secure_mode", func(t *testing.T) {
		cfg := Config{TLS: true, TLSInsecure: false}
		if tc := cfg.BuildTLSConfig(); tc != nil {
			t.Fatalf("BuildTLSConfig() = %+v, want nil when not insecure", tc)
		}
	})

	t.Run("returns_insecure_config", func(t *testing.T) {
		cfg := Config{TLS: true, TLSInsecure: true}
		tc := cfg.BuildTLSConfig()
		if tc == nil {
			t.Fatal("BuildTLSConfig() = nil, want *tls.Config")
		}
		if !tc.InsecureSkipVerify {
			t.Fatalf("BuildTLSConfig().InsecureSkipVerify = false, want true")
		}
	})
}
