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

func TestConfigFromEnvCanonicalPrecedenceAndSanitize(t *testing.T) {
	t.Setenv("NATS_URL", "nats://legacy:secret@legacy.invalid:4222")
	t.Setenv("NATS_TOKEN", "legacy-token")
	t.Setenv("FOUNDATIONX_NATS_NAME", "orders")
	t.Setenv("FOUNDATIONX_NATS_URL", "nats://canon:secret@127.0.0.1:4222")
	t.Setenv("FOUNDATIONX_NATS_SERVERS", "nats://127.0.0.1:4222, nats://localhost:4223")
	t.Setenv("FOUNDATIONX_NATS_TOKEN", "canon-token")
	t.Setenv("FOUNDATIONX_NATS_PASSWORD", "canon-password")
	t.Setenv("FOUNDATIONX_NATS_NKEY_SEED", "canon-seed")
	t.Setenv("FOUNDATIONX_NATS_CREDENTIALS_FILE", "/tmp/canon.creds")
	t.Setenv("FOUNDATIONX_NATS_TIMEOUT", "2s")
	t.Setenv("FOUNDATIONX_NATS_DRAIN_TIMEOUT", "3s")
	t.Setenv("FOUNDATIONX_NATS_MAX_RECONNECTS", "7")
	t.Setenv("FOUNDATIONX_NATS_RECONNECT_WAIT", "250ms")
	t.Setenv("FOUNDATIONX_NATS_ENABLE_JETSTREAM", "true")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.Name != "orders" || cfg.URL != "nats://canon:secret@127.0.0.1:4222" {
		t.Fatalf("canonical config not applied: %+v", cfg)
	}
	if len(cfg.Servers) != 2 || cfg.Servers[1] != "nats://localhost:4223" {
		t.Fatalf("servers = %#v, want canonical server list", cfg.Servers)
	}
	if cfg.Token != "canon-token" || cfg.Timeout != 2*time.Second || cfg.DrainTimeout != 3*time.Second || cfg.MaxReconnects != 7 || cfg.ReconnectWait != 250*time.Millisecond || !cfg.EnableJetStream {
		t.Fatalf("unexpected parsed config: %+v", cfg)
	}
	sanitized := cfg.Sanitize()
	joined := sanitized.URL + strings.Join(sanitized.Servers, ",") + sanitized.Token + sanitized.Password + sanitized.NKeySeed + sanitized.CredentialsFile
	for _, raw := range []string{"secret", "canon-token", "canon-password", "canon-seed", "/tmp/canon.creds", "legacy-token", "legacy.invalid"} {
		if strings.Contains(joined, raw) {
			t.Fatalf("sanitized env config leaked %q: %+v", raw, sanitized)
		}
	}
}

func TestConfigFromEnvLegacyFallback(t *testing.T) {
	t.Setenv("NATS_NAME", "legacy-orders")
	t.Setenv("NATS_URL", "nats://127.0.0.1:4222")
	t.Setenv("NATS_TIMEOUT", "1500ms")
	t.Setenv("NATS_ENABLE_JETSTREAM", "1")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.Name != "legacy-orders" || cfg.URL != "nats://127.0.0.1:4222" || cfg.Timeout != 1500*time.Millisecond || !cfg.EnableJetStream {
		t.Fatalf("legacy config not applied: %+v", cfg)
	}
}

func TestConfigFromEnvRejectsInvalidDurationWithoutSecretLeak(t *testing.T) {
	t.Setenv("FOUNDATIONX_NATS_URL", "nats://127.0.0.1:4222")
	t.Setenv("FOUNDATIONX_NATS_TOKEN", "super-secret-token")
	t.Setenv("FOUNDATIONX_NATS_TIMEOUT", "not-a-duration")

	_, err := ConfigFromEnv()
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ConfigFromEnv() error = %v, want validation", err)
	}
	if strings.Contains(err.Error(), "super-secret-token") || strings.Contains(err.Error(), "not-a-duration") {
		t.Fatalf("ConfigFromEnv() error leaked env value: %v", err)
	}
}
