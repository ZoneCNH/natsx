package natsx

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestConfigFromEnvCanonicalPrecedence(t *testing.T) {
	clearNATSEnv(t)
	t.Setenv("FOUNDATIONX_NATS_NAME", "foundationx-client")
	t.Setenv("FOUNDATIONX_NATS_URL", "nats://canonical.example.test:4222")
	t.Setenv("FOUNDATIONX_NATS_SERVERS", "nats://one.example.test:4222, nats://two.example.test:4222")
	t.Setenv("FOUNDATIONX_NATS_TOKEN", "canonical-token")
	t.Setenv("FOUNDATIONX_NATS_USERNAME", "canonical-user")
	t.Setenv("FOUNDATIONX_NATS_PASSWORD", "canonical-password")
	t.Setenv("FOUNDATIONX_NATS_NKEY_SEED", "canonical-seed")
	t.Setenv("FOUNDATIONX_NATS_CREDENTIALS_FILE", "/secure/canonical.creds")
	t.Setenv("FOUNDATIONX_NATS_TIMEOUT", "3s")
	t.Setenv("FOUNDATIONX_NATS_DRAIN_TIMEOUT", "4s")
	t.Setenv("FOUNDATIONX_NATS_MAX_RECONNECTS", "7")
	t.Setenv("FOUNDATIONX_NATS_RECONNECT_WAIT", "250ms")
	t.Setenv("FOUNDATIONX_NATS_ENABLE_JETSTREAM", "true")

	t.Setenv("NATS_NAME", "legacy-client")
	t.Setenv("NATS_URL", "nats://legacy.example.test:4222")
	t.Setenv("NATS_TOKEN", "legacy-token")
	t.Setenv("NATS_USERNAME", "legacy-user")
	t.Setenv("NATS_PASSWORD", "legacy-password")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.Name != "foundationx-client" || cfg.URL != "nats://canonical.example.test:4222" {
		t.Fatalf("canonical env did not take precedence: %+v", cfg.Sanitize())
	}
	if !reflect.DeepEqual(cfg.Servers, []string{"nats://one.example.test:4222", "nats://two.example.test:4222"}) {
		t.Fatalf("Servers = %#v", cfg.Servers)
	}
	if cfg.Token != "canonical-token" || cfg.Username != "canonical-user" || cfg.Password != "canonical-password" || cfg.NKeySeed != "canonical-seed" || cfg.CredentialsFile != "/secure/canonical.creds" {
		t.Fatalf("secret or credential fields did not use canonical env: %+v", cfg.Sanitize())
	}
	if cfg.Timeout != 3*time.Second || cfg.DrainTimeout != 4*time.Second || cfg.MaxReconnects != 7 || cfg.ReconnectWait != 250*time.Millisecond || !cfg.EnableJetStream {
		t.Fatalf("parsed env fields = %+v", cfg.Sanitize())
	}
}

func TestConfigFromEnvLegacyFallback(t *testing.T) {
	clearNATSEnv(t)
	t.Setenv("NATS_URL", "nats://legacy.example.test:4222")
	t.Setenv("NATS_ENABLE_JETSTREAM", "1")
	t.Setenv("NATS_TIMEOUT", "2s")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.URL != "nats://legacy.example.test:4222" {
		t.Fatalf("URL = %q", cfg.URL)
	}
	if !cfg.EnableJetStream || cfg.Timeout != 2*time.Second {
		t.Fatalf("legacy parsed fields = %+v", cfg.Sanitize())
	}
}

func TestConfigFromEnvRejectsInvalidValuesWithoutSecretLeak(t *testing.T) {
	clearNATSEnv(t)
	t.Setenv("FOUNDATIONX_NATS_URL", "nats://alice:super-secret@example.test:4222")
	t.Setenv("FOUNDATIONX_NATS_PASSWORD", "also-secret")
	t.Setenv("FOUNDATIONX_NATS_TIMEOUT", "super-secret")

	_, err := ConfigFromEnv()
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ConfigFromEnv() error kind = %v, want validation", err)
	}
	message := err.Error()
	for _, leaked := range []string{"super-secret", "also-secret", "alice"} {
		if strings.Contains(message, leaked) {
			t.Fatalf("ConfigFromEnv() error leaked %q: %s", leaked, message)
		}
	}
	if !strings.Contains(message, "FOUNDATIONX_NATS_TIMEOUT") {
		t.Fatalf("ConfigFromEnv() error = %q, want env key", message)
	}
}

func clearNATSEnv(t *testing.T) {
	t.Helper()
	for _, prefix := range []string{foundationXNATSEnvPrefix, legacyNATSEnvPrefix} {
		for _, suffix := range natsEnvSuffixes {
			t.Setenv(prefix+suffix, "")
		}
	}
}
