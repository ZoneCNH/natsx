package natsx

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	foundationXNATSEnvPrefix = "FOUNDATIONX_NATS_"
	legacyNATSEnvPrefix      = "NATS_"
)

var natsEnvSuffixes = []string{
	"NAME",
	"CLIENT_NAME",
	"URL",
	"SERVERS",
	"TOKEN",
	"USERNAME",
	"PASSWORD",
	"NKEY_SEED",
	"CREDENTIALS_FILE",
	"TIMEOUT",
	"DRAIN_TIMEOUT",
	"MAX_RECONNECTS",
	"RECONNECT_WAIT",
	"ENABLE_JETSTREAM",
	"TLS",
	"TLS_INSECURE",
}

// ConfigFromEnv builds a Config from FoundationX NATS environment variables.
//
// Canonical FOUNDATIONX_NATS_* variables take precedence over legacy NATS_*
// variables. Empty values are ignored. Parse errors name the invalid key but do
// not echo the raw value so callers can safely log err.Error().
func ConfigFromEnv() (Config, error) {
	return LoadConfigFromEnv()
}

// LoadConfigFromEnv is an alias for ConfigFromEnv retained for callers that use
// loader-style naming.
func LoadConfigFromEnv() (Config, error) {
	cfg := Config{}.withDefaults()

	if value, _, ok := lookupNATSEnv("NAME", "CLIENT_NAME"); ok {
		cfg.Name = value
	}
	if value, _, ok := lookupNATSEnv("URL"); ok {
		cfg.URL = value
	}
	if value, _, ok := lookupNATSEnv("SERVERS"); ok {
		cfg.Servers = splitEnvList(value)
	}
	if value, _, ok := lookupNATSEnv("TOKEN"); ok {
		cfg.Token = value
	}
	if value, _, ok := lookupNATSEnv("USERNAME"); ok {
		cfg.Username = value
	}
	if value, _, ok := lookupNATSEnv("PASSWORD"); ok {
		cfg.Password = value
	}
	if value, _, ok := lookupNATSEnv("NKEY_SEED"); ok {
		cfg.NKeySeed = value
	}
	if value, _, ok := lookupNATSEnv("CREDENTIALS_FILE"); ok {
		cfg.CredentialsFile = value
	}
	if value, key, ok := lookupNATSEnv("TIMEOUT"); ok {
		duration, err := parseDurationEnv(key, value)
		if err != nil {
			return Config{}, err
		}
		cfg.Timeout = duration
	}
	if value, key, ok := lookupNATSEnv("DRAIN_TIMEOUT"); ok {
		duration, err := parseDurationEnv(key, value)
		if err != nil {
			return Config{}, err
		}
		cfg.DrainTimeout = duration
	}
	if value, key, ok := lookupNATSEnv("MAX_RECONNECTS"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return Config{}, envParseError(key, "integer", err)
		}
		cfg.MaxReconnects = parsed
	}
	if value, key, ok := lookupNATSEnv("RECONNECT_WAIT"); ok {
		duration, err := parseDurationEnv(key, value)
		if err != nil {
			return Config{}, err
		}
		cfg.ReconnectWait = duration
	}
	if value, key, ok := lookupNATSEnv("ENABLE_JETSTREAM"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, envParseError(key, "boolean", err)
		}
		cfg.EnableJetStream = parsed
	}
	if value, key, ok := lookupNATSEnv("TLS"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, envParseError(key, "boolean", err)
		}
		cfg.TLS = parsed
	}
	if value, key, ok := lookupNATSEnv("TLS_INSECURE"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, envParseError(key, "boolean", err)
		}
		cfg.TLSInsecure = parsed
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func lookupNATSEnv(suffixes ...string) (string, string, bool) {
	for _, prefix := range []string{foundationXNATSEnvPrefix, legacyNATSEnvPrefix} {
		for _, suffix := range suffixes {
			key := prefix + suffix
			value, ok := os.LookupEnv(key)
			value = strings.TrimSpace(value)
			if ok && value != "" {
				return value, key, true
			}
		}
	}
	return "", "", false
}

func splitEnvList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseDurationEnv(key, value string) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, envParseError(key, "duration", err)
	}
	return duration, nil
}

func envParseError(key, kind string, _ error) error {
	return validationError("natsx.ConfigFromEnv", "invalid "+kind+" for "+key, nil)
}
