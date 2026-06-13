package natsx

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// ConfigFromEnv returns a Config populated from canonical FOUNDATIONX_NATS_*
// environment variables, falling back to legacy NATS_* names when the
// canonical variable is unset. Secret values are never included in parse
// errors.
func ConfigFromEnv() (Config, error) {
	cfg := Config{}
	if value, ok := lookupNATSEnv("NAME"); ok {
		cfg.Name = value
	}
	if value, ok := lookupNATSEnv("URL"); ok {
		cfg.URL = value
	}
	if value, ok := lookupNATSEnv("SERVERS"); ok {
		cfg.Servers = splitNATSServers(value)
	}
	if value, ok := lookupNATSEnv("TOKEN"); ok {
		cfg.Token = value
	}
	if value, ok := lookupNATSEnv("USERNAME"); ok {
		cfg.Username = value
	}
	if value, ok := lookupNATSEnv("PASSWORD"); ok {
		cfg.Password = value
	}
	if value, ok := lookupNATSEnv("NKEY_SEED"); ok {
		cfg.NKeySeed = value
	}
	if value, ok := lookupNATSEnv("CREDENTIALS_FILE"); ok {
		cfg.CredentialsFile = value
	}
	if value, key, ok := lookupNATSEnvWithKey("TIMEOUT"); ok {
		duration, err := parseEnvDuration(key, value)
		if err != nil {
			return Config{}, err
		}
		cfg.Timeout = duration
	}
	if value, key, ok := lookupNATSEnvWithKey("DRAIN_TIMEOUT"); ok {
		duration, err := parseEnvDuration(key, value)
		if err != nil {
			return Config{}, err
		}
		cfg.DrainTimeout = duration
	}
	if value, key, ok := lookupNATSEnvWithKey("MAX_RECONNECTS"); ok {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return Config{}, validationError("ConfigFromEnv", "invalid integer in "+key, err)
		}
		cfg.MaxReconnects = parsed
	}
	if value, key, ok := lookupNATSEnvWithKey("RECONNECT_WAIT"); ok {
		duration, err := parseEnvDuration(key, value)
		if err != nil {
			return Config{}, err
		}
		cfg.ReconnectWait = duration
	}
	if value, key, ok := lookupNATSEnvWithKey("ENABLE_JETSTREAM"); ok {
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return Config{}, validationError("ConfigFromEnv", "invalid boolean in "+key, err)
		}
		cfg.EnableJetStream = parsed
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg.withDefaults(), nil
}

// LoadConfigFromEnv is kept as a descriptive alias for callers that prefer
// loader-style naming.
func LoadConfigFromEnv() (Config, error) { return ConfigFromEnv() }

func lookupNATSEnv(suffix string) (string, bool) {
	value, _, ok := lookupNATSEnvWithKey(suffix)
	return value, ok
}

func lookupNATSEnvWithKey(suffix string) (string, string, bool) {
	canonical := "FOUNDATIONX_NATS_" + suffix
	if value, ok := os.LookupEnv(canonical); ok {
		return strings.TrimSpace(value), canonical, true
	}
	legacy := "NATS_" + suffix
	if value, ok := os.LookupEnv(legacy); ok {
		return strings.TrimSpace(value), legacy, true
	}
	return "", "", false
}

func parseEnvDuration(key, value string) (time.Duration, error) {
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, validationError("ConfigFromEnv", "invalid duration in "+key, err)
	}
	return duration, nil
}

func splitNATSServers(value string) []string {
	parts := strings.Split(value, ",")
	servers := make([]string, 0, len(parts))
	for _, part := range parts {
		if endpoint := strings.TrimSpace(part); endpoint != "" {
			servers = append(servers, endpoint)
		}
	}
	return servers
}
