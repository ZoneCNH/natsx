package natsx

import (
	"crypto/tls"
	"net/url"
	"strings"
	"time"

	"github.com/ZoneCNH/natsx/internal/sanitize"
)

type Config struct {
	Name            string
	URL             string
	Servers         []string
	Token           string
	Username        string
	Password        string
	NKeySeed        string
	CredentialsFile string
	Timeout         time.Duration
	DrainTimeout    time.Duration
	MaxReconnects   int
	ReconnectWait   time.Duration
	EnableJetStream bool
	TLS             bool
	TLSInsecure     bool
}

// BuildTLSConfig returns a *tls.Config suitable for nats.Secure() when TLSInsecure
// is set. If TLS is not enabled or verification is not skipped, it returns nil.
func (c Config) BuildTLSConfig() *tls.Config {
	if !c.TLS || !c.TLSInsecure {
		return nil
	}
	return &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in for dev only
}

type SanitizedConfig struct {
	Name            string   `json:"name"`
	URL             string   `json:"url,omitempty"`
	Servers         []string `json:"servers,omitempty"`
	Token           string   `json:"token,omitempty"`
	Username        string   `json:"username,omitempty"`
	Password        string   `json:"password,omitempty"`
	NKeySeed        string   `json:"nkey_seed,omitempty"`
	CredentialsFile string   `json:"credentials_file,omitempty"`
	Timeout         string   `json:"timeout"`
	DrainTimeout    string   `json:"drain_timeout"`
	MaxReconnects   int      `json:"max_reconnects"`
	ReconnectWait   string   `json:"reconnect_wait"`
	EnableJetStream bool     `json:"enable_jetstream"`
	TLS             bool     `json:"tls"`
	TLSInsecure     bool     `json:"tls_insecure"`
}

func (c Config) withDefaults() Config {
	if c.Name == "" {
		c.Name = "natsx"
	}
	if c.URL == "" && len(c.Servers) == 0 {
		c.URL = "nats://127.0.0.1:4222"
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 30 * time.Second
	}
	if c.ReconnectWait == 0 {
		c.ReconnectWait = time.Second
	}
	return c
}

func (c Config) Validate() error {
	c = c.withDefaults()
	if strings.TrimSpace(c.Name) == "" {
		return validationError("Config.Validate", "name is required", nil)
	}
	if c.Timeout < 0 {
		return validationError("Config.Validate", "timeout must not be negative", nil)
	}
	if c.DrainTimeout < 0 {
		return validationError("Config.Validate", "drain timeout must not be negative", nil)
	}
	if c.ReconnectWait < 0 {
		return validationError("Config.Validate", "reconnect wait must not be negative", nil)
	}
	if c.TLSInsecure && !c.TLS {
		return validationError("Config.Validate", "tls_insecure requires tls to be enabled", nil)
	}
	// endpoints 恒非空：withDefaults() 已在上方保证 URL 非空（Servers 为空时回退默认 DSN），
	// endpoints() 至少返回按逗号切分的 1 个端点，故原 len==0 守卫为不可达死分支，已移除。
	endpoints := c.endpoints()
	for _, endpoint := range endpoints {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return validationError("Config.Validate", "invalid NATS server URL", nil)
		}
	}
	return nil
}

func (c Config) Sanitize() SanitizedConfig {
	c = c.withDefaults()
	return SanitizedConfig{Name: c.Name, URL: sanitizeDSN(c.URL), Servers: sanitizeServers(c.Servers), Token: sanitize.Secret(c.Token), Username: c.Username, Password: sanitize.Secret(c.Password), NKeySeed: sanitize.Secret(c.NKeySeed), CredentialsFile: redactPath(c.CredentialsFile), Timeout: c.Timeout.String(), DrainTimeout: c.DrainTimeout.String(), MaxReconnects: c.MaxReconnects, ReconnectWait: c.ReconnectWait.String(), EnableJetStream: c.EnableJetStream, TLS: c.TLS, TLSInsecure: c.TLSInsecure}
}

func (c Config) endpoints() []string {
	if len(c.Servers) > 0 {
		return append([]string(nil), c.Servers...)
	}
	if c.URL == "" {
		return nil
	}
	return strings.Split(c.URL, ",")
}

func sanitizeServers(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		out = append(out, sanitizeDSN(s))
	}
	return out
}
func sanitizeDSN(raw string) string {
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		if u, err := url.Parse(strings.TrimSpace(p)); err == nil && u.User != nil {
			u.User = url.UserPassword("***", "***")
			parts[i] = u.String()
		} else {
			parts[i] = strings.TrimSpace(p)
		}
	}
	return strings.Join(parts, ",")
}
func redactPath(path string) string {
	if path == "" {
		return ""
	}
	return sanitize.Secret(path)
}
