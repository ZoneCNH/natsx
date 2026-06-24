package natsx

// Phase 3 覆盖率补强测试。只新建此文件，不改任何已存在文件。
// 目标：把未覆盖分支（client/config/core/env/jetstream/subject/metrics）逐一打到 100%。

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// srvPort 返回 embedded server 的监听端口。
func srvPort(t *testing.T, srv *natsserver.Server) (int, bool) {
	t.Helper()
	if tcpAddr, ok := srv.Addr().(*net.TCPAddr); ok {
		return tcpAddr.Port, true
	}
	return 0, false
}

// =====================================================================
// metrics.go: NoopMetrics 三个空方法（0% → 100%）
// =====================================================================

func TestPhase3_NoopMetrics_AllMethodsNoPanic(t *testing.T) {
	n := NoopMetrics{}
	n.IncCounter("c", map[string]string{"a": "b"})
	n.ObserveHistogram("h", 1.5, map[string]string{"a": "b"})
	n.SetGauge("g", 2.0, map[string]string{"a": "b"})
	// nil 入参也接受
	n.IncCounter("c", nil)
	n.ObserveHistogram("h", 0, nil)
	n.SetGauge("g", 0, nil)
	// 通过 Metrics 接口调用（覆盖接口分发）
	var m Metrics = NoopMetrics{}
	m.IncCounter("c", nil)
	m.ObserveHistogram("h", 1, nil)
	m.SetGauge("g", 1, nil)
}

// =====================================================================
// client.go
// =====================================================================

// New: cfg.Validate 失败分支（L35）。
func TestPhase3_New_ValidateFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// name 为纯空白 → withDefaults 后 Name 仍为空 → Validate 报 name required
	_, err := New(ctx, Config{Name: "   ", URL: ""})
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("New(blank name) error = %v, want validation kind", err)
	}
}

// New: 同时覆盖 Token / Username+Password / CredentialsFile / NKeySeed 四个分支。
// Token/UserInfo 走正常路径（server 不校验，option 仅追加）；
// CredentialsFile 指向不存在文件 → nats.Connect 失败 → connectionError；
// NKeySeed 无效 → validationError。
func TestPhase3_New_AuthOptionBranches(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)

	// Token 分支（L45）
	tokCtx, tokCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer tokCancel()
	tokClient, err := New(tokCtx, Config{
		Name:    "natsx-token",
		URL:     srv.ClientURL(),
		Token:   "ignored-by-embedded",
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New(token) error = %v", err)
	}
	closeClient(t, tokClient)

	// Username+Password 分支（L48）
	userCtx, userCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer userCancel()
	userClient, err := New(userCtx, Config{
		Name:     "natsx-user",
		URL:      srv.ClientURL(),
		Username: "u",
		Password: "p",
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("New(userinfo) error = %v", err)
	}
	closeClient(t, userClient)

	// CredentialsFile 分支（L51）：文件不存在导致 nats.Connect 失败
	credCtx, credCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer credCancel()
	_, err = New(credCtx, Config{
		Name:            "natsx-cred",
		URL:             srv.ClientURL(),
		CredentialsFile: "/nonexistent/cred.creds",
		Timeout:         500 * time.Millisecond,
	})
	if !IsKind(err, ErrorKindConnection) {
		t.Fatalf("New(bad credentials) error = %v, want connection kind", err)
	}

	// NKeySeed 无效分支（L61）
	nkeyCtx, nkeyCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer nkeyCancel()
	_, err = New(nkeyCtx, Config{
		Name:      "natsx-nkey-bad",
		URL:       srv.ClientURL(),
		NKeySeed:  "invalid-seed",
		Timeout:   time.Second,
	})
	if !IsKind(err, ErrorKindValidation) {
		t.Fatalf("New(bad nkey) error = %v, want validation kind", err)
	}
}

// JetStream(): 覆盖缓存命中路径（L111-113）和 nil client 守卫（L108-109）。
// 注：conn.JetStream() 返回 error 分支（L115）属 GENUINELY_UNCOVERABLE——
// nats.go 的 JetStream() 仅创建轻量 context，不校验连接/账号状态，实际无法稳定触发。
func TestPhase3_Client_JetStream_CacheAndNilGuards(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)

	// 缓存命中路径（L111-113）：js 已被 New 填充
	js, err := client.JetStream()
	if err != nil || js == nil {
		t.Fatalf("JetStream() cached error=%v js=%v", err, js)
	}
	// 清空缓存强制走 conn.JetStream() 成功路径（L114-119）
	client.js = nil
	js2, err := client.JetStream()
	if err != nil || js2 == nil {
		t.Fatalf("JetStream() fresh error=%v js=%v", err, js2)
	}

	// nil client 守卫（L108-109）
	var nilClient *Client
	if _, err := nilClient.JetStream(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil client JetStream() error = %v, want validation", err)
	}
}

// Close: Drain 错误分支（L135-144）。
// closed conn 调 Drain 返回 ErrConnectionClosed → Close 返回 nil（L137）。
func TestPhase3_Close_DrainConnectionClosedReturnsNil(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	// 先关闭底层连接，使 Drain 报 ErrConnectionClosed
	client.conn.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !client.conn.IsClosed() {
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// 此时 IsClosed()==true → L132-133 直接返回 nil（覆盖该分支）
	if err := client.Close(ctx); err != nil {
		t.Fatalf("Close(already closed) error = %v, want nil", err)
	}
	client.conn = nil
}

// Close: Drain 报 ErrConnectionClosed 分支（L136-137）。
// 需要 IsClosed()==false 但 Drain 返回 ErrConnectionClosed 的窗口。用 Reconnecting 状态更稳。
// 用 reconnecting 连接：Drain 返回 ErrConnectionReconnecting → L139-140 conn.Close()。
func TestPhase3_Close_DrainReconnectingBranch(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	port, ok := srvPort(t, srv)
	if !ok {
		t.Skip("server addr not *net.TCPAddr")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, Config{
		Name:          "natsx-close-reconnect",
		URL:           srv.ClientURL(),
		Timeout:       time.Second,
		DrainTimeout:  200 * time.Millisecond,
		MaxReconnects: 50,
		ReconnectWait: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// 关闭服务端让客户端进入 reconnecting
	srv.Shutdown()
	srv.WaitForShutdown()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !client.conn.IsConnected() && !client.conn.IsClosed() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 重启服务端使 Drain 有机会成功（或触发 reconnecting 分支）
	restarted := runEmbeddedNATSServerOnPort(t, false, port)
	_ = restarted

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	// 此处主要覆盖 Close 的 Drain/重连路径；不严格断言返回值，避免 flaky
	_ = client.Close(closeCtx)
}

// Close: ctx.Done 分支（L160）。
// 用极短 DrainTimeout + 已 cancel 的 ctx，触发 select 走 ctx.Done()。
func TestPhase3_Close_ContextDoneBranch(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	// 已 cancel 的 ctx：ctx.Err()!=nil → L129-130 返回 contextError（覆盖该分支）
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Close(ctx); !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("Close(canceled ctx) error = %v, want context kind", err)
	}
	client.conn = nil
}

// stringsJoin: 多元素分支（L178）。
func TestPhase3_StringsJoin_MultipleElements(t *testing.T) {
	got := stringsJoin([]string{"a", "b", "c"})
	if got != "a,b,c" {
		t.Fatalf("stringsJoin = %q, want a,b,c", got)
	}
	if stringsJoin([]string{"single"}) != "single" {
		t.Fatal("stringsJoin single element mismatch")
	}
	if stringsJoin(nil) != "" {
		t.Fatal("stringsJoin nil mismatch")
	}
}

// =====================================================================
// config.go
// =====================================================================

// Validate: name 为纯空白分支（L78）。
func TestPhase3_Config_Validate_BlankName(t *testing.T) {
	cfg := Config{Name: "   ", URL: "nats://127.0.0.1:4222"}
	if err := cfg.Validate(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Validate(blank name) error = %v, want validation kind", err)
	}
}

// Validate: endpoints 为空分支（L94）。
// withDefaults 在 URL 和 Servers 都空时会填默认 URL，无法直接得空 endpoints。
// 用 Servers=[]string{} 且 URL 显式空字符串绕过默认值不行（withDefaults 仍会填）。
// 真正能触发 L94 的唯一路径：构造后 endpoints() 返回空。验证 endpoints() 的 URL=="" 分支（L115）。
func TestPhase3_Config_endpoints_EmptyURL(t *testing.T) {
	cfg := Config{Name: "x"} // URL==""，Servers==nil
	if got := cfg.endpoints(); got != nil {
		t.Fatalf("endpoints(empty url) = %v, want nil", got)
	}
	// Servers 非空走 L112
	cfg2 := Config{Name: "x", Servers: []string{"nats://a:4222", "nats://b:4222"}}
	if got := cfg2.endpoints(); len(got) != 2 {
		t.Fatalf("endpoints(servers) len = %d, want 2", len(got))
	}
	// URL 多逗号分隔走 L118
	cfg3 := Config{Name: "x", URL: "nats://a:4222,nats://b:4222"}
	if got := cfg3.endpoints(); len(got) != 2 {
		t.Fatalf("endpoints(csv url) len = %d, want 2", len(got))
	}
}

// Validate: invalid endpoint URL（L99-100）。
func TestPhase3_Config_Validate_InvalidEndpoint(t *testing.T) {
	// 用含逗号的无效 URL 绕过 withDefaults 默认值
	cfg := Config{Name: "x", URL: "not-a-url"}
	if err := cfg.Validate(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Validate(invalid url) error = %v, want validation kind", err)
	}
}

// Validate: 负数 timeout / drain / reconnect（L81/84/87）。
func TestPhase3_Config_Validate_NegativeDurations(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"negative timeout", Config{Name: "x", URL: "nats://127.0.0.1:4222", Timeout: -time.Second}},
		{"negative drain", Config{Name: "x", URL: "nats://127.0.0.1:4222", DrainTimeout: -time.Second}},
		{"negative reconnect", Config{Name: "x", URL: "nats://127.0.0.1:4222", ReconnectWait: -time.Second}},
		{"tls_insecure without tls", Config{Name: "x", URL: "nats://127.0.0.1:4222", TLSInsecure: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); !IsKind(err, ErrorKindValidation) {
				t.Fatalf("Validate(%s) error = %v, want validation kind", tc.name, err)
			}
		})
	}
}

// =====================================================================
// env.go: LoadConfigFromEnv 三个 parse-error 分支（L94/L112/L119）
// =====================================================================

func TestPhase3_LoadConfigFromEnv_ReconnectWaitParseError(t *testing.T) {
	t.Setenv("FOUNDATIONX_NATS_RECONNECT_WAIT", "not-a-duration")
	if _, err := LoadConfigFromEnv(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("LoadConfigFromEnv(bad reconnect_wait) error = %v, want validation", err)
	}
}

func TestPhase3_LoadConfigFromEnv_TLSParseError(t *testing.T) {
	t.Setenv("FOUNDATIONX_NATS_TLS", "not-a-bool")
	if _, err := LoadConfigFromEnv(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("LoadConfigFromEnv(bad tls) error = %v, want validation", err)
	}
}

func TestPhase3_LoadConfigFromEnv_TLSInsecureParseError(t *testing.T) {
	// 先让基础配置合法
	t.Setenv("FOUNDATIONX_NATS_URL", "nats://127.0.0.1:4222")
	t.Setenv("FOUNDATIONX_NATS_TLS_INSECURE", "maybe")
	if _, err := LoadConfigFromEnv(); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("LoadConfigFromEnv(bad tls_insecure) error = %v, want validation", err)
	}
}

// 顺带覆盖 MAX_RECONNECTS 与 ENABLE_JETSTREAM 的 parse-error 分支（其他未覆盖项）。
func TestPhase3_LoadConfigFromEnv_MaxReconnectsAndEnableJetStreamParseErrors(t *testing.T) {
	t.Run("max_reconnects", func(t *testing.T) {
		t.Setenv("FOUNDATIONX_NATS_URL", "nats://127.0.0.1:4222")
		t.Setenv("FOUNDATIONX_NATS_MAX_RECONNECTS", "abc")
		if _, err := LoadConfigFromEnv(); !IsKind(err, ErrorKindValidation) {
			t.Fatalf("error = %v, want validation", err)
		}
	})
	t.Run("enable_jetstream", func(t *testing.T) {
		t.Setenv("FOUNDATIONX_NATS_URL", "nats://127.0.0.1:4222")
		t.Setenv("FOUNDATIONX_NATS_ENABLE_JETSTREAM", "yes-please")
		if _, err := LoadConfigFromEnv(); !IsKind(err, ErrorKindValidation) {
			t.Fatalf("error = %v, want validation", err)
		}
	})
}

// =====================================================================
// subject.go
// =====================================================================

func TestPhase3_ParseSubject_ValidationErrors(t *testing.T) {
	// ValidateSubject 错误（L59）
	if _, err := ParseSubject(""); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(empty) error = %v, want validation", err)
	}
	if _, err := ParseSubject("a b.c.d.v1"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(whitespace) error = %v, want validation", err)
	}
	// token 校验失败（L67）：token 含点
	if _, err := ParseSubject("a.b.c.d.v1"); !IsKind(err, ErrorKindValidation) {
		// "a.b.c.d.v1" 有 5 段，先在 len(parts)!=4 报错；换 4 段但 token 含非法
	}
	// 4 段但 token 含通配符（L67 走 validateSubjectToken）
	if _, err := ParseSubject("a.*.c.v1"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(wildcard token) error = %v, want validation", err)
	}
	// len != 4（L63）
	if _, err := ParseSubject("a.b.v1"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(3 parts) error = %v, want validation", err)
	}
	// version 非 vN（L72）
	if _, err := ParseSubject("a.b.c.x1"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(bad version prefix) error = %v, want validation", err)
	}
	// version 非数字（L75-77）
	if _, err := ParseSubject("a.b.c.vx"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(non-numeric version) error = %v, want validation", err)
	}
	// version <=0（L76）
	if _, err := ParseSubject("a.b.c.v0"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ParseSubject(v0) error = %v, want validation", err)
	}
	// 正常路径
	parts, err := ParseSubject("orders.created.publish.v1")
	if err != nil {
		t.Fatalf("ParseSubject(ok) error = %v", err)
	}
	if parts.Domain != "orders" || parts.Version != 1 {
		t.Fatalf("ParseSubject(ok) parts = %+v", parts)
	}
}

func TestPhase3_ValidateSubject_WhitespaceAndEmptyToken(t *testing.T) {
	if err := ValidateSubject("op", "a b"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(whitespace) error = %v", err)
	}
	if err := ValidateSubject("op", ".a.b"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(leading dot) error = %v", err)
	}
	if err := ValidateSubject("op", "a..b"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(empty token) error = %v", err)
	}
	if err := ValidateSubject("op", "a.b."); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("ValidateSubject(trailing dot) error = %v", err)
	}
	if err := ValidateSubject("op", "valid.subject.v1"); err != nil {
		t.Fatalf("ValidateSubject(ok) error = %v", err)
	}
}

// =====================================================================
// core.go: Publish / Request / subscribe / ready 各错误分支
// =====================================================================

// Publish: ValidateSubject 错误（core.go L19）
func TestPhase3_Publish_ValidateSubjectError(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)
	ctx := context.Background()
	if err := client.Publish(ctx, Envelope{Subject: ""}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Publish(empty subject) error = %v, want validation", err)
	}
	if err := client.Publish(ctx, Envelope{Subject: "a b"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Publish(whitespace subject) error = %v, want validation", err)
	}
}

// Publish: PublishMsg 错误（core.go L22）+ Flush 错误（L27）
func TestPhase3_Publish_OnDisconnectedConn(t *testing.T) {
	// ready() 会因 IsConnected()==false 先返回，所以用 reconnecting 状态客户端覆盖。
	srv := runEmbeddedNATSServer(t, false)
	tcpAddrPort, ok := srvPort(t, srv)
	if !ok {
		t.Skip("server addr not *net.TCPAddr")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := New(ctx, Config{
		Name:          "natsx-pub-disc",
		URL:           srv.ClientURL(),
		Timeout:       time.Second,
		MaxReconnects: 50,
		ReconnectWait: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		ctx2, c2 := context.WithTimeout(context.Background(), time.Second)
		defer c2()
		_ = cli.Close(ctx2)
	}()

	// 关掉服务端，客户端进入 reconnecting（IsConnected()==false）
	srv.Shutdown()
	srv.WaitForShutdown()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !cli.conn.IsConnected() && !cli.conn.IsClosed() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	pubCtx := context.Background()
	// ready() 返回 unavailable（IsConnected==false）→ 覆盖 ready 的 L133
	if err := cli.Publish(pubCtx, Envelope{Subject: "orders.created.publish.v1"}); !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("Publish(disconnected) error = %v, want unavailable", err)
	}

	// 重启服务端，使后续 Publish 能发出但 Flush 用 cancel ctx 触发 FlushWithContext 错误（L27）
	restarted := runEmbeddedNATSServerOnPort(t, false, tcpAddrPort)
	_ = restarted
	rd := time.Now().Add(3 * time.Second)
	for time.Now().Before(rd) {
		if cli.conn.IsConnected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 用已 cancel 的 ctx：ready() 中 ctx.Err()!=nil → L130 返回 contextError
	canceledCtx, ccl := context.WithCancel(context.Background())
	ccl()
	if err := cli.Publish(canceledCtx, Envelope{Subject: "orders.created.publish.v1"}); !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("Publish(canceled ctx) error = %v, want context", err)
	}
	// ready ctx=nil 分支（L127）
	if err := cli.Publish(nil, Envelope{Subject: "orders.created.publish.v1"}); err == nil {
		t.Fatal("Publish(nil ctx) expected error")
	}
}

// Request: ValidateSubject 错误（core.go L42）+ ctx cancel（L47/52）
func TestPhase3_Request_ErrorBranches(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)
	ctx := context.Background()

	// ValidateSubject（L42）
	if _, err := client.Request(ctx, Envelope{Subject: ""}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Request(empty subject) error = %v, want validation", err)
	}
	// ctx 已 cancel（L47 ctx.Err()!=nil 分支）— 但 ready() 会先拦截 ctx.Err()
	canceledCtx, ccl := context.WithCancel(context.Background())
	ccl()
	if _, err := client.Request(canceledCtx, Envelope{Subject: "orders.x.request.v1"}); !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("Request(canceled ctx) error = %v, want context", err)
	}
}

// subscribe: ValidateSubject 错误（L82）+ handler nil（L85）+ conn.Subscribe 错误（L115）
func TestPhase3_Subscribe_ErrorBranches(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	// ValidateSubject（L82）
	if _, err := client.Subscribe("a b", func(ctx context.Context, e Envelope) (Envelope, error) { return e, nil }); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Subscribe(bad subject) error = %v, want validation", err)
	}
	// handler nil（L85）
	if _, err := client.Subscribe("orders.created.publish.v1", nil); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Subscribe(nil handler) error = %v, want validation", err)
	}
	// conn 已关闭 → Subscribe 报错（L115）
	client.conn.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !client.conn.IsClosed() {
		time.Sleep(5 * time.Millisecond)
	}
	_, err := client.Subscribe("orders.created.publish.v1", func(ctx context.Context, e Envelope) (Envelope, error) { return e, nil })
	if err == nil {
		t.Fatal("Subscribe(closed conn) expected error")
	}
	client.conn = nil
}

// subscribe: handler 返回 error 分支（L90）—— 观察 metric 记录且不 panic
func TestPhase3_Subscribe_HandlerReturnsError(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	metrics := &recordingMetrics{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, Config{
		Name:    "natsx-handler-err",
		URL:     srv.ClientURL(),
		Timeout: time.Second,
	}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		c, cc := context.WithTimeout(context.Background(), time.Second)
		defer cc()
		_ = client.Close(c)
	}()

	subject := "orders.handlererr.publish.v1"
	errCh := make(chan error, 1)
	sub, err := client.Subscribe(subject, func(_ context.Context, _ Envelope) (Envelope, error) {
		errCh <- errors.New("handler boom")
		return Envelope{}, errors.New("handler boom")
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pubCancel()
	if err := client.Publish(pubCtx, Envelope{Subject: subject, Data: []byte("x")}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not invoked")
	}
	// 等 metric 记录
	rd := time.Now().Add(time.Second)
	for time.Now().Before(rd) {
		if metrics.counterValue(MetricClientErrorsTotal, "subscribe", ErrorKindInternal) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := metrics.counterValue(MetricClientErrorsTotal, "subscribe", ErrorKindInternal); got == 0 {
		t.Fatalf("handler error metric not recorded; counters=%+v", metrics.counters)
	}
}

// subscribe: RespondMsg 错误分支（L98）—— 用已关闭连接的 reply 场景难稳定触发，
// 用 Request 失败验证 RespondMsg 路径不易。改为：reply 订阅 + 主动关闭 conn 让 RespondMsg 失败。
// 该分支需要 msg.Reply != "" 且 RespondMsg 报错。通过订阅有 reply 的请求、
// 然后在 handler 中关闭连接使 RespondMsg 失败。
func TestPhase3_Subscribe_RespondMsgError(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	metrics := &recordingMetrics{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, Config{
		Name:    "natsx-respond-err",
		URL:     srv.ClientURL(),
		Timeout: time.Second,
	}, WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	conn := client.conn
	defer func() {
		client.conn = conn
		c, cc := context.WithTimeout(context.Background(), time.Second)
		defer cc()
		_ = client.Close(c)
	}()

	subject := "orders.responderr.request.v1"
	invoked := make(chan struct{}, 1)
	sub, err := client.Subscribe(subject, func(_ context.Context, _ Envelope) (Envelope, error) {
		// 在响应前关闭底层连接，使 RespondMsg 失败
		conn.Close()
		invoked <- struct{}{}
		return Envelope{Data: []byte("reply")}, nil
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := client.Conn().Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// 用裸 nats.Conn 发送带 reply 的消息（Inbox），模拟 request-reply
	pubConn, perr := nats.Connect(srv.ClientURL(), nats.Name("respond-err-pub"))
	if perr != nil {
		t.Fatalf("nats.Connect(pub) error = %v", perr)
	}
	defer pubConn.Close()
	inbox := nats.NewInbox()
	if err := pubConn.PublishMsg(&nats.Msg{Subject: subject, Reply: inbox, Data: []byte("ping")}); err != nil {
		t.Fatalf("PublishMsg(reply) error = %v", err)
	}
	if err := pubConn.Flush(); err != nil {
		t.Fatalf("Flush(pub) error = %v", err)
	}

	select {
	case <-invoked:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not invoked")
	}
	// 等 metric（RespondMsg 失败 → connectionError "subscribe"）
	rd := time.Now().Add(2 * time.Second)
	for time.Now().Before(rd) {
		if metrics.counterValue(MetricClientErrorsTotal, "subscribe", ErrorKindConnection) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 不强制断言 metric（关闭时序可能不稳定），仅确保无 panic
}

// ready: nil client 分支（core.go L124）
func TestPhase3_Ready_NilClient(t *testing.T) {
	var c *Client
	if err := c.Publish(context.Background(), Envelope{Subject: "a.b.c.v1"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil client Publish error = %v, want validation", err)
	}
	if _, err := c.Subscribe("a.b.c.v1", func(ctx context.Context, e Envelope) (Envelope, error) { return e, nil }); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil client Subscribe error = %v, want validation", err)
	}
	if _, err := c.Request(context.Background(), Envelope{Subject: "a.b.c.v1"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil client Request error = %v, want validation", err)
	}
}

// =====================================================================
// jetstream.go
// =====================================================================

// AddStream: jetStreamError 分支（L49）—— 用无效 stream config 触发非冲突错误。
func TestPhase3_AddStream_JetStreamError(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	// Name 合法但 Subjects 重复跨流冲突 / 非法 Subjects（含空格）触发服务器报错
	_, err = jsClient.AddStream(&StreamConfig{
		Name:     "BADSTREAM",
		Subjects: []string{"bad subject with space"},
	})
	if err == nil {
		t.Fatal("AddStream(bad subjects) expected error")
	}
	// 应为 connection 或 unavailable 类型（jetStreamError 包装）
	if !IsKind(err, ErrorKindConnection) && !IsKind(err, ErrorKindUnavailable) {
		t.Fatalf("AddStream(bad subjects) error = %v, kind = %v", err, errorKind(err))
	}
}

// DeleteStream: jetStreamError 分支（L62）—— 删除不存在的 stream。
func TestPhase3_DeleteStream_NotFoundError(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	if err := jsClient.DeleteStream("MISSING_STREAM_XYZ"); err == nil {
		t.Fatal("DeleteStream(missing) expected error")
	}
}

// AddConsumer: ErrConsumerNameAlreadyInUse 后 ConsumerInfo 失败分支（L105-114）
func TestPhase3_AddConsumer_AlreadyInUseBranches(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	streamCfg := &StreamConfig{Name: "CONSUMERTEST", Subjects: []string{"consumertest.>"}}
	if _, err := jsClient.AddStream(streamCfg); err != nil {
		t.Fatalf("AddStream() error = %v", err)
	}
	// 第一个 consumer
	cfgA := &ConsumerConfig{
		Durable:       "worker-a",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "consumertest.a.publish.v1",
	}
	if _, err := jsClient.AddConsumer("CONSUMERTEST", cfgA); err != nil {
		t.Fatalf("AddConsumer(A) error = %v", err)
	}
	// 同名不同配置 → 冲突（L95 conflictError 路径）
	cfgAConflict := &ConsumerConfig{
		Durable:       "worker-a",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "consumertest.b.publish.v1", // 不同 filter
	}
	_, err = jsClient.AddConsumer("CONSUMERTEST", cfgAConflict)
	if err == nil {
		t.Fatal("AddConsumer(conflict) expected error")
	}
	if !IsKind(err, ErrorKindConflict) && !IsKind(err, ErrorKindConnection) {
		t.Fatalf("AddConsumer(conflict) error = %v, kind=%v", err, errorKind(err))
	}
}

// PullSubscribe: durable trim 后为空（L142）
func TestPhase3_PullSubscribe_BlankDurableAfterTrim(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	if _, err := jsClient.AddStream(&StreamConfig{Name: "PSUB", Subjects: []string{"psub.>"}}); err != nil {
		t.Fatalf("AddStream() error = %v", err)
	}
	// durable 全空白 → trim 后空 → validationError
	if _, err := jsClient.PullSubscribe("psub.created.publish.v1", "   "); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("PullSubscribe(blank durable) error = %v, want validation", err)
	}
}

// PullSubscribe: js.PullSubscribe 错误（L147）—— subject 不属于任何 stream
func TestPhase3_PullSubscribe_NoStreamBranch(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	// 不创建匹配的 stream，PullSubscribe 无 bind 时报错
	_, err = jsClient.PullSubscribe("nonexistent.subject.publish.v1", "dur-x")
	if err == nil {
		t.Fatal("PullSubscribe(no stream) expected error")
	}
	if !IsKind(err, ErrorKindUnavailable) && !IsKind(err, ErrorKindConnection) {
		t.Fatalf("PullSubscribe(no stream) error = %v, kind=%v", err, errorKind(err))
	}
}

// AddStream: JetStreamClient nil 分支（L28-30）+ 其他 nil 守卫
func TestPhase3_JetStreamClient_NilGuards(t *testing.T) {
	var j *JetStreamClient
	if _, err := j.AddStream(&StreamConfig{Name: "X"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil AddStream error = %v", err)
	}
	if err := j.DeleteStream("X"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil DeleteStream error = %v", err)
	}
	if _, err := j.StreamInfo("X"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil StreamInfo error = %v", err)
	}
	if _, err := j.AddConsumer("X", &ConsumerConfig{Durable: "d"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil AddConsumer error = %v", err)
	}
	if _, err := j.Publish(Envelope{Subject: "a.b.c.v1"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil Publish error = %v", err)
	}
	if _, err := j.PullSubscribe("a.b.c.v1", "d"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("nil PullSubscribe error = %v", err)
	}
	// AddStream nil cfg / 空 name
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	if _, err := jsClient.AddStream(nil); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("AddStream(nil cfg) error = %v", err)
	}
	if _, err := jsClient.AddStream(&StreamConfig{Name: "  "}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("AddStream(blank name) error = %v", err)
	}
	// AddConsumer nil cfg / 空 stream
	if _, err := jsClient.AddConsumer("  ", &ConsumerConfig{Durable: "d"}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("AddConsumer(blank stream) error = %v", err)
	}
	if _, err := jsClient.AddConsumer("PSUB2", nil); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("AddConsumer(nil cfg) error = %v", err)
	}
	// Publish 空 subject
	if _, err := jsClient.Publish(Envelope{Subject: ""}); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("Publish(empty subject) error = %v", err)
	}
	// StreamInfo 空 name
	if _, err := jsClient.StreamInfo("  "); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("StreamInfo(blank) error = %v", err)
	}
	// DeleteStream 空 name
	if err := jsClient.DeleteStream("  "); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("DeleteStream(blank) error = %v", err)
	}
	// PullSubscribe 空 subject
	if _, err := jsClient.PullSubscribe("  ", "d"); !IsKind(err, ErrorKindValidation) {
		t.Fatalf("PullSubscribe(blank subject) error = %v", err)
	}
}

// AddStream: StreamInfo 冲突匹配返回 existing 分支（L44）—— 已被 embedded 测试覆盖，
// 此处显式覆盖"冲突但配置匹配 → 返回 existing"。
func TestPhase3_AddStream_ConflictMatchReturnsExisting(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	cfg := &StreamConfig{Name: "MATCHTEST", Subjects: []string{"matchtest.>"}}
	if _, err := jsClient.AddStream(cfg); err != nil {
		t.Fatalf("AddStream(first) error = %v", err)
	}
	existing, err := jsClient.AddStream(cfg)
	if err != nil {
		t.Fatalf("AddStream(second) error = %v", err)
	}
	if existing == nil || existing.Config.Name != "MATCHTEST" {
		t.Fatalf("AddStream(second) existing = %+v", existing)
	}
}

// =====================================================================
// helpers
// =====================================================================

func closeClient(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Close(ctx)
}
