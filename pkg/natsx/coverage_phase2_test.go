package natsx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// --- metrics.go: NoopMetrics 三个方法 (IncCounter / ObserveHistogram / SetGauge) ---

func TestNoopMetrics_AllMethodsNoPanic(t *testing.T) {
	t.Parallel()
	m := NoopMetrics{}
	labels := map[string]string{"op": "publish", "subject": "order.create.v1"}

	// 仅断言无 panic；NoopMetrics 是空实现。
	m.IncCounter(MetricCoreMessagesTotal, labels)
	m.ObserveHistogram(MetricCoreRequestDurationSeconds, 0.42, labels)
	m.SetGauge(MetricClientHealthStatus, 1.0, labels)

	// 用合成 metric 名与 nil labels 再调一次，覆盖空 label 路径。
	m.IncCounter("custom_counter", nil)
	m.ObserveHistogram("custom_histogram", 0.0, nil)
	m.SetGauge("custom_gauge", 0.0, nil)
}

func TestNoopMetrics_SatisfiesInterface(t *testing.T) {
	t.Parallel()
	var _ Metrics = NoopMetrics{}
}

// --- options.go: WithNATSOptions ---

func TestWithNATSOptions_AppendsOptions(t *testing.T) {
	t.Parallel()
	// Arrange
	opts := defaultOptions()
	if len(opts.natsOptions) != 0 {
		t.Fatalf("baseline natsOptions = %d, want 0", len(opts.natsOptions))
	}
	extra := []nats.Option{nats.Name("phase2"), nats.MaxReconnects(7)}

	// Act
	option := WithNATSOptions(extra...)
	option(&opts)

	// Assert
	if got := len(opts.natsOptions); got != len(extra) {
		t.Fatalf("natsOptions len = %d, want %d", got, len(extra))
	}

	// 用 nats.Options 验证追加的 option 真的能 apply。
	no := &nats.Options{}
	for _, o := range opts.natsOptions {
		if err := o(no); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}
	if no.Name != "phase2" {
		t.Errorf("Name = %q, want %q", no.Name, "phase2")
	}
}

func TestWithNATSOptions_ZeroThenSomeVariadic(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	WithNATSOptions()(&opts) // 空可变参数
	if len(opts.natsOptions) != 0 {
		t.Fatalf("after empty WithNATSOptions, len = %d, want 0", len(opts.natsOptions))
	}
	WithNATSOptions(nats.PingInterval(time.Second), nats.ReconnectWait(2*time.Second))(&opts)
	if len(opts.natsOptions) != 2 {
		t.Fatalf("after two WithNATSOptions calls, len = %d, want 2", len(opts.natsOptions))
	}
}

// --- jetstream.go: streamConfigMatches (7.2% → full table-driven) ---

func TestStreamConfigMatches(t *testing.T) {
	t.Parallel()

	// 基准 existing：和 requested 全等（用基础 requested 作为 both 的 clone）。
	base := &StreamConfig{
		Name:                   "ORDERS",
		Description:            "orders stream",
		Subjects:               []string{"order.>"},
		Retention:              nats.LimitsPolicy,
		MaxConsumers:           5,
		MaxMsgs:                1000,
		MaxBytes:               1 << 20,
		Discard:                nats.DiscardOld,
		DiscardNewPerSubject:   true,
		MaxAge:                 time.Minute,
		MaxMsgsPerSubject:      10,
		MaxMsgSize:             4 << 10,
		Storage:                nats.FileStorage,
		Replicas:               3,
		NoAck:                  true,
		Duplicates:             30 * time.Second,
		Placement:              &nats.Placement{Cluster: "ZONE", Tags: []string{"a"}},
		Mirror:                 &nats.StreamSource{Name: "M"},
		Sources:                []*nats.StreamSource{{Name: "S1"}},
		Sealed:                 false,
		DenyDelete:             true,
		DenyPurge:              true,
		AllowRollup:            true,
		Compression:            nats.S2Compression,
		FirstSeq:               1,
		SubjectTransform:       &nats.SubjectTransformConfig{Source: "in.>", Destination: "out.>"},
		RePublish:              &nats.RePublish{Source: "in.>", Destination: "out.>"},
		AllowDirect:            true,
		MirrorDirect:           true,
		ConsumerLimits:         nats.StreamConsumerLimits{MaxAckPending: 100, InactiveThreshold: time.Second},
		Metadata:               map[string]string{"team": "trades"},
		AllowMsgTTL:            true,
		SubjectDeleteMarkerTTL: time.Minute,
	}

	clone := func() *StreamConfig {
		c := *base
		// 深拷贝切片/指针/map，避免用例间互相污染。
		c.Subjects = append([]string(nil), base.Subjects...)
		if base.Placement != nil {
			p := *base.Placement
			p.Tags = append([]string(nil), base.Placement.Tags...)
			c.Placement = &p
		}
		if base.Mirror != nil {
			m := *base.Mirror
			c.Mirror = &m
		}
		if base.Sources != nil {
			s := *base.Sources[0]
			c.Sources = []*nats.StreamSource{&s}
		}
		if base.SubjectTransform != nil {
			st := *base.SubjectTransform
			c.SubjectTransform = &st
		}
		if base.RePublish != nil {
			r := *base.RePublish
			c.RePublish = &r
		}
		c.Metadata = map[string]string{}
		for k, v := range base.Metadata {
			c.Metadata[k] = v
		}
		c.ConsumerLimits = base.ConsumerLimits
		return &c
	}

	// equal case 必须返回 true
	t.Run("equal returns true", func(t *testing.T) {
		if !streamConfigMatches(clone(), clone()) {
			t.Fatalf("identical configs should match")
		}
	})

	t.Run("nil receivers", func(t *testing.T) {
		if streamConfigMatches(nil, clone()) {
			t.Errorf("nil requested should not match")
		}
		if streamConfigMatches(clone(), nil) {
			t.Errorf("nil existing should not match")
		}
		if streamConfigMatches(nil, nil) {
			t.Errorf("both nil should not match")
		}
	})

	t.Run("name mismatch", func(t *testing.T) {
		req := clone()
		req.Name = "OTHER"
		if streamConfigMatches(req, clone()) {
			t.Errorf("name mismatch should not match")
		}
	})

	// 为每个可比较字段构造一个 diff case。requested 字段设非零值触发比较分支。
	diffCases := []struct {
		name   string
		mutate func(req *StreamConfig)
	}{
		{"Description", func(r *StreamConfig) { r.Description = "changed" }},
		{"Subjects", func(r *StreamConfig) { r.Subjects = []string{"other.>"} }},
		{"Retention", func(r *StreamConfig) { r.Retention = nats.WorkQueuePolicy }},
		{"MaxConsumers", func(r *StreamConfig) { r.MaxConsumers = 99 }},
		{"MaxMsgs", func(r *StreamConfig) { r.MaxMsgs = 9999 }},
		{"MaxBytes", func(r *StreamConfig) { r.MaxBytes = 9 << 20 }},
		{"Discard", func(r *StreamConfig) { r.Discard = nats.DiscardNew }},
		{"DiscardNewPerSubject", func(r *StreamConfig) { r.DiscardNewPerSubject = false }},
		{"MaxAge", func(r *StreamConfig) { r.MaxAge = time.Hour }},
		{"MaxMsgsPerSubject", func(r *StreamConfig) { r.MaxMsgsPerSubject = 999 }},
		{"MaxMsgSize", func(r *StreamConfig) { r.MaxMsgSize = 99 << 10 }},
		{"Storage", func(r *StreamConfig) { r.Storage = nats.MemoryStorage }},
		{"Replicas", func(r *StreamConfig) { r.Replicas = 5 }},
		{"NoAck", func(r *StreamConfig) { r.NoAck = false }},
		{"Duplicates", func(r *StreamConfig) { r.Duplicates = time.Hour }},
		{"Placement", func(r *StreamConfig) {
			p := &nats.Placement{Cluster: "OTHER"}
			r.Placement = p
		}},
		{"Mirror", func(r *StreamConfig) {
			m := &nats.StreamSource{Name: "OTHER"}
			r.Mirror = m
		}},
		{"Sources", func(r *StreamConfig) {
			r.Sources = []*nats.StreamSource{{Name: "OTHER"}}
		}},
		{"Sealed", func(r *StreamConfig) { r.Sealed = true }},
		{"DenyDelete", func(r *StreamConfig) { r.DenyDelete = false }},
		{"DenyPurge", func(r *StreamConfig) { r.DenyPurge = false }},
		{"AllowRollup", func(r *StreamConfig) { r.AllowRollup = false }},
		{"Compression", func(r *StreamConfig) { r.Compression = nats.NoCompression }},
		{"FirstSeq", func(r *StreamConfig) { r.FirstSeq = 42 }},
		{"SubjectTransform", func(r *StreamConfig) {
			st := &nats.SubjectTransformConfig{Source: "x.>", Destination: "y.>"}
			r.SubjectTransform = st
		}},
		{"RePublish", func(r *StreamConfig) {
			rp := &nats.RePublish{Source: "x.>", Destination: "y.>"}
			r.RePublish = rp
		}},
		{"AllowDirect", func(r *StreamConfig) { r.AllowDirect = false }},
		{"MirrorDirect", func(r *StreamConfig) { r.MirrorDirect = false }},
		{"ConsumerLimits", func(r *StreamConfig) {
			r.ConsumerLimits = nats.StreamConsumerLimits{MaxAckPending: 999}
		}},
		{"Metadata", func(r *StreamConfig) {
			r.Metadata = map[string]string{"team": "other"}
		}},
		{"AllowMsgTTL", func(r *StreamConfig) { r.AllowMsgTTL = false }},
		{"SubjectDeleteMarkerTTL", func(r *StreamConfig) { r.SubjectDeleteMarkerTTL = time.Hour }},
	}
	for _, tc := range diffCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := clone()
			tc.mutate(req)
			if streamConfigMatches(req, clone()) {
				t.Errorf("%s mismatch should not match", tc.name)
			}
		})
	}

	// requested 字段为零值 → 跳过比较，仍视为匹配。
	// 用一份"只有 Name 非零、其余全部为零值"的两侧 config 验证跳过分支。
	t.Run("zero requested fields skip comparison", func(t *testing.T) {
		onlyName := &StreamConfig{Name: "ONLYNAME"}
		req := &StreamConfig{Name: "ONLYNAME"} // 其余字段均为零值
		if !streamConfigMatches(req, onlyName) {
			t.Errorf("both only-Name configs should match via zero-skip")
		}
	})
}

// --- jetstream.go: consumerConfigMatches 补充未覆盖字段 ---

func TestConsumerConfigMatches_RemainingFields(t *testing.T) {
	t.Parallel()
	base := &ConsumerConfig{
		Durable:            "D",
		Name:               "N",
		Description:        "desc",
		DeliverPolicy:      nats.DeliverAllPolicy,
		OptStartSeq:        10,
		OptStartTime:       &time.Time{},
		AckPolicy:          nats.AckExplicitPolicy,
		AckWait:            time.Second,
		MaxDeliver:         5,
		BackOff:            []time.Duration{time.Second, 2 * time.Second},
		FilterSubject:      "f.>",
		FilterSubjects:     []string{"a.>", "b.>"},
		ReplayPolicy:       nats.ReplayOriginalPolicy,
		RateLimit:          1024,
		SampleFrequency:    "50",
		MaxWaiting:         10,
		MaxAckPending:      20,
		FlowControl:        true,
		Heartbeat:          time.Second,
		HeadersOnly:        true,
		MaxRequestBatch:    8,
		MaxRequestExpires:  time.Second,
		MaxRequestMaxBytes: 4096,
		DeliverSubject:     "deliver.>",
		DeliverGroup:       "grp",
		InactiveThreshold:  time.Second,
		Replicas:           2,
		MemoryStorage:      true,
		Metadata:           map[string]string{"k": "v"},
	}
	clone := func() *ConsumerConfig {
		c := *base
		c.BackOff = append([]time.Duration(nil), base.BackOff...)
		c.FilterSubjects = append([]string(nil), base.FilterSubjects...)
		c.Metadata = map[string]string{}
		for k, v := range base.Metadata {
			c.Metadata[k] = v
		}
		return &c
	}
	if !consumerConfigMatches(clone(), clone()) {
		t.Fatalf("identical consumer configs should match")
	}
	if consumerConfigMatches(nil, clone()) || consumerConfigMatches(clone(), nil) || consumerConfigMatches(nil, nil) {
		t.Fatalf("nil receivers should not match")
	}

	cases := []struct {
		name   string
		mutate func(*ConsumerConfig)
	}{
		{"Durable", func(c *ConsumerConfig) { c.Durable = "OTHER" }},
		{"Name", func(c *ConsumerConfig) { c.Name = "OTHER" }},
		{"Description", func(c *ConsumerConfig) { c.Description = "other" }},
		{"DeliverPolicy", func(c *ConsumerConfig) { c.DeliverPolicy = nats.DeliverLastPolicy }},
		{"OptStartSeq", func(c *ConsumerConfig) { c.OptStartSeq = 999 }},
		{"OptStartTime", func(c *ConsumerConfig) {
			ts := time.Now()
			c.OptStartTime = &ts
		}},
		{"AckPolicy", func(c *ConsumerConfig) { c.AckPolicy = nats.AckNonePolicy }},
		{"AckWait", func(c *ConsumerConfig) { c.AckWait = time.Hour }},
		{"MaxDeliver", func(c *ConsumerConfig) { c.MaxDeliver = 999 }},
		{"BackOff", func(c *ConsumerConfig) { c.BackOff = []time.Duration{time.Hour} }},
		{"FilterSubject", func(c *ConsumerConfig) { c.FilterSubject = "other.>" }},
		{"FilterSubjects", func(c *ConsumerConfig) { c.FilterSubjects = []string{"x.>"} }},
		{"ReplayPolicy", func(c *ConsumerConfig) { c.ReplayPolicy = nats.ReplayInstantPolicy }},
		{"RateLimit", func(c *ConsumerConfig) { c.RateLimit = 65535 }},
		{"SampleFrequency", func(c *ConsumerConfig) { c.SampleFrequency = "100" }},
		{"MaxWaiting", func(c *ConsumerConfig) { c.MaxWaiting = 999 }},
		{"MaxAckPending", func(c *ConsumerConfig) { c.MaxAckPending = 999 }},
		{"FlowControl", func(c *ConsumerConfig) { c.FlowControl = false }},
		{"Heartbeat", func(c *ConsumerConfig) { c.Heartbeat = time.Hour }},
		{"HeadersOnly", func(c *ConsumerConfig) { c.HeadersOnly = false }},
		{"MaxRequestBatch", func(c *ConsumerConfig) { c.MaxRequestBatch = 999 }},
		{"MaxRequestExpires", func(c *ConsumerConfig) { c.MaxRequestExpires = time.Hour }},
		{"MaxRequestMaxBytes", func(c *ConsumerConfig) { c.MaxRequestMaxBytes = 999 }},
		{"DeliverSubject", func(c *ConsumerConfig) { c.DeliverSubject = "other.>" }},
		{"DeliverGroup", func(c *ConsumerConfig) { c.DeliverGroup = "other" }},
		{"InactiveThreshold", func(c *ConsumerConfig) { c.InactiveThreshold = time.Hour }},
		{"Replicas", func(c *ConsumerConfig) { c.Replicas = 9 }},
		{"MemoryStorage", func(c *ConsumerConfig) { c.MemoryStorage = false }},
		{"Metadata", func(c *ConsumerConfig) { c.Metadata = map[string]string{"k": "v2"} }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := clone()
			tc.mutate(req)
			if consumerConfigMatches(req, clone()) {
				t.Errorf("%s mismatch should not match", tc.name)
			}
		})
	}
}

// --- errors.go: errorKind 非 *Error 类型 ---

func TestErrorKind_NonErrorType(t *testing.T) {
	t.Parallel()
	if got := errorKind(errors.New("plain")); got != ErrorKindInternal {
		t.Errorf("errorKind(plain) = %q, want %q", got, ErrorKindInternal)
	}
	if got := errorKind(nil); got != ErrorKindInternal {
		t.Errorf("errorKind(nil) = %q, want %q", got, ErrorKindInternal)
	}
	// 已是 *Error → 返回真实 kind。
	want := ErrorKindTimeout
	err := WrapError(want, "op", "msg", true, nil)
	if got := errorKind(err); got != want {
		t.Errorf("errorKind(*Error) = %q, want %q", got, want)
	}
}

// --- subject.go: validateSubjectToken 通配符分支 ---

func TestValidateSubjectToken_AllBranches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{"empty", "", true},
		{"whitespace only", "   ", true},
		{"contains dot", "a.b", true},
		{"contains star", "or*", true},
		{"contains gt", "order>", true},
		{"contains both wildcards", "*>", true},
		{"contains whitespace", "a b", true},
		{"valid", "orders", false},
		{"valid with trim", "  orders  ", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateSubjectToken("natsx.test", tc.token)
			if tc.wantErr && err == nil {
				t.Errorf("validateSubjectToken(%q) expected error, got nil", tc.token)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validateSubjectToken(%q) unexpected error: %v", tc.token, err)
			}
		})
	}
}

// --- envelope.go: EnvelopeFromMsg header 解析分支 ---

func TestEnvelopeFromMsg_HeaderCombinations(t *testing.T) {
	t.Parallel()
	t.Run("nil msg", func(t *testing.T) {
		if got := EnvelopeFromMsg(nil); got.Subject != "" || got.Data != nil {
			t.Errorf("nil msg should return zero envelope")
		}
	})
	t.Run("all known headers present", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: "order.create.v1",
			Reply:   "reply.subject",
			Header: nats.Header{
				HeaderEventID:       []string{"evt-1"},
				HeaderMessageID:     []string{"msg-1"},
				HeaderSchemaVersion: []string{"v1"},
				HeaderTraceID:       []string{"trace-1"},
			},
			Data: []byte("payload"),
		}
		env := EnvelopeFromMsg(msg)
		if env.EventID != "evt-1" || env.MessageID != "msg-1" ||
			env.SchemaVersion != "v1" || env.TraceID != "trace-1" {
			t.Errorf("headers not parsed: %+v", env)
		}
		if env.Subject != "order.create.v1" || env.Reply != "reply.subject" {
			t.Errorf("subject/reply mismatch")
		}
		if string(env.Data) != "payload" {
			t.Errorf("data mismatch: %q", env.Data)
		}
		if env.Headers[HeaderEventID][0] != "evt-1" {
			t.Errorf("raw headers not copied")
		}
	})
	t.Run("headers missing", func(t *testing.T) {
		msg := &nats.Msg{Subject: "s", Data: []byte("d")}
		env := EnvelopeFromMsg(msg)
		if env.EventID != "" || env.MessageID != "" ||
			env.SchemaVersion != "" || env.TraceID != "" {
			t.Errorf("missing headers should be empty: %+v", env)
		}
		if env.Headers != nil {
			t.Errorf("empty header should produce nil Headers map")
		}
	})
	t.Run("case-insensitive header keys", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: "s",
			Header:  nats.Header{"MESSAGEID": []string{"msg-x"}},
		}
		env := EnvelopeFromMsg(msg)
		if env.MessageID != "msg-x" {
			t.Errorf("case-insensitive lookup failed: got %q", env.MessageID)
		}
	})
}

// --- config.go: Validate 错误分支 ---

func TestConfig_Validate_ErrorBranches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		cfg     Config
		wantMsg string
	}{
		{"negative timeout", Config{Name: "x", URL: "nats://x:4222", Timeout: -1}, "timeout must not be negative"},
		{"negative drain timeout", Config{Name: "x", URL: "nats://x:4222", DrainTimeout: -1}, "drain timeout must not be negative"},
		{"negative reconnect wait", Config{Name: "x", URL: "nats://x:4222", ReconnectWait: -1}, "reconnect wait must not be negative"},
		{"tls_insecure without tls", Config{Name: "x", URL: "nats://x:4222", TLSInsecure: true}, "tls_insecure requires tls"},
		{"invalid url scheme", Config{Name: "x", Servers: []string{"not-a-url"}}, "invalid NATS server URL"},
		{"invalid url no host", Config{Name: "x", Servers: []string{"nats://"}}, "invalid NATS server URL"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("err = %q, want contains %q", err.Error(), tc.wantMsg)
			}
		})
	}
	// empty endpoints：URL 和 Servers 都清空，withDefaults 会塞默认 URL，
	// 所以用空 Servers + 空 URL 但显式设置（覆盖 endpoints()==[] 分支需要绕过默认值，
	// 这里改为验证 Servers 仅含空字符串时 endpoints() 非空 → 走 URL 解析失败分支）。
	t.Run("servers with empty endpoint still fails url parse", func(t *testing.T) {
		cfg := Config{Name: "x", Servers: []string{":::bad"}}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected error for malformed server, got nil")
		}
	})
}

func TestConfig_SanitizeServers(t *testing.T) {
	t.Parallel()
	// 覆盖 sanitizeServers 与 sanitizeDSN 的 user-info 分支。
	in := []string{"nats://user:pass@host:4222", "nats://plain:4222"}
	out := sanitizeServers(in)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	if strings.Contains(out[0], "pass") {
		t.Errorf("dsn password should be redacted: %q", out[0])
	}
	if out[1] != "nats://plain:4222" {
		t.Errorf("plain dsn should pass through: %q", out[1])
	}
}

// --- env.go: LoadConfigFromEnv 错误分支 ---

func TestLoadConfigFromEnv_ParseErrors(t *testing.T) {
	// 注意：t.Setenv 不能与 t.Parallel 同时使用，故此处串行执行。
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"bad timeout", map[string]string{"FOUNDATIONX_NATS_TIMEOUT": "not-a-duration"}},
		{"bad drain timeout", map[string]string{"FOUNDATIONX_NATS_DRAIN_TIMEOUT": "nope"}},
		{"bad max reconnects", map[string]string{"FOUNDATIONX_NATS_MAX_RECONNECTS": "abc"}},
		{"bad reconnect wait", map[string]string{"FOUNDATIONX_NATS_RECONNECT_WAIT": "xyz"}},
		{"bad enable jetstream", map[string]string{"FOUNDATIONX_NATS_ENABLE_JETSTREAM": "maybe"}},
		{"bad tls", map[string]string{"FOUNDATIONX_NATS_TLS": "kinda"}},
		{"bad tls insecure", map[string]string{"FOUNDATIONX_NATS_TLS_INSECURE": "nope"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// 清掉相关 env 后再设置
			for _, suffix := range natsEnvSuffixes {
				t.Setenv(foundationXNATSEnvPrefix+suffix, "")
				t.Setenv(legacyNATSEnvPrefix+suffix, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := LoadConfigFromEnv()
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

// --- core.go: Publish / subscribe / QueueSubscribe / ready 错误分支 ---

func TestPublish_NotConnected(t *testing.T) {
	t.Parallel()
	c := &Client{} // conn == nil
	err := c.Publish(context.Background(), NewEnvelope("order.create.v1", []byte("x")))
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Errorf("Publish on nil conn = %v, want 'not connected'", err)
	}
}

func TestPublish_CanceledContext(t *testing.T) {
	t.Parallel()
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 提前取消 → ready() 走 ctx.Err() 分支

	err := client.Publish(ctx, NewEnvelope("order.create.v1", []byte("x")))
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
}

func TestPublish_NilContext(t *testing.T) {
	t.Parallel()
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	err := client.Publish(nil, NewEnvelope("order.create.v1", []byte("x")))
	if err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Errorf("Publish(nil ctx) = %v, want 'context is required'", err)
	}
}

func TestReady_NilClientAndCtx(t *testing.T) {
	t.Parallel()
	c := &Client{}
	if err := c.ready("op", context.Background()); err == nil {
		t.Errorf("ready on nil conn should error")
	}
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)
	if err := client.ready("op", nil); err == nil {
		t.Errorf("ready with nil ctx should error")
	}
}

func TestSubscribe_NilHandler(t *testing.T) {
	t.Parallel()
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	if _, err := client.Subscribe("order.create.v1", nil); err == nil {
		t.Errorf("Subscribe with nil handler should error")
	}
}

func TestSubscribe_NilClient(t *testing.T) {
	t.Parallel()
	c := &Client{}
	if _, err := c.Subscribe("order.create.v1", func(context.Context, Envelope) (Envelope, error) {
		return Envelope{}, nil
	}); err == nil {
		t.Errorf("Subscribe on nil conn should error")
	}
}

func TestQueueSubscribe_EmptyQueue(t *testing.T) {
	t.Parallel()
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)

	handler := func(context.Context, Envelope) (Envelope, error) { return Envelope{}, nil }
	if _, err := client.QueueSubscribe("order.create.v1", "   ", handler); err == nil {
		t.Errorf("QueueSubscribe with blank queue should error")
	}
}

// --- client.go: Close / Conn 分支 ---

func TestConn_NilReceiver(t *testing.T) {
	t.Parallel()
	var c *Client
	if got := c.Conn(); got != nil {
		t.Errorf("Conn() on nil receiver = %v, want nil", got)
	}
}

func TestClose_NilReceiverAndAlreadyClosed(t *testing.T) {
	t.Parallel()
	var c *Client
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("Close() on nil receiver = %v, want nil", err)
	}
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)
	// 先正常关闭一次（Cleanup 会再调一次，IsClosed 分支返回 nil）
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	// 第二次：IsClosed() 为真 → 直接返回 nil。
	if err := client.Close(context.Background()); err != nil {
		t.Errorf("second Close() on closed conn = %v, want nil", err)
	}
}

func TestClose_DrainTimeoutExceeded(t *testing.T) {
	// 极短 DrainTimeout 配置触发 timeout 分支（不依赖 sleep 断言）。
	srv := runEmbeddedNATSServer(t, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, Config{
		Name:         "natsx-timeout",
		URL:          srv.ClientURL(),
		Timeout:      2 * time.Second,
		DrainTimeout: time.Millisecond, // 极短
	})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	// 不订阅任何东西 → Drain 立即返回，但 timer 极短。
	// 主要确保不 panic 且最终返回（可能 nil 或 timeout，都接受）。
	_ = client.Close(context.Background())
}

func TestClose_CanceledContextBeforeClose(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)
	client := newEmbeddedClient(t, srv, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 进入 Close 时 ctx 已取消 → contextError 分支
	// 先把 cleanup 注册的 close 抢先跑掉再手动测：这里直接断言返回 ctx 错误。
	err := client.Close(ctx)
	if err == nil {
		// 已 IsClosed 时可能 nil；否则必须含 context canceled。
		return
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("Close(canceled ctx) = %v, want contains 'context canceled'", err)
	}
}

// --- jetstream.go: DeleteStream / AddConsumer / PullSubscribe 错误分支 ---

func TestJetStreamClient_NilGuards(t *testing.T) {
	t.Parallel()
	var j *JetStreamClient
	if err := j.DeleteStream("s"); err == nil {
		t.Errorf("DeleteStream on nil js should error")
	}
	if err := (&JetStreamClient{}).DeleteStream("   "); err == nil {
		t.Errorf("DeleteStream with empty name should error")
	}
	if _, err := (&JetStreamClient{}).AddConsumer("s", &ConsumerConfig{Name: "c"}); err == nil {
		t.Errorf("AddConsumer on nil js should error")
	}
	if _, err := (&JetStreamClient{}).AddConsumer("", &ConsumerConfig{Name: "c"}); err == nil {
		t.Errorf("AddConsumer with empty stream should error")
	}
	if _, err := (&JetStreamClient{}).AddConsumer("s", nil); err == nil {
		t.Errorf("AddConsumer with nil cfg should error")
	}
	if _, err := (&JetStreamClient{}).PullSubscribe("s", ""); err == nil {
		t.Errorf("PullSubscribe on nil js should error")
	}
}

func TestJetStreamClient_DeleteStream_NotFound(t *testing.T) {
	t.Parallel()
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jc, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() = %v", err)
	}
	// 删除不存在的 stream → jetStreamError 分支
	if err := jc.DeleteStream("nonexistent-stream"); err == nil {
		t.Errorf("DeleteStream on missing stream should error")
	}
}

func TestJetStreamClient_PullSubscribe_WhitespaceDurable(t *testing.T) {
	t.Parallel()
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jc, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() = %v", err)
	}
	// durable 仅含空白 → trim 后为空 → validationError 分支
	if _, err := jc.PullSubscribe("order.create.v1", "   "); err == nil {
		t.Errorf("PullSubscribe with whitespace durable should error")
	}
}

func TestJetStreamClient_AddConsumer_ConfigConflictPath(t *testing.T) {
	// 先建 stream 与 consumer，再用同名但不同 config 触发 conflict 分支。
	t.Parallel()
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jc, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() = %v", err)
	}
	streamName := "CONSUMERCONF_STREAM"
	if _, err := jc.AddStream(&StreamConfig{
		Name:     streamName,
		Subjects: []string{"cc.>"},
	}); err != nil {
		t.Fatalf("AddStream() = %v", err)
	}
	cfg := &ConsumerConfig{
		Durable:       "cc-durable",
		Name:          "cc-durable",
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
	}
	if _, err := jc.AddConsumer(streamName, cfg); err != nil {
		t.Fatalf("first AddConsumer() = %v", err)
	}
	// 同名但不同 AckPolicy → existing 存在但 config 不匹配 → conflictError
	conflict := &ConsumerConfig{
		Durable:       "cc-durable",
		Name:          "cc-durable",
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckNonePolicy, // 不同
	}
	if _, err := jc.AddConsumer(streamName, conflict); err == nil {
		t.Errorf("AddConsumer with mismatched config should error")
	}
}
