package natsx

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// runEmbeddedNATSServerWithMaxPayload 启动一个 MaxPayload 受限的内嵌 NATS 服务器，
// 用于触发客户端 PublishMsg 的 ErrMaxPayload 错误路径（core.go Publish L22-26）。
func runEmbeddedNATSServerWithMaxPayload(t testing.TB, maxPayload int32) *natsserver.Server {
	t.Helper()

	opts := &natsserver.Options{
		Host:       "127.0.0.1",
		Port:       -1,
		NoLog:      true,
		NoSigs:     true,
		MaxPayload: maxPayload,
	}

	srv := natsserver.New(opts)
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("embedded NATS server (max payload) did not become ready")
	}

	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return srv
}

// TestPhase4WrapWithOptions 覆盖 client.go Wrap() L94-96：传入 Option 时执行 options 应用循环。
func TestPhase4WrapWithOptions(t *testing.T) {
	srv := runEmbeddedNATSServer(t, false)

	rawConn, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(rawConn.Close)

	client, err := Wrap(rawConn, WithMetrics(NoopMetrics{}), WithNATSOptions(nats.Name("phase4-wrap")))
	if err != nil {
		t.Fatalf("Wrap() error = %v", err)
	}
	if client == nil || client.Conn() == nil {
		t.Fatalf("Wrap() returned nil client/conn")
	}

	// nil 连接必须被拒绝（Wrap L90-92 校验路径）
	if _, err := Wrap(nil); err == nil {
		t.Fatalf("Wrap(nil) should error")
	}
}

// TestPhase4PublishMaxPayloadError 覆盖 core.go Publish() L22-26：
// 在 MaxPayload 极小的服务器上发布超长数据，conn.PublishMsg 返回 ErrMaxPayload，
// 触发 connectionError + recordErrorMetric 分支。
func TestPhase4PublishMaxPayloadError(t *testing.T) {
	srv := runEmbeddedNATSServerWithMaxPayload(t, 1) //nolint:gomnd // tiny payload to force ErrMaxPayload
	client := newEmbeddedClient(t, srv, false)

	subject := mustSubject(t, "phase4", "maxpayload", "publish", 1)
	err := client.Publish(context.Background(), Envelope{
		Subject: subject,
		Data:    []byte("this payload exceeds the 1-byte server max payload limit"),
	})
	if err == nil {
		t.Fatalf("Publish(oversized) should error")
	}
	if !IsKind(err, ErrorKindConnection) {
		t.Fatalf("Publish(oversized) error = %v, want connection kind", err)
	}
}
