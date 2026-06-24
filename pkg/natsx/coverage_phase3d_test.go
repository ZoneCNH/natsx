package natsx

// Phase 3d: 覆盖 AddConsumer pre-check 中 ConsumerInfo 返回非 NotFound 错误的分支（jetstream.go:97-99）。
// 只新建此文件，不改任何已存在文件。
//
// 触发路径：AddConsumer 用带 Name/Durable 的 cfg + 不存在的 stream 名。
// pre-check 的 ConsumerInfo 返回 ErrStreamNotFound（不是 ErrConsumerNotFound），
// → L97 `infoErr != nil && !errors.Is(NotFound)` 为真 → L98 jetStreamError。

import (
	"testing"
)

func TestPhase3d_AddConsumer_PreCheckStreamNotFound(t *testing.T) {
	srv := runEmbeddedNATSServer(t, true)
	client := newEmbeddedClient(t, srv, true)
	jsClient, err := client.JetStreamClient()
	if err != nil {
		t.Fatalf("JetStreamClient() error = %v", err)
	}
	// 创建一个合法 stream 作为对照组
	if _, err := jsClient.AddStream(&StreamConfig{Name: "ACEXIST", Subjects: []string{"acexist.>"}}); err != nil {
		t.Fatalf("AddStream() error = %v", err)
	}
	// 对不存在的 stream 调 AddConsumer，cfg 带 Name（触发 pre-check ConsumerInfo）。
	// ConsumerInfo("MISSING_STREAM", name) 返回 ErrStreamNotFound（非 ConsumerNotFound）
	// → L97-99 jetStreamError。
	cfg := &ConsumerConfig{
		Name:         "consumer-x",
		FilterSubject: "missing.a.publish.v1",
	}
	_, err = jsClient.AddConsumer("MISSING_STREAM_XYZ", cfg)
	if err == nil {
		t.Fatal("AddConsumer(missing stream) expected error")
	}
	// ErrStreamNotFound 经 jetStreamError → ErrorKindUnavailable
	if !IsKind(err, ErrorKindUnavailable) && !IsKind(err, ErrorKindConnection) {
		t.Fatalf("AddConsumer(missing stream) error = %v, kind=%v, want unavailable/connection", err, errorKind(err))
	}
}
