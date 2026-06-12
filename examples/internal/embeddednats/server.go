package embeddednats

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

func Run(t testing.TB, jetStream bool) string {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: jetStream,
	}
	if jetStream {
		opts.StoreDir = t.TempDir()
	}

	srv := natsserver.New(opts)
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("embedded NATS server did not become ready")
	}

	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return srv.ClientURL()
}
