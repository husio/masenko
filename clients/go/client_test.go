package masenkoclient_test

import (
	"context"
	"os"
	"testing"
	"time"

	masenkoclient "github.com/husio/masenko/clients/go"
	"github.com/husio/masenko/masenko"
)

func TestConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := RunServerAndClient(ctx, t)
	if err := c.Close(); err != nil {
		t.Fatalf("close: %s", err)
	}
}

func TestPushAndFetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := RunServerAndClient(ctx, t)
	if err := c.Push(ctx, "my-task", "my-queue", nil, "", 20, nil); err != nil {
		t.Fatalf("cannot push: %s", err)
	}
	task, err := c.Fetch(ctx, []string{"my-queue"}, time.Second)
	if err != nil {
		t.Fatalf("cannot fetch: %s", err)
	}
	if task.Name != "my-task" {
		t.Fatalf("invalid task: %+v", task)
	}
}

func RunServerAndClient(ctx context.Context, t testing.TB) masenkoclient.Client {
	t.Helper()

	serverAddr := RunServer(ctx, t)
	c, err := masenkoclient.Dial(serverAddr)
	if err != nil {
		t.Fatalf("cannot connect: %s", err)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("cannot ping: %s", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func RunServer(ctx context.Context, t testing.TB) string {
	t.Helper()

	conf := masenko.ServerConfiguration{
		Heartbeat:  time.Second,
		MaxWALSize: 1e8,
		StoreDir:   os.TempDir(),
		ListenTCP:  "localhost:13456",
		ListenHTTP: "localhost:13457",
	}
	server, err := masenko.StartServer(ctx, conf)
	if err != nil {
		t.Fatalf("start server: %s", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return conf.ListenTCP
}
