package masenkoclient_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path"
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
	task, err := c.Fetch(ctx, []string{"my-queue"})
	if err != nil {
		t.Fatalf("cannot fetch: %s", err)
	}
	if task.Name != "my-task" {
		t.Fatalf("invalid task: %+v", task)
	}
}

func TestFetchTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := RunServerAndClient(ctx, t)

	shortCtx, done := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer done()

	start := time.Now()
	if task, err := c.Fetch(shortCtx, []string{"my-queue"}); !errors.Is(err, masenkoclient.ErrEmpty) {
		t.Fatalf("want empty response, got %+v, %+v", err, task)
	}
	duration := time.Now().Sub(start)

	if duration < 50*time.Millisecond || duration > 70*time.Millisecond {
		// An approximate duration, as timeout is never that precise.
		t.Fatalf("unexpected fetch duration: %s", duration)
	}

}

func TestAcknowledge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := RunServerAndClient(ctx, t)
	if err := c.Push(ctx, "my-task", "my-queue", nil, "", 20, nil); err != nil {
		t.Fatalf("cannot push: %s", err)
	}
	task1, err := c.Fetch(ctx, []string{"my-queue"})
	if err != nil {
		t.Fatalf("cannot fetch first task: %s", err)
	}
	if err := c.Nack(ctx, task1.ID); err != nil {
		t.Fatalf("cannot NACK: %s", err)
	}
	task2, err := c.Fetch(ctx, []string{"my-queue"})
	if err != nil {
		t.Fatalf("cannot fetch second task: %s", err)
	}

	if task1.ID != task2.ID || task1.Name != task2.Name {
		t.Fatalf("want the same task to be delivered again: %+v != %+v", task1, task2)
	}

	if err := c.Ack(ctx, task2.ID); err != nil {
		t.Fatalf("cannot ACK: %s", err)
	}

	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if result, err := c.Fetch(ctx2, []string{"my-queue"}); !errors.Is(err, masenkoclient.ErrEmpty) {
		t.Fatalf("want ErrEmpty, got %+v, %#v", err, result)
	}
}

func RunServerAndClient(ctx context.Context, t testing.TB) masenkoclient.Client {
	t.Helper()

	serverAddr := RunServer(ctx, t)
	c, err := masenkoclient.Dial(serverAddr)
	if err != nil {
		t.Fatalf("cannot connect: %s", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func RunServer(ctx context.Context, t testing.TB) string {
	t.Helper()

	conf := masenko.ServerConfiguration{
		Heartbeat:  5 * time.Second,
		MaxWALSize: 1e8,
		StoreDir:   tempdir(t),
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

func tempdir(t testing.TB) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("read random: %s", err)
	}
	dir := path.Join(os.TempDir(), hex.EncodeToString(b))
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		t.Fatalf("create temporary directory %q: %s", dir, err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
