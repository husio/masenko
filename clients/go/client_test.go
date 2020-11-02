package masenkoclient_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path"
	"testing"
	"time"

	masenkoclient "github.com/husio/masenko/clients/go"
	"github.com/husio/masenko/masenko"
)

func ExampleHeartbeatClient_Push() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mclient, err := masenkoclient.Dial("localhost:12345")
	if err != nil {
		panic("cannot connect: " + err.Error())
	}
	defer mclient.Close()

	// Task payload can be any JSON serializable data.
	newUser := struct {
		Name  string
		Admin bool
	}{
		Name:  "John Smith",
		Admin: false,
	}

	if err := mclient.Push(ctx, "register-user", "", newUser, "", 0, nil); err != nil {
		panic("cannot push task: " + err.Error())
	}
}

func ExampleHeartbeatClient_Push_delayed() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mclient, err := masenkoclient.Dial("localhost:12345")
	if err != nil {
		panic("cannot connect: " + err.Error())
	}
	defer mclient.Close()

	// Task payload can be any JSON serializable data.
	email := struct {
		Subject string
		To      string
		Content string
	}{
		Subject: "Welcome!",
		To:      "john.smith@example.com",
		Content: "Warm welcome John Smith.",
	}

	// Instead of sending an email now, delay it by at least 10 minutes.
	// Delayed task cannot be consumed until the deadline is reached.
	future := time.Now().Add(10 * time.Minute)

	if err := mclient.Push(ctx, "send-email", "", email, "", 0, &future); err != nil {
		panic("cannot push task: " + err.Error())
	}
}

func ExampleHeartbeatClient_Tx() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mclient, err := masenkoclient.Dial("localhost:12345")
	if err != nil {
		panic("cannot connect: " + err.Error())
	}
	defer mclient.Close()

	// Create a new transaction instance that will aggregate one or more
	// operations.
	tx := mclient.Tx(ctx)

	if err := tx.Push(ctx, "register-user", "", "John Smith", "", 0, nil); err != nil {
		panic("cannot push task: " + err.Error())
	}
	if err := tx.Ack(ctx, 12345); err != nil {
		panic("cannot ack task 12345: " + err.Error())
	}

	// Until Commit method is called, all operations executed on
	// transaction are cached and not shared with the Masenko server.
	// Transaction commit sends all operations to the server and executes
	// atomically.
	// There is no transaction rollback. To abort any executed so far
	// transaction operation, simply do not commit the transaction.
	if err := tx.Commit(ctx); err != nil {
		panic("cannot commit transaction: " + err.Error())
	}
}

func ExampleHeartbeatClient_Fetch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mclient, err := masenkoclient.Dial("localhost:12345")
	if err != nil {
		panic("cannot connect: " + err.Error())
	}
	defer mclient.Close()

	for {
		response, err := mclient.Fetch(ctx, []string{"priority", "default"})
		if err != nil {
			panic("cannot fetch: " + err.Error())
		}

		switch response.Name {
		case "register-user":
			var newUser struct {
				Name  string
				Admin bool
			}
			if err := json.Unmarshal(response.Payload, &newUser); err != nil {
				panic("cannot unmarshal register-user task payload: " + err.Error())
			}
			err = handleRegisterUser(newUser)
		case "send-email":
			var email struct {
				Subject string
				To      string
				Content string
			}
			if err := json.Unmarshal(response.Payload, &email); err != nil {
				panic("cannot unmarshal send-email task payload: " + err.Error())
			}
			err = handleSendEmail(email)
		default:
			if err := mclient.Nack(ctx, response.ID); err != nil {
				panic("cannot NACK: " + err.Error())
			}
		}

		if err == nil {
			if err := mclient.Ack(ctx, response.ID); err != nil {
				panic("cannot ACK: " + err.Error())
			}
		} else {
			if err := mclient.Nack(ctx, response.ID); err != nil {
				panic("cannot NACK: " + err.Error())
			}
		}
	}
}

// handleRegisterUser is a stub function.
func handleRegisterUser(interface{}) error { return nil }

// handleSendEmail is a stub function.
func handleSendEmail(interface{}) error { return nil }

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

func TestTransactionSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := RunServerAndClient(ctx, t)

	tx := c.Tx(ctx)
	if err := tx.Push(ctx, "my-task", "my-queue", nil, "", 20, nil); err != nil {
		t.Fatalf("cannot push: %s", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("cannot commit: %s", err)
	}

	response, err := c.Fetch(ctx, []string{"my-queue"})
	if err != nil {
		t.Fatalf("cannot fetch: %s", err)
	}
	if response.Name != "my-task" {
		t.Fatalf("unexpected task: %+v", response)
	}

	tx = c.Tx(ctx)
	if err := tx.Push(ctx, "another-task", "my-queue", map[string]string{"dict": "payload"}, "", 0, nil); err != nil {
		t.Fatalf("cannot push: %s", err)
	}
	if err := tx.Ack(ctx, response.ID); err != nil {
		t.Fatalf("cannot ack: %s", err)
	}
	if err := tx.Push(ctx, "yet-another-task", "my-queue", "a string payload", "", 0, nil); err != nil {
		t.Fatalf("cannot push: %s", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("cannot commit: %s", err)
	}

	// There should be two tasks now.

	if response, err := c.Fetch(ctx, []string{"my-queue"}); err != nil {
		t.Fatalf("cannot fetch: %s", err)
	} else if response.Name != "another-task" {
		t.Fatalf("unexpected task: %+v", response)
	}

	if response, err := c.Fetch(ctx, []string{"my-queue"}); err != nil {
		t.Fatalf("cannot fetch: %s", err)
	} else if response.Name != "yet-another-task" {
		t.Fatalf("unexpected task: %+v", response)
	}
}

func RunServerAndClient(ctx context.Context, t testing.TB) masenkoclient.Client {
	t.Helper()

	serverAddr := RunServer(ctx, t)

	for i := 0; ; i++ {
		c, err := masenkoclient.Dial(serverAddr)
		if err != nil {
			if i > 200 {
				t.Fatalf("cannot connect: %s", err)
			}
			time.Sleep(10 * time.Millisecond)
		} else {
			return c
		}
	}
}

func RunServer(ctx context.Context, t testing.TB) string {
	t.Helper()

	conf := masenko.ServerConfiguration{
		Heartbeat:        5 * time.Second,
		MaxWALSize:       1e8,
		StoreDir:         tempdir(t),
		ListenTCP:        "localhost:13456",
		ListenPrometheus: "localhost:12002",
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
