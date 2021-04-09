package store

import (
	"context"
	"path"
	"testing"
)

func TestSQLiteQueuePush(t *testing.T) {
	testQueuePush(t, func() Queue { return newSQLiteQueue(t) })
}

func TestSQLiteQueuePull(t *testing.T) {
	testQueuePull(t, func() Queue { return newSQLiteQueue(t) })
}

func newSQLiteQueue(t *testing.T) *SQLiteQueue {
	t.Helper()
	q, err := OpenSQLiteQueue(t.TempDir() + "/db.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestOpen(t *testing.T) {
	q, err := OpenSQLiteQueue(t.TempDir() + "/db.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduled := Task{
		Name:  "a-task",
		Queue: "default",
	}
	tid, err := q.Push(ctx, scheduled)
	if err != nil {
		t.Fatalf("push: %s", err)
	}

	pulled, err := q.Pull(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("pull: %s", err)
	}
	if pulled.ID != tid {
		t.Errorf("id: expected %d, got %d", tid, pulled.ID)
	}
	if pulled.Name != scheduled.Name {
		t.Errorf("name: want %q, got %q", scheduled.Name, pulled.Name)
	}
	if pulled.ID != 1 {
		t.Errorf("id: want 1, got %d", pulled.ID)
	}

	if err := q.Acknowledge(ctx, pulled.ID, true); err != nil {
		t.Errorf("ack: %s", err)
	}
}

func TestSQLiteStorePushPull(t *testing.T) {
	store, err := OpenSQLiteQueue(path.Join(tempdir(t), "db.sqlite"))
	if err != nil {
		t.Fatalf("new sqlite store: %s", err)
	}
	defer store.Close()

	task := Task{
		Name:      "a-task",
		Queue:     "my-queue",
		Payload:   nil,
		Deadqueue: "",
		ExecuteAt: nil,
		Retry:     4,
	}
	for i := 0; i < 10; i++ {
		tid, err := store.Push(context.Background(), task)
		if err != nil {
			t.Fatalf("push: %s", err)
		}
		res, err := store.Pull(context.Background(), []string{task.Queue})
		if err != nil {
			t.Fatalf("pull %d: %s", i, err)
		}
		if res.ID != tid {
			t.Fatalf("want %d, got %d", tid, res.ID)
		}
	}
}

func BenchmarkSQLiteStorePush(b *testing.B) {
	store, err := OpenSQLiteQueue(path.Join(tempdir(b), "db.sqlite"))
	if err != nil {
		b.Fatalf("new sqlite store: %s", err)
	}
	defer store.Close()
	benchmarkPush(b, store)
}

func BenchmarkSQLiteStorePull(b *testing.B) {
	store, err := OpenSQLiteQueue(path.Join(tempdir(b), "db.sqlite"))
	if err != nil {
		b.Fatalf("new sqlite store: %s", err)
	}
	defer store.Close()
	benchmarkPull(b, store)
}
