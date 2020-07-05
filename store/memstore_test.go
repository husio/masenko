package store

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestOpenStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	dir := tempdir(t)

	store, err := OpenMemStore(dir, 1e6, testlog(t))
	if err != nil {
		t.Fatalf("cannot open store with an empty directory: %s", err)
	}
	defer store.Close()

	if _, err := store.Push(ctx, Task{Name: "first", Queue: "default"}); err != nil {
		t.Fatalf("cannot push first task: %s", err)
	}
	if _, err := store.Push(ctx, Task{Name: "second", Queue: "default"}); err != nil {
		t.Fatalf("cannot push second task: %s", err)
	}
	store.Close()

	store, err = OpenMemStore(dir, 1e6, testlog(t))
	if err != nil {
		t.Fatalf("cannot open store with an a WAL file present: %s", err)
	}
	defer store.Close()

	if task, err := store.Pull(ctx, []string{"default"}); err != nil {
		t.Fatalf("expected first task, got error %+v", err)
	} else if task.Name != "first" {
		t.Fatalf("expected first task, got %+v", task)
	}

	if task, err := store.Pull(ctx, []string{"default"}); err != nil {
		t.Fatalf("expected second task, got error %+v", err)
	} else if task.Name != "second" {
		t.Fatalf("expected second task, got %+v", task)
	}
	store.Close()
}

func TestWALVacuum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := tempdir(t)

	const (
		tasksPushed  = 10
		walEntrySize = 41
	)
	store, err := OpenMemStore(dir, walEntrySize*tasksPushed, testlog(t))
	if err != nil {
		t.Fatalf("cannot open store with an empty directory: %s", err)
	}
	defer store.Close()

	var taskIDs []uint32
	for i := int64(0); i < tasksPushed; i++ {
		if _, err := store.Push(ctx, Task{Name: "a-task", Queue: "myqueue"}); err != nil {
			t.Fatalf("cannot push: %s", err)
		}
		// To discover the entry size in the WAL file, uncomment the below line.
		// t.Log("wal size:", store.walSize)
		task, err := store.Pull(ctx, []string{"myqueue"})
		if err != nil {
			t.Fatalf("cannot pull: %s", err)
		}
		taskIDs = append(taskIDs, task.ID)
	}

	// There are more than enough entries to trigger vacuum. Vacuum is
	// triggered on commit or ack, so no extra wal file yet.
	assertWALCount(t, dir, 1)

	id := taskIDs[0]
	taskIDs = taskIDs[1:]
	if err := store.Acknowledge(ctx, id, true); err != nil {
		t.Fatalf("cannot ack: %s", err)
	}
	// A single ack must trigger WAL rebuild.
	assertWALCount(t, dir, 2)

	if err := store.Close(); err != nil {
		t.Fatalf("store close: %s", err)
	}

	// Ensure that opening a new store will recreate the state from the
	// log. A single task was acknowledge, all others must be there.
	store, err = OpenMemStore(dir, walEntrySize*tasksPushed, testlog(t))
	if err != nil {
		t.Fatalf("cannot open store with a non empty directory: %s", err)
	}
	defer store.Close()
	var recreated int
	pullCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
countTasks:
	for {
		switch _, err := store.Pull(pullCtx, []string{"myqueue"}); {
		case err == nil:
			recreated++
		case errors.Is(err, context.DeadlineExceeded):
			// No more tasks.
			break countTasks
		default:
			t.Fatalf("pull task: %s", err)
		}
	}
	// One was ACKed.
	if recreated != tasksPushed-1 {
		t.Errorf("want %d recreated, got %d", tasksPushed-1, recreated)
	}

	for _, id := range taskIDs {
		if err := store.Acknowledge(ctx, id, true); err != nil {
			t.Fatalf("cannot ack: %s", err)
		}
	}
	assertWALCount(t, dir, 3)

}

func assertWALCount(t testing.TB, walDir string, wantFiles uint) {
	t.Helper()

	// Vacuum is a background process. Give it time to kick in and create a
	// new WAL file.
	time.Sleep(150 * time.Millisecond)

	files, err := ioutil.ReadDir(walDir)
	if err != nil {
		t.Fatalf("cannot read WAL dir: %s", err)
	}

	if uint(len(files)) != wantFiles {
		for i, f := range files {
			t.Logf("WAL file %d: %s", i, f.Name())
		}
		t.Fatalf("expected %d WAL files, found %d", wantFiles, len(files))
	}
}

func BenchmarkSwitchWAL(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := OpenMemStore(tempdir(b), 1e6, testlog(b))
	if err != nil {
		b.Fatalf("new mem store: %s", err)
	}
	defer store.Close()

	store.vacuumSize = 0 // Disable vacuum.

	task := Task{
		ID:      123456789,
		Queue:   "my-queue",
		Name:    "a-task-that-i-want-to-execute",
		Payload: []byte(`1234567890123456789012345678901234567890`),
		Retry:   20,
	}
	for n := 0; n < b.N; n++ {
		if _, err := store.Push(ctx, task); err != nil {
			b.Fatalf("push: %s", err)
		}
	}

	b.ResetTimer()

	if err := store.rebuildWAL(); err != nil {
		b.Fatalf("rebuild WAL: %s", err)
	}
}

func BenchmarkMemStorePush(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := OpenMemStore(tempdir(b), 1e6, testlog(b))
	if err != nil {
		b.Fatalf("new mem store: %s", err)
	}
	defer store.Close()

	store.vacuumSize = 0 // Disable vacuum.

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			task, err := store.Pull(ctx, []string{"myqueue"})
			if err != nil {
				panic(err)
			}
			if err := store.Acknowledge(ctx, task.ID, true); err != nil {
				panic(err)
			}
		}
	}()

	task := Task{Queue: "myqueue", Name: "a-task"}
	for i := 0; i < b.N; i++ {
		if _, err := store.Push(ctx, task); err != nil {
			b.Fatalf("push task: %s", err)
		}
	}

	wg.Wait()
}

func BenchmarkMemStorePull(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := OpenMemStore(tempdir(b), 1e6, testlog(b))
	if err != nil {
		b.Fatalf("new mem store: %s", err)
	}
	defer store.Close()

	store.vacuumSize = 0 // Disable vacuum.

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			if _, err := store.Push(ctx, Task{Queue: "myqueue", Name: "a-task"}); err != nil {
				panic("push task: " + err.Error())
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		task, err := store.Pull(ctx, []string{"myqueue"})
		if err != nil {
			b.Fatalf("cannot pull: %s", err)
		}
		if err := store.Acknowledge(ctx, task.ID, true); err != nil {
			b.Fatalf("cannot ack: %s", err)
		}
	}

	wg.Wait()
}

func tempdir(t testing.TB) string {
	t.Helper()

	dir, err := ioutil.TempDir(os.TempDir(), "masenko-tests-*")
	if err != nil {
		t.Fatalf("cannot create temporary directory: %s", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}

func testlog(t testing.TB) *log.Logger {
	t.Helper()

	if !testing.Verbose() {
		return log.New(ioutil.Discard, "", 0)
	}

	return log.New(ioutil.Discard, t.Name(), log.Lshortfile)
}
