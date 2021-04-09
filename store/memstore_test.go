package store

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/husio/masenko/store/wal"
)

func TestMemStoreQueueMetric(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	now := time.Now()
	nowPlus := func(d time.Duration) *time.Time {
		t := now.Add(d)
		return &t
	}

	dir := tempdir(t)

	var metrics counter
	store, err := OpenMemStore(dir, 1e6, testlog(t), &metrics)
	if err != nil {
		t.Fatalf("cannot open store with an empty directory: %s", err)
	}
	store.now = func() time.Time { return now }
	defer store.Close()

	if _, err := store.Push(ctx, Task{
		Name:      "one",
		Queue:     "a",
		Retry:     5,
		Deadqueue: "dead",
	}); err != nil {
		t.Fatalf("push: %s", err)
	}
	assertCounters(t, &metrics, "a", 1, 0, 0)

	if _, err := store.Push(ctx, Task{
		Name:      "two",
		Queue:     "a",
		ExecuteAt: nowPlus(-time.Hour),
		Retry:     0,
		Deadqueue: "dead",
	}); err != nil {
		t.Fatalf("push: %s", err)
	}
	assertCounters(t, &metrics, "a", 1, 1, 0)

	taskTwo, err := store.Pull(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("pull: %s", err)
	}
	if taskTwo.Name != "two" {
		t.Fatalf("task two has execute at in the past, so it should be returned first, got %+v", taskTwo)
	}
	assertCounters(t, &metrics, "a", 1, 0, 1)

	taskOne, err := store.Pull(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("pull: %s", err)
	}
	assertCounters(t, &metrics, "a", 0, 0, 2)

	if err := store.Acknowledge(ctx, taskOne.ID, false); err != nil {
		t.Fatalf("nack one: %s", err)
	}
	assertCounters(t, &metrics, "a", 0, 1, 1)
	assertCounters(t, &metrics, "dead", 0, 0, 0)

	if err := store.Acknowledge(ctx, taskTwo.ID, false); err != nil {
		t.Fatalf("nack two: %s", err)
	}
	assertCounters(t, &metrics, "a", 0, 1, 0)
	assertCounters(t, &metrics, "dead", 1, 0, 0)

	// Move the clock to the future, because NACK caused task one to be
	// scheduled for returning in the future.
	now = now.Add(time.Hour)

	taskOne, err = store.Pull(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("pull: %s", err)
	}
	if taskOne.Name != "one" {
		t.Fatalf("want task one, got %+v", taskOne)
	}
	assertCounters(t, &metrics, "a", 0, 0, 1)
	assertCounters(t, &metrics, "dead", 1, 0, 0)
	if err := store.Acknowledge(ctx, taskOne.ID, true); err != nil {
		t.Fatalf("ack one: %s", err)
	}
	assertCounters(t, &metrics, "a", 0, 0, 0)
	assertCounters(t, &metrics, "dead", 1, 0, 0)
}

func assertCounters(t testing.TB, metrics *counter, queueName string, ready, delayed, toack int64) {
	t.Helper()

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if got := metrics.Value(queueName, "ready"); got != ready {
		t.Errorf("want %s:ready to have %d, got %d", queueName, ready, got)
	}
	if got := metrics.Value(queueName, "delayed"); got != delayed {
		t.Errorf("want %s:delayed to have %d, got %d", queueName, delayed, got)
	}
	if got := metrics.Value(queueName, "toack"); got != toack {
		t.Errorf("want %s:toack to have %d, got %d", queueName, toack, got)
	}
}

func TestMemStoreQueueMetricInTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	now := time.Now()
	nowPlus := func(d time.Duration) *time.Time {
		t := now.Add(d)
		return &t
	}

	dir := tempdir(t)

	var metrics counter
	store, err := OpenMemStore(dir, 1e6, testlog(t), &metrics)
	if err != nil {
		t.Fatalf("cannot open store with an empty directory: %s", err)
	}
	store.now = func() time.Time { return now }
	defer store.Close()

	tx := store.Begin(ctx)
	if _, err := tx.Push(ctx, Task{Name: "one", Queue: "q"}); err != nil {
		t.Fatalf("push: %s", err)
	}
	if _, err := tx.Push(ctx, Task{Name: "two", Queue: "q", ExecuteAt: nowPlus(-time.Minute)}); err != nil {
		t.Fatalf("push: %s", err)
	}

	// Transaction was not yet commited.
	assertCounters(t, &metrics, "q", 0, 0, 0)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %s", err)
	}

	assertCounters(t, &metrics, "q", 1, 1, 0)

	_, err = store.Pull(ctx, []string{"q"})
	if err != nil {
		t.Fatalf("pull: %s", err)
	}
	taskOne, err := store.Pull(ctx, []string{"q"})
	if err != nil {
		t.Fatalf("pull: %s", err)
	}

	assertCounters(t, &metrics, "q", 0, 0, 2)

	tx = store.Begin(ctx)
	if _, err := tx.Push(ctx, Task{Name: "one", Queue: "q"}); err != nil {
		t.Fatalf("push: %s", err)
	}
	if err := tx.Acknowledge(ctx, taskOne.ID, true); err != nil {
		t.Fatalf("ack task one: %s", err)
	}

	// Transaction was not yet commited.
	assertCounters(t, &metrics, "q", 0, 0, 2)

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit second transaction: %s", err)
	}
	assertCounters(t, &metrics, "q", 1, 0, 1)
}

func TestMemStoreQueueMetricOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dir := tempdir(t)
	var metrics counter

	(func() {
		future := time.Now().Add(time.Hour)
		store, err := OpenMemStore(dir, 1e6, testlog(t), &metrics)
		if err != nil {
			t.Fatalf("cannot open store with an empty directory: %s", err)
		}
		defer store.Close()

		if _, err := store.Push(ctx, Task{Name: "one", Queue: "a"}); err != nil {
			t.Fatalf("push: %s", err)
		}
		if _, err := store.Push(ctx, Task{Name: "two", Queue: "a"}); err != nil {
			t.Fatalf("push: %s", err)
		}
		if _, err := store.Push(ctx, Task{Name: "three", Queue: "b"}); err != nil {
			t.Fatalf("push: %s", err)
		}
		if _, err := store.Push(ctx, Task{Name: "four", Queue: "b", ExecuteAt: &future}); err != nil {
			t.Fatalf("push: %s", err)
		}
		if _, err := store.Pull(ctx, []string{"a"}); err != nil {
			t.Fatalf("pull: %s", err)
		}
		assertCounters(t, &metrics, "a", 1, 0, 1)
		assertCounters(t, &metrics, "b", 1, 1, 0)
	}())

	t.Run("opening using existing metrics store", func(t *testing.T) {
		store, err := OpenMemStore(dir, 1e6, testlog(t), &metrics)
		if err != nil {
			t.Fatalf("cannot open an existing store: %s", err)
		}
		_ = store.Close()

		assertCounters(t, &metrics, "a", 2, 0, 0)
		assertCounters(t, &metrics, "b", 1, 1, 0)
	})

	t.Run("opening using existing an empty metrics store", func(t *testing.T) {
		var fresh counter
		store, err := OpenMemStore(dir, 1e6, testlog(t), &fresh)
		if err != nil {
			t.Fatalf("cannot open an existing store: %s", err)
		}
		_ = store.Close()

		assertCounters(t, &fresh, "a", 2, 0, 0)
		assertCounters(t, &fresh, "b", 1, 1, 0)
	})
}

func TestOpenMemStore(t *testing.T) {
	cases := map[string]struct {
		Ops func(context.Context, testing.TB, *MemStore)
	}{
		"empty state": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {},
		},
		"two tasks scheduled one consumed": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				if _, err := s.Push(ctx, Task{Name: "first", Queue: "default"}); err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "second", Queue: "default"}); err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}
				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if err := s.Acknowledge(ctx, task.ID, true); err != nil {
					t.Fatalf("cannot ack a task: %s", err)
				}
			},
		},
		"two tasks scheduled one false acked": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				if _, err := s.Push(ctx, Task{Name: "first", Queue: "default"}); err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "second", Queue: "default"}); err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}
				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if err := s.Acknowledge(ctx, task.ID, false); err != nil {
					t.Fatalf("cannot ack a task: %s", err)
				}
			},
		},
		"a delayed task scheduled": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				future := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
				if _, err := s.Push(ctx, Task{
					Name:      "first",
					Queue:     "default",
					ExecuteAt: &future,
				}); err != nil {
					t.Fatalf("cannot push delayed task: %s", err)
				}
			},
		},
		"a task failed once": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				if _, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 10}); err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if task.Name != "first" {
					t.Fatalf("wanted first task, got %+v", task)
				} else if err := s.Acknowledge(ctx, task.ID, false); err != nil {
					t.Fatalf("cannot ack a task: %s", err)
				}
			},
		},
		"a task is moved to dead letter queue": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				if _, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 1}); err != nil {
					t.Fatalf("cannot push task: %s", err)
				}
				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if err := s.Acknowledge(ctx, task.ID, false); err != nil {
					t.Fatalf("cannot acknowledge task: %s", err)
				}
				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if err := s.Acknowledge(ctx, task.ID, false); err != nil {
					t.Fatalf("cannot acknowledge task: %s", err)
				}
			},
		},
	}

	for testName, tc := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			dir := tempdir(t)

			var metrics counter
			original, err := OpenMemStore(dir, 1e6, testlog(t), &metrics)
			if err != nil {
				t.Fatalf("cannot open store with an empty directory: %s", err)
			}
			defer original.Close()
			tc.Ops(ctx, t, original)

			t.Run("rebuild", func(t *testing.T) {
				rebuild, err := OpenMemStore(dir, 1e6, testlog(t), &metrics)
				if err != nil {
					t.Fatalf("cannot open store with an a WAL file present: %s", err)
				}
				defer rebuild.Close()

				ensureStoreEqual(t, original, rebuild)
			})

			t.Run("vacuum", func(t *testing.T) {
				if err := original.rebuildWAL(); err != nil {
					t.Fatalf("rebuild WAL: %s", err)
				}
				rebuild, err := OpenMemStore(dir, 1e6, testlog(t), &metrics)
				if err != nil {
					t.Fatalf("cannot open store with a rebuild WAL: %s", err)
				}
				defer rebuild.Close()

				readWal(t, rebuild)

				ensureStoreEqual(t, original, rebuild)
			})
		})
	}
}

func ensureStoreEqual(t testing.TB, original, rebuild *MemStore) {
	t.Helper()

	if o, r := original.nextTaskID, rebuild.nextTaskID; o != r {
		t.Errorf("original next task id is %d, rebuild %d", o, r)
	}

	if o, r := len(original.toack), len(rebuild.toack); o != r {
		t.Fatalf("original store has %d messages to ack, rebuild has %d", o, r)
	}

	if o, r := len(original.queues), len(rebuild.queues); o != r {
		t.Errorf("original store has %d queues %q, rebuild has %d %q",
			o, queueNames(original.queues), r, queueNames(rebuild.queues))
	}
	for _, o := range original.queues {
		found := false
		var r *namedQueue
		for _, r = range rebuild.queues {
			if r.name == o.name {
				found = true

				break
			}
		}
		if !found {
			t.Errorf("original store has %q queue with %d, %d, %d tasks, rebuild does not",
				o.name, o.ntoack, o.ready.Len(), o.delayed.Len())
			continue
		}
		assertTaskListsEqual(t, o.ready, r.ready)
		assertTaskListsEqual(t, o.delayed, r.delayed)
	}
}

func queueNames(queues []*namedQueue) []string {
	var names []string
	for _, q := range queues {
		names = append(names, q.name)
	}
	return names
}

func TestMemStoreWALVacuum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := tempdir(t)

	const (
		tasksPushed  = 10
		walEntrySize = 41
	)
	var metrics counter
	store, err := OpenMemStore(dir, walEntrySize*tasksPushed, testlog(t), &metrics)
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
	store, err = OpenMemStore(dir, walEntrySize*tasksPushed, testlog(t), &metrics)
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

func BenchmarkMemStoreSwitchWAL(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var metrics counter
	store, err := OpenMemStore(tempdir(b), 1e6, testlog(b), &metrics)
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
	var metrics counter
	store, err := OpenMemStore(tempdir(b), 1e6, testlog(b), &metrics)
	if err != nil {
		b.Fatalf("new mem store: %s", err)
	}
	defer store.Close()

	store.vacuumSize = 0 // Disable vacuum.

	benchmarkPush(b, store)
}

func BenchmarkMemStorePull(b *testing.B) {
	var metrics counter
	store, err := OpenMemStore(tempdir(b), 1e6, testlog(b), &metrics)
	if err != nil {
		b.Fatalf("new mem store: %s", err)
	}
	defer store.Close()

	store.vacuumSize = 0 // Disable vacuum.

	benchmarkPull(b, store)
}

func assertTaskListsEqual(t testing.TB, a, b *list.List) {
	for i, ae, be := 0, a.Front(), b.Front(); ; i, ae, be = i+1, ae.Next(), be.Next() {

		if ae == nil && be != nil {
			t.Logf("%d: second: %+v", i, be.Value)
			t.Fatalf("first list is shorter than the second one %d < %d", a.Len(), b.Len())
		}
		if be == nil && ae != nil {
			t.Logf("%d:  first: %+v", i, ae.Value)
			t.Fatalf("second list is shorter than the first one %d > %d", a.Len(), b.Len())
		}
		if ae == nil && be == nil {
			return
		}

		t.Logf("%d:  first: %+v", i, ae.Value)
		t.Logf("%d: second: %+v", i, be.Value)

		at := ae.Value.(*Task)
		bt := be.Value.(*Task)
		if !reflect.DeepEqual(at, bt) {
			t.Errorf("task difference at position %d", i)
		}
	}
}

func readWal(t testing.TB, s *MemStore) {
	fd, err := os.Open(s.walFd.(*os.File).Name())
	if err != nil {
		t.Fatalf("cannot open wal file: %s", err)
	}
	defer fd.Close()

	nx := wal.NewOpNexter(fd, 1e6)
	var entryNo uint
	for {
		ops, err := nx.Next()
		switch {
		case err == nil:
			// All good.
		case errors.Is(err, io.EOF):
			return
		default:
			t.Fatalf("read WAL entry: %s", err)
		}

		for _, op := range ops {
			raw, err := json.Marshal(op)
			if err != nil {
				t.Fatalf("JSON marshal WAL operation: %s", err)
			}
			t.Logf("WAL entry %2d: %14T %s", entryNo, op, string(raw))
			entryNo++
		}
	}
}
