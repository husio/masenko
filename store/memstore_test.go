package store

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/husio/masenko/store/wal"
)

func TestDelayedBlockedTaskIsUnblocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dir := tempdir(t)

	s, err := OpenMemStore(dir, 1e6, testlog(t))
	if err != nil {
		t.Fatalf("cannot open store with an empty directory: %s", err)
	}
	defer s.Close()

	first, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 0})
	if err != nil {
		t.Fatalf("cannot push first task: %s", err)
	}
	future := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	if _, err := s.Push(ctx, Task{Name: "second", Queue: "default", BlockedBy: []uint32{first}, ExecuteAt: &future}); err != nil {
		t.Fatalf("cannot push second task: %s", err)
	}

	if task, err := s.Pull(ctx, []string{"default"}); err != nil {
		t.Fatalf("cannot pull a task: %s", err)
	} else if task.Name != "first" {
		t.Fatalf("wanted first task, got %+v", task)
	} else if err := s.Acknowledge(ctx, task.ID, true); err != nil {
		t.Fatalf("cannot ack a task: %s", err)
	}

	shortCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if task, err := s.Pull(shortCtx, []string{"default"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v, %+v", err, task)
	}

	queue := s.namedQueue("default")
	if queue.ready.Len() != 0 {
		t.Errorf("expected ready tasks queue to be 0 length, got %d", queue.ready.Len())
	}
	if queue.delayed.Len() != 1 {
		t.Errorf("expected delayed tasks queue to be 1 length, got %d", queue.delayed.Len())
	}
}

func TestOpenStore(t *testing.T) {
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
		"a blocked task scheduled": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				id, err := s.Push(ctx, Task{Name: "first", Queue: "default"})
				if err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "second", Queue: "default", BlockedBy: []uint32{id}}); err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}
			},
		},
		"a nested blocking task succeeded": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				first, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 0})
				if err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				second, err := s.Push(ctx, Task{Name: "second", Queue: "default", BlockedBy: []uint32{first}})
				if err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}
				third, err := s.Push(ctx, Task{Name: "third", Queue: "default", BlockedBy: []uint32{first, second}})
				if err != nil {
					t.Fatalf("cannot push third task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "fourth", Queue: "default", BlockedBy: []uint32{third}}); err != nil {
					t.Fatalf("cannot push fourth task: %s", err)
				}
				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if err := s.Acknowledge(ctx, task.ID, true); err != nil {
					t.Fatalf("cannot ack a task: %s", err)
				}
			},
		},
		"another nested blocking task succeeded": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				first, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 0})
				if err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				second, err := s.Push(ctx, Task{Name: "second", Queue: "default"})
				if err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}
				third, err := s.Push(ctx, Task{Name: "third", Queue: "default", BlockedBy: []uint32{second}})
				if err != nil {
					t.Fatalf("cannot push third task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "fourth", Queue: "default", BlockedBy: []uint32{third, first}}); err != nil {
					t.Fatalf("cannot push fourth task: %s", err)
				}
				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if err := s.Acknowledge(ctx, task.ID, true); err != nil {
					t.Fatalf("cannot ack a task: %s", err)
				}
			},
		},
		"a blocking task succeeded": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				first, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 10})
				if err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "second", Queue: "default", BlockedBy: []uint32{first}}); err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "third", Queue: "default", BlockedBy: []uint32{first}}); err != nil {
					t.Fatalf("cannot push third task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "fourth", Queue: "default", BlockedBy: []uint32{first}}); err != nil {
					t.Fatalf("cannot push fourth task: %s", err)
				}

				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if err := s.Acknowledge(ctx, task.ID, true); err != nil {
					t.Fatalf("cannot ack a task: %s", err)
				}
			},
		},
		"a blocking task failed": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				id, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 0})
				if err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "second", Queue: "default", BlockedBy: []uint32{id}}); err != nil {
					t.Fatalf("cannot push second task: %s", err)
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
		"a nested blocking task failed": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				first, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 0})
				if err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				second, err := s.Push(ctx, Task{Name: "second", Queue: "default", BlockedBy: []uint32{first}})
				if err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}
				third, err := s.Push(ctx, Task{Name: "third", Queue: "default", BlockedBy: []uint32{first, second}})
				if err != nil {
					t.Fatalf("cannot push third task: %s", err)
				}
				if _, err := s.Push(ctx, Task{Name: "fourth", Queue: "default", BlockedBy: []uint32{third}}); err != nil {
					t.Fatalf("cannot push fourth task: %s", err)
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
		"a delayed blocked task is unblocked": {
			Ops: func(ctx context.Context, t testing.TB, s *MemStore) {
				first, err := s.Push(ctx, Task{Name: "first", Queue: "default", Retry: 0})
				if err != nil {
					t.Fatalf("cannot push first task: %s", err)
				}
				future := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
				if _, err := s.Push(ctx, Task{Name: "second", Queue: "default", BlockedBy: []uint32{first}, ExecuteAt: &future}); err != nil {
					t.Fatalf("cannot push second task: %s", err)
				}

				if task, err := s.Pull(ctx, []string{"default"}); err != nil {
					t.Fatalf("cannot pull a task: %s", err)
				} else if task.Name != "first" {
					t.Fatalf("wanted first task, got %+v", task)
				} else if err := s.Acknowledge(ctx, task.ID, true); err != nil {
					t.Fatalf("cannot ack a task: %s", err)
				}
			},
		},
	}

	for testName, tc := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			dir := tempdir(t)

			original, err := OpenMemStore(dir, 1e6, testlog(t))
			if err != nil {
				t.Fatalf("cannot open store with an empty directory: %s", err)
			}
			defer original.Close()
			tc.Ops(ctx, t, original)

			t.Run("rebuild", func(t *testing.T) {
				rebuild, err := OpenMemStore(dir, 1e6, testlog(t))
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
				rebuild, err := OpenMemStore(dir, 1e6, testlog(t))
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
