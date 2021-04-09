package store

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testQueuePush(t *testing.T, newQueue func() Queue) {
	cases := map[string]struct {
		task    Task
		wantID  uint32
		wantErr error
	}{
		"only a task name provided": {
			task: Task{
				Name: "a-task",
			},
			wantID: 1,
		},
		"no task data provided": {
			task:    Task{},
			wantErr: ErrInvalid,
		},
	}

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for testName, tc := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(baseCtx)
			defer cancel()

			q := newQueue()

			id, err := q.Push(ctx, tc.task)
			if id != tc.wantID {
				t.Errorf("want %d ID, got %d", tc.wantID, id)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("want %q error, got %q", tc.wantErr, err)
			}
		})
	}
}

func testQueuePull(t *testing.T, newQueue func() Queue) {
	cases := map[string]struct {
		tasks      []Task
		pullQueues []string
		wantTask   Task
		wantErr    error
	}{
		"pull timeout": {
			tasks:      []Task{},
			pullQueues: []string{"default"},
			wantErr:    context.DeadlineExceeded,
		},
		"pull without giving a queue name": {
			tasks: []Task{
				{Name: "first", Queue: "default"},
			},
			pullQueues: nil,
			wantErr:    ErrNotFound,
		},
		"pull a single task": {
			tasks: []Task{
				{Name: "first", Queue: "default"},
			},
			//pullQueues: []string{"high-prio", "default"},
			pullQueues: []string{"default"},
			wantTask:   Task{ID: 1, Name: "first", Queue: "default", Payload: []byte{}},
		},
	}

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for testName, tc := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(baseCtx, time.Second)
			defer cancel()

			q := newQueue()
			for _, task := range tc.tasks {
				if _, err := q.Push(ctx, task); err != nil {
					t.Fatalf("push: %s", err)
				}
			}

			task, err := q.Pull(ctx, tc.pullQueues)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %q error, got %q", tc.wantErr, err)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(task, &tc.wantTask) {
				t.Fatalf("unexpected task result\nwant %+v\n got %+v", &tc.wantTask, task)
			}
		})
	}
}

func benchmarkPush(b *testing.B, q Queue) {
	b.Skip()
}

func benchmarkPull(b *testing.B, q Queue) {
	b.Helper()

	cases := []struct {
		workers int
	}{
		{workers: 1},
		{workers: 10},
		{workers: 50},
	}

	for _, cs := range cases {
		b.Run(fmt.Sprintf("%d workers, %d tasks", cs.workers, b.N), func(b *testing.B) {
			benchmarkPullConcurrent(b, q, cs.workers, b.N)
		})
	}
}

func benchmarkPullConcurrent(b *testing.B, q Queue, nworkers, ntasks int) {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := Task{
		Queue: "myqueue",
		Name:  "a-task",
	}
	for n := 0; n < ntasks; n++ {
		if _, err := q.Push(ctx, task); err != nil {
			b.Fatalf("push: %s", err)
		}
	}

	var consumed uint64

	start := make(chan struct{})

	var wg sync.WaitGroup
	for n := 0; n < nworkers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start

			for {
				res, err := q.Pull(ctx, []string{task.Queue})
				if errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					panic(err)
				}
				if err := q.Acknowledge(ctx, res.ID, true); err != nil {
					panic(err)
				}
				if total := atomic.AddUint64(&consumed, 1); total >= uint64(ntasks) {
					cancel()
					return
				}
			}
		}()
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()

	if total := atomic.LoadUint64(&consumed); total != uint64(ntasks) {
		b.Errorf("want %d consumed, got %d", ntasks, total)
	}
}

type counter struct {
	mu     sync.Mutex
	queues map[string]int64
}

func (c *counter) IncrQueue(queueName, kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queues == nil {
		c.queues = make(map[string]int64)
	}
	c.queues[queueName+":"+kind]++
}
func (c *counter) DecrQueue(queueName, kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queues == nil {
		c.queues = make(map[string]int64)
	}
	c.queues[queueName+":"+kind]--
}

func (c *counter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues = make(map[string]int64)
}

func (c *counter) SetQueueSize(queueName, kind string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queues == nil {
		c.queues = make(map[string]int64)
	}
	c.queues[queueName+":"+kind] = int64(n)
}

func (c *counter) Value(queueName, kind string) int64 {
	if c.queues == nil {
		return 0
	}
	return c.queues[queueName+":"+kind]
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
