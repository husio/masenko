package store

import (
	"bufio"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/husio/masenko/store/wal"
)

// MemStore is a Queue implementation that keeps the state in memory. All
// operations are written to WAL file, so state recovery is possible at any
// moment.
type MemStore struct {
	now     func() time.Time
	walDir  string
	log     *log.Logger
	metrics MetricCounter

	mu         sync.Mutex
	rand       rand.Rand
	walSize    uint64
	walWr      *wal.OpAppender
	walFd      io.Closer
	vacuumSize uint64
	nextTaskID uint32
	queues     []*namedQueue
	toack      []*Task
}

// OpenMemStore returns a new MemStore instance initialized with WAL in given
// directory. If WAL files are found, the oldest (decided by file name) of the
// WAL files is used to populate the state.
func OpenMemStore(walDir string, vacuumSize uint64, logger *log.Logger, mc MetricCounter) (*MemStore, error) {
	// The current implementation does not sync the data after write. This
	// is safe if the Masenko process crash, because Masenko does not
	// buffer writes and system should make sure file buffer is flushed
	// fairly often.
	// If asynchronous (kernel) writes are not safe enough, using O_SYNC or
	// O_DSYNC modes here would ensure all writes are blocking until hard
	// drive write. This would also lower the WAL write performance.
	const walMode = 0 // O_SYNC or O_DSYNC

	fd, err := openLatestWALFile(walDir, walMode)
	if err != nil {
		return nil, fmt.Errorf("open WAL file: %w", err)
	}

	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}

	store := &MemStore{
		metrics:    mc,
		vacuumSize: vacuumSize,
		walDir:     walDir,
		log:        logger,
		walWr:      nil,
		walSize:    0,
		walFd:      fd,
		now:        func() time.Time { return time.Now().UTC() },
		nextTaskID: 1,
		toack:      make([]*Task, 0, 1024),
	}

	nx := wal.NewOpNexter(fd, walBufferSize)
	if err := store.readWAL(nx); err != nil {
		fd.Close()
		return nil, fmt.Errorf("read %q: %w", fd.Name(), err)
	}

	if n, err := fd.Seek(0, os.SEEK_END); err != nil {
		fd.Close()
		return nil, fmt.Errorf("wal seek: %w", err)
	} else {
		store.walSize = uint64(n)
	}

	store.walWr = wal.NewOpAppender(fd, walBufferSize)
	return store, nil
}

type MetricCounter interface {
	// IncrQueue increments the total amount of tasks in the queue with the
	// given name.
	IncrQueue(string, string)
	// DecrQueue decrements the total amount of tasks in the queue with the
	// given name.
	DecrQueue(string, string)
	// SetQueueSize sets the current queue size to given value.
	SetQueueSize(string, string, int)
}

// Size of the WAL write buffer. Should be big enough to acommodate all
// operations written within a single WAL append.
const walBufferSize = 1e6 // Around 1Mb.

// readWAL consumes the reader and apply all operations to the current state.
func (m *MemStore) readWAL(nx *wal.OpNexter) error {

	// Keep an index of all present tasks. This is needed because delete or
	// fail operations contains only the task ID. To delete a task, we must
	// know which queue it belongs to.
	// This index can be dropped once the rebuild process is complete.
	byid := make(map[uint32]*Task)

	for {
		operations, err := nx.Next()
		switch {
		case err == nil:
			// All good.
		case errors.Is(err, io.EOF):
			for _, q := range m.queues {
				m.metrics.SetQueueSize(q.name, "toack", 0)
				m.metrics.SetQueueSize(q.name, "ready", q.ready.Len())
				m.metrics.SetQueueSize(q.name, "delayed", q.delayed.Len())
				if q.Empty() {
					m.deleteMasenko(q.name)
				}
			}
			return nil
		default:
			return fmt.Errorf("read wal entry: %w", err)
		}

		for _, op := range operations {
			switch op := op.(type) {
			case *wal.OpNextID:
				m.nextTaskID = op.NextID
			case *wal.OpAdd:
				task := opAddToTask(op)
				byid[op.ID] = task
				if op.ID >= m.nextTaskID {
					m.nextTaskID = op.ID + 1
				}
				m.namedQueue(op.Queue).pushTask(task)
			case *wal.OpFail:
				task, ok := byid[op.ID]
				if !ok {
					return fmt.Errorf("fail missing task #%d", op.ID)
				}
				task.Failures++
				task.ExecuteAt = &op.ExecuteAt

				queue := m.namedQueue(task.Queue)
				for e := queue.ready.Front(); e != nil; e = e.Next() {
					t := e.Value.(*Task)
					if t.ID != task.ID {
						continue
					}
					queue.ready.Remove(e)
					break
				}
				for e := queue.delayed.Front(); e != nil; e = e.Next() {
					t := e.Value.(*Task)
					if t.ID != task.ID {
						continue
					}
					queue.delayed.Remove(e)
					break
				}
				m.namedQueue(task.Queue).pushTask(task) // Requeue delayed.
			case *wal.OpDelete:
				task, ok := byid[op.ID]
				if !ok {
					return fmt.Errorf("delete missing task #%d", op.ID)
				}
				queue := m.namedQueue(task.Queue)

				deleted := false
				for e := queue.ready.Front(); e != nil; e = e.Next() {
					t := e.Value.(*Task)
					if t.ID != op.ID {
						continue
					}
					queue.ready.Remove(e)
					deleted = true
					break
				}
				if !deleted {
					for e := queue.delayed.Front(); e != nil; e = e.Next() {
						t := e.Value.(*Task)
						if t.ID != op.ID {
							continue
						}
						queue.delayed.Remove(e)
						deleted = true
						break
					}
				}
				if !deleted {
					return fmt.Errorf("missing task %d", op.ID)
				}
				delete(byid, op.ID)
			default:
				return fmt.Errorf("unknown WAL operation kind: %T", op)
			}
		}

	}
}

// openLatestWALFile search in given directory for WAL files and returns the
// oldest of them.
func openLatestWALFile(walDir string, mode int) (*os.File, error) {
	infos, err := ioutil.ReadDir(walDir)
	if err != nil {
		return nil, fmt.Errorf("read WAL dir: %w", err)
	}
	var latestWal string
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		if !isWalFileName(info.Name()) {
			continue
		}

		if strings.Compare(latestWal, info.Name()) < 0 {
			latestWal = info.Name()
		}
	}

	if latestWal == "" {
		latestWal = fmt.Sprintf("masenko.%d.wal", time.Now().UnixNano())
	}

	// Just to be sure. This must never happen. Checking it is cheap.
	if !isWalFileName(latestWal) {
		return nil, fmt.Errorf("created a non WAL file name: %s", latestWal)
	}

	fd, err := os.OpenFile(path.Join(walDir, latestWal), os.O_RDWR|os.O_CREATE|mode, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return fd, nil
}

var isWalFileName = regexp.MustCompile(`masenko\.\d+\.wal`).MatchString

// namedQueue returns a taks queue with a given name. If not yet exist, a queue
// is created and initialized. Returned result is always safe to use.
func (m *MemStore) namedQueue(queueName string) *namedQueue {
	for _, q := range m.queues {
		if q.name == queueName {
			return q
		}
	}
	q := &namedQueue{
		name:    queueName,
		ready:   list.New(),
		delayed: list.New(),
	}
	m.queues = append(m.queues, q)
	return q
}

// deleteMasenko removes a task queue from the list of available queues. This
// function does not test if the queue is empty.
func (m *MemStore) deleteMasenko(queueName string) {
	for i, q := range m.queues {
		if q.name != queueName {
			continue
		}
		// Order does not matter.
		m.queues[i] = m.queues[len(m.queues)-1]
		m.queues = m.queues[:len(m.queues)-1]
		return
	}
}

func (m *MemStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.walFd.Close()
}

func (m *MemStore) Stats() []QueueStat {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := make([]QueueStat, 0, len(m.queues))
	for _, tq := range m.queues {
		stats = append(stats, QueueStat{
			Name:    tq.name,
			Ready:   uint32(tq.ready.Len()),
			Delayed: uint32(tq.delayed.Len()),
			ToACK:   uint32(tq.ntoack),
		})
	}

	return stats
}

func (m *MemStore) Push(ctx context.Context, task Task) (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	task.ID = m.genTaskID()
	if n, err := m.walWr.Append(taskToOpAdd(&task)); err != nil {
		return 0, fmt.Errorf("wall append: %w", err)
	} else {
		m.walSize += uint64(n)
	}

	m.namedQueue(task.Queue).pushTask(&task)
	if task.ExecuteAt == nil {
		m.metrics.IncrQueue(task.Queue, "ready")
	} else {
		m.metrics.IncrQueue(task.Queue, "delayed")
	}
	return task.ID, nil
}

// taskByID returns the task with given ID. This is an extremply unperformant
// lookup algorithm.
func (m *MemStore) taskByID(id uint32) (*Task, bool) {
	for _, t := range m.toack {
		if t.ID == id {
			return t, true
		}
	}
	for _, queue := range m.queues {
		for e := queue.ready.Front(); e != nil; e = e.Next() {
			t := e.Value.(*Task)
			if t.ID == id {
				return t, true
			}
		}
		for e := queue.delayed.Front(); e != nil; e = e.Next() {
			t := e.Value.(*Task)
			if t.ID == id {
				return t, true
			}
		}
	}
	return nil, false
}

func (m *MemStore) walVacuum() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Another vacuum task might have been executed.
	if m.vacuumSize == 0 {
		return
	}
	if m.walSize <= m.vacuumSize {
		return
	}
	// Logging is the only way such error could be
	// reported.
	if err := m.rebuildWAL(); err != nil {
		m.log.Printf("Vacuum process failed: %s", err)
	}
}

func (m *MemStore) Pull(ctx context.Context, queues []string) (*Task, error) {
	for {
		m.mu.Lock()
		now := m.now()
		// We expect both queues and m.queues to be a small collection,
		// so 2x loop should not be expensive.
		for _, name := range queues {
			for _, tq := range m.queues {
				if tq.name != name {
					continue
				}
				if task, ok := tq.popTask(now); ok {
					if tq.Empty() {
						m.deleteMasenko(tq.name)
					}
					m.toack = append(m.toack, task)
					m.mu.Unlock()

					if task.ExecuteAt == nil {
						m.metrics.DecrQueue(tq.name, "ready")
					} else {
						m.metrics.DecrQueue(tq.name, "delayed")
					}
					m.metrics.IncrQueue(tq.name, "toack")
					return task, nil
				}
			}
		}
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
			// Busy waiting. This is simplicity over accuracy.
		}
	}
}

func (m *MemStore) Acknowledge(ctx context.Context, taskID uint32, ack bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, task := range m.toack {
		if task.ID != taskID {
			continue
		}

		// Order does not matter.
		m.toack[i] = m.toack[len(m.toack)-1]
		m.toack = m.toack[:len(m.toack)-1]
		m.namedQueue(task.Queue).ntoack--

		if ack {
			if n, err := m.walWr.Append(&wal.OpDelete{ID: task.ID}); err != nil {
				return fmt.Errorf("wal append: %w", err)
			} else {
				m.walSize += uint64(n)
			}
			if m.vacuumSize != 0 && m.walSize > m.vacuumSize {
				go m.walVacuum()
			}
			m.metrics.DecrQueue(task.Queue, "toack")
			return nil
		}

		task.Failures++
		if task.Failures <= task.Retry {
			// Scheduling a failed task involves setting a new
			// execution time. Original execution time (if set) can
			// be overwritten, because it is no longer needed. No
			// need to write the new time to the WAL file.
			//
			// The below delay gives around 10min delay after 4
			// failures,  2 hours after 9 failures, 36 hours after
			// 20 failures.
			//
			// To See the delay values, run the below code:
			//
			//   for i := 1; i < 20; i++ {
			//       delay := time.Duration(math.Pow(float64(i), 4)) * time.Second
			//       fmt.Printf("failures=%d  delay=%s\n", i, delay)
			//   }
			delay := time.Duration(math.Pow(float64(task.Failures), 4)) * time.Second
			executeAt := m.now().Add(delay).Truncate(time.Second)

			task.ExecuteAt = &executeAt
			n, err := m.walWr.Append(&wal.OpFail{ID: task.ID, ExecuteAt: executeAt})
			if err != nil {
				return fmt.Errorf("wal append: %w", err)
			}
			m.walSize += uint64(n)
			m.namedQueue(task.Queue).pushTask(task)
			m.metrics.IncrQueue(task.Queue, "delayed")
			m.metrics.DecrQueue(task.Queue, "toack")
			return nil
		}

		batch := []wal.Operation{
			&wal.OpDelete{ID: task.ID},
		}

		var deadTask *Task
		if task.Deadqueue != "" {
			deadTask = taskToDeadTask(task)
			batch = append(batch, taskToOpAdd(deadTask))
		}

		if m.namedQueue(task.Queue).Empty() {
			m.deleteMasenko(task.Queue)
		}

		if n, err := m.walWr.Append(batch...); err != nil {
			return fmt.Errorf("wal append: %w", err)
		} else {
			m.walSize += uint64(n)
		}

		if deadTask != nil {
			m.namedQueue(deadTask.Queue).pushTask(deadTask)
			m.metrics.IncrQueue(deadTask.Queue, "ready")
		}
		m.metrics.DecrQueue(task.Queue, "toack")

		return nil
	}
	return ErrNotFound
}

// genTaskID returns the next free ID that should be assigned to a newly
// created task. Calling this method always increments the counter.
func (m *MemStore) genTaskID() uint32 {
	if m.nextTaskID == math.MaxUint32 {
		m.nextTaskID = 1
	}
	id := m.nextTaskID
	m.nextTaskID++
	return id
}

// rebuildWAL rewrites the current state in WAL format to given file. All
// future WAL entries writes are made to the new file. Previous WAL file descriptor
// is closed can be deleted.
func (m *MemStore) rebuildWAL() error {
	// Until the file is finished, do not save it under a WAL file name.
	// This is because interrupting this process could produce a unfinished
	// but a valid WAL file and loose data when loading it.
	ipWalPath := path.Join(m.walDir, "masenko.inprogress.wal")
	fd, err := os.OpenFile(ipWalPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open inprogress WAL file: %w", err)
	}

	// Rebuiding new WAL can be buffered until the final write. We do not
	// have to keep it synced.
	bfd := bufio.NewWriter(fd)

	newWal := wal.NewOpAppender(bfd, walBufferSize)

	var (
		newSize  int
		writeErr error
		ops      = make([]wal.Operation, 257)
		written  = make(map[uint32]struct{})
	)

	var writeTask func(t *Task)
	writeTask = func(t *Task) {
		if writeErr != nil {
			return
		}
		if _, ok := written[t.ID]; ok {
			return
		}

		ops := ops[:1]
		ops[0] = taskToOpAdd(t)
		written[t.ID] = struct{}{}

		for i := uint8(0); i < t.Failures; i++ {
			ops = append(ops, &wal.OpFail{ID: t.ID, ExecuteAt: *t.ExecuteAt})
		}
		if n, err := newWal.Append(ops...); err != nil {
			writeErr = fmt.Errorf("append to WAL: %w", err)
			return
		} else {
			newSize += n
		}
	}

	for _, t := range m.toack {
		writeTask(t)
	}
	for _, q := range m.queues {
		for el := q.delayed.Front(); el != nil; el = el.Next() {
			writeTask(el.Value.(*Task))
		}
		for el := q.ready.Front(); el != nil; el = el.Next() {
			writeTask(el.Value.(*Task))
		}
	}

	if writeErr != nil {
		return writeErr
	}

	if n, err := newWal.Append(&wal.OpNextID{NextID: m.nextTaskID}); err != nil {
		writeErr = fmt.Errorf("append to WAL next ID: %w", err)
	} else {
		newSize += n
	}

	if err := bfd.Flush(); err != nil {
		return fmt.Errorf("flush new WAL: %w", err)
	}
	if err := fd.Close(); err != nil {
		return fmt.Errorf("close next WAL file: %w", err)
	}

	// Now that the new WAL is finished with data, we can rename it to a
	// valid WAL file name that will be discovered at startup.
	name := fmt.Sprintf("masenko.%d.wal", time.Now().UnixNano())
	// Just to be sure. This must never happen. Checking it is cheap.
	if !isWalFileName(name) {
		return fmt.Errorf("created a non wal name: %s", name)
	}
	walPath := path.Join(m.walDir, name)
	if err := os.Rename(ipWalPath, walPath); err != nil {
		return fmt.Errorf("move new WAL file: %w", err)
	}
	fd, err = os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open new WAL file: %w", err)
	}

	// Previous WAL file is closed so that it can be deleted. Failures can
	// be ignored because we no longer care about this file.
	_ = m.walFd.Close()

	m.walWr = wal.NewOpAppender(fd, walBufferSize)
	m.walSize = uint64(newSize)
	m.walFd = fd

	return nil
}

func maxUint(ns []uint32) uint32 {
	if len(ns) == 0 {
		return 0
	}
	max := ns[0]
	for _, n := range ns {
		if n > max {
			max = n
		}
	}
	return max
}

func (m *MemStore) Begin(ctx context.Context) Transaction {
	// Lock is acquired until returned transaction is finalized.
	m.mu.Lock()

	return &MemStoreTransaction{
		m:        m,
		ops:      make([]wal.Operation, 0, 8),
		finished: false,
	}
}

type MemStoreTransaction struct {
	m        *MemStore
	ops      []wal.Operation
	pushed   []*Task
	acked    []*Task
	finished bool
}

func (tx *MemStoreTransaction) Push(ctx context.Context, task Task) (uint32, error) {
	if tx.finished {
		return 0, errors.New("transaction finishedd")
	}
	task.ID = tx.m.genTaskID()
	tx.pushed = append(tx.pushed, &task)
	tx.ops = append(tx.ops, taskToOpAdd(&task))
	return task.ID, nil
}

func (tx *MemStoreTransaction) Acknowledge(ctx context.Context, taskID uint32, ack bool) error {
	if tx.finished {
		return errors.New("transaction finishedd")
	}
	for _, task := range tx.m.toack {
		if task.ID != taskID {
			continue
		}
		if ack {
			tx.ops = append(tx.ops, &wal.OpDelete{ID: task.ID})
			tx.acked = append(tx.acked, task)
		} else {
			return errors.New("NACK not supported")
		}
		return nil
	}
	return ErrNotFound
}

func (tx *MemStoreTransaction) Commit(ctx context.Context) error {
	if tx.finished {
		return errors.New("transaction finishedd")
	}
	tx.finished = true
	defer tx.m.mu.Unlock()

	n, err := tx.m.walWr.Append(tx.ops...)
	if err != nil {
		return fmt.Errorf("write to WAL: %w", err)
	}
	tx.m.walSize += uint64(n)

	for _, task := range tx.pushed {
		tx.m.namedQueue(task.Queue).pushTask(task)
		if task.ExecuteAt == nil {
			tx.m.metrics.IncrQueue(task.Queue, "ready")
		} else {
			tx.m.metrics.IncrQueue(task.Queue, "delayed")
		}
	}

	var hasAck bool
	for _, op := range tx.ops {
		switch op := op.(type) {
		case *wal.OpAdd:
			// Those were added earlier.
		case *wal.OpDelete:
			hasAck = true
			for i, task := range tx.m.toack {
				if task.ID != op.ID {
					continue
				}
				// Order does not matter.
				tx.m.toack[i] = tx.m.toack[len(tx.m.toack)-1]
				tx.m.toack = tx.m.toack[:len(tx.m.toack)-1]
				tx.m.namedQueue(task.Queue).ntoack--
				tx.m.metrics.DecrQueue(task.Queue, "toack")
				break
			}
		default:
			panic(fmt.Sprintf("WAL operation not supported: %T", op))
		}
	}

	if hasAck && tx.m.vacuumSize != 0 && tx.m.walSize > tx.m.vacuumSize {
		go tx.m.walVacuum()
	}

	return nil
}

func (tx *MemStoreTransaction) Rollback(ctx context.Context) error {
	if tx.finished {
		return errors.New("transaction finishedd")
	}
	tx.finished = true
	tx.m.mu.Unlock()
	return nil
}

type namedQueue struct {
	name string
	// Tasks that a ready to be consumed. Ordered from front to back.
	ready *list.List
	// Tasks that execution was delayed, ordered by their execution date.
	// Earliest execution at the front. Before returning a task from the
	// ready list, consider this collection first.
	delayed *list.List
	// Keep a counter of number of messages that were fetched but not yet
	// acked.
	ntoack uint16
}

// Empty returns true if this task queue does not contain any data.
func (tq *namedQueue) Empty() bool {
	return tq.ready.Len() == 0 && tq.delayed.Len() == 0 && tq.ntoack == 0
}

// popTask returns a ready to be consumed task from this queue. Returned task
// is removed from this queue. Delayed task has a return priority.
func (tq *namedQueue) popTask(now time.Time) (*Task, bool) {
	if tq.delayed.Len() > 0 {
		first := tq.delayed.Front().Value.(*Task)
		if first.ExecuteAt.Before(now) {
			tq.delayed.Remove(tq.delayed.Front())
			tq.ntoack++
			return first, true
		}
	}
	if tq.ready.Len() > 0 {
		tq.ntoack++
		return tq.ready.Remove(tq.ready.Front()).(*Task), true
	}
	return nil, false
}

// pushTask adds task to this queue.
func (tq *namedQueue) pushTask(t *Task) {
	if t.ExecuteAt == nil {
		tq.ready.PushBack(t)
		return
	}

	if tq.delayed.Len() == 0 {
		tq.delayed.PushBack(t)
		return
	}

	for e := tq.delayed.Front(); e != nil; e = e.Next() {
		if t.ExecuteAt.Before(*e.Value.(*Task).ExecuteAt) {
			tq.delayed.InsertBefore(t, e)
			return
		}
	}
	tq.delayed.PushBack(t)
}

func taskToOpAdd(task *Task) wal.Operation {
	return &wal.OpAdd{
		ID:        task.ID,
		Queue:     task.Queue,
		Name:      task.Name,
		Payload:   task.Payload,
		Deadqueue: task.Deadqueue,
		ExecuteAt: task.ExecuteAt,
		Retry:     task.Retry,
	}
}

func opAddToTask(op *wal.OpAdd) *Task {
	return &Task{
		ID:        op.ID,
		Queue:     op.Queue,
		Name:      op.Name,
		Payload:   op.Payload,
		Deadqueue: op.Deadqueue,
		ExecuteAt: op.ExecuteAt,
		Retry:     op.Retry,
		Failures:  0,
	}
}

func taskToDeadTask(t *Task) *Task {
	return &Task{
		ID:        t.ID,
		Queue:     t.Deadqueue,
		Name:      t.Name,
		Payload:   t.Payload,
		Deadqueue: "",
		ExecuteAt: nil,
		Retry:     t.Retry,
		Failures:  0,
	}
}
