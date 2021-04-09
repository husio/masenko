package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"crawshaw.io/sqlite"
)

type SQLiteQueue struct {
	now func() time.Time
	// Below mutex protects the only access to the
	// database, so no transaction is required to ensure
	// consistent view.
	mu sync.Mutex
	c  *sqlite.Conn
}

func OpenSQLiteQueue(dbpath string) (*SQLiteQueue, error) {
	c, err := sqlite.OpenConn(dbpath, sqlite.SQLITE_OPEN_CREATE|sqlite.SQLITE_OPEN_READWRITE|sqlite.SQLITE_OPEN_WAL)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	queries := []string{
		`CREATE TABLE IF NOT EXISTS tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			queue TEXT NOT NULL,
			name TEXT NOT NULL,
			payload BLOB NOT NULL DEFAULT '',
			deadqueue TEXT NOT NULL DEFAULT '',
			execute_at INTEGER NOT NULL DEFAULT 0,
			retry INTEGER NOT NULL DEFAULT 0,
			failures INTEGER NOT NULL DEFAULT 0,
			acquired INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS tasks_pull_order_idx
			ON tasks(execute_at DESC, id ASC)
			WHERE acquired = 0`,
		// There is a single connection protected by the mutex.
		// Exclusive lock is increasing its performance.
		`PRAGMA locking_mode = EXCLUSIVE`,
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA synchronous = NORMAL`,
	}
	for i, sql := range queries {
		if err := exec(c, sql); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("exec %d: %w", i, err)
		}
	}

	q := &SQLiteQueue{
		c:   c,
		now: func() time.Time { return time.Now().UTC() },
	}
	return q, nil
}

func exec(c *sqlite.Conn, sql string) error {
	s, err := c.Prepare(sql)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer s.Finalize()
	if _, err := s.Step(); err != nil {
		return fmt.Errorf("step: %w", err)
	}
	return nil
}

func (q *SQLiteQueue) withConn(ctx context.Context) (*sqlite.Conn, func()) {
	q.mu.Lock()
	q.c.SetInterrupt(ctx.Done())

	cleanup := func() {
		q.c.SetInterrupt(nil)
		q.mu.Unlock()
	}

	return q.c, cleanup
}

func (q *SQLiteQueue) Pull(ctx context.Context, queues []string) (*Task, error) {
	if len(queues) == 0 {
		return nil, ErrNotFound
	}
	if len(queues) > 1 {
		return nil, fmt.Errorf("only a single queue pull is supported")
	}
	for {
		switch t, err := q.pullNowait(ctx, queues); {
		case err == nil:
			return t, nil
		case errors.Is(err, errNoResult):
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(25 * time.Millisecond):
				// Try again.
			}
		case isInterruptErr(err):
			return nil, ctx.Err()
		default:
			return nil, err
		}
	}
}

func isInterruptErr(err error) bool {
	var e sqlite.Error
	if errors.As(err, &e) {
		return e.Code == sqlite.SQLITE_INTERRUPT
	}
	return false
}

func (q *SQLiteQueue) pullNowait(ctx context.Context, queues []string) (*Task, error) {
	c, done := q.withConn(ctx)
	defer done()

	s, err := c.Prepare(`
		SELECT * FROM tasks
		WHERE acquired = 0
			AND queue = :queue
			AND execute_at <= :now
		ORDER BY execute_at DESC, id ASC
		LIMIT 1
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}
	defer s.Reset()

	s.SetText(":queue", queues[0]) // TODO
	s.SetInt64(":now", q.now().UnixNano())
	ok, err := s.Step()
	if err != nil {
		return nil, fmt.Errorf("step: %w", err)
	}
	if !ok {
		return nil, errNoResult
	}
	var executeAt *time.Time
	if ns := s.GetInt64("execute_at"); ns != 0 {
		t := time.Unix(0, ns)
		executeAt = &t
	}
	task := &Task{
		ID:        uint32(s.GetInt64("id")),
		Queue:     s.GetText("queue"),
		Name:      s.GetText("name"),
		Payload:   []byte(s.GetText("payload")),
		Deadqueue: s.GetText("deadqueue"),
		ExecuteAt: executeAt,
		Retry:     uint8(s.GetInt64("retry")),
		Failures:  uint8(s.GetInt64("failures")),
	}

	s, err = c.Prepare(`
		UPDATE tasks
		SET acquired = 1
		WHERE id = :id
	`)
	if err != nil {
		return nil, fmt.Errorf("lock %d task: prepare: %w", task.ID, err)
	}
	defer s.Reset()

	s.SetInt64(":id", int64(task.ID))
	if _, err := s.Step(); err != nil {
		return nil, fmt.Errorf("lock %d task: step: %w", task.ID, err)
	}

	return task, nil
}

var errNoResult = errors.New("no result")

func (q *SQLiteQueue) Push(ctx context.Context, t Task) (uint32, error) {
	c, done := q.withConn(ctx)
	defer done()
	return pushTask(c, t)
}

func pushTask(c *sqlite.Conn, t Task) (uint32, error) {
	if t.Name == "" {
		return 0, fmt.Errorf("%w: name is required", ErrInvalid)
	}
	s, err := c.Prepare(`
		INSERT INTO tasks (
			queue,
			name,
			payload,
			deadqueue,
			execute_at,
			retry,
			failures
		)
		VALUES (
			:queue,
			:name,
			coalesce(:payload, ''),
			:deadqueue,
			:execute_at,
			:retry,
			0)
	`)
	if err != nil {
		return 0, fmt.Errorf("prepare: %w", err)
	}
	defer s.Reset()

	s.SetText(":queue", t.Queue)
	s.SetText(":name", t.Name)
	s.SetBytes(":payload", t.Payload)
	s.SetText(":deadqueue", t.Deadqueue)
	if t.ExecuteAt == nil {
		s.SetInt64(":execute_at", 0)
	} else {
		s.SetInt64(":execute_at", t.ExecuteAt.UnixNano())
	}
	s.SetInt64(":retry", int64(t.Retry))
	if _, err := s.Step(); err != nil {
		return 0, fmt.Errorf("step: %w", err)
	}
	if err := s.Reset(); err != nil {
		return 0, fmt.Errorf("reset: %w", err)
	}
	return uint32(c.LastInsertRowID()), nil
}

func (q *SQLiteQueue) Acknowledge(ctx context.Context, taskID uint32, ack bool) error {
	c, done := q.withConn(ctx)
	defer done()

	if ack {
		return deleteTask(c, taskID)
	}
	return failTask(c, taskID, q.now())
}

func deleteTask(c *sqlite.Conn, taskID uint32) error {

	s, err := c.Prepare(`
			DELETE FROM tasks
			WHERE id = :id AND acquired != 0
		`)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	defer s.Reset()

	s.SetInt64(":id", int64(taskID))
	if _, err := s.Step(); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if n := c.Changes(); n != 1 {
		return fmt.Errorf("%d rows deleted", n)
	}
	return nil

}

func failTask(c *sqlite.Conn, taskID uint32, now time.Time) error {
	s, err := c.Prepare(`SAVEPOINT task_ack`)
	if err != nil {
		return fmt.Errorf("savepoint: %w", err)
	}
	defer s.Reset()
	if _, err := s.Step(); err != nil {
		return fmt.Errorf("savepoint step: %w", err)
	}
	success := false
	defer func() {
		sql := "ROLLBACK TO SAVEPOINT task_ack"
		if success {
			sql = "RELEASE SAVEPOINT task_ack"
		}
		s, err := c.Prepare(sql)
		if err != nil {
			return
		}
		defer s.Reset()
		_, _ = s.Step()
	}()

	s, err = c.Prepare(`
			UPDATE tasks
			SET failures = failures + 1, acquired = 0
			WHERE id = :id AND acquired != 0
		`)
	if err != nil {
		return fmt.Errorf("prepare update: %w", err)
	}
	defer s.Reset()

	s.SetInt64(":id", int64(taskID))
	if _, err := s.Step(); err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if n := c.Changes(); n != 1 {
		return fmt.Errorf("%d rows updated", n)
	}

	s, err = c.Prepare(`SELECT failures, retry, deadqueue FROM tasks WHERE id = :id`)
	if err != nil {
		return fmt.Errorf("prepare failures select: %w", err)
	}
	defer s.Reset()
	s.SetInt64(":id", int64(taskID))
	if ok, err := s.Step(); err != nil {
		return fmt.Errorf("step failures select: %w", err)
	} else if !ok {
		return fmt.Errorf("step failures select: no result")
	}
	if s.GetInt64("failures") < s.GetInt64("retry") {
		// Task was failed but not yet reached maximum number of
		// retries.
		success = true
		return nil
	}

	if deadqueue := s.GetText("deadqueue"); deadqueue == "" {
		s, err := c.Prepare(`DELETE FROM tasks WHERE id = :id`)
		if err != nil {
			return fmt.Errorf("prepare delete failed task: %w", err)
		}
		defer s.Reset()
		if _, err := s.Step(); err != nil {
			return fmt.Errorf("delete failed task: %w", err)
		}
	} else {
		s, err = c.Prepare(`
			UPDATE tasks
			SET
				queue = :queue,
				deadqueue = '',
				execute_at = :now,
				retry = 3,
				failures = 0,
				acquired = 0
			WHERE id = :id
		`)
		if err != nil {
			return fmt.Errorf("prepare deadqueue update: %w", err)
		}
		defer s.Reset()
		s.SetText(":queue", deadqueue)
		s.SetInt64(":now", now.UnixNano())
		s.SetInt64(":id", int64(taskID))
		if _, err := s.Step(); err != nil {
			return fmt.Errorf("move task to deadqueue: %w", err)
		}
	}

	success = true
	return nil
}

func (q *SQLiteQueue) Begin(ctx context.Context) Transaction {
	c, done := q.withConn(ctx)
	tx := transaction(c)
	return &sqliteQueueTx{
		q:    q,
		tx:   &tx,
		done: done,
	}
}

type sqliteQueueTx struct {
	q    *SQLiteQueue
	tx   *tx
	err  error
	done func()
}

func (tx *sqliteQueueTx) Push(ctx context.Context, task Task) (uint32, error) {
	if tx.err != nil {
		return 0, tx.err
	}
	id, err := pushTask(tx.q.c, task)
	tx.err = err
	return id, err
}

func (tx *sqliteQueueTx) Acknowledge(ctx context.Context, taskID uint32, ack bool) error {
	if tx.err != nil {
		return tx.err
	}
	if ack {
		return deleteTask(tx.q.c, taskID)
	}
	return failTask(tx.q.c, taskID, tx.q.now())
}

func (tx *sqliteQueueTx) Commit(context.Context) error {
	if tx.err != nil {
		return tx.err
	}
	defer tx.done()
	if err := tx.tx.Commit(); err != nil {
		tx.err = err
		return err
	}
	tx.err = errTxClosed
	return nil
}

func (tx *sqliteQueueTx) Rollback(context.Context) error {
	if tx.err != nil {
		return tx.err
	}
	defer tx.done()
	if err := tx.tx.Rollback(); err != nil {
		tx.err = err
		return err
	}
	tx.err = errTxClosed
	return nil
}

func (q *SQLiteQueue) Stats() []QueueStat {
	panic("todo")
}

func (q *SQLiteQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.c.Close()
}

func transaction(c *sqlite.Conn) tx {
	name := fmt.Sprintf(`tx_%d`, atomic.AddUint64(&txid, 1))
	s, err := c.Prepare(`SAVEPOINT ` + name)
	if err != nil {
		return tx{err: fmt.Errorf("create savepoint: %w", err)}
	}
	defer s.Finalize()
	if _, err := s.Step(); err != nil {
		return tx{err: fmt.Errorf("step savepoint: %w", err)}
	}
	return tx{c: c, name: name}
}

type tx struct {
	name string
	c    *sqlite.Conn
	err  error
}

func (tx *tx) Commit() error {
	if tx.err != nil {
		return tx.err
	}
	s, err := tx.c.Prepare("RELEASE SAVEPOINT " + tx.name)
	if err != nil {
		return fmt.Errorf("prepare release: %w", err)
	}
	defer s.Finalize()
	if _, err := s.Step(); err != nil {
		return fmt.Errorf("step release: %w", err)
	}
	tx.err = errTxClosed
	return nil
}

func (tx *tx) Rollback() error {
	if tx.err != nil {
		return tx.err
	}
	s, err := tx.c.Prepare("ROLLBACK TO SAVEPOINT " + tx.name)
	if err != nil {
		return fmt.Errorf("prepare rollback: %w", err)
	}
	defer s.Finalize()
	if _, err := s.Step(); err != nil {
		return fmt.Errorf("step rollback: %w", err)
	}
	tx.err = errTxClosed
	return nil
}

var errTxClosed = errors.New("closed")

var txid uint64 = 1

var ErrInvalid = errors.New("invalid")
