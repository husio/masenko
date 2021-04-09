package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"crawshaw.io/sqlite"
)

type SQLiteStore struct {
	log     *log.Logger
	metrics MetricCounter
	now     func() time.Time

	mu sync.Mutex
	db *sqlite.Conn
}

func (store *SQLiteStore) useDB(ctx context.Context) func() {
	store.mu.Lock()
	if store.db == nil {
		// Database is closed.
		store.mu.Unlock()
		return func() {}
	}
	store.db.SetInterrupt(ctx.Done())

	return func() {
		store.db.SetInterrupt(nil)
		store.mu.Unlock()
	}
}

func OpenSQLiteStore(dbpath string, logger *log.Logger, mc MetricCounter) (*SQLiteStore, error) {
	db, err := sqlite.OpenConn(dbpath, sqlite.SQLITE_OPEN_CREATE|sqlite.SQLITE_OPEN_READWRITE|sqlite.SQLITE_OPEN_WAL)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}

	if err := initializeDB(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize db: %w", err)
	}

	store := &SQLiteStore{
		db:      db,
		log:     logger,
		metrics: mc,
		now:     func() time.Time { return time.Now().UTC() },
	}
	return store, nil
}

func initializeDB(c *sqlite.Conn) error {
	if err := execDB(c, `
		CREATE TABLE IF NOT EXISTS tasks(
			id integer primary key,
			queue text not null,
			name text not null,
			payload text not null,
			deadqueue text not null,
			execute_at int not null, -- when to execute in seconds
			retry int not null,
			failures int not null,
			acquired_at int not null -- aquisition time in seconds
		)
	`); err != nil {
		return fmt.Errorf("create queue table: %w", err)
	}
	return nil
}

func execDB(c *sqlite.Conn, sql string) error {
	s, err := c.Prepare(sql)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	if _, err := s.Step(); err != nil {
		_ = s.Finalize()
		return fmt.Errorf("step: %w", err)
	}
	if err := s.Finalize(); err != nil {
		return fmt.Errorf("finalize: %w", err)
	}
	return nil
}

func (store *SQLiteStore) Pull(ctx context.Context, queues []string) (*Task, error) {
	task, err := store.pull(ctx, queues)
	if err == nil {
		return task, nil
	}
	var sqlerr sqlite.Error
	if errors.As(err, &sqlerr) && sqlerr.Code == sqlite.SQLITE_INTERRUPT {
		return nil, fmt.Errorf("sqlite interrupt: %w", ctx.Err())
	}
	return nil, err
}

func (store *SQLiteStore) pull(ctx context.Context, queues []string) (*Task, error) {
	if len(queues) > 1 {
		return nil, errors.New("multiple queues filter is not implemented")
	}

	// Manual mutex management is required.
	// defer store.useDB(ctx)()
	store.mu.Lock()
	defer func() {
		store.mu.Unlock()
	}()
	if store.db == nil {
		return nil, ErrClosed
	}

	var task *Task

findReadyTask:
	for {
		// This does not lock a task, but that is ok as long as Pull is
		// using a mutex.  A transaction might be the right thing to do
		// here.
		s, err := store.db.Prepare(`
			SELECT *
			FROM tasks
			WHERE acquired_at = 0
				AND queue = $queue
				AND execute_at <= $now
			ORDER BY id DESC
			LIMIT 1
		`)
		if err != nil {
			return nil, fmt.Errorf("prepare select sql: %w", err)
		}
		s.SetText("$queue", queues[0])
		s.SetInt64("$now", store.now().Unix())
		if ok, err := s.Step(); err != nil {
			_ = s.Reset()
			return nil, fmt.Errorf("find a task: %w", err)
		} else if ok {
			task = &Task{
				ID:        uint32(s.GetInt64("id")),
				Queue:     s.GetText("queue"),
				Name:      s.GetText("name"),
				Payload:   []byte(s.GetText("payload")),
				Deadqueue: s.GetText("deadqueue"),
				Retry:     uint8(s.GetInt64("retry")),
				Failures:  uint8(s.GetInt64("failures")),
			}
			if sec := s.GetInt64("execute_at"); sec != 0 {
				at := time.Unix(sec, 0)
				task.ExecuteAt = &at
			}
			if err := s.Reset(); err != nil {
				return nil, fmt.Errorf("statement reset: %w", err)
			}
			break findReadyTask
		} else {
			if err := s.Reset(); err != nil {
				return nil, fmt.Errorf("statement reset: %w", err)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(25 * time.Millisecond):
				// Busy waiting. This is simplicity over accuracy.
			}
		}
	}

	s, err := store.db.Prepare(`
		UPDATE tasks
		SET acquired_at = $now
		WHERE id = $id
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare update sql: %w", err)
	}
	defer s.Reset()
	s.SetInt64("$now", store.now().Unix())
	s.SetInt64("$id", int64(task.ID))
	if _, err := s.Step(); err != nil {
		return nil, fmt.Errorf("acquire task: %w", err)
	}
	return task, nil
}

func (store *SQLiteStore) Push(ctx context.Context, task Task) (uint32, error) {
	defer store.useDB(ctx)()
	if store.db == nil {
		return 0, ErrClosed
	}

	s, err := store.db.Prepare(`
		INSERT INTO tasks (
			queue,
			name,
			payload,
			deadqueue,
			execute_at,
			retry,
			failures,
			acquired_at
		)
		VALUES (
			$queue,
			$name,
			$payload,
			$deadqueue,
			$execute_at,
			$retry,
			$failures,
			0
		)
	`)
	if err != nil {
		return 0, fmt.Errorf("prepare sql: %w", err)
	}
	defer s.Reset()
	s.SetText("$queue", task.Queue)
	s.SetText("$name", task.Name)
	if len(task.Payload) == 0 {
		s.SetText("$payload", "")
	} else {
		s.SetBytes("$payload", task.Payload)
	}
	s.SetText("$deadqueue", task.Deadqueue)
	if task.ExecuteAt == nil {
		s.SetInt64("$execute_at", 0)
	} else {
		s.SetInt64("$execute_at", task.ExecuteAt.Unix())
	}
	s.SetInt64("$retry", int64(task.Retry))
	s.SetInt64("$failures", int64(task.Failures))

	if _, err := s.Step(); err != nil {
		return 0, fmt.Errorf("exec: %w", err)
	}
	return uint32(store.db.LastInsertRowID()), nil
}

func (store *SQLiteStore) Acknowledge(ctx context.Context, taskID uint32, ack bool) error {
	defer store.useDB(ctx)()
	if store.db == nil {
		return ErrClosed
	}

	var (
		s   *sqlite.Stmt
		err error
	)
	if ack {
		s, err = store.db.Prepare(`
			DELETE FROM tasks WHERE id = $id
		`)
	} else {
		s, err = store.db.Prepare(`
			UPDATE tasks
			SET
				acquired_at = 0,
				failures = failures + 1
			WHERE id = $id
		`)
		// TODO -- if failures >= retry, move to deadqueue
	}
	if err != nil {
		return fmt.Errorf("prepare sql: %w", err)
	}
	defer s.Reset()
	s.SetInt64("$id", int64(taskID))
	if _, err := s.Step(); err != nil {
		return fmt.Errorf("ack task: %w", err)
	}
	return nil
}

func (store *SQLiteStore) Begin(ctx context.Context) Transaction {
	defer store.useDB(ctx)()

	return nil
}

func (store *SQLiteStore) Stats() []QueueStat {
	return nil
}

func (store *SQLiteStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrClosed
	}
	err := store.db.Close()
	store.db = nil
	return err
}

var ErrClosed = errors.New("store closed")
