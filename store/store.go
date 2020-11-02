package store

import (
	"context"
	"errors"
	"time"
)

// Task represents a task entity as persisted in the queue.
type Task struct {
	// Globally Unique Identified of a task, always provided by the server.
	ID uint32
	// Queue is the name of the queue that this task belongs to.
	Queue string
	// Name is the name of the task (i.e. function name to execute when
	// processed).
	Name string
	// Payload is a serialized data necessary to process this task.
	Payload []byte
	// Deadqueue is a name of a queue that this task will be moved to after
	// too many failures. Can be empty to disable moving to dead letter
	// queue.
	Deadqueue string
	// ExecuteAt is an optional time at which this task should be processed
	// the earliest. If provided, allows to schedule task processing in the
	// future.
	ExecuteAt *time.Time
	// Retry defines how many times a failed task processing should be
	// repeated before removed from the queue.
	Retry uint8
	// Failures is a counter for failed processing attempts.
	Failures uint8
}

// Queue is implemented by any task queue backend.
type Queue interface {
	// Push a task intro the queue by appending it to a named queue of
	// choice.
	Push(ctx context.Context, task Task) (uint32, error)

	// Pull returns a task that belongs to one of requested named queues.
	// Named queues are checked in provided order and task is returned from
	// the first that is non empty.
	// This function blocks until a task can be returned or the context is
	// cancelled.
	Pull(ctx context.Context, queues []string) (*Task, error)

	// Acknowledge task if given ID. Positive acknowledgment removes the
	// task. Negative acknowledgment push it back and ready for
	// consumption. Depending on the implementation, negatively
	// acknowledged task can be pushed back with a delay (back off).
	Acknowledge(ctx context.Context, taskID uint32, ack bool) error

	// Being returns a new transaction that belongs to this queue.
	Begin(context.Context) Transaction

	// Stats returns a list of metrics per each existing queue name.
	Stats() []QueueStat
}

// Transaction represents a batch of operations that are executed atomically.
// Transaction implementation is accumulating operations that are then executed
// on commit.
// Transaction supports a subset of Queue operations.
type Transaction interface {
	// Push a task intro the queue by appending it to a named queue of
	// choice.
	// This method works exactly like corresponding Queue.Push method.
	Push(ctx context.Context, task Task) (uint32, error)
	// Acknowledge task if given ID.
	// This method works exactly like corresponding Queue.Push method.
	Acknowledge(ctx context.Context, taskID uint32, ack bool) error
	// Commit executes the transaction and apply changes from accumulated
	// by this transaction operations. Committed transaction must not be
	// used any further.
	Commit(context.Context) error
	// Rollback cancels this transaction and drops all changes accumulated.
	// Rolled back transaction must not be used any further.
	Rollback(context.Context) error
}

// QueueStat represents metrics of a single named queue.
type QueueStat struct {
	Name    string `json:"name"`
	Ready   uint32 `json:"ready"`
	Delayed uint32 `json:"delayed"`
	ToACK   uint32 `json:"to_ack"`
}

// ErrNotFound is returned when an operation cannot be completed because
// requested entity does not exist or cannot be found.
var ErrNotFound = errors.New("not found")
