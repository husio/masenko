/*

Package masenkoclient provides an implementation of a client that allows to
comunicate with a single Masenko server.

*/
package masenkoclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Client is the base interface implemented by all Masenko clients.
type Client interface {
	// Push schedules a task execution. Payload must be a (usually JSON)
	// serializable.
	Push(ctx context.Context, taskName string, queueName string, payload interface{}, deadqueue string, retry uint8, executeAt *time.Time) error
	// Fetch blocks until a task can be returned or context timeout is
	// reached. If timed out without being able to return a task, ErrEmpty
	// is returned (even if caused by context timeout).
	Fetch(ctx context.Context, queues []string) (*FetchResponse, error)
	// Ack marks the task as processed. This operation removes the task
	// from the server.
	Ack(ctx context.Context, taskID uint32) error
	// Nack marks the task processing as failed. This operation reschedules
	// the task accodring to the retry policy. Depending on the retry
	// configuration and failures count, this task might be rescheduled for
	// future processing or moved to a dead letter queue.
	Nack(ctx context.Context, taskID uint32) error
	// Close the client. Release all resources allocated.
	Close() error

	// Tx returns a new transaction that executes all requested operations
	// atomically.
	Tx(ctx context.Context) Transaction
}

// Transaction is an interface implemented by Masenko transaction wrapper. It
// allows to collect operations and execute them atomically together. Each
// transaction can be executed only once.
// To execute a transaction, Commit method must be called. Transaction that is
// not committed is dropped.
type Transaction interface {
	// Push schedules a task execution. Payload must be a (usually JSON)
	// serializable.
	Push(ctx context.Context, taskName string, queueName string, payload interface{}, deadqueue string, retry uint8, executeAt *time.Time) error
	// Fetch blocks until a task can be returned or context timeout is
	// reached. If timed out without being able to return a task, ErrEmpty
	// is returned (even if caused by context timeout).
	Ack(ctx context.Context, taskID uint32) error
	// Commit executes all operations collected within this transaction.
	Commit(context.Context) error
}

// Dial returns a Client connected to a Masenko server reachable under provided
// address.
// Returned client maintains the connection health by periodically sending a
// heartbeat request.
func Dial(address string) (*HeartbeatClient, error) {
	c, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("TCP dial: %w", err)
	}
	hb := &HeartbeatClient{
		bc: bareClient{
			cl: c,
			rd: bufio.NewReader(c),
			wr: bufio.NewWriter(c),
		},
		stop: make(chan struct{}),
	}
	go hb.heartbeatLoop()

	return hb, nil
}

// HeartbeatClient is a Masenko client implementation. It provides all core
// functionality necessary to publish and consume tasks. Addionally, this
// client implementation maintains connection health by sending a heartbeat
// request if needed.
type HeartbeatClient struct {
	bc   bareClient
	stop chan struct{}
}

var _ Client = (*HeartbeatClient)(nil)

func (hb *HeartbeatClient) heartbeatLoop() {
	// In the current implementation, heartbeat PING is sent periodically
	// regardless the traffic. This could be optimized to send it only if
	// no other request was executed.
	// Sending at a regular pace is much easier to implement. Penalty is
	// very low.
	t := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-hb.stop:
			return
		case <-t.C:
			if err := hb.bc.Ping(context.Background()); err != nil {
				hb.Close()
			}
		}
	}
}

// Tx returns a new transaction that executes all requested operations
// atomically. Only a subset of requests can be executed within a transaction.
func (hb *HeartbeatClient) Tx(ctx context.Context) Transaction {
	return &transaction{hb: hb}
}

type transaction struct {
	hb       *HeartbeatClient
	requests []request
}

func (t *transaction) Push(ctx context.Context, taskName string, queueName string, payload interface{}, deadqueue string, retry uint8, executeAt *time.Time) error {
	taskPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal task payload: %w", err)
	}
	requestPayload, err := json.Marshal(pushRequest{
		Name:      taskName,
		Queue:     queueName,
		Deadqueue: deadqueue,
		Payload:   taskPayload,
		Retry:     retry,
		ExecuteAt: executeAt,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	t.requests = append(t.requests, request{verb: "PUSH", payload: requestPayload})
	return nil
}

func (t *transaction) Ack(ctx context.Context, taskID uint32) error {
	payload, err := json.Marshal(ackRequest{TaskID: taskID})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	t.requests = append(t.requests, request{verb: "ACK", payload: payload})
	return nil
}

// Commit sends the transaction to the server which cause atomic execution of
// all accumulated operations in specified order. Only if all operations were
// successful, the server state is updated. Any operation failure cause the
// whole transaction to be aborted and no change is made to the server state.
func (t *transaction) Commit(ctx context.Context) error {
	if len(t.requests) == 0 {
		return nil
	}
	select {
	case <-t.hb.stop:
		return ErrClosed
	default:
		return t.hb.bc.Atomic(ctx, t.requests)
	}
}

// Ack marks the task as processed. This operation removes the task from the
// server.
func (hb *HeartbeatClient) Ack(ctx context.Context, taskID uint32) error {
	select {
	case <-hb.stop:
		return ErrClosed
	default:
		return hb.bc.Ack(ctx, taskID)
	}
}

// Nack marks the task processing as failed. This operation reschedules the
// task accodring to the retry policy. Depending on the retry configuration and
// failures count, this task might be rescheduled for future processing or
// moved to a dead letter queue.
func (hb *HeartbeatClient) Nack(ctx context.Context, taskID uint32) error {
	select {
	case <-hb.stop:
		return ErrClosed
	default:
		return hb.bc.Nack(ctx, taskID)
	}
}

// Push schedules a task execution.
//
// Task name must be provided.
//
// Queue name is optional name of the queue that this task is pushed to. Leave
// empty to use server default queue.
//
// Payload is an optional JSON serializable data. Can be nil.
//
// Dead letter queue is an optional queue name that this task is moved to after
// reaching failed processing attempts limit. Use empty string to disable.
//
// Retry is the number of attempts that this task should be retried after a
// failed processing. Use zero to use server default value.
//
// Execute at is an optional time that this task should be executed at the
// earliest. Providing a value allows to schedule task execution in the future.
// Keep in mind that Masenko is not a database and this functionality should be
// used with caution.
func (hb *HeartbeatClient) Push(ctx context.Context, taskName string, queueName string, payload interface{}, deadqueue string, retry uint8, executeAt *time.Time) error {
	select {
	case <-hb.stop:
		return ErrClosed
	default:
		return hb.bc.Push(ctx, taskName, queueName, payload, deadqueue, retry, executeAt)
	}
}

func (hb *HeartbeatClient) Fetch(ctx context.Context, queues []string) (*FetchResponse, error) {
	select {
	case <-hb.stop:
		return nil, ErrClosed
	default:
		return hb.bc.Fetch(ctx, queues)
	}
}

func (hb *HeartbeatClient) Close() error {
	select {
	case <-hb.stop:
		return ErrClosed
	default:
	}

	err := hb.bc.Close()

	// Subsequent closing must not panic.
	defer func() {
		if e := recover(); e != nil && err == nil {
			err = ErrClosed
		}
	}()
	close(hb.stop)

	return err
}

type bareClient struct {
	cl io.Closer
	mu sync.Mutex
	rd *bufio.Reader
	wr *bufio.Writer
}

func (c *bareClient) Atomic(ctx context.Context, requests []request) error {
	c.mu.Lock()

	if _, err := c.wr.WriteString("ATOMIC\n"); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("write ATOMIC: %w", err)
	}
	for i, r := range requests {
		if _, err := fmt.Fprintf(c.wr, "%s %s\n", r.verb, string(r.payload)); err != nil {
			c.mu.Unlock()
			return fmt.Errorf("write %d: %w", i, err)
		}
	}
	if _, err := c.wr.WriteString("DONE\n"); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("write DONE: %w", err)
	}
	if err := c.wr.Flush(); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("write flush: %w", err)
	}

	line, err := c.rd.ReadBytes('\n')
	c.mu.Unlock()
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	line = bytes.TrimSpace(line)
	chunks := bytes.SplitN(line, []byte{' '}, 2)
	if len(chunks) != 2 {
		return fmt.Errorf("invalid respones: %s", line)
	}

	switch code := string(chunks[0]); code {
	case "OK":
		return nil
	case "ERR":
		var info struct {
			Msg string
		}
		if err := json.Unmarshal(chunks[1], &info); err != nil {
			return fmt.Errorf("unmarshal error payload: %s", string(chunks[1]))
		}
		return errors.New(info.Msg)
	default:
		return fmt.Errorf("%w: %s", ErrUnexpectedResponse, code)
	}
}

type request struct {
	verb    string
	payload []byte
}

func (c *bareClient) Ping(ctx context.Context) error {
	resp, err := c.do(ctx, "PING", emptyBody, nil)
	if err != nil {
		return err
	}
	if resp != "PONG" {
		return fmt.Errorf("%w: %s", ErrUnexpectedResponse, resp)
	}
	return nil
}

func (c *bareClient) Ack(ctx context.Context, taskID uint32) error {
	resp, err := c.do(ctx, "ACK", ackRequest{TaskID: taskID}, nil)
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("%w: %s", ErrUnexpectedResponse, resp)
	}
	return nil
}

type ackRequest struct {
	TaskID uint32 `json:"id"`
}

func (c *bareClient) Nack(ctx context.Context, taskID uint32) error {
	resp, err := c.do(ctx, "NACK", nackRequest{TaskID: taskID}, nil)
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("%w: %s", ErrUnexpectedResponse, resp)
	}
	return nil
}

type nackRequest struct {
	TaskID uint32 `json:"id"`
}

func (c *bareClient) Quit(ctx context.Context) error {
	resp, err := c.do(ctx, "QUIT", emptyBody, nil)
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("%s: %s", ErrUnexpectedResponse, resp)
	}
	return nil
}

func (c *bareClient) Push(
	ctx context.Context,
	taskName string,
	queueName string,
	payload interface{},
	deadqueue string,
	retry uint8,
	executeAt *time.Time,
) error {
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("serialize payload: %w", err)
	}
	_, err = c.do(ctx, "PUSH", pushRequest{
		Name:      taskName,
		Queue:     queueName,
		Payload:   rawPayload,
		Deadqueue: deadqueue,
		Retry:     retry,
		ExecuteAt: executeAt,
	}, nil)
	return err
}

type pushRequest struct {
	Name      string          `json:"name"`
	Queue     string          `json:"queue"`
	Deadqueue string          `json:"deadqueue"`
	Payload   json.RawMessage `json:"payload"`
	Retry     uint8           `json:"retry"`
	ExecuteAt *time.Time      `json:"execute_at,omitempty"`
}

func (c *bareClient) Fetch(ctx context.Context, queues []string) (*FetchResponse, error) {
	var timeout string
	if deadline, ok := ctx.Deadline(); ok {
		timeout = deadline.Sub(time.Now()).String()
	}

	var resp FetchResponse
	_, err := c.do(ctx, "FETCH", fetchRequest{
		Queues:  queues,
		Timeout: timeout,
	}, &resp)
	return &resp, err
}

type fetchRequest struct {
	Queues  []string `json:"queues"`
	Timeout string   `json:"timeout,omitempty"`
}

// FetchRespones represents a single task returned by the FETCH response.
type FetchResponse struct {
	// ID is the identifier of the task.
	ID uint32 `json:"id"`
	// Queue is the name of the queue to which the task belongs to.
	Queue string `json:"queue"`
	// Name is the task name.
	Name string `json:"name"`
	// Payload is an optional JSON serialized payload of the task.
	Payload json.RawMessage `json:"payload,omitempty"`
	// Failures is the number of failed processing attempts of this task.
	Failures uint8 `json:"failures,omitempty"`
}

func (c *bareClient) Close() error {
	err := c.Quit(context.Background())
	c.cl.Close()
	return err
}

func (c *bareClient) do(
	ctx context.Context,
	verb string,
	payload interface{},
	response interface{},
) (string, error) {
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("JSON marshal payload: %w", err)
	}

	c.mu.Lock()
	if _, err := fmt.Fprintf(c.wr, "%s %s\n", verb, string(rawPayload)); err != nil {
		c.mu.Unlock()
		return "", fmt.Errorf("write: %w", err)
	}
	if err := c.wr.Flush(); err != nil {
		c.mu.Unlock()
		return "", fmt.Errorf("write flush: %w", err)
	}
	line, err := c.rd.ReadBytes('\n')
	c.mu.Unlock()
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	line = bytes.TrimSpace(line)
	chunks := bytes.SplitN(line, []byte{' '}, 2)
	if len(chunks) != 2 {
		return "", fmt.Errorf("invalid respones: %s", line)
	}

	code := string(chunks[0])
	if code == "ERR" {
		var info struct {
			Msg string
		}
		if err := json.Unmarshal(chunks[1], &info); err != nil {
			return "", fmt.Errorf("unmarshal error payload: %s", string(chunks[1]))
		}
		return code, errors.New(info.Msg)
	}
	if code == "EMPTY" {
		return code, ErrEmpty
	}
	if response != nil {
		if err := json.Unmarshal(chunks[1], response); err != nil {
			return code, fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return code, nil
}

var (
	// ErrClient is a main Masenko client error, that all other errors
	// extend.
	ErrClient = errors.New("masenko client")

	// ErrEmpty is returned when a task fetch timed out without being able
	// to return a task from any of the queues.
	ErrEmpty = fmt.Errorf("%w: empty queue", ErrClient)

	// ErrUnexpectedResponse is returned when an unexpected response for
	// made request is received.
	ErrUnexpectedResponse = fmt.Errorf("%w: unexpected response", ErrClient)

	// ErrClosed is returned when a closed client is used.
	ErrClosed = fmt.Errorf("%w: closed", ErrClient)
)

var emptyBody = json.RawMessage("{}")
