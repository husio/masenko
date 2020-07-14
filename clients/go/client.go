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
	// Push schedules a task execution. Payload must be a (usually JSON) serializable.
	Push(ctx context.Context, taskName string, queueName string, payload interface{}, deadqueue string, retry uint8, executeAt *time.Time) error
	// Fetch blocks until a task can be returned or timeout is reached. If
	// timed out without being able to return a task, ErrEmpty is returned.
	Fetch(ctx context.Context, queues []string, timeout time.Duration) (*FetchResponse, error)
	// Close the client. Release all resources allocated.
	Close() error
}

type bareClient struct {
	cl io.Closer
	mu sync.Mutex
	rd *bufio.Reader
	wr *bufio.Writer
}

func Dial(address string) (Client, error) {
	c, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("TCP dial: %w", err)
	}
	return &bareClient{
		cl: c,
		rd: bufio.NewReader(c),
		wr: bufio.NewWriter(c),
	}, nil
}

func (c *bareClient) Ping(ctx context.Context) error {
	resp, err := c.do(ctx, "PING", emptyBody, nil)
	if err != nil {
		return err
	}
	if resp != "PONG" {
		return fmt.Errorf("unexpected response: %s", resp)
	}
	return nil
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

func (c *bareClient) Fetch(
	ctx context.Context,
	queues []string,
	timeout time.Duration,
) (*FetchResponse, error) {
	var resp FetchResponse
	_, err := c.do(ctx, "FETCH", fetchRequest{
		Queues:  queues,
		Timeout: timeout.String(),
	}, &resp)
	return &resp, err
}

type fetchRequest struct {
	Queues  []string `json:"queues"`
	Timeout string   `json:"timeout"`
}

type FetchResponse struct {
	ID       uint32          `json:"id"`
	Queue    string          `json:"queue"`
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Failures uint8           `json:"failures,omitempty"`
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
	ErrEmpty = fmt.Errorf("%w: empty", ErrClient)

	// ErrUnexpectedResponse is returned when an unexpected response for
	// made request is received.
	ErrUnexpectedResponse = fmt.Errorf("%w: unexpected response", ErrClient)
)

var emptyBody = json.RawMessage("{}")
