package proto

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/husio/masenko/store"
)

// MetricsCollector is implemeneted by any metric counter backend, for example
// a set of Prometheus collectors.
type MetricsCollector interface {
	IncrClient()
	DecrClient()
	IncrFetch()
	IncrPush()
	IncrAtomic()
	IncrAck()
	IncrNack()
	IncrPing()
	IncrInfo()
	IncrQuit()
	IncrResponseOK()
	IncrResponseEmpty()
	IncrResponseErr()
	IncrResponsePong()
}

func HandleClient(
	ctx context.Context,
	c io.ReadWriteCloser,
	q store.Queue,
	heartbeat time.Duration,
	metrics MetricsCollector,
) {
	defer c.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h := clientHandler{
		queue:     q,
		wr:        c,
		rd:        bufio.NewReader(c),
		heartbeat: heartbeat,
		metrics:   metrics,
	}
	h.lastMsg.Time = time.Now()

	h.metrics.IncrClient()
	defer h.metrics.DecrClient()

	go func() {
		timeout := time.After(heartbeat)
		for {

			select {
			case <-ctx.Done():
				return
			case now := <-timeout:
				h.lastMsg.Lock()
				lastMsg := h.lastMsg.Time
				h.lastMsg.Unlock()

				if lastMsg.Add(heartbeat).Before(now) {
					cancel()
					h.writeErr("no heartbeat")
					c.Close()
					return
				}
				timeout = time.After(heartbeat - now.Sub(lastMsg) + 10*time.Millisecond)
			}
		}
	}()

	if err := h.handleLoop(ctx); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "client failure: %s\n", err.Error())
		}
	}
}

type clientHandler struct {
	queue     store.Queue
	toack     []uint32
	wr        io.Writer
	rd        *bufio.Reader
	heartbeat time.Duration
	lastMsg   struct {
		sync.Mutex
		time.Time
	}
	metrics MetricsCollector
}

func (c *clientHandler) handleLoop(ctx context.Context) error {
	defer func() {
		for _, taskID := range c.toack {
			_ = c.queue.Acknowledge(ctx, taskID, false)
		}
	}()

	for {
		line, err := c.rd.ReadBytes('\n')
		if err != nil {
			return fmt.Errorf("read verb: %w", err)
		}

		c.lastMsg.Lock()
		c.lastMsg.Time = time.Now()
		c.lastMsg.Unlock()

		verb := line[:len(line)-1]
		var payload []byte
		for i, cc := range line {
			if cc == ' ' {
				verb = line[:i]
				payload = line[i+1 : len(line)-1]
				break
			}
		}

		switch strings.ToUpper(string(verb)) {
		case "ATOMIC":
			err = c.handleAtomic(ctx, payload)
		case "PUSH":
			err = c.handlePush(ctx, payload)
		case "FETCH":
			err = c.handleFetch(ctx, payload)
		case "ACK":
			err = c.handleAck(ctx, payload)
		case "NACK":
			err = c.handleNack(ctx, payload)
		case "PING":
			err = c.handlePing(ctx, payload)
		case "INFO":
			err = c.handleInfo(ctx, payload)
		case "QUIT":
			// Quit finalize the request/response loop.
			return c.handleQuit(ctx, payload)
		default:
			err = c.handleUnknownVerb(ctx, string(verb))
		}

		if err != nil {
			return err
		}
	}
}

func (c *clientHandler) handleQuit(ctx context.Context, payload []byte) error {
	c.metrics.IncrQuit()
	return c.write("OK", nil)
}

func (c *clientHandler) handleInfo(ctx context.Context, payload []byte) error {
	c.metrics.IncrInfo()

	info := struct {
		Queues []store.QueueStat `json:"queues"`
	}{
		Queues: c.queue.Stats(),
	}
	return c.write("OK", info)
}

func (c *clientHandler) handleAtomic(ctx context.Context, _ []byte) error {
	requests := make([]request, 0, 8)
	for {
		line, err := c.rd.ReadBytes('\n')
		if err != nil {
			return fmt.Errorf("read verb: %w", err)
		}

		var payload []byte
		verb := line[:len(line)-1]
		for i, cc := range line {
			if cc == ' ' {
				verb = line[:i]
				payload = line[i+1 : len(line)-1]
				break
			}
		}

		verb = bytes.ToUpper(verb)

		if bytes.Equal(verb, []byte("DONE")) {
			break
		}
		requests = append(requests, request{verb: verb, payload: payload})
	}

	tx := c.queue.Begin(ctx)
	defer tx.Rollback(ctx)

	// Keep track of Task IDs that should be removed from the list of IDs
	// to ACK. This list is maintained by the handler and not by the store.
	// Remove all IDs only if all operations succeedded.
	var acked []uint32

	var pushed []uint32
processAtomicRequests:
	for i, r := range requests {
		switch {
		case bytes.Equal(r.verb, []byte("ACK")):
			taskID, err := parseAckRequest(r.payload)
			if err != nil {
				return c.writeErr(fmt.Sprintf("message %d: ACK: %s", i, err))
			}
			for _, id := range c.toack {
				if id == taskID {
					if err := tx.Acknowledge(ctx, taskID, true); err != nil {
						return c.writeErr(fmt.Sprintf("message %d: ACK: %s", i, err))
					}
					acked = append(acked, taskID)
					continue processAtomicRequests
				}
			}
			return c.writeErr(fmt.Sprintf("task %d not acquired", taskID))
		case bytes.Equal(r.verb, []byte("PUSH")):
			req, err := parsePushRequest(r.payload)
			if err != nil {
				return c.writeErr(fmt.Sprintf("message %d: PUSH: %s", i, err))
			}

			var blockedBy []uint32
			for _, n := range req.BlockedBy {
				if n > 0 {
					blockedBy = append(blockedBy, uint32(n))
				} else {
					pos := int(n * -1)
					if pos == 0 || pos > len(pushed) {
						return c.writeErr(fmt.Sprintf("message %d: PUSH: invalid task position in blocked by", i))
					}
					blockedBy = append(blockedBy, pushed[len(pushed)-pos-1])
				}
			}

			task := store.Task{
				Name:      req.Name,
				Queue:     req.Queue,
				Deadqueue: req.Deadqueue,
				Payload:   req.Payload,
				Retry:     req.Retry,
				ExecuteAt: req.ExecuteAt,
				BlockedBy: blockedBy,
			}
			if id, err := tx.Push(ctx, task); err != nil {
				return c.writeErr(fmt.Sprintf("message %d: PUSH: %s", i, err))
			} else {
				pushed = append(pushed, id)
			}
		default:
			return c.writeErr(fmt.Sprintf("%s is not allowed in transaction.", string(r.verb)))
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return c.writeErr(fmt.Sprintf("commit transaction: %s", err))
	}

	c.metrics.IncrAtomic()
	for _, r := range requests {
		switch {
		case bytes.Equal(r.verb, []byte("ACK")):
			c.metrics.IncrAck()
		case bytes.Equal(r.verb, []byte("PUSH")):
			c.metrics.IncrPush()
		}
	}

	for _, taskID := range acked {
		for i, id := range c.toack {
			if id == taskID {
				// Order does not matter.
				c.toack[i] = c.toack[len(c.toack)-1]
				c.toack = c.toack[:len(c.toack)-1]
				break
			}
		}
	}

	return c.write("OK", atomicResponse{IDs: pushed})
}

type atomicResponse struct {
	IDs []uint32 `json:"ids,omitempty"`
}

type request struct {
	verb    []byte
	payload []byte
}

func (c *clientHandler) handlePush(ctx context.Context, payload []byte) error {
	req, err := parsePushRequest(payload)
	if err != nil {
		return c.writeErr(err.Error())
	}
	var blockedBy []uint32
	for _, n := range req.BlockedBy {
		if n <= 0 {
			return c.writeErr("cannot use relative blocked by outside of a transaction")
		}
		blockedBy = append(blockedBy, uint32(n))
	}
	task := store.Task{
		Name:      req.Name,
		Queue:     req.Queue,
		Deadqueue: req.Deadqueue,
		Payload:   req.Payload,
		Retry:     req.Retry,
		ExecuteAt: req.ExecuteAt,
		BlockedBy: blockedBy,
	}
	taskID, err := c.queue.Push(ctx, task)
	if err != nil {
		return c.writeErr(fmt.Sprintf("cannot push to queue: %s", err))
	}

	c.metrics.IncrPush()

	return c.write("OK", pushResponse{
		ID: taskID,
	})
}

func parsePushRequest(payload []byte) (*pushRequest, error) {
	input := pushRequest{
		Queue: "default",
		Retry: 20,
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, errors.New("cannot unmarshal payload")
	}

	if !validQueueName(input.Queue) {
		return nil, fmt.Errorf("invalid queue name %q", input.Queue)
	}
	if len(input.Deadqueue) > 0 && !validQueueName(input.Deadqueue) {
		return nil, fmt.Errorf("invalid queue name %q", input.Queue)
	}
	if !validTaskName(input.Name) {
		return nil, fmt.Errorf("invalid task name %q", input.Name)
	}
	if input.ExecuteAt != nil {
		t := input.ExecuteAt.Truncate(time.Second).UTC()
		input.ExecuteAt = &t
	}
	if len(input.BlockedBy) > 255 {
		return nil, errors.New("at most 255 blocked by task IDs can be provided")
	}
	return &input, nil
}

type pushRequest struct {
	Name      string          `json:"name"`
	Queue     string          `json:"queue"`
	Deadqueue string          `json:"deadqueue"`
	Payload   json.RawMessage `json:"payload"`
	Retry     uint8           `json:"retry"`
	ExecuteAt *time.Time      `json:"execute_at,omitempty"`
	BlockedBy []int64         `json:"blocked_by,omitempty"`
}

type pushResponse struct {
	ID uint32 `json:"id"`
}

var (
	validQueueName = regexp.MustCompile(`[0-9A-Za-z_\-]{1,32}`).MatchString
	validTaskName  = regexp.MustCompile(`[0-9A-Za-z_\-]{1,64}`).MatchString
)

func (c *clientHandler) handleFetch(ctx context.Context, payload []byte) error {
	input := fetchRequest{
		Queues:  []string{"default"},
		Timeout: SDuration(c.heartbeat / 2),
	}
	if len(payload) != 0 {
		if err := json.Unmarshal(payload, &input); err != nil {
			return c.writeErr("cannot unmarshal payload")
		}
	}
	if input.Timeout <= 0 {
		return c.writeErr("timeout must be greater than zero")
	}
	if input.Timeout.Duration() > c.heartbeat {
		return c.writeErr(fmt.Sprintf("timeout must be smaller than the heartbeat frequency %s", c.heartbeat))
	}
	if len(input.Queues) == 0 {
		return c.writeEmpty()
	}

	ctx, cancel := context.WithTimeout(ctx, input.Timeout.Duration())
	defer cancel()

	switch task, err := c.queue.Pull(ctx, input.Queues); {
	case err == nil:
		c.toack = append(c.toack, task.ID)
		c.metrics.IncrFetch()
		return c.write("OK", fetchResponse{
			ID:       task.ID,
			Queue:    task.Queue,
			Name:     task.Name,
			Payload:  task.Payload,
			Failures: task.Failures,
		})
	case errors.Is(err, context.DeadlineExceeded):
		return c.writeEmpty()
	default:
		return fmt.Errorf("queue fetch: %w", err)
	}
}

type fetchRequest struct {
	Queues  []string  `json:"queues"`
	Timeout SDuration `json:"timeout"`
}

type fetchResponse struct {
	ID       uint32          `json:"id"`
	Queue    string          `json:"queue"`
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Failures uint8           `json:"failures,omitempty"`
}

func (c *clientHandler) handleAck(ctx context.Context, payload []byte) error {
	taskID, err := parseAckRequest(payload)
	if err != nil {
		return c.writeErr(err.Error())
	}

	for i, id := range c.toack {
		if id == taskID {
			// Order does not matter.
			c.toack[i] = c.toack[len(c.toack)-1]
			c.toack = c.toack[:len(c.toack)-1]

			if err := c.queue.Acknowledge(ctx, taskID, true); err != nil {
				return c.writeErr(fmt.Sprintf("acknowledge: %s", err))
			}
			c.metrics.IncrAck()
			return c.write("OK", nil)
		}
	}
	return c.writeErr("task not acquired")
}

func parseAckRequest(payload []byte) (uint32, error) {
	var input ackRequest
	if err := json.Unmarshal(payload, &input); err != nil {
		return 0, fmt.Errorf("unmarshal payload: %s", err)
	}
	if input.ID == 0 {
		return 0, errors.New("missing id")
	}
	return input.ID, nil
}

type ackRequest struct {
	ID uint32 `json:"id"`
}

func (c *clientHandler) handleNack(ctx context.Context, payload []byte) error {
	var input nackRequest
	if err := json.Unmarshal(payload, &input); err != nil {
		return c.writeErr(fmt.Sprintf("unmarshal payload: %s", err))
	}
	if input.ID == 0 {
		return c.writeErr("missing id")
	}
	for i, id := range c.toack {
		if id == input.ID {
			// Order does not matter.
			c.toack[i] = c.toack[len(c.toack)-1]
			c.toack = c.toack[:len(c.toack)-1]

			if err := c.queue.Acknowledge(ctx, input.ID, false); err != nil {
				return c.writeErr(fmt.Sprintf("acknowledge: %s", err))
			}
			c.metrics.IncrNack()
			return c.write("OK", nil)
		}
	}
	return c.writeErr("task not acquired")
}

type nackRequest struct {
	ID uint32 `json:"id"`
}

func (c *clientHandler) handlePing(ctx context.Context, payload []byte) error {
	return c.write("PONG", nil)
}

func (c *clientHandler) handleUnknownVerb(ctx context.Context, verb string) error {
	return c.writeErr("unknown verb " + verb)
}

func (c *clientHandler) write(verb string, payload interface{}) error {
	var b bytes.Buffer
	if _, err := fmt.Fprintf(&b, "%s ", verb); err != nil {
		return fmt.Errorf("write verb: %w", err)
	}
	if payload != nil {
		// Encode finish with a LF character.
		if err := json.NewEncoder(&b).Encode(payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	} else {
		if _, err := io.WriteString(&b, "{}\n"); err != nil {
			return fmt.Errorf("write LF: %w", err)
		}
	}
	if _, err := b.WriteTo(c.wr); err != nil {
		return fmt.Errorf("write to output: %w", err)
	}

	switch verb {
	case "OK":
		c.metrics.IncrResponseOK()
	case "EMPTY":
		c.metrics.IncrResponseEmpty()
	case "PONG":
		c.metrics.IncrResponsePong()
	case "ERR":
		c.metrics.IncrResponseErr()
	}

	return nil
}

func (c *clientHandler) writeErr(msg string) error {
	return c.write("ERR", errResponse{Msg: msg})
}

type errResponse struct {
	Msg string `json:"msg"`
}

func (c *clientHandler) writeEmpty() error {
	if _, err := io.WriteString(c.wr, "EMPTY {}\n"); err != nil {
		return fmt.Errorf("writing empty response: %w", err)
	}
	return nil
}
