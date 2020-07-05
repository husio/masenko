package masenko

import "sync/atomic"

func NewMetricsCounter() *MetricsCounter {
	return &MetricsCounter{}
}

type MetricsCounter struct {
	clients      int64
	push         int64
	fetch        int64
	ack          int64
	nack         int64
	request      int64
	requestError int64
}

func (m *MetricsCounter) State() map[string]int64 {
	cpy := map[string]int64{
		"clients":       atomic.LoadInt64(&m.clients),
		"push":          atomic.LoadInt64(&m.push),
		"fetch":         atomic.LoadInt64(&m.fetch),
		"ack":           atomic.LoadInt64(&m.ack),
		"nack":          atomic.LoadInt64(&m.nack),
		"request":       atomic.LoadInt64(&m.request),
		"request-error": atomic.LoadInt64(&m.requestError),
	}
	return cpy
}

func (c *MetricsCounter) ClientConnected() {
	atomic.AddInt64(&c.clients, 1)
}

func (c *MetricsCounter) ClientDisconnected() {
	atomic.AddInt64(&c.clients, -1)
}

func (c *MetricsCounter) TaskPushed() {
	atomic.AddInt64(&c.push, 1)
}

func (c *MetricsCounter) TaskFetched() {
	atomic.AddInt64(&c.fetch, 1)
}

func (c *MetricsCounter) TaskAcked() {
	atomic.AddInt64(&c.ack, 1)
}

func (c *MetricsCounter) TaskNacked() {
	atomic.AddInt64(&c.nack, 1)
}

func (c *MetricsCounter) RequestHandled() {
	atomic.AddInt64(&c.request, 1)
}

func (c *MetricsCounter) RequestErrored() {
	atomic.AddInt64(&c.requestError, 1)
}
