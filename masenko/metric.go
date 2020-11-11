package masenko

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// NewMetrics returns an instance of Prometheus metrics collector that
// registers within given registerer. No counter is exposed and all metric
// collecting is exposed via specialized methods.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		clients: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "masenko",
			Name:      "clients_total",
			Help:      "Current number of connected clients.",
		}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "masenko",
			Name:      "requests_total",
			Help:      "Total number of requests processed.",
		}, []string{"verb"}),
		responses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "masenko",
			Name:      "responses_total",
			Help:      "Total number of responses.",
		}, []string{"verb"}),
		queues: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "masenko",
			Name:      "tasks_total",
			Help:      "The number of tasks in each queue ready to be fetched.",
		}, []string{"queue", "kind"}),
	}
	if err := reg.Register(m.clients); err != nil {
		return nil, fmt.Errorf("register clients: %w", err)
	}
	if err := reg.Register(m.requests); err != nil {
		return nil, fmt.Errorf("register requests: %w", err)
	}
	if err := reg.Register(m.responses); err != nil {
		return nil, fmt.Errorf("register responses: %w", err)
	}
	if err := reg.Register(m.queues); err != nil {
		return nil, fmt.Errorf("register queues: %w", err)
	}
	return m, nil
}

// Metrics clubs together various metric counters and expose a single interface
// to collect data.
type Metrics struct {
	clients   prometheus.Gauge
	requests  *prometheus.CounterVec
	responses *prometheus.CounterVec
	queues    *prometheus.GaugeVec
}

// IncrClient must be called when a new client is connected.
func (m Metrics) IncrClient() { m.clients.Inc() }

// DecrClient must be called when a client disconnects.
func (m Metrics) DecrClient() { m.clients.Dec() }

func (m *Metrics) IncrFetch()         { m.requests.With(prometheus.Labels{"verb": "FETCH"}).Inc() }
func (m *Metrics) IncrPush()          { m.requests.With(prometheus.Labels{"verb": "PUSH"}).Inc() }
func (m *Metrics) IncrAtomic()        { m.requests.With(prometheus.Labels{"verb": "ATOMIC"}).Inc() }
func (m *Metrics) IncrAck()           { m.requests.With(prometheus.Labels{"verb": "ACK"}).Inc() }
func (m *Metrics) IncrNack()          { m.requests.With(prometheus.Labels{"verb": "NACK"}).Inc() }
func (m *Metrics) IncrPing()          { m.requests.With(prometheus.Labels{"verb": "PING"}).Inc() }
func (m *Metrics) IncrInfo()          { m.requests.With(prometheus.Labels{"verb": "INFO"}).Inc() }
func (m *Metrics) IncrQuit()          { m.requests.With(prometheus.Labels{"verb": "QUIT"}).Inc() }
func (m *Metrics) IncrResponseOK()    { m.responses.With(prometheus.Labels{"verb": "OK"}).Inc() }
func (m *Metrics) IncrResponseErr()   { m.responses.With(prometheus.Labels{"verb": "ERR"}).Inc() }
func (m *Metrics) IncrResponseEmpty() { m.responses.With(prometheus.Labels{"verb": "EMPTY"}).Inc() }
func (m *Metrics) IncrResponsePong()  { m.responses.With(prometheus.Labels{"verb": "PONG"}).Inc() }

func (m *Metrics) IncrQueue(queueName, kind string) {
	m.queues.With(prometheus.Labels{"queue": queueName, "kind": kind}).Inc()
}
func (m *Metrics) DecrQueue(queueName, kind string) {
	m.queues.With(prometheus.Labels{"queue": queueName, "kind": kind}).Dec()
}

func (m *Metrics) SetQueueSize(queueName, kind string, n int) {
	m.queues.With(prometheus.Labels{"queue": queueName, "kind": kind}).Set(float64(n))
}
