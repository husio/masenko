package webui

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/husio/masenko/store"
)

type WebUI struct {
	queue   store.Queue
	metrics Stater
}

type Stater interface {
	State() map[string]int64
}

func NewWebUI(queue store.Queue, metrics Stater) *WebUI {
	return &WebUI{
		queue:   queue,
		metrics: metrics,
	}
}

//go:generate inlineasset webui WebUIHTML ./webui.html ./asset_webui.html.go
//go:generate inlineasset webui MithrilJS ./mithril.min.js ./asset_mithril.js.go

func (ui *WebUI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/":
		http.ServeContent(w, r, "index.html", startTime, bytes.NewReader(WebUIHTML))
	case "/stats.json":
		ui.handleCurrentStats(w, r)
	case "/js/mithril.js":
		http.ServeContent(w, r, "mithril.html", startTime, bytes.NewReader(MithrilJS))
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

var startTime = time.Now()

func (ui *WebUI) handleCurrentStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, struct {
		Queues  []store.QueueStat `json:"queues"`
		Metrics map[string]int64  `json:"metrics"`
	}{
		Queues:  ui.queue.Stats(),
		Metrics: ui.metrics.State(),
	})
}

// writeJSON writes a HTTP response with JSON serialized payload.
func writeJSON(w http.ResponseWriter, code int, content interface{}) {
	b, err := json.Marshal(content)
	if err != nil {
		code = http.StatusInternalServerError
		b = []byte(`{"errors":["Internal Server Errror"]}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)
	w.Write(b)
	w.Write([]byte{'\n'}) // Be nice to CLI.
}
