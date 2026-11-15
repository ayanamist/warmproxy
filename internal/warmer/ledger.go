package warmer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ayanamist/warmproxy/internal/anthropic"
)

// Entry is one refresh; it carries no request content.
type Entry struct {
	Time         time.Time       `json:"time"`
	Key          string          `json:"key"`
	Model        string          `json:"model"`
	Phase        string          `json:"phase"`
	PromptTokens int             `json:"prompt_tokens"`
	Usage        anthropic.Usage `json:"usage"`
	Hit          bool            `json:"hit"`
	CostUSD      float64         `json:"cost_usd"`
	Status       int             `json:"status"`
	Millis       int64           `json:"ms"`
	Error        string          `json:"error,omitempty"`
}

type Ledger interface{ Record(Entry) }

// FileLedger appends entries as JSON lines.
type FileLedger struct {
	mu sync.Mutex
	f  *os.File
}

func OpenLedger(path string) (*FileLedger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileLedger{f: f}, nil
}

func (l *FileLedger) Record(e Entry) {
	raw, err := json.Marshal(e)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.f.Write(append(raw, '\n'))
}

func (l *FileLedger) Close() error { return l.f.Close() }
