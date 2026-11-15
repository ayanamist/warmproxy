package warmer

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// meta is a run as it is persisted. The request headers carry the client's credentials,
// so the files are 0600 in a 0700 directory.
type meta struct {
	Key        string        `json:"key"`
	Model      string        `json:"model"`
	Path       string        `json:"path"`
	Query      string        `json:"query"`
	Headers    http.Header   `json:"headers"`
	Tokens     int           `json:"tokens"`
	Uncached   int           `json:"uncached"` // part of Tokens after the last cache breakpoint
	TTL        time.Duration `json:"ttl"`
	Phase      string        `json:"phase"`
	StartedAt  time.Time     `json:"started_at"`
	NextWarmAt time.Time     `json:"next_warm_at"`
	Misses     int           `json:"misses"`
	Warms      int           `json:"warms"`
	SpentUSD   float64       `json:"spent_usd"`
}

type stored struct {
	meta
	body []byte
}

// Store keeps runs on disk so a restart can pick them up while their cache entries are alive.
type Store struct{ dir string }

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path(key, ext string) string { return filepath.Join(s.dir, key+ext) }

// Save writes the run's meta, and its body too when body is not nil. The body goes first:
// a meta without its body is never loaded.
func (s *Store) Save(m meta, body []byte) error {
	if body != nil {
		if err := writeAtomic(s.path(m.Key, ".body"), body); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeAtomic(s.path(m.Key, ".json"), raw)
}

func (s *Store) Delete(key string) {
	os.Remove(s.path(key, ".json"))
	os.Remove(s.path(key, ".body"))
}

// Load returns every complete run; a half-written one is removed.
func (s *Store) Load() ([]stored, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []stored
	for _, e := range ents {
		key, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok {
			continue
		}
		raw, err := os.ReadFile(s.path(key, ".json"))
		var m meta
		if err == nil {
			err = json.Unmarshal(raw, &m)
		}
		var body []byte
		if err == nil {
			body, err = os.ReadFile(s.path(key, ".body"))
		}
		if err != nil || m.Key != key {
			s.Delete(key)
			continue
		}
		out = append(out, stored{m, body})
	}
	return out, nil
}

func writeAtomic(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
