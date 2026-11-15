package proxy

import (
	"bytes"
	"compress/gzip"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayanamist/warmproxy/internal/config"
	"github.com/ayanamist/warmproxy/internal/warmer"
)

const sse = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":99990}}}\n\n" +
	"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":5}}\n\n" +
	"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

const reqBody = `{"model":"claude-sonnet-5","max_tokens":100,"stream":true,"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hi"}]}`

type env struct {
	proxy *httptest.Server
	mgr   *warmer.Manager
	mu    sync.Mutex
	seen  []seen
}

type seen struct {
	path, accept string
	body         string
}

func setup(t *testing.T, reply http.HandlerFunc) *env {
	t.Helper()
	e := &env{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		e.mu.Lock()
		e.seen = append(e.seen, seen{r.URL.Path, r.Header.Get("Accept-Encoding"), string(b)})
		e.mu.Unlock()
		reply(w, r)
	}))
	t.Cleanup(up.Close)

	p := filepath.Join(t.TempDir(), "c.toml")
	os.WriteFile(p, []byte("[[model]]\nmatch = \"^claude\"\ninput = 3\noutput = 15\n"), 0o600)
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Upstream, _ = url.Parse(up.URL)
	cfg.MaxBodyBytes = 1 << 20
	st, _ := warmer.NewStore(t.TempDir())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	e.mgr = warmer.New(warmer.Options{Config: cfg, Doer: http.DefaultClient, Store: st, Log: log})
	t.Cleanup(e.mgr.Close)
	e.proxy = httptest.NewServer(New(cfg, e.mgr, http.DefaultTransport, log))
	t.Cleanup(e.proxy.Close)
	return e
}

func post(t *testing.T, e *env, path, body string, h map[string]string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", e.proxy.URL+path, strings.NewReader(body))
	req.Header.Set("X-Api-Key", "k")
	req.Header.Set("X-Session-Affinity", "sess")
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func streamReply(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(w, sse)
}

func TestStreamIsForwardedAndArms(t *testing.T) {
	e := setup(t, streamReply)
	resp, got := post(t, e, "/v1/messages", reqBody, nil)
	if resp.StatusCode != 200 || got != sse {
		t.Fatalf("%d, reply changed: %q", resp.StatusCode, got)
	}
	if e.seen[0].body != reqBody {
		t.Error("the request was altered on its way")
	}
	st := e.mgr.Status()
	if len(st) != 1 || st[0].Tokens != 100_000 || st[0].Phase != "streaming" {
		t.Errorf("status %+v", st)
	}
}

// The reply reaches the client chunk by chunk, not when the upstream is done.
func TestStreamIsNotBuffered(t *testing.T) {
	release := make(chan struct{})
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse[:40])
		w.(http.Flusher).Flush()
		<-release
		io.WriteString(w, sse[40:])
	})
	req, _ := http.NewRequest("POST", e.proxy.URL+"/v1/messages", strings.NewReader(reqBody))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 40)
	done := make(chan struct{})
	go func() { io.ReadFull(resp.Body, buf); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the first chunk did not arrive before the stream ended")
	}
	close(release)
}

func TestJSONReplyArms(t *testing.T) {
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":1,"cache_read_input_tokens":999999}}`)
	})
	post(t, e, "/v1/messages", strings.Replace(reqBody, `"stream":true`, `"stream":false`, 1), nil)
	if st := e.mgr.Status(); len(st) != 1 || st[0].Phase != "idle" || st[0].Tokens != 1_000_000 {
		t.Errorf("status %+v", st)
	}
}

func TestGzipReplyIsReadable(t *testing.T) {
	e := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		io.WriteString(zw, sse)
		zw.Close()
	})
	// Not through http.DefaultClient: it would add Accept-Encoding and unzip by itself.
	req, _ := http.NewRequest("POST", e.proxy.URL+"/v1/messages", strings.NewReader(reqBody))
	req.Header.Set("Accept-Encoding", "gzip, br")
	req.Header.Set("X-Session-Affinity", "sess")
	resp, err := (&http.Client{Transport: &http.Transport{DisableCompression: true}}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if len(e.mgr.Status()) != 1 {
		t.Error("a compressed reply was not read")
	}
	if ae := e.seen[0].accept; strings.Contains(ae, "br") {
		t.Errorf("the upstream was offered %q", ae)
	}
}

func TestNothingArmedFor(t *testing.T) {
	for name, c := range map[string]struct {
		path, body string
		reply      http.HandlerFunc
	}{
		"other model": {"/v1/messages", strings.Replace(reqBody, "claude-sonnet-5", "claude-haiku-5", 1), streamReply},
		"other path":  {"/v1/messages/count_tokens", reqBody, streamReply},
		"error reply": {"/v1/messages", reqBody, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(529) }},
		"cut stream": {"/v1/messages", reqBody, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, sse[:strings.Index(sse, "event: message_stop")])
		}},
		"not json": {"/v1/messages", "nope", streamReply},
	} {
		e := setup(t, c.reply)
		resp, _ := post(t, e, c.path, c.body, nil)
		resp.Body.Close()
		if len(e.seen) != 1 {
			t.Errorf("%s: the request did not reach the upstream (%d)", name, len(e.seen))
		}
		if len(e.mgr.Status()) != 0 {
			t.Errorf("%s: armed", name)
		}
	}
}

// The new request is what wrote the newest cache entry, so a model that is not warmed ends the
// run of the same conversation; another agent's run (other system prompt) stays.
func TestUnwarmedModelEndsTheSameConversationsRun(t *testing.T) {
	e := setup(t, streamReply)
	post(t, e, "/v1/messages", reqBody, nil)
	if len(e.mgr.Status()) != 1 {
		t.Fatal("not armed")
	}
	haiku := strings.Replace(reqBody, "claude-sonnet-5", "claude-haiku-5", 1)
	post(t, e, "/v1/messages", strings.Replace(haiku, `"text":"s"`, `"text":"sub"`, 1), nil)
	if len(e.mgr.Status()) != 1 {
		t.Fatal("another agent's request ended the run")
	}
	post(t, e, "/v1/messages", haiku, nil)
	if len(e.mgr.Status()) != 0 {
		t.Error("the run of the model the agent left is still warming")
	}
}

func TestOversizedBody(t *testing.T) {
	e := setup(t, streamReply)
	resp, _ := post(t, e, "/v1/messages", strings.Repeat("x", 2<<20), nil)
	if resp.StatusCode != http.StatusRequestEntityTooLarge || len(e.seen) != 0 {
		t.Errorf("status %d, %d upstream requests", resp.StatusCode, len(e.seen))
	}
}

func TestStatusIsLocalOnly(t *testing.T) {
	e := setup(t, streamReply)
	resp, err := http.Get(e.proxy.URL + "/_warm/status")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(b, []byte(`"runs"`)) {
		t.Errorf("%d %s", resp.StatusCode, b)
	}
}
