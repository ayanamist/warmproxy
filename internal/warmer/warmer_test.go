package warmer

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayanamist/warmproxy/internal/anthropic"
	"github.com/ayanamist/warmproxy/internal/clock"
	"github.com/ayanamist/warmproxy/internal/config"
	"github.com/ayanamist/warmproxy/internal/policy"
)

var t0 = time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

// upstream stands in for magpie: it records each refresh and answers with the usage it is told.
type upstream struct {
	mu     sync.Mutex
	got    []string
	urls   []string
	heads  []http.Header
	status int
	usage  anthropic.Usage
}

func (u *upstream) Do(r *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(r.Body)
	u.mu.Lock()
	defer u.mu.Unlock()
	u.got = append(u.got, string(b))
	u.urls = append(u.urls, r.URL.String())
	u.heads = append(u.heads, r.Header.Clone())
	rec := httptest.NewRecorder()
	code := u.status
	if code == 0 {
		code = 200
	}
	rec.WriteHeader(code)
	if strings.HasSuffix(r.URL.Path, anthropic.ChatPath) {
		json.NewEncoder(rec).Encode(map[string]any{
			"choices": []map[string]string{{"finish_reason": "length"}},
			"usage": map[string]any{
				"prompt_tokens": u.usage.Prompt(), "completion_tokens": u.usage.Output,
				"prompt_tokens_details": map[string]int{"cached_tokens": u.usage.CacheRead, "cache_write_tokens": u.usage.CacheWrite},
			},
		})
	} else {
		json.NewEncoder(rec).Encode(map[string]any{"type": "message", "stop_reason": "max_tokens", "usage": u.usage})
	}
	return rec.Result(), nil
}

func (u *upstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.got)
}

type fixture struct {
	m    *Manager
	clk  *clock.Fake
	up   *upstream
	dir  string
	cfg  *config.Config
	ledg *memLedger
}

type memLedger struct {
	mu sync.Mutex
	es []Entry
}

func (l *memLedger) Record(e Entry) { l.mu.Lock(); l.es = append(l.es, e); l.mu.Unlock() }

func newCfg(t *testing.T, mode string) *config.Config {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.toml")
	text := "warm.mode = \"" + mode + "\"\n[[model]]\nmatch = \"^([^/]+/)*claude-\"\ninput = 3\noutput = 15\n"
	if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := config.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	c.Upstream, _ = url.Parse("http://magpie.test")
	return c
}

func newFixture(t *testing.T, mode string, dir string) *fixture {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	st, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{
		clk: clock.NewFake(t0), up: &upstream{usage: anthropic.Usage{CacheRead: 100_000, Input: 20, Output: 1}},
		dir: dir, cfg: newCfg(t, mode), ledg: &memLedger{},
	}
	f.m = New(Options{Config: f.cfg, Clock: f.clk, Doer: f.up, Store: st, Ledger: f.ledg})
	t.Cleanup(f.m.Close)
	return f
}

const body = `{"model":"claude-sonnet-5","max_tokens":8000,"stream":true,"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hi"}]}`

func armInput(t *testing.T, key, stop string, tokens int) ArmInput {
	t.Helper()
	info, err := anthropic.Inspect([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	h := http.Header{"X-Api-Key": {"k"}, "Anthropic-Beta": {"b"}, "Accept-Encoding": {"gzip"}, "X-Forwarded-For": {"1.2.3.4"}}
	return ArmInput{
		Key: key, Info: info, Path: "/v1/messages", Header: h, Body: []byte(body),
		Result: anthropic.Result{Usage: anthropic.Usage{CacheRead: tokens}, StopReason: stop, Complete: true},
	}
}

func key(c byte) string { return strings.Repeat(string(c), 32) }

func TestRefreshesBeforeExpiryWithOneToken(t *testing.T) {
	f := newFixture(t, "idle", "")
	// streaming phase, 100k tokens: pays off
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	f.clk.Advance(269 * time.Second)
	if f.up.count() != 0 {
		t.Fatal("refreshed before 90% of the lifetime")
	}
	f.clk.Advance(2 * time.Second)
	if f.up.count() != 1 {
		t.Fatalf("%d refreshes at 271s", f.up.count())
	}
	var sent map[string]any
	json.Unmarshal([]byte(f.up.got[0]), &sent)
	if sent["max_tokens"] != float64(1) || sent["stream"] != false || sent["model"] != "claude-sonnet-5" {
		t.Errorf("refresh body %v", sent)
	}
	h := f.up.heads[0]
	if h.Get("X-Api-Key") != "k" || h.Get("Anthropic-Beta") != "b" || h.Get("Accept-Encoding") != "" || h.Get("X-Forwarded-For") != "" {
		t.Errorf("replayed headers %v", h)
	}
	// and again on the next cadence, until the hour is up
	f.clk.Advance(270 * time.Second)
	if f.up.count() != 2 {
		t.Errorf("%d refreshes after two periods", f.up.count())
	}
	st := f.m.Status()
	if len(st) != 1 || st[0].Warms != 2 || st[0].SpentUSD <= 0 {
		t.Errorf("status %+v", st)
	}
}

func TestNewRequestReplacesTheRun(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	f.clk.Advance(200 * time.Second)
	f.m.Arm(armInput(t, key('a'), "tool_use", 110_000))
	f.clk.Advance(100 * time.Second)
	if f.up.count() != 0 {
		t.Error("the replaced run's timer fired")
	}
	f.clk.Advance(175 * time.Second)
	if f.up.count() != 1 {
		t.Errorf("%d refreshes", f.up.count())
	}
	if len(f.m.Status()) != 1 {
		t.Error("one conversation must hold one run")
	}
}

func TestIdleBelowBreakEvenIsNotWarmed(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.m.Arm(armInput(t, key('a'), "end_turn", 100_000)) // idle needs ~230k
	f.clk.Advance(5 * time.Minute)
	if f.up.count() != 0 {
		t.Error("warmed an idle conversation below the break-even size")
	}
	if len(f.m.Status()) != 0 {
		t.Error("a run that was refused must not stay")
	}
	f.m.Arm(armInput(t, key('b'), "end_turn", 300_000))
	f.clk.Advance(5 * time.Minute)
	if f.up.count() != 1 {
		t.Error("an idle conversation above the break-even size was not warmed")
	}
}

func TestStreamingModeIgnoresIdle(t *testing.T) {
	f := newFixture(t, "streaming", "")
	f.m.Arm(armInput(t, key('a'), "end_turn", 900_000))
	f.clk.Advance(5 * time.Minute)
	if f.up.count() != 0 || len(f.m.Status()) != 0 {
		t.Error("streaming mode warmed a settled run")
	}
}

func TestSafetyWindow(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.cfg.MaxAgeIdle = 20 * time.Minute
	f.up.usage = anthropic.Usage{CacheRead: 900_000, Output: 1}
	f.m.Arm(armInput(t, key('a'), "end_turn", 900_000))
	f.clk.Advance(25 * time.Minute)
	if n := f.up.count(); n != 4 { // 4.5, 9, 13.5 and 18 minutes
		t.Errorf("%d refreshes inside a 20 minute window", n)
	}
	if len(f.m.Status()) != 0 {
		t.Error("the run outlived its window")
	}
}

func TestMissesStopWarming(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.up.usage = anthropic.Usage{CacheWrite: 100_000, Output: 1} // read nothing, wrote it again
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	f.clk.Advance(10 * time.Minute)
	if f.up.count() != f.cfg.StopAfterMisses {
		t.Errorf("%d refreshes before giving up, want %d", f.up.count(), f.cfg.StopAfterMisses)
	}
	if len(f.m.Status()) != 0 {
		t.Error("still warming after repeated misses")
	}
}

func TestUpstreamErrors(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.up.status = 529
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	f.clk.Advance(271 * time.Second)
	if len(f.m.Status()) != 1 {
		t.Error("an overloaded upstream must not end warming while the cache entry lives")
	}
	f.up.status = 401
	f.clk.Advance(5 * time.Minute)
	if len(f.m.Status()) != 0 {
		t.Error("a refused credential must end warming")
	}
}

// A failed refresh does not extend the cache entry: retries stay inside its remaining life,
// and a recovery after it expired must not send a full-price write.
func TestFailedRefreshKeepsTheCacheDeadline(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.up.status = 529
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	f.clk.Advance(310 * time.Second) // the entry written at 0s expired at 300s
	if len(f.m.Status()) != 0 {
		t.Error("still warming an entry that has expired")
	}
	sent := f.up.count()
	f.up.status = 200
	f.clk.Advance(5 * time.Minute)
	if f.up.count() != sent {
		t.Errorf("%d refreshes after the cache entry expired", f.up.count()-sent)
	}
}

// queueRefreshFor arms a run, lets its refresh pass the checks and queue for a slot, holds it
// there for wait, then frees the slots and returns once the refresh has been decided.
func queueRefreshFor(t *testing.T, f *fixture, wait time.Duration) {
	t.Helper()
	for range cap(f.m.sem) {
		f.m.sem <- struct{}{}
	}
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	done := make(chan struct{})
	go func() { f.clk.Advance(270 * time.Second); close(done) }()
	// checkLocked records its decision once the first check has passed: the refresh is then
	// on its way to the slot queue, and time may move on.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		f.m.mu.Lock()
		checked := f.m.runs[key('a')] != nil && f.m.runs[key('a')].decision != ""
		f.m.mu.Unlock()
		if checked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the refresh never passed its first check")
		}
	}
	f.clk.Advance(wait)
	for range cap(f.m.sem) {
		<-f.m.sem
	}
	<-done
}

// A refresh that waited for a free slot past its deadline must not be sent.
func TestQueuedRefreshIsRecheckedWhenItsTurnComes(t *testing.T) {
	f := newFixture(t, "idle", "")
	queueRefreshFor(t, f, 40*time.Second)
	if f.up.count() != 0 {
		t.Error("a refresh was sent after its cache entry expired in the queue")
	}
	if len(f.m.Status()) != 0 {
		t.Error("the run outlived its deadline")
	}
}

// The wait can also end the safety window while the cache entry is still alive.
func TestQueuedRefreshIsRecheckedAgainstTheSafetyWindow(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.cfg.MaxAgeStreaming = 275 * time.Second
	queueRefreshFor(t, f, 10*time.Second) // 280s: past the window, before the 285s deadline
	if f.up.count() != 0 {
		t.Error("a refresh was sent after the safety window ended in the queue")
	}
	if len(f.m.Status()) != 0 {
		t.Error("the run outlived its safety window")
	}
}

// The refresh goes where a forwarded request would, whatever shape the upstream path has.
func TestRefreshURLFollowsSetURL(t *testing.T) {
	for _, raw := range []string{
		"http://magpie.test",
		"http://magpie.test/api/?tenant=abc",
		"http://magpie.test/api%2Ftenant?tenant=abc",
		"http://magpie.test/api//?tenant=abc",
	} {
		f := newFixture(t, "idle", "")
		f.cfg.Upstream, _ = url.Parse(raw)
		in := armInput(t, key('a'), "tool_use", 100_000)
		in.Query = "beta=true"
		f.m.Arm(in)
		f.clk.Advance(271 * time.Second)
		if f.up.count() != 1 {
			t.Fatalf("%s: %d refreshes", raw, f.up.count())
		}

		in2 := &http.Request{URL: &url.URL{Path: "/v1/messages", RawQuery: "beta=true"}}
		out := &http.Request{URL: &url.URL{Path: "/v1/messages", RawQuery: "beta=true"}}
		(&httputil.ProxyRequest{In: in2, Out: out}).SetURL(f.cfg.Upstream)
		if f.up.urls[0] != out.URL.String() {
			t.Errorf("%s: refresh went to %s, forwarding goes to %s", raw, f.up.urls[0], out.URL)
		}
	}
}

// The cache entry's life counts from when the request was sent, not from the end of its reply.
func TestDeadlineCountsFromTheRequestStart(t *testing.T) {
	f := newFixture(t, "idle", "")
	in := armInput(t, key('a'), "tool_use", 100_000)
	in.Started = t0
	f.clk.Advance(45 * time.Second) // the reply took 45s
	f.m.Arm(in)
	f.clk.Advance(224 * time.Second) // 269s after the request
	if f.up.count() != 0 {
		t.Fatal("refreshed before 90% of the lifetime")
	}
	f.clk.Advance(2 * time.Second)
	if f.up.count() != 1 {
		t.Fatalf("%d refreshes 271s after the request", f.up.count())
	}
	f.clk.Advance(270 * time.Second) // the next one counts from the refresh just sent
	if f.up.count() != 2 {
		t.Errorf("%d refreshes", f.up.count())
	}

	g := newFixture(t, "idle", "")
	in = armInput(t, key('a'), "tool_use", 100_000)
	in.Started = t0
	g.clk.Advance(310 * time.Second) // the reply outlived the cache entry
	g.m.Arm(in)
	if len(g.m.Status()) != 0 {
		t.Error("armed a cache entry that had already expired")
	}
}

// Tokens after the last cache breakpoint are billed as plain input on every replay.
func TestUncachedSuffixCountsAgainstWarming(t *testing.T) {
	f := newFixture(t, "idle", "")
	in := armInput(t, key('a'), "tool_use", 0)
	in.Result.Usage = anthropic.Usage{Input: 98_000, CacheWrite: 2_000}
	f.m.Arm(in)
	f.clk.Advance(271 * time.Second)
	if f.up.count() != 0 || len(f.m.Status()) != 0 {
		t.Error("warmed 2k cached tokens that cost $0.29 of suffix per replay")
	}
	in.Result.Usage = anthropic.Usage{Input: 1_000, CacheWrite: 100_000}
	f.m.Arm(in)
	if len(f.m.Status()) != 1 {
		t.Error("a large cached prefix with a small suffix was not warmed")
	}
	f.up.usage = anthropic.Usage{Input: 1_000, CacheRead: 100_000, Output: 1}
	f.clk.Advance(271 * time.Second)
	if st := f.m.Status(); len(st) != 1 || st[0].Misses != 0 {
		t.Errorf("a refresh that read the whole prefix counted as a miss: %+v", st)
	}
}

func hourInput(t *testing.T, key string, u anthropic.Usage) ArmInput {
	t.Helper()
	in := armInput(t, key, "tool_use", 0)
	in.Body = []byte(strings.Replace(body, `"ephemeral"`, `"ephemeral","ttl":"1h"`, 1))
	info, err := anthropic.Inspect(in.Body)
	if err != nil {
		t.Fatal(err)
	}
	in.Info, in.Result.Usage = info, u
	return in
}

// An upstream that turns the one hour the request asked for into five minutes must still be
// kept warm, on the five minute schedule.
func TestShortenedLifetimeIsWarmedOnTheShortSchedule(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.m.Arm(hourInput(t, key('a'), anthropic.Usage{
		CacheWrite: 100_000, Creation: anthropic.CacheCreation{Ephemeral5m: 100_000},
	}))
	f.clk.Advance(271 * time.Second)
	if f.up.count() != 1 {
		t.Fatalf("%d refreshes at 271s of a five minute entry", f.up.count())
	}

	g := newFixture(t, "idle", "")
	g.m.Arm(hourInput(t, key('a'), anthropic.Usage{
		CacheWrite: 100_000, Creation: anthropic.CacheCreation{Ephemeral1h: 100_000},
	}))
	g.clk.Advance(271 * time.Second)
	if g.up.count() != 0 {
		t.Errorf("%d refreshes: an entry written for one hour kept the five minute schedule", g.up.count())
	}
}

// A reply that read the whole prefix says nothing about its lifetime; the first refresh that
// writes reports a shorter lifetime, and the schedule follows it from then on.
func TestRefreshLearnsShortenedLifetime(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.up.usage = anthropic.Usage{CacheWrite: 100_000, Creation: anthropic.CacheCreation{Ephemeral5m: 100_000}, Output: 1}
	f.m.Arm(hourInput(t, key('a'), anthropic.Usage{
		CacheWrite: 100_000, Creation: anthropic.CacheCreation{Ephemeral1h: 100_000},
	}))
	f.clk.Advance(54*time.Minute + time.Second)
	if f.up.count() != 1 {
		t.Fatalf("%d refreshes at 54 minutes", f.up.count())
	}
	if st := f.m.Status(); len(st) != 1 || st[0].NextWarmAt.After(f.clk.Now().Add(5*time.Minute)) {
		t.Fatalf("the next refresh is not on the five minute schedule: %+v", st)
	}
	f.up.usage = anthropic.Usage{CacheRead: 100_000, Output: 1}
	f.clk.Advance(271 * time.Second)
	if f.up.count() != 2 {
		t.Errorf("%d refreshes after one five minute period", f.up.count())
	}
}

// The schedule keeps the shorter lifetime once it is learned, but a refresh is billed for what
// it reports having written.
func TestRefreshCostFollowsReportedWriteLifetime(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.up.usage = anthropic.Usage{CacheWrite: 100_000, Creation: anthropic.CacheCreation{Ephemeral1h: 100_000}, Output: 1}
	f.m.Arm(hourInput(t, key('a'), anthropic.Usage{
		CacheWrite: 100_000, Creation: anthropic.CacheCreation{Ephemeral5m: 100_000},
	}))
	f.clk.Advance(271 * time.Second)
	if len(f.ledg.es) != 1 {
		t.Fatalf("%d refreshes", len(f.ledg.es))
	}
	// 100k tokens written at $6 per million, plus one output token at $15 per million.
	if got, want := f.ledg.es[0].CostUSD, 0.600015; math.Abs(got-want) > 1e-9 {
		t.Errorf("cost %v, want %v", got, want)
	}
}

func TestClientNeverFollowsRedirects(t *testing.T) {
	var hits int
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer other.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer up.Close()
	req, _ := http.NewRequest("POST", up.URL, strings.NewReader(body))
	req.Header.Set("X-Api-Key", "k")
	resp, err := NewClient(http.DefaultTransport).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if hits != 0 || resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("the refresh followed a redirect: %d hits, status %d", hits, resp.StatusCode)
	}
}

func TestRefreshKeepsTheUpstreamQuery(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.cfg.Upstream, _ = url.Parse("http://magpie.test/api?tenant=abc")
	in := armInput(t, key('a'), "tool_use", 100_000)
	in.Query = "beta=true"
	f.m.Arm(in)
	f.clk.Advance(271 * time.Second)
	if f.up.count() != 1 {
		t.Fatalf("%d refreshes", f.up.count())
	}
	if want := "http://magpie.test/api/v1/messages?tenant=abc&beta=true"; f.up.urls[0] != want {
		t.Errorf("refresh went to %s, want %s", f.up.urls[0], want)
	}
}

// A timer that fires long after it was due (suspend, a stalled process) must not send a
// refresh that would be a full-price write.
func TestLateRefreshIsDropped(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	f.m.mu.Lock()
	r := f.m.runs[key('a')]
	r.timer.Stop() // the timer never fires, as in a suspended process
	f.m.mu.Unlock()
	f.clk.Advance(290 * time.Second) // 270s was due, 285s is the latest safe time
	f.m.refresh(r)
	if f.up.count() != 0 || len(f.m.Status()) != 0 {
		t.Error("a refresh past its deadline was sent")
	}
}

func TestNotWarmed(t *testing.T) {
	for name, mut := range map[string]func(*ArmInput){
		"no prompt tokens": func(in *ArmInput) { in.Result.Usage = anthropic.Usage{} },
		"unmarked": func(in *ArmInput) {
			in.Info.Cacheable, in.Info.TTL = false, 0
		},
		"budget thinking": func(in *ArmInput) { in.Info.Replayable, in.Info.Reason = false, "budget" },
		"no price":        func(in *ArmInput) { in.Info.Model = "gemini" },
	} {
		f := newFixture(t, "idle", "")
		in := armInput(t, key('a'), "tool_use", 100_000)
		mut(&in)
		f.m.Arm(in)
		if len(f.m.Status()) != 0 {
			t.Errorf("%s: armed", name)
		}
	}
	f := newFixture(t, "off", "")
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	if len(f.m.Status()) != 0 {
		t.Error("armed while off")
	}
}

func TestSessionLimit(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.cfg.MaxSessions = 2
	for _, k := range []byte{'a', 'b', 'c'} {
		f.m.Arm(armInput(t, key(k), "tool_use", 100_000))
		f.clk.Advance(time.Second)
	}
	st := f.m.Status()
	if len(st) != 2 || st[0].Session != key('b')[:8] {
		t.Errorf("the oldest run must go first: %+v", st)
	}
}

// A restart picks the runs up again while their cache entries are alive, with the credentials
// they were sent with, and keeps the safety window of the original request.
func TestRestore(t *testing.T) {
	dir := t.TempDir()
	a := newFixture(t, "idle", dir)
	a.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	a.clk.Advance(60 * time.Second)
	a.m.Close()

	for _, e := range mustList(t, dir) {
		if fi, _ := os.Stat(filepath.Join(dir, e)); fi.Mode().Perm() != 0o600 {
			t.Errorf("%s is %v: it holds credentials", e, fi.Mode().Perm())
		}
	}

	b := newFixture(t, "idle", dir)
	b.clk.Advance(60 * time.Second) // the new process starts a minute later
	n, err := b.m.Restore()
	if err != nil || n != 1 {
		t.Fatalf("restored %d (%v)", n, err)
	}
	b.clk.Advance(215 * time.Second) // 275s after the original request: due at 270s
	if b.up.count() != 1 {
		t.Fatalf("%d refreshes after restore", b.up.count())
	}
	if b.up.heads[0].Get("X-Api-Key") != "k" {
		t.Error("credentials were not restored")
	}
}

func TestChatRefreshAfterRestore(t *testing.T) {
	dir := t.TempDir()
	a := newFixture(t, "idle", dir)
	body := []byte(`{"model":"cursor/claude-sonnet-5-5@1m","messages":[{"role":"user","content":"hello"}],"max_tokens":200,"max_completion_tokens":300,"stream":true,"stream_options":{"include_usage":true}}`)
	info, err := anthropic.InspectChat(body)
	if err != nil {
		t.Fatal(err)
	}
	in := armInput(t, key('a'), "tool_use", 100_000)
	in.Info, in.Body, in.Path, in.Query = info, body, anthropic.ChatPath, "tenant=test"
	a.m.Arm(in)
	a.m.Close()

	b := newFixture(t, "idle", dir)
	if n, err := b.m.Restore(); n != 1 || err != nil {
		t.Fatalf("restored %d (%v)", n, err)
	}
	b.clk.Advance(270 * time.Second)
	if b.up.count() != 1 || b.up.urls[0] != "http://magpie.test/v1/chat/completions?tenant=test" {
		t.Fatalf("unexpected refresh URLs %v", b.up.urls)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal([]byte(b.up.got[0]), &got); err != nil {
		t.Fatal(err)
	}
	if string(got["max_completion_tokens"]) != "1" || string(got["stream"]) != "false" ||
		got["max_tokens"] != nil || got["stream_options"] != nil ||
		string(got["model"]) != `"cursor/claude-sonnet-5-5@1m"` ||
		string(got["messages"]) != `[{"role":"user","content":"hello"}]` {
		t.Fatalf("unexpected refresh body %s", b.up.got[0])
	}
	if len(b.ledg.es) != 1 || !b.ledg.es[0].Hit || b.ledg.es[0].Usage.Input != 20 {
		t.Fatalf("unexpected refresh ledger %+v", b.ledg.es)
	}
}

func TestRestoreDropsWhatExpired(t *testing.T) {
	dir := t.TempDir()
	a := newFixture(t, "idle", dir)
	a.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	a.m.Close()

	b := newFixture(t, "idle", dir)
	b.clk.Advance(10 * time.Minute)
	if n, _ := b.m.Restore(); n != 0 {
		t.Errorf("restored %d runs whose cache entry is gone", n)
	}
	if len(mustList(t, dir)) != 0 {
		t.Error("expired runs were left on disk")
	}
}

func TestCloseStopsTimers(t *testing.T) {
	f := newFixture(t, "idle", "")
	f.m.Arm(armInput(t, key('a'), "tool_use", 100_000))
	f.m.Close()
	f.clk.Advance(time.Hour)
	if f.up.count() != 0 {
		t.Error("refreshed after Close")
	}
	f.m.Arm(armInput(t, key('b'), "tool_use", 100_000))
	if len(f.m.Status()) != 0 {
		t.Error("armed after Close")
	}
}

func mustList(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

func TestKey(t *testing.T) {
	sessions := []string{"X-Magpie-Session", "X-Session-Affinity"}
	info := func(first string) anthropic.Info {
		i, _ := anthropic.Inspect([]byte(`{"model":"m","messages":[` + first + `]}`))
		return i
	}
	one := info(`{"role":"user","content":"a"}`)
	two := info(`{"role":"user","content":"b"}`)

	named := func(k, s string) http.Header { return http.Header{"X-Api-Key": {k}, "X-Session-Affinity": {s}} }
	if Key("/v1/messages", named("k", "s1"), sessions, one) != Key("/v1/messages", named("k", "s1"), sessions, two) {
		t.Error("a named session is one conversation whatever its opening")
	}
	if Key("/v1/messages", named("k", "s1"), sessions, one) == Key("/v1/messages", named("k", "s2"), sessions, one) {
		t.Error("two sessions share a key")
	}
	if Key("/v1/messages", named("k1", "s"), sessions, one) == Key("/v1/messages", named("k2", "s"), sessions, one) {
		t.Error("two credentials share a key: their caches are separate")
	}
	// A subagent shares the parent's session id but not its system prompt or tools.
	agent := func(system, tools string) anthropic.Info {
		i, _ := anthropic.Inspect([]byte(`{"model":"m","system":"` + system + `","tools":[` + tools + `],"messages":[{"role":"user","content":"a"}]}`))
		return i
	}
	h := named("k", "s1")
	if Key("/v1/messages", h, sessions, agent("main", "")) != Key("/v1/messages", h, sessions, agent("main", "")) {
		t.Error("the same agent in a session must keep its key")
	}
	if Key("/v1/messages", h, sessions, agent("main", "")) == Key("/v1/messages", h, sessions, agent("sub", "")) {
		t.Error("a different system prompt in one session shares a key")
	}
	if Key("/v1/messages", h, sessions, agent("main", "")) == Key("/v1/messages", h, sessions, agent("main", `{"name":"t"}`)) {
		t.Error("a different toolset in one session shares a key")
	}
	plain := http.Header{"X-Api-Key": {"k"}}
	if Key("/v1/messages", plain, sessions, one) != Key("/v1/messages", plain, sessions, one) || Key("/v1/messages", plain, sessions, one) == Key("/v1/messages", plain, sessions, two) {
		t.Error("without a session the key follows how the conversation opens")
	}
	if Key("/v1/messages", http.Header{"X-Magpie-Session": {"m"}, "X-Session-Affinity": {"a"}}, sessions, one) !=
		Key("/v1/messages", http.Header{"X-Magpie-Session": {"m"}}, sessions, one) {
		t.Error("X-Magpie-Session must come first")
	}
}

var _ = policy.PhaseIdle
