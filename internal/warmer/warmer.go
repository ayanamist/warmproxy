// Package warmer keeps prompt cache entries alive by replaying each conversation's last request.
package warmer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/textproto"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ayanamist/warmproxy/internal/anthropic"
	"github.com/ayanamist/warmproxy/internal/clock"
	"github.com/ayanamist/warmproxy/internal/config"
	"github.com/ayanamist/warmproxy/internal/policy"
)

type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// NewClient sends refreshes over transport and never follows a redirect: a refresh, with its
// credentials and body, goes to the configured upstream and nowhere else.
func NewClient(transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// retryDelay is the wait before a failed refresh is tried again inside the cache entry's life.
const retryDelay = 10 * time.Second

type Options struct {
	Config *config.Config
	Clock  clock.Clock
	Doer   Doer
	Store  *Store
	Ledger Ledger
	Log    *slog.Logger
}

// ArmInput is a real request that completed, whose cache entry should be kept warm.
type ArmInput struct {
	Key  string
	Info anthropic.Info
	// Started is when the request was sent: the cache entry's life counts from it, not from the
	// end of the reply. Zero means now.
	Started time.Time
	Path    string
	Query   string
	Header  http.Header
	Body    []byte
	Result  anthropic.Result
}

type run struct {
	m        meta
	body     []byte // the request as sent
	warmBody []byte // the same with a one-token output limit and no streaming
	price    policy.Price
	delay    time.Duration
	timer    clock.Stopper
	ctx      context.Context
	cancel   context.CancelFunc
	decision string
}

type Manager struct {
	cfg    *config.Config
	clock  clock.Clock
	doer   Doer
	store  *Store
	ledger Ledger
	log    *slog.Logger

	sem    chan struct{}
	mu     sync.Mutex
	runs   map[string]*run
	closed bool
	wg     sync.WaitGroup
}

func New(o Options) *Manager {
	m := &Manager{
		cfg: o.Config, clock: o.Clock, doer: o.Doer, store: o.Store, ledger: o.Ledger, log: o.Log,
		sem:  make(chan struct{}, o.Config.MaxConcurrent),
		runs: map[string]*run{},
	}
	if m.clock == nil {
		m.clock = clock.Real{}
	}
	if m.log == nil {
		m.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return m
}

// Key identifies a conversation: the credential, the system prompt and tools (a subagent shares
// its parent's session id but not these), and the session the agent named or, when it names
// none, how the conversation opens. The model is left out on purpose: switching models within
// a session replaces the run, as the new request is what wrote the newest cache entry.
func Key(path string, h http.Header, sessionHeaders []string, info anthropic.Info) string {
	sid := ""
	for _, n := range sessionHeaders {
		if v := strings.TrimSpace(h.Get(n)); v != "" {
			sid = v[:min(len(v), 128)]
			break
		}
	}
	sum := sha256.New()
	if path == anthropic.ChatPath {
		sum.Write([]byte(anthropic.ChatPath + "\x00"))
	}
	for _, part := range [][]byte{[]byte(h.Get("X-Api-Key")), []byte(h.Get("Authorization"))} {
		sum.Write(part)
		sum.Write([]byte{0})
	}
	for _, part := range [][]byte{info.System, info.Tools} {
		sum.Write(part)
		sum.Write([]byte{0})
	}
	if sid != "" {
		sum.Write([]byte("s:" + sid))
	} else {
		sum.Write([]byte("d:"))
		sum.Write(info.First)
	}
	return hex.EncodeToString(sum.Sum(nil))[:32]
}

// ReplayHeaders are the request headers a refresh repeats: the credentials, and everything
// that can change what the upstream caches or how it routes the request.
func ReplayHeaders(in http.Header, sessionHeaders []string) http.Header {
	keep := map[string]bool{
		"Authorization": true, "X-Api-Key": true, "Anthropic-Version": true,
		"Anthropic-Beta": true, "Content-Type": true, "User-Agent": true,
	}
	for _, n := range sessionHeaders {
		keep[textproto.CanonicalMIMEHeaderKey(n)] = true
	}
	out := http.Header{}
	for k, v := range in {
		switch {
		case strings.HasPrefix(k, "X-Forwarded-") || k == "X-Real-Ip":
			continue
		case keep[k], strings.HasPrefix(k, "X-"), strings.HasPrefix(k, "Anthropic-"):
			out[k] = append([]string(nil), v...)
		}
	}
	return out
}

func (m *Manager) maxAge(phase string) time.Duration {
	if phase == string(policy.PhaseStreaming) {
		return m.cfg.MaxAgeStreaming
	}
	return m.cfg.MaxAgeIdle
}

// deadlineOf is the latest time a due refresh may still be sent: half of the margin before expiry.
func deadlineOf(next time.Time, ttl, delay time.Duration) time.Time {
	return next.Add((ttl - delay) / 2)
}

// Disarm ends the conversation's run: its newest request went to a model that is not warmed.
func (m *Manager) Disarm(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked(key, true)
}

// Arm starts warming for a completed real request, replacing the conversation's run.
func (m *Manager) Arm(in ArmInput) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.stopLocked(in.Key, false)
	now := m.clock.Now()
	started := in.Started
	if started.IsZero() || started.After(now) {
		started = now
	}

	price, priced := m.cfg.RuleFor(in.Info.Model)
	phase := policy.PhaseIdle
	if in.Result.StopReason == "tool_use" {
		// The agent is most likely still running: it will send the tool result.
		phase = policy.PhaseStreaming
	}
	// The upstream may keep what it wrote for less time than the request asked for.
	ttl := in.Result.Usage.TTL(in.Info.TTL)
	delay, delayOK := policy.Delay(ttl)
	warm, werr := anthropic.WarmRequest(in.Path, in.Body)

	reason := ""
	switch {
	case m.cfg.Mode == "off":
		reason = "warming disabled"
	case m.cfg.Mode == string(policy.PhaseStreaming) && phase == policy.PhaseIdle:
		reason = "agent run settled"
	case !in.Info.Replayable:
		reason = in.Info.Reason
	case !in.Info.Cacheable:
		reason = "request has no cache_control"
	case in.Path == anthropic.ChatPath && in.Result.Usage.CacheRead == 0 && in.Result.Usage.CacheWrite == 0:
		reason = "chat reply reported no cached tokens"
	case in.Result.Usage.Prompt() == 0:
		reason = "reply reported no prompt tokens"
	case !priced:
		reason = "no price for model"
	case !delayOK:
		reason = "cache lifetime too short"
	case now.After(deadlineOf(started.Add(delay), ttl, delay)):
		reason = "cache entry expired before the reply ended"
	case werr != nil:
		reason = "request cannot be rewritten"
	}
	if reason != "" {
		m.store.Delete(in.Key)
		m.log.Debug("not warming", "session", in.Key[:8], "path", in.Path, "model", in.Info.Model, "reason", reason)
		return
	}

	for len(m.runs) >= m.cfg.MaxSessions {
		m.stopLocked(m.oldestLocked(), true)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{
		m: meta{
			Key: in.Key, Model: in.Info.Model, Path: in.Path, Query: in.Query,
			Headers: ReplayHeaders(in.Header, m.cfg.SessionHeaders),
			Tokens:  in.Result.Usage.Prompt(), Uncached: in.Result.Usage.Input,
			TTL: ttl, Phase: string(phase), StartedAt: started,
		},
		body: in.Body, warmBody: warm, price: price, delay: delay, ctx: ctx, cancel: cancel,
	}
	m.runs[in.Key] = r
	m.scheduleLocked(r, started.Add(delay), true)
	m.log.Info("warming scheduled", "session", in.Key[:8], "path", in.Path, "model", r.m.Model, "phase", phase, "tokens", r.m.Tokens, "ttl", ttl, "next", started.Add(delay).Sub(now))
}

func (m *Manager) oldestLocked() string {
	var key string
	var at time.Time
	for k, r := range m.runs {
		if key == "" || r.m.StartedAt.Before(at) {
			key, at = k, r.m.StartedAt
		}
	}
	return key
}

// scheduleLocked arms the refresh due at the given time, which is counted from the last
// request that read or wrote the cache entry, and persists the run, with its body when it is new.
func (m *Manager) scheduleLocked(r *run, at time.Time, withBody bool) {
	r.m.NextWarmAt = at
	r.timer = m.clock.AfterFunc(max(0, at.Sub(m.clock.Now())), func() { m.refresh(r) })
	var body []byte
	if withBody {
		body = r.body
	}
	if err := m.store.Save(r.m, body); err != nil {
		m.log.Warn("saving run failed", "session", r.m.Key[:8], "err", err)
	}
}

func (m *Manager) stopLocked(key string, del bool) {
	r := m.runs[key]
	if r == nil {
		return
	}
	delete(m.runs, key)
	if r.timer != nil {
		r.timer.Stop()
	}
	r.cancel()
	if del {
		m.store.Delete(key)
	}
}

// checkLocked says why a refresh that is due must not be sent, "" when it may be.
func (m *Manager) checkLocked(r *run, now time.Time) string {
	phase := policy.Phase(r.m.Phase)
	switch {
	case m.cfg.Mode == "off":
		return "warming disabled"
	case m.cfg.Mode == string(policy.PhaseStreaming) && phase == policy.PhaseIdle:
		return "agent run settled"
	}
	// A timer that ran late (sleep, a stalled process) sends a refresh that is likely a
	// full-price cache write: keep half of the margin before expiry, as pi does.
	if now.After(deadlineOf(r.m.NextWarmAt, r.m.TTL, r.delay)) {
		return "refresh deadline missed"
	}
	if now.Sub(r.m.StartedAt) > m.maxAge(r.m.Phase) {
		return "safety window over"
	}
	d := policy.EvaluateSplit(r.price, r.m.TTL, r.m.Tokens-r.m.Uncached, r.m.Uncached, phase, m.cfg.Params)
	r.decision = decisionText(d)
	if !d.Warm {
		return "expected savings below threshold"
	}
	return ""
}

func (m *Manager) refresh(r *run) {
	m.mu.Lock()
	if m.closed || m.runs[r.m.Key] != r {
		m.mu.Unlock()
		return
	}
	m.wg.Add(1)
	defer m.wg.Done()
	if why := m.checkLocked(r, m.clock.Now()); why != "" {
		m.stopLocked(r.m.Key, true)
		m.mu.Unlock()
		m.log.Info("warming stopped", "session", r.m.Key[:8], "model", r.m.Model, "reason", why)
		return
	}
	m.mu.Unlock()

	select {
	case m.sem <- struct{}{}:
		defer func() { <-m.sem }()
	case <-r.ctx.Done():
		return
	}
	// The wait for a slot can outlast the cache entry or the safety window.
	m.mu.Lock()
	if m.closed || m.runs[r.m.Key] != r {
		m.mu.Unlock()
		return
	}
	if why := m.checkLocked(r, m.clock.Now()); why != "" {
		m.stopLocked(r.m.Key, true)
		m.mu.Unlock()
		m.log.Info("warming stopped", "session", r.m.Key[:8], "model", r.m.Model, "reason", why)
		return
	}
	m.mu.Unlock()
	entry, usage, outcome := m.send(r)
	if m.ledger != nil {
		m.ledger.Record(entry)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.runs[r.m.Key] != r {
		return
	}
	switch outcome {
	case outcomeStop:
		m.stopLocked(r.m.Key, true)
		m.log.Warn("warming stopped", "session", r.m.Key[:8], "status", entry.Status, "reason", "upstream refused the refresh")
		return
	case outcomeRetry:
		// The failed refresh did not extend the cache entry: retry only while it lives.
		m.log.Warn("refresh failed", "session", r.m.Key[:8], "path", r.m.Path, "status", entry.Status, "reason", entry.Error)
		if m.clock.Now().Add(retryDelay).After(deadlineOf(r.m.NextWarmAt, r.m.TTL, r.delay)) {
			m.stopLocked(r.m.Key, true)
			m.log.Info("warming stopped", "session", r.m.Key[:8], "reason", "cache entry expired")
			return
		}
		r.timer = m.clock.AfterFunc(retryDelay, func() { m.refresh(r) })
		return
	}
	if ttl := usage.TTL(r.m.TTL); ttl < r.m.TTL {
		// The first request wrote nothing that told the lifetime; this refresh did.
		m.log.Info("upstream keeps the cache for less than requested", "session", r.m.Key[:8], "ttl", ttl, "requested_ttl", r.m.TTL)
		r.m.TTL = ttl
		r.delay, _ = policy.Delay(ttl)
	}
	r.m.Warms++
	r.m.SpentUSD += entry.CostUSD
	if entry.Hit {
		r.m.Misses = 0
		m.log.Info("refresh hit cache", "session", r.m.Key[:8], "path", r.m.Path, "cache_read", usage.CacheRead, "cost_usd", entry.CostUSD)
	} else {
		r.m.Misses++
		m.log.Warn("refresh missed the cache", "session", r.m.Key[:8], "prompt", r.m.Tokens, "cache_read", usage.CacheRead)
		if r.m.Misses >= m.cfg.StopAfterMisses {
			m.stopLocked(r.m.Key, true)
			m.log.Warn("warming stopped", "session", r.m.Key[:8], "reason", "cache keeps missing")
			return
		}
	}
	// The refresh read or wrote the entry when it was sent.
	next := entry.Time.Add(r.delay)
	if next.Sub(r.m.StartedAt) > m.maxAge(r.m.Phase) {
		m.stopLocked(r.m.Key, true)
		m.log.Info("warming stopped", "session", r.m.Key[:8], "reason", "safety window over")
		return
	}
	m.scheduleLocked(r, next, false)
}

type outcome int

const (
	outcomeOK outcome = iota
	outcomeRetry
	outcomeStop
)

func (m *Manager) send(r *run) (Entry, anthropic.Usage, outcome) {
	start := m.clock.Now()
	e := Entry{Time: start, Key: r.m.Key[:8], Model: r.m.Model, Phase: r.m.Phase, PromptTokens: r.m.Tokens}
	ctx, cancel := context.WithTimeout(r.ctx, m.cfg.RequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://replay.invalid"+r.m.Path, bytes.NewReader(r.warmBody))
	if err != nil {
		e.Error = "invalid replay URL"
		return e, anthropic.Usage{}, outcomeStop
	}
	req.URL.RawQuery = r.m.Query
	// The same URL rules as forwarding the real request.
	(&httputil.ProxyRequest{In: req, Out: req}).SetURL(m.cfg.Upstream)
	req.Header = r.m.Headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.doer.Do(req)
	if err != nil {
		e.Error = "upstream request failed"
		e.Millis = m.clock.Now().Sub(start).Milliseconds()
		return e, anthropic.Usage{}, outcomeRetry
	}
	defer resp.Body.Close()
	e.Status = resp.StatusCode
	e.Millis = m.clock.Now().Sub(start).Milliseconds()
	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		e.Error = "retryable upstream status"
		return e, anthropic.Usage{}, outcomeRetry
	default:
		return e, anthropic.Usage{}, outcomeStop
	}
	const maxReply = 1 << 20
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxReply+1))
	e.Millis = m.clock.Now().Sub(start).Milliseconds()
	if readErr != nil || len(raw) > maxReply {
		e.Error = "incomplete or oversized reply"
		return e, anthropic.Usage{}, outcomeRetry
	}
	res, ok := anthropic.ParseReply(r.m.Path, raw)
	if r.m.Path == anthropic.ChatPath && res.Usage.Prompt() == 0 {
		ok = false
	}
	if !ok {
		e.Error = "unreadable reply"
		return e, anthropic.Usage{}, outcomeRetry
	}
	u2 := res.Usage
	e.Usage = u2
	// Bill each write at the price of the lifetime the reply reported; what it does not
	// split follows the schedule's lifetime.
	w5, w1 := u2.Creation.Ephemeral5m, u2.Creation.Ephemeral1h
	if rest := max(0, u2.CacheWrite-w5-w1); r.m.TTL >= time.Hour {
		w1 += rest
	} else {
		w5 += rest
	}
	e.CostUSD = (float64(u2.Input)*r.price.Input + float64(u2.CacheRead)*r.price.CacheRead +
		float64(w5)*r.price.CacheWrite5m + float64(w1)*r.price.CacheWrite1h + float64(u2.Output)*r.price.Output) / 1e6
	// A refresh that read less than half of the cached prefix wrote it again instead.
	e.Hit = u2.CacheRead*2 >= r.m.Tokens-r.m.Uncached
	return e, u2, outcomeOK
}

// Restore picks up runs saved by an earlier process whose cache entries can still be refreshed.
func (m *Manager) Restore() (int, error) {
	saved, err := m.store.Load()
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Now()
	n := 0
	for _, s := range saved {
		price, priced := m.cfg.RuleFor(s.Model)
		delay, delayOK := policy.Delay(s.TTL)
		warm, werr := anthropic.WarmRequest(s.Path, s.body)
		if m.cfg.Mode == "off" || !priced || !delayOK || werr != nil || now.After(deadlineOf(s.NextWarmAt, s.TTL, delay)) ||
			now.Sub(s.StartedAt) > m.maxAge(s.Phase) || len(m.runs) >= m.cfg.MaxSessions {
			m.store.Delete(s.Key)
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		r := &run{m: s.meta, body: s.body, warmBody: warm, price: price, delay: delay, ctx: ctx, cancel: cancel}
		m.runs[s.Key] = r
		// NextWarmAt stays what was saved, so the deadline check still holds.
		wait := max(0, s.NextWarmAt.Sub(now))
		r.timer = m.clock.AfterFunc(wait, func() { m.refresh(r) })
		n++
	}
	return n, nil
}

type RunStatus struct {
	Session    string    `json:"session"`
	Model      string    `json:"model"`
	Phase      string    `json:"phase"`
	Tokens     int       `json:"prompt_tokens"`
	StartedAt  time.Time `json:"started_at"`
	NextWarmAt time.Time `json:"next_warm_at"`
	Warms      int       `json:"warms"`
	Misses     int       `json:"misses"`
	SpentUSD   float64   `json:"spent_usd"`
	Decision   string    `json:"last_decision,omitempty"`
}

func (m *Manager) Status() []RunStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RunStatus, 0, len(m.runs))
	for _, r := range m.runs {
		out = append(out, RunStatus{
			Session: r.m.Key[:8], Model: r.m.Model, Phase: r.m.Phase, Tokens: r.m.Tokens,
			StartedAt: r.m.StartedAt, NextWarmAt: r.m.NextWarmAt, Warms: r.m.Warms,
			Misses: r.m.Misses, SpentUSD: r.m.SpentUSD, Decision: r.decision,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out
}

// Close stops every timer and refresh in flight; saved runs stay on disk for the next process.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	for _, r := range m.runs {
		if r.timer != nil {
			r.timer.Stop()
		}
		r.cancel()
	}
	m.runs = map[string]*run{}
	m.mu.Unlock()
	m.wg.Wait()
}

func decisionText(d policy.Decision) string {
	if !d.Available {
		return "economics unavailable"
	}
	act := "stop"
	if d.Warm {
		act = "warm"
	}
	return act + ": expected savings $" + trim(d.Expected) + " (warm $" + trim(d.WarmCost) + ", miss $" + trim(d.MissCost) + ")"
}
