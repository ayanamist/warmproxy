// Package config loads warmproxy's TOML configuration.
package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/ayanamist/warmproxy/internal/policy"
)

type file struct {
	Listen         string      `toml:"listen"`
	Upstream       string      `toml:"upstream"`
	StateDir       string      `toml:"state_dir"`
	Match          string      `toml:"match"`
	MaxBodyBytes   int64       `toml:"max_body_bytes"`
	RequestTimeout string      `toml:"request_timeout"`
	SessionHeaders []string    `toml:"session_headers"`
	Warm           warmFile    `toml:"warm"`
	Model          []modelFile `toml:"model"`
}

type warmFile struct {
	Mode            string  `toml:"mode"`
	StreamingP      float64 `toml:"streaming_continue_p"`
	IdleP           float64 `toml:"idle_continue_p"`
	MinSavingsUSD   float64 `toml:"min_savings_usd"`
	MaxConcurrent   int     `toml:"max_concurrent"`
	MaxSessions     int     `toml:"max_sessions"`
	StopAfterMisses int     `toml:"stop_after_misses"`
	MaxAgeStreaming string  `toml:"max_age_streaming"`
	MaxAgeIdle      string  `toml:"max_age_idle"`
}

// modelFile prices are USD per million tokens; the cache prices default to
// Anthropic's multiples of input: 0.1x read, 1.25x 5-minute write, 2x 1-hour write.
type modelFile struct {
	Match        string   `toml:"match"`
	Input        float64  `toml:"input"`
	Output       float64  `toml:"output"`
	CacheRead    *float64 `toml:"cache_read"`
	CacheWrite5m *float64 `toml:"cache_write_5m"`
	CacheWrite1h *float64 `toml:"cache_write_1h"`
}

type Rule struct {
	Match *regexp.Regexp
	Price policy.Price
}

type Config struct {
	Listen          string
	Upstream        *url.URL
	StateDir        string
	Match           *regexp.Regexp
	MaxBodyBytes    int64
	RequestTimeout  time.Duration
	SessionHeaders  []string
	Mode            string // off, streaming or idle
	Params          policy.Params
	MaxConcurrent   int
	MaxSessions     int
	StopAfterMisses int
	MaxAgeStreaming time.Duration
	MaxAgeIdle      time.Duration
	Rules           []Rule
}

func defaults() file {
	return file{
		Listen:         "127.0.0.1:3426",
		Upstream:       "http://127.0.0.1:3425",
		StateDir:       "~/.local/state/warmproxy",
		Match:          "^claude-(sonnet|opus)",
		MaxBodyBytes:   32 << 20,
		RequestTimeout: "60s",
		SessionHeaders: []string{
			"X-Magpie-Session", "X-Session-Affinity", "Session_id",
			"X-Opencode-Session", "X-Claude-Code-Session-Id", "X-Conversation-Id",
		},
		Warm: warmFile{
			Mode: "idle", StreamingP: 1, IdleP: 0.15, MinSavingsUSD: 0.05,
			MaxConcurrent: 4, MaxSessions: 256, StopAfterMisses: 2,
			MaxAgeStreaming: "1h", MaxAgeIdle: "30m",
		},
	}
}

// Load reads path, rejecting unknown keys so a typo does not silently change the policy.
func Load(path string) (*Config, error) {
	f := defaults()
	md, err := toml.DecodeFile(path, &f)
	if err != nil {
		return nil, err
	}
	if u := md.Undecoded(); len(u) > 0 {
		return nil, fmt.Errorf("%s: unknown keys %v", path, u)
	}
	return build(f)
}

func build(f file) (*Config, error) {
	c := &Config{
		Listen: f.Listen, MaxBodyBytes: f.MaxBodyBytes, SessionHeaders: f.SessionHeaders,
		Mode: f.Warm.Mode, MaxConcurrent: f.Warm.MaxConcurrent, MaxSessions: f.Warm.MaxSessions,
		StopAfterMisses: f.Warm.StopAfterMisses,
		Params: policy.Params{
			MinSavingsUSD: f.Warm.MinSavingsUSD, StreamingP: f.Warm.StreamingP, IdleP: f.Warm.IdleP,
		},
	}
	var err error
	if c.Upstream, err = url.Parse(f.Upstream); err != nil || c.Upstream.Host == "" || c.Upstream.Scheme == "" {
		return nil, fmt.Errorf("upstream %q is not an absolute URL", f.Upstream)
	}
	if c.StateDir, err = expand(f.StateDir); err != nil {
		return nil, err
	}
	if c.Match, err = regexp.Compile(f.Match); err != nil {
		return nil, fmt.Errorf("match: %w", err)
	}
	for name, dst := range map[string]struct {
		src string
		dst *time.Duration
	}{
		"request_timeout":   {f.RequestTimeout, &c.RequestTimeout},
		"max_age_streaming": {f.Warm.MaxAgeStreaming, &c.MaxAgeStreaming},
		"max_age_idle":      {f.Warm.MaxAgeIdle, &c.MaxAgeIdle},
	} {
		d, err := time.ParseDuration(dst.src)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%s: %q is not a positive duration", name, dst.src)
		}
		*dst.dst = d
	}
	switch c.Mode {
	case "off", "streaming", "idle":
	default:
		return nil, fmt.Errorf("warm.mode %q: want off, streaming or idle", c.Mode)
	}
	if c.MaxConcurrent < 1 || c.MaxSessions < 1 || c.StopAfterMisses < 1 || c.MaxBodyBytes < 1 {
		return nil, fmt.Errorf("max_concurrent, max_sessions, stop_after_misses and max_body_bytes must be positive")
	}
	for i, m := range f.Model {
		re, err := regexp.Compile(m.Match)
		if err != nil {
			return nil, fmt.Errorf("model[%d].match: %w", i, err)
		}
		if m.Input <= 0 || m.Output <= 0 {
			return nil, fmt.Errorf("model[%d] (%s): input and output prices must be positive", i, m.Match)
		}
		p := policy.Price{
			Input: m.Input, Output: m.Output,
			CacheRead: m.Input / 10, CacheWrite5m: m.Input * 5 / 4, CacheWrite1h: m.Input * 2,
		}
		if m.CacheRead != nil {
			p.CacheRead = *m.CacheRead
		}
		if m.CacheWrite5m != nil {
			p.CacheWrite5m = *m.CacheWrite5m
		}
		if m.CacheWrite1h != nil {
			p.CacheWrite1h = *m.CacheWrite1h
		}
		c.Rules = append(c.Rules, Rule{Match: re, Price: p})
	}
	return c, nil
}

// RuleFor is the first price rule matching model; a model without one is never warmed.
func (c *Config) RuleFor(model string) (policy.Price, bool) {
	for _, r := range c.Rules {
		if r.Match.MatchString(model) {
			return r.Price, true
		}
	}
	return policy.Price{}, false
}

func expand(p string) (string, error) {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = filepath.Join(home, p[1:])
	}
	return p, nil
}
