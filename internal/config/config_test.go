package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func load(t *testing.T, text string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

func TestDefaults(t *testing.T) {
	c, err := load(t, "")
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:3426" || c.Upstream.Host != "127.0.0.1:3425" || c.Mode != "idle" ||
		c.Params.IdleP != 0.15 || c.Params.StreamingP != 1 || c.Params.MinSavingsUSD != 0.05 ||
		c.MaxAgeIdle != 30*time.Minute || c.MaxAgeStreaming != time.Hour {
		t.Errorf("%+v", c)
	}
	if !c.Match.MatchString("claude-sonnet-5") || !c.Match.MatchString("claude-opus-5-5") || c.Match.MatchString("claude-haiku-5") {
		t.Error("the default match must cover Sonnet and Opus only")
	}
	if _, ok := c.RuleFor("claude-sonnet-5"); ok {
		t.Error("without a price a model is never warmed")
	}
}

func TestModelPrices(t *testing.T) {
	c, err := load(t, `
warm.mode = "streaming"

[[model]]
match = "^claude-sonnet"
input = 3
output = 15

[[model]]
match = "^claude-opus"
input = 5
output = 25
cache_read = 0.5
cache_write_5m = 6.25
cache_write_1h = 10
`)
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != "streaming" {
		t.Errorf("mode %q", c.Mode)
	}
	// Left out, the cache prices follow Anthropic's multiples of the input price.
	p, ok := c.RuleFor("claude-sonnet-5-5")
	if !ok || p.CacheRead != 0.3 || p.CacheWrite5m != 3.75 || p.CacheWrite1h != 6 {
		t.Errorf("derived prices %+v", p)
	}
	p, _ = c.RuleFor("claude-opus-5")
	if p.CacheRead != 0.5 || p.CacheWrite5m != 6.25 || p.CacheWrite1h != 10 {
		t.Errorf("given prices %+v", p)
	}
}

func TestRejects(t *testing.T) {
	for name, text := range map[string]string{
		"unknown key":    `listn = "x"`,
		"unknown warm":   "[warm]\nmod = \"idle\"",
		"bad mode":       "[warm]\nmode = \"always\"",
		"bad upstream":   `upstream = "magpie"`,
		"bad regexp":     `match = "("`,
		"bad duration":   "[warm]\nmax_age_idle = \"soon\"",
		"zero sessions":  "[warm]\nmax_sessions = 0",
		"unpriced model": "[[model]]\nmatch = \"x\"",
	} {
		if _, err := load(t, text); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestStateDirExpandsHome(t *testing.T) {
	t.Setenv("HOME", "/home/x")
	c, err := load(t, `state_dir = "~/s"`)
	if err != nil || !strings.HasPrefix(c.StateDir, "/home/x") {
		t.Errorf("%q %v", c.StateDir, err)
	}
}
