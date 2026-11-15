package policy

import (
	"testing"
	"time"
)

var sonnet = Price{Input: 3, Output: 15, CacheRead: 0.3, CacheWrite5m: 3.75, CacheWrite1h: 6}
var params = Params{MinSavingsUSD: 0.05, StreamingP: 1, IdleP: 0.15}

func TestDelay(t *testing.T) {
	for _, tc := range []struct {
		ttl  time.Duration
		want time.Duration
		ok   bool
	}{
		{5 * time.Minute, 270 * time.Second, true},
		{time.Hour, 54 * time.Minute, true},
		{20 * time.Second, 10 * time.Second, true},
		{10 * time.Second, 0, false},
	} {
		got, ok := Delay(tc.ttl)
		if got != tc.want || ok != tc.ok {
			t.Errorf("Delay(%v) = %v, %v; want %v, %v", tc.ttl, got, ok, tc.want, tc.ok)
		}
	}
}

// The break-even sizes follow from pi's formula with Sonnet's list price.
func TestEvaluateThresholds(t *testing.T) {
	for _, tc := range []struct {
		phase  Phase
		ttl    time.Duration
		tokens int
		warm   bool
	}{
		{PhaseStreaming, 5 * time.Minute, 15_000, false},
		{PhaseStreaming, 5 * time.Minute, 16_000, true},
		{PhaseIdle, 5 * time.Minute, 229_000, false},
		{PhaseIdle, 5 * time.Minute, 231_000, true},
		{PhaseStreaming, time.Hour, 9_000, false},
		{PhaseStreaming, time.Hour, 10_000, true},
		{PhaseIdle, 5 * time.Minute, 0, false},
	} {
		if got := Evaluate(sonnet, tc.ttl, tc.tokens, tc.phase, params).Warm; got != tc.warm {
			t.Errorf("%s ttl=%v tokens=%d: warm=%v, want %v", tc.phase, tc.ttl, tc.tokens, got, tc.warm)
		}
	}
}

// A replay pays plain input price for what lies after the last cache breakpoint.
func TestEvaluateSplit(t *testing.T) {
	if d := EvaluateSplit(sonnet, 5*time.Minute, 2_000, 98_000, PhaseStreaming, params); d.Warm {
		t.Errorf("warmed a 2k prefix behind a 98k suffix: %+v", d)
	}
	if d := EvaluateSplit(sonnet, 5*time.Minute, 100_000, 1_000, PhaseStreaming, params); !d.Warm {
		t.Errorf("did not warm a 100k prefix: %+v", d)
	}
	if EvaluateSplit(sonnet, 5*time.Minute, 20_000, 0, PhaseStreaming, params) != Evaluate(sonnet, 5*time.Minute, 20_000, PhaseStreaming, params) {
		t.Error("without a suffix both must agree")
	}
}

func TestEvaluateWithoutPrices(t *testing.T) {
	if d := Evaluate(Price{}, 5*time.Minute, 500_000, PhaseStreaming, params); d.Warm || d.Available {
		t.Errorf("no prices must not warm: %+v", d)
	}
}
