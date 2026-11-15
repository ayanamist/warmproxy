// Package policy ports pi's prompt-cache warming policy (packages/ai/src/cache-warming.ts).
package policy

import "time"

type Phase string

const (
	PhaseStreaming Phase = "streaming"
	PhaseIdle      Phase = "idle"
)

// Price is USD per million tokens.
type Price struct {
	Input, Output, CacheRead, CacheWrite5m, CacheWrite1h float64
}

type Params struct {
	MinSavingsUSD float64
	StreamingP    float64
	IdleP         float64
}

type Decision struct {
	Warm      bool
	Available bool
	WarmCost  float64
	MissCost  float64
	P         float64
	Expected  float64
}

// Delay is when to refresh: 90% of the TTL, keeping at least ten seconds of margin.
func Delay(ttl time.Duration) (time.Duration, bool) {
	if ttl <= 10*time.Second {
		return 0, false
	}
	return max(1, min(ttl*9/10, ttl-10*time.Second)), true
}

// Evaluate decides whether refreshing a promptTokens-token cache entry pays for itself.
func Evaluate(p Price, ttl time.Duration, promptTokens int, phase Phase, par Params) Decision {
	return EvaluateSplit(p, ttl, promptTokens, 0, phase, par)
}

// EvaluateSplit is Evaluate for a prompt whose last cache breakpoint leaves uncached tokens
// behind: a replay bills them at the input price, and a cache hit does not save them.
func EvaluateSplit(p Price, ttl time.Duration, cached, uncached int, phase Phase, par Params) Decision {
	t := float64(cached) / 1e6
	hit := t * p.CacheRead
	write := p.CacheWrite5m
	if ttl >= time.Hour {
		write = p.CacheWrite1h
	}
	miss := t * p.Input
	if write > 0 {
		miss = t * write
	}
	warm := hit + float64(uncached)/1e6*p.Input + p.Output/1e6
	missCost := max(0, miss-hit)
	prob := par.IdleP
	if phase == PhaseStreaming {
		prob = par.StreamingP
	}
	d := Decision{
		Available: cached > 0 && (hit > 0 || miss > 0),
		WarmCost:  warm,
		MissCost:  missCost,
		P:         prob,
		Expected:  prob*missCost - warm,
	}
	d.Warm = d.Available && d.Expected >= par.MinSavingsUSD
	return d
}
