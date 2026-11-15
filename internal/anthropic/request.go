// Package anthropic reads what warming needs from Anthropic Messages requests and replies.
package anthropic

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
)

type Info struct {
	Model string
	// TTL is the shortest cache_control lifetime in the request; 0 when nothing is marked.
	TTL        time.Duration
	Cacheable  bool
	Replayable bool
	Reason     string // why the request cannot be replayed

	// The parts that identify a conversation's cache prefix.
	System, Tools, First json.RawMessage
}

// Inspect reads a /v1/messages request body.
func Inspect(body []byte) (Info, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return Info{}, err
	}
	var info Info
	_ = json.Unmarshal(top["model"], &info.Model)
	if info.Model == "" {
		return Info{}, errors.New("request has no model")
	}
	info.System, info.Tools = top["system"], top["tools"]
	var msgs []json.RawMessage
	_ = json.Unmarshal(top["messages"], &msgs)
	if len(msgs) > 0 {
		info.First = msgs[0]
	}

	info.Replayable = true
	// Budget thinking derives budget_tokens from max_tokens, so a one-token replay
	// would be a different request, and Anthropic keys the message cache on it.
	var th struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(top["thinking"], &th)
	if th.Type == "enabled" {
		info.Replayable, info.Reason = false, "budget thinking depends on max_tokens"
	}
	// A message thread continues server-side state a replay would have to disturb.
	if _, ok := top["thread"]; ok {
		info.Replayable, info.Reason = false, "request continues a message thread"
	}

	var ttl time.Duration
	for _, k := range []string{"system", "tools", "messages"} {
		var v any
		if json.Unmarshal(top[k], &v) == nil {
			walk(v, &ttl)
		}
	}
	if raw, ok := top["cache_control"]; ok {
		var cc map[string]any
		if json.Unmarshal(raw, &cc) == nil {
			note(cc, &ttl)
		}
	}
	info.TTL, info.Cacheable = ttl, ttl > 0
	return info, nil
}

func walk(v any, ttl *time.Duration) {
	switch x := v.(type) {
	case map[string]any:
		if cc, ok := x["cache_control"].(map[string]any); ok {
			note(cc, ttl)
		}
		for _, c := range x {
			walk(c, ttl)
		}
	case []any:
		for _, c := range x {
			walk(c, ttl)
		}
	}
}

func note(cc map[string]any, ttl *time.Duration) {
	d := 5 * time.Minute
	if s, _ := cc["ttl"].(string); s == "1h" {
		d = time.Hour
	}
	if *ttl == 0 || d < *ttl {
		*ttl = d
	}
}

// WarmBody is body with max_tokens 1 and stream false, every other byte as sent.
func WarmBody(body []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, errors.New("request body is not an object")
	}
	over := map[string]string{"max_tokens": "1", "stream": "false"}
	done := map[string]bool{}
	var out bytes.Buffer
	out.WriteByte('{')
	put := func(key string, raw []byte) {
		if out.Len() > 1 {
			out.WriteByte(',')
		}
		k, _ := json.Marshal(key)
		out.Write(k)
		out.WriteByte(':')
		out.Write(raw)
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key := kt.(string)
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, err
		}
		if v, ok := over[key]; ok {
			raw, done[key] = []byte(v), true
		}
		put(key, raw)
	}
	for _, key := range []string{"max_tokens", "stream"} {
		if !done[key] {
			put(key, []byte(over[key]))
		}
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}
