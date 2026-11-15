package anthropic

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestInspectTTL(t *testing.T) {
	mark := `"cache_control":{"type":"ephemeral"}`
	hour := `"cache_control":{"type":"ephemeral","ttl":"1h"}`
	for _, tc := range []struct {
		name string
		body string
		ttl  time.Duration
	}{
		{"unmarked", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, 0},
		{"five minutes", `{"model":"m","system":[{"type":"text","text":"s",` + mark + `}],"messages":[]}`, 5 * time.Minute},
		{"hour", `{"model":"m","system":[{"type":"text","text":"s",` + hour + `}],"messages":[]}`, time.Hour},
		{"shortest wins", `{"model":"m","system":[{"type":"text","text":"s",` + hour + `}],"messages":[{"role":"user","content":[{"type":"text","text":"x",` + mark + `}]}]}`, 5 * time.Minute},
		{"top level", `{"model":"m",` + hour + `,"messages":[]}`, time.Hour},
		{"only mentioned in text", `{"model":"m","messages":[{"role":"user","content":"cache_control"}]}`, 0},
	} {
		info, err := Inspect([]byte(tc.body))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if info.TTL != tc.ttl || info.Cacheable != (tc.ttl > 0) {
			t.Errorf("%s: ttl %v cacheable %v, want %v", tc.name, info.TTL, info.Cacheable, tc.ttl)
		}
	}
}

func TestInspectReplayable(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"plain", `{"model":"m"}`, true},
		{"adaptive thinking", `{"model":"m","thinking":{"type":"adaptive"}}`, true},
		{"budget thinking", `{"model":"m","thinking":{"type":"enabled","budget_tokens":4000}}`, false},
		{"message thread", `{"model":"m","thread":{"type":"continue"}}`, false},
	} {
		info, err := Inspect([]byte(tc.body))
		if err != nil || info.Replayable != tc.want {
			t.Errorf("%s: replayable %v (%v), want %v", tc.name, info.Replayable, err, tc.want)
		}
	}
	if _, err := Inspect([]byte(`{"messages":[]}`)); err == nil {
		t.Error("a request without a model must be rejected")
	}
	if _, err := Inspect([]byte(`not json`)); err == nil {
		t.Error("a body that is not JSON must be rejected")
	}
}

// Only max_tokens and stream change; every other byte, key order and spacing included, stays.
func TestWarmBodyKeepsTheRest(t *testing.T) {
	in := `{"model":"m",  "system":[{"type":"text","text":"a  b"}],"max_tokens": 4096,"stream":true,"messages":[]}`
	out, err := WarmBody([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"model":"m","system":[{"type":"text","text":"a  b"}],"max_tokens":1,"stream":false,"messages":[]}`
	if string(out) != want {
		t.Errorf("got  %s\nwant %s", out, want)
	}

	out, err = WarmBody([]byte(`{"model":"m","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(out, &m) != nil || string(m["max_tokens"]) != "1" || string(m["stream"]) != "false" {
		t.Errorf("missing fields were not added: %s", out)
	}
	if _, err := WarmBody([]byte(`[1]`)); err == nil {
		t.Error("a body that is not an object must be rejected")
	}
}

const stream = `event: message_start
data: {"type":"message_start","message":{"id":"m","usage":{"input_tokens":12,"cache_read_input_tokens":900,"cache_creation_input_tokens":88,"cache_creation":{"ephemeral_5m_input_tokens":88,"ephemeral_1h_input_tokens":0},"output_tokens":1}}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"message_stop in the text"}}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":40}}

event: message_stop
data: {"type":"message_stop"}

`

func TestSSEParser(t *testing.T) {
	// Fed in awkward pieces, as a network delivers it.
	for _, step := range []int{1, 7, 64, len(stream)} {
		var p SSEParser
		for i := 0; i < len(stream); i += step {
			p.Feed([]byte(stream[i:min(i+step, len(stream))]))
		}
		r := p.Result()
		want := Usage{Input: 12, Output: 40, CacheRead: 900, CacheWrite: 88, Creation: CacheCreation{Ephemeral5m: 88}}
		if r.Usage != want || r.StopReason != "tool_use" || !r.Complete || r.Usage.Prompt() != 1000 {
			t.Errorf("step %d: %+v", step, r)
		}
	}
}

func TestSSEParserIncomplete(t *testing.T) {
	var p SSEParser
	p.Feed([]byte(stream[:strings.Index(stream, "event: message_stop")]))
	if p.Result().Complete {
		t.Error("a stream cut before message_stop is not complete")
	}

	p = SSEParser{}
	p.Feed([]byte(stream + "data: {\"type\":\"error\",\"error\":{}}\n"))
	if p.Result().Complete {
		t.Error("a stream with an error event is not complete")
	}

	p = SSEParser{}
	p.Feed([]byte("data: " + strings.Repeat("x", maxSSELine+10) + "\n" + stream))
	if r := p.Result(); !r.Complete || r.Usage.Input != 12 {
		t.Errorf("an oversized line must be skipped without losing the rest: %+v", r)
	}
}

func TestUsageTTL(t *testing.T) {
	five, hour := CacheCreation{Ephemeral5m: 10}, CacheCreation{Ephemeral1h: 10}
	for _, tc := range []struct {
		name      string
		creation  CacheCreation
		requested time.Duration
		want      time.Duration
	}{
		{"hour cut to five minutes", five, time.Hour, 5 * time.Minute},
		{"hour kept", hour, time.Hour, time.Hour},
		{"nothing written", CacheCreation{}, time.Hour, time.Hour},
		{"mixed writes follow the shorter", CacheCreation{Ephemeral5m: 1, Ephemeral1h: 1}, time.Hour, 5 * time.Minute},
		{"never longer than requested", hour, 5 * time.Minute, 5 * time.Minute},
	} {
		if got := (Usage{Creation: tc.creation}).TTL(tc.requested); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestParseJSON(t *testing.T) {
	r, ok := ParseJSON([]byte(`{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":5,"cache_read_input_tokens":95,"output_tokens":1}}`))
	if !ok || !r.Complete || r.StopReason != "end_turn" || r.Usage.Prompt() != 100 {
		t.Errorf("%+v %v", r, ok)
	}
	if _, ok := ParseJSON([]byte(`{"type":"error"}`)); ok {
		t.Error("an error reply is not a message")
	}
}
