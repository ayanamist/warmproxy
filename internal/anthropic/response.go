package anthropic

import (
	"bytes"
	"encoding/json"
	"time"
)

const maxSSELine = 1 << 20

// CacheCreation splits the tokens written to the cache by lifetime.
type CacheCreation struct {
	Ephemeral5m int `json:"ephemeral_5m_input_tokens"`
	Ephemeral1h int `json:"ephemeral_1h_input_tokens"`
}

type Usage struct {
	Input      int `json:"input_tokens"`
	Output     int `json:"output_tokens"`
	CacheRead  int `json:"cache_read_input_tokens"`
	CacheWrite int `json:"cache_creation_input_tokens"`
	// Creation is zero when the reply wrote nothing or the upstream does not report the split.
	Creation CacheCreation `json:"cache_creation"`
}

// Prompt is the size of the prompt the usage was reported for.
func (u Usage) Prompt() int { return u.Input + u.CacheRead + u.CacheWrite }

// TTL is how long the cache entry lives, given the lifetime the request asked for. An upstream
// that gave some of what it wrote only five minutes has shortened it; missing lifetime data
// conservatively defaults to five minutes.
func (u Usage) TTL(requested time.Duration) time.Duration {
	if u.Creation.Ephemeral5m > 0 {
		return min(requested, 5*time.Minute)
	}
	if u.Creation.Ephemeral1h > 0 {
		return requested
	}
	return min(requested, 5*time.Minute)
}

type Result struct {
	Usage      Usage
	StopReason string
	// Complete is true for a reply that ended normally: a stream that reached
	// message_stop without an error event, or a whole JSON message.
	Complete bool
}

type event struct {
	Type    string `json:"type"`
	Message struct {
		Usage      Usage  `json:"usage"`
		StopReason string `json:"stop_reason"`
	} `json:"message"`
	Delta struct {
		StopReason string `json:"stop_reason"`
	} `json:"delta"`
	Usage Usage `json:"usage"`
}

// ParseJSON reads a non-streaming reply.
func ParseJSON(b []byte) (Result, bool) {
	var m struct {
		Type       string `json:"type"`
		Usage      Usage  `json:"usage"`
		StopReason string `json:"stop_reason"`
	}
	if json.Unmarshal(b, &m) != nil || m.Type == "error" {
		return Result{}, false
	}
	return Result{Usage: m.Usage, StopReason: m.StopReason, Complete: true}, true
}

// SSEParser follows a Claude event stream, keeping only usage and the stop reason.
type SSEParser struct {
	chat bool
	line []byte
	over bool
	res  Result
	stop bool
	bad  bool
}

func NewSSEParser(path string) SSEParser { return SSEParser{chat: path == ChatPath} }

var marks = [][]byte{[]byte(`message_start`), []byte(`message_delta`), []byte(`message_stop`), []byte(`error`)}

func (p *SSEParser) Feed(b []byte) {
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			p.add(b)
			return
		}
		p.add(b[:i])
		p.flush()
		b = b[i+1:]
	}
}

func (p *SSEParser) add(b []byte) {
	if p.over {
		return
	}
	if len(p.line)+len(b) > maxSSELine {
		p.over, p.line = true, p.line[:0]
		return
	}
	p.line = append(p.line, b...)
}

func (p *SSEParser) flush() {
	line := bytes.TrimSuffix(p.line, []byte("\r"))
	p.line = p.line[:0]
	if p.over {
		p.bad = p.bad || p.chat
		p.over = false
		return
	}
	if p.chat {
		if name, ok := bytes.CutPrefix(line, []byte("event:")); ok && bytes.Equal(bytes.TrimSpace(name), []byte("error")) {
			p.bad = true
		}
	}
	payload, ok := bytes.CutPrefix(line, []byte("data:"))
	if !ok {
		return
	}
	if p.chat {
		if p.stop {
			p.bad = true
			return
		}
		if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
			p.stop = true
			return
		}
		var c chatReply
		if json.Unmarshal(payload, &c) != nil || !c.update(&p.res) {
			p.bad = true
		}
		return
	}
	var relevant bool
	for _, m := range marks {
		if bytes.Contains(payload, m) {
			relevant = true
			break
		}
	}
	if !relevant {
		return
	}
	var ev event
	if json.Unmarshal(payload, &ev) != nil {
		return
	}
	switch ev.Type {
	case "message_start":
		p.res.Usage = ev.Message.Usage
		if ev.Message.StopReason != "" {
			p.res.StopReason = ev.Message.StopReason
		}
	case "message_delta":
		u := &p.res.Usage
		for _, f := range []struct {
			dst *int
			v   int
		}{
			{&u.Input, ev.Usage.Input}, {&u.Output, ev.Usage.Output},
			{&u.CacheRead, ev.Usage.CacheRead}, {&u.CacheWrite, ev.Usage.CacheWrite},
			{&u.Creation.Ephemeral5m, ev.Usage.Creation.Ephemeral5m},
			{&u.Creation.Ephemeral1h, ev.Usage.Creation.Ephemeral1h},
		} {
			if f.v > 0 {
				*f.dst = f.v
			}
		}
		if ev.Delta.StopReason != "" {
			p.res.StopReason = ev.Delta.StopReason
		}
	case "message_stop":
		p.stop = true
	case "error":
		p.bad = true
	}
}

func (p *SSEParser) Result() Result {
	r := p.res
	r.Complete = p.stop && !p.bad && (!p.chat || r.StopReason != "" && len(bytes.TrimSpace(p.line)) == 0 && !p.over)
	return r
}
