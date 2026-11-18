package anthropic

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const ChatPath = "/v1/chat/completions"

// InspectChat reads requests sent through magpie's Chat Completions bridge.
// The bridge can add Claude cache breakpoints, so the reply must confirm usage.
func InspectChat(body []byte) (Info, error) {
	var req struct {
		Model           string            `json:"model"`
		Tools           json.RawMessage   `json:"tools"`
		Messages        []json.RawMessage `json:"messages"`
		CacheKey        json.RawMessage   `json:"prompt_cache_key"`
		ReasoningEffort string            `json:"reasoning_effort"`
		Reasoning       json.RawMessage   `json:"reasoning"`
		Thinking        json.RawMessage   `json:"thinking"`
		OutputConfig    json.RawMessage   `json:"output_config"`
		Thread          json.RawMessage   `json:"thread"`
		WebSearch       json.RawMessage   `json:"web_search_options"`
		N               int               `json:"n"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return Info{}, err
	}
	if req.Model == "" {
		return Info{}, errors.New("request has no model")
	}
	info := Info{Model: req.Model, Tools: req.Tools, TTL: 5 * time.Minute, Cacheable: true, Replayable: true}
	serverTool := present(req.WebSearch) || hasWebSearch(req.Tools)
	prefix := []json.RawMessage{req.CacheKey}
	for _, raw := range req.Messages {
		var msg struct {
			Role  string          `json:"role"`
			Tools json.RawMessage `json:"tools"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			return Info{}, err
		}
		if msg.Role == "system" || msg.Role == "developer" {
			prefix = append(prefix, raw)
		} else if info.First == nil {
			info.First = raw
		}
		if present(msg.Tools) {
			prefix = append(prefix, msg.Tools)
			serverTool = serverTool || hasWebSearch(msg.Tools)
		}
	}
	info.System, _ = json.Marshal(prefix)

	switch {
	case present(req.Thread), req.N > 1, serverTool:
		info.Replayable, info.Reason = false, "chat request uses state, web search, or multiple choices"
	case (req.ReasoningEffort != "" && !strings.EqualFold(strings.TrimSpace(req.ReasoningEffort), "none")),
		present(req.Reasoning), present(req.Thinking), present(req.OutputConfig):
		// Magpie can derive the Claude thinking budget from the output cap.
		info.Replayable, info.Reason = false, "chat reasoning may depend on the output limit"
	}
	return info, nil
}

func hasWebSearch(raw json.RawMessage) bool {
	var tools []struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &tools)
	for _, tool := range tools {
		if strings.HasPrefix(tool.Type, "web_search") {
			return true
		}
	}
	return false
}

func present(raw json.RawMessage) bool {
	return len(raw) != 0 && !bytes.Equal(raw, []byte("null"))
}

// WarmRequest preserves the cache prefix and reduces the output to one token.
func WarmRequest(path string, body []byte) ([]byte, error) {
	if path != ChatPath {
		return WarmBody(body)
	}
	var fields struct {
		MaxCompletionTokens json.RawMessage `json:"max_completion_tokens"`
	}
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	limit := "max_tokens"
	if len(fields.MaxCompletionTokens) != 0 {
		limit = "max_completion_tokens"
	}
	return rewrite(body, limit, true)
}

type chatReply struct {
	Error   json.RawMessage `json:"error"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		Prompt  int `json:"prompt_tokens"`
		Output  int `json:"completion_tokens"`
		Details struct {
			Read  *int `json:"cached_tokens"`
			Write *int `json:"cache_write_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

func (c chatReply) update(res *Result) bool {
	if present(c.Error) || len(c.Choices) > 1 {
		return false
	}
	if len(c.Choices) == 1 {
		switch c.Choices[0].FinishReason {
		case "":
		case "tool_calls", "function_call":
			res.StopReason = "tool_use"
		case "stop", "length":
			res.StopReason = c.Choices[0].FinishReason
		default:
			return false
		}
	}
	if u := c.Usage; u != nil && u.Prompt > 0 && (u.Details.Read != nil || u.Details.Write != nil) {
		var read, write int
		if u.Details.Read != nil {
			read = *u.Details.Read
		}
		if u.Details.Write != nil {
			write = *u.Details.Write
		}
		if read < 0 || write < 0 || read > u.Prompt || write > u.Prompt-read || u.Output < 0 {
			return false
		}
		res.Usage = Usage{Input: u.Prompt - read - write, Output: u.Output, CacheRead: read, CacheWrite: write}
	}
	return true
}

func ParseReply(path string, body []byte) (Result, bool) {
	if path != ChatPath {
		return ParseJSON(body)
	}
	var c chatReply
	var res Result
	if json.Unmarshal(body, &c) != nil || !c.update(&res) || res.StopReason == "" {
		return Result{}, false
	}
	res.Complete = true
	return res, true
}
