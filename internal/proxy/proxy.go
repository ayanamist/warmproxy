// Package proxy forwards requests to the upstream and arms the warmer from completed replies.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"time"

	"github.com/ayanamist/warmproxy/internal/anthropic"
	"github.com/ayanamist/warmproxy/internal/config"
	"github.com/ayanamist/warmproxy/internal/warmer"
)

const maxJSONReply = 8 << 20

type tapCtx struct {
	info    anthropic.Info
	key     string
	body    []byte
	header  http.Header
	path    string
	query   string
	started time.Time
	// unwarmed marks a model that is not warmed: its reply ends the conversation's run.
	unwarmed bool
}

type ctxKey struct{}

type Server struct {
	cfg *config.Config
	mgr *warmer.Manager
	rp  *httputil.ReverseProxy
	log *slog.Logger
}

// New builds the proxy; transport carries every request to the upstream.
func New(cfg *config.Config, mgr *warmer.Manager, transport http.RoundTripper, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, mgr: mgr, log: log}
	s.rp = &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(cfg.Upstream)
			// Without it the transport asks for gzip itself and hands over the plain body,
			// which the usage parser needs; a br or zstd reply could not be read.
			if _, ok := pr.In.Context().Value(ctxKey{}).(*tapCtx); ok {
				pr.Out.Header.Del("Accept-Encoding")
			}
		},
		FlushInterval:  -1,
		ModifyResponse: s.modify,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Warn("upstream error", "path", r.URL.Path, "err", err)
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/_warm/status":
		s.status(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/messages" && s.cfg.Mode != "off":
		s.messages(w, r)
	default:
		s.rp.ServeHTTP(w, r)
	}
}

func (s *Server) messages(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "cannot read request body", http.StatusBadRequest)
		}
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	info, err := anthropic.Inspect(body)
	if err != nil {
		s.rp.ServeHTTP(w, r)
		return
	}
	t := &tapCtx{
		info: info, body: body, header: r.Header.Clone(),
		path: r.URL.Path, query: r.URL.RawQuery, started: time.Now(),
		key:      warmer.Key(r.Header, s.cfg.SessionHeaders, info),
		unwarmed: !s.cfg.Match.MatchString(info.Model),
	}
	s.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, t)))
}

func (s *Server) modify(resp *http.Response) error {
	t, ok := resp.Request.Context().Value(ctxKey{}).(*tapCtx)
	if !ok || resp.StatusCode != http.StatusOK {
		return nil
	}
	ct := resp.Header.Get("Content-Type")
	stream := strings.HasPrefix(ct, "text/event-stream")
	if !stream && !strings.HasPrefix(ct, "application/json") {
		return nil
	}
	resp.Body = &tap{rc: resp.Body, stream: stream, finish: func(res anthropic.Result) {
		if !res.Complete {
			return
		}
		if t.unwarmed {
			s.mgr.Disarm(t.key)
			return
		}
		s.mgr.Arm(warmer.ArmInput{
			Key: t.key, Info: t.info, Path: t.path, Query: t.query, Started: t.started,
			Header: t.header, Body: t.body, Result: res,
		})
	}}
	return nil
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"mode": s.cfg.Mode, "runs": s.mgr.Status()})
}

// tap passes a reply body through unchanged while following its usage, and reports the
// result once, when the body ends. A body the client abandons before its end reports nothing.
type tap struct {
	rc     io.ReadCloser
	stream bool
	finish func(anthropic.Result)

	sse  anthropic.SSEParser
	json bytes.Buffer
	over bool
	once sync.Once
}

func (t *tap) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		switch {
		case t.stream:
			t.sse.Feed(p[:n])
		case !t.over:
			if t.json.Len()+n > maxJSONReply {
				t.over = true
				t.json.Reset()
			} else {
				t.json.Write(p[:n])
			}
		}
	}
	if errors.Is(err, io.EOF) {
		t.once.Do(t.report)
	}
	return n, err
}

func (t *tap) report() {
	if t.stream {
		t.finish(t.sse.Result())
		return
	}
	if t.over {
		return
	}
	if res, ok := anthropic.ParseJSON(t.json.Bytes()); ok {
		t.finish(res)
	}
}

func (t *tap) Close() error { return t.rc.Close() }
