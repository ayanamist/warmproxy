package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ayanamist/warmproxy/internal/config"
	"github.com/ayanamist/warmproxy/internal/proxy"
	"github.com/ayanamist/warmproxy/internal/warmer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "warmproxy:", err)
		os.Exit(1)
	}
}

func defaultConfig() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "config.toml"
	}
	return filepath.Join(dir, "warmproxy", "config.toml")
}

func run() error {
	path := flag.String("config", defaultConfig(), "path of the TOML configuration")
	debug := flag.Bool("debug", false, "log skipped warming decisions")
	flag.Parse()

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	if host, _, err := net.SplitHostPort(cfg.Listen); err == nil {
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			log.Warn("listening beyond loopback: the proxy holds the credentials of every client it has seen", "listen", cfg.Listen)
		}
	}

	store, err := warmer.NewStore(filepath.Join(cfg.StateDir, "runs"))
	if err != nil {
		return err
	}
	ledger, err := warmer.OpenLedger(filepath.Join(cfg.StateDir, "ledger.jsonl"))
	if err != nil {
		return err
	}
	defer ledger.Close()

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer ln.Close()

	// The upstream is a local program: never through an environment proxy.
	transport := &http.Transport{
		MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second,
		DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	}
	mgr := warmer.New(warmer.Options{
		Config: cfg, Doer: warmer.NewClient(transport), Store: store, Ledger: ledger, Log: log,
	})
	n, err := mgr.Restore()
	if err != nil {
		log.Warn("restoring saved runs failed", "err", err)
	}
	log.Info("warmproxy up", "listen", ln.Addr().String(), "upstream", cfg.Upstream.Host, "mode", cfg.Mode, "restored", n)

	srv := &http.Server{
		Addr: cfg.Listen, Handler: proxy.New(cfg, mgr, transport, log),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()

	select {
	case err := <-failed:
		mgr.Close()
		return err
	case <-ctx.Done():
	}
	sd, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sd); err != nil {
		log.Warn("shutdown", "err", err)
	}
	mgr.Close()
	log.Info("warmproxy stopped")
	return nil
}
