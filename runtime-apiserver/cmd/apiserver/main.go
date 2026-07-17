// Command apiserver runs wasmCloud WorkloadDeployments against one or more
// wash-hosts over NATS, without Kubernetes or a CRD in the loop.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"go.wasmcloud.dev/runtime-operator/v2/pkg/wasmbus"

	"go.wasmcloud.dev/runtime-apiserver/internal/api"
	"go.wasmcloud.dev/runtime-apiserver/internal/hostregistry"
	"go.wasmcloud.dev/runtime-apiserver/internal/reconciler"
	"go.wasmcloud.dev/runtime-apiserver/internal/store"
)

func main() {
	var (
		listenAddr       string
		natsURL          string
		natsCreds        string
		reconcileEvery   time.Duration
		hostHeartbeatTTL time.Duration
		jsonLog          bool
	)

	flag.StringVar(&listenAddr, "listen-addr", ":8080", "Address the HTTP API binds to")
	flag.StringVar(&natsURL, "nats-url", wasmbus.NatsDefaultURL, "NATS server URL to connect to")
	flag.StringVar(&natsCreds, "nats-creds", "", "Path to a NATS credentials file")
	flag.DurationVar(&reconcileEvery, "reconcile-interval", 5*time.Second, "How often to reconcile WorkloadDeployments against host state")
	flag.DurationVar(&hostHeartbeatTTL, "host-heartbeat-ttl", 45*time.Second, "How long a host stays eligible for placement after its last heartbeat")
	flag.BoolVar(&jsonLog, "json-log", false, "Emit logs as JSON")
	flag.Parse()

	logHandlerOpts := &slog.HandlerOptions{}
	var handler slog.Handler
	if jsonLog {
		handler = slog.NewJSONHandler(os.Stdout, logHandlerOpts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, logHandlerOpts)
	}
	log := slog.New(handler)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	natsOpts := []nats.Option{
		nats.Name("runtime-apiserver"),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Error("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("nats reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Error("nats connection closed")
		}),
	}
	if natsCreds != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(natsCreds))
	}

	nc, err := wasmbus.NatsConnect(natsURL, natsOpts...)
	if err != nil {
		log.Error("failed to connect to nats", "url", natsURL, "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	bus := wasmbus.NewNatsBus(nc)

	st := store.New()
	hosts := hostregistry.New(hostHeartbeatTTL)
	recon := reconciler.New(st, hosts, bus, reconcileEvery, log.With("component", "reconciler"))

	go func() {
		if err := hosts.Run(ctx, bus); err != nil {
			log.Error("host registry stopped", "error", err)
		}
	}()
	go recon.Run(ctx)

	handlerFn := api.NewHandler(api.Deps{
		Store:      st,
		Hosts:      hosts,
		Reconciler: recon,
		Ready:      func() bool { return !nc.IsClosed() },
	}, log.With("component", "api"))

	httpServer := &http.Server{
		Addr:              listenAddr,
		Handler:           handlerFn,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("apiserver listening", "addr", listenAddr, "natsUrl", natsURL)
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Error("http server shutdown error", "error", err)
		}
	}
}
