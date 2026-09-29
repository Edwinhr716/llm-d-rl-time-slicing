// Command timeslice-webhook is the timeslice mutating admission webhook (pkg/webhook). It serves
// /mutate, /metrics, /healthz and /readyz on one HTTPS port, with the certificate from a mounted
// Secret. It has no Kubernetes client: it only answers the API server.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

func main() {
	if err := run(); err != nil {
		slog.Error("timeslice-webhook failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := webhook.ConfigFromFlags(os.Args[1:])
	if err != nil {
		return err
	}
	certs, err := webhook.NewCertLoader(cfg.CertDir)
	if err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := webhook.NewMetrics(reg)

	mux := http.NewServeMux()
	mux.Handle("/mutate", webhook.NewHandler(cfg, metrics, os.Stdout))
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ok := func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") }
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)

	srv := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(cfg.Port)),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: certs.GetCertificate},
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeTLS("", "") }()
	slog.Info("timeslice-webhook serving", "port", cfg.Port, "guest_steering", cfg.GuestSteering,
		"virtual_node_label", cfg.VirtualNodeLabel, "donor_client_wiring", cfg.DonorClientWiring,
		"group_format", cfg.GroupFormat, "vk_username", cfg.VKUsername)
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
