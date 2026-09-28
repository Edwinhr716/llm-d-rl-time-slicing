package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/logging"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/app"
)

func main() {
	// Initialize slog with ContextHandler
	jsonHandler := slog.NewJSONHandler(os.Stdout, nil)
	ctxHandler := logging.NewContextHandler(jsonHandler)
	slog.SetDefault(slog.New(ctxHandler))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := app.Run(ctx, os.Args[1:], app.Deps{})
	stop()
	if err != nil {
		slog.Error("Failed to run server", "error", err)
		os.Exit(1)
	}
}
