package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/probe"
)

// debugHandler serves the test hooks behind --debug-addr (loopback only, no authentication; the
// guest kubelet runs with host networking). With a prober (M2), probe.DebugHandler's
// /debug/readiness and /debug/ready-edges. Always (M3, through the snapshot agent since M4),
// suspend and resume by hand:
//
//	POST /debug/suspend?namespace=<ns>&name=<guest>
//	POST /debug/resume?namespace=<ns>&name=<guest>
//	GET  /debug/freeze-state?namespace=<ns>&name=<guest>
//
// The reply is the mirror.Result as JSON (durations in nanoseconds), or on failure
// {"error": "...", "result": {...}}. A call that ended in the kill sequence answers 410 Gone.
// freeze-state (M5) replies with mirror.FreezeState: the mirror, its recorded suspend state and
// epoch, and what the agent says now (suspended or not).
func debugHandler(backend *mirror.Backend, prober *probe.Manager, edges *probe.EdgeLog) http.Handler {
	mux := http.NewServeMux()
	if prober != nil {
		mux.Handle("/debug/", probe.DebugHandler(prober, edges))
	}
	op := func(f func(context.Context, string, string) (mirror.Result, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// A client that hangs up must not leave a suspend half done.
			ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Minute)
			defer cancel()
			q := r.URL.Query()
			res, err := f(ctx, q.Get("namespace"), q.Get("name"))
			w.Header().Set("Content-Type", "application/json")
			var body any = res
			if err != nil {
				body = map[string]any{"error": err.Error(), "result": res}
				w.WriteHeader(debugStatus(err))
			}
			if err := json.NewEncoder(w).Encode(body); err != nil {
				log.G(r.Context()).WithError(err).Debug("debug reply not written")
			}
		}
	}
	mux.Handle("POST /debug/suspend", op(backend.Suspend))
	mux.Handle("POST /debug/resume", op(backend.Resume))
	mux.HandleFunc("GET /debug/freeze-state", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		fs, err := backend.FreezeStateOf(r.Context(), q.Get("namespace"), q.Get("name"))
		w.Header().Set("Content-Type", "application/json")
		var body any = fs
		if err != nil {
			body = map[string]string{"error": err.Error()}
			w.WriteHeader(debugStatus(err))
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			log.G(r.Context()).WithError(err).Debug("debug reply not written")
		}
	})
	return mux
}

func debugStatus(err error) int {
	switch {
	case errdefs.IsNotFound(err):
		return http.StatusNotFound
	case errdefs.IsInvalidInput(err):
		return http.StatusBadRequest
	case errors.Is(err, mirror.ErrKilled):
		return http.StatusGone
	case errors.Is(err, mirror.ErrBusy):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
