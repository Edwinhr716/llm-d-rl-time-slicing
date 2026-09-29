package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
	"github.com/edwinhr716/guest-kubelet/internal/probe"
)

// debugHandler serves the test hooks behind --debug-addr (loopback only, no authentication; the
// guest kubelet runs with host networking). With a prober (M2), probe.DebugHandler's
// /debug/readiness and /debug/ready-edges. With a snapshot agent (M4), suspend and resume by
// hand, per guest or for the whole host (D-NS-5 ns-host):
//
//	POST /debug/suspend?namespace=<ns>&name=<guest>[&within=<duration>]
//	POST /debug/resume?namespace=<ns>&name=<guest>[&within=<duration>]
//	POST /debug/suspend-all[?within=<duration>]
//	POST /debug/resume-all[?within=<duration>]
//	GET  /debug/agent
//
// within sets the agent deadline to now + within; without it the deadline is now + N - K. The
// reply is the mirror.Result (or mirror.HostResult) as JSON (durations in nanoseconds); on
// failure {"error": "...", "result": {...}}. With --agent-fault-injection (VK-A4 fault tests):
//
//	GET    /debug/fault                               armed faults and their hits
//	POST   /debug/fault?rpc=<Suspend|...>&kind=<hang|...>[&count=<n, -1 = until cleared>]
//	DELETE /debug/fault                               clear all
func debugHandler(
	backend *mirror.Backend, prober *probe.Manager, edges *probe.EdgeLog, faults *hostcmd.FaultInjector,
) http.Handler {
	mux := http.NewServeMux()
	if prober != nil {
		mux.Handle("/debug/", probe.DebugHandler(prober, edges))
	}
	run := func(w http.ResponseWriter, r *http.Request, f func(context.Context, time.Time) (any, error)) {
		// A client that hangs up must not leave a suspend half done.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Minute)
		defer cancel()
		var deadline time.Time
		if v := r.URL.Query().Get("within"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				writeJSON(r, w, http.StatusBadRequest, map[string]string{"error": "within: " + err.Error()})
				return
			}
			deadline = time.Now().Add(d)
		}
		res, err := f(ctx, deadline)
		if err != nil {
			writeJSON(r, w, debugStatus(err), map[string]any{"error": err.Error(), "result": res})
			return
		}
		writeJSON(r, w, http.StatusOK, res)
	}
	guestOp := func(f func(context.Context, string, string, time.Time) (mirror.Result, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			run(w, r, func(ctx context.Context, deadline time.Time) (any, error) {
				return f(ctx, q.Get("namespace"), q.Get("name"), deadline)
			})
		}
	}
	hostOp := func(f func(context.Context, time.Time) (mirror.HostResult, error)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			run(w, r, func(ctx context.Context, deadline time.Time) (any, error) { return f(ctx, deadline) })
		}
	}
	mux.Handle("POST /debug/suspend", guestOp(backend.Suspend))
	mux.Handle("POST /debug/resume", guestOp(backend.Resume))
	mux.Handle("POST /debug/suspend-all", hostOp(backend.SuspendAll))
	mux.Handle("POST /debug/resume-all", hostOp(backend.ResumeAll))
	mux.HandleFunc("GET /debug/agent", func(w http.ResponseWriter, r *http.Request) {
		jobs, at, err := backend.AgentJobs()
		hold, why := backend.Hold()
		body := map[string]any{"jobs": jobs, "readAt": at, "hold": hold, "holdReason": why}
		if err != nil {
			body["error"] = err.Error()
		}
		writeJSON(r, w, http.StatusOK, body)
	})
	if faults != nil {
		mux.HandleFunc("GET /debug/fault", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(r, w, http.StatusOK, faults.List())
		})
		mux.HandleFunc("POST /debug/fault", func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			count := 0
			if v := q.Get("count"); v != "" {
				n, err := strconv.Atoi(v)
				if err != nil {
					writeJSON(r, w, http.StatusBadRequest, map[string]string{"error": "count: " + err.Error()})
					return
				}
				count = n
			}
			if err := faults.Arm(q.Get("rpc"), q.Get("kind"), count); err != nil {
				writeJSON(r, w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			log.G(r.Context()).WithField("rpc", q.Get("rpc")).WithField("kind", q.Get("kind")).WithField("count", count).
				Warn("agent fault armed")
			writeJSON(r, w, http.StatusOK, faults.List())
		})
		mux.HandleFunc("DELETE /debug/fault", func(w http.ResponseWriter, r *http.Request) {
			faults.Clear()
			log.G(r.Context()).Warn("agent faults cleared")
			writeJSON(r, w, http.StatusOK, faults.List())
		})
	}
	return mux
}

func writeJSON(r *http.Request, w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.G(r.Context()).WithError(err).Debug("debug reply not written")
	}
}

func debugStatus(err error) int {
	switch {
	case errdefs.IsNotFound(err):
		return http.StatusNotFound
	case errdefs.IsInvalidInput(err):
		return http.StatusBadRequest
	case errors.Is(err, mirror.ErrBusy):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
