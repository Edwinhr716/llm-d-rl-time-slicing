package hostcmd

import (
	"context"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	hcpb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/hostcommand/v1alpha1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
)

// Phases of a journal record.
const (
	PhaseRunning = "running"
	PhaseDone    = "done"
)

// Record is the host command journal (M5): the last command this node accepted, written before
// the command acts and again when it is done. After a restart it restores the epoch fence, the
// lent or held state and the outcome of a finished command, and restarts an unfinished one, so
// the orchestrator's retry with the same epoch joins or completes it instead of acting twice.
// The per-guest state is not here: it is on each mirror (timeslice.io/suspend-state).
type Record struct {
	Epoch    int64     `json:"epoch"`
	Command  string    `json:"command"` // hcpb.Command name, for example COMMAND_VACATE
	Deadline time.Time `json:"deadline"`
	Phase    string    `json:"phase"`
	Outcome  string    `json:"outcome,omitempty"` // hcpb.Outcome name once done
	Error    string    `json:"error,omitempty"`
	Guests   []Guest   `json:"guests,omitempty"`
	// Attempts is the mirror attempt counter per guest UID (job ids must not repeat).
	Attempts map[types.UID]int `json:"attempts,omitempty"`
	Updated  time.Time         `json:"updated"`
	// HostUID is the UID of the host Node the record was written on (provider.NodeJournal
	// stamps it). A record from an earlier host Node object of the same name is not restored.
	HostUID string `json:"hostUID,omitempty"`
}

// Guest is one guest's entry of a finished command.
type Guest struct {
	Guest   string `json:"guest"`
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
}

// Journal stores the Record durably. provider.NodeJournal keeps it on the virtual Node.
type Journal interface {
	// Load returns the stored record, or nil when there is none.
	Load(ctx context.Context) (*Record, error)
	Save(ctx context.Context, rec *Record) error
}

// attemptSource is implemented by *mirror.Backend.
type attemptSource interface {
	Attempts() map[types.UID]int
}

// gateRestorer is implemented by *mirror.Backend.
type gateRestorer interface {
	RestoreGate(guest *corev1.Pod, released bool, reason string)
}

func (s *Server) recordOf(op *operation, deadline time.Time, phase string) *Record {
	rec := &Record{
		Epoch: op.epoch, Command: op.command.String(), Deadline: deadline, Phase: phase, Updated: time.Now().UTC(),
	}
	if phase == PhaseDone {
		rec.Outcome, rec.Error = op.outcome.String(), op.err
		for _, g := range op.guests {
			rec.Guests = append(rec.Guests, Guest{Guest: g.GetGuest(), Outcome: g.GetOutcome().String(), Error: g.GetError()})
		}
	}
	if a, ok := s.cfg.Host.(attemptSource); ok {
		rec.Attempts = a.Attempts()
	}
	return rec
}

// save writes op's record unless a newer command replaced op. Best effort: a failed write is
// logged and the command goes on; the restart after it then knows less (fail closed).
func (s *Server) save(ctx context.Context, op *operation, deadline time.Time, phase string) {
	if s.cfg.Journal == nil {
		return
	}
	s.journalMu.Lock()
	defer s.journalMu.Unlock()
	s.mu.Lock()
	current := s.op == op
	s.mu.Unlock()
	if !current {
		return
	}
	rec := s.recordOf(op, deadline, phase)
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.cfg.Journal.Save(wctx, rec); err != nil {
		log.G(ctx).WithError(err).WithField("epoch", op.epoch).WithField("phase", phase).
			Warn("host command: journal not written; a restart now would not know this command")
	}
}

// Restored is what Restore re-derived, for the recovery log line and the tests.
type Restored struct {
	Epoch   int64  `json:"epoch"`
	Command string `json:"command,omitempty"`
	Phase   string `json:"phase,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Lent    bool   `json:"lent"`
	// Restarted: the command was unfinished and runs again with its epoch.
	Restarted bool `json:"restarted"`
	// Per-guest state found on the mirrors.
	Mirrors    int `json:"mirrors"`
	Suspended  int `json:"suspended"`
	Suspending int `json:"suspending"`
	Resuming   int `json:"resuming"`
	Released   int `json:"released"`
}

// Restore re-derives the server's state after a restart (M5), before it serves any command.
// rec is the journal record (nil: none, the server stays fail closed as after a first start);
// guests are the guest pods of the virtual node with their mirrors, read from the API.
//
//   - The epoch fence is the record's epoch: an older command is STALE_EPOCH, as before the restart.
//   - A finished command is kept as done: a retry with the same epoch gets the recorded ack
//     without acting again.
//   - An unfinished command runs again with its epoch (a Resume gets at least ResumeBudget
//     more), once Ready is closed; the orchestrator's retry joins it. Each guest step is
//     idempotent on the mirror's suspend state.
//   - The last command a Resume: the node is lent again (the orchestrator never resends it).
//     A guest whose mirror runs, is not suspended and was Ready keeps Ready (no flap).
//   - Held, or suspended: each guest's Ready stays held with the reason its mirror shows.
//
// An unfinished command runs under the server's context (s.ctx), not the caller's: it outlives Restore.
func (s *Server) Restore(ctx context.Context, rec *Record, guests []mirror.Guest) Restored { //nolint:contextcheck // see above
	out := Restored{}
	lent := rec != nil && rec.Command == hcpb.Command_COMMAND_RESUME.String()
	gates, hasGates := s.cfg.Host.(gateRestorer)
	for _, guest := range guests {
		if !s.cfg.IsGuest(guest.Pod) || guest.Mirror == nil {
			continue
		}
		out.Mirrors++
		state, _ := mirror.SuspendState(guest.Mirror)
		switch state {
		case mirror.StateSuspended:
			out.Suspended++
		case mirror.StateSuspending:
			out.Suspending++
		case mirror.StateResuming:
			out.Resuming++
		}
		if !hasGates {
			continue
		}
		switch {
		case state == mirror.StateSuspended:
			gates.RestoreGate(guest.Pod, false, mirror.ReasonSuspended)
		case state != "":
			gates.RestoreGate(guest.Pod, false, mirror.ReasonSuspending)
		case lent && guestLive(guest.Pod) && podReady(guest.Pod) && guest.Mirror.Status.Phase == corev1.PodRunning:
			gates.RestoreGate(guest.Pod, true, "")
			s.setReleased(guest.Pod.UID, true)
			s.markServed(guest.Mirror)
			out.Released++
		}
	}
	if rec == nil {
		log.G(ctx).WithField("restored", out).Info("host command: no journal; fail closed until the first command")
		return out
	}
	cmd := hcpb.Command(hcpb.Command_value[rec.Command])
	out.Epoch, out.Command, out.Phase, out.Outcome, out.Lent = rec.Epoch, rec.Command, rec.Phase, rec.Outcome, lent

	s.mu.Lock()
	s.epoch, s.epochCmd = rec.Epoch, cmd
	op := &operation{command: cmd, epoch: rec.Epoch, cancel: func() {}, done: make(chan struct{})}
	s.op = op
	if rec.Phase == PhaseDone {
		op.outcome, op.err = hcpb.Outcome(hcpb.Outcome_value[rec.Outcome]), rec.Error
		for _, g := range rec.Guests {
			op.guests = append(op.guests, &hcpb.GuestResult{
				Guest: g.Guest, Outcome: hcpb.Outcome(hcpb.Outcome_value[g.Outcome]), Error: g.Error,
			})
		}
		close(op.done)
		s.mu.Unlock()
		if lent {
			s.startLending(op) //nolint:contextcheck // the serve loop lives as long as the server
		}
	} else {
		deadline := rec.Deadline
		if cmd == hcpb.Command_COMMAND_RESUME {
			deadline = maxTime(deadline, time.Now().Add(s.cfg.ResumeBudget))
		}
		opCtx, cancel := context.WithCancel(s.ctx)
		op.cancel = cancel
		s.wg.Add(1)
		s.mu.Unlock()
		out.Restarted = true
		go func() {
			defer s.wg.Done()
			defer cancel()
			s.run(opCtx, op, deadline)
		}()
	}
	log.G(ctx).WithField("restored", out).Info("host command: state restored from the journal and the mirrors")
	return out
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
