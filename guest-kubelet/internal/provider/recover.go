package provider

// M5: restart and relist. After a guest-kubelet restart nothing in memory survives: not the host
// command epoch, not which guests are suspended, not whether the node is lent, not the mirror
// attempt counters. Recover re-derives them before the pod controller and the host command
// server start, from what outlives the process:
//
//   - the guests bound to the virtual node and their mirrors, read from the API;
//   - each mirror's suspend state and guest epoch (timeslice.io/suspend-state, guest-epoch);
//   - the host command journal, an annotation on the virtual Node (NodeJournal);
//   - on the M3 path, the cgroup freezer itself (the host wins over a half-written record);
//   - the kill sequence marks on the mirrors (timeslice.io/killing, M4): a kill cut short is finished.
//
// The cordon while held (D-NS-8) needs nothing here: it counts the suspended mirrors, whose state
// is on the mirrors.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/edwinhr716/guest-kubelet/internal/backend/mirror"
	"github.com/edwinhr716/guest-kubelet/internal/hostcmd"
)

// AnnotationHostCommand holds the host command journal (hostcmd.Record, JSON) on the virtual
// Node. The node library's status patches keep annotations they did not set.
const AnnotationHostCommand = "timeslice.io/host-command"

// NodeJournal is a hostcmd.Journal kept on the virtual Node.
type NodeJournal struct {
	Nodes corev1client.NodeInterface
	Name  string
	// HostUID binds the journal to one host Node object. Save stamps it on the record; Load
	// refuses a record stamped with another UID (or none) with a *StaleJournalError, so a lent
	// state written for a host Node that was since deleted and recreated (same name, new UID,
	// GPUs and pods gone) is never restored. Empty disables the check.
	HostUID types.UID
}

// StaleJournalError is returned by NodeJournal.Load when the record was written for another
// host Node object than the one this process runs on.
type StaleJournalError struct {
	Node      string
	SavedUID  string
	ActualUID types.UID
}

func (e *StaleJournalError) Error() string {
	saved := e.SavedUID
	if saved == "" {
		saved = "<none>"
	}
	return fmt.Sprintf("host command journal on Node %s was written for host Node UID %s, not %s",
		e.Node, saved, e.ActualUID)
}

// Load returns the journal record on the Node, or nil when there is none (no Node, no
// annotation). An unreadable record is an error; the caller then treats it as none. A record
// from another host Node object is a *StaleJournalError (see HostUID).
func (j *NodeJournal) Load(ctx context.Context) (*hostcmd.Record, error) {
	n, err := j.Nodes.Get(ctx, j.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil //nolint:nilnil // no Node: no journal
	}
	if err != nil {
		return nil, fmt.Errorf("read the host command journal: %w", err)
	}
	v, ok := n.Annotations[AnnotationHostCommand]
	if !ok || v == "" {
		return nil, nil //nolint:nilnil // no journal yet
	}
	rec := &hostcmd.Record{}
	if err := json.Unmarshal([]byte(v), rec); err != nil {
		return nil, fmt.Errorf("host command journal on Node %s unreadable: %w", j.Name, err)
	}
	if j.HostUID != "" && rec.HostUID != string(j.HostUID) {
		return nil, &StaleJournalError{Node: j.Name, SavedUID: rec.HostUID, ActualUID: j.HostUID}
	}
	return rec, nil
}

// Save writes the record on the Node with a merge patch of the one annotation.
func (j *NodeJournal) Save(ctx context.Context, rec *hostcmd.Record) error {
	if j.HostUID != "" {
		stamped := *rec
		stamped.HostUID = string(j.HostUID)
		rec = &stamped
	}
	v, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode the host command journal: %w", err)
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]string{AnnotationHostCommand: string(v)}},
	})
	if err != nil {
		return fmt.Errorf("encode the journal patch: %w", err)
	}
	if _, err := j.Nodes.Patch(ctx, j.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("write the host command journal: %w", err)
	}
	return nil
}

// RecoverHost is what Recover needs from the mirror backend. *mirror.Backend implements it.
type RecoverHost interface {
	MirrorNow(ctx context.Context, guest *corev1.Pod) (*corev1.Pod, bool, error)
	RestoreAttempts(guests []mirror.Guest, saved map[types.UID]int)
	ReconcileSuspend(ctx context.Context) ([]mirror.Reconciled, error)
	FinishKills(
		ctx context.Context, guests []mirror.Guest, kill mirror.KillFunc, budget time.Duration,
	) ([]mirror.FinishedKill, error)
}

// RecoverServer is what Recover needs from the host command server. *hostcmd.Server implements it.
type RecoverServer interface {
	Restore(ctx context.Context, rec *hostcmd.Record, guests []mirror.Guest) hostcmd.Restored
}

// RecoverConfig configures Recover.
type RecoverConfig struct {
	// Pods lists pods in all namespaces (client.CoreV1().Pods("")).
	Pods corev1client.PodInterface
	// NodeName is the virtual node.
	NodeName string
	Host     RecoverHost
	// Server and Journal are set in host command mode; Server nil means the M3 path (or none).
	Server  RecoverServer
	Journal hostcmd.Journal
	// IsGuest picks the guests among the pods bound to the virtual node (MatchesGuest).
	IsGuest func(*corev1.Pod) bool
	// Kill repeats the agent Kill of a kill sequence a restart interrupted (FinishKills), bounded
	// by KillBudget; nil skips it (the mirror is still deleted).
	Kill       mirror.KillFunc
	KillBudget time.Duration
}

// Recovery is what Recover found and re-derived, logged as one line.
type Recovery struct {
	Guests     int                   `json:"guests"`
	Mirrors    int                   `json:"mirrors"`
	Journal    string                `json:"journal"` // none, unreadable, stale-host, or "<command> epoch <n> <phase>"
	Restored   *hostcmd.Restored     `json:"restored,omitempty"`
	Reconciled []mirror.Reconciled   `json:"reconciled,omitempty"`
	Killed     []mirror.FinishedKill `json:"killed,omitempty"`
	Took       string                `json:"took"`
}

// Recover relists the virtual node's guests and their mirrors after a start and restores the
// state that lives only in memory (see the file comment). Call it after the mirror backend has
// started and before the host command server serves or the pod controller runs. An error means
// the guests could not be listed; the caller should exit and be restarted, not run half blind.
// A missing or unreadable journal is not an error: the server stays fail closed (held) until
// the orchestrator's next command.
func Recover(ctx context.Context, cfg *RecoverConfig) (*Recovery, error) {
	start := time.Now()
	logger := log.G(ctx).WithField("node", cfg.NodeName)
	list, err := cfg.Pods.List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", cfg.NodeName).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("relist guests: %w", err)
	}
	out := &Recovery{Journal: "none"}
	var guests []mirror.Guest
	var errs []error
	for i := range list.Items {
		pod := &list.Items[i]
		if cfg.IsGuest != nil && !cfg.IsGuest(pod) {
			continue
		}
		g := mirror.Guest{Pod: pod}
		m, found, err := cfg.Host.MirrorNow(ctx, pod)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("guest %s/%s: %w", pod.Namespace, pod.Name, err))
		case found:
			g.Mirror = m
			out.Mirrors++
		}
		guests = append(guests, g)
	}
	out.Guests = len(guests)
	if len(errs) > 0 {
		return nil, fmt.Errorf("relist mirrors: %w", errors.Join(errs...))
	}

	// A kill sequence cut short by the restart (timeslice.io/killing, M4) is finished first. Not
	// fatal: a mirror left marked keeps the mark (M4 then never suspends or resumes it), and the
	// next start retries.
	killed, err := cfg.Host.FinishKills(ctx, guests, cfg.Kill, cfg.KillBudget)
	if err != nil {
		logger.WithError(err).Warn("not every interrupted kill sequence was finished")
	}
	out.Killed = killed

	if cfg.Server != nil {
		var rec *hostcmd.Record
		if cfg.Journal != nil {
			rec, err = cfg.Journal.Load(ctx)
			var stale *StaleJournalError
			switch {
			case errors.As(err, &stale):
				// The host Node was recreated: whatever the record says (lent, epoch, attempts)
				// was about pods and GPUs that are gone. Restore nothing; held until the
				// orchestrator's next command.
				logger.WithError(err).Warn("host command journal is from another host Node; fail closed until the next command")
				out.Journal, rec = "stale-host", nil
			case err != nil:
				logger.WithError(err).Warn("host command journal not read; fail closed until the next command")
				out.Journal, rec = "unreadable", nil
			case rec != nil:
				out.Journal = fmt.Sprintf("%s epoch %d %s", rec.Command, rec.Epoch, rec.Phase)
			}
		}
		var saved map[types.UID]int
		if rec != nil {
			saved = rec.Attempts
		}
		cfg.Host.RestoreAttempts(guests, saved)
		restored := cfg.Server.Restore(ctx, rec, guests)
		out.Restored = &restored
	} else {
		rec, err := cfg.Host.ReconcileSuspend(ctx)
		if err != nil {
			// Not fatal: a mirror whose freezer state is unknown keeps its recorded state; the
			// next suspend or resume of it acts as recorded.
			logger.WithError(err).Warn("suspend state not reconciled with the freezer for every mirror")
		}
		out.Reconciled = rec
	}
	out.Took = time.Since(start).String()
	logger.WithField("recovery", out).Info("relisted after start (M5)")
	return out, nil
}
