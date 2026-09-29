// Package donorstandin is a stand-in for the donor controller's host-death duty: when the
// real Node a virtual Node runs on is gone, it releases the virtual Node (deletes it and
// removes the VK finalizer), so the guests on it are cleaned up and their controllers retry.
//
// The VK cannot do this itself: it runs on the host that died. The stand-in runs on another
// node and acts on exactly one virtual Node and one host, both named by flag. It is a
// stand-in only; the real donor controller does not exist yet.
package donorstandin

import (
	"context"
	"errors"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// Config names the one virtual Node and the one real Node the stand-in watches.
type Config struct {
	VirtualNode string
	HostNode    string
	Interval    time.Duration // how often the host is checked
}

// StandIn watches one host and releases one virtual Node when the host is gone.
type StandIn struct {
	client  kubernetes.Interface
	cfg     Config
	hostUID types.UID // first UID seen for the host; a different UID means it was recreated
}

// New returns a StandIn. It fails on an empty node name.
func New(client kubernetes.Interface, cfg Config) (*StandIn, error) {
	if cfg.VirtualNode == "" || cfg.HostNode == "" {
		return nil, errors.New("both the virtual Node and the host Node are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	return &StandIn{client: client, cfg: cfg}, nil
}

// Run checks the host every Interval until ctx is cancelled. Errors are logged and retried.
func (s *StandIn) Run(ctx context.Context) error {
	log.G(ctx).WithField("node", s.cfg.VirtualNode).WithField("host", s.cfg.HostNode).
		WithField("interval", s.cfg.Interval.String()).Info("donor stand-in watching host")
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		if _, err := s.Check(ctx); err != nil {
			log.G(ctx).WithError(err).WithField("node", s.cfg.VirtualNode).Warn("donor stand-in check failed; retrying")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Check runs one pass. If the host is gone and the virtual Node still exists, it releases
// the virtual Node and reports true.
func (s *StandIn) Check(ctx context.Context) (bool, error) {
	why, err := s.hostGone(ctx)
	if err != nil || why == "" {
		return false, err
	}
	released, err := provider.ReleaseNode(ctx, s.client, s.cfg.VirtualNode, provider.ReasonHostGone)
	if err != nil || !released {
		return false, err
	}
	log.G(ctx).WithField("node", s.cfg.VirtualNode).WithField("host", s.cfg.HostNode).
		WithField("reason", provider.ReasonHostGone).WithField("detail", why).Info("virtual node released")
	return true, nil
}

// hostGone returns why the host counts as gone, or "" while it is present.
func (s *StandIn) hostGone(ctx context.Context) (string, error) {
	host, err := s.client.CoreV1().Nodes().Get(ctx, s.cfg.HostNode, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "host Node not found", nil
	}
	if err != nil {
		return "", err
	}
	if s.hostUID == "" {
		s.hostUID = host.UID
		return "", nil
	}
	if host.UID != s.hostUID {
		return "host Node was recreated", nil
	}
	return "", nil
}
