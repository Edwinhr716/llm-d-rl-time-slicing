package mirror

import (
	"context"
	"fmt"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// holdInformerWait bounds how long a suspend-state change waits for the mirror informer before
// it pokes OnHoldChange. The cordon loop's own poll covers a slower informer.
const holdInformerWait = time.Second

// Hold reports whether the donor holds the GPU in M3: at least one guest of this virtual Node is
// Suspending or Suspended. Resuming is not held (the guest kubelet has the GPU back). The answer
// comes from the mirror informer, so a restarted or new leader derives the same one. It feeds
// the ns-cordon option of --cordon-while-held (pending lead decision D-NS-8).
//
//nolint:gocritic // unnamedResult: nonamedreturns forbids naming them
func (b *Backend) Hold() (bool, string) {
	ms, err := b.mirrors.List(labels.Everything())
	if err != nil {
		return false, "mirror list failed: " + err.Error()
	}
	n := 0
	for _, m := range ms {
		switch state, _ := SuspendState(m); state {
		case StateSuspending, StateSuspended:
			n++
		}
	}
	if n == 0 {
		return false, "no guest suspended"
	}
	return true, fmt.Sprintf("%d guest(s) suspended", n)
}

// holdChanged tells OnHoldChange that a guest's suspend state became state, once the mirror
// informer shows it (at most holdInformerWait), so Hold already sees the new answer.
func (b *Backend) holdChanged(ctx context.Context, guest *corev1.Pod, state string) {
	f := b.opts.Suspend.OnHoldChange
	if f == nil {
		return
	}
	if err := b.waitInformerState(ctx, guest, state, holdInformerWait); err != nil {
		log.G(ctx).WithError(err).Debug("hold poke before the informer caught up; the cordon poll repairs it")
	}
	f()
}
