// Package probe is the guest kubelet's readiness prober (M2).
//
// The mirror pod carries no probes: a frozen process would fail them, and the real kubelet would
// then restart or unready a pod that is only suspended. So the guest kubelet runs each guest
// container's readinessProbe itself, against the mirror's pod IP, with the probe's own period,
// timeout and thresholds, and computes the guest's Ready condition from the results. A debug
// override forces a guest's readiness without touching the process, so the endpoint path can be
// exercised many times quickly.
package probe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
)

// Event reasons written on the guest.
const (
	// ReasonUnhealthy is the real kubelet's reason for a failed probe.
	ReasonUnhealthy = "Unhealthy"
	// ReasonProbeUnsupported marks a readinessProbe the guest kubelet cannot run (exec, gRPC).
	ReasonProbeUnsupported = "ReadinessProbeUnsupported"
)

// Kubernetes API defaults for probe fields left at zero.
const (
	defaultPeriod           = 10 * time.Second
	defaultTimeout          = 1 * time.Second
	defaultSuccessThreshold = 1
	defaultFailureThreshold = 3
	maxBodyBytes            = 10 * 1024
	userAgent               = "kube-probe/guest-kubelet"
)

// ErrUnsupported is returned for probe handlers the guest kubelet cannot run from outside the
// container (exec and gRPC).
var ErrUnsupported = errors.New("probe handler not supported by the guest kubelet")

// Target is one probe to run: the container's readinessProbe against the mirror's pod IP.
type Target struct {
	PodIP     string
	Container *corev1.Container
}

// Prober runs one probe attempt. A nil error means success.
type Prober interface {
	Probe(ctx context.Context, target Target) error
}

// Options configures a Manager.
type Options struct {
	// OnChange is called, without any lock held, when a guest's readiness verdict changes.
	// The backend re-translates the guest's status and notifies the pod controller.
	OnChange func(namespace, name string)
	// Recorder receives Unhealthy and ReadinessProbeUnsupported events on the guest. May be nil.
	Recorder record.EventRecorder
	// Prober runs the probes. Nil means NetProber.
	Prober Prober
}

type workerKey struct {
	uid       types.UID
	container string
}

type worker struct {
	cancel      context.CancelFunc
	containerID string
	podIP       string
}

// Manager runs one probe worker per guest container that has a readinessProbe, while that
// container runs in the mirror. It keeps results in memory only: after a restart every probed
// container starts not ready again, which is what the real kubelet does too.
type Manager struct {
	base context.Context // workers outlive the Sync call that starts them
	opts Options

	mu          sync.Mutex
	workers     map[workerKey]*worker
	results     map[workerKey]bool
	unsupported map[workerKey]bool
	overrides   map[string]bool // namespace/name -> forced readiness
}

// NewManager returns a Manager whose workers stop when ctx ends.
func NewManager(ctx context.Context, opts Options) *Manager {
	if opts.Prober == nil {
		opts.Prober = NetProber{}
	}
	if opts.OnChange == nil {
		opts.OnChange = func(string, string) {}
	}
	return &Manager{
		base: ctx, opts: opts,
		workers:     map[workerKey]*worker{},
		results:     map[workerKey]bool{},
		unsupported: map[workerKey]bool{},
		overrides:   map[string]bool{},
	}
}

func podKey(namespace, name string) string { return namespace + "/" + name }

// ContainerReady returns the guest kubelet's readiness verdict for one guest container, and
// whether it has one. It has none when the container has no readinessProbe and no override is
// set; the container is then ready once it runs, as with the real kubelet.
func (m *Manager) ContainerReady(guest *corev1.Pod, container string) (bool, bool) { //nolint:gocritic // see nonamedreturns
	m.mu.Lock()
	defer m.mu.Unlock()
	if forced, isSet := m.overrides[podKey(guest.Namespace, guest.Name)]; isSet {
		return forced, true
	}
	spec := findContainer(guest, container)
	if spec == nil || spec.ReadinessProbe == nil {
		return false, false
	}
	return m.results[workerKey{guest.UID, container}], true
}

// SetOverride forces a guest's readiness (true or false) or, with nil, returns it to the probes.
// It is a test hook behind the loopback-only debug endpoint; nothing else calls it.
func (m *Manager) SetOverride(namespace, name string, ready *bool) {
	m.mu.Lock()
	if ready == nil {
		delete(m.overrides, podKey(namespace, name))
	} else {
		m.overrides[podKey(namespace, name)] = *ready
	}
	m.mu.Unlock()
	m.opts.OnChange(namespace, name)
}

// Sync starts, restarts or stops the probe workers for a guest given its mirror's current state.
// The backend calls it on every mirror change, before it translates the status, so a stopped
// worker's result (not ready) is already in place for that translation.
func (m *Manager) Sync(guest, mirror *corev1.Pod) {
	podRunning := mirror.Status.Phase == corev1.PodRunning && mirror.Status.PodIP != ""
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range guest.Spec.Containers {
		spec := &guest.Spec.Containers[i]
		if spec.ReadinessProbe == nil {
			continue
		}
		key := workerKey{guest.UID, spec.Name}
		status := findStatus(mirror.Status.ContainerStatuses, spec.Name)
		if !podRunning || status == nil || status.State.Running == nil {
			m.stopLocked(key)
			continue
		}
		if err := Supported(spec.ReadinessProbe); err != nil {
			m.stopLocked(key)
			if !m.unsupported[key] {
				m.unsupported[key] = true
				m.eventf(guest, corev1.EventTypeWarning, ReasonProbeUnsupported,
					"readinessProbe of container %q: %v; the container is reported not ready", spec.Name, err)
			}
			continue
		}
		if cur := m.workers[key]; cur != nil && cur.containerID == status.ContainerID && cur.podIP == mirror.Status.PodIP {
			continue
		}
		// New container (first start or a restart) or a new IP: start over from not ready.
		m.stopLocked(key)
		ctx, cancel := context.WithCancel(m.base)
		job := &worker{cancel: cancel, containerID: status.ContainerID, podIP: mirror.Status.PodIP}
		m.workers[key] = job
		target := Target{PodIP: mirror.Status.PodIP, Container: spec.DeepCopy()}
		go m.run(ctx, job, key, guest.DeepCopy(), target, status.State.Running.StartedAt.Time)
	}
}

// Forget stops every worker of a guest and drops its results and override. The backend calls it
// when the guest's mirror is gone.
func (m *Manager) Forget(uid types.UID, namespace, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.workers {
		if key.uid == uid {
			m.stopLocked(key)
		}
	}
	for key := range m.results {
		if key.uid == uid {
			delete(m.results, key)
		}
	}
	for key := range m.unsupported {
		if key.uid == uid {
			delete(m.unsupported, key)
		}
	}
	if namespace != "" && name != "" {
		delete(m.overrides, podKey(namespace, name))
	}
}

func (m *Manager) stopLocked(key workerKey) {
	if job := m.workers[key]; job != nil {
		job.cancel()
		delete(m.workers, key)
	}
	delete(m.results, key)
}

// setResult stores a verdict if job is still the current worker for key, and reports whether
// it did.
func (m *Manager) setResult(job *worker, key workerKey, ready bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.workers[key] != job {
		return false
	}
	m.results[key] = ready
	return true
}

func (m *Manager) eventf(guest *corev1.Pod, eventType, reason, format string, args ...any) {
	if m.opts.Recorder != nil {
		m.opts.Recorder.Eventf(guest, eventType, reason, format, args...)
	}
}

// run is one worker: wait out initialDelaySeconds from the container start, then probe every
// periodSeconds with timeoutSeconds, and flip the verdict after successThreshold consecutive
// successes or failureThreshold consecutive failures. The verdict starts not ready.
func (m *Manager) run(
	ctx context.Context, job *worker, key workerKey, guest *corev1.Pod, target Target, startedAt time.Time,
) {
	spec := target.Container.ReadinessProbe
	logger := log.G(ctx).WithField("guest", podKey(guest.Namespace, guest.Name)).WithField("container", key.container)
	if wait := time.Until(startedAt.Add(seconds(spec.InitialDelaySeconds, 0))); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
	ticker := time.NewTicker(seconds(spec.PeriodSeconds, defaultPeriod))
	defer ticker.Stop()
	th := NewThresholds(spec.SuccessThreshold, spec.FailureThreshold)
	for {
		probeCtx, cancel := context.WithTimeout(ctx, seconds(spec.TimeoutSeconds, defaultTimeout))
		err := m.opts.Prober.Probe(probeCtx, target)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			m.eventf(guest, corev1.EventTypeWarning, ReasonUnhealthy, "Readiness probe failed: %v", err)
		}
		if ready, changed := th.Observe(err == nil); changed && m.setResult(job, key, ready) {
			logger.WithField("ready", ready).Info("readiness probe verdict changed")
			m.opts.OnChange(guest.Namespace, guest.Name)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Thresholds turns probe attempts into a verdict the way the kubelet does: the verdict starts
// not ready and flips only after successThreshold consecutive successes (to ready) or
// failureThreshold consecutive failures (to not ready).
type Thresholds struct {
	success, failure int32
	ready            bool
	last             bool
	run              int32
}

// NewThresholds applies the API defaults (1 and 3) to zero values.
func NewThresholds(success, failure int32) *Thresholds {
	if success <= 0 {
		success = defaultSuccessThreshold
	}
	if failure <= 0 {
		failure = defaultFailureThreshold
	}
	return &Thresholds{success: success, failure: failure}
}

// Observe records one attempt. It returns the verdict and whether this attempt changed it.
func (t *Thresholds) Observe(success bool) (bool, bool) { //nolint:gocritic // see nonamedreturns
	if t.run == 0 || success != t.last {
		t.last, t.run = success, 0
	}
	t.run++
	switch {
	case success && !t.ready && t.run >= t.success:
		t.ready = true
		return true, true
	case !success && t.ready && t.run >= t.failure:
		t.ready = false
		return false, true
	}
	return t.ready, false
}

// Supported returns nil for the handlers the guest kubelet runs (httpGet, tcpSocket) and
// ErrUnsupported otherwise.
func Supported(p *corev1.Probe) error {
	switch {
	case p.HTTPGet != nil, p.TCPSocket != nil:
		return nil
	case p.Exec != nil:
		return fmt.Errorf("%w: exec", ErrUnsupported)
	case p.GRPC != nil:
		return fmt.Errorf("%w: grpc", ErrUnsupported)
	}
	return fmt.Errorf("%w: no handler", ErrUnsupported)
}

// NetProber runs httpGet and tcpSocket probes over the network, like the kubelet: HTTP without
// keep-alives, TLS without verification, redirects not followed (a 3xx counts as success), and
// success for any status in [200, 400).
type NetProber struct{}

// Probe runs one attempt. The deadline comes from ctx.
func (NetProber) Probe(ctx context.Context, target Target) error {
	spec := target.Container.ReadinessProbe
	switch {
	case spec.HTTPGet != nil:
		return probeHTTP(ctx, target.PodIP, target.Container, spec.HTTPGet)
	case spec.TCPSocket != nil:
		return probeTCP(ctx, target.PodIP, target.Container, spec.TCPSocket)
	}
	return Supported(spec)
}

var probeClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
		// The kubelet does not verify probe certificates either.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // probe semantics, as in the kubelet
	},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func probeHTTP(ctx context.Context, podIP string, ctr *corev1.Container, get *corev1.HTTPGetAction) error {
	port, err := ResolvePort(get.Port, ctr)
	if err != nil {
		return err
	}
	host := get.Host
	if host == "" {
		host = podIP
	}
	scheme := strings.ToLower(string(get.Scheme))
	if scheme == "" {
		scheme = "http"
	}
	path := get.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	target, err := url.Parse(scheme + "://" + net.JoinHostPort(host, strconv.Itoa(port)) + path)
	if err != nil {
		return fmt.Errorf("probe URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), http.NoBody)
	if err != nil {
		return fmt.Errorf("probe request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "*/*")
	for _, hdr := range get.HTTPHeaders {
		if strings.EqualFold(hdr.Name, "Host") {
			req.Host = hdr.Value
			continue
		}
		req.Header.Set(hdr.Name, hdr.Value)
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", target.Redacted(), err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes)); err != nil {
		return fmt.Errorf("read %s: %w", target.Redacted(), err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("HTTP probe failed with statuscode: %d", resp.StatusCode)
	}
	return nil
}

func probeTCP(ctx context.Context, podIP string, ctr *corev1.Container, sock *corev1.TCPSocketAction) error {
	port, err := ResolvePort(sock.Port, ctr)
	if err != nil {
		return err
	}
	host := sock.Host
	if host == "" {
		host = podIP
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

// ResolvePort turns a probe port (a number, or the name of one of the container's ports) into
// a port number.
func ResolvePort(port intstr.IntOrString, ctr *corev1.Container) (int, error) {
	if port.Type == intstr.Int {
		return checkPort(int(port.IntVal))
	}
	for _, cp := range ctr.Ports {
		if cp.Name == port.StrVal {
			return checkPort(int(cp.ContainerPort))
		}
	}
	num, err := strconv.Atoi(port.StrVal)
	if err != nil {
		return 0, fmt.Errorf("port %q is not a number and not a named port of container %q", port.StrVal, ctr.Name)
	}
	return checkPort(num)
}

func checkPort(port int) (int, error) {
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port %d", port)
	}
	return port, nil
}

func seconds(value int32, def time.Duration) time.Duration {
	if value <= 0 {
		return def
	}
	return time.Duration(value) * time.Second
}

func findContainer(pod *corev1.Pod, name string) *corev1.Container {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i]
		}
	}
	return nil
}

func findStatus(statuses []corev1.ContainerStatus, name string) *corev1.ContainerStatus {
	for i := range statuses {
		if statuses[i].Name == name {
			return &statuses[i]
		}
	}
	return nil
}
