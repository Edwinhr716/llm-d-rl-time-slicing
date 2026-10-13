package probe

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// The handlers below are lead decision D-VK-5 option c: the guest kubelet accepts every probe,
// so it must also run exec and grpc readiness (and startup) probes. Options a and b never reach
// them (admission refuses those probes) and keep NetProber and Supported.

// SupportedAll accepts every probe handler (httpGet, tcpSocket, exec, grpc).
func SupportedAll(p *corev1.Probe) error {
	switch {
	case p.HTTPGet != nil, p.TCPSocket != nil, p.Exec != nil, p.GRPC != nil:
		return nil
	}
	return fmt.Errorf("%w: no handler", ErrUnsupported)
}

// Execer runs a command in a container of a pod and returns an error unless it exits 0.
type Execer interface {
	Exec(ctx context.Context, namespace, pod, container string, command []string) error
}

// AllProber runs every probe handler: httpGet and tcpSocket over the network (NetProber), grpc
// with the standard gRPC health check against the pod IP, and exec inside the mirror's
// container through Exec. A frozen mirror fails all of them by timeout, as it should.
type AllProber struct {
	Exec Execer
}

// Probe runs one attempt. The deadline comes from ctx.
func (p AllProber) Probe(ctx context.Context, target Target) error {
	spec := target.Spec()
	switch {
	case spec.Exec != nil:
		if p.Exec == nil {
			return fmt.Errorf("%w: exec (no executor)", ErrUnsupported)
		}
		return p.Exec.Exec(ctx, target.MirrorNamespace, target.MirrorName, target.Container.Name, spec.Exec.Command)
	case spec.GRPC != nil:
		return probeGRPC(ctx, target.PodIP, spec.GRPC)
	}
	return NetProber{}.Probe(ctx, target)
}

// probeGRPC is the kubelet's grpc probe: grpc.health.v1.Health/Check on podIP:port without TLS,
// for the probe's service (default ""), success only on SERVING.
func probeGRPC(ctx context.Context, podIP string, action *corev1.GRPCAction) error {
	port, err := checkPort(int(action.Port))
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(net.JoinHostPort(podIP, strconv.Itoa(port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUserAgent(userAgent))
	if err != nil {
		return fmt.Errorf("grpc client: %w", err)
	}
	defer conn.Close()
	service := ""
	if action.Service != nil {
		service = *action.Service
	}
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		return fmt.Errorf("grpc health check: %w", err)
	}
	if st := resp.GetStatus(); st != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("grpc service unhealthy (responded with %q)", st.String())
	}
	return nil
}

// maxExecOutput bounds how much exec probe output is kept for the event message.
const maxExecOutput = 1024

// PodExecer runs exec probes through the API server (pods/exec on the mirror), which is how
// the guest kubelet reaches inside a container it does not run itself. It needs the pods/exec
// permission.
type PodExecer struct {
	Config *rest.Config
	Client kubernetes.Interface
}

// Exec runs command in the container and waits for it, up to the deadline in ctx.
func (e PodExecer) Exec(ctx context.Context, namespace, pod, container string, command []string) error {
	req := e.Client.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).
		SubResource("exec").VersionedParams(&corev1.PodExecOptions{
		Container: container, Command: command, Stdout: true, Stderr: true,
	}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(e.Config, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("exec setup: %w", err)
	}
	var out limitedBuffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &out, Stderr: &out}); err != nil {
		return fmt.Errorf("exec %v: %w (output %q)", command, err, out.String())
	}
	return nil
}

// limitedBuffer keeps the first maxExecOutput bytes written to it and drops the rest. Stdout
// and stderr are copied into it from two goroutines.
type limitedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := maxExecOutput - b.buf.Len(); room > 0 {
		b.buf.Write(data[:min(room, len(data))])
	}
	return len(data), nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
