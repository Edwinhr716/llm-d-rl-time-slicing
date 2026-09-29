package dsrule

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

// Review answers one admission request. It always allows: DaemonSet CREATE and UPDATE get the
// exclusion patch, anything else passes unchanged. A request it cannot decode is allowed with
// a warning rather than blocking another team's write.
func Review(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	resp := &admissionv1.AdmissionResponse{UID: req.UID, Allowed: true}
	if req.Kind.Group != "apps" || req.Kind.Kind != "DaemonSet" ||
		(req.Operation != admissionv1.Create && req.Operation != admissionv1.Update) {
		return resp
	}
	var ds appsv1.DaemonSet
	if err := json.Unmarshal(req.Object.Raw, &ds); err != nil {
		resp.Warnings = []string{"daemonset-rule: could not decode the DaemonSet; virtual nodes not excluded"}
		return resp
	}
	patch, err := Patch(&ds)
	if err != nil {
		resp.Warnings = []string{"daemonset-rule: " + err.Error()}
		return resp
	}
	if patch != nil {
		pt := admissionv1.PatchTypeJSONPatch
		resp.Patch, resp.PatchType = patch, &pt
	}
	return resp
}

// Handler serves AdmissionReview v1 on POST.
type Handler struct{}

func (Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil || review.Request == nil {
		http.Error(w, "expected an AdmissionReview with a request", http.StatusBadRequest)
		return
	}
	review.Response = Review(review.Request)
	if review.Response.Patch != nil {
		log.G(r.Context()).WithField("daemonset", review.Request.Namespace+"/"+review.Request.Name).
			WithField("operation", string(review.Request.Operation)).Info("daemonset-rule: excluded virtual nodes")
	}
	review.Request = nil
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&review); err != nil {
		log.G(r.Context()).WithError(err).Warn("daemonset-rule: write admission response")
	}
}

// Config is what the webhook server needs.
type Config struct {
	Namespace     string // namespace of the Service and the certificate Secret
	Service       string // Service name; the certificate is valid for <svc>.<ns>.svc
	Secret        string // Secret holding the serving certificate, shared by all replicas
	WebhookConfig string // MutatingWebhookConfiguration whose caBundle this server sets
	Addr          string // listen address, e.g. ":10250"
}

// DNSNames are the names the API server may use for the Service.
func (c *Config) DNSNames() []string {
	svc := c.Service + "." + c.Namespace
	return []string{c.Service, svc, svc + ".svc", svc + ".svc.cluster.local"}
}

// KeyPair is a PEM-encoded certificate and its private key.
type KeyPair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// GenerateCert makes a self-signed ECDSA serving certificate for dnsNames. It is its own CA:
// the same PEM goes into the webhook's caBundle.
func GenerateCert(dnsNames []string, now time.Time) (KeyPair, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return KeyPair{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return KeyPair{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: dnsNames[0]},
		DNSNames:              dnsNames,
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return KeyPair{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return KeyPair{}, err
	}
	return KeyPair{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// EnsureCert returns the serving certificate from the Secret, creating the Secret with a new
// certificate if it does not exist. Replicas racing to create it all end up with the winner's.
func EnsureCert(ctx context.Context, client kubernetes.Interface, cfg *Config) (KeyPair, error) {
	secrets := client.CoreV1().Secrets(cfg.Namespace)
	get := func() (KeyPair, error) {
		s, err := secrets.Get(ctx, cfg.Secret, metav1.GetOptions{})
		if err != nil {
			return KeyPair{}, err
		}
		if len(s.Data[corev1.TLSCertKey]) == 0 || len(s.Data[corev1.TLSPrivateKeyKey]) == 0 {
			return KeyPair{}, fmt.Errorf("secret %s/%s has no %s/%s",
				cfg.Namespace, cfg.Secret, corev1.TLSCertKey, corev1.TLSPrivateKeyKey)
		}
		return KeyPair{CertPEM: s.Data[corev1.TLSCertKey], KeyPEM: s.Data[corev1.TLSPrivateKeyKey]}, nil
	}
	pair, err := get()
	if err == nil || !apierrors.IsNotFound(err) {
		return pair, err
	}
	pair, err = GenerateCert(cfg.DNSNames(), time.Now())
	if err != nil {
		return KeyPair{}, err
	}
	_, err = secrets.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.Secret, Namespace: cfg.Namespace},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: pair.CertPEM, corev1.TLSPrivateKeyKey: pair.KeyPEM},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return get()
	}
	return pair, err
}

// SetCABundle writes caPEM into every webhook of the MutatingWebhookConfiguration.
func SetCABundle(ctx context.Context, client kubernetes.Interface, name string, caPEM []byte) error {
	mwcs := client.AdmissionregistrationV1().MutatingWebhookConfigurations()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := mwcs.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		changed := false
		for i := range cur.Webhooks {
			if !bytes.Equal(cur.Webhooks[i].ClientConfig.CABundle, caPEM) {
				cur.Webhooks[i].ClientConfig.CABundle = caPEM
				changed = true
			}
		}
		if !changed {
			return nil
		}
		_, err = mwcs.Update(ctx, cur, metav1.UpdateOptions{})
		return err
	})
}

// Run gets the certificate, publishes its CA bundle, and serves /mutate and /healthz over TLS
// until ctx ends. /healthz answers only once the CA bundle is in place, so the pod turns Ready
// only when the API server can call it.
func Run(ctx context.Context, client kubernetes.Interface, cfg *Config) error {
	kp, err := EnsureCert(ctx, client, cfg)
	if err != nil {
		return fmt.Errorf("serving certificate: %w", err)
	}
	pair, err := tls.X509KeyPair(kp.CertPEM, kp.KeyPEM)
	if err != nil {
		return fmt.Errorf("serving certificate: %w", err)
	}
	if err := SetCABundle(ctx, client, cfg.WebhookConfig, kp.CertPEM); err != nil {
		return fmt.Errorf("caBundle on %s: %w", cfg.WebhookConfig, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/mutate", Handler{})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shut); err != nil {
			log.G(ctx).WithError(err).Debug("daemonset-rule: shutdown")
		}
	}()
	log.G(ctx).WithField("addr", cfg.Addr).WithField("webhookConfig", cfg.WebhookConfig).Info("daemonset-rule webhook serving")
	if err := srv.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return ctx.Err()
}
