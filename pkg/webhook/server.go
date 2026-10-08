package webhook

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// maxBodyBytes bounds an AdmissionReview body (the API server caps objects well below this).
const maxBodyBytes = 6 << 20

// reviewType is the envelope of every response; only admission.k8s.io/v1 is registered.
var reviewType = metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"}

// Metrics are the webhook's Prometheus metrics.
type Metrics struct {
	admissions *prometheus.CounterVec
	duration   *prometheus.HistogramVec
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		admissions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "timeslice_webhook_admissions_total",
			Help: "Admission requests handled, by pod kind, operation, result and rule.",
		}, []string{"kind", "op", "result", "rule"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "timeslice_webhook_admission_duration_seconds",
			Help:    "Time to decide one admission request, from body read to response written.",
			Buckets: prometheus.ExponentialBuckets(0.00005, 2, 16),
		}, []string{"kind"}),
	}
	reg.MustRegister(m.admissions, m.duration)
	return m
}

// Handler serves AdmissionReview requests: it calls Review, writes the response, logs one line per
// request and records the metrics.
type Handler struct {
	cfg     Config
	metrics *Metrics
	mu      sync.Mutex
	out     io.Writer
	now     func() time.Time
}

// NewHandler returns the /mutate handler. Log lines go to out.
//
//nolint:gocritic // hugeParam: called once at startup; the handler keeps its own copy.
func NewHandler(cfg Config, metrics *Metrics, out io.Writer) *Handler {
	return &Handler{cfg: cfg, metrics: metrics, out: out, now: time.Now}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := h.now()
	if r.Method != http.MethodPost {
		http.Error(w, "POST an AdmissionReview", http.StatusMethodNotAllowed)
		return
	}
	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&review); err != nil || review.Request == nil {
		http.Error(w, "body is not an AdmissionReview with a request", http.StatusBadRequest)
		return
	}
	resp, out := Review(r.Context(), review.Request, &h.cfg)
	resp.UID = review.Request.UID
	body, err := json.Marshal(admissionv1.AdmissionReview{TypeMeta: reviewType, Response: resp})
	if err != nil {
		http.Error(w, "cannot encode the response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(body); err != nil {
		out.Message = "write failed: " + err.Error()
	}
	dur := h.now().Sub(start)
	h.record(&out, dur)
}

// record writes the admit log line and updates the metrics.
func (h *Handler) record(out *Outcome, dur time.Duration) {
	if h.metrics != nil {
		h.metrics.admissions.WithLabelValues(string(out.Kind), out.Op, string(out.Result), out.Rule).Inc()
		h.metrics.duration.WithLabelValues(string(out.Kind)).Observe(dur.Seconds())
	}
	line := fmt.Sprintf("time=%s level=info msg=\"admit\" kind=%s op=%s result=%s rule=%s patch_ops=%d "+
		"dur_ms=%s ns=%s name=%s reason=%s\n",
		h.now().UTC().Format(time.RFC3339Nano), out.Kind, out.Op, out.Result, out.Rule, out.PatchOps,
		strconv.FormatFloat(float64(dur.Microseconds())/1000, 'f', 3, 64),
		strconv.Quote(out.Namespace), strconv.Quote(out.Name), strconv.Quote(out.Message))
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprint(h.out, line)
}
