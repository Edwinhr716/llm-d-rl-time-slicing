package webhook_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

func serve(t *testing.T, handler http.Handler, method string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, "/mutate", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func reviewBody(t *testing.T, labels map[string]string, uid string) []byte {
	t.Helper()
	raw, err := json.Marshal(newPod(labels))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request: &admissionv1.AdmissionRequest{
			UID:       apitypes.UID("uid-" + uid),
			Resource:  podsGVR("pods"),
			Namespace: testNS,
			Operation: admissionv1.Create,
			UserInfo:  authv1.UserInfo{Username: testUser},
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestHandler_ReviewLogAndMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	var logs bytes.Buffer
	handler := webhook.NewHandler(*mustConfig(t, nsFlags), webhook.NewMetrics(reg), &logs)

	rec := serve(t, handler, http.MethodPost, reviewBody(t, map[string]string{webhook.LabelGuest: "true"}, "g"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(rec.Body.Bytes(), &review); err != nil {
		t.Fatal(err)
	}
	if review.APIVersion != "admission.k8s.io/v1" || review.Kind != "AdmissionReview" {
		t.Errorf("envelope = %+v", review.TypeMeta)
	}
	if review.Response == nil || review.Response.UID != "uid-g" || !review.Response.Allowed || len(review.Response.Patch) == 0 {
		t.Fatalf("response = %+v", review.Response)
	}
	line := logs.String()
	for _, want := range []string{`msg="admit"`, "kind=guest", "op=CREATE", "result=patched", "rule=W1", "patch_ops=", "dur_ms="} {
		if !strings.Contains(line, want) {
			t.Errorf("log line %q lacks %q", line, want)
		}
	}

	serve(t, handler, http.MethodPost, reviewBody(t, map[string]string{webhook.LabelDonor: "true"}, "d"))
	if !strings.Contains(logs.String(), "result=denied rule=R4") {
		t.Errorf("denial not logged: %q", logs.String())
	}
	if strings.Count(logs.String(), "\n") != 2 {
		t.Errorf("want one line per request, got %q", logs.String())
	}
	metrics := `
# HELP timeslice_webhook_admissions_total Admission requests handled, by pod kind, operation, result and rule.
# TYPE timeslice_webhook_admissions_total counter
timeslice_webhook_admissions_total{kind="donor",op="CREATE",result="denied",rule="R4"} 1
timeslice_webhook_admissions_total{kind="guest",op="CREATE",result="patched",rule="W1"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(metrics), "timeslice_webhook_admissions_total"); err != nil {
		t.Error(err)
	}
	if n := testutil.CollectAndCount(reg, "timeslice_webhook_admission_duration_seconds"); n != 2 {
		t.Errorf("duration series = %d, want 2", n)
	}
}

func TestHandler_BadRequests(t *testing.T) {
	handler := webhook.NewHandler(*mustConfig(t, nil), nil, &bytes.Buffer{})
	if rec := serve(t, handler, http.MethodGet, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d", rec.Code)
	}
	if rec := serve(t, handler, http.MethodPost, []byte("{")); rec.Code != http.StatusBadRequest {
		t.Errorf("bad JSON: %d", rec.Code)
	}
	if rec := serve(t, handler, http.MethodPost, []byte(`{"apiVersion":"admission.k8s.io/v1"}`)); rec.Code != http.StatusBadRequest {
		t.Errorf("no request: %d", rec.Code)
	}
}
