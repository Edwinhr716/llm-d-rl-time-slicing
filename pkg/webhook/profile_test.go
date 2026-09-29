package webhook_test

import (
	"os"
	"strings"
	"testing"

	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/webhook"
)

// TestProfile_Today checks that deploy/timeslice-webhook/profiles/today.args, rendered as an
// installer would, parses and matches the TODAY stack profile of the evaluation.
func TestProfile_Today(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/timeslice-webhook/profiles/today.args")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.NewReplacer("__ORCH_ADDR__", orchAddr, "__VK_SA__", "vk-system:guest-kubelet").Replace(string(raw))
	var flags []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		flags = append(flags, line)
	}
	if strings.Contains(strings.Join(flags, " "), "__") {
		t.Fatalf("unrendered placeholder in %v", flags)
	}
	got, err := webhook.ConfigFromFlags(flags)
	if err != nil {
		t.Fatal(err)
	}
	want, err := webhook.ConfigFromFlags(todayFlags)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("today.args = %+v, want %+v", got, want)
	}
	if got.GuestSteering != webhook.SteeringRequired || got.DonorClientWiring != webhook.WiringEnv {
		t.Errorf("today.args steering %v wiring %v, want required and env", got.GuestSteering, got.DonorClientWiring)
	}
	render(t, "deployment.yaml", flags)
}
