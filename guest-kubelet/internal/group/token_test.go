package group

import "testing"

// The same vector is pinned in the orchestrator's GroupToken test; both must change together.
func TestTokenVector(t *testing.T) {
	const want = "gt-eb06e970a8b68e2de029dbb2ffc471b7"
	if got := Token("team-a.trainer.workers"); got != want {
		t.Fatalf("Token = %q, want %q", got, want)
	}
	if !IsToken(want) || IsToken("team-a.trainer.workers") {
		t.Fatal("IsToken")
	}
	if len(want) > 63 {
		t.Fatal("token is not a valid label value")
	}
}
