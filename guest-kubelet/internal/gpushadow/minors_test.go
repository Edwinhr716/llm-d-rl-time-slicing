package gpushadow

import (
	"testing"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

func TestFilterMinors(t *testing.T) {
	gpus := []api.GPU{{Minor: 0, Device: "nvidia0"}, {Minor: 1, Device: "nvidia1"}}
	if got, err := FilterMinors(gpus, ""); err != nil || len(got) != 2 {
		t.Fatalf("empty list: %v %v", got, err)
	}
	got, err := FilterMinors(gpus, "1")
	if err != nil || len(got) != 1 || got[0].Minor != 1 {
		t.Fatalf("list 1: %v %v", got, err)
	}
	if _, err := FilterMinors(gpus, "2"); err == nil {
		t.Fatal("minor 2 is not on the host; want error")
	}
	if _, err := FilterMinors(gpus, "x"); err == nil {
		t.Fatal("bad minor; want error")
	}
}
