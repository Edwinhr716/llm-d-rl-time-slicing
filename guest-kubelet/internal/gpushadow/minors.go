package gpushadow

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/edwinhr716/guest-kubelet/internal/gpushadow/api"
)

// FilterMinors keeps the GPUs whose device minor is in list (comma-separated,
// --shadow-minors). An empty list keeps every GPU. A minor that is not on the host is an error,
// so a typo never silently lends nothing or the wrong GPU.
func FilterMinors(gpus []api.GPU, list string) ([]api.GPU, error) {
	list = strings.TrimSpace(list)
	if list == "" {
		return gpus, nil
	}
	want := map[int]bool{}
	for _, f := range strings.Split(list, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 0 {
			return nil, fmt.Errorf("--shadow-minors: bad minor %q", f)
		}
		want[n] = true
	}
	out := make([]api.GPU, 0, len(want))
	for i := range gpus {
		if want[gpus[i].Minor] {
			out = append(out, gpus[i])
			delete(want, gpus[i].Minor)
		}
	}
	if len(want) > 0 {
		return nil, fmt.Errorf("--shadow-minors: minors %v are not on this host", want)
	}
	return out, nil
}
