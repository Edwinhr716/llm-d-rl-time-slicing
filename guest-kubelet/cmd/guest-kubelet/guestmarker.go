package main

import (
	"flag"

	"github.com/edwinhr716/guest-kubelet/internal/provider"
)

// --guest-marker (pending lead decision D-VK-4) is registered here, before main parses the flags,
// so that this option and the D-NS-2 option (--guest-node-label, in main.go) are in separate files.
// flag.Parse exits with status 2 on an unknown value, so a bad marker stops the VK at startup.
func init() {
	flag.Func("guest-marker",
		"what marks a pod on the virtual Node as a guest: toleration (the timeslice.io/guest toleration; "+
			"the default), label (the label timeslice.io/guest=true) or both (either one)",
		provider.SetGuestMarker)
}
