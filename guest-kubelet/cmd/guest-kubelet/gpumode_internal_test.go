package main

import (
	"context"
	"testing"
)

func TestCheckGPUMode_Pooled(t *testing.T) {
	o := &options{logFormat: "json", gpuMode: "pooled", gpuDonorSelector: "timeslice.io/donor=true", holdersURL: "http://x", gpuClaim: "c"}
	if err := checkGPUMode(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if o.holdersURL != "" || o.gpuClaim != "" {
		t.Errorf("pooled mode reads no holders endpoint and no claim: %+v", o)
	}
	o = &options{logFormat: "json", gpuMode: "pooled"}
	if err := checkGPUMode(context.Background(), o); err == nil {
		t.Error("pooled needs a donor selector (the fence)")
	}
	o = &options{logFormat: "json", gpuMode: "pooled", gpuDonorSelector: "a=b", reserveClaim: true}
	if err := checkGPUMode(context.Background(), o); err == nil {
		t.Error("--reserve-claim needs claim mode")
	}
	o = &options{logFormat: "json", gpuMode: "deviceplugin", gpuDonorSelector: "a=b"}
	if err := checkGPUMode(context.Background(), o); err == nil {
		t.Error("deviceplugin still needs the holders URL")
	}
}
