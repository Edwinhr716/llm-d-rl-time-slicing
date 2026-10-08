package main

import (
	"context"
	"testing"
)

func TestCheckGPUMode_Pooled(t *testing.T) {
	o := &options{logFormat: "json", gpuMode: "pooled", gpuDonorSelector: "timeslice.io/donor=true"}
	if err := checkGPUMode(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	o = &options{logFormat: "json", gpuMode: "pooled"}
	if err := checkGPUMode(context.Background(), o); err == nil {
		t.Error("pooled needs a donor selector (the fence)")
	}
	for _, m := range []string{"claim", "deviceplugin", ""} {
		o = &options{logFormat: "json", gpuMode: m, gpuDonorSelector: "a=b"}
		if err := checkGPUMode(context.Background(), o); err == nil {
			t.Errorf("--gpu-mode=%q must be refused: pooled is the only mode", m)
		}
	}
	o = &options{logFormat: "yaml", gpuMode: "pooled", gpuDonorSelector: "a=b"}
	if err := checkGPUMode(context.Background(), o); err == nil {
		t.Error("--log-format=yaml must be refused")
	}
}
