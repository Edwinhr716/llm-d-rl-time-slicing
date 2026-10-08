package main

import (
	"testing"

	"k8s.io/client-go/rest"
)

// The VK's API client gets the flag's rate limit; zero keeps client-go's default.
func TestWithRateLimit(t *testing.T) {
	cfg := &rest.Config{}
	withRateLimit(cfg, 50, 100)
	if cfg.QPS != 50 || cfg.Burst != 100 {
		t.Fatalf("QPS, Burst = %v, %d; want 50, 100", cfg.QPS, cfg.Burst)
	}
	keep := &rest.Config{QPS: 5, Burst: 10}
	withRateLimit(keep, 0, 0)
	if keep.QPS != 5 || keep.Burst != 10 {
		t.Fatalf("QPS, Burst = %v, %d; want 5, 10 (unchanged)", keep.QPS, keep.Burst)
	}
}
