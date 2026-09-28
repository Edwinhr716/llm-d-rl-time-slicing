package cgroup

// SetSignal replaces the SIGKILL fallback, for tests.
func (c *FS) SetSignal(signal func(pid int) error) {
	c.signal = signal
}
