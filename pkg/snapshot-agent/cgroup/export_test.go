package cgroup

import "syscall"

// SetSignal replaces the SIGKILL fallback, for tests.
func (m *Manager) SetSignal(signal func(pid int) error) {
	m.kill = func(pid int, _ syscall.Signal) error { return signal(pid) }
}
