// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package publisher

import (
	"syscall"
	"time"
)

// processCPUTime is the user and system CPU time this process has used.
func processCPUTime() (time.Duration, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), true
}
