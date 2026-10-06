// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package publisher

import "time"

// processCPUTime reports nothing where the standard library has no portable
// reading of process CPU time; `cpu` is then omitted from the stats.
func processCPUTime() (time.Duration, bool) { return 0, false }
