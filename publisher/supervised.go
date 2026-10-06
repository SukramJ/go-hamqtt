// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"os"
	"strings"
	"sync"
)

// supervisorProbes are what [DetectSupervised] reads from the process, kept
// apart so a test can answer them without an environment or a filesystem.
type supervisorProbes struct {
	getenv  func(string) string
	getppid func() int
	exists  func(string) bool
}

var osProbes = supervisorProbes{
	getenv:  os.Getenv,
	getppid: os.Getppid,
	exists: func(path string) bool {
		_, err := os.Stat(path)
		return err == nil
	},
}

// DetectSupervised returns an [InstanceConfig.Supervised] answer: whether a
// supervisor restarts this process after a clean exit, so that
// `maintenance/set/restart` is a restart rather than a stop. The answer is
// evaluated once, on the first call, and kept.
//
// It is openccu-loom's detectSupervisedRestart, shared so the bridges do not
// each write it again. The check is a cheap heuristic: it looks for markers
// that the process's immediate parent or runtime is a supervisor, not for
// the supervisor's restart policy. In order:
//
//   - envVar, the consumer's own explicit switch (e.g. `MTEC_SUPERVISED`):
//     `1` or `true` answers true, `0` or `false` answers false, in any case.
//     An explicit answer wins over everything below; any other value, or an
//     empty envVar, leaves the answer to detection.
//   - systemd: the parent is PID 1 and either `JOURNAL_STREAM` is set
//     (journald holds stdout/stderr) or `/run/systemd/system` exists.
//     `INVOCATION_ID` alone is not used, because a terminal emulator started
//     from a systemd user session inherits it.
//   - Kubernetes: `KUBERNETES_SERVICE_HOST` is set; the kubelet restarts a
//     dead container.
//   - A container: `/.dockerenv` exists.
//
// The container signal is a known false positive: a container started
// without a restart policy is detected as supervised, and a restart then
// stops it for good. That is the trade loom made, because presence of the
// marker is the usual case; setting envVar to `0` is the operator's way out.
func DetectSupervised(envVar string) func() bool {
	return sync.OnceValue(func() bool { return detectSupervised(envVar, osProbes) })
}

func detectSupervised(envVar string, p supervisorProbes) bool {
	if envVar != "" {
		switch strings.ToLower(strings.TrimSpace(p.getenv(envVar))) {
		case "1", "true":
			return true
		case "0", "false":
			return false
		}
	}
	if p.getppid() == 1 && (p.getenv("JOURNAL_STREAM") != "" || p.exists("/run/systemd/system")) {
		return true
	}
	if p.getenv("KUBERNETES_SERVICE_HOST") != "" {
		return true
	}
	return p.exists("/.dockerenv")
}
