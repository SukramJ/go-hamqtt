// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"bytes"
	"math"
	"os"
	"runtime/metrics"
	"strconv"
	"time"
)

// processStats is the `<name>/maintenance/stats` document (spec §7). `cpu`
// is omitted where the platform cannot answer it rather than reported as
// zero, which a dashboard would plot as a real reading.
//
// `rss` is never omitted: she discards the whole document when it is not a
// number (parseStats in its services inventory), so a missing `rss` would
// cost every other field too. Where the kernel's resident set size cannot be
// read without cgo — anywhere but Linux — it is approximated, see
// [residentApprox].
type processStats struct {
	RSS       uint64   `json:"rss"`
	HeapUsed  uint64   `json:"heapUsed"`
	HeapTotal uint64   `json:"heapTotal"`
	CPU       *float64 `json:"cpu,omitempty"`
	Uptime    int64    `json:"uptime"`
	TS        int64    `json:"ts"`
}

// heapMetrics are read in one call: the live heap, and the parts of the
// heap's mapped memory that hold no live object yet are still the process's.
var heapMetrics = []string{
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/heap/unused:bytes",
	"/memory/classes/heap/free:bytes",
	"/memory/classes/total:bytes",
	"/memory/classes/heap/released:bytes",
}

// statsSampler remembers the previous CPU reading, so `cpu` is the share of
// one core over the interval rather than since start.
type statsSampler struct {
	started time.Time
	statm   string
	samples []metrics.Sample

	lastWall time.Time
	lastCPU  time.Duration
	haveCPU  bool
}

func newStatsSampler(started time.Time, statm string) *statsSampler {
	s := &statsSampler{started: started, statm: statm, lastWall: started}
	s.samples = make([]metrics.Sample, len(heapMetrics))
	for i, name := range heapMetrics {
		s.samples[i].Name = name
	}
	// The first interval runs from the process start, where CPU time was
	// zero by definition.
	_, s.haveCPU = processCPUTime()
	return s
}

func (s *statsSampler) sample(now time.Time) processStats {
	metrics.Read(s.samples)
	value := func(i int) uint64 {
		if s.samples[i].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return s.samples[i].Value.Uint64()
	}
	out := processStats{
		HeapUsed:  value(0),
		HeapTotal: value(0) + value(1) + value(2),
		Uptime:    int64(now.Sub(s.started) / time.Second),
		TS:        now.UnixMilli(),
	}
	if rss, ok := residentBytes(s.statm); ok {
		out.RSS = rss
	} else {
		out.RSS = residentApprox(value(3), value(4))
	}
	if used, ok := processCPUTime(); ok && s.haveCPU {
		if wall := now.Sub(s.lastWall); wall > 0 {
			pct := math.Round(float64(used-s.lastCPU)/float64(wall)*1000) / 10
			out.CPU = &pct
		}
		s.lastCPU, s.lastWall = used, now
	}
	return out
}

// residentApprox stands in for the resident set size where no statm file
// exists: all memory the Go runtime has mapped, less the heap memory it has
// released back to the operating system. It is the runtime's own
// mapped-and-not-released figure, not the kernel's RSS — it counts mapped
// pages that were never touched and misses memory outside the Go runtime —
// but it tracks the same growth, which is what a dashboard plots.
func residentApprox(total, released uint64) uint64 {
	if released > total {
		return 0
	}
	return total - released
}

// residentBytes reads the resident set size from a Linux statm file: the
// second field, in pages. Anywhere the file does not exist it reports false,
// and [residentApprox] answers instead.
func residentBytes(path string) (uint64, bool) {
	raw, err := os.ReadFile(path) //nolint:gosec // the path is this package's own constant, overridden only by tests
	if err != nil {
		return 0, false
	}
	fields := bytes.Fields(raw)
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0, false
	}
	pageSize := os.Getpagesize()
	if pageSize <= 0 {
		return 0, false
	}
	return pages * uint64(pageSize), true
}
