// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import "testing"

func fakeProbes(env map[string]string, ppid int, files ...string) supervisorProbes {
	return supervisorProbes{
		getenv:  func(k string) string { return env[k] },
		getppid: func() int { return ppid },
		exists: func(path string) bool {
			for _, f := range files {
				if f == path {
					return true
				}
			}
			return false
		},
	}
}

// TestDetectSupervised keeps loom's detectSupervisedRestart semantics, plus
// the explicit false that lets an operator overrule a false positive.
func TestDetectSupervised(t *testing.T) {
	t.Parallel()

	const v = "MTEC_SUPERVISED"
	cases := []struct {
		name   string
		envVar string
		probes supervisorProbes
		want   bool
	}{
		{"bare shell", v, fakeProbes(nil, 4711), false},
		{"explicit 1", v, fakeProbes(map[string]string{v: "1"}, 4711), true},
		{"explicit TRUE", v, fakeProbes(map[string]string{v: " TRUE "}, 4711), true},
		{"explicit 0 beats docker", v, fakeProbes(map[string]string{v: "0"}, 1, "/.dockerenv"), false},
		{"explicit false beats kubernetes", v, fakeProbes(map[string]string{v: "false", "KUBERNETES_SERVICE_HOST": "10.0.0.1"}, 1), false},
		{"unparseable falls through", v, fakeProbes(map[string]string{v: "maybe"}, 1, "/.dockerenv"), true},
		{"no variable named", "", fakeProbes(map[string]string{"": "0"}, 1, "/.dockerenv"), true},
		{"systemd journal", v, fakeProbes(map[string]string{"JOURNAL_STREAM": "8:1"}, 1), true},
		{"systemd runtime dir", v, fakeProbes(nil, 1, "/run/systemd/system"), true},
		{"journal without pid 1 parent", v, fakeProbes(map[string]string{"JOURNAL_STREAM": "8:1"}, 4711), false},
		{"systemd dir without pid 1 parent", v, fakeProbes(nil, 4711, "/run/systemd/system"), false},
		{"invocation id alone", v, fakeProbes(map[string]string{"INVOCATION_ID": "abc"}, 1), false},
		{"kubernetes", v, fakeProbes(map[string]string{"KUBERNETES_SERVICE_HOST": "10.0.0.1"}, 4711), true},
		{"docker", v, fakeProbes(nil, 4711, "/.dockerenv"), true},
	}
	for _, c := range cases {
		if got := detectSupervised(c.envVar, c.probes); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// TestDetectSupervisedIsAnInstanceAnswer: the exported form plugs into
// InstanceConfig.Supervised and answers the same every time.
func TestDetectSupervisedIsAnInstanceAnswer(t *testing.T) {
	t.Parallel()

	answer := DetectSupervised("GO_HAMQTT_TEST_UNSET_SUPERVISED")
	cfg := InstanceConfig{Supervised: answer}
	if cfg.Supervised() != answer() {
		t.Error("the answer changed between calls")
	}
}
