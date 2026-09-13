// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"log/slog"
	"strings"
	"testing"
)

// TestLegacyFormsAreNamedAtBoot is the diagnostic the v0.27.0–v0.29.0 review
// asked for: nothing anywhere said which legacy topic forms were active.
// LegacyEntityTopics replaces the default rather than extending it, so a
// fleet spanning releases can state one form and silently stop retracting the
// other — and that failure looks entirely clean from this side.
func TestLegacyFormsAreNamedAtBoot(t *testing.T) {
	t.Parallel()

	var lines []string
	log := slog.New(countingHandler{fn: func(msg string) { lines = append(lines, msg) }})

	r := New(newFake(), Config{Logger: log})
	got := r.LegacyForms()
	if len(got) != 1 || !strings.Contains(got[0], "LegacyTopicWithNodeID") || !strings.Contains(got[0], "default") {
		t.Fatalf("LegacyForms() = %v, want the five-segment default named explicitly", got)
	}

	stated := New(newFake(), Config{
		Logger:             log,
		LegacyEntityTopics: []LegacyTopicFunc{LegacyTopicByUniqueID, nil, LegacyTopicByObjectID},
	})
	got = stated.LegacyForms()
	if len(got) != 2 ||
		!strings.Contains(got[0], "LegacyTopicByUniqueID") ||
		!strings.Contains(got[1], "LegacyTopicByObjectID") {
		t.Fatalf("LegacyForms() = %v, want both stated forms in order and no entry for the nil", got)
	}

	// A consumer's own closure still gets an entry, because the count is
	// half the diagnostic.
	own := New(newFake(), Config{
		Logger:             log,
		LegacyEntityTopics: []LegacyTopicFunc{func(LegacyEntity) string { return "" }},
	})
	if got := own.LegacyForms(); len(got) != 1 || got[0] == "" {
		t.Fatalf("LegacyForms() = %v, want one named entry for the consumer's own form", got)
	}

	if len(lines) != 3 {
		t.Fatalf("logged %v, want one line per runtime built", lines)
	}
	for _, l := range lines {
		if l != "publisher.legacy_forms" {
			t.Fatalf("logged %q, want publisher.legacy_forms", l)
		}
	}
}
