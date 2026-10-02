package main

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
)

// `snapshot list` must mark torn rows so an operator can see what latest
// skipped, without printing any of the damaged bytes.
func TestSnapshotListMarksPayloadCorruption(t *testing.T) {
	rows := []history.SnapshotSummary{
		{ContentHash: "good", Kind: history.SchemaKind()},
		{ContentHash: "bad", Kind: history.SchemaKind(), PayloadCorrupt: true},
	}
	if got := countCorruptRows(rows); got != 1 {
		t.Errorf("countCorruptRows = %d, want 1", got)
	}
	if marker := payloadCorruptMarker(rows[0]); marker != "" {
		t.Errorf("clean row marked: %q", marker)
	}
	marker := payloadCorruptMarker(rows[1])
	if !strings.Contains(marker, "PAYLOAD CORRUPT") {
		t.Errorf("marker = %q", marker)
	}
}

// `snapshot nodes` distinguishes a fallback (older valid capture served) from
// a fully unavailable node, and says nothing when the newest capture is fine.
func TestSnapshotNodesWarnsAboutPayloadFallback(t *testing.T) {
	served := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	nodes := []history.NodeSummary{
		{
			Label:              "alpha",
			PayloadCorruptRows: 2,
			PayloadFallback: []history.NodeStreamFallback{{
				Stream: "activity", Skipped: 2, ServedAt: &served,
			}},
		},
		{
			Label:              "beta",
			PayloadCorruptRows: 1,
			PayloadFallback: []history.NodeStreamFallback{{
				Stream: "query", Unavailable: true, Skipped: 1,
			}},
		},
		{
			Label:              "gamma",
			PayloadCorruptRows: 1,
		},
	}
	warnings := nodeWarnings(nodes)
	joined := strings.Join(warnings, "\n")

	if !strings.Contains(joined, "alpha: 2 newest activity payload(s) unreadable; latest falls back") ||
		!strings.Contains(joined, "2026-08-20 09:00:00") {
		t.Errorf("fallback warning missing/wrong:\n%s", joined)
	}
	if !strings.Contains(joined, "beta: query history is unusable: every stored payload is unreadable") {
		t.Errorf("unavailable warning missing:\n%s", joined)
	}
	if !strings.Contains(joined, "gamma: 1 unreadable payload row(s) in history; the newest capture is unaffected") {
		t.Errorf("clean-newest warning missing:\n%s", joined)
	}

	// a healthy node produces no payload warnings
	if ws := nodeWarnings([]history.NodeSummary{{Label: "delta", Role: history.NodeRolePrimary}}); len(ws) != 0 {
		t.Errorf("healthy node warned: %v", ws)
	}
}
