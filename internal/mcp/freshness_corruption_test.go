package mcp

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/lint"
)

// historyStoreOnDisk opens a store and returns its path so the test can write
// torn rows through a second connection, simulating an interrupted capture.
func historyStoreOnDisk(t *testing.T) (*history.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.db")
	hist, err := history.Open(path)
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	t.Cleanup(func() { hist.Close() })
	return hist, path
}

func corruptExec(t *testing.T, path, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("corrupt write (%s): %v", query, err)
	}
}

func findHistorySpan(t *testing.T, spans []historySpan, stream string) historySpan {
	t.Helper()
	for _, sp := range spans {
		if sp.Stream == stream {
			return sp
		}
	}
	t.Fatalf("no %s span in %+v", stream, spans)
	return historySpan{}
}

// Cold start against a torn newest schema serves the nearest older valid
// bundle and flags the response degraded -- offline tools must not blank out.
func TestBootstrapFallsBackToOlderValidBundle(t *testing.T) {
	t0 := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	hist, path := historyStoreOnDisk(t)
	put(t, hist, datedSnap(t, t0, "old"))
	put(t, hist, datedSnap(t, t0.Add(time.Hour), "new"))
	corruptExec(t, path,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'new'`)

	srv := NewOfflineServerAnnotated(nil, lint.DefaultConfig())
	srv.SetHistory(hist)
	srv.SetSnapshotKey(testKey)
	if !srv.BootstrapFromHistory(context.Background()) {
		t.Fatal("bootstrap must serve the older valid bundle")
	}
	if got := servedTable(t, srv); got != "old" {
		t.Fatalf("served %q, want old", got)
	}
	if !srv.ServedDegraded() {
		t.Error("cold start on a torn newest row must mark degraded")
	}

	meta := srv.newMeta("", nil)
	if !meta.ServedDegraded {
		t.Error("typed _meta missing served_degraded")
	}
	sp := findHistorySpan(t, meta.History, "schema")
	if !sp.FellBack || sp.PayloadCorruptRows != 1 || sp.UnavailableReason != "" {
		t.Errorf("schema span = %+v", sp)
	}

	// thin clients only see the text header
	if suffix := srv.captureTimes().suffix(); !strings.Contains(suffix, "corrupt payload") {
		t.Errorf("header hides the fallback: %q", suffix)
	}

	// the map-based _meta path carries the same marker
	wrapper := map[string]any{}
	srv.injectMeta(wrapper, "", nil)
	mm, _ := wrapper["_meta"].(map[string]any)
	if mm["served_degraded"] != true {
		t.Errorf("map _meta missing served_degraded: %v", mm)
	}
}

// A running server keeps its current valid bundle when a bad generation lands,
// retries on the freshness cadence, and adopts the next valid capture,
// clearing the degraded state automatically.
func TestRunningServerKeepsBundleThenAutoRecovers(t *testing.T) {
	t0 := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	hist, path := historyStoreOnDisk(t)
	put(t, hist, datedSnap(t, t0, "old"))

	srv := serverWithHistory(t, datedSnap(t, t0, "old"), hist)
	if got := servedTable(t, srv); got != "old" {
		t.Fatalf("baseline served %q", got)
	}
	if srv.ServedDegraded() {
		t.Fatal("clean store must not start degraded")
	}

	// a torn newer capture lands
	put(t, hist, datedSnap(t, t0.Add(time.Hour), "mid"))
	corruptExec(t, path,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'mid'`)
	expireFreshness(srv)

	if got := servedTable(t, srv); got != "old" {
		t.Fatalf("bad generation replaced the served bundle with %q", got)
	}
	if !srv.ServedDegraded() {
		t.Error("server kept serving but did not mark degraded")
	}
	sp := findHistorySpan(t, srv.newMeta("", nil).History, "schema")
	if !sp.FellBack {
		t.Errorf("fallback not reported: %+v", sp)
	}

	// a later valid capture arrives on the next freshness tick
	put(t, hist, datedSnap(t, t0.Add(2*time.Hour), "latest"))
	expireFreshness(srv)
	if got := servedTable(t, srv); got != "latest" {
		t.Fatalf("valid capture not adopted: %q", got)
	}
	if srv.ServedDegraded() {
		t.Error("degraded state must clear once a valid capture is served")
	}
	sp = findHistorySpan(t, srv.newMeta("", nil).History, "schema")
	if sp.FellBack || sp.UnavailableReason != "" {
		t.Errorf("fallback marker did not clear: %+v", sp)
	}
	// the torn row is still counted store-wide; recovery is what cleared
	if sp.PayloadCorruptRows != 1 {
		t.Errorf("store-wide corruption count lost: %+v", sp)
	}
}

// No decodable schema anywhere: the uninitialized error names the corruption
// instead of pretending the database was never captured.
func TestNoValidSchemaReportsExplicitCorruption(t *testing.T) {
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	hist, path := historyStoreOnDisk(t)
	put(t, hist, datedSnap(t, t0, "only"))
	corruptExec(t, path,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'only'`)

	srv := NewOfflineServerAnnotated(nil, lint.DefaultConfig())
	srv.SetHistory(hist)
	srv.SetSnapshotKey(testKey)
	if srv.BootstrapFromHistory(context.Background()) {
		t.Fatal("bootstrap must fail when no valid predecessor exists")
	}
	note := srv.HistoryLoadError()
	if note == "" || !strings.Contains(note, "corrupt history payload") {
		t.Fatalf("load error = %q", note)
	}
	_, err := srv.getSchema()
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("tool error must name the corrupted range, got %v", err)
	}
}

// One node's newest bad row degrades only that node; other nodes keep serving.
func TestNodePayloadCorruptionDoesNotWipeOtherNodes(t *testing.T) {
	t0 := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	hist, path := historyStoreOnDisk(t)
	snap := datedSnap(t, t0, "s")
	put(t, hist, snap)
	putActivityAt(t, hist, snap.ContentHash, "alpha", t0.Add(time.Minute))
	putActivityAt(t, hist, snap.ContentHash, "beta", t0.Add(2*time.Minute))
	putActivityAt(t, hist, snap.ContentHash, "beta", t0.Add(3*time.Minute))
	// newest beta row torn; alpha untouched
	corruptExec(t, path,
		`UPDATE activity_stats SET payload_json = 'oops'
		  WHERE node_source = 'beta' AND timestamp = ?`,
		t0.Add(3*time.Minute).Format(time.RFC3339))

	srv := NewOfflineServerAnnotated(nil, lint.DefaultConfig())
	srv.SetHistory(hist)
	srv.SetSnapshotKey(testKey)
	if !srv.BootstrapFromHistory(context.Background()) {
		t.Fatal("bootstrap failed")
	}
	a, err := srv.getAnnotated()
	if err != nil {
		t.Fatal(err)
	}
	if a.Merged == nil || len(a.Merged.Nodes) != 2 {
		t.Fatalf("both nodes must serve, got %+v", a.Merged)
	}
	for _, n := range a.Merged.Nodes {
		if n.Node.Source == "beta" && !n.Node.Timestamp.Equal(t0.Add(2*time.Minute)) {
			t.Errorf("beta did not fall back: %s", n.Node.Timestamp)
		}
	}

	sp := findHistorySpan(t, srv.newMeta("", nil).History, "activity")
	if !srv.ServedDegraded() || len(sp.NodeFallbacks) != 1 {
		t.Fatalf("activity span = %+v", sp)
	}
	nf := sp.NodeFallbacks[0]
	if nf.Node != "beta" || nf.Skipped != 1 || nf.Reason != "" {
		t.Errorf("node fallback = %+v", nf)
	}
	if sp.PayloadCorruptRows != 1 {
		t.Errorf("payload_corrupt_rows = %d", sp.PayloadCorruptRows)
	}
}

// Explicit history tools keep strict addressing even when latest tolerates.
func TestStrictRefsStillErrorThroughStore(t *testing.T) {
	t0 := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	hist, path := historyStoreOnDisk(t)
	put(t, hist, datedSnap(t, t0, "old"))
	put(t, hist, datedSnap(t, t0.Add(time.Hour), "new"))
	corruptExec(t, path,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'new'`)

	// the named corrupt record errors instead of resolving to "old"
	if _, err := hist.GetSchema(context.Background(), testKey, history.NewRefHash("new")); err == nil {
		t.Error("explicit hash silently fell back")
	}
}
