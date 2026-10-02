package main

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// captureStdoutStderr runs fn with both file descriptors redirected into
// pipes and returns what was written. The commands under test use fmt.Println
// directly, not cobra's output writers.
func captureStdoutStderr(t *testing.T, fn func() error) (string, string, error) {
	t.Helper()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	prevOut, prevErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outW, errW
	runErr := fn()
	_ = outW.Close()
	_ = errW.Close()
	os.Stdout, os.Stderr = prevOut, prevErr
	out, _ := io.ReadAll(outR)
	errOut, _ := io.ReadAll(errR)
	return string(out), string(errOut), runErr
}

// End to end: a torn newest schema and one node's torn newest activity row are
// visible in `snapshot list` and `snapshot nodes`, with no payload bytes and
// no command failure -- the same recovery the MCP server serves on.
func TestSnapshotListAndNodesReportPayloadCorruption(t *testing.T) {
	dir := writeTOML(t, t.TempDir(), `
[project]
id = "demo"
`)
	withCWD(t, dir)

	store, err := history.Open(filepath.Join(dir, ".dryrun", "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := resolveSnapshotKey()
	t0 := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)

	old := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "appdb",
		Timestamp: t0, ContentHash: "h-old",
		Tables: []schema.Table{{Schema: "public", Name: "users"}},
	}
	newer := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "appdb",
		Timestamp: t0.Add(time.Hour), ContentHash: "h-new",
		Tables: []schema.Table{{Schema: "public", Name: "users"}},
	}
	if _, err := store.PutSchema(ctx, key, old); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSchema(ctx, key, newer); err != nil {
		t.Fatal(err)
	}

	aOld := &schema.ActivityStatsSnapshot{
		SchemaRefHash: "h-old", ContentHash: "a-old",
		Node: schema.NodeIdentity{Source: "primary", Timestamp: t0.Add(time.Minute)},
	}
	aNew := &schema.ActivityStatsSnapshot{
		SchemaRefHash: "h-new", ContentHash: "a-new",
		Node: schema.NodeIdentity{Source: "primary", Timestamp: t0.Add(2 * time.Hour)},
	}
	if _, err := store.PutActivity(ctx, key, aOld); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutActivity(ctx, key, aNew); err != nil {
		t.Fatal(err)
	}
	// torn newest rows, written through a second connection like an
	// interrupted capture would leave them
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(dir, ".dryrun", "history.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'h-new'`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx,
		`UPDATE activity_stats SET payload_json = 'oops' WHERE content_hash = 'a-new'`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// snapshot list: the torn row is marked and the warning names the fallback
	var historyDB string
	listOut, listErr, err := captureStdoutStderr(t, func() error {
		return snapshotListCmd(&historyDB).Execute()
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !strings.Contains(listOut, "PAYLOAD CORRUPT") {
		t.Errorf("list output hides the corrupt row:\n%s", listOut)
	}
	if !strings.Contains(listErr, "latest serves the nearest older valid record") {
		t.Errorf("list warning missing:\n%s", listErr)
	}
	// damaged bytes must never appear
	if strings.Contains(listOut, "broken") || strings.Contains(listErr, "broken") {
		t.Error("corrupt payload content leaked into output")
	}

	// snapshot nodes: newest primary capture fell back to the older one
	nodesOut, nodesErr, err := captureStdoutStderr(t, func() error {
		return snapshotNodesCmd(&historyDB).Execute()
	})
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	_ = nodesOut
	if !strings.Contains(nodesErr, "newest activity payload(s) unreadable; latest falls back") {
		t.Errorf("nodes fallback warning missing:\n%s", nodesErr)
	}
}
