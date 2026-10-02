package history

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func streamRecovery(rec LatestRecovery, stream string) (StreamFallback, bool) {
	for _, f := range rec.Streams {
		if f.Stream == stream {
			return f, true
		}
	}
	return StreamFallback{}, false
}

func nodeRecovery(fb StreamFallback, node string) (NodeFallback, bool) {
	for _, n := range fb.Nodes {
		if n.Node == node {
			return n, true
		}
	}
	return NodeFallback{}, false
}

func putActivityRef(t *testing.T, s *Store, k SnapshotKey, schemaRef, hash, label string, ts time.Time) {
	t.Helper()
	a := activityFixture(schemaRef, hash, label, false)
	a.Node.Timestamp = ts
	if _, err := s.PutActivity(context.Background(), k, a); err != nil {
		t.Fatalf("PutActivity %s: %v", hash, err)
	}
}

func putQueryRef(t *testing.T, s *Store, k SnapshotKey, schemaRef, hash, label string, ts time.Time) {
	t.Helper()
	q := queryStatsFixture(schemaRef, hash, label)
	q.Node.Timestamp = ts
	if _, err := s.PutQueryStats(context.Background(), k, q); err != nil {
		t.Fatalf("PutQueryStats %s: %v", hash, err)
	}
}

// A torn write to the newest schema row must not blank offline tools: latest
// serves the nearest older decodable schema and reports the skip.
func TestLatest_SchemaFallbackToOlderValid(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h-old", base)
	putSchemaAt(t, s, k, "h-new", base.Add(time.Hour))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'h-new'`); err != nil {
		t.Fatal(err)
	}

	a, rec, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatalf("GetAnnotatedLatest: %v", err)
	}
	if a.Schema.ContentHash != "h-old" {
		t.Fatalf("want h-old served, got %q", a.Schema.ContentHash)
	}
	fb, ok := streamRecovery(rec, "schema")
	if !ok || fb.Skipped != 1 {
		t.Fatalf("schema recovery = %+v ok=%v", fb, ok)
	}
	if rec.ServedSchemaHash != "h-old" {
		t.Fatalf("ServedSchemaHash = %q", rec.ServedSchemaHash)
	}

	// the tolerant wrapper used by every latest caller serves the same bundle
	got, err := s.GetAnnotated(ctx, k, NewRefLatest())
	if err != nil || got.Schema.ContentHash != "h-old" {
		t.Fatalf("GetAnnotated(latest) = %+v, %v", got, err)
	}
}

// The strict addressing paths name one record and must fail against it even
// though latest would skip the same bytes.
func TestLatest_ExplicitRefsFailOnTheCorruptRecord(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h-old", base)
	putSchemaAt(t, s, k, "h-new", base.Add(time.Hour))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'h-new'`); err != nil {
		t.Fatal(err)
	}

	for name, ref := range map[string]SnapshotRef{
		"hash":   NewRefHash("h-new"),
		"index0": NewRefIndex(0),
		"attime": NewRefAt(base.Add(2 * time.Hour)),
	} {
		if _, err := s.GetSchema(ctx, k, ref); err == nil {
			t.Errorf("%s: corrupt record served instead of erroring", name)
		} else if !errors.Is(err, ErrSnapshotNotFound) && !strings.Contains(err.Error(), "corrupt") {
			t.Errorf("%s: error should name the corrupt record: %v", name, err)
		}
	}

	// GetAnnotated with an explicit point in time stays strict too
	if _, err := s.GetAnnotated(ctx, k, NewRefAt(base.Add(2*time.Hour))); err == nil {
		t.Error("GetAnnotated(at) silently fell back")
	}
	// the older record is still explicitly addressable
	if snap, err := s.GetSchema(ctx, k, NewRefHash("h-old")); err != nil || snap.ContentHash != "h-old" {
		t.Fatalf("h-old not addressable: snap=%+v err=%v", snap, err)
	}
}

// NewRefAt constructor for the strict-ref test above.
func NewRefAt(at time.Time) SnapshotRef { return SnapshotRef{Kind: RefAt, At: at} }

// When the schema falls back a generation, stats bound to the corrupt newer
// schema must never be spliced in: every stream is scoped to the served hash.
func TestLatest_StatsStayInsideTheServedSchemaGeneration(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)

	putSchemaAt(t, s, k, "h-old", base)
	putPlannerAt(t, s, k, "h-old", "p-old", base.Add(time.Minute))
	putActivityRef(t, s, k, "h-old", "a-old", "primary", base.Add(time.Minute))

	putSchemaAt(t, s, k, "h-new", base.Add(time.Hour))
	// valid stats for the corrupt generation -- they must stay out
	putPlannerAt(t, s, k, "h-new", "p-new", base.Add(time.Hour+time.Minute))
	putActivityRef(t, s, k, "h-new", "a-new", "primary", base.Add(time.Hour+time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = '{"broken' WHERE content_hash = 'h-new'`); err != nil {
		t.Fatal(err)
	}

	a, _, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if a.Planner == nil || a.Planner.ContentHash != "p-old" {
		t.Fatalf("planner leaked across generations: %+v", a.Planner)
	}
	if a.Merged == nil || len(a.Merged.Nodes) != 1 {
		t.Fatalf("activity merge = %+v", a.Merged)
	}
	for _, n := range a.Merged.Nodes {
		if !n.Node.Timestamp.Equal(base.Add(time.Minute)) {
			t.Errorf("new-generation activity spliced in: %s", n.Node.Timestamp)
		}
	}
}

// Planner picks the newest VALID row inside the served hash's range.
func TestLatest_PlannerFallsBackInsideRange(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h1", base)
	putPlannerAt(t, s, k, "h1", "p-old", base.Add(time.Minute))
	putPlannerAt(t, s, k, "h1", "p-new", base.Add(2*time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE planner_stats SET payload_json = 'oops' WHERE content_hash = 'p-new'`); err != nil {
		t.Fatal(err)
	}

	a, rec, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if a.Planner == nil || a.Planner.ContentHash != "p-old" {
		t.Fatalf("want p-old, got %+v", a.Planner)
	}
	fb, ok := streamRecovery(rec, "planner")
	if !ok || fb.Skipped != 1 {
		t.Fatalf("planner recovery = %+v", fb)
	}

	// every planner row unusable: a named unavailable range, not "uncaptured"
	if _, err := s.db.ExecContext(ctx,
		`UPDATE planner_stats SET payload_json = 'oops'`); err != nil {
		t.Fatal(err)
	}
	a2, rec2, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if a2.Planner != nil {
		t.Fatalf("no valid planner row must serve: %+v", a2.Planner)
	}
	fb2, _ := streamRecovery(rec2, "planner")
	if fb2.Reason == "" {
		t.Fatalf("want an unavailable reason, got %+v", fb2)
	}
}

// One node's bad newest row falls back to its own older row; other nodes keep
// their newest capture.
func TestLatest_ActivityFallbackIsIndependentPerNode(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h1", base)
	putActivityRef(t, s, k, "h1", "a-good", "alpha", base.Add(time.Minute))
	putActivityRef(t, s, k, "h1", "b-old", "beta", base.Add(2*time.Minute))
	putActivityRef(t, s, k, "h1", "b-new", "beta", base.Add(3*time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE activity_stats SET payload_json = 'oops' WHERE content_hash = 'b-new'`); err != nil {
		t.Fatal(err)
	}

	a, rec, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if a.Merged == nil || len(a.Merged.Nodes) != 2 {
		t.Fatalf("want both nodes, got %+v", a.Merged)
	}
	byNode := map[string]time.Time{}
	for _, n := range a.Merged.Nodes {
		byNode[n.Node.Source] = n.Node.Timestamp
	}
	if !byNode["alpha"].Equal(base.Add(time.Minute)) {
		t.Errorf("alpha moved: %s", byNode["alpha"])
	}
	if !byNode["beta"].Equal(base.Add(2 * time.Minute)) {
		t.Errorf("beta should fall back to b-old, got %s", byNode["beta"])
	}
	fb, ok := streamRecovery(rec, "activity")
	if !ok {
		t.Fatal("no activity recovery reported")
	}
	nf, ok := nodeRecovery(fb, "beta")
	if !ok || nf.Skipped != 1 || nf.Reason != "" {
		t.Fatalf("beta recovery = %+v", nf)
	}
	if _, reported := nodeRecovery(fb, "alpha"); reported {
		t.Error("alpha was clean and must not be reported")
	}
}

// A node whose every row in the required range is unusable is named
// unavailable without taking the other nodes down with it.
func TestLatest_NodeFullyCorruptStaysUnavailable(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h1", base)
	putActivityRef(t, s, k, "h1", "a-good", "alpha", base.Add(time.Minute))
	putActivityRef(t, s, k, "h1", "b-1", "beta", base.Add(2*time.Minute))
	putActivityRef(t, s, k, "h1", "b-2", "beta", base.Add(3*time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE activity_stats SET payload_json = 'oops' WHERE node_source = 'beta'`); err != nil {
		t.Fatal(err)
	}

	a, rec, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if a.Merged == nil || len(a.Merged.Nodes) != 1 || a.Merged.Nodes[0].Node.Source != "alpha" {
		t.Fatalf("only alpha should serve, got %+v", a.Merged)
	}
	fb, _ := streamRecovery(rec, "activity")
	nf, ok := nodeRecovery(fb, "beta")
	if !ok || nf.Reason == "" || nf.Skipped != 2 {
		t.Fatalf("beta unavailable reason = %+v", nf)
	}
}

// Query stats follow the same per-node rule.
func TestLatest_QueryFallbackIsIndependentPerNode(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h1", base)
	putQueryRef(t, s, k, "h1", "q-good", "alpha", base.Add(time.Minute))
	putQueryRef(t, s, k, "h1", "q-old", "beta", base.Add(2*time.Minute))
	putQueryRef(t, s, k, "h1", "q-new", "beta", base.Add(3*time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE query_stats SET payload_json = 'oops' WHERE content_hash = 'q-new'`); err != nil {
		t.Fatal(err)
	}

	a, rec, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.QueryStats) != 2 {
		t.Fatalf("want both query nodes, got %d", len(a.QueryStats))
	}
	fb, ok := streamRecovery(rec, "query")
	if !ok {
		t.Fatal("no query recovery")
	}
	nf, ok := nodeRecovery(fb, "beta")
	if !ok || nf.Skipped != 1 {
		t.Fatalf("beta query recovery = %+v", nf)
	}
}

// Every schema row unusable: an explicit corruption error naming the range,
// not ErrSnapshotNotFound's "nothing captured".
func TestLatest_AllSchemaCorruptReturnsExplicitReason(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

	putSchemaAt(t, s, k, "h1", base)
	if _, err := s.db.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = 'oops'`); err != nil {
		t.Fatal(err)
	}

	_, rec, err := s.GetAnnotatedLatest(ctx, k)
	if !errors.Is(err, ErrHistoryCorrupt) {
		t.Fatalf("want ErrHistoryCorrupt, got %v", err)
	}
	fb, ok := streamRecovery(rec, "schema")
	if !ok || fb.Skipped != 1 || fb.Reason == "" {
		t.Fatalf("recovery = %+v", rec.Streams)
	}

	// nothing stored at all stays the ordinary not-found
	empty := testStore(t)
	if _, _, err := empty.GetAnnotatedLatest(context.Background(), k); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("empty store: want ErrSnapshotNotFound, got %v", err)
	}
}

// Valid JSON whose embedded identity disagrees with the row columns is a
// different record's bytes and must be skipped just like malformed JSON.
func TestLatest_IdentityMismatchIsCorruption(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h-old", base)
	putSchemaAt(t, s, k, "h-new", base.Add(time.Hour))
	// valid JSON, but the digest inside belongs to another row
	good, err := json.Marshal(testSnapshot("h-old", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = ? WHERE content_hash = 'h-new'`, string(good)); err != nil {
		t.Fatal(err)
	}

	a, rec, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if a.Schema.ContentHash != "h-old" {
		t.Fatalf("identity-mismatched row served: %q", a.Schema.ContentHash)
	}
	if fb, _ := streamRecovery(rec, "schema"); fb.Skipped != 1 {
		t.Fatalf("want 1 skipped, got %+v", fb)
	}
}

// Pulled rows (captured_locally = 0) are still valid predecessors: a corrupt
// local newest row falls back onto a pulled older one.
func TestLatest_FallbackCanUsePulledRows(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h1", base)
	// simulate a pulled planner row
	p := plannerFixture("h1", "p-pulled", "acme")
	p.Timestamp = base.Add(time.Minute)
	if _, err := s.Put(ctx, k, WrapPlanner(p)); err != nil {
		t.Fatal(err)
	}
	putPlannerAt(t, s, k, "h1", "p-local", base.Add(2*time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE planner_stats SET payload_json = 'oops' WHERE content_hash = 'p-local'`); err != nil {
		t.Fatal(err)
	}

	a, _, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if a.Planner == nil || a.Planner.ContentHash != "p-pulled" {
		t.Fatalf("pulled predecessor not used: %+v", a.Planner)
	}
}

// Inventory and List surface payload corruption as counts/markers.
func TestLatest_PayloadCorruptionSurfacedInInventoryAndList(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h-old", base)
	putSchemaAt(t, s, k, "h-new", base.Add(time.Hour))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = 'oops' WHERE content_hash = 'h-new'`); err != nil {
		t.Fatal(err)
	}
	putActivityRef(t, s, k, "h-old", "a1", "primary", base)
	if _, err := s.db.ExecContext(ctx,
		`UPDATE activity_stats SET payload_json = 'oops' WHERE content_hash = 'a1'`); err != nil {
		t.Fatal(err)
	}

	spans, err := s.Inventory(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"schema": 1, "activity": 1}
	for _, sp := range spans {
		if got := sp.PayloadCorruptRows; got != want[sp.Stream] {
			t.Errorf("stream %s payload_corrupt_rows = %d, want %d", sp.Stream, got, want[sp.Stream])
		}
	}

	list, err := s.List(ctx, k, SchemaKind(), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	var marked int
	for _, row := range list {
		if row.PayloadCorrupt {
			marked++
			if row.ContentHash != "h-new" {
				t.Errorf("wrong row marked: %s", row.ContentHash)
			}
		}
	}
	if marked != 1 {
		t.Fatalf("want 1 marked row, got %d", marked)
	}

	nodes, err := s.ListNodes(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Label != "primary" || nodes[0].PayloadCorruptRows != 1 {
		t.Fatalf("node summary = %+v", nodes)
	}
	if len(nodes[0].PayloadFallback) != 1 || !nodes[0].PayloadFallback[0].Unavailable {
		t.Fatalf("node fallback = %+v", nodes[0].PayloadFallback)
	}
}

// Single-kind latest reads (snapshot show/diff --kind, top-queries helpers)
// recover newest-to-oldest as well; explicit refs still do not.
func TestLatest_SingleKindLatestRecoversButRefsStayStrict(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putPlannerAt(t, s, k, "h1", "p-old", base)
	putPlannerAt(t, s, k, "h1", "p-new", base.Add(time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE planner_stats SET payload_json = 'oops' WHERE content_hash = 'p-new'`); err != nil {
		t.Fatal(err)
	}
	putActivityRef(t, s, k, "h1", "a-old", "primary", base.Add(2*time.Minute))
	putActivityRef(t, s, k, "h1", "a-new", "primary", base.Add(3*time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE activity_stats SET payload_json = 'oops' WHERE content_hash = 'a-new'`); err != nil {
		t.Fatal(err)
	}

	if got, err := s.Get(ctx, k, PlannerKind(), NewRefLatest()); err != nil || got.AsPlanner().ContentHash != "p-old" {
		t.Fatalf("planner latest = %+v, %v", got, err)
	}
	if got, err := s.Get(ctx, k, ActivityKind("primary"), NewRefLatest()); err != nil || got.AsActivity().ContentHash != "a-old" {
		t.Fatalf("activity latest = %+v, %v", got, err)
	}

	// hash and index addressing the corrupt rows stay loud
	if _, err := s.Get(ctx, k, PlannerKind(), NewRefHash("p-new")); err == nil {
		t.Error("planner hash fell back silently")
	}
	if _, err := s.Get(ctx, k, ActivityKind("primary"), NewRefIndex(0)); err == nil {
		t.Error("activity latest~0 fell back silently")
	}
}

// LatestQueryStats/LatestActivity (unscoped) skip one node's torn newest row
// per node, so list_top_queries-style reads survive a single bad capture.
func TestLatest_UnscopedNodeReadsToleratePerNodeCorruption(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putQueryRef(t, s, k, "h1", "q-good", "alpha", base)
	putQueryRef(t, s, k, "h1", "q-old", "beta", base.Add(time.Minute))
	putQueryRef(t, s, k, "h1", "q-new", "beta", base.Add(2*time.Minute))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE query_stats SET payload_json = 'oops' WHERE content_hash = 'q-new'`); err != nil {
		t.Fatal(err)
	}
	putActivityRef(t, s, k, "h1", "a-good", "alpha", base)

	qs, err := s.LatestQueryStats(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	byNode := map[string]string{}
	for i := range qs {
		byNode[qs[i].Node.Source] = qs[i].ContentHash
	}
	if byNode["alpha"] != "q-good" || byNode["beta"] != "q-old" {
		t.Fatalf("per-node fallback wrong: %+v", byNode)
	}

	acts, err := s.LatestActivity(ctx, k)
	if err != nil || len(acts) != 1 || acts[0].ContentHash != "a-good" {
		t.Fatalf("LatestActivity = %+v, %v", acts, err)
	}
}

// normal latest semantics are unchanged when everything decodes: no recovery
// report at all.
func TestLatest_CleanStoreHasNoRecovery(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

	putSchemaAt(t, s, k, "h1", base)
	putPlannerAt(t, s, k, "h1", "p1", base.Add(time.Minute))
	putActivityRef(t, s, k, "h1", "a1", "primary", base.Add(time.Minute))

	_, rec, err := s.GetAnnotatedLatest(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Degraded() || len(rec.Streams) != 0 {
		t.Fatalf("clean store reported recovery: %+v", rec)
	}
	if n := rec.SkippedPayloads(); n != 0 {
		t.Fatalf("skipped = %d", n)
	}
}

// latest~N bounds-check semantics are unchanged and the resolved offset still
// errors against its specific row.
func TestLatest_IndexNStillPositionalAndStrict(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	putSchemaAt(t, s, k, "h-old", base)
	putSchemaAt(t, s, k, "h-new", base.Add(time.Hour))
	if _, err := s.db.ExecContext(ctx,
		`UPDATE snapshots SET snapshot_json = 'oops' WHERE content_hash = 'h-new'`); err != nil {
		t.Fatal(err)
	}

	kind, ref, err := s.ResolveToken(ctx, k, "latest~1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := s.Get(ctx, k, kind, ref); err != nil || snap.AsSchema().ContentHash != "h-old" {
		t.Fatalf("latest~1 = %+v, %v", snap, err)
	}
	if _, _, err := s.ResolveToken(ctx, k, "latest~5", "", ""); err == nil {
		t.Error("out-of-range latest~N must still error")
	}
	kind0, ref0, err := s.ResolveToken(ctx, k, "latest~0", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, k, kind0, ref0); err == nil {
		t.Error("latest~0 must report its corrupt record, not fall back")
	}
}
