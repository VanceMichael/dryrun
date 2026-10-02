package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/boringsql/dryrun/internal/schema"
)

// ErrHistoryCorrupt marks a latest resolution that found stored rows but no
// decodable, internally consistent predecessor in a required range. Explicit
// refs (content hash, latest~N, point in time) never wrap this: they fail
// against the one record they named.
var ErrHistoryCorrupt = errors.New("corrupt history payload")

type (
	// NodeFallback is the content-free report for one node of a node-scoped
	// stream during tolerant latest assembly: how many unusable newest rows
	// were skipped, and why the node is unavailable when no valid row remained.
	// It never carries payload bytes.
	NodeFallback struct {
		Node    string `json:"node"`
		Skipped int    `json:"skipped_payloads,omitempty"`
		// set when the node had rows in the required range but every one was
		// unusable, so the bundle serves nothing for this node rather than
		// pretending the stream was never captured
		Reason string `json:"unavailable_reason,omitempty"`
	}

	// StreamFallback reports what latest resolution skipped for one stream.
	// Planner/schema use Skipped/Reason; node-scoped streams detail each
	// affected node in Nodes.
	StreamFallback struct {
		Stream  string         `json:"stream"`
		Skipped int            `json:"skipped_payloads,omitempty"`
		Reason  string         `json:"unavailable_reason,omitempty"`
		Nodes   []NodeFallback `json:"nodes,omitempty"`
	}

	// LatestRecovery is the audit trail of one tolerant latest assembly.
	// ServedSchemaHash names the generation the bundle was actually bound to;
	// Streams lists only streams that did not serve their newest row cleanly.
	LatestRecovery struct {
		ServedSchemaHash string
		Streams          []StreamFallback
	}
)

func (f StreamFallback) degraded() bool {
	return f.Skipped > 0 || f.Reason != "" || len(f.Nodes) > 0
}

// Degraded reports whether the assembly skipped anything: schema fallback,
// planner fallback, per-node activity/query fallback, or an unavailable range.
func (r LatestRecovery) Degraded() bool {
	for i := range r.Streams {
		if r.Streams[i].degraded() {
			return true
		}
	}
	return false
}

// SkippedPayloads totals skipped payload rows across every stream and node.
func (r LatestRecovery) SkippedPayloads() int {
	n := 0
	for i := range r.Streams {
		n += r.Streams[i].Skipped
		for j := range r.Streams[i].Nodes {
			n += r.Streams[i].Nodes[j].Skipped
		}
	}
	return n
}

// GetAnnotatedLatest is the corruption-tolerant assembly behind an unqualified
// "latest". It walks every stream newest-to-oldest and serves the nearest
// record that decodes AND whose embedded identity matches the row columns:
//
//   - schema first; the schema generation actually found binds every other
//     stream, so stats from a newer corrupt schema (or any other generation)
//     can never be spliced in;
//   - planner: the newest valid row bound to that schema_ref_hash;
//   - activity and query: the newest valid row independently PER NODE under
//     that hash, so one node's torn write never blanks the others.
//
// When no valid predecessor exists in a required range the returned report
// carries an explicit reason and, for the schema itself, GetAnnotatedLatest
// fails with ErrHistoryCorrupt rather than serving an empty bundle.
func (s *Store) GetAnnotatedLatest(ctx context.Context, key SnapshotKey) (*schema.AnnotatedSchema, LatestRecovery, error) {
	pid, did := string(key.ProjectID), string(key.DatabaseID)
	rec := LatestRecovery{}

	snap, schemaFB, err := s.latestValidSchema(ctx, pid, did)
	if err != nil {
		if schemaFB.degraded() {
			// carry the skip count even though there is nothing to serve
			rec.Streams = append(rec.Streams, schemaFB)
		}
		return nil, rec, err
	}
	rec.ServedSchemaHash = snap.ContentHash
	if schemaFB.degraded() {
		rec.Streams = append(rec.Streams, schemaFB)
	}

	planner, plannerFB, err := s.latestValidPlannerRow(ctx, pid, did, snap.ContentHash)
	if err != nil {
		return nil, rec, err
	}
	if plannerFB.degraded() {
		rec.Streams = append(rec.Streams, plannerFB)
	}

	acts, activityFB, err := scanLatestValidNodeRows[schema.ActivityStatsSnapshot](
		ctx, s.db, "activity_stats", "activity", pid, did, snap.ContentHash,
		func(a *schema.ActivityStatsSnapshot, contentHash, schemaRef, node string) bool {
			return a.ContentHash != "" && a.ContentHash == contentHash &&
				a.SchemaRefHash == schemaRef && a.Node.Source == node
		})
	if err != nil {
		return nil, rec, err
	}
	if activityFB.degraded() {
		rec.Streams = append(rec.Streams, activityFB)
	}

	qs, queryFB, err := scanLatestValidNodeRows[schema.QueryStatsSnapshot](
		ctx, s.db, "query_stats", "query", pid, did, snap.ContentHash,
		func(q *schema.QueryStatsSnapshot, contentHash, schemaRef, node string) bool {
			return q.ContentHash != "" && q.ContentHash == contentHash &&
				q.SchemaRefHash == schemaRef && q.Node.Source == node
		})
	if err != nil {
		return nil, rec, err
	}
	if queryFB.degraded() {
		rec.Streams = append(rec.Streams, queryFB)
	}

	out := &schema.AnnotatedSchema{Schema: snap}
	if planner != nil {
		out.Planner = schema.RollUpPartitionSizing(snap, planner)
	}
	nodes, err := s.joinActivityNodes(ctx, key, snap, acts)
	if err != nil {
		return nil, rec, err
	}
	if len(nodes) > 0 {
		out.Merged = &schema.MergedActivity{Nodes: nodes}
	}
	out.QueryStats = qs
	return out, rec, nil
}

// latestValidSchema walks schema rows newest-first and returns the first that
// decodes into a snapshot whose embedded content hash matches the row column.
// A mismatched identity means the bytes belong to another record and is as
// unusable as one that will not parse.
func (s *Store) latestValidSchema(ctx context.Context, pid, did string) (*schema.SchemaSnapshot, StreamFallback, error) {
	fb := StreamFallback{Stream: "schema"}
	rows, err := s.db.QueryContext(ctx,
		`SELECT content_hash, snapshot_json FROM snapshots
		  WHERE project_id = ? AND database_id = ?
		  ORDER BY timestamp DESC, id DESC`,
		pid, did)
	if err != nil {
		return nil, fb, err
	}
	defer rows.Close()

	sawRow := false
	for rows.Next() {
		var colHash, payload string
		if err := rows.Scan(&colHash, &payload); err != nil {
			return nil, fb, err
		}
		sawRow = true
		var snap schema.SchemaSnapshot
		if json.Unmarshal([]byte(payload), &snap) != nil ||
			snap.ContentHash == "" || snap.ContentHash != colHash {
			fb.Skipped++
			continue
		}
		return &snap, fb, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fb, err
	}
	if !sawRow {
		return nil, fb, fmt.Errorf("%w (latest schema for project=%s database=%s)",
			ErrSnapshotNotFound, pid, did)
	}
	fb.Reason = fmt.Sprintf(
		"no readable schema snapshot; skipped %d unusable newest row(s); recapture required",
		fb.Skipped)
	return nil, fb, fmt.Errorf("%w: project=%s database=%s: %s",
		ErrHistoryCorrupt, pid, did, fb.Reason)
}

// latestValidPlannerRow walks planner rows newest-first. schemaRef "" means
// unscoped (a payload is checked against its row's schema_ref_hash column).
// No rows at all is the ordinary "planner not captured yet" absence; rows
// present but every one unusable is reported as an unavailable range instead.
func (s *Store) latestValidPlannerRow(ctx context.Context, pid, did, schemaRef string) (*schema.PlannerStatsSnapshot, StreamFallback, error) {
	fb := StreamFallback{Stream: "planner"}
	query := `SELECT content_hash, schema_ref_hash, payload_json FROM planner_stats
		  WHERE project_id = ? AND database_id = ?`
	args := []any{pid, did}
	if schemaRef != "" {
		query += ` AND schema_ref_hash = ?`
		args = append(args, schemaRef)
	}
	query += ` ORDER BY timestamp DESC, id DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fb, err
	}
	defer rows.Close()

	sawRow := false
	for rows.Next() {
		var colHash, colRef, payload string
		if err := rows.Scan(&colHash, &colRef, &payload); err != nil {
			return nil, fb, err
		}
		sawRow = true
		var p schema.PlannerStatsSnapshot
		if json.Unmarshal([]byte(payload), &p) != nil ||
			p.ContentHash == "" || p.ContentHash != colHash || p.SchemaRefHash != colRef {
			fb.Skipped++
			continue
		}
		if schemaRef != "" && p.SchemaRefHash != schemaRef {
			fb.Skipped++
			continue
		}
		return &p, fb, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fb, err
	}
	if !sawRow {
		// never bound: not corruption, nothing to report
		return nil, StreamFallback{}, nil
	}
	if schemaRef != "" {
		fb.Reason = fmt.Sprintf(
			"every planner payload bound to schema %s is unreadable; skipped %d row(s)",
			schemaRef, fb.Skipped)
	} else {
		fb.Reason = fmt.Sprintf(
			"every planner payload is unreadable; skipped %d row(s)", fb.Skipped)
	}
	return nil, fb, nil
}

// latestValidNodeStatRow walks one node table newest-first and returns the
// first decodable, identity-consistent row. nodeLabel "" spans every node and
// schemaRef "" spans every generation. saw==false means no row matched at all.
func latestValidNodeStatRow[T any](
	ctx context.Context, db *sql.DB, table, pid, did, nodeLabel, schemaRef string,
	check func(v *T, contentHash, schemaRefHash, node string) bool,
) (*T, int, bool, error) {
	query := `SELECT node_source, content_hash, schema_ref_hash, payload_json FROM ` + table + `
		  WHERE project_id = ? AND database_id = ?`
	args := []any{pid, did}
	if nodeLabel != "" {
		query += ` AND node_source = ?`
		args = append(args, nodeLabel)
	}
	if schemaRef != "" {
		query += ` AND schema_ref_hash = ?`
		args = append(args, schemaRef)
	}
	query += ` ORDER BY timestamp DESC, id DESC`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, false, err
	}
	defer rows.Close()

	saw := false
	skipped := 0
	for rows.Next() {
		var node, colHash, colRef, payload string
		if err := rows.Scan(&node, &colHash, &colRef, &payload); err != nil {
			return nil, skipped, saw, err
		}
		saw = true
		var v T
		if json.Unmarshal([]byte(payload), &v) != nil || !check(&v, colHash, colRef, node) {
			skipped++
			continue
		}
		if schemaRef != "" && colRef != schemaRef {
			skipped++
			continue
		}
		return &v, skipped, saw, nil
	}
	return nil, skipped, saw, rows.Err()
}

// scanLatestValidNodeRows is the per-node fallback core for activity_stats and
// query_stats. Rows arrive newest-first per node_source; the first valid row
// for a node is what it serves, and anything newer that was skipped is that
// node's fallback count. A node whose rows are all unusable gets a reason
// instead of vanishing. schemaRef "" means unscoped: a payload is checked
// against its own row's schema_ref_hash column rather than one generation.
// Generic over the concrete snapshot type; it is a free function because Go
// methods cannot declare type parameters.
func scanLatestValidNodeRows[T any](
	ctx context.Context, db *sql.DB, table, stream, pid, did, schemaRef string,
	check func(v *T, contentHash, schemaRefHash, node string) bool,
) ([]T, StreamFallback, error) {
	fb := StreamFallback{Stream: stream}
	// table is a caller-side literal, never user input.
	query := `SELECT node_source, content_hash, schema_ref_hash, payload_json FROM ` + table + `
		  WHERE project_id = ? AND database_id = ?`
	args := []any{pid, did}
	if schemaRef != "" {
		query += ` AND schema_ref_hash = ?`
		args = append(args, schemaRef)
	}
	query += ` ORDER BY node_source ASC, timestamp DESC, id DESC`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fb, err
	}
	defer rows.Close()

	type nodeState struct {
		picked  bool
		saw     bool
		skipped int
	}
	states := map[string]*nodeState{}
	var order []string
	var out []T

	for rows.Next() {
		var node, colHash, colRef, payload string
		if err := rows.Scan(&node, &colHash, &colRef, &payload); err != nil {
			return nil, fb, err
		}
		st, ok := states[node]
		if !ok {
			st = &nodeState{}
			states[node] = st
			order = append(order, node)
		}
		st.saw = true
		if st.picked {
			continue
		}
		var v T
		if json.Unmarshal([]byte(payload), &v) != nil || !check(&v, colHash, colRef, node) {
			st.skipped++
			continue
		}
		st.picked = true
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fb, err
	}

	fb.Skipped = 0
	for _, label := range order {
		st := states[label]
		fb.Skipped += st.skipped
		if st.skipped == 0 && st.picked {
			continue
		}
		nf := NodeFallback{Node: label, Skipped: st.skipped}
		if !st.picked {
			if schemaRef != "" {
				nf.Reason = fmt.Sprintf(
					"every %s payload for node %q bound to schema %s is unreadable",
					stream, label, schemaRef)
			} else {
				nf.Reason = fmt.Sprintf(
					"every %s payload for node %q is unreadable", stream, label)
			}
		}
		fb.Nodes = append(fb.Nodes, nf)
	}
	return out, fb, nil
}

// joinActivityNodes mirrors GetAnnotated's merge: newest row per label first,
// then the other members of an oscillating pool label, partition rollups
// applied to every entry. Best-effort member lookups never drop the label.
func (s *Store) joinActivityNodes(ctx context.Context, key SnapshotKey, snap *schema.SchemaSnapshot, acts []schema.ActivityStatsSnapshot) ([]schema.NodeActivity, error) {
	var nodes []schema.NodeActivity
	add := func(r schema.ActivityStatsSnapshot) {
		tables, indexes := schema.RollUpPartitionActivity(snap, r.Tables, r.Indexes)
		nodes = append(nodes, schema.NodeActivity{Node: r.Node, Tables: tables, Indexes: indexes})
	}
	for i := range acts {
		members, err := s.activityMembers(ctx, key, snap.ContentHash, acts[i])
		if err != nil {
			if ctxErr(err) {
				return nil, err
			}
			slog.Debug("pool servers unavailable; label keeps its newest row", "node", acts[i].Node.Source, "error", err)
		}
		add(acts[i])
		for _, m := range members {
			add(m)
		}
	}
	return nodes, nil
}
