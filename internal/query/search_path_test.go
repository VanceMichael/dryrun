package query

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// searchPathSnap: app.orders and public.orders are different tables; app is
// first on the captured path. archive/off1/off2 sit off the path.
func searchPathSnap() *schema.AnnotatedSchema {
	s := &schema.SchemaSnapshot{
		PgVersion:   "PostgreSQL 17.0",
		Database:    "appdb",
		Timestamp:   time.Now().UTC(),
		ContentHash: "sp",
		SearchPath:  []string{"app", "public"},
		Tables: []schema.Table{
			{
				Schema: "app", Name: "orders",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "app_col", Ordinal: 2, TypeName: "text"},
					{Name: "tenant_id", Ordinal: 3, TypeName: "bigint"},
				},
				Indexes: []schema.Index{
					{Name: "orders_app_col_idx", Columns: []string{"app_col"}},
				},
			},
			{
				Schema: "public", Name: "orders",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "pub_col", Ordinal: 2, TypeName: "text"},
				},
			},
			{
				Schema: "app", Name: "events",
				Columns: []schema.Column{
					{Name: "id", TypeName: "bigint"},
					{Name: "created_at", TypeName: "timestamptz"},
				},
				PartitionInfo: &schema.PartitionInfo{
					Strategy: schema.PartitionRange,
					Key:      "created_at",
					Children: []schema.PartitionChild{
						{Schema: "app", Name: "events_2026", Bound: "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"},
					},
				},
			},
			{Schema: "archive", Name: "audit_log", Columns: []schema.Column{{Name: "id", TypeName: "bigint"}}},
			{Schema: "off1", Name: "dup", Columns: []schema.Column{{Name: "id", TypeName: "bigint"}}},
			{Schema: "off2", Name: "dup", Columns: []schema.Column{{Name: "id", TypeName: "bigint"}}},
		},
	}
	return &schema.AnnotatedSchema{
		Schema: s,
		Planner: &schema.PlannerStatsSnapshot{
			Timestamp: time.Now().UTC(),
			Tables: []schema.TableSizingEntry{
				{Table: schema.QualifiedName{Schema: "app", Name: "orders"}, Sizing: schema.TableSizing{Reltuples: 9_000_000, TableSize: 50 << 30}},
				{Table: schema.QualifiedName{Schema: "public", Name: "orders"}, Sizing: schema.TableSizing{Reltuples: 10, TableSize: 8192}},
			},
		},
	}
}

func TestSearchPath_ValidateBareNameResolvesFirstOnPath(t *testing.T) {
	snap := searchPathSnap().Schema

	// the app table's own column validates; the public table's does not
	res, err := ValidateQuery("SELECT app_col FROM orders WHERE tenant_id = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Errorf("expected valid against app.orders, got %v", res.Errors)
	}

	res, err = ValidateQuery("SELECT o.pub_col FROM orders o", snap)
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid {
		t.Fatal("pub_col must not validate against app.orders")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "column 'pub_col' does not exist on table 'app.orders'") {
			found = true
		}
	}
	if !found {
		t.Errorf("error must name app.orders, not another schema's copy: %v", res.Errors)
	}
}

func TestSearchPath_ExplicitQualifierBypassesPath(t *testing.T) {
	snap := searchPathSnap().Schema
	res, err := ValidateQuery("SELECT pub_col FROM public.orders WHERE id = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Errorf("public.orders must validate with its own column, got %v", res.Errors)
	}

	// an explicit schema that lacks the table is a hard miss, not path fallback
	res, err = ValidateQuery("SELECT id FROM tenant.orders", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range res.Errors {
		if strings.Contains(e, "table or view 'tenant.orders' does not exist") {
			return
		}
	}
	t.Errorf("expected explicit-miss message, got %v", res.Errors)
}

func TestSearchPath_SelectStarExpandsResolvedTable(t *testing.T) {
	snap := searchPathSnap().Schema
	res, err := ValidateQuery("SELECT * FROM orders", snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ResolvedStarColumns) != 1 {
		t.Fatalf("want one star expansion, got %d", len(res.ResolvedStarColumns))
	}
	star := res.ResolvedStarColumns[0]
	if star.Table != "app.orders" {
		t.Errorf("star table = %q, want app.orders", star.Table)
	}
	wantCols := []string{"id", "app_col", "tenant_id"}
	if strings.Join(star.Columns, ",") != strings.Join(wantCols, ",") {
		t.Errorf("star columns = %v, want %v", star.Columns, wantCols)
	}
}

func TestSearchPath_MissingDiagnosticIsStable(t *testing.T) {
	snap := searchPathSnap().Schema
	res, err := ValidateQuery("SELECT * FROM nowhere", snap)
	if err != nil {
		t.Fatal(err)
	}
	want := "table or view 'nowhere' does not exist (searched schemas: app, public)"
	for _, e := range res.Errors {
		if e == want {
			return
		}
	}
	t.Errorf("want %q, got %v", want, res.Errors)
}

func TestSearchPath_CorrectionQualifiesUniqueOffPathHolder(t *testing.T) {
	snap := searchPathSnap().Schema
	res, err := ValidateQuery("SELECT * FROM audit_log", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.CorrectedSQL, "archive.audit_log") {
		t.Fatalf("want corrected_sql qualifying archive.audit_log, got %q", res.CorrectedSQL)
	}
	if len(res.Fixes) != 1 || res.Fixes[0].To != "archive.audit_log" {
		t.Errorf("fixes = %+v", res.Fixes)
	}

	// same name in two off-path schemas: no mechanical rewrite
	res, err = ValidateQuery("SELECT * FROM dup", snap)
	if err != nil {
		t.Fatal(err)
	}
	if res.CorrectedSQL != "" {
		t.Errorf("an ambiguous off-path name must not be rewritten, got %q", res.CorrectedSQL)
	}
}

func TestSearchPath_SuggestIndexesTargetsResolvedTable(t *testing.T) {
	snap := searchPathSnap().Schema
	// tenant_id has no index on either copy, so it survives the leading-index gate
	suggestions, err := SuggestIndex("SELECT id FROM orders WHERE tenant_id = 1", snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(suggestions) != 1 {
		t.Fatalf("want one suggestion, got %+v", suggestions)
	}
	s := suggestions[0]
	if s.Table != "app.orders" {
		t.Errorf("suggestion table = %q, want app.orders", s.Table)
	}
	if !strings.Contains(s.DDL, "ON app.orders") {
		t.Errorf("DDL must target app.orders: %s", s.DDL)
	}
}

func TestSearchPath_PlanSuggestionWithBarePlanSchema(t *testing.T) {
	snap := searchPathSnap().Schema
	plan := &PlanNode{
		NodeType:     "Seq Scan",
		RelationName: strp("orders"),
		PlanRows:     50_000,
		Filter:       strp("(tenant_id = 1)"),
	}
	suggestions, err := SuggestIndex("", snap, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(suggestions) != 1 || suggestions[0].Table != "app.orders" {
		t.Fatalf("want one app.orders suggestion, got %+v", suggestions)
	}
}

func TestSearchPath_PartitionAntipatternNamesAppTable(t *testing.T) {
	snap := searchPathSnap().Schema
	res, err := ValidateQuery("SELECT id FROM events WHERE id = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w.Message, "partitioned table 'app.events'") {
			found = true
		}
	}
	if !found {
		t.Errorf("want app.events partition warning, got %v", res.Warnings)
	}
}

func TestSearchPath_MigrationSizesResolvedTable(t *testing.T) {
	a := searchPathSnap()

	// bare DDL sizes the 9M-row app table, not the 10-row public one
	checks, err := CheckMigration("ALTER TABLE orders ALTER COLUMN id TYPE text", a)
	if err != nil {
		t.Fatal(err)
	}
	c := checks[0]
	if c.Safety != SafetyDangerous {
		t.Errorf("app.orders is large, want dangerous, got %q", c.Safety)
	}
	if c.RowEstimate == nil || *c.RowEstimate < 1_000_000 {
		t.Errorf("row estimate = %v, want ~9M from app.orders sizing", c.RowEstimate)
	}

	// explicit public picks the small copy and softens to caution
	checks, err = CheckMigration("ALTER TABLE public.orders ALTER COLUMN id TYPE text", a)
	if err != nil {
		t.Fatal(err)
	}
	c = checks[0]
	if c.Safety != SafetyCaution {
		t.Errorf("public.orders is small, want caution, got %q", c.Safety)
	}
	if c.RowEstimate == nil || *c.RowEstimate > 100 {
		t.Errorf("row estimate = %v, want 10 from public.orders sizing", c.RowEstimate)
	}
}

func TestSearchPath_MigrationFileCatalogKeysFollowPath(t *testing.T) {
	a := searchPathSnap()
	ddl := "CREATE TABLE made_it (id bigint);\nALTER TABLE made_it ADD COLUMN note text;"
	checks, err := CheckMigration(ddl, a)
	if err != nil {
		t.Fatal(err)
	}
	last := checks[len(checks)-1]
	// the ADD COLUMN only reports 0 bytes when its catalog lookup keyed the
	// bare name to the same schema the CREATE did
	if last.TableSize == nil || *last.TableSize != "0 bytes" {
		t.Errorf("file-created table must size as empty, got %v (keys mismatched across statements)", last.TableSize)
	}
}

func TestSearchPath_SaferSQLIndexNameDisambiguatesInResolvedSchema(t *testing.T) {
	a := searchPathSnap()
	// app.orders already holds orders_app_col_idx; the generated concurrent
	// name must collide there and take the _1 suffix, never reuse or jump
	// schemas
	checks, err := CheckMigration("CREATE INDEX ON orders (app_col)", a)
	if err != nil {
		t.Fatal(err)
	}
	// the rewrite keeps the bare table name (its resolution is replayed the
	// same way), but the generated name is disambiguated inside app's index
	// namespace, where orders_app_col_idx is already taken
	found := false
	for _, c := range checks {
		for _, s := range c.SaferSQL {
			if strings.Contains(s, "orders_app_col_idx1") && strings.Contains(s, "ON orders") {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("want safer_sql with orders_app_col_idx1 disambiguated in app, got %+v", checks[0].SaferSQL)
	}
}

func TestSearchPath_LegacyBundleResolvesPublic(t *testing.T) {
	a := searchPathSnap()
	a.Schema.SearchPath = nil // an old history.db row or remote bundle

	res, err := ValidateQuery("SELECT pub_col FROM orders", a.Schema)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Errorf("legacy path must resolve bare orders as public, got %v", res.Errors)
	}

	res, err = ValidateQuery("SELECT o.app_col FROM orders o", a.Schema)
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid {
		t.Error("app_col must not resolve on the public copy")
	}

	// legacy missing message keeps the public-qualified shape
	res, err = ValidateQuery("SELECT * FROM nope", a.Schema)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "table or view 'public.nope' does not exist") {
			found = true
		}
	}
	if !found {
		t.Errorf("legacy miss must name public.nope, got %v", res.Errors)
	}
}

func TestSearchPath_ResolversDoNotLeakAcrossSnapshots(t *testing.T) {
	one := searchPathSnap().Schema
	two := searchPathSnap().Schema
	two.SearchPath = []string{"public", "app"}

	r1, r2 := schema.NewResolver(one), schema.NewResolver(two)
	t1, q1, _ := r1.ResolveTable("", "orders")
	t2, q2, _ := r2.ResolveTable("", "orders")
	if q1.Schema != "app" || q2.Schema != "public" {
		t.Fatalf("resolution leaked across snapshots: %s vs %s", q1.Schema, q2.Schema)
	}
	if t1.Columns[1].Name != "app_col" || t2.Columns[1].Name != "pub_col" {
		t.Fatalf("columns came from the wrong snapshot: %v %v", t1.Columns, t2.Columns)
	}
}
