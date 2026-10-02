package snapshot

import (
	"reflect"
	"testing"
)

func searchPathSnap() *SchemaSnapshot {
	return &SchemaSnapshot{
		SearchPath: []string{"app", "tenant", "public"},
		Tables: []Table{
			{Schema: "app", Name: "orders", Columns: []Column{{Name: "app_col"}, {Name: "id"}}},
			{Schema: "tenant", Name: "orders", Columns: []Column{{Name: "tenant_col"}, {Name: "id"}}},
			{Schema: "public", Name: "orders", Columns: []Column{{Name: "pub_col"}, {Name: "id"}}},
			{Schema: "app", Name: "events"},
		},
		Views: []View{
			{Schema: "app", Name: "summary"},
			{Schema: "public", Name: "summary"},
		},
	}
}

func TestEffectiveSearchPath_LegacyFallback(t *testing.T) {
	if got := EffectiveSearchPath(nil); !reflect.DeepEqual(got, []string{"public"}) {
		t.Errorf("nil snap = %v, want [public]", got)
	}
	if got := EffectiveSearchPath(&SchemaSnapshot{}); !reflect.DeepEqual(got, []string{"public"}) {
		t.Errorf("empty path = %v, want [public]", got)
	}
	if got := EffectiveSearchPath(&SchemaSnapshot{SearchPath: []string{"  ", "", "x"}}); !reflect.DeepEqual(got, []string{"x"}) {
		t.Errorf("garbage-only path = %v, want [x]", got)
	}
}

func TestEffectiveSearchPath_CleansRepeats(t *testing.T) {
	s := &SchemaSnapshot{SearchPath: []string{" app ", "app", "tenant", "", "tenant"}}
	got := EffectiveSearchPath(s)
	want := []string{"app", "tenant"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestResolver_BareNameFollowsPathOrder(t *testing.T) {
	r := NewResolver(searchPathSnap())
	tbl, q, ok := r.ResolveTable("", "orders")
	if !ok {
		t.Fatal("orders should resolve")
	}
	if q.Schema != "app" {
		t.Fatalf("bare orders resolved to %s, want app", q.Schema)
	}
	if len(tbl.Columns) != 2 || tbl.Columns[0].Name != "app_col" {
		t.Fatalf("got columns %+v, want the app table's own columns", tbl.Columns)
	}
}

func TestResolver_ExplicitSchemaNeverFallsBack(t *testing.T) {
	r := NewResolver(searchPathSnap())
	tbl, q, ok := r.ResolveTable("tenant", "orders")
	if !ok || q.Schema != "tenant" {
		t.Fatalf("explicit tenant.orders = %+v %v %v", tbl, q, ok)
	}
	if tbl.Columns[0].Name != "tenant_col" {
		t.Fatalf("got %s's columns from another schema: %+v", q.Schema, tbl.Columns)
	}

	// an explicit schema that lacks the relation must not fall back to app,
	// even though app is earlier on the path and holds it
	if _, _, ok := r.ResolveTable("public", "events"); ok {
		t.Fatal("public.events must not silently resolve to app.events")
	}
	if _, _, ok := r.ResolveTable("missing_schema", "orders"); ok {
		t.Fatal("missing schema must not fall back along search_path")
	}
}

func TestResolver_TableAndViewShareOneRule(t *testing.T) {
	r := NewResolver(searchPathSnap())
	kind, q, ok := r.LookupRelation("", "summary")
	if !ok || kind != RelView || q.Schema != "app" {
		t.Fatalf("bare summary = %v %v %v", kind, q, ok)
	}
	kind, _, ok = r.LookupRelation("public", "summary")
	if !ok || kind != RelView {
		t.Fatalf("public summary = %v %v", kind, ok)
	}
	kind, _, ok = r.LookupRelation("", "events")
	if !ok || kind != RelTable {
		t.Fatalf("events = %v %v", kind, ok)
	}
	if _, _, ok := r.LookupRelation("", "nope"); ok {
		t.Fatal("missing relation should report absent")
	}
}

func TestResolver_ResolveView(t *testing.T) {
	r := NewResolver(searchPathSnap())
	v, q, ok := r.ResolveView("", "summary")
	if !ok || q.Schema != "app" || v.Name != "summary" {
		t.Fatalf("bare summary view = %+v %v %v", v, q, ok)
	}
	if _, _, ok := r.ResolveView("tenant", "summary"); ok {
		t.Fatal("tenant has no summary view")
	}
}

func TestResolver_SchemaNameAndTarget(t *testing.T) {
	r := NewResolver(searchPathSnap())
	if got := r.ResolveSchemaName("", "orders"); got != "app" {
		t.Errorf("existing bare name = %q, want app", got)
	}
	// a name the snapshot does not hold denotes the creation target
	if got := r.ResolveSchemaName("", "brand_new"); got != "app" {
		t.Errorf("new bare name = %q, want app", got)
	}
	if got := r.ResolveSchemaName("explicit_schema", "brand_new"); got != "explicit_schema" {
		t.Errorf("explicit = %q, want explicit_schema", got)
	}
	if got := r.TargetSchema(); got != "app" {
		t.Errorf("target = %q, want app", got)
	}

	// legacy bundle: creation target stays public
	legacy := NewResolver(&SchemaSnapshot{Tables: []Table{{Schema: "public", Name: "t"}}})
	if got := legacy.ResolveSchemaName("", "brand_new"); got != "public" {
		t.Errorf("legacy target = %q, want public", got)
	}
}

func TestResolver_AllNamedRelations(t *testing.T) {
	r := NewResolver(searchPathSnap())
	got := r.AllNamedRelations("orders")
	want := []QualifiedName{
		{Schema: "app", Name: "orders"},
		{Schema: "public", Name: "orders"},
		{Schema: "tenant", Name: "orders"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("holders = %v, want %v", got, want)
	}
}

func TestResolver_NilSnapshotIsSafe(t *testing.T) {
	r := NewResolver(nil)
	if got := r.Path(); !reflect.DeepEqual(got, []string{"public"}) {
		t.Errorf("nil resolver path = %v", got)
	}
	if _, _, ok := r.ResolveTable("", "anything"); ok {
		t.Error("nil snapshot must not resolve")
	}
	if _, _, ok := r.ResolveTable("app", "anything"); ok {
		t.Error("nil snapshot explicit lookup must not resolve")
	}
}

func TestSearchPathSetting(t *testing.T) {
	if got := SearchPathSetting(&SchemaSnapshot{SearchPath: []string{"app", "public"}}); got != `"app", "public"` {
		t.Errorf("setting = %q", got)
	}
	// embedded quotes are doubled so SET LOCAL cannot be injected through
	if got := SearchPathSetting(&SchemaSnapshot{SearchPath: []string{`a"b`}}); got != `"a""b"` {
		t.Errorf("quoting = %q", got)
	}
	if got := SearchPathSetting(nil); got != `"public"` {
		t.Errorf("legacy setting = %q, want \"public\"", got)
	}
}

func TestResolver_PathIsACopy(t *testing.T) {
	r := NewResolver(searchPathSnap())
	p := r.Path()
	p[0] = "mutated"
	if r.Path()[0] != "app" {
		t.Fatal("Path() must return a copy; a caller mutated resolver state")
	}
}

// search_path is resolution context, not DDL: adding it must not move the
// content hash, or a capture after this feature would dedupe away from every
// pre-feature snapshot and old bundles would read as drifted.
func TestContentHash_IgnoresSearchPath(t *testing.T) {
	base := func() *SchemaSnapshot {
		return &SchemaSnapshot{
			FormatVersion: FormatVersion,
			Tables:        []Table{{Schema: "app", Name: "orders", Columns: []Column{{Name: "id"}}}},
		}
	}
	without := DigestFor(base())
	withPath := base()
	withPath.SearchPath = []string{"app", "tenant", "public"}
	if got := DigestFor(withPath); got != without {
		t.Fatalf("search_path moved the content hash: %s vs %s", got, without)
	}
}
