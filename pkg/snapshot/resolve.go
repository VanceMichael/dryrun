package snapshot

import (
	"sort"
	"strings"
)

// LegacyDefaultSchema is what every name resolved against before snapshots
// carried an effective search_path. Old history.db rows and remote bundles
// without one keep this behavior.
const LegacyDefaultSchema = "public"

// EffectiveSearchPath is the replayable schema order names resolve along:
// trimmed, de-duplicated entries from the captured list. A nil snapshot, an
// empty list, or a list holding only empty/unreadable entries resolves the
// legacy way as ["public"]. Explicit schema qualifiers never consult this.
func EffectiveSearchPath(s *SchemaSnapshot) []string {
	if s == nil || len(s.SearchPath) == 0 {
		return []string{LegacyDefaultSchema}
	}
	out := make([]string, 0, len(s.SearchPath))
	seen := make(map[string]bool, len(s.SearchPath))
	for _, raw := range s.SearchPath {
		name := strings.TrimSpace(raw)
		// an empty or unreadable entry is skipped, exactly as Postgres
		// skips a schema it cannot find on the path
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	if len(out) == 0 {
		return []string{LegacyDefaultSchema}
	}
	return out
}

// quoteSettingIdent renders one schema name for SET LOCAL search_path.
// Quoting every entry is always legal; doubling embedded quotes keeps the
// value injection-safe.
func quoteSettingIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// SearchPathSetting renders the effective path as a SET LOCAL value that
// replays the same resolution order in a live session:
//
//	"app", "tenant", public
//
// Every entry is quoted, so mixed-case or punctuation-bearing names survive
// the round trip, and a schema missing from the live catalog is skipped by
// Postgres rather than erroring.
func SearchPathSetting(s *SchemaSnapshot) string {
	path := EffectiveSearchPath(s)
	parts := make([]string, len(path))
	for i, sch := range path {
		parts[i] = quoteSettingIdent(sch)
	}
	return strings.Join(parts, ", ")
}

type RelationKind uint8

const (
	RelNone RelationKind = iota
	RelTable
	RelView
)

// Resolver applies the single relation-resolution rule every analysis entry
// point shares: an explicit schema qualifier is authoritative and never
// falls back; an unqualified name picks the first match along the snapshot's
// effective search_path, so same-named objects in different schemas resolve
// to exactly one table/view, its own columns, constraints and statistics.
//
// A Resolver is built per use from one snapshot; holding no package-level
// state, two snapshots (different profiles or databases) can never leak
// resolution state into each other.
type Resolver struct {
	snap *SchemaSnapshot
	path []string

	tables map[QualifiedName]*Table
	views  map[QualifiedName]*View

	// first object holding a bare name along path order
	bareTable map[string]*Table
	bareView  map[string]*View
	bareRel   map[string]resolvedRel
}

type resolvedRel struct {
	kind  RelationKind
	q     QualifiedName
	table *Table
	view  *View
}

// NewResolver indexes one snapshot. A nil snapshot resolves as legacy
// ["public"] against no objects, which yields stable missing results rather
// than panicking.
func NewResolver(s *SchemaSnapshot) *Resolver {
	r := &Resolver{
		snap:      s,
		path:      EffectiveSearchPath(s),
		tables:    make(map[QualifiedName]*Table),
		views:     make(map[QualifiedName]*View),
		bareTable: make(map[string]*Table),
		bareView:  make(map[string]*View),
		bareRel:   make(map[string]resolvedRel),
	}
	if s == nil {
		return r
	}
	for i := range s.Tables {
		t := &s.Tables[i]
		r.tables[QualifiedName{Schema: t.Schema, Name: t.Name}] = t
	}
	for i := range s.Views {
		v := &s.Views[i]
		r.views[QualifiedName{Schema: v.Schema, Name: v.Name}] = v
	}
	// path order is the outer loop so an object in an earlier schema always
	// shadows a same-named one in a later schema
	for _, sch := range r.path {
		for i := range s.Tables {
			t := &s.Tables[i]
			if t.Schema != sch {
				continue
			}
			if _, ok := r.bareRel[t.Name]; !ok {
				q := QualifiedName{Schema: t.Schema, Name: t.Name}
				r.bareRel[t.Name] = resolvedRel{kind: RelTable, q: q, table: t}
				r.bareTable[t.Name] = t
			}
		}
		for i := range s.Views {
			v := &s.Views[i]
			if v.Schema != sch {
				continue
			}
			if _, ok := r.bareRel[v.Name]; !ok {
				q := QualifiedName{Schema: v.Schema, Name: v.Name}
				r.bareRel[v.Name] = resolvedRel{kind: RelView, q: q, view: v}
				r.bareView[v.Name] = v
			}
		}
	}
	return r
}

// Path returns a copy of the effective schema order, earliest first.
func (r *Resolver) Path() []string {
	return append([]string(nil), r.path...)
}

// Snapshot returns the snapshot this resolver was built for.
func (r *Resolver) Snapshot() *SchemaSnapshot { return r.snap }

// ResolveTable finds the table an explicit ("schema.name") or unqualified
// ("name") reference denotes. An explicit schema is matched exactly and
// never falls through to search_path. The returned QualifiedName is the
// unique identity columns, constraints, indexes and stats must be read with.
func (r *Resolver) ResolveTable(explicitSchema, name string) (*Table, QualifiedName, bool) {
	if explicitSchema != "" {
		t := r.tables[QualifiedName{Schema: explicitSchema, Name: name}]
		if t == nil {
			return nil, QualifiedName{Schema: explicitSchema, Name: name}, false
		}
		return t, t.Qual(), true
	}
	t := r.bareTable[name]
	if t == nil {
		return nil, QualifiedName{Schema: r.TargetSchema(), Name: name}, false
	}
	return t, t.Qual(), true
}

// ResolveView is ResolveTable for views and materialized views.
func (r *Resolver) ResolveView(explicitSchema, name string) (*View, QualifiedName, bool) {
	if explicitSchema != "" {
		v := r.views[QualifiedName{Schema: explicitSchema, Name: name}]
		if v == nil {
			return nil, QualifiedName{Schema: explicitSchema, Name: name}, false
		}
		return v, QualifiedName{Schema: v.Schema, Name: v.Name}, true
	}
	v := r.bareView[name]
	if v == nil {
		return nil, QualifiedName{Schema: r.TargetSchema(), Name: name}, false
	}
	return v, QualifiedName{Schema: v.Schema, Name: v.Name}, true
}

// LookupRelation resolves a table-or-view reference under one rule,
// reporting which kind was found. Views and tables share a namespace in
// Postgres, so within a schema the name can only denote one of them.
func (r *Resolver) LookupRelation(explicitSchema, name string) (RelationKind, QualifiedName, bool) {
	if explicitSchema != "" {
		q := QualifiedName{Schema: explicitSchema, Name: name}
		if t := r.tables[q]; t != nil {
			return RelTable, t.Qual(), true
		}
		if v := r.views[q]; v != nil {
			return RelView, QualifiedName{Schema: v.Schema, Name: v.Name}, true
		}
		return RelNone, q, false
	}
	if rel, ok := r.bareRel[name]; ok {
		return rel.kind, rel.q, true
	}
	return RelNone, QualifiedName{Schema: r.TargetSchema(), Name: name}, false
}

// TargetSchema is where an unqualified CREATE puts a new object: the first
// creatable entry of the path. The captured list already excludes empty and
// non-existent schemas.
func (r *Resolver) TargetSchema() string {
	if len(r.path) > 0 {
		return r.path[0]
	}
	return LegacyDefaultSchema
}

// ResolveSchemaName says which schema a parsed RangeVar denotes. An explicit
// qualifier always wins, even when nothing is in it yet. An unqualified name
// on an existing object resolves to that object's schema (first on the
// path); on a name the snapshot does not hold it denotes the creation
// target, which is also the legacy default "public" for old bundles.
func (r *Resolver) ResolveSchemaName(explicitSchema, name string) string {
	if explicitSchema != "" {
		return explicitSchema
	}
	if rel, ok := r.bareRel[name]; ok {
		return rel.q.Schema
	}
	return r.TargetSchema()
}

// AllNamedRelations returns every table/view holding the bare name,
// qualified, sorted by schema then name. It backs "the name exists in
// several schemas" diagnostics for callers that take unqualified input
// outside of SQL text (e.g. the describe_table tool).
func (r *Resolver) AllNamedRelations(name string) []QualifiedName {
	var out []QualifiedName
	for q := range r.tables {
		if q.Name == name {
			out = append(out, q)
		}
	}
	for q := range r.views {
		if q.Name == name {
			out = append(out, q)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// AllNamedRelationsInSchema lists every table/view in one schema, sorted by
// name. Used to build the typo-candidate pool reachable from one path entry.
func (r *Resolver) AllNamedRelationsInSchema(sch string) []QualifiedName {
	var out []QualifiedName
	for q := range r.tables {
		if q.Schema == sch {
			out = append(out, q)
		}
	}
	for q := range r.views {
		if q.Schema == sch {
			out = append(out, q)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
