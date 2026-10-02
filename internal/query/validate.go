package query

import (
	"fmt"
	"slices"
	"strings"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	ValidationResult struct {
		Valid               bool                `json:"valid"`
		Errors              []string            `json:"errors"`
		Warnings            []ValidationWarning `json:"warnings"`
		ReferencedObjects   []ReferencedTable   `json:"referenced_objects"`
		ResolvedStarColumns []ResolvedStar      `json:"resolved_star_columns"`

		// present only when every error was a name with one obvious candidate;
		// the query below has the fixes applied and validates clean
		CorrectedSQL string `json:"corrected_sql,omitempty"`
		Fixes        []Fix  `json:"fixes,omitempty"`
	}

	ValidationWarning struct {
		Severity WarningSeverity `json:"severity"`
		Message  string          `json:"message"`
	}

	WarningSeverity string

	ResolvedStar struct {
		Table   string   `json:"table"`
		Columns []string `json:"columns"`
	}
)

const (
	SeverityInfo    WarningSeverity = "info"
	SeverityWarning WarningSeverity = "warning"
	SeverityError   WarningSeverity = "error"

	maxCorrectionRounds = 3
)

// parses SQL and validates references against the schema
func ValidateQuery(sql string, snap *schema.SchemaSnapshot) (*ValidationResult, error) {
	result, plan, err := validate(sql, snap)
	if err != nil || result.Valid {
		return result, err
	}

	// A wrong table name hides the column errors under it, so correcting is a
	// loop: fix what resolves, re-validate, look again. Only a pass that ends
	// with the query clean is ever handed back.
	cur, curPlan := sql, plan
	var applied []Fix

	for range maxCorrectionRounds {
		cands, fixes := correctOnce(cur, curPlan)
		if len(cands) == 0 {
			break
		}
		advanced := false
		for _, c := range cands {
			res, p, err := validate(c, snap)
			if err != nil {
				continue
			}
			cur, curPlan = c, p
			applied = append(applied, fixes...)
			advanced = true
			if res.Valid {
				result.CorrectedSQL, result.Fixes = cur, applied
			}
			break
		}
		if !advanced || result.CorrectedSQL != "" {
			break
		}
	}
	return result, nil
}

// correct() re-validates its own rewrite, so the correction pass must not
// recurse back into it.
func validate(sql string, snap *schema.SchemaSnapshot) (*ValidationResult, fixPlan, error) {
	var plan fixPlan

	parsed, err := ParseSQL(sql)
	if err != nil {
		return nil, plan, err
	}

	var (
		errors       []string
		warnings     []ValidationWarning
		resolvedStar []ResolvedStar
	)

	// one resolver for the whole pass: every step reads the same object for
	// the same name, along the snapshot's captured search_path
	resolver := schema.NewResolver(snap)

	// check each referenced table exists
	for _, ref := range parsed.Info.Tables {
		if ref.Schema == nil && slices.Contains(parsed.Info.cteNames, ref.Name) {
			continue
		}
		explicit := ""
		if ref.Schema != nil {
			explicit = *ref.Schema
		}
		if _, _, ok := resolver.LookupRelation(explicit, ref.Name); !ok {
			errors = append(errors, missingRelationMessage(ref, resolver))
			plan.add(tableFix(resolver, ref))
		}
	}

	validateReferencedColumns(parsed, resolver, &errors, &plan)

	// resolve SELECT *
	if parsed.Info.HasSelectStar {
		for _, ref := range parsed.Info.Tables {
			if ref.Schema == nil && slices.Contains(parsed.Info.cteNames, ref.Name) {
				continue
			}
			explicit := ""
			if ref.Schema != nil {
				explicit = *ref.Schema
			}
			t, _, ok := resolver.ResolveTable(explicit, ref.Name)
			if !ok {
				continue
			}
			cols := make([]string, len(t.Columns))
			for i, c := range t.Columns {
				cols[i] = c.Name
			}
			resolvedStar = append(resolvedStar, ResolvedStar{
				Table:   t.Schema + "." + t.Name,
				Columns: cols,
			})
		}
	}

	detectAntipatterns(parsed, resolver, &warnings)

	return &ValidationResult{
		Valid:               len(errors) == 0,
		Errors:              errors,
		Warnings:            warnings,
		ReferencedObjects:   parsed.Info.Tables,
		ResolvedStarColumns: resolvedStar,
	}, plan, nil
}

func validateReferencedColumns(parsed *ParsedQuery, resolver *schema.Resolver, errors *[]string, plan *fixPlan) {
	for _, fc := range parsed.Info.ReferencedColumns {
		if fc.Table == nil {
			continue
		}
		alias := *fc.Table

		var ref *ReferencedTable
		for i := range parsed.Info.Tables {
			t := &parsed.Info.Tables[i]
			if (t.Alias != nil && *t.Alias == alias) || t.Name == alias {
				ref = t
				break
			}
		}
		// a CTE shadows the real table; its columns are not the table's
		if ref == nil || (ref.Schema == nil && slices.Contains(parsed.Info.cteNames, ref.Name)) {
			continue
		}

		explicit := ""
		if ref.Schema != nil {
			explicit = *ref.Schema
		}
		t, _, ok := resolver.ResolveTable(explicit, ref.Name)
		if !ok {
			// the missing table already has its own error and fix; do not
			// pile column errors or pick another schema's columns
			continue
		}
		found := false
		for _, c := range t.Columns {
			if c.Name == fc.Column {
				found = true
				break
			}
		}
		if !found {
			*errors = append(*errors, fmt.Sprintf(
				"column '%s' does not exist on table '%s.%s'",
				fc.Column, t.Schema, t.Name))
			plan.add(columnFix(t, alias, fc))
		}
	}
}

// missingRelationMessage is the one stable diagnostic for an unresolvable
// reference. An explicit qualifier names it verbatim; an unqualified name on
// a multi-schema path also names the schemas Postgres walked, in order, so
// the same query always yields the same message.
func missingRelationMessage(ref ReferencedTable, resolver *schema.Resolver) string {
	if ref.Schema != nil {
		return fmt.Sprintf("table or view '%s.%s' does not exist", *ref.Schema, ref.Name)
	}
	path := resolver.Path()
	if len(path) == 1 && path[0] == schema.LegacyDefaultSchema {
		return fmt.Sprintf("table or view '%s.%s' does not exist", schema.LegacyDefaultSchema, ref.Name)
	}
	return fmt.Sprintf("table or view '%s' does not exist (searched schemas: %s)",
		ref.Name, strings.Join(path, ", "))
}

// An unqualified name that exists in exactly one schema off the search_path
// is a missing qualifier, not a typo; that outranks any distance guess.
func tableFix(resolver *schema.Resolver, ref ReferencedTable) *pendingFix {
	written := ref.Name
	if ref.Schema != nil {
		written = *ref.Schema + "." + ref.Name
	}
	expect := make([]string, len(ref.locs))
	for i := range expect {
		expect[i] = written
	}
	mk := func(to string) *pendingFix {
		return &pendingFix{kind: "table", from: written, to: to, locs: ref.locs, expect: expect}
	}

	if ref.Schema == nil {
		// the name failed resolution along the path, so every holder is
		// necessarily off it; one holder qualifies unambiguously
		holders := resolver.AllNamedRelations(ref.Name)
		if len(holders) == 1 {
			return mk(holders[0].Schema + "." + ref.Name)
		}
		if len(holders) > 1 {
			return nil
		}

		// typo candidates stay unqualified: the pool is every bare name
		// reachable along the path, so a bare replacement resolves exactly
		// as intended
		var pool []string
		seen := map[string]bool{}
		for _, sch := range resolver.Path() {
			for _, q := range resolver.AllNamedRelationsInSchema(sch) {
				if !seen[q.Name] {
					seen[q.Name] = true
					pool = append(pool, q.Name)
				}
			}
		}
		if cand, ok := bestCandidate(ref.Name, pool); ok {
			return mk(cand)
		}
		return nil
	}

	// explicit qualifier: candidates come from the named schema only, and
	// the qualifier is preserved
	var pool []string
	for _, q := range resolver.AllNamedRelationsInSchema(*ref.Schema) {
		pool = append(pool, q.Name)
	}
	if cand, ok := bestCandidate(ref.Name, pool); ok {
		return mk(*ref.Schema + "." + cand)
	}
	return nil
}

func columnFix(t *schema.Table, alias string, fc FilterColumn) *pendingFix {
	pool := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		pool[i] = c.Name
	}
	cand, ok := bestCandidate(fc.Column, pool)
	if !ok {
		return nil
	}
	return &pendingFix{
		kind:     "column",
		from:     fc.Column,
		to:       cand,
		locs:     []int32{fc.loc},
		lastOnly: true,
		expect:   []string{alias + "." + fc.Column},
	}
}
