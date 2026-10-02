package query

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/schema"
)

// Live: the EXPLAIN transaction must SET LOCAL search_path to the captured
// order, so a bare table that only exists off the live role's default path
// resolves exactly the way offline analysis resolved it.
func TestExplainQuery_ReplaysCapturedSearchPath(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping live explain test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sp := "qpath_explain_test"
	cleanup := func() {
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+sp+" CASCADE")
	}
	cleanup()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+sp); err != nil {
		t.Skipf("cannot create schema: %v", err)
	}
	t.Cleanup(cleanup)
	if _, err := pool.Exec(ctx, "CREATE TABLE "+sp+".sp_only (id integer)"); err != nil {
		t.Fatal(err)
	}

	// captured path puts the schema first; the live role's default does not
	snap := &schema.SchemaSnapshot{SearchPath: []string{sp, "public"}}
	res, err := ExplainQuery(ctx, pool, "SELECT * FROM sp_only", false, snap)
	if err != nil {
		t.Fatalf("EXPLAIN with replayed search_path failed: %v", err)
	}
	if !strings.Contains(res.SearchPathReplayed, sp) {
		t.Errorf("search_path_replayed = %q, want it to name %s", res.SearchPathReplayed, sp)
	}

	// a pre-feature bundle (no path) replays public, and a table that only
	// exists off public must then fail -- the two surfaces agree
	legacy := &schema.SchemaSnapshot{}
	if _, err := ExplainQuery(ctx, pool, "SELECT * FROM sp_only", false, legacy); err == nil {
		t.Error("legacy public-only replay must not resolve a table hidden in another schema")
	}
}
