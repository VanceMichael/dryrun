package schema

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A capturing role with a non-default search_path must have that effective
// path recorded, in order, with $user expanded and missing entries dropped.
func TestIntrospect_CapturesEffectiveSearchPath(t *testing.T) {
	admin := livePool(t)
	ctx := context.Background()

	// throwaway names; test databases grant CREATE on the database to the
	// login role used by the other live tests
	spSchema := "dryrun_sp_test_schema"
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+spSchema+" CASCADE"); err != nil {
		t.Skipf("cannot set up live search_path test: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+spSchema); err != nil {
		t.Skipf("cannot create schema: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+spSchema+" CASCADE") })

	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	// existing, "$user" (no role-named schema here -> dropped), and a
	// non-existent entry that Postgres silently skips at resolution
	cfg.ConnConfig.RuntimeParams["search_path"] = spSchema + ", pg_catalog, \"$user\", no_such_schema_xyz, public"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	snap, err := IntrospectSchema(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}

	// assert order and filters without assuming the role has no own schema:
	// the custom schema leads, pg_catalog follows, the dead entry is gone,
	// and public survives last
	path := snap.SearchPath
	if len(path) < 3 || path[0] != spSchema || path[1] != "pg_catalog" || path[len(path)-1] != "public" {
		t.Errorf("captured search_path = %v, want %s first, pg_catalog next, public last", path, spSchema)
	}
	for _, s := range path {
		if s == "no_such_schema_xyz" {
			t.Errorf("non-existent schema must be dropped, got %v", path)
		}
	}
	// the captured path must be replayable as a SET LOCAL value
	if setting := SearchPathSetting(snap); !strings.Contains(setting, spSchema) ||
		!strings.Contains(setting, `"public"`) || strings.Contains(strings.ToLower(setting), "no_such_schema_xyz") {
		t.Errorf("replay setting = %q", setting)
	}

	// and it must actually steer name resolution in a live session
	var resolved string
	if err := pool.QueryRow(ctx, "SELECT current_schema").Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved != spSchema {
		t.Errorf("current_schema = %q, want %q", resolved, spSchema)
	}
}

// Without a configured path, the capture still records at least one schema
// for the role (the default "$user", public resolves somewhere).
func TestIntrospect_SearchPathNeverEmptyOnLive(t *testing.T) {
	pool := livePool(t)
	snap, err := IntrospectSchema(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.SearchPath) == 0 {
		t.Fatal("a live role always resolves at least one schema; capture came back empty")
	}
}
