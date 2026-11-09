package converterroundtrip

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

func withSourceMetadata(t *testing.T, input []byte, value any) []byte {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	return rewriteData(t, func(members map[string][]byte) {
		clear(members)
		members["manifest.json"], _ = archive.Member("manifest.json")
		for name := range archive.Manifest().Files {
			members[name], _ = archive.Member(name)
		}
		var output bytes.Buffer
		for _, record := range archive.Records("bead") {
			record.Source.Fields["metadata"], record.Body["metadata"] = value, value
			output.Write(encode(t, record))
		}
		members["records/bead.ndjson"] = output.Bytes()
	})
}

func TestRestoreSourceSQLMetadataAndOntologyConserveAllSQLTables(t *testing.T) {
	const raw = " {\"large\":9007199254740993,\"nullable\":null,\"nested\":{\"literal\":\" 外部 \",\"array\":[1,null]}} \n"
	data := withSourceMetadata(t, importedOntologyBytes(t, "queued:active,working:wip,shelved:frozen,finished:done"), raw)
	profile, archive := admittedImportProfile(t, data)
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, db, reopen := openDependencySQLTarget(t, engine)
			before := dependencySQLCensus(t, db, engine, false)
			failure, hit := errors.New("injected protected source lineage failure"), false
			wrapped := &observedTarget{Store: store, wrap: func(tx driver.WriteTx) driver.WriteTx {
				return &ontologySourceFaultTx{WriteTx: tx, RestoreImportTx: tx.(driver.RestoreImportTx), AllocatorContinuation: tx.(driver.AllocatorContinuation), hit: &hit, err: failure}
			}}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), wrapped, lease, profile); !errors.Is(err, failure) || !hit {
				t.Fatalf("actual staging fault not reached: hit=%v err=%v", hit, err)
			}
			if got := dependencySQLCensus(t, db, engine, false); !reflect.DeepEqual(before, got) {
				t.Fatalf("refused import changed application SQL tables, including versions, blobs, states, counters, receipts or leases: %s", dependencySQLDifference(before, got))
			}
			stable := dependencySQLCensus(t, db, engine, true)
			store, lease = reopen()
			if got := dependencySQLCensus(t, db, engine, true); !reflect.DeepEqual(stable, got) {
				t.Fatal("physical reopen exposed staged rows after rollback")
			}
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			requireImportedOntology(t, store, archive, profile, report.Restored.Head)
			assertSourceSQLMetadata(t, store, archive, raw)
			stable = dependencySQLCensus(t, db, engine, true)
			store, lease = reopen()
			if got := dependencySQLCensus(t, db, engine, true); !reflect.DeepEqual(stable, got) {
				t.Fatal("physical reopen changed committed application SQL tables")
			}
			requireImportedOntology(t, store, archive, profile, report.Restored.Head)
			assertSourceSQLMetadata(t, store, archive, raw)
			before = dependencySQLCensus(t, db, engine, false)
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); !errors.Is(err, ErrTargetNotEmpty) {
				t.Fatalf("nonempty target refusal changed: %v", err)
			}
			if !reflect.DeepEqual(before, dependencySQLCensus(t, db, engine, false)) {
				t.Fatal("nonempty-target refusal changed complete SQL census")
			}
			t.Logf("SOURCE_SQL_METADATA engine=%s all_application_tables=%d physical_reopens=2 staging_rollback=true raw_metadata_preserved=true native_recording=false", engine, len(before))
		})
	}
}

// Preserve the complete comparison and expose every changed table on failure.
// Coordination rows remain in the census; diagnostics never waive a delta.
func dependencySQLDifference(before, after map[string][]string) string {
	changed := map[string]map[string][]string{}
	for table, rows := range before {
		if other, present := after[table]; !present || !reflect.DeepEqual(rows, other) {
			changed[table] = map[string][]string{"before": rows, "after": other}
		}
	}
	for table, rows := range after {
		if _, present := before[table]; !present {
			changed[table] = map[string][]string{"before": nil, "after": rows}
		}
	}
	data, _ := json.Marshal(changed)
	return string(data)
}

func assertSourceSQLMetadata(t *testing.T, store driver.Store, archive *dumpformat.Archive, raw string) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	want, err := dumpformat.SourceMetadataObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, bead := range archive.Records("bead") {
		rows, err := r.Get(ctx, []model.Ref{model.RefByID(bead.ID, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatalf("source bead missing: %s %v", bead.ID, err)
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range want {
			var got any
			if decodeReverseJSON(doc.Metadata[key], &got) != nil || !reflect.DeepEqual(got, value) {
				t.Fatalf("SQL metadata semantic value changed at %s/%s: %s", bead.ID, key, doc.Metadata[key])
			}
		}
		var fields map[string]any
		if decodeReverseJSON(doc.Metadata["beads.source_fields"], &fields) != nil || fields["metadata"] != raw || bead.Body["metadata"] != raw || bead.Source.Fields["metadata"] != raw {
			t.Fatal("source SQL metadata text was flattened or replaced")
		}
	}
	envelope, err := r.Blob(ctx, driver.Hash(archive.SHA256()))
	if err != nil || !bytes.Equal(envelope, archive.TarBytes()) {
		t.Fatal("exact source envelope was not retained")
	}
}

func TestRestoreSQLMetadataRefusesMalformedAndReservedObjectsBeforeWrite(t *testing.T) {
	base := admittedFixtureBytes(t)
	profile, _ := admittedImportProfile(t, base)
	for _, raw := range []string{`{"beads.source_fields":{}}`, `{"plug.restore.allocator":1}`, `{"n":1,"n":2}`, `{} {}`, `[]`, `null`, `"{}"`, `{`} {
		t.Run(raw, func(t *testing.T) {
			data := withSourceMetadata(t, base, raw)
			store, lease, cleanup := openTarget(t, "memory")
			defer cleanup()
			before := reverseState(t, store)
			target := &observedTarget{Store: store}
			if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), target, lease, profile); err == nil || !strings.Contains(err.Error(), "metadata") {
				t.Fatalf("malformed/reserved SQL metadata admitted: %v", err)
			}
			if target.writes != 0 || !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("metadata refusal opened a write or changed target")
			}
		})
	}
}

// The database handle is an independent observer of every application table.
// Reopen closes the actual driver and reacquires its lease on the same store.
func openDependencySQLTarget(t *testing.T, engine string) (driver.Store, driver.ActiveLease, *sql.DB, func() (driver.Store, driver.ActiveLease)) {
	t.Helper()
	ctx := context.Background()
	var open func() (driver.Store, error)
	dsn, sqlDriver := filepath.Join(t.TempDir(), "dependency.sqlite"), "sqlite"
	if engine == "postgres" {
		dsn, sqlDriver = identityPostgresDSN(t), "pgx"
		open = func() (driver.Store, error) { return postgres.Open(ctx, driver.Config{DSN: dsn}) }
	} else {
		open = func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn}) }
	}
	store, err := open()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "dependency-semantics", driver.LeaseTTL)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Release(ctx); err != nil {
			t.Errorf("release dependency lease: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close dependency store: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("close dependency SQL observer: %v", err)
		}
	})
	return store, lease, db, func() (driver.Store, driver.ActiveLease) {
		t.Helper()
		if err := lease.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = open()
		if err != nil {
			t.Fatal(err)
		}
		lease, err = store.Lease(ctx, "dependency-semantics", driver.LeaseTTL)
		if err != nil {
			t.Fatal(err)
		}
		return store, lease
	}
}

// Only lease identity/time is excluded across an intentional release/reacquire.
// Immediate refusal comparisons include it. Locks, indexes, allocators,
// versions, log, receipts, blobs and all other discovered tables always count.
func dependencySQLCensus(t *testing.T, db *sql.DB, engine string, reopened bool) map[string][]string {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	prefix := ""
	if engine == "postgres" {
		query = `SELECT tablename FROM pg_tables WHERE schemaname='plug_postgres' ORDER BY tablename`
		prefix = `"plug_postgres".`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(tables) < 18 {
		t.Fatalf("incomplete application SQL table inventory: %v", tables)
	}
	result := map[string][]string{}
	for _, table := range tables {
		if reopened && (table == "active_lease" || table == "lease") {
			continue
		}
		rows, err := tx.QueryContext(ctx, `SELECT * FROM `+prefix+`"`+strings.ReplaceAll(table, `"`, `""`)+`"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var values []string
		for rows.Next() {
			row := make([]any, len(columns))
			args := make([]any, len(columns))
			for i := range row {
				args[i] = &row[i]
			}
			if err := rows.Scan(args...); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, string(encoded))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(values)
		columnJSON, _ := json.Marshal(columns)
		result[table] = append([]string{string(columnJSON)}, values...)
	}
	return result
}
