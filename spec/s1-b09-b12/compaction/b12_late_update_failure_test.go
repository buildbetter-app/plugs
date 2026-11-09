package compaction_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The existing Put/pre-Commit faults do not reach the Writer's separate
// BumpSentinels error branch. Forward both the typed source write and actual
// predicate staging before returning a determinate error from that branch.
type b12LateUpdateStore struct {
	driver.Store
	puts, bumps, failures, commits, rollbacks int
	beforeFailure                             func(driver.WriteTx)
}

func (s *b12LateUpdateStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &b12LateUpdateTx{WriteTx: tx, store: s}, nil
}

type b12LateUpdateTx struct {
	driver.WriteTx
	store *b12LateUpdateStore
}

func (tx *b12LateUpdateTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	writer, ok := tx.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	if err := writer.PutGraphRecord(ctx, record, parent, links); err != nil {
		return err
	}
	tx.store.puts++
	return nil
}

func (tx *b12LateUpdateTx) BumpSentinels(ctx context.Context, keys []driver.Sentinel) error {
	if err := tx.WriteTx.BumpSentinels(ctx, keys); err != nil {
		return err
	}
	tx.store.bumps += len(keys)
	tx.store.failures++
	tx.store.beforeFailure(tx.WriteTx)
	return fmt.Errorf("injected B12 update failure after real text and predicate staging")
}

func (tx *b12LateUpdateTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	tx.store.commits++
	return tx.WriteTx.Commit(ctx, header)
}

func (tx *b12LateUpdateTx) Rollback(ctx context.Context) error {
	tx.store.rollbacks++
	return tx.WriteTx.Rollback(ctx)
}

// Done = a public compaction that fails after staging preserves every original
// text field and permanent audit version, proven on all three real engines and
// after SQL reopen, followed by same-key recovery and D32-authorized reversal;
// an error before the fault, or unchanged head alone, does not count.
func TestB12LateUpdateFailureConservesTextAndPermanentAudit(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, tier := range []int{1, 2} {
				t.Run(fmt.Sprintf("tier-%d", tier), func(t *testing.T) {
					ctx := context.Background()
					const key = "late-update-failure"
					original := map[string]any{
						"title":       "title survives failed compaction",
						"description": strings.Repeat("Permanent description λ. ", 240),
						"design":      strings.Repeat("Permanent design. ", 100),
						"notes":       "Permanent notes λ.", "acceptance_criteria": "Permanent acceptance.",
						"compaction_level": 0,
					}
					intermediate := strings.Repeat("Retained tier-one summary λ. ", 12)
					calls := 0
					var inputs []service.CompactionInput
					authority := &presenceAuthority{}
					config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(_ context.Context, input service.CompactionInput) (string, error) {
						calls++
						inputs = append(inputs, input)
						if input.Tier == 1 {
							return intermediate, nil
						}
						if input.Tier != 2 {
							return "", fmt.Errorf("unexpected tier %d", input.Tier)
						}
						return "tiny", nil
					}), VerifyPresence: authority.verify}
					f := newFixture(t, engine, service.WithCompaction(config))
					f.extendedOntology = true
					f.bind()
					made := f.call("bead.create", "late-source-create", map[string]any{"type": "issue", "fields": original}, "")
					ref := made["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
					closed := f.call("bead.close", "late-source-close", map[string]any{"id": ref}, "")
					closedAt := f.clock
					f.clock = f.clock.Add(time.Hour)
					other := f.create("late-other-create", strings.Repeat("Other eligible source. ", 80))
					f.call("bead.close", "late-other-close", map[string]any{"id": other}, "")
					f.clock = closedAt.Add(30 * 24 * time.Hour)
					if tier == 2 {
						first := f.call("admin.compact", "late-existing-tier-one", map[string]any{}, "")
						assertCommitTarget(t, first, ref)
						assertTextAndTier(t, f, ref, intermediate, 1)
						f.clock = closedAt.Add(90 * 24 * time.Hour)
					}
					predicates := b09B10PredicateKeys(t, decodedRef(t, ref), decodedRef(t, other))
					before := b09B10FreshImage(t, f, key, predicates)
					meta := f.meta()
					sqlBefore := b12LateUpdateSQLImage(t, f)
					publicBefore := f.call("bead.get", "late-before", map[string]any{"id": ref}, "")
					auditBefore := f.call("history.get", "late-original-audit", map[string]any{"ref": ref, "asOf": map[string]any{"revision": closed["revision"]}}, "")
					wantVersions := tier + 1 // create, close, and existing tier one
					if before.Receipt != nil || len(before.Records) != 2 || len(before.Versions[ref["id"].(string)]) != wantVersions || calls != tier-1 {
						t.Fatalf("unproven tier-%d baseline: calls=%d versions=%d receipt=%+v", tier, calls, len(before.Versions[ref["id"].(string)]), before.Receipt)
					}
					pinned, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
					if err != nil {
						t.Fatal(err)
					}
					defer pinned.Close()
					fault := &b12LateUpdateStore{Store: f.store}
					fault.beforeFailure = func(tx driver.WriteTx) {
						staged, err := tx.Get(ctx, []model.Ref{decodedRef(t, ref)})
						if err != nil || len(staged) != 1 {
							t.Fatalf("missing actual staged record: %+v %v", staged, err)
						}
						var body struct {
							Fields struct {
								Description        string `json:"description"`
								Design             string `json:"design"`
								Notes              string `json:"notes"`
								AcceptanceCriteria string `json:"acceptance_criteria"`
								Level              int    `json:"compaction_level"`
							} `json:"fields"`
							Metadata map[string]json.RawMessage `json:"metadata"`
						}
						if err := json.Unmarshal(staged[0].Body, &body); err != nil {
							t.Fatal(err)
						}
						var state struct {
							Tier           int            `json:"tier"`
							SourceRevision model.Revision `json:"sourceRevision"`
						}
						if err := json.Unmarshal(body.Metadata["plug.compaction"], &state); err != nil {
							t.Fatal(err)
						}
						wantText := intermediate
						if tier == 2 {
							wantText = "tiny"
						}
						if body.Fields.Description != wantText || body.Fields.Design != "" || body.Fields.Notes != "" || body.Fields.AcceptanceCriteria != "" || body.Fields.Level != tier || state.Tier != tier || state.SourceRevision.String() != fmt.Sprint(publicBefore["bead"].(map[string]any)["revision"]) {
							t.Fatalf("fault preceded complete staged text/level/audit pointer: %s", staged[0].Body)
						}
						if fault.puts != 1 || fault.bumps == 0 {
							t.Fatalf("fault missed actual typed write/predicate staging: puts=%d bumps=%d", fault.puts, fault.bumps)
						}
						b09B10Equal(t, "fresh reader during late failure", before, b09B10FreshImage(t, f, key, predicates))
						b09B10Equal(t, "pinned reader during late failure", before, b09B10ReadImage(t, pinned, meta.HistoryFloor, key, predicates))
						b09B10Equal(t, "independent SQL during late failure", sqlBefore, b12LateUpdateSQLImage(t, f))
					}
					f.store = fault
					f.bind()
					f.call("admin.compact", key, map[string]any{}, "internal_error")
					if calls != tier || fault.failures != 1 || fault.commits != 0 || fault.rollbacks != 1 {
						t.Fatalf("wrong failure branch: calls=%d faults=%d commits=%d rollbacks=%d", calls, fault.failures, fault.commits, fault.rollbacks)
					}
					assertConserved := func(label string) {
						f.assertSameHead(meta)
						f.assertNoReceipt("admin.compact", key)
						b09B10Equal(t, label+" complete committed image", before, b09B10FreshImage(t, f, key, predicates))
						b09B10Equal(t, label+" all durable SQL tables", sqlBefore, b12LateUpdateSQLImage(t, f))
						b09B10Equal(t, label+" public text and metadata", publicBefore, f.call("bead.get", "late-conserved-"+label, map[string]any{"id": ref}, ""))
						b09B10Equal(t, label+" original full public audit", auditBefore, f.call("history.get", "late-audit-"+label, map[string]any{"ref": ref, "asOf": map[string]any{"revision": closed["revision"]}}, ""))
						assertHistoricalText(t, f, ref, closed["revision"], original["description"].(string))
					}
					assertConserved("after-rollback")
					b09B10Equal(t, "pinned reader after rollback", before, b09B10ReadImage(t, pinned, meta.HistoryFloor, key, predicates))
					if err := pinned.Close(); err != nil {
						t.Fatal(err)
					}
					f.store = fault.Store
					f.bind()
					if engine != "memory" {
						f.close()
						f.open()
						assertConserved("after-physical-reopen")
						t.Log("physical SQL close/open retained full text, history, predicates and absence of failed receipt")
					}
					// Removing the fault is a new real attempt with the failed key.
					// It must compact the same item, not replay or skip to the next one.
					recovered := f.call("admin.compact", key, map[string]any{}, "")
					assertCommitTarget(t, recovered, ref)
					if calls != tier+1 || fmt.Sprint(recovered["revision"]) != revisionAfter(t, meta.Head).String() {
						t.Fatalf("recovery consumed extra summary/revision: calls=%d revision=%v", calls, recovered["revision"])
					}
					b09B10Equal(t, "same-key recovery input", inputs[len(inputs)-2], inputs[len(inputs)-1])
					after := b09B10FreshImage(t, f, key, predicates)
					if after.Receipt == nil || after.Receipt.ResponsePending || len(after.Log) != len(before.Log)+1 || len(after.Versions[ref["id"].(string)]) != wantVersions+1 {
						t.Fatal("recovery did not publish exactly one final receipt, version and audit entry")
					}
					b09B10Equal(t, "retained audit prefix", before.Log, after.Log[:len(before.Log)])
					b09B10Equal(t, "retained permanent version bytes", before.Versions[ref["id"].(string)], after.Versions[ref["id"].(string)][:wantVersions])
					b09B10Equal(t, "other eligible record history", before.Versions[other["id"].(string)], after.Versions[other["id"].(string)])
					params := map[string]any{"ref": ref, "baseRevision": recovered["revision"], "expect": []any{}}
					params["humanPresence"] = authority.issue(f, params, "late-revert")
					reverted := f.call("history.revert", "late-revert", params, "")
					assertKind(t, f, reverted["revision"], model.ChangesetRevert)
					restored := f.call("bead.get", "late-restored", map[string]any{"id": ref}, "")["bead"].(map[string]any)
					prior := publicBefore["bead"].(map[string]any)
					b09B10Equal(t, "all restored text fields and level", prior["fields"], restored["fields"])
					b09B10Equal(t, "restored compaction audit pointer", prior["metadata"], restored["metadata"])
					final := b09B10FreshImage(t, f, key, predicates)
					if len(final.Log) != len(before.Log)+2 || len(final.Versions[ref["id"].(string)]) != wantVersions+2 {
						t.Fatal("reversal removed an audit row or published extra versions")
					}
					b09B10Equal(t, "reversal retains compaction audit", after.Log, final.Log[:len(after.Log)])
					b09B10Equal(t, "reversal retains permanent version bytes", after.Versions[ref["id"].(string)], final.Versions[ref["id"].(string)][:wantVersions+1])
					b09B10Equal(t, "reversal retains original full public audit", auditBefore, f.call("history.get", "late-final-audit", map[string]any{"ref": ref, "asOf": map[string]any{"revision": closed["revision"]}}, ""))
					assertHistoricalText(t, f, ref, closed["revision"], original["description"].(string))
					t.Logf("tier-%d actual staged-update fault=1 rollback=1 commit=0; text/audit conserved; same-key recovery and authorized reversal passed", tier)
				})
			}
		})
	}
}

// Keep the accepted generic SQL oracle, then add the typed source tables that
// PutGraphRecord also publishes. The live schema inventory must match exactly:
// a future durable table cannot silently escape the conservation comparison.
func b12LateUpdateSQLImage(t *testing.T, f *fixture) map[string][]string {
	t.Helper()
	image := b09B10SQLImage(t, f)
	if f.engine == "memory" {
		return image
	}
	name, schema := "sqlite", ""
	inventoryQuery := "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name"
	// Lease ownership and lock bookkeeping may change across physical reopen;
	// they are the only application tables excluded from data conservation.
	excluded := []string{"active_lease"}
	if f.engine == "postgres" {
		name, schema = "pgx", "plug_postgres."
		inventoryQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema='plug_postgres' AND table_type='BASE TABLE' ORDER BY table_name"
		excluded = []string{"lease", "locks"}
	}
	typed := []string{
		"hierarchy_index_coverage", "hierarchy_source_current", "hierarchy_source_history",
		"graph_link_index_coverage", "graph_link_source_current", "graph_link_current",
		"graph_link_source_history", "graph_link_history",
	}
	if f.engine == "postgres" {
		typed = append(typed,
			"selection_coverage", "selection_facts", "selection_mutation_fence",
			"selection_mutation_keys", "selection_rows", "selection_sources",
		)
	} else {
		typed = append(typed,
			"selection_backfill_rows", "selection_backfill_runs", "selection_bead_history",
			"selection_coverage", "selection_fact_sets", "selection_facts",
			"selection_family_coverage", "selection_source_history", "selection_virgin_state",
		)
	}
	expected := append([]string{}, excluded...)
	for table := range image {
		expected = append(expected, table)
	}
	expected = append(expected, typed...)
	sort.Strings(expected)
	db, err := sql.Open(name, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, inventoryQuery)
	if err != nil {
		t.Fatal(err)
	}
	var actual []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		actual = append(actual, table)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	b09B10Equal(t, "actual application SQL table inventory including explicit lease/lock exclusions", expected, actual)
	for _, table := range typed {
		// Table names are the fixed source inventory above, never RPC input.
		rows, err := db.QueryContext(ctx, "SELECT * FROM "+schema+table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		image[table] = []string{}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			row := make([]string, len(values))
			for i, value := range values {
				row[i] = fmt.Sprintf("%T:%v", value, value)
			}
			raw, err := json.Marshal(row)
			if err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			image[table] = append(image[table], string(raw))
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(image[table])
	}
	return image
}
