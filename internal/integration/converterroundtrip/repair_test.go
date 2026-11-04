package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const (
	idA = "pl-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	idB = "pl-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	idC = "pl-cccccccccccccccccccccccccccccccc"
	// Fixed 20-row, nine-table image. The archive is unchanged; only the
	// default ontology row gains the adopted blocked:boolean declaration.
	// TestB26BuiltinDerivedGoldenRetainsLegacyAndAddsOnlyBlocked independently
	// reconstructs both complete images without the restore/ontology builders.
	legacySupportedDerived = "f170bf742699fb0bbc577c57b19e7907872e2ea99a34a94fda5e0659197f20ed"
	supportedDerived       = "6f1b59b29636cf665d447b1a4d49c1b5670af05f9d0170cf2c548ff1e51d62ab"
)

// This is a separately constructed head-only input, NOT the pinned exporter
// fixture. Remove unsupported semantics, re-key to the actual random allocator
// profile and retain its exact source census. The unchanged tar is refused.
func supportedMembers(t *testing.T, members map[string][]byte) {
	t.Helper()
	replace := strings.NewReplacer("demo-7.4", idB, "demo-7", idA, "demo-8", idC, `"demo"`, `"pl"`)
	for name, data := range members {
		members[name] = []byte(replace.Replace(string(data)))
	}
	var manifest map[string]any
	decode(t, members["manifest.json"], &manifest)
	manifest["history"] = "unavailable"
	manifest["projects"] = []any{map[string]any{"prefix": "pl", "id_mode": "hash", "hash_length": 32, "last_id": 0, "child_counters": map[string]any{}}}
	counts := map[string]int{}
	kinds := []any{}
	for _, rawKind := range manifest["kind_registry"].([]any) {
		kind := rawKind.(map[string]any)
		name, path := kind["name"].(string), kind["file"].(string)
		if name == "comment" {
			delete(members, path)
			delete(manifest["files"].(map[string]any), path)
			continue
		}
		var output bytes.Buffer
		n := 0
		for _, line := range bytes.Split(bytes.TrimSpace(members[path]), []byte{'\n'}) {
			var record map[string]any
			decode(t, line, &record)
			table := record["source"].(map[string]any)["table"].(string)
			if table == "events" || table == "issue_versions" || table == "child_counters" || table == "issue_counter" {
				continue
			}
			if name == "edge" {
				source(record)["type"] = "blocks"
				body(record)["type"] = "blocks"
			}
			counts[table]++
			if name == "bead" {
				counts["labels"] += len(body(record)["labels"].([]any))
			}
			output.Write(encode(t, record))
			n++
		}
		members[path] = output.Bytes()
		kind["count"] = n
		kinds = append(kinds, kind)
	}
	manifest["kind_registry"] = kinds
	counts["issues.row_lock"], counts["issues.is_blocked"] = counts["issues"], counts["issues"]
	for _, raw := range manifest["sources"].([]any) {
		row := raw.(map[string]any)
		family := row["family"].(string)
		if _, ok := counts[family]; ok || family == "comments" || family == "events" || family == "issue_versions" || family == "child_counters" || family == "issue_counter" {
			row["rows"] = counts[family]
		}
	}
	members["manifest.json"] = encode(t, manifest)
}
func supportedBytes(t *testing.T, change func(map[string][]byte)) []byte {
	return rewriteData(t, func(m map[string][]byte) {
		supportedMembers(t, m)
		if change != nil {
			change(m)
		}
	})
}
func supportedVerification(t *testing.T) Verification {
	t.Helper()
	data := supportedBytes(t, nil)
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildPlan(a)
	if err != nil {
		t.Fatal(err)
	}
	if plan.derivedDigest != supportedDerived {
		t.Fatalf("profile golden digest: got %s", plan.derivedDigest)
	}
	return Verification{ArchiveSHA256: digestBytes(data), DerivedSHA256: supportedDerived}
}

type observedTarget struct {
	driver.Store
	writes int
	wrap   func(driver.WriteTx) driver.WriteTx
}

func (s *observedTarget) Write(ctx context.Context, l driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	tx, err := s.Store.Write(ctx, l)
	if err == nil && s.wrap != nil {
		tx = s.wrap(tx)
	}
	return tx, err
}

type interceptedTx struct {
	driver.WriteTx
	beforeHead   func()
	beforeCommit func()
	uncertain    bool
	abort        bool
	failDerived  bool
}

func (t *interceptedTx) PrepareOfflineRestore(ctx context.Context, id driver.StoreID) (driver.Meta, error) {
	return t.WriteTx.(driver.OfflineRestoreTx).PrepareOfflineRestore(ctx, id)
}
func (t *interceptedTx) Head(ctx context.Context) (model.Revision, error) {
	if t.beforeHead != nil {
		t.beforeHead()
	}
	return t.WriteTx.Head(ctx)
}
func (t *interceptedTx) PutDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	err := t.WriteTx.PutDerived(ctx, table, del, add)
	if err == nil && t.failDerived {
		return errors.New("injected derived staging failure")
	}
	return err
}
func (t *interceptedTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	if t.beforeCommit != nil {
		t.beforeCommit()
	}
	if t.abort {
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
	}
	c, err := t.WriteTx.Commit(ctx, h)
	if err == nil && t.uncertain {
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
	}
	return c, err
}

func TestRestoreRefusesUnsupportedSemanticsBeforeWrite(t *testing.T) {
	tests := []struct {
		name  string
		data  func(*testing.T) []byte
		cause string
	}{
		{"unchanged exporter fixture", fixtureBytes, "unverified source history"},
		{"parent structure", func(t *testing.T) []byte {
			return supportedBytes(t, func(m map[string][]byte) {
				changeRecord(t, m, "edge", func(r map[string]any) { source(r)["type"] = "parent-child"; body(r)["type"] = "parent-child" })
			})
		}, "relation type parent-child"},
		{"sequential allocator", func(t *testing.T) []byte {
			return supportedBytes(t, func(m map[string][]byte) {
				for name, data := range m {
					m[name] = []byte(strings.NewReplacer(idA, "pl-1", idB, "pl-2", idC, "pl-3").Replace(string(data)))
				}
				changeManifest(t, m, func(x map[string]any) {
					p := x["projects"].([]any)[0].(map[string]any)
					p["id_mode"] = "sequential"
					p["hash_length"] = 0
					p["last_id"] = 3
				})
			})
		}, "allocator continuation"},
		{"child allocator", func(t *testing.T) []byte {
			return supportedBytes(t, func(m map[string][]byte) {
				changeManifest(t, m, func(x map[string]any) {
					x["projects"].([]any)[0].(map[string]any)["child_counters"] = map[string]any{idA: 4}
				})
			})
		}, "allocator continuation"},
		{"unverified comment author", func(t *testing.T) []byte {
			return rewriteData(t, func(m map[string][]byte) {
				raw := append([]byte(nil), m["records/comment.ndjson"]...)
				var orig map[string]any
				decode(t, m["manifest.json"], &orig)
				supportedMembers(t, m)
				m["records/comment.ndjson"] = []byte(strings.NewReplacer("demo-7", idA, `"demo"`, `"pl"`).Replace(string(raw)))
				changeManifest(t, m, func(x map[string]any) {
					kinds := x["kind_registry"].([]any)
					kinds = append(kinds, orig["kind_registry"].([]any)[2])
					sort.Slice(kinds, func(i, j int) bool {
						return kinds[i].(map[string]any)["name"].(string) < kinds[j].(map[string]any)["name"].(string)
					})
					x["kind_registry"] = kinds
					x["files"].(map[string]any)["records/comment.ndjson"] = map[string]any{}
					for _, r := range x["sources"].([]any) {
						d := r.(map[string]any)
						if d["family"] == "comments" {
							d["rows"] = 1
						}
					}
				})
			})
		}, "unverified comment attribution"},
	}
	for _, prefix := range []string{"beads.agent_state", "beads.await_id", "beads.source.status", "beads.source_fields", "plug.restore.allocator"} {
		key := prefix
		tests = append(tests, struct {
			name  string
			data  func(*testing.T) []byte
			cause string
		}{key, func(t *testing.T) []byte {
			return supportedBytes(t, func(m map[string][]byte) {
				changeRecord(t, m, "bead", func(r map[string]any) {
					v := body(r)["metadata"].(map[string]any)
					v[key] = "original"
					source(r)["metadata"] = v
				})
			})
		}, "reserved restore metadata"})
	}
	for _, kind := range []string{"memory", "sqlite"} {
		for _, tt := range tests {
			t.Run(kind+"/"+tt.name, func(t *testing.T) {
				data := tt.data(t)
				if _, err := dumpformat.Read(bytes.NewReader(data)); err != nil {
					t.Fatalf("test did not reach restore validation: %v", err)
				}
				store, lease, cleanup := openTarget(t, kind)
				defer cleanup()
				before := snapshot(t, store)
				target := &observedTarget{Store: store}
				_, err := RestoreWithLease(context.Background(), bytes.NewReader(data), target, lease)
				if err == nil || !strings.Contains(err.Error(), tt.cause) {
					t.Fatalf("got %v; wanted %s", err, tt.cause)
				}
				if tt.name == "external target even when ID matches a bead" {
					var unresolved *dumpformat.UnresolvedDependencyError
					if !errors.As(err, &unresolved) || unresolved.EdgeID == "" || unresolved.Project != "pl" || unresolved.Source != idB || unresolved.Target != idA || unresolved.TargetKind != "depends_on_external" {
						t.Fatalf("external dependency lost its typed endpoint report: %+v", err)
					}
				}
				if target.writes != 0 || !reflect.DeepEqual(before, snapshot(t, store)) {
					t.Fatal("semantic refusal opened a write or changed the target")
				}
			})
		}
	}
}

func TestRestoreRequiresMatchingIndependentComparison(t *testing.T) {
	data := supportedBytes(t, nil)
	check := supportedVerification(t)
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			target := &observedTarget{Store: store}
			before := snapshot(t, store)
			for _, checks := range [][]Verification{
				nil,
				{{ArchiveSHA256: check.ArchiveSHA256, DerivedSHA256: strings.Repeat("0", 64)}},
				{{ArchiveSHA256: strings.Repeat("0", 64), DerivedSHA256: check.DerivedSHA256}},
				// The previous image omitted mandatory self ancestors. Its
				// comparison must refuse before any write, even on an empty store.
				{{ArchiveSHA256: check.ArchiveSHA256, DerivedSHA256: "76f2bbff964bca8043b9952adeb69583432b567d622a9dbca2d541cf816229eb"}},
				// The old array-valued closure image is also incompatible with
				// canonical hierarchy rows and must never authorize publication.
				{{ArchiveSHA256: check.ArchiveSHA256, DerivedSHA256: "89f6eb61310b4f9193e9925e2a2305101b126f763e870986bc7ca51f6043d28a"}},
			} {
				if _, err := RestoreWithLease(context.Background(), bytes.NewReader(data), target, lease, checks...); !errors.Is(err, ErrRestoreUnavailable) {
					t.Fatalf("mismatch: %v", err)
				}
			}
			if target.writes != 0 || !reflect.DeepEqual(before, snapshot(t, store)) {
				t.Fatal("comparison refusal mutated target")
			}
		})
	}
}

func assertRestoreSelfAncestors(t *testing.T, store driver.Store) {
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
	// Observe requires encoded key order, which differs from the source ID order.
	ids := []string{idC, idA, idB}
	// Fixed contract bytes, independently derived without the production codec.
	selfKeys := []string{
		"cGwtY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2M.cGwtY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2M",
		"cGwtYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE.cGwtYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE",
		"cGwtYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmI.cGwtYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmI",
	}
	keys := make([]driver.LockKey, len(ids))
	for i, key := range selfKeys {
		keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: key}}
	}
	rows, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil || len(rows) != len(ids) {
		t.Fatalf("self ancestor census: %+v %v", rows, err)
	}
	states, err := r.Observe(ctx, keys)
	if err != nil || len(states) != len(ids) {
		t.Fatalf("ancestor rows: %+v %v", states, err)
	}
	for i, id := range ids {
		state := states[i]
		// The parent-free image has exactly one self row per bead. Canonical
		// depth is the scalar integer 0, independent of status/blocking links.
		want := "0"
		var got string
		if state.Derived != nil {
			got = string(state.Derived.Value)
		}
		if !state.Present || state.Derived == nil || state.Derived.Key != keys[i].Derived.Key || state.Revision.String() != "1" || string(state.Derived.Value) != want {
			t.Errorf("ancestor closure %s: got revision %s, value %q; want revision 1, value %s", id, state.Revision, got, want)
		}
	}
}

func TestRestoreDurableImageHistoryFloorAndRecovery(t *testing.T) {
	data := supportedBytes(t, nil)
	check := supportedVerification(t)
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var store driver.Store
			path := filepath.Join(t.TempDir(), "restore.db")
			if kind == "sqlite" {
				var err error
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				store = openStore(t, kind)
			}
			defer func() { store.Close() }()
			before, _ := store.Meta(ctx)
			oldReader, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer oldReader.Close()
			lease, err := store.Lease(ctx, "restore-test", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			target := &observedTarget{Store: store, wrap: func(tx driver.WriteTx) driver.WriteTx { return &interceptedTx{WriteTx: tx, uncertain: true} }}
			report, err := RestoreWithLease(ctx, bytes.NewReader(data), target, lease, check)
			if err != nil || !report.Recovered {
				t.Fatalf("uncertain committed restore: %+v %v", report, err)
			}
			if report.Restored.Incarnation == before.Incarnation || report.Restored.HistoryFloor.String() != "1" {
				t.Fatal("incarnation/floor not restored")
			}
			assertRestoreSelfAncestors(t, store)
			oldPage, err := oldReader.Query(ctx, driver.Query{Limit: 1000})
			if err != nil || len(oldPage.Items) != 0 {
				t.Fatalf("old pinned reader lost snapshot: %v", err)
			}
			oldReader.Close()
			lease.Release(ctx)
			if kind == "sqlite" {
				store.Close()
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: path})
				if err != nil {
					t.Fatal(err)
				}
				assertRestoreSelfAncestors(t, store)
			}
			recovered, err := Recover(ctx, store, check)
			if err != nil || recovered.Restored.Incarnation != report.Restored.Incarnation {
				t.Fatalf("restart recovery: %+v %v", recovered, err)
			}
			bad := check
			bad.DerivedSHA256 = strings.Repeat("0", 64)
			if _, err = Recover(ctx, store, bad); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("mismatched receipt accepted: %v", err)
			}
			if _, err = store.Read(ctx, driver.View{Incarnation: before.Incarnation}); err == nil {
				t.Fatal("old incarnation accepted")
			}
			zero := model.Natural64(0)
			if _, err = store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &zero}); err == nil {
				t.Fatal("history below floor accepted")
			}
			r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if _, err = r.Changesets(ctx, nil, 1000); err == nil {
				t.Fatal("changeset cursor below floor accepted")
			}
			versions, err := r.Versions(ctx, model.RefByID(idA, zero), driver.PageReq{Limit: 1000})
			if err != nil || len(versions.Items) != 1 || !versions.Items[0].Base || versions.Items[0].Revision.String() != "1" {
				t.Fatalf("native history base: %+v %v", versions, err)
			}
			a, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := buildPlan(a)
			if err != nil {
				t.Fatal(err)
			}
			keys := []driver.LockKey{}
			for _, d := range plan.derived {
				keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: d.Table, Key: d.Key}})
			}
			states, err := r.Observe(ctx, keys)
			if err != nil {
				t.Fatal(err)
			}
			for i, state := range states {
				if !state.Present || state.Derived == nil || !bytes.Equal(state.Derived.Value, plan.derived[i].Value) || state.Revision.String() != "1" {
					t.Fatalf("derived row missing: %+v", state)
				}
			}
			for hash, want := range plan.blobs {
				got, err := r.Blob(ctx, driver.Hash(hash))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("source member bytes not durable: %s %v", hash, err)
				}
			}
			rows, err := r.Get(ctx, []model.Ref{model.RefByID("restore/archive", zero), model.RefByID(idA, zero), model.RefByID(idB, zero)})
			if err != nil || len(rows) != 3 {
				t.Fatal(err)
			}
			byID := map[string]driver.Record{}
			for _, row := range rows {
				byID[row.Ref.ID] = row
			}
			var evidence archiveEvidence
			if err = json.Unmarshal(byID["restore/archive"].Body, &evidence); err != nil {
				t.Fatal(err)
			}
			if evidence.ArchiveSHA256 != digestBytes(data) || evidence.DerivedSHA256 != supportedDerived || len(evidence.Members) != len(a.Manifest().Files)+1 {
				t.Fatal("durable digest set incomplete")
			}
			for name, want := range evidence.Members {
				raw, ok := a.Member(name)
				if !ok || want.SHA256 != digestBytes(raw) || want.Size != int64(len(raw)) {
					t.Fatalf("digest mismatch: %s", name)
				}
			}
			doc, _ := pipeline.DecodeDocument(byID[idA].Body)
			if !reflect.DeepEqual(doc.Labels, []string{"alpha", "zeta"}) || len(doc.Comments) != 0 {
				t.Fatal("labels or principals changed")
			}
			doc, _ = pipeline.DecodeDocument(byID[idB].Body)
			if len(doc.Links) != 1 || doc.Links[0].To.ID != idA || doc.Links[0].Kind != "blocks" {
				t.Fatal("internal dependency not preserved")
			}
			wantReady := map[string]string{idA: `{"blocked":false,"ready":true}`, idB: `{"blocked":true,"ready":false}`, idC: `{"blocked":false,"ready":false}`}
			for _, state := range states {
				if state.Key.Derived.Table == driver.DerivedReadyState && string(state.Derived.Value) != wantReady[state.Key.Derived.Key] {
					t.Fatalf("wrong rebuilt readiness: %+v", state)
				}
			}
		})
	}
}

func TestRestoreAtomicRefusalAndSameLeaseRace(t *testing.T) {
	ctx := context.Background()
	data := supportedBytes(t, nil)
	check := supportedVerification(t)
	for _, kind := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"receipt-only", "checkpoint-only", "same-lease writer before Head", "checkpoint at publication", "derived staging rollback", "uncertain uncommitted"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				store, lease, cleanup := openTarget(t, kind)
				defer cleanup()
				before := snapshot(t, store)
				checkpoint := func() {
					_, err := store.(driver.CheckpointStore).SaveCheckpoint(ctx, lease, "restore-race", 0, []byte("other writer"))
					if err != nil {
						t.Fatal(err)
					}
				}
				target := &observedTarget{Store: store}
				switch mode {
				case "receipt-only":
					meta, _ := store.Meta(ctx)
					tx, e := store.Write(ctx, lease)
					if e != nil {
						t.Fatal(e)
					}
					defer tx.Close()
					if _, e = tx.Lock(ctx, nil); e != nil {
						t.Fatal(e)
					}
					if _, e = tx.Head(ctx); e != nil {
						t.Fatal(e)
					}
					_, e = tx.(driver.NoopCommitter).CommitNoop(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: restorePrincipal, Method: "other.noop", IdempotencyKey: "other", RequestHash: "other"})
					if e != nil {
						t.Fatal(e)
					}
				case "checkpoint-only":
					checkpoint()
				case "same-lease writer before Head":
					target.wrap = func(tx driver.WriteTx) driver.WriteTx {
						return &interceptedTx{WriteTx: tx, beforeHead: func() { putOther(t, store, lease) }}
					}
				case "checkpoint at publication":
					target.wrap = func(tx driver.WriteTx) driver.WriteTx { return &interceptedTx{WriteTx: tx, beforeCommit: checkpoint} }
				case "derived staging rollback":
					target.wrap = func(tx driver.WriteTx) driver.WriteTx { return &interceptedTx{WriteTx: tx, failDerived: true} }
				case "uncertain uncommitted":
					target.wrap = func(tx driver.WriteTx) driver.WriteTx { return &interceptedTx{WriteTx: tx, abort: true} }
				}
				report, err := RestoreWithLease(ctx, bytes.NewReader(data), target, lease, check)
				if err == nil {
					t.Fatal("restore accepted occupied/failed target")
				}
				if mode == "receipt-only" || strings.Contains(mode, "checkpoint") || mode == "same-lease writer before Head" {
					if !errors.Is(err, ErrTargetNotEmpty) {
						t.Fatalf("wrong refusal: %v", err)
					}
				}
				if mode == "uncertain uncommitted" {
					var uncertain driver.ErrIndeterminate
					if !errors.As(err, &uncertain) || uncertain.IdempotencyKey != report.IdempotencyKey {
						t.Fatalf("uncertainty lost key: %v", err)
					}
				}
				after := snapshot(t, store)
				if after.Meta.Incarnation != before.Meta.Incarnation || !after.Meta.HistoryFloor.IsZero() {
					t.Fatal("failed restore changed metadata")
				}
				if mode != "same-lease writer before Head" && !reflect.DeepEqual(before, after) {
					t.Fatal("failed restore published source rows/history")
				}
				if mode == "same-lease writer before Head" && (len(after.Records) != 1 || after.Records[0].Ref.ID != "other") {
					t.Fatal("restore overwrote competing writer")
				}
			})
		}
	}
}
func putOther(t *testing.T, store driver.Store, lease driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	meta, _ := store.Meta(ctx)
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	ref := model.RefByID("other", model.Natural64(0))
	key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
	if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "config", Body: []byte(`{"other":true}`)}); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: restorePrincipal, LockKeys: []driver.LockKey{key}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}}}})
	if err != nil {
		t.Fatal(err)
	}
}
func TestConcurrentSameLeaseRestoresHaveOneWinner(t *testing.T) {
	data := supportedBytes(t, nil)
	check := supportedVerification(t)
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			var wg sync.WaitGroup
			results := make(chan error, 2)
			start := make(chan struct{})
			for range 2 {
				wg.Go(func() {
					<-start
					_, err := RestoreWithLease(context.Background(), bytes.NewReader(data), store, lease, check)
					results <- err
				})
			}
			close(start)
			wg.Wait()
			close(results)
			wins := 0
			for err := range results {
				if err == nil {
					wins++
				}
			}
			if wins != 1 {
				t.Fatalf("winners=%d", wins)
			}
			meta, _ := store.Meta(context.Background())
			if meta.Head.String() != "1" {
				t.Fatal("multiple restore commits")
			}
		})
	}
}

func TestB27NonemptyAuxiliaryArchiveRetentionAcrossDrivers(t *testing.T) {
	// Detached consumer input only: no native recording or public capture claim.
	rows := map[string]dumpformat.Row{
		"issue_snapshots": {
			"id": "snapshot-1", "issue_id": idA, "snapshot_time": "2026-11-03T23:00:00Z", "compaction_level": json.Number("2"),
			"original_size": json.Number("9007199254740993"), "compressed_size": json.Number("37"),
			"original_content": "Snapshot λ\nexact", "archived_events": "{\"revision\":9007199254740993, \"optional\":null}\n",
		},
		"compaction_snapshots": {
			"id": "compaction-1", "issue_id": idA, "compaction_level": json.Number("3"),
			"snapshot_json": "{ \"title\":\"雪\", \"ratio\":1.0, \"optional\":null }\n", "created_at": "2026-11-03T23:00:01Z",
		},
		"provenance_events": {
			"id": "provenance-1", "issue_id": idA, "kind": "import", "actor": "unverified-source-actor", "ref": "external-spec", "ref_kind": "spec",
			"payload": "{\"literal\":9007199254740993,\"text\":\"λ\"}\n", "source": "independent-fixture", "occurred_at": nil, "created_at": "2026-11-03T23:00:02Z",
		},
		"events": {
			"id": "event-1", "issue_id": idA, "event_type": "updated", "actor": "unverified-source-actor", "old_value": nil,
			"new_value": "{ \"ratio\":1.0, \"large\":9007199254740993 }", "comment": "Event λ\nexact", "created_at": "2026-11-03T23:00:03Z",
		},
	}
	want := map[string]dumpformat.Record{}
	data := supportedBytes(t, func(members map[string][]byte) {
		var records []dumpformat.Record
		for _, line := range bytes.Split(bytes.TrimSpace(members["records/beads-archive.ndjson"]), []byte{'\n'}) {
			var record dumpformat.Record
			decode(t, line, &record)
			records = append(records, record)
		}
		for family, fields := range rows {
			record := dumpformat.Record{
				Kind: "beads-archive", ID: family + "/" + digestBytes(encode(t, fields)),
				Body:   dumpformat.Row{"read_only": true, "redactable": true, "history_verified": false, "attribution_verified": false},
				Source: dumpformat.Provenance{Table: family, Fields: fields},
			}
			records = append(records, record)
			want[record.ID] = record
		}
		sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
		var raw []byte
		for _, record := range records {
			raw = append(raw, encode(t, record)...)
		}
		members["records/beads-archive.ndjson"] = raw
		changeManifest(t, members, func(m map[string]any) {
			m["history"] = "available-unverified"
			for _, raw := range m["kind_registry"].([]any) {
				kind := raw.(map[string]any)
				if kind["name"] == "beads-archive" {
					kind["count"] = len(records)
				}
			}
			for _, raw := range m["sources"].([]any) {
				source := raw.(map[string]any)
				if _, present := rows[source["family"].(string)]; present {
					source["rows"] = 1
				}
			}
		})
	})
	profile, sourceArchive := admittedImportProfile(t, data)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			check := func(t *testing.T) {
				meta, err := store.Meta(ctx)
				if err != nil || meta != report.Restored {
					t.Fatalf("restored archive identity changed: %+v %v", meta, err)
				}
				r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				retained, err := r.Blob(ctx, driver.Hash(sourceArchive.SHA256()))
				if err != nil || !bytes.Equal(retained, data) {
					t.Fatalf("nonempty auxiliary archive envelope changed: %v", err)
				}
				for name, file := range sourceArchive.Manifest().Files {
					member, _ := sourceArchive.Member(name)
					got, err := r.Blob(ctx, driver.Hash(file.SHA256))
					if err != nil || !bytes.Equal(got, member) {
						t.Fatalf("retained source member changed: %s %v", name, err)
					}
				}
				for id, record := range want {
					got, err := r.Get(ctx, []model.Ref{model.RefByID("source/beads-archive/"+id, model.Natural64(0))})
					if err != nil || len(got) != 1 || got[0].Kind != "beads-archive" {
						t.Fatalf("nonempty auxiliary source row missing: %s %v", record.Source.Table, err)
					}
					var body map[string]any
					decode(t, got[0].Body, &body)
					if body["source_table"] != record.Source.Table || body["source_id"] != record.ID || !bytes.Equal(encode(t, body["source"]), encode(t, record.Source.Fields)) ||
						body["history_verified"] != false || body["attribution_verified"] != false || body["read_only"] != true || body["redactable"] != true {
						t.Fatalf("source fields, opaque bytes or verification flags changed: %s %s", record.Source.Table, got[0].Body)
					}
				}
				dump, err := retainedReverseDump(ctx, r)
				if err != nil || dump.Manifest.History != "available-unverified" {
					t.Fatalf("retained reverse archive lost unverified disposition: %v", err)
				}
				member, _ := sourceArchive.Member("records/beads-archive.ndjson")
				if !bytes.Equal(dump.Members["records/beads-archive.ndjson"], member) {
					t.Fatal("retained reverse archive lost nonempty auxiliary rows")
				}
			}
			t.Run("restored", check)
			store, lease = reopen() // SQL closes/reopens; memory refreshes its lease.
			t.Run("reopened", check)
		})
	}
}

func TestOfflineRestoreDriverCapabilityRequiresHeadAndRecoveryHeader(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store, lease, cleanup := openTarget(t, kind)
			defer cleanup()
			before := snapshot(t, store)
			tx, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Close()
			capability := tx.(driver.OfflineRestoreTx)
			if _, err = capability.PrepareOfflineRestore(ctx, "source"); err == nil {
				t.Fatal("restore acquired no global fence")
			}
			if _, err = tx.Lock(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: before.Meta.Incarnation, Kind: model.ChangesetRestore, Principal: restorePrincipal}); !errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("ordinary transaction claimed restore: %v", err)
			}
			meta, err := capability.PrepareOfflineRestore(ctx, "source")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: restorePrincipal, Method: "offline.restore"}); err == nil {
				t.Fatal("unrecoverable restore accepted")
			}
			if _, err = tx.(driver.NoopCommitter).CommitNoop(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: restorePrincipal, Method: "offline.restore"}); err == nil {
				t.Fatal("restore metadata escaped through no-op")
			}
			tx.Close()
			if !reflect.DeepEqual(before, snapshot(t, store)) {
				t.Fatal("invalid/rolled-back capability changed target")
			}
		})
	}
}

// This frozen normative ontology image and the explicit rows below are an
// independent expected image, not a production-computed digest approval.
// Keeping the old hash live proves the current change is exactly the one
// admitted boolean declaration, with every other row and table conserved.
const legacySupportedOntologyJSON = `{"statuses":{"backlog":{"name":"Backlog","category":"backlog"},"canceled":{"name":"Canceled","category":"canceled"},"done":{"name":"Done","category":"completed","outcome":"success"},"failed":{"name":"Failed","category":"completed","outcome":"failure"},"in_progress":{"name":"In progress","category":"started"},"todo":{"name":"Todo","category":"unstarted"}},"workflows":{"issue":{"statuses":["todo","in_progress","done","failed","canceled","backlog"],"initial":"todo","slots":{"cancel":"canceled","claim":"in_progress","close":"done","defer":"backlog","fail":"failed","reopen":"todo"},"transitions":[{"from":["todo"],"to":"in_progress","guards":[{"kind":"not_template"},{"kind":"no_open_blockers"},{"kind":"permission"}]},{"to":"done","guards":[{"kind":"not_template"},{"kind":"no_open_children"},{"kind":"no_open_blockers"},{"kind":"permission"}]},{"to":"failed","guards":[{"kind":"no_open_children"},{"kind":"permission"}]},{"to":"canceled","guards":[{"kind":"permission"}]},{"to":"todo","guards":[{"kind":"permission"}]},{"to":"backlog","guards":[{"kind":"permission"}]}]}},"types":{"issue":{"name":"Issue","workflow":"issue","fields":{"description":{"type":"string"},"labels":{"type":"strings"},"title":{"type":"string"}}}}}`

func TestB26BuiltinDerivedGoldenRetainsLegacyAndAddsOnlyBlocked(t *testing.T) {
	row := func(table driver.DerivedTable, key, value string) derivedImage {
		return derivedImage{Table: table, Key: key, Value: json.RawMessage(value)}
	}
	var rows []derivedImage
	for _, id := range []string{idA, idB, idC} {
		self := base64.RawURLEncoding.EncodeToString([]byte(id))
		ready, links := `{"blocked":false,"ready":false}`, `[]`
		if id == idA {
			ready = `{"blocked":false,"ready":true}`
		}
		if id == idB {
			ready = `{"blocked":true,"ready":false}`
			// Natural is a JSON integer even though its Go backing type is string.
			links = fmt.Sprintf(`[{"to":{"id":%q,"branch":0},"kind":"blocks"}]`, idA)
		}
		rows = append(rows,
			row(driver.DerivedAncestorClosure, self+"."+self, `0`),
			row(driver.DerivedCutClosure, id, `[]`),
			row(driver.DerivedAttribute, id, `{}`),
			row(driver.DerivedReadyState, id, ready),
			row(driver.DerivedReferenceIndex, id, links),
			row(driver.DerivedSortKey, id, fmt.Sprintf(`{"id":%q,"priority":2}`, id)),
		)
	}
	rows = append(rows, row(driver.DerivedOntology, "builtin", legacySupportedOntologyJSON), row(driver.DerivedPlugins, "builtin", `[]`))
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Table != rows[j].Table {
			return rows[i].Table < rows[j].Table
		}
		return rows[i].Key < rows[j].Key
	})
	for _, current := range []bool{false, true} {
		wantDigest := legacySupportedDerived
		if current {
			wantDigest = supportedDerived
			for i := range rows {
				if rows[i].Table == driver.DerivedOntology {
					rows[i].Value = json.RawMessage(strings.Replace(legacySupportedOntologyJSON, `"fields":{"description"`, `"fields":{"blocked":{"type":"boolean"},"description"`, 1))
				}
			}
		}
		image := struct {
			Profile string                `json:"profile"`
			Tables  []driver.DerivedTable `json:"tables"`
			Rows    []derivedImage        `json:"rows"`
		}{"builtin-head/1", []driver.DerivedTable{"ancestor_closure", "cut_closure", "derived_attribute", "grant", "ontology", "plugins", "ready_state", "reference_index", "sort_key"}, rows}
		data, err := json.Marshal(image)
		if err != nil {
			t.Fatal(err)
		}
		if got := digestBytes(data); got != wantDigest {
			t.Fatalf("independent complete image current=%v: got %s want %s; bytes=%s", current, got, wantDigest, data)
		}
		if current {
			archive, err := dumpformat.Read(bytes.NewReader(supportedBytes(t, nil)))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := buildPlan(archive)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.derived) != 20 || plan.derivedDigest != wantDigest || !reflect.DeepEqual(plan.derived, rows) {
				t.Fatal("restore differs from the independently fixed 20-row current image")
			}
		}
	}
}
