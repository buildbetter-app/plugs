package offlinehistory_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var actor = model.PrincipalRef{Kind: "user", ID: "restore-admin"}

func profile(mode driver.RestoreHistoryMode) driver.RestoreImportProfile {
	return driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: actor, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: "history-fixture", Method: "offline.restore.import", IdempotencyKey: "history-three-engine-fixture", ArchiveDigest: strings.Repeat("b", 64), DerivedDigest: strings.Repeat("c", 64), HistoryMode: mode}
}
func record(title string) driver.Record {
	return driver.Record{Ref: model.RefByID("pl-a", model.Natural64(0)), Kind: "bead", Body: []byte(`{"title":"` + title + `","metadata":{"beads.agent_state":"opaque","beads.await_id":"await","null":null,"wide":9007199254740993}}`)}
}
func payload(partial bool) driver.OfflineHistory {
	a, b := record("before"), record("after")
	h := driver.OfflineHistory{Source: "fixture-event-feed", Verified: true, Complete: true, First: 1, Head: 2, Batches: []driver.SourceHistoryBatch{{Sequence: 1, EventIDs: []string{"event:1"}, CommitIDs: []string{"dolt-fixture"}, Changes: []driver.SourceHistoryChange{{After: &a}}}, {Sequence: 2, EventIDs: []string{"event:2"}, Changes: []driver.SourceHistoryChange{{Before: &a, After: &b}}}}}
	if partial {
		h.Complete = false
		h.First = 9
		h.Head = 9
		h.BaselineVerified = true
		h.Baseline = []driver.Record{a}
		h.Gaps = []driver.SourceHistoryGap{{First: 1, Last: 8, Reason: "unavailable"}}
		h.Batches = h.Batches[1:]
		h.Batches[0].Sequence = 9
		h.Batches[0].EventIDs = []string{"event:9"}
	}
	return h
}
func locks() driver.LockSet {
	ref := record("after").Ref
	marker := model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))
	sent, _ := driver.NewBeadSentinel(ref.ID)
	ls := driver.LockSet{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}, {Key: driver.LockKey{Kind: driver.LockRecord, Ref: &marker}, Mode: driver.LockUpdate}, {Key: driver.LockKey{Kind: driver.LockSentinel, Sentinel: sent}, Mode: driver.LockUpdate}}
	sort.Slice(ls, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(ls[i].Key)
		b, _ := driver.EncodeLockKey(ls[j].Key)
		return a < b
	})
	return ls
}
func begin(t *testing.T, s driver.Store) (driver.WriteTx, driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	l, e := s.Lease(ctx, "offline-history", driver.LeaseTTL)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = l.Release(context.Background()) })
	tx, e := s.Write(ctx, l)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = tx.Close() })
	if _, e = tx.Lock(ctx, locks()); e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Head(ctx); e != nil {
		t.Fatal(e)
	}
	return tx, l
}
func header(p driver.RestoreImportProfile, m driver.Meta, when time.Time) driver.ChangesetHeader {
	hash, _ := p.RequestHash()
	keys := []driver.LockKey{}
	for _, r := range locks() {
		keys = append(keys, r.Key)
	}
	return driver.ChangesetHeader{Incarnation: m.Incarnation, Kind: model.ChangesetRestore, Time: when, Principal: actor, Chain: []string{}, Method: p.Method, IdempotencyKey: p.IdempotencyKey, RequestHash: hash, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: record("after").Ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{""}}}, Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}}
}
func stage(t *testing.T, tx driver.WriteTx) {
	t.Helper()
	ctx := context.Background()
	if e := tx.Put(ctx, record("after")); e != nil {
		t.Fatal(e)
	}
	sent, _ := driver.NewBeadSentinel("pl-a")
	if e := tx.BumpSentinels(ctx, []driver.Sentinel{sent}); e != nil {
		t.Fatal(e)
	}
}
func TestAtomicOfflineHistoryRoundTripAllDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, partial := range []bool{false, true} {
			name := "full"
			mode := driver.RestoreHistoryModeFull
			if partial {
				name = "partial"
				mode = driver.RestoreHistoryModePartial
			}
			t.Run(engine+"/"+name, func(t *testing.T) {
				ctx := context.Background()
				s := openStore(t, engine)
				old, _ := s.Meta(ctx)
				tx, _ := begin(t, s)
				port, ok := tx.(driver.OfflineHistoryTx)
				if !ok {
					t.Fatal("driver lacks guarded atomic replay capability")
				}
				p := profile(mode)
				when := time.Now().UTC()
				h := payload(partial)
				m, e := port.PrepareOfflineHistory(ctx, p, h, when)
				if e != nil {
					t.Fatal(e)
				}
				unchanged, _ := s.Meta(ctx)
				if !reflect.DeepEqual(old, unchanged) {
					t.Fatal("prepare published metadata")
				}
				stage(t, tx)
				if _, e = tx.Commit(ctx, header(p, m, when)); e == nil {
					t.Fatal("ordinary Commit flattened prepared history")
				}
				entry, e := port.CommitOfflineHistory(ctx, header(p, m, when))
				if e != nil {
					t.Fatal(e)
				}
				m2, _ := s.Meta(ctx)
				if m2.Incarnation == old.Incarnation || m2.Head.String() != "3" || entry.Revision.String() != "3" {
					t.Fatalf("meta=%+v entry=%+v", m2, entry)
				}
				if partial && m2.HistoryFloor.String() != "1" || !partial && !m2.HistoryFloor.IsZero() {
					t.Fatalf("floor=%v", m2.HistoryFloor)
				}
				for revision, want := range map[uint64]string{1: "before", 2: "after", 3: "after"} {
					at := model.Natural64(revision)
					r, e := s.Read(ctx, driver.View{Incarnation: m2.Incarnation, At: &at})
					if e != nil {
						t.Fatal(e)
					}
					rows, e := r.Get(ctx, []model.Ref{record("").Ref})
					r.Close()
					if e != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, record(want).Body) {
						t.Fatalf("revision %d rows=%+v err=%v", revision, rows, e)
					}
				}
				r, e := s.Read(ctx, driver.View{Incarnation: m2.Incarnation})
				if e != nil {
					t.Fatal(e)
				}
				defer r.Close()
				versions, e := r.Versions(ctx, record("").Ref, driver.PageReq{Limit: 10})
				if e != nil || len(versions.Items) != 2 || !versions.Items[0].Base || versions.Items[1].Base {
					t.Fatalf("versions=%+v err=%v", versions, e)
				}
				cursor := (*driver.Cursor)(nil)
				if partial {
					var truncated driver.ErrTruncated
					if _, e = r.Changesets(ctx, nil, 10); !errors.As(e, &truncated) {
						t.Fatalf("missing truncation %v", e)
					}
					cursor = &driver.Cursor{Incarnation: m2.Incarnation, Revision: m2.HistoryFloor}
					zero := model.Natural64(0)
					if _, e = s.Read(ctx, driver.View{Incarnation: m2.Incarnation, At: &zero}); !errors.As(e, &truncated) {
						t.Fatalf("below-floor read %v", e)
					}
				}
				entries, e := r.Changesets(ctx, cursor, 10)
				if e != nil {
					t.Fatal(e)
				}
				for _, v := range entries {
					if v.Principal.ID != actor.ID || v.Kind != model.ChangesetRestore || !v.Time.Equal(when) {
						t.Fatal("source identity/time fabricated")
					}
				}
				marker, e := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
				if e != nil || len(marker) != 1 {
					t.Fatal("missing gap/provenance marker")
				}
				var evidence driver.HistoryImportEvidence
				if e = json.Unmarshal(marker[0].Body, &evidence); e != nil {
					t.Fatal(e)
				}
				if partial && len(evidence.Gaps) != 1 || len(evidence.Mapping) == 0 || evidence.Mapping[0].EventIDs[0] == "" {
					t.Fatal("missing exact source mapping")
				}
				key, _ := driver.PrincipalKey(actor)
				receipt, e := r.IdempotencyByKey(ctx, key, "", p.Method, p.IdempotencyKey)
				if e != nil || receipt == nil || receipt.ResponsePending || receipt.Commit.Revision.String() != "3" {
					t.Fatalf("receipt=%+v err=%v", receipt, e)
				}
				var incarnation driver.ErrIncarnationChanged
				if _, e = s.Read(ctx, driver.View{Incarnation: old.Incarnation}); !errors.As(e, &incarnation) {
					t.Fatal("old incarnation survived")
				}
			})
		}
	}
}
func TestOfflineHistoryRollbackAndRefusalAreAtomic(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			s := openStore(t, engine)
			old, _ := s.Meta(ctx)
			tx, _ := begin(t, s)
			port, ok := tx.(driver.OfflineHistoryTx)
			if !ok {
				t.Fatal("missing replay capability")
			}
			h := payload(false)
			h.Verified = false
			if _, e := port.PrepareOfflineHistory(ctx, profile(driver.RestoreHistoryModeFull), h, time.Now()); e == nil {
				t.Fatal("unverified source admitted")
			}
			if _, e := port.PrepareOfflineHistory(ctx, profile(driver.RestoreHistoryModeFull), payload(false), time.Now()); e != nil {
				t.Fatal(e)
			}
			stage(t, tx)
			tx.Rollback(ctx)
			got, _ := s.Meta(ctx)
			if !reflect.DeepEqual(old, got) {
				t.Fatal("rollback changed metadata")
			}
			r, e := s.Read(ctx, driver.View{Incarnation: old.Incarnation})
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close()
			rows, e := r.Query(ctx, driver.Query{Limit: 10})
			if e != nil || len(rows.Items) != 0 {
				t.Fatal("rollback leaked rows")
			}
			entries, e := r.Changesets(ctx, nil, 10)
			if e != nil || len(entries) != 0 {
				t.Fatal("rollback leaked history")
			}
		})
	}
}
