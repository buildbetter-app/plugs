package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// TestNativeRestoreReopenDetachUsesCertifiedHierarchyCoverage is the causal
// reproduction for the large restored-detach failure. The target is restored
// through the native transaction, physically reopened, and then moved through
// the real Writer while its complete-graph Query path is counted.
func TestNativeRestoreReopenDetachUsesCertifiedHierarchyCoverage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "native-detach.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)

	records, closure, direct := nativeDetachCorpus(t, 20000)
	profile := driver.NativeRestoreProfile{
		StoreID:        "native-detach",
		Principal:      model.PrincipalRef{Kind: "user", ID: "operator"},
		Method:         "admin.restore.finish",
		IdempotencyKey: "native-detach-restore",
		RequestHash:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	if err := publishNativeDetachCorpus(ctx, t, s, lease, profile, records, closure); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	value, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	reopened := value.(*Store)
	defer reopened.Close()
	reopenedLease, err := reopened.Lease(ctx, "native-detach-reopen", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedLease.Release(context.Background())

	observed := &restoreObservedStore{Store: reopened}
	writer := pipeline.New(observed, reopenedLease, pipeline.WithOntology(service.NewOntology(nil)))
	_, err = writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "operator"}, model.ChangeSet{Ops: []model.Op{{
		Kind:     model.OpReparent,
		Reparent: &model.ReparentOp{Target: direct[0]},
	}}}, model.WriteMeta{Project: "local", Method: "tx.apply", AgentLabel: "native-detach", IdempotencyKey: "native-detach-write"})
	if err != nil {
		t.Fatal(err)
	}
	if observed.queries != 0 {
		t.Fatalf("restored detach used complete graph Query fallback: queries=%d parent_reads=%d inverse_reads=%d", observed.queries, observed.parentReads, observed.inverseReads)
	}
	if observed.parentReads == 0 {
		t.Fatalf("restored detach did not use the hierarchy source index: queries=%d parent_reads=%d inverse_reads=%d", observed.queries, observed.parentReads, observed.inverseReads)
	}
	t.Logf("native 20000-record restore, physical reopen, and detach used indexed hierarchy: queries=%d parent_reads=%d inverse_reads=%d", observed.queries, observed.parentReads, observed.inverseReads)
}

func nativeDetachCorpus(t *testing.T, count int) ([]driver.Record, []driver.DerivedRow, []model.Ref) {
	t.Helper()
	parent := model.RefByID("pl-parent", model.Natural64(0))
	direct := make([]model.Ref, 50)
	documents := make([]pipeline.Document, 0, count)
	documents = append(documents, pipeline.Document{ID: parent.ID, Type: "issue", Project: "local", Fields: map[string]any{"status": "todo"}})
	for i := range direct {
		direct[i] = model.RefByID(fmt.Sprintf("pl-direct-%02d", i), model.Natural64(0))
		documents = append(documents, pipeline.Document{ID: direct[i].ID, Type: "issue", Project: "local", Parent: &parent, Fields: map[string]any{"status": "todo"}})
	}
	for i := len(documents); i < count; i++ {
		documents = append(documents, pipeline.Document{ID: fmt.Sprintf("pl-unrelated-%05d", i), Type: "issue", Project: "local", Fields: map[string]any{"status": "todo"}})
	}
	records := make([]driver.Record, len(documents))
	rows := map[string]driver.DerivedRow{}
	for i, doc := range documents {
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		records[i] = driver.Record{Ref: model.RefByID(doc.ID, model.Natural64(0)), Kind: "bead", Body: body}
		for id, depth := doc.ID, uint64(0); id != ""; depth++ {
			row, err := pipeline.AncestorRow(doc.ID, id, depth)
			if err != nil {
				t.Fatal(err)
			}
			rows[row.Key] = row
			if id == parent.ID {
				break
			}
			if doc.Parent == nil {
				break
			}
			id = doc.Parent.ID
		}
	}
	closure := make([]driver.DerivedRow, 0, len(rows))
	for _, row := range rows {
		closure = append(closure, row)
	}
	sort.Slice(closure, func(i, j int) bool { return closure[i].Key < closure[j].Key })
	return records, closure, direct
}

func publishNativeDetachCorpus(ctx context.Context, t *testing.T, s *Store, lease driver.ActiveLease, profile driver.NativeRestoreProfile, records []driver.Record, closure []driver.DerivedRow) error {
	t.Helper()
	snapshot, err := driver.NewAllocatorSnapshot(driver.Meta{StoreID: profile.StoreID, Incarnation: "native-detach-source"}, model.Natural64(1), model.Natural64(0), driver.AllocatorInventory{})
	if err != nil {
		return err
	}
	profile.AllocatorBinding, err = snapshot.Binding()
	if err != nil {
		return err
	}
	fence, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		return err
	}
	keys := []driver.LockKey{fence, driver.GraphBlockingSentinelLock}
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	for _, record := range records {
		ref := record.Ref
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			return err
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
		sentinels = append(sentinels, bead)
		closureSentinel, err := driver.NewClosureSentinel(ref.ID)
		if err != nil {
			return err
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closureSentinel})
		sentinels = append(sentinels, closureSentinel)
	}
	for _, row := range closure {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	txValue, err := s.Write(ctx, lease)
	if err != nil {
		return err
	}
	tx := txValue.(*transaction)
	defer tx.Close()
	if _, err = tx.Lock(ctx, locks); err != nil {
		return err
	}
	if _, err = tx.Head(ctx); err != nil {
		return err
	}
	meta, err := tx.PrepareNativeRestore(ctx, profile)
	if err != nil {
		return err
	}
	if err = tx.RestoreNativeAllocators(ctx, snapshot); err != nil {
		return err
	}
	for _, record := range records {
		if err = pipeline.PutSourceRecord(ctx, tx, record); err != nil {
			return err
		}
	}
	if err = tx.PutDerived(ctx, driver.DerivedAncestorClosure, nil, closure); err != nil {
		return err
	}
	if err = tx.BumpSentinels(ctx, sentinels); err != nil {
		return err
	}
	changes := make([]model.ChangeRecord, len(records))
	for i, record := range records {
		changes[i] = model.ChangeRecord{Ref: record.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0)}
	}
	_, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: profile.Principal, Method: profile.Method, IdempotencyKey: profile.IdempotencyKey, RequestHash: profile.RequestHash, LockKeys: keys, Changes: changes})
	return err
}

type restoreObservedStore struct {
	driver.Store
	queries, parentReads, inverseReads int
}

func (s *restoreObservedStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	reader, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return &restoreObservedReader{Reader: reader, counts: s}, nil
}

func (s *restoreObservedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &restoreObservedTx{WriteTx: tx, counts: s}, nil
}

type restoreObservedReader struct {
	driver.Reader
	counts *restoreObservedStore
}

func (r *restoreObservedReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	if query.Filter["kind"] == "bead" {
		r.counts.queries++
	}
	return r.Reader.Query(ctx, query)
}
func (r *restoreObservedReader) HierarchyChildren(ctx context.Context, refs []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.counts.parentReads++
	return r.Reader.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, refs, limit)
}
func (r *restoreObservedReader) HierarchyChildrenExcluding(ctx context.Context, refs, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.counts.parentReads++
	return r.Reader.(driver.HierarchyChildrenExcludingReader).HierarchyChildrenExcluding(ctx, refs, excluded, limit)
}
func (r *restoreObservedReader) InverseSourceLinks(ctx context.Context, refs []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.counts.inverseReads++
	return r.Reader.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, refs, kinds, excluded, limit)
}
func (r *restoreObservedReader) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	return r.Reader.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, table, prefixes)
}

type restoreObservedTx struct {
	driver.WriteTx
	counts *restoreObservedStore
}

func (t *restoreObservedTx) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	if query.Filter["kind"] == "bead" {
		t.counts.queries++
	}
	return t.WriteTx.Query(ctx, query)
}
func (t *restoreObservedTx) HierarchyChildren(ctx context.Context, refs []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.counts.parentReads++
	return t.WriteTx.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, refs, limit)
}
func (t *restoreObservedTx) HierarchyChildrenExcluding(ctx context.Context, refs, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.counts.parentReads++
	return t.WriteTx.(driver.HierarchyChildrenExcludingReader).HierarchyChildrenExcluding(ctx, refs, excluded, limit)
}
func (t *restoreObservedTx) InverseSourceLinks(ctx context.Context, refs []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	t.counts.inverseReads++
	return t.WriteTx.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, refs, kinds, excluded, limit)
}
func (t *restoreObservedTx) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	return t.WriteTx.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, table, prefixes)
}
func (t *restoreObservedTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	return t.WriteTx.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, parent, links)
}
