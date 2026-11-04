package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

// TestNativeRestoreReopenDetachUsesCertifiedHierarchyCoverage is the causal
// reproduction for the large restored-detach failure. The target is restored
// through the product native archive manager, physically reopened, and moved through
// the real Writer while its complete-graph Query path is counted.
func TestNativeRestoreReopenDetachUsesCertifiedHierarchyCoverage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "native-detach.db")
	s, lease, closeTarget := openRenewingTarget(t, ctx, path)

	records, closure, direct := nativeDetachCorpus(t, 20000)
	profile := driver.NativeRestoreProfile{
		StoreID:        "native-detach",
		Principal:      model.PrincipalRef{Kind: "user", ID: "operator"},
		Method:         "admin.restore.finish",
		IdempotencyKey: "00000000000000000000000000000001",
		RequestHash:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	if err := publishNativeDetachCorpus(ctx, t, s, lease, profile, records, closure); err != nil {
		t.Fatal(err)
	}
	closeTarget()

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
	closeReopenedLease := renewLease(t, "native-detach reopened", reopenedLease)
	defer closeReopenedLease()
	meta, err := reopened.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := sourceindex.SQLite.ReadCoverage(ctx, reopened.db, meta.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	var hierarchyRows, linkRows int
	if err = reopened.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM hierarchy_source_current WHERE incarnation=?", meta.Incarnation).Scan(&hierarchyRows); err != nil {
		t.Fatal(err)
	}
	if err = reopened.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM graph_link_source_current WHERE incarnation=?", meta.Incarnation).Scan(&linkRows); err != nil {
		t.Fatal(err)
	}
	t.Logf("reopened native source state: coverage=%+v hierarchy_rows=%d link_rows=%d", coverage, hierarchyRows, linkRows)

	observed := &restoreObservedStore{Store: reopened}
	writer := pipeline.New(observed, reopenedLease, pipeline.WithOntology(service.NewOntology(nil)))
	_, err = writer.Apply(ctx, model.PrincipalRef{Kind: "user", ID: "operator"}, model.ChangeSet{Ops: []model.Op{{
		Kind:     model.OpReparent,
		Reparent: &model.ReparentOp{Target: direct[0]},
	}}}, model.WriteMeta{Project: "local", Method: "tx.apply", AgentLabel: "native-detach", IdempotencyKey: "00000000000000000000000000000002"})
	if err != nil {
		t.Logf("native restore/reopen detach failed before commit: err=%v queries=%d parent_reads=%d inverse_reads=%d", err, observed.queries, observed.parentReads, observed.inverseReads)
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

func openRenewingTarget(t *testing.T, ctx context.Context, path string) (*Store, driver.ActiveLease, func()) {
	t.Helper()
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	closeLease := renewLease(t, "native-detach target", lease)
	var once sync.Once
	closeTarget := func() {
		once.Do(func() {
			closeLease()
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(closeTarget)
	return s, lease, closeTarget
}

func renewLease(t *testing.T, label string, lease driver.ActiveLease) func() {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	var renewalErr error
	renewals := 0
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := lease.Renew(ctx); err != nil {
					if ctx.Err() == nil || !errors.Is(err, context.Canceled) {
						renewalErr = err
					}
					return
				}
				renewals++
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			stop()
			<-done
			if renewalErr != nil {
				t.Errorf("%s lease renewal: %v", label, renewalErr)
			}
			t.Logf("%s lease renewals=%d error=%v", label, renewals, renewalErr)
			if err := lease.Release(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
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

// Use the product restore path: typed driver publication alone certifies only
// membership, not semantic graph validity. This helper creates input bytes; it
// does not backfill either source indexes or the semantic certificate.
func publishNativeDetachCorpus(ctx context.Context, t *testing.T, s *Store, lease driver.ActiveLease, profile driver.NativeRestoreProfile, records []driver.Record, closure []driver.DerivedRow) error {
	t.Helper()
	source, err := memory.NewWithOptions(memory.Options{StoreID: profile.StoreID})
	if err != nil {
		return err
	}
	defer source.Close()
	sourceLease, err := source.Lease(ctx, "native-detach-source", driver.LeaseTTL)
	if err != nil {
		return err
	}
	closeSourceLease := renewLease(t, "native-detach source", sourceLease)
	defer closeSourceLease()
	changes := make([]conformance.FixtureChange, len(records))
	for i, record := range records {
		changes[i] = conformance.FixtureChange{Ref: record.Ref, Kind: record.Kind, Op: model.OpCreate, Body: record.Body}
	}
	if _, err := conformance.Seed(ctx, source, sourceLease, driver.ChangesetHeader{}, changes); err != nil {
		return err
	}
	data, _, err := restore.Dump(ctx, source, "head")
	if err != nil {
		return err
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		return err
	}
	manifestBytes, _ := archive.Member("manifest.json")
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		return err
	}
	digest := func(data []byte) string {
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	}
	presence := restore.Presence{"mode": "presence_check", "challenge": "native-detach", "proof": "native-detach"}
	manager := restore.NewManager(s, lease, restore.PresenceVerifierFunc(func(context.Context, string, string, restore.Presence) error { return nil }))
	status, err := manager.Begin(ctx, manifest, "head", presence)
	if err != nil {
		return err
	}
	for page, offset := uint64(0), 0; offset < len(data); page++ {
		end := min(offset+(4<<20), len(data))
		chunk := data[offset:end]
		if _, err := manager.Chunk(ctx, status.RestoreID, page, chunk, digest(chunk), presence); err != nil {
			return err
		}
		offset = end
	}
	status, err = manager.Finish(ctx, status.RestoreID, digest(canonical), profile.Principal, model.WriteMeta{Method: profile.Method, IdempotencyKey: profile.IdempotencyKey}, presence)
	if err != nil {
		return err
	}
	if status.State != "complete" {
		return fmt.Errorf("native detach restore state = %s", status.State)
	}
	meta, err := s.Meta(ctx)
	if err != nil {
		return err
	}
	r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return err
	}
	defer r.Close()
	got, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		return err
	}
	if len(got) != len(closure) {
		return fmt.Errorf("restored closure rows = %d, want %d", len(got), len(closure))
	}
	for i, row := range got {
		if row.Key != closure[i].Key || !bytes.Equal(row.Value, closure[i].Value) {
			return fmt.Errorf("restored closure row %d differs from canonical corpus", i)
		}
	}
	return nil
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
