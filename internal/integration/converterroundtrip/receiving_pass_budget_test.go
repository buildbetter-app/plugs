package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The ledger observes original source reads and retained Reader.Blob reads.
// It is deliberately outside the planner, preparation and budget observer.
type receivingPartRead struct {
	Phase, Boundary, Name, SHA256 string
	Bytes                         int
	EOF, Closed                   bool
}
type receivingTraversalLedger struct {
	t          *testing.T
	files      map[string][]byte
	phase      string
	reads      []*receivingPartRead
	controls   map[string]int
	partByHash map[driver.Hash]string
	partNames  []string
}

func receivingPublicFiles(t *testing.T) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/public-native-delta-small-0e4d5da4.tar.gz")
	if err != nil || hashBytes(raw) != "66e2a1410cb6de0532e8bd453f06353d3fde8159ed9d6ffe4b0ed17f20bf6ef8" {
		t.Fatal("frozen public capture", err)
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	files := map[string][]byte{}
	r := tar.NewReader(z)
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag != tar.TypeReg || !strings.HasPrefix(h.Name, "capture-set/") {
			continue
		}
		name := strings.TrimPrefix(h.Name, "capture-set/")
		if _, exists := files[name]; exists {
			t.Fatal("duplicate fixture file", name)
		}
		files[name], err = io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(files) != 3 {
		t.Fatal("original source file census changed")
	}
	return files
}

func newReceivingLedger(t *testing.T) *receivingTraversalLedger {
	l := &receivingTraversalLedger{t: t, files: receivingPublicFiles(t), phase: "admission", controls: map[string]int{}, partByHash: map[driver.Hash]string{}}
	for name, raw := range l.files {
		if strings.HasPrefix(name, "parts/") {
			l.partNames = append(l.partNames, name)
			l.partByHash[driver.Hash(hashBytes(raw))] = name
		}
	}
	sort.Strings(l.partNames)
	return l
}

type receivingCountedPart struct {
	r     io.Reader
	h     hash.Hash
	event *receivingPartRead
}

func (r *receivingCountedPart) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.event.Bytes += n
	_, _ = r.h.Write(p[:n])
	if err == io.EOF {
		r.event.EOF = true
	}
	return n, err
}
func (r *receivingCountedPart) Close() error {
	r.event.Closed = true
	r.event.SHA256 = hex.EncodeToString(r.h.Sum(nil))
	return nil
}
func (l *receivingTraversalLedger) open(_ context.Context, n uint64) (io.ReadCloser, error) {
	name := fmt.Sprintf("parts/%020d.part", n)
	raw, ok := l.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	event := &receivingPartRead{Phase: l.phase, Boundary: "source opener", Name: name}
	l.reads = append(l.reads, event)
	return &receivingCountedPart{r: bytes.NewReader(raw), h: sha256.New(), event: event}, nil
}
func (l *receivingTraversalLedger) archive() *dumpformat.Archive {
	l.t.Helper()
	a, err := dumpformat.ReadNativeCaptureSet(context.Background(), bytes.NewReader(l.files["capture-set.json"]), bytes.NewReader(l.files["head.tar"]), l.open, dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		l.t.Fatal("actual admission", err)
	}
	if a.SHA256() != "fd910c93ebad8da226c7ff6cc84f49c46f87aafe2c3530a4a27bf48280e4c67d" {
		l.t.Fatal("source identity changed")
	}
	return a
}
func (l *receivingTraversalLedger) assertTraversals(start, cap int, exact bool) {
	l.t.Helper()
	reads := l.reads[start:]
	if len(reads)%len(l.partNames) != 0 {
		l.t.Fatalf("incomplete ordered traversal: %+v", reads)
	}
	for i, r := range reads {
		name := l.partNames[i%len(l.partNames)]
		if r.Name != name || !r.EOF || !r.Closed || r.Bytes != len(l.files[name]) || r.SHA256 != hashBytes(l.files[name]) {
			l.t.Fatalf("actual bytes/EOF/close/order differ: %+v", r)
		}
	}
	count := len(reads) / len(l.partNames)
	if count > cap || exact && count != cap {
		l.t.Fatalf("%s traversals=%d cap=%d exact=%t", l.phase, count, cap, exact)
	}
	raw, _ := json.Marshal(reads)
	l.t.Logf("actual_receiving_traversals count=%d cap=%d reads=%s controls=%v", count, cap, raw, l.controls)
}

type receivingCountedStore struct {
	driver.Store
	driver.WorkflowCheckpointStore
	l                 *receivingTraversalLedger
	fault             string
	interrupt         bool
	writes, snapshots int
	recoveryStart     int
}

func wrapReceivingStore(t *testing.T, store driver.Store, l *receivingTraversalLedger) *receivingCountedStore {
	t.Helper()
	capability, ok := store.(driver.WorkflowCheckpointStore)
	if !ok {
		t.Fatal("real checkpoint capability missing")
	}
	return &receivingCountedStore{Store: store, WorkflowCheckpointStore: capability, l: l}
}
func (s *receivingCountedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}
func (s *receivingCountedStore) Persistent() bool {
	capability, ok := s.Store.(driver.PersistentCheckpointStore)
	return ok && capability.Persistent()
}
func (s *receivingCountedStore) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, v)
	if err != nil {
		return nil, err
	}
	return &receivingCountedReader{Reader: r, s: s}, nil
}
func (s *receivingCountedStore) ReadSourcePlaneSnapshot(ctx context.Context, v driver.View, keys []string) (driver.SourcePlaneSnapshot, error) {
	s.snapshots++
	if s.snapshots == 1 {
		s.recoveryStart = len(s.l.reads)
		s.l.controls["automatic durable receipt recovery"]++
	}
	if s.interrupt {
		s.l.controls["interrupted after durable commit"]++
		return nil, fmt.Errorf("interrupted after durable commit")
	}
	r, err := s.WorkflowCheckpointStore.ReadSourcePlaneSnapshot(ctx, v, keys)
	if err != nil {
		return nil, err
	}
	return &receivingCountedSnapshot{receivingCountedReader: &receivingCountedReader{Reader: r, s: s}, snapshot: r}, nil
}

type receivingCountedReader struct {
	driver.Reader
	s *receivingCountedStore
}

func (r *receivingCountedReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		if row.Ref.ID != "restore/archive" || r.s.fault != "current-limit" && r.s.fault != "current-ledger" {
			continue
		}
		var evidence archiveEvidence
		if err := json.Unmarshal(row.Body, &evidence); err != nil {
			return nil, err
		}
		if r.s.fault == "current-limit" {
			evidence.NativeDecodedLimitBytes = 2147483649
		} else {
			file := evidence.NativeCaptureFiles["head.tar"]
			file.Size++
			evidence.NativeCaptureFiles["head.tar"] = file
		}
		rows = append([]driver.Record(nil), rows...)
		rows[i].Body, err = json.Marshal(evidence)
		r.s.l.controls[r.s.fault]++
		return rows, err
	}
	return rows, nil
}

func (r *receivingCountedReader) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	return r.Reader.(driver.AllocatorReader).LoadAllocator(ctx, prefix)
}
func (r *receivingCountedReader) Blob(ctx context.Context, h driver.Hash) ([]byte, error) {
	raw, err := r.Reader.Blob(ctx, h)
	if err != nil {
		return nil, err
	}
	if name, ok := r.s.l.partByHash[h]; ok {
		// Blob is an atomic complete-value boundary. The real retained opener
		// subsequently parses these exact bytes through authenticated EOF.
		r.s.l.reads = append(r.s.l.reads, &receivingPartRead{Phase: r.s.l.phase, Boundary: "retained Blob complete value", Name: name, SHA256: hashBytes(raw), Bytes: len(raw), EOF: true, Closed: true})
		if r.s.fault == "changed-source-part" {
			r.s.l.controls[r.s.fault]++
			raw = bytes.Clone(raw)
			raw[len(raw)-1] ^= 1
		}
	}
	return raw, nil
}
func (r *receivingCountedReader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	receipt, err := r.Reader.IdempotencyByKey(ctx, principal, agent, method, key)
	if err == nil && receipt != nil && r.s.fault == "wrong-receipt" {
		r.s.l.controls[r.s.fault]++
		copy := *receipt
		copy.RequestHash = strings.Repeat("f", 64)
		receipt = &copy
	}
	return receipt, err
}
func (r *receivingCountedReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, q)
	if err != nil {
		return page, err
	}
	fault := r.s.fault
	for i, record := range page.Items {
		current := strings.HasPrefix(record.Ref.ID, "source/native-current/")
		history := strings.HasPrefix(record.Ref.ID, driver.NativeTableHistoryRecordPrefix)
		if !(strings.HasSuffix(fault, "current") && current || strings.HasSuffix(fault, "history") && history) {
			continue
		}
		r.s.l.controls[fault]++
		page.Items = append([]driver.Record(nil), page.Items...)
		switch {
		case strings.HasPrefix(fault, "missing-"):
			page.Items = append(page.Items[:i], page.Items[i+1:]...)
		case strings.HasPrefix(fault, "extra-"):
			page.Items = append(page.Items, record)
		case strings.HasPrefix(fault, "foreign-"):
			var body map[string]any
			if err := json.Unmarshal(record.Body, &body); err != nil {
				return page, err
			}
			body["source_identity"] = map[string]any{"store_id": "foreign-store", "project": "foreign"}
			page.Items[i].Body, err = json.Marshal(body)
		}
		return page, err
	}
	return page, nil
}

type receivingCountedSnapshot struct {
	*receivingCountedReader
	snapshot driver.SourcePlaneSnapshot
}

func (r *receivingCountedSnapshot) SourcePlaneCheckpoint(key string) (driver.Checkpoint, error) {
	cp, err := r.snapshot.SourcePlaneCheckpoint(key)
	if err == nil && r.s.fault == "wrong-origin" {
		r.s.l.controls[r.s.fault]++
		cp.Payload = bytes.Clone(cp.Payload)
		if len(cp.Payload) > 0 {
			cp.Payload[len(cp.Payload)/2] ^= 1
		}
	}
	return cp, err
}

func receivingPublicProfile(t *testing.T, a *dumpformat.Archive, s driver.Store, lease driver.ActiveLease) driver.RestoreImportProfile {
	t.Helper()
	return receivingPublicProfileMode(t, a, s, lease, driver.RestoreHistoryModeFull, nil)
}

func receivingPublicProfileMode(t *testing.T, a *dumpformat.Archive, s driver.Store, lease driver.ActiveLease, mode driver.RestoreHistoryMode, selection *driver.NativeDeltaHistoryPolicy) driver.RestoreImportProfile {
	t.Helper()
	plan := func() driver.SourcePlanePlanV1 {
		work, release := a.NativeDeltaAccountingScope()
		defer release()
		return nativeHistoryPlanFromArchive(t, work, s, lease)
	}()
	plan.Dispositions[0].StorageClass = ""
	plan.Dispositions[0].Statuses = nil
	for _, project := range a.Manifest().Projects {
		for _, status := range project.Statuses {
			plan.Dispositions[0].Statuses = append(plan.Dispositions[0].Statuses, driver.SourcePlaneStringPairV1{From: status.Name, To: status.Name})
		}
	}
	sort.Slice(plan.Dispositions[0].Statuses, func(i, j int) bool {
		return plan.Dispositions[0].Statuses[i].From < plan.Dispositions[0].Statuses[j].From
	})
	p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(a.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: a.SHA256(), HistoryMode: mode, HistoryTime: nativeFixtureTime.Format("2006-01-02T15:04:05.999999999Z07:00"), NativeDeltaHistory: selection}
	bound, err := BindNativeCaptureSetImport(a.WithNativeDeltaAccounting(context.Background()), a, p, plan)
	if err != nil {
		t.Fatal("actual bind", err)
	}
	return bound
}

func TestReceivingActualCleanNoopResumedAndHarnessTraversalCaps(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			l := newReceivingLedger(t)
			a := l.archive()
			defer a.ReleaseNativeDecoded()
			sourceBaseline, _ := a.NativeDecodedUsage()
			store, lease, reopen := openIdentityTarget(t, engine)
			s := wrapReceivingStore(t, store, l)
			if err := lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			op, heldLease, releaseLease, err := HoldRestoreLease(context.Background(), store, "receiving-budget")
			if err != nil {
				t.Fatal(err)
			}
			releaseLease = sync.OnceValue(releaseLease)
			defer func() {
				if err := releaseLease(); err != nil {
					t.Error(err)
				}
			}()
			lease = heldLease
			l.phase = "combined current/history/raw"
			p := receivingPublicProfile(t, a, s, lease)
			// The returned profile stays live through import and recovery. Its
			// two output domains belong in the exact baseline; preparation
			// staging must still disappear when the import returns.
			baseline := receivingUsageWithProfile(t, sourceBaseline, p)
			report, err := RestoreNativeCaptureSetImportWithLease(a.WithNativeDeltaAccounting(op), a, s, lease, p)
			if err != nil {
				t.Fatal("actual receive", err)
			}
			if s.writes != 1 || s.snapshots != 1 || s.recoveryStart == 0 {
				t.Fatal("actual clean/recovery controls not reached", s.writes, s.snapshots)
			}
			l.phase = "automatic no-op recovery"
			l.assertTraversals(s.recoveryStart, 0, true)
			retained := report
			retained.Archive = nil
			retainedWire, marshalErr := json.Marshal(retained)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			retainedBytes := uint64(len(retainedWire) + 1) // admitted JSON includes LF
			baseline.CurrentAccountedDecodedBytes += retainedBytes
			baseline.CurrentCategories["receiving_retained_result"] += retainedBytes
			baseline.CurrentCharges["receiving_retained_result"]++
			held, _ := a.NativeDecodedUsage()
			if held.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(held.CurrentCategories, baseline.CurrentCategories) || !reflect.DeepEqual(held.CurrentCharges, baseline.CurrentCharges) {
				t.Fatal("completed receiving retained staging", baseline, held)
			}
			l.phase = "independent clean readback"
			if err := VerifyNativeCaptureSetReadback(context.Background(), s, a, report); err != nil {
				t.Fatal(err)
			}
			l.assertTraversals(0, 3, true)
			if err := releaseLease(); err != nil {
				t.Fatal(err)
			}
			store, lease = reopen()
			_ = lease
			s = wrapReceivingStore(t, store, l)
			start := len(l.reads)
			l.phase = "resumed recovery after real reopen"
			recoveryCtx := a.WithNativeDeltaAccounting(context.Background())
			_, recovered, found, err := RecoverBoundSourceImport(recoveryCtx, s)
			if err != nil || !found || recovered.Archive == nil || !recovered.Recovered {
				t.Fatal("actual resumed recovery", found, err)
			}
			defer recovered.Archive.ReleaseNativeDecoded()
			l.assertTraversals(start, 2, false)
			if s.writes != 0 {
				t.Fatal("recovery replayed writes")
			}
			if recovered.RequestHash != report.RequestHash || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.NativeFamilies, report.NativeFamilies) || !reflect.DeepEqual(recovered.NativeSchemas, report.NativeSchemas) || !reflect.DeepEqual(recovered.NativeHistories, report.NativeHistories) {
				t.Fatal("recovery changed original receipt/report bytes")
			}
			for i := 0; i < 2; i++ {
				start = len(l.reads)
				l.phase = fmt.Sprintf("harness reopen check %d", i)
				_, again, found, err := RecoverBoundSourceImport(recoveryCtx, s)
				if err != nil || !found || again.Archive == nil {
					t.Fatal("harness check", err)
				}
				again.Archive.ReleaseNativeDecoded()
				l.assertTraversals(start, 1, true)
			}
			start = len(l.reads)
			l.phase = "reopened independent readback"
			if err := VerifyNativeCaptureSetReadback(context.Background(), s, a, recovered); err != nil {
				t.Fatal(err)
			}
			l.assertTraversals(start, 1, true)
		})
	}
}

func TestReceivingActualInterruptedRecoveryAndReachedRefusals(t *testing.T) {
	l := newReceivingLedger(t)
	a := l.archive()
	defer a.ReleaseNativeDecoded()
	store, lease, reopen := openIdentityTarget(t, "sqlite")
	ctx := a.WithNativeDeltaAccounting(context.Background())
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	op, lease, releaseLease, err := HoldRestoreLease(ctx, store, "receiving-interruption")
	if err != nil {
		t.Fatal(err)
	}
	releaseLease = sync.OnceValue(releaseLease)
	defer func() {
		if err := releaseLease(); err != nil {
			t.Error(err)
		}
	}()
	s := wrapReceivingStore(t, store, l)
	p := receivingPublicProfile(t, a, s, lease)
	s.interrupt = true
	if _, err := RestoreNativeCaptureSetImportWithLease(op, a, s, lease, p); err == nil || l.controls["interrupted after durable commit"] != 1 {
		t.Fatal("durable interruption control not reached", err)
	}
	meta, err := store.Meta(context.Background())
	if err != nil || meta.Head.IsZero() {
		t.Fatal("interruption was not after genuine publication", err)
	}
	if err := releaseLease(); err != nil {
		t.Fatal(err)
	}
	store, lease = reopen()
	_ = lease
	s = wrapReceivingStore(t, store, l)
	start := len(l.reads)
	l.phase = "interrupted resumed recovery"
	_, report, found, err := RecoverBoundSourceImport(ctx, s)
	if err != nil || !found {
		t.Fatal("resumed durable import", err)
	}
	defer report.Archive.ReleaseNativeDecoded()
	l.assertTraversals(start, 2, false)
	if s.writes != 0 {
		t.Fatal("interrupted recovery replayed writes")
	}
	for _, fault := range []string{"wrong-receipt", "wrong-origin", "changed-source-part", "current-limit", "current-ledger"} {
		t.Run(fault, func(t *testing.T) {
			before := receivingConservation(t, store, p)
			s.fault = fault
			start := len(l.reads)
			usageBefore, _ := a.NativeDecodedUsage()
			_, bad, _, err := RecoverBoundSourceImport(ctx, s)
			if bad.Archive != nil {
				bad.Archive.ReleaseNativeDecoded()
			}
			if err == nil || l.controls[fault] == 0 {
				t.Fatal("actual refusal/control missing", fault, err)
			}
			if fault == "current-limit" || fault == "current-ledger" {
				if len(l.reads) != start {
					t.Fatal("mutable evidence allocated/read source before receipt equality")
				}
			}
			s.fault = ""
			usageAfter, _ := a.NativeDecodedUsage()
			if usageAfter.CurrentAccountedDecodedBytes != usageBefore.CurrentAccountedDecodedBytes || !reflect.DeepEqual(usageAfter.CurrentCategories, usageBefore.CurrentCategories) || !reflect.DeepEqual(usageAfter.CurrentCharges, usageBefore.CurrentCharges) {
				t.Fatal("refused recovery leaked live bytes from actual shared budget")
			}
			if !reflect.DeepEqual(before, receivingConservation(t, store, p)) || s.writes != 0 {
				t.Fatal("refused recovery changed target/receipt/checkpoint/allocator/blob")
			}
			t.Logf("reached_receiving_refusal control=%s writes=0 shared_budget=conserved target_receipt_checkpoint_allocator_blob=conserved", fault)
		})
	}
	wrong, _ := p.Detached()
	wrong.HistoryTime = nativeFixtureTime.Add(1).Format("2006-01-02T15:04:05.999999999Z07:00")
	if _, err := RecoverImport(ctx, s, wrong, Verification{ArchiveSHA256: p.ArchiveDigest, DerivedSHA256: p.DerivedDigest}); !errors.Is(err, driver.ErrConflict) {
		t.Fatal("wrong full profile accepted", err)
	}
	for _, fault := range []string{"missing-current", "extra-current", "foreign-current", "missing-history", "extra-history", "foreign-history"} {
		t.Run(fault, func(t *testing.T) {
			s.fault = fault
			if err := VerifyNativeCaptureSetReadback(context.Background(), s, a, report); err == nil || l.controls[fault] == 0 {
				t.Fatal("actual readback refusal/control missing", fault, err)
			}
			s.fault = ""
		})
	}
}

type receivingConservationState struct {
	Meta        driver.Meta
	View        driver.View
	Records     []driver.Record
	Receipt     *driver.IdempotencyRecord
	Checkpoints []driver.Checkpoint
	Allocators  driver.AllocatorSnapshot
	Blobs       map[string][]byte
}

func receivingConservation(t *testing.T, store driver.Store, profile driver.RestoreImportProfile) receivingConservationState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.(driver.CheckpointInventoryStore).ReadCheckpointInventory(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	state := receivingConservationState{Meta: meta, View: r.Snapshot(), Blobs: map[string][]byte{}}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Cursor: cursor, Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		state.Records = append(state.Records, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
			t.Fatal("incomplete conservation census")
		}
		cursor = page.NextCursor
	}
	keys, err := r.CheckpointKeys()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		cp, err := r.SourcePlaneCheckpoint(key)
		if err != nil {
			t.Fatal(err)
		}
		state.Checkpoints = append(state.Checkpoints, cp)
	}
	state.Allocators, err = r.(driver.AllocatorSnapshotReader).AllocatorSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := driver.PrincipalKey(profile.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	state.Receipt, err = r.IdempotencyByKey(ctx, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
	if err != nil || state.Receipt == nil {
		t.Fatal("conservation original receipt missing", err)
	}
	var evidence archiveEvidence
	for _, row := range state.Records {
		if row.Ref.ID == "restore/archive" {
			if err := json.Unmarshal(row.Body, &evidence); err != nil {
				t.Fatal(err)
			}
		}
	}
	hashes := map[string]bool{evidence.ArchiveSHA256: true}
	for _, file := range evidence.Members {
		hashes[file.SHA256] = true
	}
	for _, file := range evidence.NativeCaptureFiles {
		hashes[file.SHA256] = true
	}
	for hash := range hashes {
		raw, err := r.Blob(ctx, driver.Hash(hash))
		if err != nil || hashBytes(raw) != hash {
			t.Fatal("conservation blob census differs", err)
		}
		state.Blobs[hash] = raw
	}
	return state
}
