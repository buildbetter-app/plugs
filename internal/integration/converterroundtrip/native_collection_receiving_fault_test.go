package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A retained Blob delivers a complete value atomically. These observations
// deliberately do not invent physical EOF/Close events for the driver API.
// External file observations retain their separate byte/EOF/Close ledger.
type joinedRetainedRead struct {
	ordinal          int
	name, phase, sha string
	bytes            uint64
}

func (l *joinedOriginalLedger) assertRetainedTraversals(t *testing.T, start, cap int, exact bool) {
	t.Helper()
	counts := map[string]int{}
	for _, read := range l.retained[start:] {
		var want *dumpformat.NativeCaptureFile
		for i := range l.binding.Sources[read.ordinal].Files {
			file := &l.binding.Sources[read.ordinal].Files[i]
			if file.Name == read.name {
				want = file
				break
			}
		}
		if want == nil || want.Bytes != read.bytes || want.SHA256 != read.sha {
			t.Fatal("retained complete-value original differs", read)
		}
		counts[fmt.Sprintf("%d/%s", read.ordinal, read.name)]++
	}
	for ordinal, source := range l.binding.Sources {
		for _, file := range source.Files {
			n := counts[fmt.Sprintf("%d/%s", ordinal, file.Name)]
			if n > cap || exact && n != cap {
				t.Fatal("retained per-original traversal cap", l.phase, ordinal, file.Name, n, cap)
			}
		}
	}
	t.Logf("JOINED_RETAINED_COMPLETE_VALUE phase=%s cap=%d counts=%v", l.phase, cap, counts)
}

type joinedCountedStore struct {
	*nativeStreamCountingStore
	l         *joinedOriginalLedger
	fault     string
	controls  map[string]int
	snapshots int
	late      func() error
}

func observeJoinedStore(t *testing.T, store driver.Store, l *joinedOriginalLedger) *joinedCountedStore {
	t.Helper()
	// This actual pair has distinct root/head/part hashes. A hash-only Blob
	// API cannot distinguish two equal-file identities, so refuse that fixture
	// ambiguity instead of assigning a fabricated source ordinal.
	seen := map[string]bool{}
	for _, source := range l.binding.Sources {
		for _, file := range source.Files {
			if seen[file.SHA256] {
				t.Fatal("physical observation needs distinct file hashes")
			}
			seen[file.SHA256] = true
		}
	}
	return &joinedCountedStore{nativeStreamCountingStore: nativeStreamObservedStore(t, store), l: l, controls: map[string]int{}}
}

func (s *joinedCountedStore) Read(ctx context.Context, v driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, v)
	if err != nil {
		return nil, err
	}
	return &joinedCountedReader{Reader: r, s: s}, nil
}
func (s *joinedCountedStore) ReadSourcePlaneSnapshot(ctx context.Context, v driver.View, keys []string) (driver.SourcePlaneSnapshot, error) {
	s.snapshots++
	r, err := s.WorkflowCheckpointStore.ReadSourcePlaneSnapshot(ctx, v, keys)
	if err != nil {
		return nil, err
	}
	return &joinedCountedSnapshot{joinedCountedReader: &joinedCountedReader{Reader: r, s: s}, snapshot: r}, nil
}
func (s *joinedCountedStore) LoadCheckpoint(ctx context.Context, key string) (driver.Checkpoint, error) {
	if s.writes > 0 && s.late != nil {
		f := s.late
		s.late = nil
		s.controls["post-history publication"]++
		if err := f(); err != nil {
			return driver.Checkpoint{}, err
		}
	}
	return s.WorkflowCheckpointStore.LoadCheckpoint(ctx, key)
}

type joinedCountedReader struct {
	driver.Reader
	s *joinedCountedStore
}

func (r *joinedCountedReader) LoadAllocator(ctx context.Context, prefix string) (driver.OfflineAllocator, error) {
	a, ok := r.Reader.(driver.AllocatorReader)
	if !ok {
		return driver.OfflineAllocator{}, driver.ErrUnsupported
	}
	return a.LoadAllocator(ctx, prefix)
}
func (r *joinedCountedReader) Blob(ctx context.Context, h driver.Hash) ([]byte, error) {
	raw, err := r.Reader.Blob(ctx, h)
	if err != nil {
		return nil, err
	}
	for ordinal, source := range r.s.l.binding.Sources {
		for _, file := range source.Files {
			if file.SHA256 == string(h) {
				r.s.l.retained = append(r.s.l.retained, joinedRetainedRead{ordinal, file.Name, r.s.l.phase, hashBytes(raw), uint64(len(raw))})
				if r.s.fault == "changed-part" && strings.HasPrefix(file.Name, "parts/") {
					raw = bytes.Clone(raw)
					raw[len(raw)-1] ^= 1
					r.s.controls[r.s.fault]++
				}
			}
		}
	}
	return raw, nil
}
func (r *joinedCountedReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		if row.Ref.ID != "restore/archive" || r.s.fault != "current-ledger" && r.s.fault != "current-limit" {
			continue
		}
		var value archiveEvidence
		if err = json.Unmarshal(row.Body, &value); err != nil {
			return nil, err
		}
		if r.s.fault == "current-limit" {
			value.NativeDecodedLimitBytes = 2<<30 + 1
		} else {
			key := dumpformat.NativeCollectionFileName(0, "head.tar")
			f := value.NativeCaptureFiles[key]
			f.Size++
			value.NativeCaptureFiles[key] = f
		}
		rows = append([]driver.Record(nil), rows...)
		rows[i].Body, err = json.Marshal(value)
		r.s.controls[r.s.fault]++
		return rows, err
	}
	return rows, nil
}
func (r *joinedCountedReader) IdempotencyByKey(ctx context.Context, principal, agent, method, key string) (*driver.IdempotencyRecord, error) {
	value, err := r.Reader.IdempotencyByKey(ctx, principal, agent, method, key)
	if err == nil && value != nil && r.s.fault == "wrong-receipt" {
		v := *value
		v.RequestHash = strings.Repeat("f", 64)
		value = &v
		r.s.controls[r.s.fault]++
	}
	return value, err
}
func (r *joinedCountedReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, q)
	if err != nil {
		return page, err
	}
	if r.s.fault != "missing-native" && r.s.fault != "extra-native" && r.s.fault != "foreign-native" {
		return page, nil
	}
	for i, record := range page.Items {
		if !strings.HasPrefix(record.Ref.ID, driver.NativeTableHistoryRecordPrefix) && !strings.HasPrefix(record.Ref.ID, "source/native-current/") {
			continue
		}
		page.Items = append([]driver.Record(nil), page.Items...)
		r.s.controls[r.s.fault]++
		switch r.s.fault {
		case "missing-native":
			page.Items = append(page.Items[:i], page.Items[i+1:]...)
		case "extra-native":
			page.Items = append(page.Items, record)
		case "foreign-native":
			var body map[string]any
			if err = json.Unmarshal(record.Body, &body); err != nil {
				return page, err
			}
			body["source_identity"] = map[string]any{"store_id": "invented-foreign-store", "project": "invented-foreign-project"}
			page.Items[i].Body, err = json.Marshal(body)
		}
		return page, err
	}
	return page, nil
}

type joinedCountedSnapshot struct {
	*joinedCountedReader
	snapshot driver.SourcePlaneSnapshot
}

func (r *joinedCountedSnapshot) SourcePlaneCheckpoint(key string) (driver.Checkpoint, error) {
	cp, err := r.snapshot.SourcePlaneCheckpoint(key)
	if err == nil && r.s.fault == "wrong-origin" && len(cp.Payload) > 0 {
		cp.Payload = bytes.Clone(cp.Payload)
		cp.Payload[len(cp.Payload)/2] ^= 1
		r.s.controls[r.s.fault]++
	}
	return cp, err
}

func assertJoinedEmptyTarget(t *testing.T, store driver.Store, before storeState, profile driver.RestoreImportProfile, binding dumpformat.NativeJoinedBinding) {
	t.Helper()
	if !reflect.DeepEqual(before, snapshot(t, store)) {
		t.Fatal("refusal changed graph/history/metadata")
	}
	ctx := context.Background()
	cp, err := store.(driver.CheckpointStore).LoadCheckpoint(ctx, "plugin/workflow/default/durable")
	if err != nil || cp.Generation != 0 || len(cp.Payload) != 0 {
		t.Fatal("abort retained checkpoint", cp.Generation, err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: before.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(profile.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(ctx, principal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
	if err != nil || receipt != nil {
		t.Fatal("abort retained receipt", err)
	}
	for _, source := range binding.Sources {
		_, err = r.(driver.AllocatorReader).LoadAllocator(ctx, source.SourceProject)
		if !errors.Is(err, driver.ErrNotFound) {
			t.Fatal("abort retained allocator", source.SourceProject, err)
		}
	}
}

func TestNativeCollectionReceivingSecondSourceProfileAndLimitRefusalsArePrewrite(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, fault := range []string{"second-close", "second-cancel", "second-corrupt", "final-profile", "shared-limit"} {
				t.Run(fault, func(t *testing.T) {
					l := newJoinedOriginalLedger(t)
					budget := dumpformat.NewNativeDeltaBudget(0)
					ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
					store, initial, _ := openIdentityTarget(t, engine)
					if err := initial.Release(ctx); err != nil {
						t.Fatal(err)
					}
					op, lease, release, err := HoldRestoreLease(ctx, store, "joined-refusal")
					if err != nil {
						t.Fatal(err)
					}
					defer release()
					profile := joinedPhysicalProfile(t, op, l, store, lease)
					observed := observeJoinedStore(t, store, l)
					before := snapshot(t, store)
					call, cancel := context.WithCancel(op)
					defer cancel()
					switch fault {
					case "second-close":
						l.faultSecondClose = true
					case "second-cancel":
						l.cancelSecondClose = cancel
					case "second-corrupt":
						name := filepath.Join(l.directories[1], l.binding.Sources[1].Files[2].Name)
						raw, e := os.ReadFile(name)
						if e != nil {
							t.Fatal(e)
						}
						raw[len(raw)-1] ^= 1
						if e = os.WriteFile(name, raw, 0600); e != nil {
							t.Fatal(e)
						}
					case "final-profile":
						profile.DerivedDigest = strings.Repeat("d", 64)
					case "shared-limit":
						budget = dumpformat.NewNativeDeltaBudget(4096)
						call = dumpformat.WithNativeDeltaBudget(call, budget)
					}
					l.phase = fault
					report, err := RestoreNativeCaptureCollectionImportWithLease(call, l.binding, l.sources(), observed, lease, profile)
					if err == nil || report.Receipt != nil || observed.writes != 0 {
						t.Fatal("source/profile/limit refusal reached publication", fault, observed.writes, err)
					}
					if fault == "second-cancel" && !errors.Is(err, context.Canceled) {
						t.Fatal("cancellation was hidden", err)
					}
					if fault == "shared-limit" {
						var limit *dumpformat.NativeDeltaDecodedLimitError
						if !errors.As(err, &limit) {
							t.Fatal("missing typed shared-budget refusal", err)
						}
					}
					assertJoinedEmptyTarget(t, store, before, profile, l.binding)
					if budget.Usage().CurrentAccountedDecodedBytes != 0 {
						t.Fatal("refusal leaked source/session owner", budget.Usage())
					}
				})
			}
		})
	}
}

func TestNativeCollectionReceivingLateAbortConservesHistoryCheckpointReceiptAndAllocators(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			// Restrict deliberate spool corruption to this test's private temp root.
			spools := t.TempDir()
			t.Setenv("TMPDIR", spools)
			l := newJoinedOriginalLedger(t)
			budget := dumpformat.NewNativeDeltaBudget(0)
			ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
			store, initial, _ := openIdentityTarget(t, engine)
			if err := initial.Release(ctx); err != nil {
				t.Fatal(err)
			}
			op, lease, release, err := HoldRestoreLease(ctx, store, "joined-late-abort")
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			profile := joinedPhysicalProfile(t, op, l, store, lease)
			observed := observeJoinedStore(t, store, l)
			before := snapshot(t, store)
			observed.late = func() error {
				dirs, err := os.ReadDir(spools)
				if err != nil {
					return err
				}
				var chosen string
				for _, d := range dirs {
					if d.IsDir() && strings.HasPrefix(d.Name(), "plug-native-collection-") {
						if chosen != "" {
							return fmt.Errorf("ambiguous private receiving spool")
						}
						chosen = filepath.Join(spools, d.Name())
					}
				}
				if chosen == "" {
					return fmt.Errorf("missing private receiving spool")
				}
				return os.Remove(filepath.Join(chosen, dumpformat.NativeCollectionFileName(1, l.binding.Sources[1].Files[2].Name)))
			}
			report, err := RestoreNativeCaptureCollectionImportWithLease(op, l.binding, l.sources(), observed, lease, profile)
			if !errors.Is(err, os.ErrNotExist) || observed.writes != 1 || observed.controls["post-history publication"] != 1 || report.Receipt != nil {
				t.Fatal("late spool refusal did not reach actual staged receiver", observed.writes, observed.controls, err)
			}
			// Deleting an original spool at PrepareRestoreCheckpoint leaves Seal
			// and history staging complete. Source checkpoint staging then succeeds;
			// VisitOriginalFiles refuses after that stage, before the single commit.
			assertJoinedEmptyTarget(t, store, before, profile, l.binding)
			if budget.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("late abort leaked operation resources", budget.Usage())
			}
			// The original key is unspent after this determinate abort. A genuine
			// complete receive must still succeed, proving allocator/stage rollback.
			report, err = RestoreNativeCaptureCollectionImportWithLease(op, l.binding, l.sources(), observed, lease, profile)
			if err != nil || report.Receipt == nil || observed.writes != 2 {
				t.Fatal("successful same-key receive after abort", observed.writes, err)
			}
			if err = report.Close(); err != nil {
				t.Fatal(err)
			}
			if budget.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("same-key completion leaked resources", budget.Usage())
			}
		})
	}
}

func TestNativeCollectionRecoveryAndIndependentReadbackRefuseTamperedObservations(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			l := newJoinedOriginalLedger(t)
			budget := dumpformat.NewNativeDeltaBudget(0)
			ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
			store, initial, _ := openIdentityTarget(t, engine)
			if err := initial.Release(ctx); err != nil {
				t.Fatal(err)
			}
			op, lease, release, err := HoldRestoreLease(ctx, store, "joined-recovery-controls")
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			profile := joinedPhysicalProfile(t, op, l, store, lease)
			observed := observeJoinedStore(t, store, l)
			report, err := RestoreNativeCaptureCollectionImportWithLease(op, l.binding, l.sources(), observed, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			defer report.Close()
			before := snapshot(t, store)
			held := budget.Usage().CurrentAccountedDecodedBytes
			writes := observed.writes
			for _, fault := range []string{"wrong-receipt", "current-ledger", "current-limit", "changed-part", "wrong-origin"} {
				t.Run(fault, func(t *testing.T) {
					observed.fault = fault
					l.phase = fault
					start := len(l.retained)
					recovered, err := RecoverImport(op, observed, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
					if err == nil || recovered.Receipt != nil || observed.controls[fault] == 0 {
						t.Fatal("tampered recovery observation was accepted or fault unexercised", fault, err)
					}
					if fault == "wrong-receipt" || fault == "current-ledger" || fault == "current-limit" {
						l.assertRetainedTraversals(t, start, 0, true)
					}
					if observed.writes != writes || !reflect.DeepEqual(before, snapshot(t, store)) || budget.Usage().CurrentAccountedDecodedBytes != held {
						t.Fatal("failed recovery mutated target or retained resources", fault, budget.Usage())
					}
				})
			}
			for _, fault := range []string{"missing-native", "extra-native", "foreign-native"} {
				t.Run(fault, func(t *testing.T) {
					observed.fault = fault
					l.phase = fault
					err := VerifyNativeCaptureCollectionReadback(op, observed, l.binding, l.sources(), profile, report)
					if err == nil || observed.controls[fault] == 0 {
						t.Fatal("independent readback accepted omission/extra/foreign row", fault, err)
					}
					if observed.writes != writes || !reflect.DeepEqual(before, snapshot(t, store)) || budget.Usage().CurrentAccountedDecodedBytes != held {
						t.Fatal("failed independent readback changed target or owner", fault, budget.Usage())
					}
				})
			}
			observed.fault = ""
		})
	}
}
