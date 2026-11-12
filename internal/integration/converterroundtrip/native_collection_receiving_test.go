package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

type joinedOriginalRead struct {
	ordinal     int
	name, phase string
	bytes, size int64
	eof, closes int
}

type joinedOriginalLedger struct {
	binding           dumpformat.NativeJoinedBinding
	directories       []string
	reads             []*joinedOriginalRead
	phase             string
	faultSecondClose  bool
	cancelSecondClose context.CancelFunc
	retained          []joinedRetainedRead
}

type joinedOriginalReader struct {
	*os.File
	read         *joinedOriginalRead
	closeFailure bool
	cancel       context.CancelFunc
}

func (r *joinedOriginalReader) Read(data []byte) (int, error) {
	n, err := r.File.Read(data)
	r.read.bytes += int64(n)
	if err == io.EOF {
		r.read.eof++
	}
	return n, err
}

func (r *joinedOriginalReader) Close() error {
	r.read.closes++
	err := r.File.Close()
	if r.cancel != nil {
		r.cancel()
	}
	if r.closeFailure {
		err = errors.Join(err, errors.New("invented second source part close refusal"))
	}
	return err
}

func newJoinedOriginalLedger(t *testing.T) *joinedOriginalLedger {
	t.Helper()
	return &joinedOriginalLedger{binding: joinedPhysicalBinding(t), directories: []string{joinedPhysicalDirectory(t, "ops"), joinedPhysicalDirectory(t, "sv2")}, phase: "constructor"}
}

func (l *joinedOriginalLedger) open(ordinal int, name string) (*joinedOriginalReader, error) {
	f, err := os.Open(filepath.Join(l.directories[ordinal], name))
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	event := &joinedOriginalRead{ordinal: ordinal, name: name, phase: l.phase, size: stat.Size()}
	l.reads = append(l.reads, event)
	r := &joinedOriginalReader{File: f, read: event, closeFailure: l.faultSecondClose && ordinal == 1 && strings.HasPrefix(name, "parts/")}
	if ordinal == 1 && strings.HasPrefix(name, "parts/") {
		r.cancel = l.cancelSecondClose
	}
	return r, nil
}

func (l *joinedOriginalLedger) sources() []historyverify.NativeCaptureSetOpener {
	result := make([]historyverify.NativeCaptureSetOpener, 0, 2)
	for ordinal := 0; ordinal < 2; ordinal++ {
		ordinal := ordinal
		result = append(result, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (archive *dumpformat.Archive, err error) {
			root, err := l.open(ordinal, "capture-set.json")
			if err != nil {
				return nil, err
			}
			defer func() { err = errors.Join(err, root.Close()) }()
			head, err := l.open(ordinal, "head.tar")
			if err != nil {
				return nil, err
			}
			defer func() { err = errors.Join(err, head.Close()) }()
			return dumpformat.ReadNativeCaptureSet(ctx, root, head, func(_ context.Context, n uint64) (io.ReadCloser, error) {
				return l.open(ordinal, fmt.Sprintf("parts/%020d.part", n))
			}, options)
		})
	}
	return result
}

func (l *joinedOriginalLedger) assertTraversals(t *testing.T, start, maximum int, exact bool) {
	t.Helper()
	counts := map[string]int{}
	for _, read := range l.reads[start:] {
		if read.bytes != read.size || read.eof == 0 || read.closes != 1 {
			t.Fatal("original did not reach byte/EOF/Close terminal", *read)
		}
		counts[fmt.Sprintf("%d/%s", read.ordinal, read.name)]++
	}
	for ordinal, source := range l.binding.Sources {
		for _, file := range source.Files {
			n := counts[fmt.Sprintf("%d/%s", ordinal, file.Name)]
			if n > maximum || exact && n != maximum {
				t.Fatal("per-source original traversal cap", l.phase, ordinal, file.Name, n, maximum)
			}
		}
	}
}

// Independent explicit test intent, not the future constructor mapper. The
// actual physical public pair is authenticated through the production issuer.
func joinedPhysicalProfile(t *testing.T, ctx context.Context, l *joinedOriginalLedger, store driver.Store, lease driver.ActiveLease) driver.RestoreImportProfile {
	t.Helper()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(l.binding)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "physical-collection-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("c", 64)}, SourceStoreID: driver.StoreID("merge-" + hashBytes(raw)), Method: "offline.restore.import", IdempotencyKey: "physical-native-two-source-receive", ArchiveDigest: hashBytes(raw), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	target := driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: capability.WorkflowCheckpointLifetime(), RuntimeContract: driver.SourcePlaneRuntimeContractV1}
	provider := driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: target.Lifetime, OriginAdapter: "beads-workflow-origins/1"}
	issuer, err := historyverify.NewNativeCollection(ctx, l.binding, historyverify.NativeCollectionAdmission{Profile: profile, Target: target, Provider: provider}, when)
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Close()
	for _, source := range l.sources() {
		if err = issuer.Append(ctx, source); err != nil {
			t.Fatal(err)
		}
	}
	a, err := issuer.FinishCurrent()
	if err != nil {
		t.Fatal(err)
	}
	plan := physicalCollectionSourcePlan(t, a, store, lease)
	plan.Providers[0].LogicalImageSHA256 = strings.Repeat("0", 64)
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes, profile.DerivedDigest = &binding, strings.Repeat("0", 64)
	prepared, err := prepareSourceArchive(a.WithNativeDeltaAccounting(ctx), a, profile, false)
	if err != nil {
		t.Fatal(err)
	}
	plan.Providers[0].LogicalImageSHA256, plan.Providers[0].LogicalImageSize = prepared.SHA256(), uint64(len(prepared.LogicalImage()))
	binding, err = driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes, profile.DerivedDigest = &binding, ""
	_, _, profile, err = issuer.Bind(ctx, profile)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestNativeCollectionReceivingOneSessionRecoveryAndPerOriginalTraversalCaps(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			l := newJoinedOriginalLedger(t)
			budget := dumpformat.NewNativeDeltaBudget(0)
			ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
			store, initial, reopen := openIdentityTarget(t, engine)
			if err := initial.Release(ctx); err != nil {
				t.Fatal(err)
			}
			op, lease, release, err := HoldRestoreLease(ctx, store, "native-collection-receive")
			if err != nil {
				t.Fatal(err)
			}
			release = sync.OnceValue(release)
			defer release()
			observed := observeJoinedStore(t, store, l)
			profile := joinedPhysicalProfile(t, op, l, store, lease)
			l.assertTraversals(t, 0, 1, true)
			if budget.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("constructor kept a historical preparation", budget.Usage())
			}
			l.phase = "receiver"
			report, err := RestoreNativeCaptureCollectionImportWithLease(op, l.binding, l.sources(), observed, lease, profile)
			if err != nil {
				t.Fatal("actual two-native-set single row receiver", err)
			}
			defer report.Close()
			if report.Receipt == nil || report.Restored.Head != model.Natural64(195) || report.Restored.HistoryFloor != model.Natural64(0) || observed.writes != 1 || len(report.NativeHistories) != 2 || report.SourcePlanes == nil || len(report.SourcePlanes.Providers) != 1 {
				t.Fatal("two-project session/receipt/provider conservation", report.Restored, observed.writes, len(report.NativeHistories))
			}
			l.assertTraversals(t, 0, 2, true) // Includes automatic no-op recovery: +0.
			l.assertRetainedTraversals(t, 0, 0, true)
			if observed.snapshots != 1 {
				t.Fatal("automatic recovery was not observed", observed.snapshots)
			}
			l.phase = "independent clean readback"
			if err = VerifyNativeCaptureCollectionReadback(op, observed, l.binding, l.sources(), profile, report); err != nil {
				t.Fatal("independent all-revision physical collection readback", err)
			}
			l.assertTraversals(t, 0, 3, true)
			before := snapshot(t, store)
			writes := observed.writes
			start := len(l.reads)
			retainedStart := len(l.retained)
			l.phase = "same-key indeterminate recovery"
			unknown := driver.ErrIndeterminate{IdempotencyKey: profile.IdempotencyKey}
			call, cancel := context.WithCancel(op)
			cancel()
			recovered, err := recoverNativeReceivingIndeterminate(call, observed, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}, unknown)
			if err != nil || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || observed.writes != writes || !reflect.DeepEqual(before, snapshot(t, store)) {
				t.Fatal("same original key indeterminate recovery replayed or changed receipt", err)
			}
			l.assertTraversals(t, start, 0, true) // Retained store reads, no input opener.
			l.assertRetainedTraversals(t, retainedStart, 2, false)
			if len(l.retained) == retainedStart {
				t.Fatal("recovery did not independently authenticate retained originals")
			}
			if err = recovered.Close(); err != nil {
				t.Fatal(err)
			}
			if err = report.Close(); err != nil {
				t.Fatal(err)
			}
			if err = release(); err != nil {
				t.Fatal(err)
			}
			if budget.Usage().CurrentAccountedDecodedBytes != 0 {
				t.Fatal("completed operation kept session resources", budget.Usage())
			}
			store, _ = reopen()
			observed = observeJoinedStore(t, store, l)
			retainedStart = len(l.retained)
			l.phase = "reopened resume"
			_, resumed, found, err := RecoverBoundSourceImport(ctx, observed)
			if err != nil || !found || !reflect.DeepEqual(resumed.Receipt, report.Receipt) || observed.writes != 0 {
				t.Fatal("real reopened collection receipt/recovery", found, err)
			}
			defer resumed.Close()
			l.assertRetainedTraversals(t, retainedStart, 2, false)
			if len(l.retained) == retainedStart {
				t.Fatal("resume skipped retained-original authentication")
			}
			l.phase = "later independent readback"
			start = len(l.reads)
			retainedStart = len(l.retained)
			if err = VerifyNativeCaptureCollectionReadback(ctx, observed, l.binding, l.sources(), profile, resumed); err != nil {
				t.Fatal(err)
			}
			l.assertTraversals(t, start, 1, true)
			l.assertRetainedTraversals(t, retainedStart, 0, true)
			if err = resumed.Close(); err != nil {
				t.Fatal(err)
			}
			if budget.Usage().CurrentAccountedDecodedBytes != 0 || budget.Usage().PeakAccountedDecodedBytes > 2<<30 {
				t.Fatal("one shared decoded owner cleanup/cap", budget.Usage())
			}
			t.Logf("JOINED_PHYSICAL_RECEIVE engine=%s sources=2 commits=194 key=one writes=one clean_original_traversals=3 later_readback=1 usage=%+v", engine, budget.Usage())
		})
	}
}
