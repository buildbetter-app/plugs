package converterroundtrip

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
			if report.collection == nil || report.collection.plan.Built() {
				t.Fatal("successful collection kept a live consumed plan")
			}
			closedPayload := reflect.ValueOf(report.collection.plan).FieldByName("planPayload").Elem()
			for _, name := range []string{"image", "archive", "profile", "sourcePlanes", "release"} {
				if !closedPayload.FieldByName(name).IsNil() {
					t.Fatal("collection report retains uncharged closed plan payload", name)
				}
			}
			assertJoinedPhysicalHistories(t, report.NativeHistories, l.binding)
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
			assertJoinedPhysicalHistories(t, recovered.NativeHistories, l.binding)
			l.assertTraversals(t, start, 0, true) // Retained store reads, no input opener.
			l.assertRetainedTraversals(t, retainedStart, 2, false)
			if len(l.retained) == retainedStart {
				t.Fatal("recovery did not independently authenticate retained originals")
			}
			// Own an independent expected value before Close clears the receipt holder.
			expectedReceipt, err := json.Marshal(report.Receipt)
			if err != nil {
				t.Fatal(err)
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
			resumedReceipt, receiptErr := json.Marshal(resumed.Receipt)
			if err != nil || receiptErr != nil || !found || string(resumedReceipt) != string(expectedReceipt) || observed.writes != 0 {
				t.Fatal("real reopened collection receipt/recovery", found, err)
			}
			defer resumed.Close()
			assertJoinedPhysicalHistories(t, resumed.NativeHistories, l.binding)
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

// Start from the frozen public physical capture pair. Only the config member,
// its dispositions, and the manifests that authenticate it are changed.
func widenJoinedPublicConfig(t *testing.T, ledger *joinedOriginalLedger) {
	t.Helper()
	const rows, leafBytes = 50, 512 << 10
	directory := ledger.directories[0]
	dest := t.TempDir()
	input := ledger.binding.Sources[0]
	for _, file := range input.Files {
		target := filepath.Join(dest, file.Name)
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(directory, file.Name), target); err != nil {
			t.Fatal(err)
		}
	}
	head, err := os.ReadFile(filepath.Join(directory, "head.tar"))
	if err != nil {
		t.Fatal(err)
	}
	members := tarMembers(t, head)
	var manifest dumpformat.Manifest
	decode(t, members["manifest.json"], &manifest)
	var entries []json.RawMessage
	decode(t, members[dumpformat.SourceConfigMember], &entries)
	value := strings.Repeat("x", leafBytes)
	for i := 0; i < rows; i++ {
		entry := fmt.Sprintf(`{"input":{"family":"project-config","path":"/public/p48/%03d","state":"json-config","precedence":%d},"values":{"row":"%s"}}`, i, 10000+i, value)
		entries = append(entries, json.RawMessage(entry))
		manifest.Sources = append(manifest.Sources, dumpformat.Disposition{
			Family: "project-config", Rows: 1, Action: "archive",
			Reason: "explicit JSON config; secrets are references; precedence retained, effective config not inferred",
		})
	}
	sort.SliceStable(manifest.Sources, func(i, j int) bool {
		left, right := manifest.Sources[i], manifest.Sources[j]
		if left.Family != right.Family {
			return left.Family < right.Family
		}
		return left.Reason < right.Reason
	})
	config := encode(t, entries)
	if len(config) <= 24<<20 {
		t.Fatalf("public config covers only %d bytes", len(config))
	}
	members[dumpformat.SourceConfigMember] = config
	manifest.Files[dumpformat.SourceConfigMember] = dumpformat.File{SHA256: hashBytes(config), Size: int64(len(config))}
	members["manifest.json"] = encode(t, manifest)
	head = writeTarMembers(t, members)

	rootRaw, err := os.ReadFile(filepath.Join(directory, "capture-set.json"))
	if err != nil {
		t.Fatal(err)
	}
	var root dumpformat.NativeCaptureSet
	decode(t, rootRaw, &root)
	root.HeadArchive = dumpformat.File{SHA256: hashBytes(head), Size: int64(len(head))}
	rootRaw = encode(t, root)
	for i, file := range input.Files {
		var data []byte
		switch file.Name {
		case "capture-set.json":
			data = rootRaw
		case "head.tar":
			data = head
		default:
			continue
		}
		target := filepath.Join(dest, file.Name)
		if err := os.Remove(target); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, data, 0o600); err != nil {
			t.Fatal(err)
		}
		input.Files[i].Bytes, input.Files[i].SHA256 = uint64(len(data)), hashBytes(data)
	}
	input.ArchiveSHA256 = hashBytes(rootRaw)
	ledger.directories[0], ledger.binding.Sources[0] = dest, input
}

func TestNativeCollectionReceivingStreamsWidePublicConfig(t *testing.T) {
	ledger := newJoinedOriginalLedger(t)
	widenJoinedPublicConfig(t, ledger)
	budget := dumpformat.NewNativeDeltaBudget(0)
	peak := map[string]uint64{
		"detached_member": 0, "config_element_decoded": 0, "config_wire_element": 0,
		"collection_config_refs": 0, "collection_config_copy_buffer": 0,
		"receiving_collection_config_stream": 0,
	}
	budget.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
		if high, tracked := peak[event.Phase]; tracked && event.Action == "reserve" {
			peak[event.Phase] = max(high, budget.ReservedBytes(event.Phase))
		}
	})
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), budget)
	store, lease, cleanup := openIdentityTarget(t, "sqlite")
	defer cleanup()
	profile := joinedPhysicalProfile(t, ctx, ledger, store, lease)
	report, err := RestoreNativeCaptureCollectionImportWithLease(ctx, ledger.binding, ledger.sources(), store, lease, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer report.Close()
	snapshot, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	meta, err := store.Meta(ctx)
	if err != nil || meta != report.Restored {
		t.Fatalf("receiving reported stale store metadata: %+v, %+v, %v", meta, report.Restored, err)
	}
	records, err := snapshot.Get(ctx, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
	if err != nil || len(records) != 1 {
		t.Fatalf("archive evidence: %d records, %v", len(records), err)
	}
	var evidence archiveEvidence
	decode(t, records[0].Body, &evidence)
	config := evidence.Members[dumpformat.SourceConfigMember]
	if config.Size <= 24<<20 {
		t.Fatalf("retained config only has %d bytes", config.Size)
	}
	source, size, err := snapshot.(driver.BlobStreamReader).OpenBlob(ctx, driver.Hash(config.SHA256))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	n, err := io.Copy(digest, source)
	if err = errors.Join(err, source.Close()); err != nil || n != size || n != config.Size || fmt.Sprintf("%x", digest.Sum(nil)) != config.SHA256 {
		t.Fatalf("published config commitment differs: %d / %d bytes, %v", n, size, err)
	}
	usage := budget.Usage()
	if usage.CumulativeCategories["config_element_decoded"] <= uint64(size) ||
		usage.CumulativeCategories["receiving_collection_config_stream"] != 1<<20 ||
		usage.CumulativeCategories["receiving_collection_config_read"] != 0 ||
		usage.CurrentCategories["collection_config_refs"] != 0 {
		t.Fatalf("wide config did not use bounded publication: %+v", usage)
	}
	for phase, bytes := range peak {
		if bytes == 0 || bytes >= 4<<20 {
			t.Fatalf("unexpected peak for %s: %d bytes", phase, bytes)
		}
	}
	t.Logf("published config %d bytes, peak charged bytes by phase %v", size, peak)
}
