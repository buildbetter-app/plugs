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
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// The expectations name original producer frames and native catalog entries.
// They are independent of the receiver's mapping and history lowerer. This
// genuine small capture has an inert current-only transient row.
type b19NativeExpected struct {
	CaptureSHA256 string            `json:"capture_sha256"`
	StoreID       string            `json:"store_id"`
	Project       string            `json:"project"`
	Statuses      map[string]string `json:"statuses"`
	Entities      map[string]string `json:"entities"`
	ConfigPolicy  map[string]string `json:"config_policy"`
	Physical      map[string]struct {
		Table      string                         `json:"table"`
		SchemaID   string                         `json:"schema_id"`
		Columns    []dumpformat.NativeDeltaColumn `json:"physical_columns"`
		Rows       uint64                         `json:"rows"`
		PhysicalID string                         `json:"physical_sha256"`
	} `json:"physical_tables"`
	Commits []struct {
		Sequence uint64            `json:"sequence"`
		CommitID string            `json:"commit_id"`
		Root     string            `json:"root_sha256"`
		Tables   map[string]string `json:"tables"`
	} `json:"commits"`
}

func TestB19NativeSelectedModesMemory(t *testing.T) {
	b19NativeSelectedModes(t, "memory")
}

func TestB19NativeSelectedModesSQLite(t *testing.T) {
	b19NativeSelectedModes(t, "sqlite")
}

func TestB19NativeSelectedModesPostgres(t *testing.T) {
	b19NativeSelectedModes(t, "postgres")
}

func b19NativeSelectedModes(t *testing.T, engine string) {
	t.Helper()
	raw, err := os.ReadFile("testdata/b19-native-small-expectations.json")
	if err != nil {
		t.Fatal(err)
	}
	var want b19NativeExpected
	decode(t, raw, &want)
	if want.CaptureSHA256 != "fd910c93ebad8da226c7ff6cc84f49c46f87aafe2c3530a4a27bf48280e4c67d" || len(want.Commits) != 32 || len(want.Statuses) != 7 || len(want.Entities) != 2 {
		t.Fatal("independent native expectation identity or census changed")
	}
	for _, tc := range []struct {
		name               string
		mode               driver.RestoreHistoryMode
		first, floor, head uint64
	}{
		{"full", driver.RestoreHistoryModeFull, 1, 0, 33},
		{"partial-first", driver.RestoreHistoryModePartial, 1, 0, 33},
		{"partial-suffix", driver.RestoreHistoryModePartial, 17, 1, 18},
		{"snapshot", driver.RestoreHistoryModeSnapshot, 0, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newReceivingLedger(t)
			a := l.archive()
			defer a.ReleaseNativeDecoded()
			if a.SHA256() != want.CaptureSHA256 || a.Manifest().StoreID != want.StoreID {
				t.Fatal("selected import source identity differs from independent expectation")
			}
			baseline, _ := a.NativeDecodedUsage()
			store, lease, reopen := openIdentityTarget(t, engine)
			empty, err := store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			op, lease, release, err := HoldRestoreLease(context.Background(), store, "b19-native-selected-mode")
			if err != nil {
				t.Fatal(err)
			}
			release = sync.OnceValue(release)
			defer func() {
				if err := release(); err != nil {
					t.Error(err)
				}
			}()
			s := wrapReceivingStore(t, store, l)
			var selection *driver.NativeDeltaHistoryPolicy
			if tc.mode != driver.RestoreHistoryModeFull {
				selection = &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat, FirstSequence: tc.first}
			}
			l.phase = "B19 bind and selected import"
			p := receivingPublicProfileMode(t, a, s, lease, tc.mode, selection)
			expectedUsage := receivingUsageWithProfile(t, baseline, p)
			report, err := RestoreNativeCaptureSetImportWithLease(a.WithNativeDeltaAccounting(op), a, s, lease, p)
			if err != nil {
				t.Fatal("selected native import", err)
			}
			if report.Restored.Head != model.Natural64(tc.head) || report.Restored.HistoryFloor != model.Natural64(tc.floor) || report.Restored.StoreID != driver.StoreID(want.StoreID) || report.Restored.Incarnation == empty.Incarnation || s.writes != 1 || s.snapshots != 1 || s.recoveryStart == 0 {
				t.Fatal("selected mode identity/floor or actual publication/recovery control differs", report.Restored, s.writes, s.snapshots)
			}
			// PR493's Full receiving path keeps its admission+combined pass cap.
			// Other modes retain a literal trace, without inheriting that budget.
			if tc.mode == driver.RestoreHistoryModeFull {
				l.assertTraversals(0, 2, true)
			}
			l.phase = "B19 automatic receipt recovery"
			l.assertTraversals(s.recoveryStart, 0, true)
			start := len(l.reads)
			l.phase = "B19 imported complete native readback"
			if err := VerifyNativeCaptureSetReadback(op, s, a, report); err != nil {
				t.Fatal(err)
			}
			l.assertTraversals(start, 1, true)
			imported := b19AssertNativeModeState(t, s, p, report, want, tc.first, tc.floor, tc.head)
			b19AssertSourceInstallation(t, s, lease, p, report)
			b19AssertHistoryRefusals(t, s, report, empty.Incarnation)
			if !bytes.Equal(imported, b19NativeCensus(t, s, report)) {
				t.Fatal("history refusals changed imported rows or namespace")
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			store, lease = reopen()
			if err := lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			// Reopen returns an unrenewed bootstrap lease. Keep the canonical
			// restore lease active through the exhaustive historical readback.
			reopenOp, lease, releaseReopened, err := HoldRestoreLease(context.Background(), store, "b19-native-selected-mode-reopen")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := releaseReopened(); err != nil {
					t.Error(err)
				}
			}()
			s = wrapReceivingStore(t, store, l)
			start = len(l.reads)
			l.phase = "B19 explicit recovery after physical reopen"
			bound, recovered, found, err := RecoverBoundSourceImport(a.WithNativeDeltaAccounting(reopenOp), s)
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			if err != nil || !found || !recovered.Recovered || recovered.Archive == nil || s.writes != 0 || recovered.RequestHash != report.RequestHash || !reflect.DeepEqual(report.Receipt, recovered.Receipt) || !reflect.DeepEqual(report.SourcePlanes, recovered.SourcePlanes) || !reflect.DeepEqual(report.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(report.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(report.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(p.NativeDeltaHistory, bound.NativeDeltaHistory) || bound.HistoryMode != tc.mode {
				t.Fatal("reopen lost exact receipt, origin, mode or wrote a second import", found, err)
			}
			if tc.mode == driver.RestoreHistoryModeFull {
				l.assertTraversals(start, 2, false)
			}
			start = len(l.reads)
			l.phase = "B19 reopened complete native readback"
			if err := VerifyNativeCaptureSetReadback(reopenOp, s, a, recovered); err != nil {
				t.Fatal(err)
			}
			l.assertTraversals(start, 1, true)
			if !bytes.Equal(imported, b19AssertNativeModeState(t, s, bound, recovered, want, tc.first, tc.floor, tc.head)) {
				t.Fatal("physical reopen changed native rows, source history, catalog or namespace")
			}
			b19AssertSourceInstallation(t, s, lease, bound, recovered)
			b19AssertHistoryRefusals(t, s, recovered, empty.Incarnation)
			if !bytes.Equal(imported, b19NativeCensus(t, s, recovered)) {
				t.Fatal("reopened history refusals changed state")
			}
			recovered.Archive.ReleaseNativeDecoded()
			usage, _ := a.NativeDecodedUsage()
			if usage.LimitBytes != dumpformat.NativeDeltaDefaultDecodedLimit || usage.LimitOrigin != "default" || usage.PeakAccountedDecodedBytes == 0 || usage.PeakAccountedDecodedBytes > usage.LimitBytes || usage.CurrentAccountedDecodedBytes != expectedUsage.CurrentAccountedDecodedBytes || !reflect.DeepEqual(usage.CurrentCategories, expectedUsage.CurrentCategories) || !reflect.DeepEqual(usage.CurrentCharges, expectedUsage.CurrentCharges) {
				t.Fatal("selected import/reopen decoded ownership did not conserve the source plus live profile", usage, expectedUsage)
			}
			proof := map[string]any{"driver": engine, "mode": tc.mode, "first": tc.first, "floor": tc.floor, "head": tc.head, "archive_sha256": a.SHA256(), "expectations_sha256": hashBytes(raw), "request_hash": report.RequestHash, "receipt": report.Receipt, "history": recovered.NativeDeltaHistory, "state_sha256": hashBytes(imported), "traversals": l.reads, "decoded": usage, "reopen": map[bool]string{true: "same-process-memory-lease-reacquired", false: "driver-connection-closed-and-reopened"}[engine == "memory"]}
			a.ReleaseNativeDecoded()
			released, _ := a.NativeDecodedUsage()
			if released.CurrentAccountedDecodedBytes != 0 || len(released.CurrentCategories) != 0 || len(released.CurrentCharges) != 0 {
				t.Fatal("selected-mode release retained decoded ownership")
			}
			proof["after_release"] = released
			if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
				if err := os.WriteFile(filepath.Join(out, "b19-native-"+engine+"-"+tc.name+".json"), encode(t, proof), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("B19_native_mode driver=%s mode=%s first=%d floor=%d head=%d source=%s state=%s physical_reopen=%t history_refusals=exact decoded_after_release=0", engine, tc.mode, tc.first, tc.floor, tc.head, want.CaptureSHA256, hashBytes(imported), engine != "memory")
		})
	}
}

func b19AssertSourceInstallation(t *testing.T, store driver.Store, lease driver.ActiveLease, p driver.RestoreImportProfile, report Report) {
	t.Helper()
	ctx := context.Background()
	before := b19NativeCensus(t, store, report)
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	state, err := pluginhost.NewSourceRuntimeState(ctx, capability, lease, report.SourcePlanes.Providers[0])
	if err != nil {
		t.Fatal(err)
	}
	provider, err := workflowplugin.NewSourceRuntime(state, report.SourcePlanes.Providers[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"unknown-mode", "partial-zero", "partial-without-policy", "full-with-policy", "changed-selection"} {
		bad, err := p.Detached()
		if err != nil {
			t.Fatal(err)
		}
		switch invalid {
		case "unknown-mode":
			bad.HistoryMode = "unsupported"
		case "partial-zero":
			bad.HistoryMode = driver.RestoreHistoryModePartial
			bad.NativeDeltaHistory = &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat}
		case "partial-without-policy":
			bad.HistoryMode, bad.NativeDeltaHistory = driver.RestoreHistoryModePartial, nil
		case "full-with-policy":
			bad.HistoryMode = driver.RestoreHistoryModeFull
			bad.NativeDeltaHistory = &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat, FirstSequence: 1}
		case "changed-selection":
			bad.HistoryMode = driver.RestoreHistoryModePartial
			first := uint64(1)
			if p.HistoryMode == driver.RestoreHistoryModePartial && p.NativeDeltaHistory.FirstSequence == first {
				first = 17
			}
			bad.NativeDeltaHistory = &driver.NativeDeltaHistoryPolicy{Format: driver.NativeDeltaHistoryPolicyFormat, FirstSequence: first}
		}
		domain := service.New(store, lease, pipeline.New(store, lease))
		if err := domain.InstallSourceWorkflow(ctx, provider, bad, *report.SourcePlanes, *report.Receipt); !errors.Is(err, driver.ErrConflict) || domain.SourceRuntimeInstalled() {
			t.Fatal("invalid source mode/policy/receipt was installed", invalid, err)
		}
	}
	wrongFloor := report.Restored
	wrongFloor.HistoryFloor = model.Natural64(0)
	if report.Restored.HistoryFloor.IsZero() {
		wrongFloor.HistoryFloor = model.Natural64(1)
	}
	if err := report.SourcePlanes.Validate(p, wrongFloor); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("source mode accepted a metadata floor inconsistent with its policy", err)
	}
	domain := service.New(store, lease, pipeline.New(store, lease))
	for i := 0; i < 2; i++ {
		if err := domain.InstallSourceWorkflow(ctx, provider, p, *report.SourcePlanes, *report.Receipt); err != nil || !domain.SourceRuntimeInstalled() {
			t.Fatal("actual selected native source runtime failed to install/reinstall", p.HistoryMode, report.Restored.HistoryFloor, err)
		}
	}
	if !bytes.Equal(before, b19NativeCensus(t, store, report)) {
		t.Fatal("source runtime installation/refusal changed native rows, history or checkpoint")
	}
	t.Logf("B19_source_runtime mode=%s floor=%s installed=true exact_reinstall=true invalid_mode_policy_receipt_floor_refused=true state_conserved=true", p.HistoryMode, report.Restored.HistoryFloor.String())
}

func b19AssertNativeModeState(t *testing.T, store driver.Store, p driver.RestoreImportProfile, report Report, want b19NativeExpected, first, floor, head uint64) []byte {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil || meta != report.Restored || meta.Head != model.Natural64(head) || meta.HistoryFloor != model.Natural64(floor) {
		t.Fatal("selected mode metadata after readback differs", meta, err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0)), model.RefByID(ImportedOntologySourceID, model.Natural64(0))})
	if err != nil || len(rows) != 2 {
		t.Fatal("committed native history/catalog missing", err)
	}
	var history driver.HistoryImportEvidence
	var catalog dumpformat.NativeStatusOntology
	for _, row := range rows {
		if row.Ref.ID == driver.OfflineHistoryEvidenceID {
			decode(t, row.Body, &history)
		} else {
			decode(t, row.Body, &catalog)
		}
	}
	if history.Mode != p.HistoryMode || history.Source != driver.NativeTableHistorySource || history.Floor != model.Natural64(floor) {
		t.Fatal("retained history mode, source or floor differs", history)
	}
	if p.HistoryMode == driver.RestoreHistoryModeSnapshot {
		if history.Complete || history.First != 0 || history.Head != 0 || len(history.Mapping) != 0 || len(history.Gaps) != 0 {
			t.Fatal("Snapshot invented source history", history)
		}
	} else {
		if history.First != first || history.Head != 32 || history.Complete != (first == 1) || len(history.Mapping) != int(33-first) {
			t.Fatal("selected source suffix completeness/census differs", history)
		}
		if first > 1 {
			if !reflect.DeepEqual(history.Gaps, []driver.SourceHistoryGap{{First: 1, Last: first - 1, Reason: "operator-selected-floor"}}) {
				t.Fatal("omitted prefix was lost or relabeled", history.Gaps)
			}
		} else if len(history.Gaps) != 0 {
			t.Fatal("full captured prefix gained a gap")
		}
		for i, mapping := range history.Mapping {
			sequence := first + uint64(i)
			revision := sequence
			if first > 1 {
				revision = sequence - first + 2
			}
			expected := want.Commits[sequence-1]
			if expected.Sequence != sequence || mapping.Sequence != sequence || mapping.SourceSequence != sequence || mapping.SourceStoreID != want.StoreID || mapping.Project != want.Project || mapping.Revision != model.Natural64(revision) || !reflect.DeepEqual(mapping.CommitIDs, []string{expected.CommitID}) || len(mapping.EventIDs) != 0 {
				t.Fatal("native source mapping differs from original frame expectation", mapping)
			}
		}
	}
	if p.HistoryMode != driver.RestoreHistoryModeFull {
		e := history.NativeDeltaHistory
		if e == nil || !reflect.DeepEqual(e, report.NativeDeltaHistory) || e.HeadCommitID != want.Commits[31].CommitID || e.HeadRootSHA256 != want.Commits[31].Root || e.FirstSequence != first {
			t.Fatal("native selected history provenance differs")
		}
		if first > 1 && (e.BaselineSequence != first-1 || e.BaselineCommitID != want.Commits[first-2].CommitID || e.BaselineRootSHA256 != want.Commits[first-2].Root) {
			t.Fatal("partial predecessor differs from original frame")
		}
		if first <= 1 && (e.BaselineSequence != 0 || e.BaselineCommitID != "" || e.BaselineRootSHA256 != "") {
			t.Fatal("selected mode fabricated a predecessor")
		}
	}
	for name, doc := range map[string]map[string]string{"base": want.Statuses, "project": want.Statuses} {
		statuses := catalog.Base.Statuses
		if name == "project" {
			statuses = catalog.Overlays[want.Project].Statuses
		}
		if len(statuses) != len(doc) {
			t.Fatal("native status catalog census changed", name)
		}
		for status, category := range doc {
			if got, exists := statuses[status]; !exists || string(got.Category) != category {
				t.Fatal("native status/category relabeled", name, status, category)
			}
		}
	}
	for id, status := range want.Entities {
		rows, err := r.Get(ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
		if err != nil || len(rows) != 1 {
			t.Fatal("native durable entity missing", id, err)
		}
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		var sourceStatus string
		if err != nil || json.Unmarshal(doc.Metadata["beads.source.status"], &sourceStatus) != nil || sourceStatus != status || doc.Fields["status"] != status || doc.Project != want.Project {
			t.Fatal("native current status or source status changed", id, err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	b19AssertPhysicalSnapshots(t, store, report, want, first)
	return b19NativeCensus(t, store, report)
}

// This oracle uses original producer end-table hashes and SQL PK descriptors.
// Reconstruction authenticates every typed cell/key against the complete table;
// expected commitments never pass through a receiver lowerer or row builder.
func b19AssertPhysicalSnapshots(t *testing.T, store driver.Store, report Report, want b19NativeExpected, first uint64) {
	t.Helper()
	ctx := context.Background()
	sequences := []uint64{}
	if first != 0 {
		if first > 1 {
			sequences = append(sequences, first-1)
		}
		for seq := first; seq <= 32; seq++ {
			sequences = append(sequences, seq)
		}
	}
	// Zero denotes the final imported attachment, including Snapshot's
	// retained authenticated HEAD checkpoint without historical mappings.
	sequences = append(sequences, 0)
	for _, seq := range sequences {
		at := report.Restored.Head
		index := uint64(31)
		if seq != 0 {
			index = seq - 1
			revision := seq
			if first > 1 {
				if seq == first-1 {
					revision = 1
				} else {
					revision = seq - first + 2
				}
			}
			at = model.Natural64(revision)
		}
		r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation, At: &at})
		if err != nil {
			t.Fatal("independent physical snapshot read", seq, at, err)
		}
		groups := nativeSnapshotGroups(t, r)
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		tables := want.Commits[index].Tables
		seen := 0
		for group, records := range groups {
			if group.History != "retained-native-commit" && seq == 0 {
				continue // The shared complete readback checks all current families.
			}
			physicalID, exists := tables[group.Table]
			if !exists || group.Source.StoreID != want.StoreID || group.Source.Project != want.Project || group.History != "retained-native-commit" {
				t.Fatal("extra or foreign physical table at selected snapshot", seq, group)
			}
			expected, exists := want.Physical[physicalID]
			if !exists || expected.Table != group.Table || expected.PhysicalID != physicalID {
				t.Fatal("independent original table commitment unavailable", group)
			}
			_, meta, err := dumpformat.ReconstructNativeRowTable(group.Source, group.Table, group.History, records, want.ConfigPolicy)
			if err != nil || meta.IdentityKind != dumpformat.NativeRowD3Physical || meta.SchemaID != expected.SchemaID || !reflect.DeepEqual(meta.PhysicalColumns, expected.Columns) || meta.RowCount != expected.Rows || uint64(len(records)) != expected.Rows+1 || meta.PhysicalTableSHA256 == nil || *meta.PhysicalTableSHA256 != physicalID {
				t.Fatal("selected physical cells/PK/schema/row commitment differs", seq, group.Table, meta, err)
			}
			seen++
		}
		if seen != len(tables) {
			t.Fatal("missing physical table in selected snapshot", seq, seen, len(tables))
		}
		t.Logf("B19_native_snapshot sequence=%d revision=%s tables=%d original_commit=%s original_root=%s exact_physical_schema_pk_rows=true", seq, at.String(), seen, want.Commits[index].CommitID, want.Commits[index].Root)
	}
}

func b19AssertHistoryRefusals(t *testing.T, store driver.Store, report Report, oldIncarnation string) {
	t.Helper()
	ctx := context.Background()
	future, _ := report.Restored.Head.Uint64()
	future++
	for _, tc := range []struct {
		name, incarnation string
		at                model.Revision
	}{
		{"old-incarnation", oldIncarnation, report.Restored.Head},
		{"future", report.Restored.Incarnation, model.Natural64(future)},
		{"floor", report.Restored.Incarnation, model.Natural64(0)},
	} {
		r, err := store.Read(ctx, driver.View{Incarnation: tc.incarnation, At: &tc.at})
		var changed driver.ErrIncarnationChanged
		var ahead driver.ErrFutureRevision
		var truncated driver.ErrTruncated
		switch tc.name {
		case "old-incarnation":
			if !errors.As(err, &changed) || changed.From != oldIncarnation || changed.To != report.Restored.Incarnation {
				t.Fatal("old incarnation did not refuse exactly", err)
			}
		case "future":
			if !errors.As(err, &ahead) || ahead.Requested != tc.at || ahead.Head != report.Restored.Head {
				t.Fatal("future address did not refuse exactly", err)
			}
		case "floor":
			if report.Restored.HistoryFloor.IsZero() {
				if err != nil || r == nil {
					t.Fatal("retained revision zero became latest or refused", err)
				}
				page, queryErr := r.Query(ctx, driver.Query{Limit: 1})
				if queryErr != nil || len(page.Items) != 0 || page.NextCursor != nil {
					t.Fatal("explicit revision zero resolved to a nonempty snapshot", page, queryErr)
				}
			} else if !errors.As(err, &truncated) || truncated.Floor != report.Restored.HistoryFloor {
				t.Fatal("discarded prefix did not refuse with the selected floor", err)
			}
		}
		if r != nil {
			if closeErr := r.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
	}
}

// Exhaust unfiltered Query, so reopen/refusal comparisons cannot hide an extra
// native row behind a first-page census. The checkpoint is the same real store.
func b19NativeCensus(t *testing.T, store driver.Store, report Report) []byte {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var rows []driver.Record
	q := driver.Query{Limit: 128}
	seen := map[string]bool{}
	for {
		page, err := r.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Items {
			key := fmt.Sprint(row.Ref)
			if seen[key] {
				t.Fatal("duplicate native census row", key)
			}
			seen[key] = true
			rows = append(rows, row)
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("native census cursor did not progress")
		}
		q.Cursor = page.NextCursor
	}
	capability, ok := store.(driver.WorkflowCheckpointStore)
	if !ok || report.SourcePlanes == nil || len(report.SourcePlanes.Providers) != 1 {
		t.Fatal("real native namespace unavailable")
	}
	cp, err := capability.LoadCheckpoint(ctx, report.SourcePlanes.Providers[0].Provider.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	return encode(t, struct {
		Meta       driver.Meta
		Records    []driver.Record
		Checkpoint driver.Checkpoint
	}{meta, rows, cp})
}
