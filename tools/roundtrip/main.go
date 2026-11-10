// Command roundtrip runs the trusted offline converter against a disposable
// store. It never prints source values, source identifiers or error strings.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type outcome struct {
	Status                   string                  `json:"status"`
	Stage                    string                  `json:"stage"`
	Reason                   string                  `json:"reason,omitempty"`
	HistoryMode              string                  `json:"history_mode"`
	ArchiveSHA256            string                  `json:"archive_sha256,omitempty"`
	Counts                   map[string]int          `json:"counts,omitempty"`
	RestoredHead             string                  `json:"restored_head,omitempty"`
	HistoryFloor             string                  `json:"history_floor,omitempty"`
	ReverseSHA256            string                  `json:"reverse_sha256,omitempty"`
	BaselineSHA256           string                  `json:"baseline_sha256,omitempty"`
	ReverseBytes             int                     `json:"reverse_bytes,omitempty"`
	HistoryLossItems         int                     `json:"history_loss_items,omitempty"`
	BeadsReimport            string                  `json:"beads_reimport"`
	BeadsReadback            string                  `json:"beads_readback"`
	BeadsBinarySHA256        string                  `json:"beads_binary_sha256,omitempty"`
	BeadsBuildRun            string                  `json:"beads_build_run,omitempty"`
	BeadsCommit              string                  `json:"beads_commit,omitempty"`
	BeadsTree                string                  `json:"beads_tree,omitempty"`
	BeadsCaptureCommit       string                  `json:"beads_capture_commit,omitempty"`
	BeadsCaptureTree         string                  `json:"beads_capture_tree,omitempty"`
	BeadsCaptureBinarySHA256 string                  `json:"beads_capture_binary_sha256,omitempty"`
	BeadsCaptureBuildRun     string                  `json:"beads_capture_build_run,omitempty"`
	BeadsReadbackSHA256      string                  `json:"beads_readback_sha256,omitempty"`
	Engine                   string                  `json:"engine"`
	RestoredIncarnation      string                  `json:"restored_incarnation,omitempty"`
	ReverseIncarnation       string                  `json:"reverse_incarnation,omitempty"`
	ReverseAt                string                  `json:"reverse_at,omitempty"`
	SourceHistoryComplete    bool                    `json:"source_history_complete"`
	SourceHistoryFirst       uint64                  `json:"source_history_first,omitempty"`
	SourceHistoryHead        uint64                  `json:"source_history_head,omitempty"`
	SourceHistoryGaps        []dumpformat.HistoryGap `json:"source_history_gaps,omitempty"`
	SourceFieldLosses        []string                `json:"source_field_losses,omitempty"`
	Claims                   int                     `json:"claims"`
	Writes                   int                     `json:"writes"`
}

func main() {
	result := run(os.Args[1:])
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if result.Status != "PASS" {
		os.Exit(2)
	}
}

func run(args []string) outcome {
	result := outcome{Status: "BLOCK", Stage: "arguments", BeadsReimport: "WAIT", BeadsReadback: "WAIT"}
	flags := flag.NewFlagSet("roundtrip", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "", "Source archive path")
	output := flags.String("output-dir", "", "New disposable directory below /tmp")
	mode := flags.String("history-mode", "full", "full, partial or snapshot")
	engine := flags.String("engine", "memory", "memory, sqlite or postgres")
	postgresDSN := flags.String("postgres-dsn", "", "Empty disposable loopback PostgreSQL database named plug_roundtrip_*")
	beads := beadsOptions{}
	flags.StringVar(&beads.Binary, "beads-binary", "", "Remotely built pinned public Beads executable")
	flags.StringVar(&beads.SHA256, "beads-sha256", "", "Independently verified Beads executable SHA256")
	flags.StringVar(&beads.BuildRun, "beads-build-run", "", "Independently verified RunsOn build receipt ID")
	flags.StringVar(&beads.Inventory, "beads-inventory", "", "Actual disposable Beads target inventory for supported raw export")
	flags.BoolVar(&beads.CapturePublicInventory, "beads-capture-public", false, "Capture the fresh public target inventory with the separately verified capture fixture")
	flags.StringVar(&beads.CaptureBinary, "beads-capture-binary", "", "Separate declared public capture fixture executable")
	flags.StringVar(&beads.CaptureSHA256, "beads-capture-sha256", "", "Verified public capture fixture executable SHA256")
	flags.StringVar(&beads.CaptureBuildRun, "beads-capture-build-run", "", "Actual public capture fixture RunsOn build ID")
	exercise := flags.Bool("exercise", true, "Exercise claim/release and an idempotent live edit")
	all := flags.Bool("all", false, "Include Ephemeral=true source rows for the explicit all-planes export")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *input == "" || *output == "" {
		result.Reason = "invalid_arguments"
		return result
	}
	if beads.CapturePublicInventory && beads.Inventory != "" {
		result.Reason = "conflicting_beads_inventory_options"
		return result
	}
	if beads.CapturePublicInventory && (beads.CaptureBinary == "" || beads.CaptureSHA256 == "" || beads.CaptureBuildRun == "") || !beads.CapturePublicInventory && (beads.CaptureBinary != "" || beads.CaptureSHA256 != "" || beads.CaptureBuildRun != "") {
		result.Reason = "explicit_public_capture_artifact_required"
		return result
	}
	historyMode, err := driver.ParseRestoreHistoryMode(*mode)
	if err != nil {
		result.Reason = "invalid_history_mode"
		return result
	}
	result.HistoryMode = string(historyMode)
	result.Engine = *engine
	parent, err := filepath.EvalSymlinks(filepath.Dir(*output))
	if err != nil || parent != "/tmp" && !strings.HasPrefix(parent, "/tmp/") || filepath.Base(*output) == "." || filepath.Base(*output) == ".." {
		result.Reason = "disposable_output_required"
		return result
	}
	outputDir := filepath.Join(parent, filepath.Base(*output))
	if os.Mkdir(outputDir, 0o700) != nil {
		result.Reason = "new_output_directory_required"
		return result
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	result.Stage = "source"
	f, err := os.Open(*input)
	if err != nil {
		result.Reason = "source_read_refused"
		return result
	}
	data, err := io.ReadAll(io.LimitReader(f, dumpformat.MaxArchiveBytes+1))
	_ = f.Close()
	if err != nil || len(data) > dumpformat.MaxArchiveBytes {
		result.Reason = "source_read_refused"
		return result
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		result.Reason = "archive_contract_refused"
		return result
	}
	if archive.Native() {
		result.Reason = "source_image_refused"
		return result
	}
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
	if err != nil {
		result.Reason = "source_image_refused"
		return result
	}
	result.ArchiveSHA256 = archive.SHA256()
	result.Counts = map[string]int{}
	// Diagnostics use a fixed vocabulary only after Beads-image admission.
	// Native manifests do not validate the Beads kind registry.
	for _, kind := range []string{"bead", "edge", "comment", "memory", "config", "beads-archive"} {
		result.Counts[kind] = len(archive.Records(kind))
	}
	if history, ok := archive.SourceHistory(); ok {
		result.SourceHistoryComplete = history.Range.Complete
		result.SourceHistoryFirst, result.SourceHistoryHead = history.Range.First, history.Range.Head
		result.SourceHistoryGaps = history.Range.Gaps
	}
	profile := driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "offline-roundtrip-owner"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: "offline-roundtrip-import", ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: historyMode,
	}
	result.Stage = "history"
	if historyMode != driver.RestoreHistoryModeSnapshot {
		profile, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), profile, time.Now().UTC())
		if err != nil {
			result.Reason = historyReason(err)
			return result
		}
	}
	store, err := openRuntimeStore(ctx, *engine, *postgresDSN, outputDir)
	if err != nil {
		result.Stage, result.Reason = "target", "disposable_target_refused"
		return result
	}
	defer store.Close()
	lease, err := store.Lease(ctx, "offline-roundtrip", driver.LeaseTTL)
	if err != nil {
		result.Reason = "lease_refused"
		return result
	}
	defer lease.Release(context.Background())
	result.Stage = "restore"
	report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
	if err != nil {
		result.Reason = "restore_contract_refused"
		return result
	}
	result.RestoredHead, result.HistoryFloor = report.Restored.Head.String(), report.Restored.HistoryFloor.String()
	result.RestoredIncarnation = string(report.Restored.Incarnation)
	result.Stage = "baseline_reverse"
	reverseOptions := reverseconverter.Options{All: *all}
	baseline, err := converterroundtrip.ReverseRestoredWithOptions(ctx, store, reverseOptions)
	if err != nil {
		result.Reason = reverseReason(err)
		return result
	}
	result.BaselineSHA256 = baseline.Result.Digest
	if err = writePrivate(outputDir, "reverse-baseline.jsonl", baseline.Result.JSONL); err != nil {
		result.Reason = "private_output_refused"
		return result
	}
	if *exercise {
		result.Stage = "workload"
		included := make(map[string]bool, len(baseline.Result.Projection.Issues))
		for _, issue := range baseline.Result.Projection.Issues {
			included[issue.ID] = true
		}
		var target dumpformat.Record
		for _, row := range archive.Records("bead") {
			if included[row.ID] {
				target = row
				break
			}
		}
		if target.ID == "" {
			result.Reason = "workload_requires_exported_bead"
			return result
		}
		ontology := service.NewOntology(nil)
		writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
		svc := service.New(store, lease, writer, service.WithOntology(ontology))
		meta := model.WriteMeta{Project: target.Project, Agent: "roundtrip", IdempotencyKey: strings.Repeat("b", 32)}
		claim, err := svc.ClaimNextWithOutcome(ctx, profile.Admission.Actor, service.ClaimNextRequest{Project: target.Project}, meta)
		if err != nil {
			result.Reason = "claim_workload_refused"
			return result
		}
		if claim != nil && len(claim.Commit.Results) != 0 {
			result.Claims = 1
			if active, err := converterroundtrip.ReverseRestoredWithOptions(ctx, store, reverseOptions); err == nil || len(active.Result.JSONL) != 0 {
				result.Reason = "active_claim_reverse_not_refused"
				return result
			}
			meta.IdempotencyKey = strings.Repeat("c", 32)
			if _, err = writer.ApplyClaimRelease(ctx, profile.Admission.Actor, claim.Commit.Results[0].Ref, "", meta); err != nil {
				result.Reason = "release_workload_refused"
				return result
			}
		}
		meta.Method, meta.IdempotencyKey = "tx.apply", strings.Repeat("d", 32)
		change := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID(target.ID, model.Natural64(0)), Fields: map[string]any{"description": "Disposable round-trip live edit"}}}}}
		commit, err := writer.Apply(ctx, profile.Admission.Actor, change, meta)
		if err != nil {
			result.Reason = "transaction_workload_refused"
			return result
		}
		replay, err := writer.Apply(ctx, profile.Admission.Actor, change, meta)
		if err != nil || !reflect.DeepEqual(commit, replay) {
			result.Reason = "transaction_replay_refused"
			return result
		}
		result.Writes = 1
	}
	result.Stage = "live_reverse"
	live, err := converterroundtrip.ReverseRestoredWithOptions(ctx, store, reverseOptions)
	if err != nil {
		result.Reason = reverseReason(err)
		return result
	}
	if *exercise && live.Result.Digest == baseline.Result.Digest {
		result.Reason = "live_reverse_stale"
		return result
	}
	if err = writePrivate(outputDir, "reverse-live.jsonl", live.Result.JSONL); err != nil {
		result.Reason = "private_output_refused"
		return result
	}
	result.ReverseSHA256, result.ReverseBytes = live.Result.Digest, len(live.Result.JSONL)
	result.ReverseIncarnation = string(live.Snapshot.Incarnation)
	if live.Snapshot.At != nil {
		result.ReverseAt = live.Snapshot.At.String()
	}
	result.HistoryLossItems = len(live.Result.Report.LostPostSwitchHistory)
	lossReport, err := json.Marshal(live.Result.Report)
	if err != nil || writePrivate(outputDir, "reverse-report.json", lossReport) != nil {
		result.Reason = "private_output_refused"
		return result
	}
	runBeads(ctx, beads, outputDir, archive, live.Result, &result)
	return result
}

func historyReason(err error) string {
	var refused driver.ErrRestoreHistoryRefused
	if errors.As(err, &refused) {
		return "history_" + string(refused.Reason)
	}
	return "history_binding_refused"
}

func reverseReason(err error) string {
	var refused converterroundtrip.ReverseRefusal
	if errors.As(err, &refused) {
		return refused.Reason
	}
	return "reverse_contract_refused"
}

func writePrivate(dir, name string, data []byte) error {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func openRuntimeStore(ctx context.Context, engine, dsn, output string) (driver.Store, error) {
	switch engine {
	case "memory":
		if dsn != "" {
			return nil, errors.New("unexpected DSN")
		}
		return memory.New(), nil
	case "sqlite":
		if dsn != "" {
			return nil, errors.New("unexpected DSN")
		}
		return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(output, "plug.db")})
	case "postgres":
		return openDisposablePostgres(ctx, dsn)
	default:
		return nil, errors.New("unsupported engine")
	}
}
