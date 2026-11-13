// Private capture-spike executable. It is not a bd command or public interface.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/formula"
	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

type captureMode uint8

const (
	legacyCaptureMode captureMode = iota
	nativeDeltaCaptureMode
)

// selectCaptureMode keeps the legacy archive as the default while making the
// capture-set route explicit. --native-delta is retained as a compatibility
// spelling for existing public procedures; both v2 spellings select the same
// bounded, on-disk emitter and neither reaches plugdump.Build.
func selectCaptureMode(captureSet, nativeDelta bool) (captureMode, error) {
	if captureSet && nativeDelta {
		return legacyCaptureMode, fmt.Errorf("--capture-set and --native-delta are mutually exclusive")
	}
	if captureSet || nativeDelta {
		return nativeDeltaCaptureMode, nil
	}
	return legacyCaptureMode, nil
}

func main() {
	if err := run(); err != nil {
		writeSafeReport(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (resultErr error) {
	stage := stageReport
	observation := schemaObservation{ExpectedVersion: plugdump.CaptureSchemaVersion(), RowCountClass: "unmeasured"}
	defer func() {
		if resultErr == nil {
			return
		}
		var staged *captureFailure
		if !errors.As(resultErr, &staged) {
			resultErr = &captureFailure{stage: stage, schema: observation, cause: resultErr}
		}
	}()
	flags := flag.NewFlagSet("native-physical-capture", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Flag errors can contain private argument values.
	dataDir := flags.String("data-dir", "", "owned disposable Dolt data directory")
	database := flags.String("database", "", "exact database name")
	beadsDir := flags.String("beads-dir", "", "owned copied .beads directory")
	output := flags.String("output-dir", "", "fresh owner-only output directory")
	disposable := flags.Bool("disposable", false, "declare this source quiesced and disposable")
	timeout := flags.Duration("timeout", 20*time.Minute, "capture deadline")
	schemaOnly := flags.Bool("schema-only", false, "measure schema cursor without inventory, history or output files")
	historyProbeOnly := flags.Bool("history-probe-only", false, "count reachable commits, readable roots and indexed storage without user rows or output files")
	captureSet := flags.Bool("capture-set", false, "capture an authenticated bounded native delta capture set")
	nativeDelta := flags.Bool("native-delta", false, "compatibility alias for --capture-set")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	mode, err := selectCaptureMode(*captureSet, *nativeDelta)
	if err != nil {
		return err
	}
	if flags.NArg() != 0 || !*disposable || (*schemaOnly && *historyProbeOnly) || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`).MatchString(*database) {
		return fmt.Errorf("explicit disposable source and database required")
	}
	for _, path := range []string{*dataDir, *beadsDir, *output} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("canonical absolute capture paths required")
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || resolved != path {
			return fmt.Errorf("capture path must exist without symlinks")
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return fmt.Errorf("capture paths must be existing owner-only directories")
		}
	}
	if os.Getenv("BEADS_DIR") != *beadsDir {
		return fmt.Errorf("explicit BEADS_DIR differs from copied source")
	}
	cwd, err := os.Getwd()
	if err != nil || cwd != filepath.Dir(*beadsDir) {
		return fmt.Errorf("capture must run in copied source workspace")
	}
	for _, name := range []string{"inventory.json", "inventory.json.sources", "capture.tar", "physical-evidence.json", "head.tar", "capture-set.json", "parts", "capture-set"} {
		if _, err := os.Lstat(filepath.Join(*output, name)); !os.IsNotExist(err) {
			return fmt.Errorf("capture refuses existing output")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	stage = stageOpen
	db, closeDriver, err := physicalcapture.Open(*dataDir, *database)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, closeDriver()) }()
	defer db.Close()
	// One stable current-source transaction plus a dedicated image connection.
	db.SetMaxOpenConns(2)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if *historyProbeOnly {
		stage = stageProbe
		report, err := probeHistory(ctx, conn)
		if err != nil {
			return err
		}
		// A failed close/rollback is a probe failure, never a successful absence
		// report followed by a failing exit. Publish only after all resources close.
		if err := conn.Close(); err != nil {
			return err
		}
		if err := db.Close(); err != nil {
			return err
		}
		if err := closeDriver(); err != nil {
			return err
		}
		stage = stageReport
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	stage = stageSchema
	observation, err = probeSchema(ctx, conn)
	if err != nil {
		return &captureFailure{stage: stageSchema, table: "schema_migrations", schema: observation, cause: err}
	}
	// Record the actual restored version before entering the own-version
	// inventory/history reader. This never runs a migration or alters a cursor.
	report := safeCaptureReport{Format: "beads-capture-diagnostic/1", Stage: stageSchema, Schema: observation, GoErrorTypes: []string{}, SQLCodes: []uint16{}}
	if *schemaOnly {
		stage = stageReport
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	stage = stageReport
	if err := json.NewEncoder(os.Stderr).Encode(report); err != nil {
		return err
	}
	stage = stageSchema
	if !plugdump.CaptureSchemaSupported(*observation.FoundVersion) {
		return &unsupportedCaptureSchema{}
	}
	stage = stageOpen
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current := plugdump.SQLSource{DB: tx}
	if err := preflightCurrent(ctx, current, observation); err != nil {
		return err
	}
	stage = stageHistory
	source := observedNativeSource{OwnVersionCommitSource: plugdump.OwnVersionCommitSource{PhysicalCommitSource: plugdump.PhysicalCommitSource{SQLSource: current, PhysicalDB: db}}, schema: observation}
	originalLog, err := source.ReadSourceCommits(ctx)
	if err != nil {
		return err
	}
	evidenceConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	working, workingErr := physicalcapture.ReadWorkingRoot(ctx, evidenceConn, originalLog.Head)
	closeEvidenceErr := evidenceConn.Close()
	if err := errors.Join(workingErr, closeEvidenceErr); err != nil {
		return err
	}
	stage = stageCensus
	capture, err := nativeCapture(*dataDir, *database, *beadsDir, *output)
	if err != nil {
		return err
	}
	inventory, sidecars, err := source.NativeInventory(ctx, capture)
	if err != nil {
		return fmt.Errorf("native inventory: %w", err)
	}
	stage = stageReport
	if err := os.Mkdir(capture.SidecarDir, 0700); err != nil {
		return err
	}
	// This directory was absent on entry and created exclusively by this
	// invocation. Refusal must remove its unpublished sidecars, while retaining
	// the real underlying error (including any cleanup error) for safe taxonomy.
	ownedOutputs := []string{capture.SidecarDir}
	defer func() {
		if resultErr != nil {
			for _, path := range ownedOutputs {
				resultErr = errors.Join(resultErr, os.RemoveAll(path))
			}
		}
	}()
	for name, data := range sidecars {
		if filepath.Base(name) != name {
			return fmt.Errorf("invalid sidecar name")
		}
		if err := writeExclusive(filepath.Join(capture.SidecarDir, name), data); err != nil {
			return err
		}
	}
	stage = stageHistory
	// The capture-set selector must branch before Build: Build enters the
	// legacy single-member path and its aggregate native-history census.
	if mode == nativeDeltaCaptureMode {
		return runNativeDelta(ctx, db, conn, tx, closeDriver, source, inventory, working, originalLog, *output, &ownedOutputs)
	}
	archive, err := plugdump.Build(ctx, source, inventory)
	if err != nil {
		return fmt.Errorf("native build: %w", err)
	}
	finalLog, err := source.ReadSourceCommits(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(originalLog, finalLog) {
		return fmt.Errorf("original commit graph/parents/metadata changed")
	}
	stage = stageOpen
	manifest, err := plugdump.Validate(bytes.NewReader(archive))
	if err != nil {
		return fmt.Errorf("archive validation: %w", err)
	}
	native, err := archiveNativeHistory(archive)
	if err != nil {
		return err
	}
	stage = stageHistory
	physicalEvidence, err := readPhysicalEvidence(ctx, db, native, working, archive)
	if err != nil {
		return err
	}
	stage = stageReport
	inventoryBytes, err := plugdump.MarshalNativeInventory(inventory)
	if err != nil {
		return err
	}
	// Release every source resource before publishing the complete capture.
	stage = stageOpen
	if err := tx.Rollback(); err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	if err := closeDriver(); err != nil {
		return err
	}
	stage = stageReport
	if err := writeExclusive(filepath.Join(*output, "inventory.json"), inventoryBytes); err != nil {
		return err
	}
	ownedOutputs = append(ownedOutputs, filepath.Join(*output, "inventory.json"))
	if err := writeExclusive(filepath.Join(*output, "capture.tar"), archive); err != nil {
		return err
	}
	ownedOutputs = append(ownedOutputs, filepath.Join(*output, "capture.tar"))
	if err := writeExclusive(filepath.Join(*output, "physical-evidence.json"), physicalEvidence); err != nil {
		return err
	}
	ownedOutputs = append(ownedOutputs, filepath.Join(*output, "physical-evidence.json"))
	counts := map[string]int{}
	for _, source := range manifest.Sources {
		counts[source.Family] = source.Rows
	}
	summary := map[string]any{"capture": "published-validated", "source_family_contract": manifest.SourceFamilyContract, "families": counts, "scope": "capture-only; portable/native completeness separate"}
	if coverage := native.HistoryCoverage; coverage != nil {
		summary["verified_commit_count"] = coverage.VerifiedCommitCount
		boundaries := []map[string]any{}
		for _, b := range coverage.Boundaries {
			report := safeReport(&captureFailure{stage: stageHistory, schema: observation, cause: plugdump.ErrSourceHistoryUnavailable{Reason: "native-history-boundary", BoundaryReason: b.Reason, SchemaVersion: b.CommitSchemaVersion, OrdinalFromHead: b.CommitOrdinalFromHead, VerifiedCommitCount: &coverage.VerifiedCommitCount, SQLCodes: b.SQLDoltCodes, Cause: originalLog.BoundaryDiagnostics[b.CommitID]}})
			if err := json.NewEncoder(os.Stderr).Encode(report); err != nil {
				return err
			}
			boundary := map[string]any{"history_boundary_reason": b.Reason, "commit_ordinal_from_head": b.CommitOrdinalFromHead}
			if b.CommitSchemaVersion != nil {
				boundary["commit_schema_version"] = *b.CommitSchemaVersion
			}
			if len(b.SQLDoltCodes) > 0 {
				boundary["sql_dolt_codes"] = b.SQLDoltCodes
			}
			boundaries = append(boundaries, boundary)
		}
		summary["history_boundaries"] = boundaries
	}
	return json.NewEncoder(os.Stdout).Encode(summary)
}

func writeExclusive(path string, data []byte) (resultErr error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	file, err := root.OpenFile(filepath.Base(path), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// Match the stock native inventory's resolved source families. The sandbox
// supplies the copied workspace and isolated home; no original source is read.
func nativeCapture(dataDir, database, active, output string) (plugdump.NativeCapture, error) {
	home, err := os.UserHomeDir()
	if err != nil || !filepath.IsAbs(home) {
		return plugdump.NativeCapture{}, fmt.Errorf("absolute isolated home required")
	}
	userConfig, err := os.UserConfigDir()
	if err != nil || !filepath.IsAbs(userConfig) {
		return plugdump.NativeCapture{}, fmt.Errorf("absolute isolated config required")
	}
	project := filepath.Dir(active)
	host, err := os.Hostname()
	if err != nil {
		return plugdump.NativeCapture{}, err
	}
	paths := map[string][]string{
		"user-legacy-config":     {filepath.Join(home, ".beads", "config.yaml")},
		"user-xdg-config":        {filepath.Join(home, ".config", "bd", "config.yaml")},
		"user-native-config":     {filepath.Join(userConfig, "bd", "config.yaml")},
		"project-config":         {filepath.Join(project, ".beads", "config.yaml")},
		"beads-dir-config":       {filepath.Join(active, "config.yaml")},
		"project-local-config":   {filepath.Join(active, "config.local.yaml"), filepath.Join(project, ".beads", "config.local.yaml")},
		"metadata-json":          {filepath.Join(active, "metadata.json")},
		"legacy-config-json":     {filepath.Join(active, "config.json"), filepath.Join(home, ".beads", "config.json")},
		"beads-hooks":            {filepath.Join(active, "hooks")},
		"legacy-hooks":           {filepath.Join(active, "hooks.json"), filepath.Join(home, ".beads", "hooks")},
		"git-hooks":              {filepath.Join(project, ".git", "hooks")},
		"external-hook-managers": {filepath.Join(project, ".husky"), filepath.Join(project, "lefthook.yml"), filepath.Join(project, "lefthook.yaml"), filepath.Join(project, ".pre-commit-config.yaml")},
		"routes-jsonl":           {filepath.Join(active, "routes.jsonl")},
		"formulas":               formula.DefaultSearchPaths(), "formula-search-paths": formula.DefaultSearchPaths(),
		"issues-jsonl": {filepath.Join(active, "issues.jsonl")}, "interactions-jsonl": {filepath.Join(active, "interactions.jsonl")},
	}
	environment := map[string]any{}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok && (strings.HasPrefix(key, "BD_") || strings.HasPrefix(key, "BEADS_") || strings.HasPrefix(key, "DOLT_") || key == "GT_ROOT" || key == "HOME" || strings.HasPrefix(key, "XDG_")) {
			environment[key] = value
		}
	}
	return plugdump.NativeCapture{SourcePlaneAddressContract: plugdump.SourcePlaneAddressContract, Public: true, StoreDir: filepath.Join(dataDir, database), BeadsDir: active, Machine: host, SidecarDir: filepath.Join(output, "inventory.json.sources"), Paths: paths, Environment: environment, Flags: map[string]any{"disposable": true, "route": "private-immutable-physical-image"}}, nil
}
