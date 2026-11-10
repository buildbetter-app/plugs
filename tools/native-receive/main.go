// Command native-receive is the trusted disposable native-archive receiver.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type request struct {
	Format         string                        `json:"format"`
	PolicySHA256   string                        `json:"policy_sha256,omitempty"`
	OriginalInputs *joinedBinding                `json:"original_inputs,omitempty"`
	ArchiveSHA256  string                        `json:"archive_sha256"`
	SourceStoreID  string                        `json:"source_store_id"`
	Projects       []string                      `json:"projects"`
	Admission      driver.RestoreImportAdmission `json:"admission"`
	IdempotencyKey string                        `json:"idempotency_key"`
	SourcePlan     driver.SourcePlanePlanV1      `json:"source_plan"`
}

type verification struct {
	CurrentFamilies      int    `json:"current_families"`
	CurrentRows          int    `json:"current_rows"`
	HistoryImages        int    `json:"history_images"`
	HistoryTables        int    `json:"history_tables"`
	HistoryRows          int    `json:"history_rows"`
	ArchiveBlob          bool   `json:"archive_blob"`
	Reopen               string `json:"reopen"`
	OpaqueMetadataFields int    `json:"opaque_metadata_fields"`
}

type receipt struct {
	Format                   string                    `json:"format"`
	PolicySHA256             string                    `json:"policy_sha256,omitempty"`
	BinarySHA256             string                    `json:"binary_sha256"`
	RequestSHA256            string                    `json:"request_sha256"`
	ArchiveSHA256            string                    `json:"archive_sha256"`
	Driver                   string                    `json:"driver"`
	HistoryMode              string                    `json:"history_mode"`
	RestoreTime              string                    `json:"restore_time"`
	Verification             verification              `json:"verification"`
	Report                   converterroundtrip.Report `json:"committed_report"`
	OriginalInputs           *joinedBinding            `json:"original_inputs,omitempty"`
	SessionCeilingSeconds    int64                     `json:"session_ceiling_seconds,omitempty"`
	CrossProjectIDCollisions int                       `json:"cross_project_id_collisions"`
}

func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

func decode(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("receiving input has trailing JSON")
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var result any
	var err error
	if len(os.Args) > 1 && os.Args[1] == "prepare-request" {
		result, err = prepareRequest(ctx, os.Args[2:])
	} else {
		result, err = run(ctx, os.Args[1:])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "native receiving refused:", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "receiving receipt output failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (out receipt, runErr error) {
	var zero receipt
	ctx, sessionCancel := context.WithTimeout(ctx, rehearsalSessionLimit)
	defer sessionCancel()
	flags := flag.NewFlagSet("native-receive", flag.ContinueOnError)
	engine := flags.String("driver", "", "memory, sqlite or postgres")
	target := flags.String("target", "", "disposable SQLite path or PostgreSQL DSN; empty for memory")
	var paths archivePaths
	flags.Var(&paths, "archive", "original native archive; repeat in declared order for joined rehearsal")
	requestPath := flags.String("request", "", "explicit trusted receiving request")
	policyPath := flags.String("policy", "", "optional exact policy used to construct the request")
	bindingPath := flags.String("bindings", "", "external ordered original source/project/database/hash bindings")
	profilePath := flags.String("profile", "", "exact mechanical source profile bytes")
	sessionLimit := flags.Duration("session-timeout", rehearsalSessionLimit, "restore session timeout; may shorten the two-hour ceiling")
	expectedBinary := flags.String("expected-binary-sha256", "", "required exact executable SHA256")
	if err := flags.Parse(args); err != nil {
		return zero, err
	}
	if flags.NArg() != 0 || len(paths) == 0 || *requestPath == "" || len(*expectedBinary) != 64 {
		return zero, errors.New("archive, request and exact executable binding are required")
	}
	if *sessionLimit <= 0 || *sessionLimit > rehearsalSessionLimit {
		return zero, errors.New("restore session exceeds two-hour ceiling")
	}
	ctx, narrowCancel := context.WithTimeout(ctx, *sessionLimit)
	defer narrowCancel()
	self, err := os.Executable()
	if err != nil {
		return zero, err
	}
	binary, err := os.Open(self)
	if err != nil {
		return zero, err
	}
	h := sha256.New()
	_, hashErr := io.Copy(h, binary)
	closeErr := binary.Close()
	actualBinary := hex.EncodeToString(h.Sum(nil))
	if hashErr != nil || closeErr != nil || actualBinary != *expectedBinary {
		return zero, errors.New("receiving executable binding differs")
	}
	originals, err := loadOriginals(paths)
	if err != nil {
		return zero, err
	}
	requestBytes, err := readBounded(*requestPath, driver.SourcePlaneMaxPlanBytes)
	if err != nil {
		return zero, err
	}
	var input request
	if err := decode(requestBytes, &input); err != nil {
		return zero, err
	}
	bindingBytes, profileBytes, err := auxiliaryInputs(*bindingPath, *profilePath)
	if err != nil {
		return zero, err
	}
	archiveBytes := originals[0]
	var policyBytes []byte
	if input.PolicySHA256 != "" || *policyPath != "" {
		policyBytes, err = readBounded(*policyPath, driver.SourcePlaneMaxPlanBytes)
		if err != nil {
			return zero, err
		}
		if input.PolicySHA256 != digest(policyBytes) {
			return zero, errors.New("request policy binding differs")
		}
		assembled, constructed, err := constructForInputs(ctx, originals, policyBytes, bindingBytes, profileBytes, input.SourcePlan.Target.Lifetime)
		if err != nil {
			return zero, err
		}
		if !reflect.DeepEqual(constructed, input) {
			return zero, errors.New("request differs from explicit policy construction")
		}
		archiveBytes = assembled
	} else if len(originals) != 1 || input.OriginalInputs != nil || len(bindingBytes) != 0 || len(profileBytes) != 0 {
		return zero, errors.New("joined originals require explicit authorized policy")
	}
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		return zero, err
	}
	projects := []string{}
	for _, project := range archive.Manifest().Projects {
		projects = append(projects, project.Prefix)
	}
	plan := input.SourcePlan
	if input.Format != "plug-native-receiving-request/1" || input.ArchiveSHA256 != digest(archiveBytes) || input.ArchiveSHA256 != plan.ArchiveSHA256 || input.SourceStoreID != archive.Manifest().StoreID || input.SourceStoreID != plan.SourceStoreID || !reflect.DeepEqual(input.Projects, projects) || len(archive.NativeSourceHistories()) == 0 || plan.Target.StoreID != "" || plan.Target.Incarnation != "" {
		return zero, errors.New("explicit source/archive/project or new-target binding differs")
	}
	// Validate the complete declaration before creating a destination. Only the
	// new target identity is filled from the selected driver's actual Meta.
	checked := plan
	checked.Target.StoreID, checked.Target.Incarnation = "pending-new-target", "pending-new-target"
	checked.Providers = append([]driver.SourcePlaneProviderV1(nil), plan.Providers...)
	for i := range checked.Providers {
		if checked.Providers[i].LogicalImageSHA256 == "" && checked.Providers[i].LogicalImageSize == 0 {
			checked.Providers[i].LogicalImageSHA256 = strings.Repeat("0", 64)
		}
	}
	if err := checked.Validate(); err != nil {
		return zero, err
	}
	if err := strictNativeHistory(archive); err != nil {
		return zero, err
	}
	actualTarget := *target
	var session *disposableSession
	if input.PolicySHA256 == rehearsalPolicySHA256 {
		session, actualTarget, err = newDisposableSession(ctx, *engine, *target)
		if err != nil {
			return zero, err
		}
		defer func() {
			if err := session.close(); err != nil {
				out = zero
				runErr = fmt.Errorf("rehearsal cleanup failed: %w", err)
			}
		}()
	}
	open, err := opener(ctx, *engine, actualTarget)
	if err != nil {
		return zero, err
	}
	store, err := open()
	if err != nil {
		return zero, err
	}
	var release func() error
	defer func() {
		if release != nil {
			_ = release()
		}
		if store != nil {
			_ = store.Close()
		}
	}()
	op, lease, release, err := hold(ctx, store)
	if err != nil {
		return zero, err
	}
	meta, err := store.Meta(op)
	if err != nil {
		return zero, err
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		return zero, err
	}
	if plan.Target.Lifetime != capability.WorkflowCheckpointLifetime() {
		return zero, errors.New("declared target lifetime differs from actual driver")
	}
	plan.Target.StoreID, plan.Target.Incarnation = string(meta.StoreID), meta.Incarnation
	profile := driver.RestoreImportProfile{Admission: input.Admission, SourceStoreID: driver.StoreID(input.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: input.IdempotencyKey, ArchiveDigest: input.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano)}
	profile, err = converterroundtrip.BindSourceImport(op, bytes.NewReader(archiveBytes), profile, plan)
	if err != nil {
		return zero, err
	}
	committed, err := converterroundtrip.RestoreImportWithLease(op, bytes.NewReader(archiveBytes), store, lease, profile)
	if err != nil {
		return zero, err
	}
	if err := release(); err != nil {
		release = nil
		return zero, err
	}
	release = nil
	reopen := "same-process-memory"
	if *engine != "memory" {
		if err := store.Close(); err != nil {
			return zero, err
		}
		store = nil
		store, err = open()
		if err != nil {
			return zero, err
		}
		reopen = "driver-reopened"
	}
	op, lease, release, err = hold(ctx, store)
	if err != nil {
		return zero, err
	}
	recoveredProfile, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(op, store)
	if err != nil {
		return zero, fmt.Errorf("recover committed receiving receipt: %w", err)
	}
	if !found || !reflect.DeepEqual(committed.Receipt, recovered.Receipt) || !reflect.DeepEqual(committed.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(committed.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(committed.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(committed.SourcePlanes, recovered.SourcePlanes) {
		return zero, errors.New("committed receiving receipt/accounting recovery differs")
	}
	proof, err := verifyNative(op, store, recoveredProfile, recovered, archiveBytes)
	if err != nil {
		return zero, err
	}
	proof.Reopen = reopen
	if err := inputsUnchanged(paths, originals); err != nil {
		return zero, err
	}
	afterRequest, err := readBounded(*requestPath, driver.SourcePlaneMaxPlanBytes)
	if err != nil {
		return zero, err
	}
	if !bytes.Equal(requestBytes, afterRequest) {
		return zero, errors.New("receiving input changed during execution")
	}
	for _, bound := range []struct {
		path string
		raw  []byte
	}{{*policyPath, policyBytes}, {*bindingPath, bindingBytes}, {*profilePath, profileBytes}} {
		if bound.path != "" {
			after, err := readBounded(bound.path, driver.SourcePlaneMaxPlanBytes)
			if err != nil || !bytes.Equal(after, bound.raw) {
				return zero, errors.New("policy/profile/original binding changed during execution")
			}
		}
	}
	if err := release(); err != nil {
		release = nil
		return zero, err
	}
	release = nil
	if err := store.Close(); err != nil {
		return zero, err
	}
	store = nil
	if session != nil {
		if err := session.finish(ctx); err != nil {
			return zero, err
		}
	}
	result := receipt{Format: "plug-native-receiving-receipt/1", PolicySHA256: input.PolicySHA256, OriginalInputs: input.OriginalInputs, BinarySHA256: actualBinary, RequestSHA256: digest(requestBytes), ArchiveSHA256: input.ArchiveSHA256, Driver: *engine, HistoryMode: "full", RestoreTime: recoveredProfile.HistoryTime, Verification: proof, Report: recovered}
	if input.PolicySHA256 == rehearsalPolicySHA256 {
		result.SessionCeilingSeconds = int64(rehearsalSessionLimit / time.Second)
	}
	return result, nil
}

func readBounded(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("receiving input exceeds its bound")
	}
	return data, nil
}

func opener(ctx context.Context, engine, target string) (func() (driver.Store, error), error) {
	switch engine {
	case "memory":
		if target == "" {
			return func() (driver.Store, error) { return memory.New(), nil }, nil
		}
	case "sqlite":
		if filepath.IsAbs(target) {
			return func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: target}) }, nil
		}
	case "postgres":
		if target != "" {
			return func() (driver.Store, error) { return postgres.Open(ctx, driver.Config{DSN: target}) }, nil
		}
	}
	return nil, errors.New("explicit disposable driver/target required")
}

func hold(ctx context.Context, store driver.Store) (context.Context, driver.ActiveLease, func() error, error) {
	return converterroundtrip.HoldRestoreLease(ctx, store, "native-receive")
}
