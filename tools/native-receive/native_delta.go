package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type nativeDeltaRSS struct {
	Status       string `json:"status"`
	Method       string `json:"method"`
	ProcessScope string `json:"process_scope"`
	Platform     string `json:"platform"`
	SourceUnits  string `json:"source_units"`
	PeakBytes    uint64 `json:"peak_bytes"`
}
type nativeDeltaReceipt struct {
	Format       string                             `json:"format"`
	Verification dumpformat.NativeDeltaVerification `json:"verification"`
	LimitOrigin  string                             `json:"decoded_limit_origin"`
	RSS          nativeDeltaRSS                     `json:"rss"`
}

func nativeDeltaRSSMeasurement() nativeDeltaRSS {
	out := nativeDeltaRSS{Status: "NOT_MEASURED", Method: "getrusage(RUSAGE_SELF).ru_maxrss", ProcessScope: "native-receive process lifetime; excludes child processes", Platform: runtime.GOOS, SourceUnits: "KiB"}
	if runtime.GOOS != "linux" {
		return out
	}
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil || usage.Maxrss < 0 {
		return out
	}
	out.Status = "MEASURED"
	out.PeakBytes = uint64(usage.Maxrss) * 1024
	return out
}
func emitNativeDeltaRefusal(err error) {
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if errors.As(err, &limit) {
		_ = json.NewEncoder(os.Stderr).Encode(struct {
			Format  string                                   `json:"format"`
			Refusal *dumpformat.NativeDeltaDecodedLimitError `json:"refusal"`
			RSS     nativeDeltaRSS                           `json:"rss"`
		}{"plug-native-delta-refusal/1", limit, nativeDeltaRSSMeasurement()})
	}
}
func nativeDeltaSelected(args []string) bool {
	for _, arg := range args {
		if arg == "--capture-set" || strings.HasPrefix(arg, "--capture-set=") {
			return true
		}
	}
	return false
}

type nativeDeltaArgs struct {
	dir, policy, request, engine, target, binary string
	limit                                        uint64
	limitOrigin                                  string
	timeout                                      time.Duration
}

func parseNativeDeltaArgs(args []string, prepare bool) (nativeDeltaArgs, error) {
	var out nativeDeltaArgs
	f := flag.NewFlagSet("native-receive capture-set", flag.ContinueOnError)
	f.StringVar(&out.dir, "capture-set", "", "canonical native delta/2 capture-set directory")
	f.StringVar(&out.policy, "policy", "", "explicit trusted construction policy bound to capture-set.json SHA256")
	f.StringVar(&out.engine, "driver", "", "memory, sqlite or postgres")
	f.StringVar(&out.binary, "expected-binary-sha256", "", "required exact receiver executable SHA256")
	f.Uint64Var(&out.limit, "native-decoded-limit-bytes", dumpformat.NativeDeltaDefaultDecodedLimit, "cumulative decoded payload ceiling; positive bytes")
	if !prepare {
		f.StringVar(&out.request, "request", "", "exact constructor stdout request")
		f.StringVar(&out.target, "target", "", "owned absent disposable SQLite path or PostgreSQL DSN")
		f.DurationVar(&out.timeout, "session-timeout", rehearsalSessionLimit, "restore session ceiling")
	}
	if err := f.Parse(args); err != nil {
		return out, err
	}
	if f.NArg() != 0 || out.dir == "" || out.policy == "" || out.limit == 0 || out.binary == "" || (!prepare && out.request == "") {
		return out, errors.New("explicit capture set, policy, driver, positive decoded limit, request and binary binding required")
	}
	out.limitOrigin = "default"
	f.Visit(func(option *flag.Flag) {
		if option.Name == "native-decoded-limit-bytes" {
			out.limitOrigin = "explicit flag"
		}
	})
	if _, err := nativeDeltaLifetime(out.engine); err != nil {
		return out, err
	}
	return out, nil
}
func nativeDeltaLifetime(engine string) (driver.CheckpointLifetime, error) {
	switch engine {
	case "memory":
		return driver.CheckpointLifetimeProcess, nil
	case "sqlite", "postgres":
		return driver.CheckpointLifetimeRestart, nil
	}
	return "", errors.New("explicit receiving driver required")
}
func verifyNativeDeltaBinary(expected string) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	f, err := os.Open(self)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	closeErr := f.Close()
	actual := hex.EncodeToString(h.Sum(nil))
	if err != nil || closeErr != nil || actual != expected {
		return "", errors.New("receiving executable binding differs")
	}
	return actual, nil
}

func prepareNativeDeltaRequest(ctx context.Context, args []string) (request, error) {
	options, err := parseNativeDeltaArgs(args, true)
	if err != nil {
		return request{}, err
	}
	if _, err := verifyNativeDeltaBinary(options.binary); err != nil {
		return request{}, err
	}
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(ctx, options.dir, dumpformat.NativeCaptureSetOptions{DecodedLimitBytes: options.limit, DecodedLimitOrigin: options.limitOrigin})
	if err != nil {
		return request{}, err
	}
	ctx = archive.WithNativeDeltaAccounting(ctx)
	policy, err := readBounded(options.policy, driver.SourcePlaneMaxPlanBytes)
	if err != nil {
		return request{}, err
	}
	lifetime, _ := nativeDeltaLifetime(options.engine)
	return constructArchiveRequest(ctx, archive, policy, lifetime)
}

func runNativeDelta(ctx context.Context, args []string) (out receipt, runErr error) {
	options, err := parseNativeDeltaArgs(args, false)
	if err != nil {
		return out, err
	}
	if options.timeout <= 0 || options.timeout > rehearsalSessionLimit {
		return out, errors.New("restore session exceeds two-hour ceiling")
	}
	ctx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()
	binary, err := verifyNativeDeltaBinary(options.binary)
	if err != nil {
		return out, err
	}
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(ctx, options.dir, dumpformat.NativeCaptureSetOptions{DecodedLimitBytes: options.limit, DecodedLimitOrigin: options.limitOrigin})
	if err != nil {
		return out, err
	}
	ctx = archive.WithNativeDeltaAccounting(ctx)
	policy, err := readBounded(options.policy, driver.SourcePlaneMaxPlanBytes)
	if err != nil {
		return out, err
	}
	raw, err := readBounded(options.request, driver.SourcePlaneMaxPlanBytes)
	if err != nil {
		return out, err
	}
	var input request
	if err := decode(raw, &input); err != nil {
		return out, err
	}
	lifetime, _ := nativeDeltaLifetime(options.engine)
	constructed, err := constructArchiveRequest(ctx, archive, policy, lifetime)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(input, constructed) {
		return out, errors.New("request differs from exact current capture-set policy construction")
	}
	// All source, wire, decoded accounting and policy checks precede target creation.
	session, target, err := newDisposableSession(ctx, options.engine, options.target)
	if err != nil {
		return out, err
	}
	defer func() {
		if err := session.close(); err != nil {
			out = receipt{}
			runErr = errors.Join(runErr, err)
		}
	}()
	open, err := opener(ctx, options.engine, target)
	if err != nil {
		return out, err
	}
	store, err := open()
	if err != nil {
		return out, err
	}
	var release func() error
	defer func() {
		if release != nil {
			runErr = errors.Join(runErr, release())
		}
		if store != nil {
			runErr = errors.Join(runErr, store.Close())
		}
	}()
	op, lease, release, err := hold(ctx, store)
	if err != nil {
		return out, err
	}
	meta, err := store.Meta(op)
	if err != nil {
		return out, err
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		return out, err
	}
	if capability.WorkflowCheckpointLifetime() != lifetime {
		return out, errors.New("actual driver lifetime differs")
	}
	plan := input.SourcePlan
	plan.Target.StoreID, plan.Target.Incarnation = string(meta.StoreID), meta.Incarnation
	profile := driver.RestoreImportProfile{Admission: input.Admission, SourceStoreID: driver.StoreID(input.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: input.IdempotencyKey, ArchiveDigest: input.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano)}
	profile, err = converterroundtrip.BindNativeCaptureSetImport(op, archive, profile, plan)
	if err != nil {
		return out, err
	}
	committed, err := converterroundtrip.RestoreNativeCaptureSetImportWithLease(op, archive, store, lease, profile)
	if err != nil {
		return out, err
	}
	if err := release(); err != nil {
		release = nil
		return out, err
	}
	release = nil
	reopen := "same-process-memory"
	if options.engine != "memory" {
		if err := store.Close(); err != nil {
			return out, err
		}
		store = nil
		store, err = open()
		if err != nil {
			return out, err
		}
		reopen = "driver-reopened"
	}
	op, _, release, err = hold(ctx, store)
	if err != nil {
		return out, err
	}
	recoveredProfile, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(op, store)
	if err != nil {
		return out, err
	}
	if !found || !reflect.DeepEqual(committed.Receipt, recovered.Receipt) || !reflect.DeepEqual(committed.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(committed.NativeHistories, recovered.NativeHistories) {
		return out, errors.New("native delta committed receipt/recovery differs")
	}
	root, _ := archive.NativeDeltaRoot()
	proof, err := verifyParsedNative(op, store, recoveredProfile, recovered, archive, root)
	if err != nil {
		return out, err
	}
	proof.Reopen = reopen
	for _, bound := range []struct {
		path string
		raw  []byte
	}{{options.policy, policy}, {options.request, raw}} {
		after, err := readBounded(bound.path, driver.SourcePlaneMaxPlanBytes)
		if err != nil || !bytes.Equal(after, bound.raw) {
			return out, errors.New("native delta receiving binding changed")
		}
	}
	if err := archive.VisitNativeCaptureFiles(ctx, func(_ string, _ []byte) error { return nil }); err != nil {
		return out, err
	}
	if err := release(); err != nil {
		release = nil
		return out, err
	}
	release = nil
	if err := store.Close(); err != nil {
		return out, err
	}
	store = nil
	if err := session.finish(ctx); err != nil {
		return out, err
	}
	verification, _ := archive.NativeDeltaVerification()
	return receipt{Format: "plug-native-receiving-receipt/1", PolicySHA256: input.PolicySHA256, BinarySHA256: binary, RequestSHA256: digest(raw), ArchiveSHA256: input.ArchiveSHA256, Driver: options.engine, HistoryMode: "full", RestoreTime: recoveredProfile.HistoryTime, Verification: proof, Report: recovered, NativeDelta: &nativeDeltaReceipt{Format: "plug-native-delta-receiving/1", Verification: verification, LimitOrigin: options.limitOrigin, RSS: nativeDeltaRSSMeasurement()}}, nil
}
