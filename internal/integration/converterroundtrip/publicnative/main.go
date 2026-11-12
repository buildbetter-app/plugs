// Command publicnative is a PUBLIC fixture proof helper. It authenticates
// pinned originals and exercises the existing collection collision census.
// It has no destination store, mapper, receiving stage or publication handle.
package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
)

//go:embed inputs.json
var registryBytes []byte

type publicSource struct {
	Name     string                         `json:"name"`
	Category string                         `json:"category"`
	StoreID  string                         `json:"source_store_id"`
	Project  string                         `json:"source_project"`
	Commits  uint64                         `json:"commits"`
	Files    []dumpformat.NativeCaptureFile `json:"files"`
}

type publicRegistry struct {
	Format  string         `json:"format"`
	Policy  string         `json:"policy_sha256"`
	Sources []publicSource `json:"sources"`
}

type names []string

func (n *names) String() string     { return strings.Join(*n, ",") }
func (n *names) Set(s string) error { *n = append(*n, s); return nil }

type proof struct {
	Format          string                             `json:"format"`
	Mode            string                             `json:"mode"`
	Sources         []string                           `json:"sources"`
	VerifiedCommits []uint64                           `json:"verified_commits,omitempty"`
	ReaderCalls     []int                              `json:"reader_calls,omitempty"`
	Refusal         *collisionRefusal                  `json:"refusal,omitempty"`
	BeforeClose     dumpformat.NativeDeltaDecodedUsage `json:"decoded_before_close"`
	AfterClose      dumpformat.NativeDeltaDecodedUsage `json:"decoded_after_close"`
}

type collisionRefusal struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

func hash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }

func registry() (publicRegistry, error) {
	var r publicRegistry
	err := json.Unmarshal(registryBytes, &r)
	if err == nil && (r.Format != "plug-public-native-proof-inputs/1" || len(r.Sources) != 6) {
		err = errors.New("public fixture registry differs")
	}
	return r, err
}

func selected(r publicRegistry, requested []string) ([]publicSource, error) {
	var result []publicSource
	for _, name := range requested {
		found := false
		for _, source := range r.Sources {
			if source.Name == name {
				result = append(result, source)
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("unregistered public input")
		}
	}
	return result, nil
}

func binding(r publicRegistry, sources []publicSource) (dumpformat.NativeJoinedBinding, []byte, error) {
	p, err := dumpformat.NativeSchemaForVersion(66, 26)
	if err != nil {
		return dumpformat.NativeJoinedBinding{}, nil, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return dumpformat.NativeJoinedBinding{}, nil, err
	}
	raw = append(raw, '\n')
	b := dumpformat.NativeJoinedBinding{Format: dumpformat.NativeJoinedInputsFormatV2, PolicySHA256: r.Policy, ProfileSHA256: hash(raw)}
	for i, s := range sources {
		project, database := "cody-ops", "ops"
		if i == 1 {
			project, database = "signals-v2", "sv2"
		}
		b.Sources = append(b.Sources, dumpformat.NativeJoinedInput{RehearsalProject: project, Database: database, SourceStoreID: s.StoreID, SourceProject: s.Project, ArchiveSHA256: s.Files[0].SHA256, Files: s.Files})
	}
	return b, raw, nil
}

// A successful prepared read authenticates each original once. Visit below
// counts the cached commit observations; it never rereads an original.
func sourceProof(ctx context.Context, source publicSource, directory string, budget *dumpformat.NativeDeltaBudget) (out proof, err error) {
	out = proof{Format: "plug-public-native-proof/1", Mode: "source", Sources: []string{source.Name}, ReaderCalls: []int{0}}
	ctx = dumpformat.WithNativeDeltaBudget(ctx, budget)
	ledgerOwner := budget.Scope()
	var archive *dumpformat.Archive
	var prepared *dumpformat.NativeDeltaPrepared
	var ledger []dumpformat.NativeCaptureFile
	defer func() {
		out.BeforeClose = budget.Usage()
		ledger, prepared = nil, nil
		owned := archive
		archive = nil
		if owned != nil {
			owned.ReleaseNativeDecoded()
		}
		ledgerOwner.Release()
		out.AfterClose = budget.Usage()
		if out.AfterClose.CurrentAccountedDecodedBytes != 0 || len(out.AfterClose.CurrentCategories) != 0 || len(out.AfterClose.CurrentCharges) != 0 {
			err = errors.Join(err, errors.New("public source owner did not release"))
		}
	}()
	archive, err = dumpformat.ReadNativeCaptureSetDirectory(ctx, directory, dumpformat.NativeCaptureSetOptions{PrepareReceiving: true})
	if err != nil {
		return out, err
	}
	out.ReaderCalls[0]++
	manifest := archive.Manifest()
	if archive.SHA256() != source.Files[0].SHA256 || manifest.StoreID != source.StoreID || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != source.Project {
		return out, errors.New("public source identity differs")
	}
	manifest = dumpformat.Manifest{}
	prepared, err = archive.PrepareNativeDelta(ctx)
	if err != nil {
		return out, err
	}
	ledger, err = prepared.NativeCaptureFileLedger(ledgerOwner)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(ledger, source.Files) {
		return out, errors.New("public original ledger differs")
	}
	var commits uint64
	err = prepared.Visit(ctx, dumpformat.NativeDeltaVisitor{Commit: func(dumpformat.NativeDeltaRowCommit) error { commits++; return nil }})
	if err != nil {
		return out, err
	}
	if commits != source.Commits {
		return out, errors.New("public authenticated commit count differs")
	}
	out.VerifiedCommits = []uint64{commits}
	return out, nil
}

// This is the actual history-inclusive issuer census, without any destination
// or receiving call. A non-collision or unrelated error cannot earn refusal.
func collisionProof(ctx context.Context, r publicRegistry, sources []publicSource, directories []string, budget *dumpformat.NativeDeltaBudget) (out proof, err error) {
	out = proof{Format: "plug-public-native-proof/1", Mode: "collision", Sources: []string{sources[0].Name, sources[1].Name}, ReaderCalls: []int{0, 0}}
	b, _, err := binding(r, sources)
	if err != nil {
		return out, err
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return out, err
	}
	when := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "public-fixture-proof"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("c", 64)}, SourceStoreID: driver.StoreID("merge-" + hash(raw)), Method: "offline.restore.import", IdempotencyKey: "public-collision-proof", ArchiveDigest: hash(raw), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	target := driver.SourcePlaneTargetV1{StoreID: "unopened-public-proof-target", Incarnation: "unopened-public-proof-incarnation", RequireEmpty: true, Lifetime: driver.CheckpointLifetimeProcess, RuntimeContract: driver.SourcePlaneRuntimeContractV1}
	provider := driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: target.Lifetime, OriginAdapter: "beads-workflow-origins/1"}
	ctx = dumpformat.WithNativeDeltaBudget(ctx, budget)
	issuer, err := historyverify.NewNativeCollection(ctx, b, historyverify.NativeCollectionAdmission{Profile: profile, Target: target, Provider: provider}, when)
	if err != nil {
		return out, err
	}
	defer func() {
		out.BeforeClose = budget.Usage()
		err = errors.Join(err, issuer.Close())
		out.AfterClose = budget.Usage()
		if issuer.Owner() != nil || out.AfterClose.CurrentAccountedDecodedBytes != 0 || len(out.AfterClose.CurrentCategories) != 0 || len(out.AfterClose.CurrentCharges) != 0 {
			err = errors.Join(err, errors.New("public collision owner did not release"))
		}
	}()
	for i, directory := range directories {
		i, directory := i, directory
		if err = issuer.Append(ctx, func(ctx context.Context, o dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
			a, e := dumpformat.ReadNativeCaptureSetDirectory(ctx, directory, o)
			if e == nil {
				out.ReaderCalls[i]++
			}
			return a, e
		}); err != nil {
			return out, err
		}
	}
	_, err = issuer.FinishCurrent()
	var refused *historyverify.NativeIDCollisionError
	if !errors.As(err, &refused) || refused.Count <= 0 {
		return out, errors.New("expected typed public collision refusal was not observed")
	}
	out.Refusal = &collisionRefusal{Code: "cross_project_id_collision", Count: refused.Count}
	err = nil
	return out, nil
}

func run(ctx context.Context, args []string) (any, error) {
	f := flag.NewFlagSet("publicnative", flag.ContinueOnError)
	mode := f.String("mode", "", "source, collision or inputs")
	output := f.String("output", "", "fresh output directory for inputs mode")
	var requested, directories names
	f.Var(&requested, "source", "pinned public source name; repeat in order")
	f.Var(&directories, "directory", "retained capture-set directory; repeat in order")
	if err := f.Parse(args); err != nil {
		return nil, err
	}
	if f.NArg() != 0 {
		return nil, errors.New("unexpected proof arguments")
	}
	r, err := registry()
	if err != nil {
		return nil, err
	}
	sources, err := selected(r, requested)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()
	switch *mode {
	case "source":
		if len(sources) != 1 || len(directories) != 1 || *output != "" {
			return nil, errors.New("one public source and directory required")
		}
		return sourceProof(ctx, sources[0], directories[0], dumpformat.NewNativeDeltaBudget(0))
	case "collision":
		if len(sources) != 2 || len(directories) != 2 || !reflect.DeepEqual([]string(requested), []string{"public1200-loose", "public1200-archived"}) || *output != "" {
			return nil, errors.New("exact public1200 collision pair required")
		}
		return collisionProof(ctx, r, sources, directories, dumpformat.NewNativeDeltaBudget(0))
	case "inputs":
		if len(sources) != 2 || len(directories) != 0 || *output == "" || sources[0].Category != sources[1].Category || sources[0].Project != "ops" || sources[1].Project != "sv2" {
			return nil, errors.New("ordered distinct ops/sv2 input pair required")
		}
		b, profile, err := binding(r, sources)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(b)
		if err != nil {
			return nil, err
		}
		if err = os.Mkdir(*output, 0700); err != nil {
			return nil, err
		}
		for name, data := range map[string][]byte{"binding.json": append(raw, '\n'), "profile.json": profile, "provenance.json": registryBytes} {
			if err = os.WriteFile(filepath.Join(*output, name), data, 0600); err != nil {
				return nil, err
			}
		}
		return struct {
			Binding        dumpformat.NativeJoinedBinding `json:"binding"`
			Commits        []uint64                       `json:"commits"`
			RegistrySHA256 string                         `json:"registry_sha256"`
		}{b, []uint64{sources[0].Commits, sources[1].Commits}, hash(registryBytes)}, nil
	default:
		return nil, errors.New("explicit public proof mode required")
	}
}

func main() {
	result, err := run(context.Background(), os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "public native proof refused:", err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
