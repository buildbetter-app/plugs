package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type collectionCommandRequest struct {
	receivingRequest
	Preparations []int `json:"collection_source_preparations"`
}

type collectionCommandReceipt struct {
	receivingReceipt
	Collection struct {
		SourceCount      int    `json:"source_count"`
		MappedCommits    []int  `json:"mapped_commits"`
		ImportedReadback bool   `json:"imported_byte_readback"`
		ReopenedReadback bool   `json:"reopened_byte_readback"`
		Reopen           string `json:"reopen"`
		Receiving        []int  `json:"receiving_source_preparations"`
		Readback         []int  `json:"imported_readback_source_preparations"`
		Later            []int  `json:"later_readback_source_preparations"`
	} `json:"native_collection_verification"`
	Measurements struct {
		Decoded dumpformat.NativeDeltaDecodedUsage `json:"decoded"`
	} `json:"capture_set_measurements"`
}

func collectionCommandInputs(t *testing.T) ([]string, dumpformat.NativeJoinedBinding) {
	t.Helper()
	policy, err := os.ReadFile("testdata/real-store-import-policy-v2.md")
	if err != nil || hashBytes(policy) != publicRehearsalPolicyHash {
		t.Fatal(err)
	}
	p, err := dumpformat.NativeSchemaForVersion(66, 26)
	if err != nil {
		t.Fatal(err)
	}
	profile := encode(t, p)
	b := joinedPhysicalBinding(t)
	b.PolicySHA256, b.ProfileSHA256 = hashBytes(policy), hashBytes(profile)
	dir := t.TempDir()
	args := []string{"--capture-set", joinedPhysicalDirectory(t, "ops"), "--capture-set", joinedPhysicalDirectory(t, "sv2")}
	for _, f := range []struct {
		name, flag string
		data       []byte
	}{{"policy.md", "--policy", policy}, {"binding.json", "--bindings", encode(t, b)}, {"profile.json", "--profile", profile}} {
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, f.data, 0o600); err != nil {
			t.Fatal(err)
		}
		args = append(args, f.flag, path)
	}
	return args, b
}

// Unlike the legacy TAR helper, this records a directory's exact individual
// originals. Every command's argv/stdout/stderr/input packet stays separate.
func collectionCommand(t *testing.T, binary string, args []string, label string) ([]byte, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	err := cmd.Run()
	if root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); root != "" {
		dir := filepath.Join(root, label)
		if e := os.Mkdir(dir, 0o700); e != nil {
			t.Fatal(e)
		}
		for name, raw := range map[string][]byte{"argv.json": encode(t, append([]string{binary}, args...)), "stdout.json": out.Bytes(), "stderr.txt": errout.Bytes()} {
			if e := os.WriteFile(filepath.Join(dir, name), raw, 0o600); e != nil {
				t.Fatal(e)
			}
		}
		for i, flag := range args {
			if i+1 >= len(args) {
				continue
			}
			if flag == "--capture-set" {
				for _, name := range []string{"capture-set.json", "head.tar", "parts/00000000000000000001.part"} {
					raw, e := os.ReadFile(filepath.Join(args[i+1], name))
					if errors.Is(e, os.ErrNotExist) {
						continue
					}
					if e != nil {
						t.Fatal(e)
					}
					if e := os.WriteFile(filepath.Join(dir, strings.ReplaceAll(filepath.Join("source-"+string(rune('0'+i)), name), "/", "_")), raw, 0o600); e != nil {
						t.Fatal(e)
					}
				}
			} else if flag == "--policy" || flag == "--bindings" || flag == "--profile" || flag == "--request" {
				raw, e := os.ReadFile(args[i+1])
				if e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(dir, filepath.Base(args[i+1])), raw, 0o600); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	return out.Bytes(), errout.Bytes(), err
}

func TestNativeCollectionCommandConstructsAndReceivesPhysicalPair(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			args, binding := collectionCommandInputs(t)
			prepareArgs := append([]string{"prepare-request", "--driver", engine, "--expected-binary-sha256", binaryHash}, args...)
			raw, stderr, err := collectionCommand(t, binary, prepareArgs, "collection-"+engine+"-prepare")
			if err != nil {
				t.Fatalf("physical collection construction: %v %s", err, stderr)
			}
			var request collectionCommandRequest
			if err := decodeNativeSchemaFixture(raw, &request); err != nil {
				t.Fatal(err)
			}
			commitment := struct {
				Format string                         `json:"format"`
				Inputs dumpformat.NativeJoinedBinding `json:"inputs"`
			}{"plug-native-rehearsal-idempotency/2", binding}
			keyBytes, err := json.Marshal(commitment)
			if err != nil {
				t.Fatal(err)
			}
			archiveBytes, err := json.Marshal(binding)
			if err != nil {
				t.Fatal(err)
			}
			if request.IdempotencyKey != hashBytes(keyBytes) || request.ArchiveSHA256 != hashBytes(archiveBytes) || request.SourceStoreID != "merge-"+hashBytes(archiveBytes) || request.PolicySHA256 != publicRehearsalPolicyHash || !reflect.DeepEqual(request.Preparations, []int{1, 1}) || request.SourcePlan.Target.StoreID != "" || len(request.SourcePlan.Providers) != 1 || len(request.SourcePlan.Principals) != 1 || len(request.SourcePlan.Runs) != 0 {
				t.Fatal("external ordered commitment or authority differs")
			}
			requestPath := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(requestPath, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			target := ""
			if engine == "sqlite" {
				target = filepath.Join(t.TempDir(), "collection.db")
			} else if engine == "postgres" {
				target = identityPostgresDSN(t)
			}
			runArgs := append([]string{"--driver", engine, "--target", target, "--request", requestPath, "--expected-binary-sha256", binaryHash}, args...)
			stdout, errout, err := collectionCommand(t, binary, runArgs, "collection-"+engine+"-receive")
			if err != nil {
				t.Fatalf("physical collection receive: %v %s", err, errout)
			}
			var result collectionCommandReceipt
			if err := decodeNativeSchemaFixture(stdout, &result); err != nil {
				t.Fatal(err)
			}
			assertJoinedPhysicalHistories(t, result.Report.NativeHistories, binding)
			if result.Report.Receipt == nil || result.Report.IdempotencyKey != request.IdempotencyKey || result.Report.Restored.Head != model.Natural64(195) || len(result.Report.NativeHistories) != 2 || len(result.Report.NativeSchemas) != 2 || result.Report.SourcePlanes == nil || len(result.Report.SourcePlanes.Providers) != 1 || result.Collection.SourceCount != 2 || !reflect.DeepEqual(result.Collection.MappedCommits, []int{97, 97}) || !result.Collection.ImportedReadback || !result.Collection.ReopenedReadback || result.ArchiveSHA256 != request.ArchiveSHA256 || result.RequestSHA256 != hashBytes(raw) || result.BinarySHA256 != binaryHash || result.PolicySHA256 != publicRehearsalPolicyHash || result.SessionCeilingSeconds != 7200 {
				t.Fatal("committed two-source receipt/readback differs")
			}
			for i := 0; i < 2; i++ {
				if result.Collection.Receiving[i] != 1 || result.Collection.Readback[i] != 1 || request.Preparations[i]+result.Collection.Receiving[i]+result.Collection.Readback[i] != 3 || result.Collection.Later[i] != 1 {
					t.Fatal("observed constructor/receiving/readback preparation census differs")
				}
			}
			if engine == "memory" {
				if result.Collection.Reopen != "same-process-memory-lease-reacquired" {
					t.Fatal("memory lifetime changed")
				}
				return
			}
			if result.Collection.Reopen != "driver-reopened" {
				t.Fatal("SQL reopen omitted")
			}
			var store driver.Store
			if engine == "sqlite" {
				store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: target})
			} else {
				store, err = postgres.Open(context.Background(), driver.Config{DSN: target})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			profile, recovered, found, err := RecoverBoundSourceImport(context.Background(), store)
			if err != nil || !found {
				t.Fatal("independent published SQL recovery", err)
			}
			defer recovered.Close()
			if !reflect.DeepEqual(result.Report.Receipt, recovered.Receipt) || !reflect.DeepEqual(result.Report.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(result.Report.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(result.Report.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(result.Report.SourcePlanes, recovered.SourcePlanes) {
				t.Fatal("published SQL report channels differ")
			}
			openers := []historyverify.NativeCaptureSetOpener{}
			for _, path := range []string{args[1], args[3]} {
				path := path
				openers = append(openers, func(ctx context.Context, options dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error) {
					return dumpformat.ReadNativeCaptureSetDirectory(ctx, path, options)
				})
			}
			if err := VerifyNativeCaptureCollectionReadback(context.Background(), store, binding, openers, profile, recovered); err != nil {
				t.Fatal("external SQL full-revision byte readback", err)
			}
		})
	}
}

func TestNativeCollectionCommandRejectsChangedBindingPolicyProfileAndLegacyInputs(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	for _, attack := range []string{"swap", "hash", "policy", "profile", "legacy", "relabel-v1"} {
		t.Run(attack, func(t *testing.T) {
			args, binding := collectionCommandInputs(t)
			switch attack {
			case "swap":
				args[1], args[3] = args[3], args[1]
			case "hash":
				binding.Sources[1].Files[2].SHA256 = strings.Repeat("f", 64)
				if err := os.WriteFile(args[7], encode(t, binding), 0o600); err != nil {
					t.Fatal(err)
				}
			case "policy":
				if err := os.WriteFile(args[5], []byte("unapproved invented policy\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "profile":
				f, err := os.OpenFile(args[9], os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString("\n")
				if e := f.Close(); err != nil || e != nil {
					t.Fatal(err, e)
				}
			case "legacy":
				if err := os.Remove(filepath.Join(args[3], "capture-set.json")); err != nil {
					t.Fatal(err)
				}
			case "relabel-v1":
				binding.Format = "plug-native-joined-inputs/1"
				if err := os.WriteFile(args[7], encode(t, binding), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			argv := append([]string{"prepare-request", "--driver", "sqlite", "--expected-binary-sha256", binaryHash}, args...)
			stdout, stderr, err := collectionCommand(t, binary, argv, "collection-refusal-"+attack)
			if err == nil || len(stdout) != 0 || len(stderr) == 0 {
				t.Fatal("changed native collection input accepted", attack)
			}
		})
	}
}
