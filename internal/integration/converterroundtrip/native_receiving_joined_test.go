package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const publicRehearsalPolicyHash = "3afb19b934e69e90b9f8e966d5e4e4d1fab27a47686eacf38543e23d1c7542d9"

type publicOriginalBinding struct {
	RehearsalProject string `json:"rehearsal_project"`
	Database         string `json:"database"`
	SourceStoreID    string `json:"source_store_id"`
	SourceProject    string `json:"source_project"`
	ArchiveSHA256    string `json:"archive_sha256"`
}

type publicJoinedBinding struct {
	Format        string                  `json:"format"`
	PolicySHA256  string                  `json:"policy_sha256"`
	ProfileSHA256 string                  `json:"profile_sha256"`
	Sources       []publicOriginalBinding `json:"sources"`
}

func publicRehearsalInputs(t *testing.T) ([][]byte, []byte, []byte, []byte) {
	t.Helper()
	// These are declared synthetic histories. They are not copies of either
	// authorized private backup or a substitute for genuine d7 proof.
	originals := [][]byte{native66ReceivingFixture(t, "ops", "wip"), native66ReceivingFixture(t, "sv2", "done")}
	return publicRehearsalInputsFor(t, originals)
}

func publicRehearsalInputsFor(t *testing.T, originals [][]byte) ([][]byte, []byte, []byte, []byte) {
	t.Helper()
	if len(originals) != 2 {
		t.Fatal("exactly two ordered public rehearsal originals required")
	}
	policy, err := os.ReadFile("testdata/real-store-import-policy-v2.md")
	if err != nil || hashBytes(policy) != publicRehearsalPolicyHash {
		t.Fatal("exact read-only policy bytes required", err)
	}
	p, err := dumpformat.NativeSchemaForVersion(66, 26)
	if err != nil {
		t.Fatal(err)
	}
	profile := encode(t, p)
	b := publicJoinedBinding{Format: "plug-native-joined-inputs/1", PolicySHA256: hashBytes(policy), ProfileSHA256: hashBytes(profile)}
	for i, raw := range originals {
		a, err := dumpformat.Read(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		b.Sources = append(b.Sources, publicOriginalBinding{RehearsalProject: []string{"cody-ops", "signals-v2"}[i], Database: []string{"ops", "sv2"}[i], SourceStoreID: a.Manifest().StoreID, SourceProject: a.Manifest().Projects[0].Prefix, ArchiveSHA256: hashBytes(raw)})
	}
	return originals, policy, encode(t, b), profile
}

func joinedArguments(t *testing.T, originals [][]byte, policy, binding, profile []byte) []string {
	t.Helper()
	dir := t.TempDir()
	args := []string{}
	for i, raw := range originals {
		path := filepath.Join(dir, []string{"ops.tar", "sv2.tar"}[i])
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		args = append(args, "--archive", path)
	}
	for _, f := range []struct {
		name, flag string
		data       []byte
	}{{"policy.md", "--policy", policy}, {"bindings.json", "--bindings", binding}, {"profile.json", "--profile", profile}} {
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, f.data, 0o600); err != nil {
			t.Fatal(err)
		}
		args = append(args, f.flag, path)
	}
	return args
}

func joinedCommand(t *testing.T, binary string, args []string, name string) ([]byte, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		dir := filepath.Join(out, name)
		if e := os.Mkdir(dir, 0o700); e != nil {
			t.Fatal(e)
		}
		for file, data := range map[string][]byte{"argv.json": encode(t, append([]string{binary}, args...)), "stdout.json": stdout.Bytes(), "stderr.txt": stderr.Bytes()} {
			if e := os.WriteFile(filepath.Join(dir, file), data, 0o600); e != nil {
				t.Fatal(e)
			}
		}
		for i, arg := range args {
			if (arg == "--archive" || arg == "--policy" || arg == "--bindings" || arg == "--profile" || arg == "--request") && i+1 < len(args) {
				raw, e := os.ReadFile(args[i+1])
				if e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(dir, fmt.Sprintf("input-%02d-%s", i, filepath.Base(args[i+1]))), raw, 0o600); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

func TestNativeReceivingRehearsalPolicyJoinsTwoOriginals(t *testing.T) {
	originals, policy, binding, profile := publicRehearsalInputs(t)
	exerciseNativeReceivingRehearsalPolicy(t, originals, policy, binding, profile, "joined", true)
}

// These two synthetic source archives exercise the exact ordered policyv2
// constructor/receiver route. They do not claim genuine private namespaces or
// replace the producer's independently supplied shallow and migration inputs.
func TestSyntheticNativeReceivingRehearsalPolicyPreservesBoundaryReports(t *testing.T) {
	for _, bothBounded := range []bool{false, true} {
		label := map[bool]string{false: "boundary-and-complete", true: "two-boundaries"}[bothBounded]
		t.Run(label, func(t *testing.T) {
			left := syntheticNativeBoundaryFromArchive(t, native66ReceivingFixture(t, "ops", "wip"))
			right := native66ReceivingFixture(t, "sv2", "done")
			if bothBounded {
				right = syntheticNativeBoundaryFromArchive(t, right)
			}
			originals, policy, binding, profile := publicRehearsalInputsFor(t, [][]byte{left, right})
			exerciseNativeReceivingRehearsalPolicy(t, originals, policy, binding, profile, "joined-"+label, false)
		})
	}
}

func exerciseNativeReceivingRehearsalPolicy(t *testing.T, originals [][]byte, policy, binding, profile []byte, label string, complete bool) {
	t.Helper()
	binary, hash := receivingBinary(t)
	joined, err := dumpformat.Merge(bytes.NewReader(originals[0]), bytes.NewReader(originals[1]))
	if err != nil {
		t.Fatal(err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(joined))
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			args := joinedArguments(t, originals, policy, binding, profile)
			prepare := append([]string{"prepare-request", "--driver", engine, "--expected-binary-sha256", hash}, args...)
			stdout, stderr, err := joinedCommand(t, binary, prepare, label+"-"+engine+"-prepare")
			if err != nil {
				t.Fatalf("joined construction refused: %v %s", err, stderr)
			}
			var request receivingRequest
			if err := decodeNativeSchemaFixture(stdout, &request); err != nil {
				t.Fatal(err)
			}
			var b publicJoinedBinding
			if err := json.Unmarshal(binding, &b); err != nil {
				t.Fatal(err)
			}
			// Independently encode the documented ordered struct commitment; a
			// trailing newline in the profile input is part of its own digest.
			commitment := struct {
				Format        string                  `json:"format"`
				ArchiveSHA256 []string                `json:"archive_sha256"`
				Bindings      []publicOriginalBinding `json:"bindings"`
				ProfileSHA256 string                  `json:"profile_sha256"`
				PolicySHA256  string                  `json:"policy_sha256"`
			}{"plug-native-rehearsal-idempotency/1", []string{hashBytes(originals[0]), hashBytes(originals[1])}, b.Sources, hashBytes(profile), hashBytes(policy)}
			encoded, err := json.Marshal(commitment)
			if err != nil {
				t.Fatal(err)
			}
			if request.IdempotencyKey != hashBytes(encoded) || request.PolicySHA256 != publicRehearsalPolicyHash || request.ArchiveSHA256 != hashBytes(joined) || !bytes.Equal(bytes.TrimSpace(request.OriginalInputs), bytes.TrimSpace(binding)) || len(request.SourcePlan.Runs) != 0 || len(request.SourcePlan.Principals) != 1 || request.Admission.Actor.ID != "plug-restore-rehearsal" {
				t.Fatal("joined identity/policy/inert plan commitment differs")
			}
			requestPath := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(requestPath, stdout, 0o600); err != nil {
				t.Fatal(err)
			}
			target := ""
			if engine == "sqlite" {
				target = filepath.Join(t.TempDir(), "joined.db")
			} else if engine == "postgres" {
				target = identityPostgresDSN(t)
			}
			receive := append([]string{"--driver", engine, "--target", target, "--request", requestPath, "--expected-binary-sha256", hash}, args...)
			out, errout, err := joinedCommand(t, binary, receive, label+"-"+engine+"-receive")
			if err != nil {
				t.Fatalf("joined receiving refused: %v %s", err, errout)
			}
			var result receivingReceipt
			if err := decodeNativeSchemaFixture(out, &result); err != nil {
				t.Fatal(err)
			}
			if result.SessionCeilingSeconds != 7200 || result.PolicySHA256 != hashBytes(policy) || result.BinarySHA256 != hash || result.ArchiveSHA256 != hashBytes(joined) || result.RequestSHA256 != hashBytes(stdout) || len(result.Report.NativeSchemas) != 2 || len(result.Report.NativeFamilies) != 60 {
				t.Fatal("joined committed receipt/census differs")
			}
			assertNativeReceivingHistoryReport(t, archive, result.Report, complete)
			wantReopen := "driver-reopened"
			if engine == "memory" {
				wantReopen = "same-process-memory"
			}
			if result.Verification.Reopen != wantReopen {
				t.Fatal("joined receiving lifetime differs from actual driver")
			}
			for _, schema := range result.Report.NativeSchemas {
				if len(schema.Profile.Absences) != 7 || schema.SchemaDefaultCount != 0 || len(schema.PlugDefaults) != 1 || schema.PlugDefaults[0].Count != 1 || schema.OpaqueMetadataFields == 0 {
					t.Fatal("actual opaque/default/schema accounting missing")
				}
			}
			if engine == "memory" {
				return
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
			bound, recovered, found, err := RecoverBoundSourceImport(context.Background(), store)
			if err != nil || !found || !reflect.DeepEqual(result.Report.Receipt, recovered.Receipt) || !reflect.DeepEqual(result.Report.NativeSchemas, recovered.NativeSchemas) || !reflect.DeepEqual(result.Report.NativeFamilies, recovered.NativeFamilies) || !reflect.DeepEqual(result.Report.NativeHistories, recovered.NativeHistories) || !reflect.DeepEqual(result.Report.SourcePlanes, recovered.SourcePlanes) || !bytes.Equal(recovered.Archive.TarBytes(), joined) {
				t.Fatal("joined destination recovery differs", err)
			}
			if bound.IdempotencyKey != request.IdempotencyKey || !reflect.DeepEqual(bound.Admission.Actor, request.Admission.Actor) {
				t.Fatal("committed admission/idempotency differs")
			}
			assertNativeReceivingHistoryReport(t, archive, recovered, complete)
			latest, err := store.Read(context.Background(), driver.View{Incarnation: recovered.Restored.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := latest.Get(context.Background(), []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
			if err != nil || len(rows) != 1 {
				t.Fatal("joined committed history evidence missing", err)
			}
			var history driver.HistoryImportEvidence
			decode(t, rows[0].Body, &history)
			if err := latest.Close(); err != nil {
				t.Fatal(err)
			}
			assertNativeHistoryCoverageReadback(t, store, archive, recovered, history)
			r, err := store.Read(context.Background(), driver.View{Incarnation: result.Report.Restored.Incarnation, At: &result.Report.Restored.Head})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			a, err := dumpformat.Read(bytes.NewReader(joined))
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range a.Records("bead") {
				rows, err := r.Get(context.Background(), []model.Ref{model.RefByID(row.ID, model.Natural64(0))})
				if err != nil || len(rows) != 1 {
					t.Fatal(err)
				}
				var actual struct {
					Metadata map[string]json.RawMessage `json:"metadata"`
				}
				decode(t, rows[0].Body, &actual)
				for _, field := range []string{"assignee", "owner", "created_by", "timeout_ns"} {
					if !bytes.Equal(actual.Metadata["beads."+field], bytes.TrimSpace(encode(t, row.Source.Fields[field]))) {
						t.Fatal("opaque actor/exact source metadata differs", field)
					}
				}
			}
			for i, raw := range originals {
				path := args[2*i+1]
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(raw, after) {
					t.Fatal("original input changed", err)
				}
			}
		})
	}
}

func TestNativeReceivingRehearsalPolicyRefusesUnboundOrderAndAbortsSession(t *testing.T) {
	binary, hash := receivingBinary(t)
	originals, policy, binding, profile := publicRehearsalInputs(t)
	var b publicJoinedBinding
	decode(t, binding, &b)
	for _, name := range []string{"swapped-originals", "wrong-policy", "wrong-profile", "wrong-database", "profile-encoding", "historical-id-collision"} {
		t.Run(name, func(t *testing.T) {
			inputs := append([][]byte(nil), originals...)
			p, prof, bind := bytes.Clone(policy), bytes.Clone(profile), bytes.Clone(binding)
			switch name {
			case "swapped-originals":
				inputs[0], inputs[1] = inputs[1], inputs[0]
			case "wrong-policy":
				p = append(p, ' ')
			case "wrong-profile":
				prof = bytes.Replace(prof, []byte(`"main_version":66`), []byte(`"main_version":72`), 1)
			case "profile-encoding":
				prof = append([]byte(" "), prof...)
				copy := b
				copy.ProfileSHA256 = hashBytes(prof)
				bind = encode(t, copy)
			case "historical-id-collision":
				copy := b
				copy.Sources = append([]publicOriginalBinding(nil), b.Sources...)
				for i, original := range inputs {
					members := tarMembers(t, original)
					var m dumpformat.Manifest
					var h dumpformat.NativeHistory
					decode(t, members["manifest.json"], &m)
					decode(t, members[dumpformat.NativeHistoryMember], &h)
					// Only a retained old-schema row changes. Current IDs stay disjoint.
					h.Images[0].Tables["issues"].Rows[0]["id"] = "historical-overlap"
					members[dumpformat.NativeHistoryMember] = encode(t, h)
					raw := members[dumpformat.NativeHistoryMember]
					m.Files[dumpformat.NativeHistoryMember] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
					members["manifest.json"] = encode(t, m)
					inputs[i] = writeTarMembers(t, members)
					if _, err := dumpformat.Read(bytes.NewReader(inputs[i])); err != nil {
						t.Fatal("synthetic history collision input invalid", err)
					}
					copy.Sources[i].ArchiveSHA256 = hashBytes(inputs[i])
				}
				bind = encode(t, copy)
			case "wrong-database":
				copy := b
				copy.Sources = append([]publicOriginalBinding(nil), b.Sources...)
				copy.Sources[0].Database = "sv2"
				bind = encode(t, copy)
			}
			args := append([]string{"prepare-request", "--driver", "sqlite", "--expected-binary-sha256", hash}, joinedArguments(t, inputs, p, bind, prof)...)
			stdout, stderr, err := joinedCommand(t, binary, args, "joined-negative-"+name)
			if err == nil || len(stdout) != 0 {
				t.Fatal("unbound constructor produced request")
			}
			if name == "historical-id-collision" && (!bytes.Contains(stderr, []byte("cross_project_id_collision")) || !bytes.Contains(stderr, []byte("count=1"))) {
				t.Fatal("historical overlap lacked typed/count refusal", string(stderr))
			}
		})
	}
	args := joinedArguments(t, originals, policy, binding, profile)
	prepare := append([]string{"prepare-request", "--driver", "sqlite", "--expected-binary-sha256", hash}, args...)
	request, stderr, err := joinedCommand(t, binary, prepare, "joined-abort-prepare")
	if err != nil {
		t.Fatal(err, string(stderr))
	}
	requestPath := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(requestPath, request, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Run("expiry", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "expired.db")
		receive := append([]string{"--driver", "sqlite", "--target", target, "--request", requestPath, "--expected-binary-sha256", hash, "--session-timeout", "1ns"}, args...)
		stdout, _, err := joinedCommand(t, binary, receive, "joined-expiry")
		if err == nil || len(stdout) != 0 {
			t.Fatal("expired session succeeded")
		}
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatal("expired session created target", err)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "cancelled.db")
		receive := append([]string{"--driver", "sqlite", "--target", target, "--request", requestPath, "--expected-binary-sha256", hash}, args...)
		cmd := exec.Command(binary, receive...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		cancelled := false
		for !cancelled {
			select {
			case err := <-done:
				t.Fatalf("receiver finished before cancellation: %v %s", err, &stderr)
			case <-timer.C:
				_ = cmd.Process.Kill()
				<-done
				t.Fatal("no actual staging observed")
			case <-ticker.C:
				stages, err := filepath.Glob(filepath.Join(dir, ".native-receive-*", "store.db"))
				if err != nil {
					t.Fatal(err)
				}
				if len(stages) != 0 {
					if err := cmd.Process.Signal(os.Interrupt); err != nil {
						t.Fatal(err)
					}
					cancelled = true
				}
			}
		}
		if err := <-done; err == nil || stdout.Len() != 0 {
			t.Fatal("cancelled actual staging produced success")
		}
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatal("cancelled staging published destination", err)
		}
		stages, _ := filepath.Glob(filepath.Join(dir, ".native-receive-*"))
		if len(stages) != 0 {
			t.Fatal("cancelled staging not deleted")
		}
		if !strings.Contains(stderr.String(), "canceled") {
			t.Fatal("cancel reason missing", stderr.String())
		}
	})
}
