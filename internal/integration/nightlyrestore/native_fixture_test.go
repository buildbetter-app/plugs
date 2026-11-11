package nightlyrestore_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const nativeContainerSHA = "46677d8d6021ee35611e81238fd037d33739e4e44eb5308430b90d5f0ba5f93a"

var nativeOriginalSHA = map[string]string{
	"public-shallow-boundary-v66.tar":                    "0afc3756eaf4094ffa6bb76441a3b48e0d142b1614885f6996c0b86880f6382d",
	"public-shallow-boundary-v66-physical-evidence.json": "de781286afd5d5fdc11e91bc406063ff3be133fd03aa944c53699c15994f2b20",
	"public-shallow-boundary-v66-facts.json":             "934a27be3d256e7e60dc52d9fc76437d1d2e056f786d268fc924ec3d9b0e2180",
	"public-shallow-boundary-v66-stderr.jsonl":           "cce9fd12e48de6ec0ff116581f43ef5530826586df893cb6150b3e131cff6e83",
	"public-shallow-boundary-v66-stdout.json":            "f8b445574233f69aab29d6d2895d50c022cd0f3a0247eb869c2a20221faf5999",
	"public-multiversion-v66.tar":                        "d27127f59239c50d83e6ab162339f210a950b483118ceda66f49094cd63802f3",
	"public-multiversion-v66-physical-evidence.json":     "2a0fd8e1f2f2fbb669db94f30acbc4d7d4973005de89da2dd6191439f92a40d5",
	"public-multiversion-v66-facts.json":                 "597aabc0f039a532a73d62b4311ed16da87ea8472c7c0b64c2c604883f2d3c39",
	"public-multiversion-unsupported-diagnostic.jsonl":   "f150db3897b0c1ab461f725591896e8527212ded056abef3af4d4f4aefb0ca55",
}

func nativeDecode(t *testing.T, data []byte, value any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(value); err != nil {
		t.Fatal(err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatal("native evidence contains trailing JSON", err)
	}
}

func nativeTarMembers(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	r := tar.NewReader(bytes.NewReader(data))
	members := map[string][]byte{}
	for {
		h, err := r.Next()
		if err == io.EOF {
			return members
		}
		if err != nil || h.Typeflag != tar.TypeReg || members[h.Name] != nil {
			t.Fatal("invalid or duplicate public fixture member", err)
		}
		members[h.Name], err = io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Nine unchanged outputs from original producer e25cf419, run 42786069732.
// This lane consumes its public/public_commit_history sources verbatim; the
// container is neither a new capture nor a two-project/private fixture.
func nativeOriginals(t *testing.T) map[string][]byte {
	t.Helper()
	path := "internal/integration/converterroundtrip/testdata/public-history-boundary-e25cf419.tar"
	paths := []string{filepath.Join("..", "converterroundtrip", "testdata", "public-history-boundary-e25cf419.tar")}
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		paths = append(paths, filepath.Join(root, os.Getenv("TEST_WORKSPACE"), path))
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if digest(data) != nativeContainerSHA {
			t.Fatal("original public native container changed")
		}
		members := nativeTarMembers(t, data)
		if len(members) != len(nativeOriginalSHA) {
			t.Fatal("original native fixture member census changed")
		}
		for name, hash := range nativeOriginalSHA {
			if digest(members[name]) != hash {
				t.Fatal("original public native output changed", name)
			}
		}
		return members
	}
	t.Fatal("declared original native fixture container is absent")
	return nil
}

type nativeRequest struct {
	Format         string                        `json:"format"`
	PolicySHA256   string                        `json:"policy_sha256"`
	ArchiveSHA256  string                        `json:"archive_sha256"`
	SourceStoreID  string                        `json:"source_store_id"`
	Projects       []string                      `json:"projects"`
	Admission      driver.RestoreImportAdmission `json:"admission"`
	IdempotencyKey string                        `json:"idempotency_key"`
	SourcePlan     driver.SourcePlanePlanV1      `json:"source_plan"`
}

// Use the receiving owner's executable policy constructor rather than a
// competing source-schema mapper. Only preparation executes in this process;
// publication and independent readback use the actual fresh driver below.
func nativeRequestFor(t *testing.T, data []byte, engine string) nativeRequest {
	t.Helper()
	a := readArchive(t, data)
	path := os.Getenv("NATIVE_RECEIVE_BINARY")
	if path == "" {
		t.Fatal("declared receiving executable is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), path)
	}
	binary, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lifetime := driver.CheckpointLifetimeProcess
	if engine != "memory" {
		lifetime = driver.CheckpointLifetimeRestart
	}
	policy := append(jsonBytes(t, map[string]any{
		"format": "plug-native-receiving-policy/1", "archive_sha256": a.SHA256(), "source_store_id": a.Manifest().StoreID,
		"projects": []string{"public"}, "sources": []dumpformat.SourceIdentity{{StoreID: a.Manifest().StoreID, Project: "public"}},
		"admission": driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "n1-test-restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, "idempotency_key": strings.Repeat("b", 32),
		"principal":     driver.SourcePlanePrincipalV1{Key: "owner", Source: "explicit N1 public fixture policy", Authority: "trusted-explicit/1", PrincipalJSON: jsonBytes(t, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})},
		"provider":      driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, OriginAdapter: "beads-workflow-origins/1"},
		"entity_policy": "durable-bead-identity", "tuple_policy": "copy-observed-exact", "status_policy": "source-name-identity", "target_id_policy": "source-id", "route_policy": "active-first",
		"field_mappings":  []driver.SourcePlaneStringPairV1{{From: "id", To: "id"}, {From: "title", To: "title"}, {From: "issue_type", To: "type"}, {From: "status", To: "status"}, {From: "metadata", To: "metadata"}, {From: "created_at", To: "created_at"}, {From: "priority", To: "priority"}, {From: "description", To: "description"}},
		"unmapped_fields": "opaque-metadata", "other_records": "inert-origin", "sidecars": "retain-captured-origins", "dependency_policy": "exact-typed",
	}), '\n')
	dir := t.TempDir()
	archivePath, policyPath := filepath.Join(dir, "source.tar"), filepath.Join(dir, "policy.json")
	for name, raw := range map[string][]byte{archivePath: data, policyPath: policy} {
		if err := os.WriteFile(name, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"prepare-request", "--archive", archivePath, "--policy", policyPath, "--driver", engine, "--expected-binary-sha256", digest(binary)}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("N1 public receiving preparation failed: %v %s", err, &stderr)
	}
	var request nativeRequest
	nativeDecode(t, stdout.Bytes(), &request)
	if request.Format != "plug-native-receiving-request/1" || request.PolicySHA256 != digest(policy) || request.ArchiveSHA256 != a.SHA256() || request.SourceStoreID != a.Manifest().StoreID || request.SourcePlan.Target.StoreID != "" || request.SourcePlan.Target.Incarnation != "" || request.SourcePlan.Target.Lifetime != lifetime {
		t.Fatal("receiving constructor changed explicit public source/target bindings")
	}
	sameJSON(t, "original native project identity", request.Projects, []string{"public"})
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		packet := filepath.Join(out, "native-constructor-"+strings.ReplaceAll(t.Name(), "/", "-"))
		if err := os.Mkdir(packet, 0700); err != nil {
			t.Fatal(err)
		}
		for name, raw := range map[string][]byte{"request.json": stdout.Bytes(), "policy.json": policy, "argv.json": jsonBytes(t, append([]string{path}, args...)), "stderr.txt": stderr.Bytes()} {
			if err := os.WriteFile(filepath.Join(packet, name), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("N1 public constructor binary_sha256=%s policy_sha256=%s request_sha256=%s", digest(binary), digest(policy), digest(stdout.Bytes()))
	return request
}

// Only declared damaged copies are rewritten for the no-write controls. The
// original positive fixtures and their physical sidecars remain byte-exact.
func damagedNativeArchive(t *testing.T, data []byte, change func(*dumpformat.NativeHistory)) []byte {
	t.Helper()
	members := nativeTarMembers(t, data)
	var manifest dumpformat.Manifest
	var native dumpformat.NativeHistory
	nativeDecode(t, members["manifest.json"], &manifest)
	nativeDecode(t, members[dumpformat.NativeHistoryMember], &native)
	change(&native)
	members[dumpformat.NativeHistoryMember] = append(jsonBytes(t, native), '\n')
	for name := range manifest.Files {
		manifest.Files[name] = dumpformat.File{SHA256: digest(members[name]), Size: int64(len(members[name]))}
	}
	members["manifest.json"] = append(jsonBytes(t, manifest), '\n')
	names := []string{}
	for name := range members {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var out bytes.Buffer
	w := tar.NewWriter(&out)
	for _, name := range append([]string{"manifest.json"}, names...) {
		raw := members[name]
		if err := w.WriteHeader(&tar.Header{Name: name, Size: int64(len(raw)), Mode: 0600, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(out.Bytes(), data) {
		t.Fatal("negative fixture did not change")
	}
	return out.Bytes()
}
