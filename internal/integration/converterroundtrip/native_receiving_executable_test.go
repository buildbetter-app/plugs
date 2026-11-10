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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type receivingRequest struct {
	Format         string                        `json:"format"`
	PolicySHA256   string                        `json:"policy_sha256,omitempty"`
	ArchiveSHA256  string                        `json:"archive_sha256"`
	SourceStoreID  string                        `json:"source_store_id"`
	Projects       []string                      `json:"projects"`
	Admission      driver.RestoreImportAdmission `json:"admission"`
	IdempotencyKey string                        `json:"idempotency_key"`
	SourcePlan     driver.SourcePlanePlanV1      `json:"source_plan"`
	OriginalInputs json.RawMessage               `json:"original_inputs,omitempty"`
}

type receivingReceipt struct {
	Format        string `json:"format"`
	PolicySHA256  string `json:"policy_sha256,omitempty"`
	BinarySHA256  string `json:"binary_sha256"`
	RequestSHA256 string `json:"request_sha256"`
	ArchiveSHA256 string `json:"archive_sha256"`
	Driver        string `json:"driver"`
	HistoryMode   string `json:"history_mode"`
	RestoreTime   string `json:"restore_time"`
	Verification  struct {
		CurrentFamilies      int    `json:"current_families"`
		CurrentRows          int    `json:"current_rows"`
		HistoryImages        int    `json:"history_images"`
		HistoryTables        int    `json:"history_tables"`
		HistoryRows          int    `json:"history_rows"`
		ArchiveBlob          bool   `json:"archive_blob"`
		Reopen               string `json:"reopen"`
		OpaqueMetadataFields int    `json:"opaque_metadata_fields"`
	} `json:"verification"`
	Report                   Report          `json:"committed_report"`
	OriginalInputs           json.RawMessage `json:"original_inputs,omitempty"`
	SessionCeilingSeconds    int64           `json:"session_ceiling_seconds,omitempty"`
	CrossProjectIDCollisions int             `json:"cross_project_id_collisions"`
}

func receivingBinary(t *testing.T) (string, string) {
	t.Helper()
	path := os.Getenv("NATIVE_RECEIVE_BINARY")
	if path == "" {
		t.Fatal("built receiving executable is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		if err := os.WriteFile(filepath.Join(out, "native-receive"), raw, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return path, hashBytes(raw)
}

// Request construction is deliberately outside the command. Each source,
// disposition, principal, route and archive commitment is explicit input.
func receivingRequestFor(t *testing.T, data []byte, actualVersion int, engine string) receivingRequest {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	defer store.Close()
	lease, err := store.Lease(context.Background(), "public-request-planner", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(context.Background())
	profile := driver.RestoreImportProfile{}
	if actualVersion != 0 {
		profile = actualOwnVersionProfile(t, archive, data, store, lease)
	} else {
		profile = boundNativeTableProfile(t, data, store, lease)
	}
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	plan.Target.StoreID, plan.Target.Incarnation = "", ""
	lifetime := driver.CheckpointLifetimeProcess
	if engine != "memory" {
		lifetime = driver.CheckpointLifetimeRestart
	}
	plan.Target.Lifetime = lifetime
	for i := range plan.Providers {
		plan.Providers[i].Lifetime = lifetime
		plan.Providers[i].LogicalImageSHA256, plan.Providers[i].LogicalImageSize = "", 0
	}
	projects := []string{}
	for _, project := range archive.Manifest().Projects {
		projects = append(projects, project.Prefix)
	}
	return receivingRequest{Format: "plug-native-receiving-request/1", ArchiveSHA256: archive.SHA256(), SourceStoreID: archive.Manifest().StoreID, Projects: projects, Admission: profile.Admission, IdempotencyKey: profile.IdempotencyKey, SourcePlan: plan}
}

func receivingPolicyFor(t *testing.T, data []byte, engine string) []byte {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	projects, sources := []string{}, []dumpformat.SourceIdentity{}
	for _, project := range archive.Manifest().Projects {
		projects = append(projects, project.Prefix)
	}
	for _, scope := range archive.NativeSourceHistories() {
		sources = append(sources, dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]})
	}
	lifetime := driver.CheckpointLifetimeProcess
	if engine != "memory" {
		lifetime = driver.CheckpointLifetimeRestart
	}
	fields := []driver.SourcePlaneStringPairV1{{From: "id", To: "id"}, {From: "title", To: "title"}, {From: "issue_type", To: "type"}, {From: "status", To: "status"}, {From: "metadata", To: "metadata"}, {From: "created_at", To: "created_at"}, {From: "priority", To: "priority"}, {From: "description", To: "description"}}
	return encode(t, map[string]any{
		"format": "plug-native-receiving-policy/1", "archive_sha256": archive.SHA256(), "source_store_id": archive.Manifest().StoreID, "projects": projects, "sources": sources,
		"admission": driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, "idempotency_key": strings.Repeat("b", 32),
		"principal":     driver.SourcePlanePrincipalV1{Key: "owner", Source: "explicit public constructor policy", Authority: "trusted-explicit/1", PrincipalJSON: encode(t, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})},
		"provider":      driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, OriginAdapter: "beads-workflow-origins/1"},
		"entity_policy": "durable-bead-identity", "tuple_policy": "copy-observed-exact", "status_policy": "source-name-identity", "target_id_policy": "source-id", "route_policy": "active-first", "field_mappings": fields, "unmapped_fields": "origin-only", "other_records": "inert-origin", "sidecars": "retain-captured-origins", "dependency_policy": "exact-typed",
	})
}

func prepareReceivingRequest(t *testing.T, binary, binaryHash, engine string, data, policy []byte, name string) receivingRequest {
	t.Helper()
	dir := t.TempDir()
	archivePath, policyPath := filepath.Join(dir, "source.tar"), filepath.Join(dir, "policy.json")
	for path, raw := range map[string][]byte{archivePath: data, policyPath: policy} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"prepare-request", "--archive", archivePath, "--policy", policyPath, "--driver", engine, "--expected-binary-sha256", binaryHash}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("general request construction failed: %v %s", err, &stderr)
	}
	var request receivingRequest
	if err := decodeNativeSchemaFixture(stdout.Bytes(), &request); err != nil {
		t.Fatal(err)
	}
	if request.PolicySHA256 != hashBytes(policy) || request.ArchiveSHA256 != hashBytes(data) || request.SourcePlan.Target.StoreID != "" || request.SourcePlan.Target.Incarnation != "" {
		t.Fatal("constructed explicit-policy source binding differs")
	}
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		packet := filepath.Join(out, name+"-constructor")
		if err := os.Mkdir(packet, 0o700); err != nil {
			t.Fatal(err)
		}
		for file, raw := range map[string][]byte{"request.json": stdout.Bytes(), "policy.json": policy, "argv.json": encode(t, append([]string{binary}, args...)), "stderr.txt": stderr.Bytes()} {
			if err := os.WriteFile(filepath.Join(packet, file), raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return request
}

func executeReceiving(t *testing.T, binary, binaryHash, engine, target string, data []byte, request receivingRequest, name string, policies ...[]byte) ([]byte, []byte, error) {
	t.Helper()
	dir := t.TempDir()
	archivePath, requestPath := filepath.Join(dir, "source.tar"), filepath.Join(dir, "request.json")
	requestBytes := encode(t, request)
	for path, raw := range map[string][]byte{archivePath: data, requestPath: requestBytes} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"--driver", engine, "--target", target, "--archive", archivePath, "--request", requestPath, "--expected-binary-sha256", binaryHash}
	if len(policies) != 0 {
		policyPath := filepath.Join(dir, "policy.json")
		if err := os.WriteFile(policyPath, policies[0], 0o600); err != nil {
			t.Fatal(err)
		}
		args = append(args, "--policy", policyPath)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		packet := filepath.Join(out, name)
		if err := os.Mkdir(packet, 0o700); err != nil {
			t.Fatal(err)
		}
		for file, raw := range map[string][]byte{"request.json": requestBytes, "source.tar": data, "stdout.json": stdout.Bytes(), "stderr.txt": stderr.Bytes(), "argv.json": encode(t, append([]string{binary}, args...))} {
			if err := os.WriteFile(filepath.Join(packet, file), raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	for path, want := range map[string][]byte{archivePath: data, requestPath: requestBytes} {
		actual, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(actual, want) {
			t.Fatal("receiving changed input bytes", readErr)
		}
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

func inspectReceivingTarget(t *testing.T, engine, target string, data []byte, actualVersion int, expected Report) []byte {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	var err error
	if engine == "sqlite" {
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: target})
	} else {
		store, err = postgres.Open(ctx, driver.Config{DSN: target})
	}
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lease, err := store.Lease(ctx, "public-receiving-verifier", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release(ctx)
	profile, report, found, err := RecoverBoundSourceImport(ctx, store)
	if err != nil || !found || !reflect.DeepEqual(expected.Receipt, report.Receipt) || !reflect.DeepEqual(expected.NativeSchemas, report.NativeSchemas) || !reflect.DeepEqual(expected.NativeFamilies, report.NativeFamilies) || !reflect.DeepEqual(expected.SourcePlanes, report.SourcePlanes) {
		t.Fatal("executable committed receipt differs from actual destination recovery", err)
	}
	if actualVersion != 0 {
		assertActualOwnVersionReceiving(t, store, profile, report, data)
	} else {
		assertNative66Receiving(t, store, report)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := capability.LoadCheckpoint(ctx, plan.Providers[0].Namespace)
	if err != nil {
		t.Fatal(err)
	}
	return encode(t, struct {
		Core       any
		Checkpoint any
	}{reverseState(t, store), checkpoint})
}

func TestNativeReceivingExecutableSynthetic66AndActual66And72(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	for _, version := range []int{0, 66, 72} {
		name := fmt.Sprintf("actual-v%d", version)
		var data []byte
		if version == 0 {
			name = "synthetic-v66"
			var err error
			data, err = dumpformat.Merge(bytes.NewReader(native66ReceivingFixture(t, "pl", "active")), bytes.NewReader(native66ReceivingFixture(t, "qa", "done")))
			if err != nil {
				t.Fatal(err)
			}
		} else {
			_, data = actualOwnVersionArchive(t, version)
		}
		t.Run(name, func(t *testing.T) {
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					target := ""
					if engine == "sqlite" {
						target = filepath.Join(t.TempDir(), "receiving.db")
					}
					if engine == "postgres" {
						target = identityPostgresDSN(t)
					}
					policy := receivingPolicyFor(t, data, engine)
					request := prepareReceivingRequest(t, binary, binaryHash, engine, data, policy, name+"-"+engine)
					stdout, stderr, err := executeReceiving(t, binary, binaryHash, engine, target, data, request, name+"-"+engine, policy)
					if err != nil {
						t.Fatalf("built receiver failed: %v %s", err, stderr)
					}
					var result receivingReceipt
					if err := decodeNativeSchemaFixture(stdout, &result); err != nil {
						t.Fatal("executable receipt", err)
					}
					if result.Format != "plug-native-receiving-receipt/1" || result.PolicySHA256 != hashBytes(policy) || result.BinarySHA256 != binaryHash || result.ArchiveSHA256 != hashBytes(data) || result.RequestSHA256 != hashBytes(encode(t, request)) || result.Driver != engine || result.HistoryMode != "full" || !result.Verification.ArchiveBlob || result.Report.Receipt == nil {
						t.Fatal("executable/input/committed receipt binding differs")
					}
					archive, err := dumpformat.Read(bytes.NewReader(data))
					if err != nil {
						t.Fatal(err)
					}
					families, currentRows, images, tables, rows := 0, 0, 0, 0, 0
					when, err := time.Parse(time.RFC3339Nano, result.RestoreTime)
					if err != nil {
						t.Fatal(err)
					}
					_, accounts, err := nativeCurrentRecords(archive, when)
					if err != nil {
						t.Fatal(err)
					}
					for _, account := range accounts {
						families++
						currentRows += account.ImportedRows
					}
					for _, scope := range archive.NativeSourceHistories() {
						for _, image := range scope.Images {
							images++
							for _, table := range image.Tables {
								tables++
								rows += len(table.Rows)
							}
						}
					}
					v := result.Verification
					if v.CurrentFamilies != families || v.CurrentRows != currentRows || v.HistoryImages != images || v.HistoryTables != tables || v.HistoryRows != rows {
						t.Fatal("executable verified current/history census differs")
					}
					wantSchemas := len(archive.NativeSourceHistories())
					if version == 72 {
						wantSchemas = 0
					}
					if len(result.Report.NativeSchemas) != wantSchemas {
						t.Fatal("executable source-schema receipt count differs")
					}
					for _, schema := range result.Report.NativeSchemas {
						if schema.Profile.MainVersion != 66 || schema.Profile.IgnoredVersion != 26 || len(schema.Profile.Absences) != 7 || schema.SchemaDefaultCount != 0 || len(schema.PlugDefaults) != 1 || schema.PlugDefaults[0].Count != 1 || len(schema.TargetOmissions) != 2 {
							t.Fatal("executable counted defaults/omissions differ")
						}
					}
					if engine == "memory" {
						if v.Reopen != "same-process-memory" {
							t.Fatal("memory lifetime mislabeled")
						}
						return
					}
					if v.Reopen != "driver-reopened" {
						t.Fatal("persistent receiving not reopened")
					}
					before := inspectReceivingTarget(t, engine, target, data, version, result.Report)
					stdout, stderr, err = executeReceiving(t, binary, binaryHash, engine, target, data, request, name+"-"+engine+"-populated", policy)
					if err == nil || len(stdout) != 0 || !strings.Contains(string(stderr), "not empty") {
						t.Fatal("populated target must refuse without success receipt", err, string(stderr))
					}
					after := inspectReceivingTarget(t, engine, target, data, version, result.Report)
					if !bytes.Equal(before, after) {
						t.Fatal("populated target refusal changed core/checkpoint")
					}
				})
			}
		})
	}
}

func TestNativeReceivingExecutableRefusesBeforeDestinationCreation(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	_, data := actualOwnVersionArchive(t, 66)
	request := receivingRequestFor(t, data, 66, "sqlite")
	for _, kind := range []string{"binary", "source", "project", "version", "history-gap"} {
		t.Run(kind, func(t *testing.T) {
			var input receivingRequest
			decode(t, encode(t, request), &input)
			payload, expectedHash := data, binaryHash
			reason := "binding differs"
			switch kind {
			case "binary":
				expectedHash = strings.Repeat("0", 64)
			case "source":
				input.SourceStoreID = "wrong-public-source"
			case "project":
				input.Projects = []string{"wrong-public-project"}
			case "version", "history-gap":
				members := tarMembers(t, data)
				if kind == "version" {
					var manifest dumpformat.Manifest
					decode(t, members["manifest.json"], &manifest)
					manifest.MainVersion = 65
					members["manifest.json"] = encode(t, manifest)
					reason = "schema"
				} else {
					var history dumpformat.NativeHistory
					decode(t, members[dumpformat.NativeHistoryMember], &history)
					missing := history.Images[1]
					inputImage := append([]dumpformat.NativeHistoryImage(nil), history.Images[:1]...)
					history.Images = append(inputImage, history.Images[2:]...)
					history.Complete = false
					history.Gaps = []dumpformat.NativeHistoryGap{{Sequence: missing.Sequence, CommitID: missing.CommitID, Family: dumpformat.DurableEntityTable, Reason: "historical-image-unavailable"}}
					members[dumpformat.NativeHistoryMember] = encode(t, history)
					reason = "historical-image-unavailable"
				}
				payload = nativeReceivingFixtureMembers(t, members)
				input.ArchiveSHA256, input.SourcePlan.ArchiveSHA256 = hashBytes(payload), hashBytes(payload)
			}
			target := filepath.Join(t.TempDir(), "must-not-exist.db")
			stdout, stderr, err := executeReceiving(t, binary, expectedHash, "sqlite", target, payload, input, "refusal-"+kind)
			if err == nil || len(stdout) != 0 || !strings.Contains(string(stderr), reason) {
				t.Fatal("source/executable/history refusal missing", err, string(stderr))
			}
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatal("refused receiving created destination", err)
			}
		})
	}
}

func TestNativeReceivingPolicyConstructionRequiresExplicitBindingsAndSemantics(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	_, data := actualOwnVersionArchive(t, 66)
	original := receivingPolicyFor(t, data, "sqlite")
	for _, kind := range []string{"archive", "source-scope", "missing-origin-policy", "workflow-inference", "duplicate-field"} {
		t.Run(kind, func(t *testing.T) {
			var policy map[string]json.RawMessage
			decode(t, original, &policy)
			reason := "binding differs"
			switch kind {
			case "archive":
				policy["archive_sha256"] = encode(t, strings.Repeat("0", 64))
			case "source-scope":
				policy["sources"] = encode(t, []dumpformat.SourceIdentity{{StoreID: "unbound-source", Project: "public"}})
			case "missing-origin-policy":
				delete(policy, "other_records")
				reason = "unsupported or missing explicit construction policy"
			case "workflow-inference":
				policy["entity_policy"] = encode(t, "guess-workflow-from-source")
				reason = "unsupported or missing explicit construction policy"
			case "duplicate-field":
				policy["field_mappings"] = encode(t, []driver.SourcePlaneStringPairV1{{From: "id", To: "id"}, {From: "id", To: "title"}})
				reason = "duplicate or empty explicit field mapping"
			}
			policyBytes := encode(t, policy)
			dir := t.TempDir()
			archivePath, policyPath := filepath.Join(dir, "source.tar"), filepath.Join(dir, "policy.json")
			for path, raw := range map[string][]byte{archivePath: data, policyPath: policyBytes} {
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			args := []string{"prepare-request", "--archive", archivePath, "--policy", policyPath, "--driver", "sqlite", "--expected-binary-sha256", binaryHash}
			cmd := exec.CommandContext(ctx, binary, args...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err == nil || stdout.Len() != 0 || !strings.Contains(stderr.String(), reason) {
				t.Fatal("constructor failed to refuse unbound/undeclared semantic policy", err, stderr.String())
			}
			if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
				packet := filepath.Join(out, "policy-refusal-"+kind)
				if err := os.Mkdir(packet, 0o700); err != nil {
					t.Fatal(err)
				}
				for file, raw := range map[string][]byte{"policy.json": policyBytes, "stdout.json": stdout.Bytes(), "stderr.txt": stderr.Bytes(), "argv.json": encode(t, append([]string{binary}, args...))} {
					if err := os.WriteFile(filepath.Join(packet, file), raw, 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestNativeReceivingPolicyRetainsDependencyRowsAsInertOrigins(t *testing.T) {
	binary, hash := receivingBinary(t)
	data := native66DependencyReceivingFixture(t)
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Records("edge")) == 0 {
		t.Fatal("nonempty dependency control required")
	}
	var policy map[string]json.RawMessage
	decode(t, receivingPolicyFor(t, data, "memory"), &policy)
	delete(policy, "dependency_policy")
	policyBytes := encode(t, policy)
	request := prepareReceivingRequest(t, binary, hash, "memory", data, policyBytes, "inert-dependency")
	if len(request.SourcePlan.Relations) != 0 {
		t.Fatal("inert policy inferred a relation")
	}
	for _, edge := range a.Records("edge") {
		found := false
		for _, origin := range request.SourcePlan.Origins {
			found = found || (origin.Project == edge.Project && origin.Table == edge.Source.Table && origin.ID == edge.ID && origin.Action == "inert-origin")
		}
		if !found {
			t.Fatal("dependency lost explicit inert disposition")
		}
	}
	out, stderr, err := executeReceiving(t, binary, hash, "memory", "", data, request, "inert-dependency-receive", policyBytes)
	if err != nil {
		t.Fatal(err, string(stderr))
	}
	var result receivingReceipt
	if err := decodeNativeSchemaFixture(out, &result); err != nil {
		t.Fatal(err)
	}
	if result.Report.Receipt == nil || !result.Verification.ArchiveBlob || result.Verification.CurrentRows == 0 || result.Verification.HistoryImages == 0 {
		t.Fatal("inert dependency publication/readback missing")
	}
}

// Declared synthetic current and head rows: two durable beads and one dependency.
// Earlier synthetic native commit images remain untouched.
func native66DependencyReceivingFixture(t *testing.T) []byte {
	t.Helper()
	data := native66ReceivingFixture(t, "ops", "wip")
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	members := tarMembers(t, data)
	var m dumpformat.Manifest
	var h dumpformat.NativeHistory
	decode(t, members["manifest.json"], &m)
	decode(t, members[dumpformat.NativeHistoryMember], &h)
	beads := a.Records("bead")
	if len(beads) != 1 || len(a.Records("edge")) != 0 {
		t.Fatal("synthetic dependency base changed")
	}
	second := beads[0]
	second.ID, second.Body, second.Source.Fields = "ops-42w", cloneRow(second.Body), cloneRow(second.Source.Fields)
	second.Source.Fields["id"] = second.ID
	beads = append(beads, second)
	issues := h.Current[dumpformat.DurableEntityTable]
	physical := cloneRow(issues.Rows[0])
	physical["id"] = second.ID
	issues.Rows = append(issues.Rows, physical)
	h.Current[dumpformat.DurableEntityTable] = issues
	labels := h.Current["labels"]
	for _, label := range second.Body["labels"].([]any) {
		labels.Rows = append(labels.Rows, dumpformat.Row{"issue_id": second.ID, "label": label})
	}
	h.Current["labels"] = labels
	fields := dumpformat.Row{"id": "ops-edge", "issue_id": beads[0].ID, "depends_on_issue_id": second.ID, dumpformat.EphemeralDependencyTargetField: nil, "depends_on_external": nil, "type": "blocks", "created_at": "2026-11-09T20:00:00Z", "created_by": "opaque source actor", "metadata": nil, "thread_id": ""}
	edge := dumpformat.Record{Kind: "edge", ID: "ops-edge", Project: "ops", Body: dumpformat.Row{"from": beads[0].ID, "to": second.ID, "target_kind": "depends_on_issue_id", "type": "blocks", "metadata": nil, "thread_id": ""}, Source: dumpformat.Provenance{Table: dumpformat.DurableDependencyTable, Fields: fields}}
	dependencies := h.Current[dumpformat.DurableDependencyTable]
	dependencies.Rows = []dumpformat.Row{fields}
	h.Current[dumpformat.DurableDependencyTable] = dependencies
	heads := 0
	for i := range h.Images {
		if h.Images[i].CommitID != h.Head {
			continue
		}
		heads++
		for _, name := range []string{dumpformat.DurableEntityTable, "labels", dumpformat.DurableDependencyTable} {
			var table dumpformat.NativeTable
			decode(t, encode(t, h.Current[name]), &table)
			h.Images[i].Tables[name] = table
		}
	}
	if heads != 1 {
		t.Fatal("synthetic dependency head missing or duplicated")
	}
	members["records/bead.ndjson"] = append(encode(t, beads[0]), encode(t, beads[1])...)
	members["records/edge.ndjson"] = encode(t, edge)
	m.Kinds = append(m.Kinds, dumpformat.Kind{Name: "edge", Version: dumpformat.EdgeVersionLocalOpaque, File: "records/edge.ndjson", Count: 1})
	sort.Slice(m.Kinds, func(i, j int) bool { return m.Kinds[i].Name < m.Kinds[j].Name })
	for i := range m.Kinds {
		if m.Kinds[i].Name == "bead" {
			m.Kinds[i].Count = 2
		}
	}
	for i := range m.Sources {
		s := &m.Sources[i]
		if table, present := h.Current[s.Family]; present {
			s.Rows = len(table.Rows)
		}
		if s.Family == "issues.row_lock" || s.Family == "issues.is_blocked" {
			s.Rows = 2
		}
	}
	members[dumpformat.NativeHistoryMember] = encode(t, h)
	for name, raw := range members {
		if name != "manifest.json" {
			m.Files[name] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
		}
	}
	members["manifest.json"] = encode(t, m)
	return writeTarMembers(t, members)
}
