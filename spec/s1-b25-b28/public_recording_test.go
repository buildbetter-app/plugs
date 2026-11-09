package s1b25b28_test

// This is an opt-in runtime recording.  It deliberately consumes the public
// native artifact prepared by RunsOn instead of building, downloading, or
// faking a Beads executable in the test. Raw evidence goes to Bazel's undeclared
// outputs; executable copies and disposable stores stay under /tmp/plug-swarm/work.
// RunsOn retains undeclared outputs; each recording still needs its own receipt.

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/publicnative"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

const (
	publicBeadsCommit      = "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724"
	publicBeadsTree        = "930405e863284a3955cbd35e73a6097b606b91a5"
	publicChildID          = "pub-root.1"
	requestedProductHead   = "2d2164ba3d8e456c9aba15fbeb8cae4edc755997"
	requestedProductTree   = "ae226eed2e9a85d4da19958db7a3520ebfe9cf05"
	requestedProductParent = "70230ccd92597a96000f90223c5d89b103e25426"
	productSourceCommit    = "258af5a5aa02c9b8d578f9f4f726a61ccd747315"
	productSourceTree      = "e36bea685c3496a4241522a80a17195793a069cf"
	productSourceParent    = "451b57270b7399d1a115d59ff307744ecf961bfd"
)

var requirePublicRecording = flag.Bool("public-recording", false, "Require the independently delivered public Beads recording artifact")
var publicRecordingID = flag.String("public-recording-id", "", "One independent RunsOn recording: recording1 or recording2")

type recordingProfile struct {
	Schema   string `json:"schema"`
	Exporter struct {
		TargetKey string `json:"target_key"`
		Label     string `json:"label"`
		Binary    string `json:"binary"`
	} `json:"exporter"`
	VersionKey     string `json:"version_key"`
	GoModuleSHA256 string `json:"go_module_sha256"`
	Producer       struct {
		RunID          string `json:"run_id"`
		ManifestSHA256 string `json:"manifest_sha256"`
		WorkflowSHA    string `json:"workflow_sha"`
		WorkflowBranch string `json:"workflow_branch"`
	} `json:"producer"`
}

type recordingFixture struct {
	Schema                string            `json:"schema"`
	SourceCommit          string            `json:"source_commit"`
	SourceTree            string            `json:"source_tree"`
	Commands              []string          `json:"commands"`
	ProjectionFields      []string          `json:"projection_fields"`
	IDs                   []string          `json:"ids"`
	MemoryKey             string            `json:"memory_key"`
	ConfigKey             string            `json:"config_key"`
	ExternalFamilies      []string          `json:"external_families"`
	StatusCategories      map[string]string `json:"status_categories"`
	ReverseArchivedFields []string          `json:"reverse_archived_fields"`
}

type nativeProductSourceBinding struct {
	Schema                     string   `json:"schema"`
	BindingScope               string   `json:"binding_scope"`
	Repository                 string   `json:"repository"`
	PullRequest                int      `json:"pull_request"`
	RequestedHead              string   `json:"requested_head"`
	RequestedTree              string   `json:"requested_tree"`
	OrderedParents             []string `json:"ordered_parents"`
	ProductCommit              string   `json:"product_commit"`
	ProductTree                string   `json:"product_tree"`
	ProductParents             []string `json:"product_ordered_parents"`
	AcceptedControlRef         string   `json:"accepted_control_ref"`
	AcceptedControlSHA         string   `json:"accepted_control_sha"`
	AcceptedProducerWorkflow   string   `json:"accepted_producer_workflow_sha"`
	AcceptedProducerDescriptor string   `json:"accepted_producer_descriptor"`
}

type publicArtifact struct {
	Binary       string
	SHA256       string
	BuildRun     string
	ManifestPath string
	ManifestSHA  string
	Size         int64
}

type publicNativeServer struct {
	Port     int    `json:"port"`
	PID      int    `json:"pid"`
	DataRoot string `json:"data_root"`
	provider *publicnative.Server
}

// The producer owns provisioning, listener identity and lifecycle. The caller
// consumes its in-process binding; no endpoint is supplied by a wrapper.
func requireNativePublicServer(t *testing.T, output string) publicNativeServer {
	t.Helper()
	server := publicnative.StartDolt(t, output)
	return publicNativeServer{Port: server.Port, PID: server.PID, DataRoot: server.DataRoot, provider: server}
}

type commandReceipt struct {
	Label        string   `json:"label"`
	Store        string   `json:"store"`
	Argv         []string `json:"argv"`
	Directory    string   `json:"directory"`
	Environment  []string `json:"environment"`
	StdinSHA256  string   `json:"stdin_sha256,omitempty"`
	StdoutSHA256 string   `json:"stdout_sha256"`
	StderrSHA256 string   `json:"stderr_sha256"`
	Exit         int      `json:"exit"`
	StdoutPath   string   `json:"stdout_path"`
	StderrPath   string   `json:"stderr_path"`
}

type publicRun struct {
	Name                 string
	Root                 string
	SourceExportPath     string
	TargetExportPath     string
	SourceInventoryPath  string
	TargetInventoryPath  string
	SourceArchivePath    string
	TargetArchivePath    string
	SourceExportSHA256   string
	TargetExportSHA256   string
	SourceArchiveSHA256  string
	TargetArchiveSHA256  string
	SourceProjectionSHA  string
	TargetProjectionSHA  string
	PlugSourceProjection map[string]string
	PlugTargetProjection map[string]string
	Commands             []commandReceipt
}

type semanticProjection struct {
	Issues   map[string]map[string]any `json:"issues"`
	Edges    []map[string]any          `json:"edges"`
	Comments []map[string]any          `json:"comments"`
	Memories map[string]any            `json:"memories"`
}

// Observations come from source readbacks/setup, never from the archive or
// reverse converter. The public corpus declares public values only. A future
// admitted observer may supply metadata or exact secret references explicitly.
type publicConfigObservation struct {
	Table          string `json:"table"`
	Key            string `json:"key"`
	Value          any    `json:"value"`
	Classification string `json:"classification"`
}

type publicConfigExpectation struct {
	Config     map[string]dumpformat.Record `json:"config"`
	UserMemory map[string]dumpformat.Record `json:"user_memory"`
	Policy     map[string]string            `json:"policy"`
	Memories   map[string]any               `json:"reverse_memories"`
	References []string                     `json:"config_references"`
}

// TestPublicNativeRecording is intentionally manual. The target fails when
// the trusted bundle environment is absent; a missing artifact is never
// converted into a skip or a successful fixture result.
func TestPublicNativeRecording(t *testing.T) {
	if !*requirePublicRecording {
		t.Fatal("public recording target requires -public-recording")
	}
	if *publicRecordingID != "recording1" && *publicRecordingID != "recording2" {
		t.Fatal("native recording requires a recording1/recording2 identity")
	}
	profile := readRecordingProfile(t)
	fixture := readRecordingFixture(t)
	product := readNativeProductSourceBinding(t)
	if product.RequestedHead != requestedProductHead || product.RequestedTree != requestedProductTree || len(product.OrderedParents) != 1 || product.OrderedParents[0] != requestedProductParent || product.ProductCommit != productSourceCommit || product.ProductTree != productSourceTree || len(product.ProductParents) != 1 || product.ProductParents[0] != productSourceParent || product.AcceptedControlSHA != product.RequestedHead {
		t.Fatalf("native product source binding is not the requested PR384 identity: %+v", product)
	}
	if fixture.SourceCommit != publicBeadsCommit || fixture.SourceTree != publicBeadsTree || fixture.Schema == "" || len(fixture.Commands) == 0 || len(fixture.ProjectionFields) == 0 {
		t.Fatalf("public recording fixture is not pinned: %+v", fixture)
	}
	artifact := verifyPublicArtifact(t, profile)
	root := makeRecordingRoot(t)
	actualProduct := recordNativeProductIdentity(t, root)
	recordR418ComparatorControls(t, root)
	recordRawFieldControls(t, root)
	t.Logf("PUBLIC_RECORDING_OUTPUT %s", root)
	storeRoot := makeRecordingDirectory(t, "/tmp/plug-swarm/work", "s1-b25-b28-public-stores-")
	t.Cleanup(func() {
		if err := os.RemoveAll(storeRoot); err != nil {
			t.Errorf("remove disposable public stores: %v", err)
		}
	})
	manifest, err := os.ReadFile(artifact.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(manifest) != artifact.ManifestSHA {
		t.Fatal("public manifest changed after artifact admission")
	}
	writeRecordingBytes(t, filepath.Join(root, "native-public-manifest.json"), manifest)
	buildInfo := recordBuildInfo(t, root, artifact, profile)
	t.Logf("PUBLIC_RECORDING_ARTIFACT build_run=%s binary_sha256=%s manifest_sha256=%s build_info_sha256=%s", artifact.BuildRun, artifact.SHA256, artifact.ManifestSHA, hashBytes(buildInfo))

	server := requireNativePublicServer(t, root)
	run := recordPublicRun(t, root, storeRoot, *publicRecordingID, artifact, fixture, server)
	if run.SourceArchiveSHA256 == "" || run.TargetArchiveSHA256 == "" {
		t.Fatal("recording did not retain both validated archives")
	}
	if err := server.provider.Stop(); err != nil {
		t.Fatalf("producer-owned server cleanup: %v", err)
	}
	provenance := map[string]any{
		"schema":                   fixture.Schema,
		"recording_id":             *publicRecordingID,
		"product_source":           actualProduct,
		"requested_and_checkpoint": product,
		"pair_status":              "requires-independent-second-run-and-comparison",
		"source_commit":            publicBeadsCommit,
		"source_tree":              publicBeadsTree,
		"build_run":                artifact.BuildRun,
		"binary_sha256":            artifact.SHA256,
		"manifest_sha256":          artifact.ManifestSHA,
		"build_info_sha256":        hashBytes(buildInfo),
		"projection_sha256": map[string]string{
			"source": run.SourceProjectionSHA,
			"target": run.TargetProjectionSHA,
		},
		"runs": []any{run},
	}
	data, err := json.MarshalIndent(provenance, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "provenance.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_RECORDING_OUTPUT %s", root)
	t.Logf("PUBLIC_RECORDING_PROVENANCE %s", filepath.Join(root, "provenance.json"))
}

func readRecordingProfile(t *testing.T) recordingProfile {
	t.Helper()
	var profile recordingProfile
	data := recordingRunfile(t, "testdata/native-public-profile.json")
	if err := json.Unmarshal(data, &profile); err != nil || profile.Schema != "plug-public-native/1" || profile.Exporter.TargetKey == "" || profile.Exporter.Label == "" || profile.Exporter.Binary == "" || profile.VersionKey == "" || len(profile.GoModuleSHA256) != 64 || !isLowerHex(profile.GoModuleSHA256) || profile.Producer.RunID == "" || len(profile.Producer.ManifestSHA256) != 64 || !isLowerHex(profile.Producer.ManifestSHA256) || len(profile.Producer.WorkflowSHA) != 40 || !isLowerHex(profile.Producer.WorkflowSHA) || profile.Producer.WorkflowBranch == "" {
		t.Fatalf("native public profile is invalid: %v", err)
	}
	return profile
}

func readRecordingFixture(t *testing.T) recordingFixture {
	t.Helper()
	var fixture recordingFixture
	data := recordingRunfile(t, "testdata/native-public-recording-fixture.json")
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("public recording fixture is invalid: %v", err)
	}
	families := map[string]bool{}
	for _, family := range fixture.ExternalFamilies {
		if strings.TrimSpace(family) == "" || families[family] {
			t.Fatal("public recording fixture has an empty or duplicate source family")
		}
		families[family] = true
	}
	if len(families) != 26 || len(fixture.StatusCategories) != 8 {
		t.Fatal("public recording fixture lacks the complete pinned source profile")
	}
	for status, category := range fixture.StatusCategories {
		if status == "" || category != "unstarted" && category != "started" && category != "backlog" && category != "completed" {
			t.Fatal("public recording fixture has an invalid status projection")
		}
	}
	return fixture
}

func readNativeProductSourceBinding(t *testing.T) nativeProductSourceBinding {
	t.Helper()
	var binding nativeProductSourceBinding
	data := recordingRunfile(t, "testdata/native-product-source-binding.json")
	if err := json.Unmarshal(data, &binding); err != nil || binding.Schema != "plug-s1-native-product-source/2" || binding.BindingScope != "requested-product-and-historical-checkpoint" || binding.Repository != "buildbetter-app/plugs" || binding.PullRequest != 384 || binding.AcceptedControlRef != "refs/heads/swarm/native-preparer-c8eb-rebind-20261104" || binding.ProductCommit == "" || binding.ProductTree == "" || binding.AcceptedProducerWorkflow == "" || binding.AcceptedProducerDescriptor == "" {
		t.Fatalf("native product source binding is invalid: %v %+v", err, binding)
	}
	return binding
}

func recordingRunfile(t *testing.T, name string) []byte {
	t.Helper()
	candidates := []string{
		filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "spec/s1-b25-b28", name),
		filepath.Join("spec", "s1-b25-b28", name),
		name,
	}
	for _, path := range candidates {
		if path == "" || path == filepath.Join("", name) {
			continue
		}
		if data, err := os.ReadFile(path); err == nil {
			return data
		}
	}
	t.Fatalf("recording runfile %q is unavailable", name)
	return nil
}

func recordR418ComparatorControls(t *testing.T, output string) {
	t.Helper()
	root := filepath.Join(output, "r418-comparer-controls")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"compare_r418_pair.py", "compare_r418_pair_test.py"} {
		writeRecordingBytes(t, filepath.Join(root, name), recordingRunfile(t, name))
	}
	var receipts []commandReceipt
	runCommand(t, root, &receipts, "typed-audit-negative-controls", "analysis-only", root,
		[]string{"PATH=/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE=1"},
		[]string{"python3", filepath.Join(root, "compare_r418_pair_test.py"), "-v"}, nil)
}

func verifyPublicArtifact(t *testing.T, profile recordingProfile) publicArtifact {
	t.Helper()
	artifact := publicArtifact{Binary: os.Getenv("PLUG_PUBLIC_BEADS_BINARY"), SHA256: os.Getenv("PLUG_PUBLIC_BEADS_SHA256"), BuildRun: os.Getenv("PLUG_PUBLIC_BEADS_BUILD_RUN"), ManifestPath: os.Getenv("PLUG_PUBLIC_BEADS_MANIFEST")}
	for name, value := range map[string]string{"PLUG_PUBLIC_BEADS_BINARY": artifact.Binary, "PLUG_PUBLIC_BEADS_SHA256": artifact.SHA256, "PLUG_PUBLIC_BEADS_BUILD_RUN": artifact.BuildRun, "PLUG_PUBLIC_BEADS_MANIFEST": artifact.ManifestPath} {
		if value == "" {
			t.Fatalf("required public artifact evidence is missing: %s", name)
		}
	}
	if !filepath.IsAbs(artifact.Binary) || !filepath.IsAbs(artifact.ManifestPath) || len(artifact.SHA256) != 64 || !isLowerHex(artifact.SHA256) {
		t.Fatal("public artifact paths or digest are invalid")
	}
	if run, err := strconv.ParseUint(artifact.BuildRun, 10, 64); err != nil || run == 0 || strconv.FormatUint(run, 10) != artifact.BuildRun {
		t.Fatal("public artifact build run is not an immutable decimal run ID")
	}
	manifestData, err := os.ReadFile(artifact.ManifestPath)
	if err != nil || len(manifestData) == 0 || len(manifestData) > 1<<20 {
		t.Fatalf("public artifact manifest is unavailable: %v", err)
	}
	artifact.ManifestSHA = hashBytes(manifestData)
	if artifact.ManifestSHA != profile.Producer.ManifestSHA256 || artifact.BuildRun != profile.Producer.RunID {
		t.Fatal("native public manifest or producer run differs from the reviewed profile")
	}
	var manifest struct {
		Schema    string                                    `json:"schema"`
		Source    map[string]struct{ Commit, Tree string }  `json:"source"`
		Targets   map[string]struct{ Label, Binary string } `json:"targets"`
		Artifacts map[string]struct {
			SHA256 string `json:"sha256"`
			Size   int64  `json:"size"`
			Mode   string `json:"mode"`
		} `json:"artifacts"`
		Build struct {
			RunID string `json:"run_id"`
		} `json:"build"`
		Workflow struct {
			Ref     string `json:"ref"`
			SHA     string `json:"sha"`
			Profile string `json:"profile"`
		} `json:"workflow"`
		Resolution map[string]json.RawMessage `json:"resolution"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("public artifact manifest is invalid: %v", err)
	}
	target := manifest.Targets[profile.Exporter.TargetKey]
	binaryInfo := manifest.Artifacts[profile.Exporter.Binary]
	var version struct {
		Commit string `json:"commit"`
		Build  string `json:"build"`
	}
	if rawVersion, ok := manifest.Resolution[profile.VersionKey]; !ok || json.Unmarshal(rawVersion, &version) != nil {
		t.Fatal("public artifact version probe is missing or malformed")
	}
	beadsSource := manifest.Source["beads"]
	if manifest.Schema != profile.Schema || beadsSource.Commit != publicBeadsCommit || beadsSource.Tree != publicBeadsTree || target.Label != profile.Exporter.Label || target.Binary != profile.Exporter.Binary || binaryInfo.SHA256 != artifact.SHA256 || binaryInfo.Mode != "0o755" || binaryInfo.Size <= 0 || manifest.Build.RunID != artifact.BuildRun || manifest.Workflow.SHA != profile.Producer.WorkflowSHA || manifest.Workflow.Ref != "refs/heads/"+profile.Producer.WorkflowBranch || manifest.Workflow.Profile != profile.Schema || version.Commit != publicBeadsCommit || version.Build != "public-"+publicBeadsCommit {
		t.Fatalf("public artifact identity does not match profile: schema=%q source=%+v target=%+v binary=%+v build=%+v version=%+v", manifest.Schema, beadsSource, target, binaryInfo, manifest.Build, version)
	}
	info, err := os.Lstat(artifact.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 || info.Size() != binaryInfo.Size {
		t.Fatalf("public executable mode or size is not manifest-bound: %v", err)
	}
	artifact.Size = info.Size()
	if got := hashFile(artifact.Binary); got != artifact.SHA256 {
		t.Fatalf("public executable digest changed: got %s want %s", got, artifact.SHA256)
	}
	return artifact
}

func makeRecordingRoot(t *testing.T) string {
	t.Helper()
	parent := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if parent == "" {
		parent = "/tmp/plug-swarm/work"
	}
	if !filepath.IsAbs(parent) {
		t.Fatal("public recording output directory must be absolute")
	}
	return makeRecordingDirectory(t, parent, "s1-b25-b28-public-recording-")
}

func makeRecordingDirectory(t *testing.T, parent, pattern string) string {
	t.Helper()
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(parent, pattern)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func writeRecordingBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// These typed raw fields are diagnostic evidence, not a public response. Read
// the underlying values without calling model marshaling/string methods. This
// also retains nil versus empty slices, optional zero fields, pointer contents,
// invalid nested codes/numbers and arbitrary bytes in a RawMessage.
func rawRecordingFields(value reflect.Value) any {
	if !value.IsValid() {
		return map[string]any{"kind": "invalid"}
	}
	node := map[string]any{"go_type": value.Type().String(), "kind": value.Kind().String()}
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			node["nil"] = true
		} else {
			node["value"] = rawRecordingFields(value.Elem())
		}
	case reflect.Struct:
		fields := map[string]any{}
		for i := 0; i < value.NumField(); i++ {
			fields[value.Type().Field(i).Name] = rawRecordingFields(value.Field(i))
		}
		node["fields"] = fields
	case reflect.Map:
		if value.IsNil() {
			node["nil"] = true
		} else {
			entries := make([]any, 0, value.Len())
			iter := value.MapRange()
			for iter.Next() {
				entries = append(entries, map[string]any{"key": rawRecordingFields(iter.Key()), "value": rawRecordingFields(iter.Value())})
			}
			node["entries"] = entries
		}
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			node["nil"] = true
		} else if value.Type().Elem().Kind() == reflect.Uint8 {
			data := make([]byte, value.Len())
			for i := range data {
				data[i] = byte(value.Index(i).Uint())
			}
			node["bytes"] = data
		} else {
			items := make([]any, value.Len())
			for i := range items {
				items[i] = rawRecordingFields(value.Index(i))
			}
			node["items"] = items
		}
	case reflect.String:
		node["value"] = value.String()
	case reflect.Bool:
		node["value"] = value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		node["value"] = value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		node["value"] = value.Uint()
	case reflect.Float32, reflect.Float64:
		node["value"] = strconv.FormatFloat(value.Float(), 'g', -1, value.Type().Bits())
	default:
		panic("unsupported raw recorder kind: " + value.Kind().String())
	}
	return node
}

func writeRawRecordingValue(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"schema": "plug-private-raw-go-fields/1", "value": rawRecordingFields(reflect.ValueOf(value))})
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, path, append(data, '\n'))
}

// Encoding controls are authored recorder tests, never native observations.
func recordRawFieldControls(t *testing.T, output string) {
	t.Helper()
	valid := model.Commit{Revision: model.Natural64(1), Results: []model.OpResult{}, Warnings: []model.Warning{}}
	badNumber := valid
	badNumber.Revision = model.Natural("-1")
	badWarning := valid
	badWarning.Warnings = []model.Warning{{Code: model.WarningCode("invalid-recorder-control")}}
	badRecord := valid
	badRecord.Results = []model.OpResult{{Ref: model.RefByID("control", model.Natural64(0)), Revision: model.Natural64(1), Record: json.RawMessage("invalid JSON")}}
	cases := map[string]any{
		"zero-outcome": port.WriteOutcome{}, "invalid-number": badNumber,
		"invalid-warning": badWarning, "invalid-record-bytes": badRecord,
		"invalid-error-code": &model.Error{Data: model.ErrorData{Code: model.ErrorCode("invalid-recorder-control")}},
	}
	for label, value := range cases {
		if _, err := json.Marshal(value); err == nil {
			t.Fatalf("raw field negative control %s was unexpectedly a valid public response", label)
		}
		writeRawRecordingValue(t, filepath.Join(output, "recorder-control-"+label+".raw-fields.json"), value)
	}
	if _, err := json.Marshal(valid); err != nil {
		t.Fatal(err)
	}
	writeRawRecordingValue(t, filepath.Join(output, "recorder-control-valid.raw-fields.json"), valid)
	zero := rawRecordingFields(reflect.ValueOf(model.Commit{})).(map[string]any)["fields"].(map[string]any)
	positive := rawRecordingFields(reflect.ValueOf(valid)).(map[string]any)["fields"].(map[string]any)
	if len(zero) != reflect.TypeOf(valid).NumField() || zero["Revision"].(map[string]any)["value"] != "" || zero["Results"].(map[string]any)["nil"] != true || zero["Warnings"].(map[string]any)["nil"] != true || positive["Results"].(map[string]any)["nil"] != nil || len(positive["Results"].(map[string]any)["items"].([]any)) != 0 {
		t.Fatal("raw field encoder lost underlying zero/nil/empty values or a commit field")
	}
	t.Log("RAW_RECORDER_ENCODING_CONTROLS one_positive=PASS five_invalid_public_responses_retained=PASS native=false")
}

// Save raw fields before public model validation can reject JSON encoding.
func writeRecordingValue(t *testing.T, path string, value any) {
	t.Helper()
	writeRawRecordingValue(t, path+".raw-fields.json", value)
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("recording JSON %s refused after raw field capture: %v", path, err)
	}
	writeRecordingBytes(t, path, append(data, '\n'))
}

func recordBuildInfo(t *testing.T, root string, artifact publicArtifact, profile recordingProfile) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, runErr := exec.CommandContext(ctx, "go", "version", "-m", artifact.Binary).CombinedOutput()
	path := filepath.Join(root, "build-info.txt")
	if err := os.WriteFile(path, output, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_BUILD_INFO path=%s sha256=%s", path, hashBytes(output))
	if runErr != nil {
		t.Fatalf("pinned Beads build info is unavailable: %v\n%s", runErr, output)
	}
	if err := checkPublicBuildInfo(output, profile.GoModuleSHA256, profile.Exporter.Label); err != nil {
		t.Fatalf("pinned Beads main package is not profile-bound: %v\n%s", err, output)
	}
	return output
}

// Bazel's Go binary records its main package path without a mod record. Bind
// that complete path to the sealed exporter label and the unchanged module
// hash. Any present mod record must agree; dependencies cannot supply either.
func checkPublicBuildInfo(data []byte, expectedModuleSHA256, exporterLabel string) error {
	if len(expectedModuleSHA256) != 64 || !isLowerHex(expectedModuleSHA256) {
		return errors.New("invalid source module hash pin")
	}
	targetPackage, target, ok := strings.Cut(exporterLabel, ":")
	if !ok || !strings.HasPrefix(targetPackage, "//") || target == "" || strings.ContainsAny(target, ":/\\ \t\r\n") {
		return errors.New("invalid sealed exporter label")
	}
	relativePackage := strings.TrimPrefix(targetPackage, "//")
	for _, part := range strings.Split(relativePackage, "/") {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "\\ \t\r\n") {
			return errors.New("invalid sealed exporter package")
		}
	}
	packageSuffix := "/" + relativePackage
	pathCount, moduleCount := 0, 0
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "path":
			pathCount++
			if len(fields) != 2 || !strings.HasSuffix(fields[1], packageSuffix) {
				return errors.New("main package does not match the sealed exporter label")
			}
			module := strings.TrimSuffix(fields[1], packageSuffix)
			if hashBytes([]byte(module)) != expectedModuleSHA256 {
				return errors.New("main package module does not match the sealed source profile")
			}
		case "mod":
			moduleCount++
			if len(fields) < 3 || hashBytes([]byte(fields[1])) != expectedModuleSHA256 {
				return errors.New("main module does not match the sealed source profile")
			}
		}
	}
	if pathCount != 1 || moduleCount > 1 {
		return errors.New("build info must contain exactly one main package and at most one matching module")
	}
	return nil
}

func recordPublicRun(t *testing.T, root, storeRoot, name string, artifact publicArtifact, fixture recordingFixture, server publicNativeServer) publicRun {
	t.Helper()
	run := publicRun{Name: name, Root: filepath.Join(root, name)}
	if err := os.MkdirAll(run.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	storeRoot = filepath.Join(storeRoot, name)
	// Exercise canonical root workspaces with the corrected accepted census.
	// Exact repeated producer paths are deduplicated upstream, never here.
	source := filepath.Join(storeRoot, "source")
	target := filepath.Join(storeRoot, "target")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{source, target} {
		home := filepath.Join(dir, "home")
		if err := os.MkdirAll(filepath.Join(home, ".config"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	pinned := filepath.Join(storeRoot, "native-exporter-pinned")
	copyPinned(t, artifact.Binary, pinned, artifact.SHA256, artifact.Size)
	if server.Port < 1 || server.Port > 65535 || server.PID < 1 || !filepath.IsAbs(server.DataRoot) {
		t.Fatal("producer helper did not return a complete in-process server binding")
	}
	dataRoot, err := filepath.EvalSymlinks(server.DataRoot)
	if err != nil || dataRoot != server.DataRoot {
		t.Fatal("producer data root must be the actual physical directory")
	}
	baseEnv := recordingEnv(server.DataRoot)
	env := func(dir string) []string { return server.provider.ChildEnv(baseEnv(dir)) }
	commandGit := func(label, store string, dir string, argv []string) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir, env(dir), append([]string{"git"}, argv...), nil)
	}
	commandReference := func(label, store, dir string, argv []string, stdin []byte) []byte {
		return runCommand(t, run.Root, &run.Commands, label, store, dir, env(dir), append([]string{pinned}, argv...), stdin)
	}
	// Consume the default native protocol with ChildEnv unchanged. Independent
	// SQL rows disambiguate a stored schema_version from CLI protocol metadata.
	commandConfigList := func(label, store, dir string) []byte {
		return commandReference(label, store, dir, []string{"--json", "config", "list"}, nil)
	}
	commandSettings := func(label, store, dir string) []byte {
		return commandReference(label, store, dir, []string{"--readonly", "--json", "sql", "SELECT `key`, `value` FROM config ORDER BY `key`"}, nil)
	}
	commandMetadata := func(label, store, dir string) []byte {
		return commandReference(label, store, dir, []string{"--readonly", "--json", "sql", "SELECT `key`, `value` FROM metadata ORDER BY `key`"}, nil)
	}
	commandGit("source-git-init", "source", source, []string{"-c", "init.defaultBranch=main", "init", "--quiet"})
	version := commandReference("source-version", "source", source, []string{"version", "--json"}, nil)
	var identity struct {
		Commit string `json:"commit"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(version), &identity); err != nil || identity.Commit != publicBeadsCommit {
		t.Fatalf("pinned version identity changed: %v %s", err, version)
	}
	commandReference("source-init", "source", source, nativeInitArgs(source, server.Port), nil)
	recordNativeDatabaseBinding(t, run.Root, "source", source, server)
	// Record the existing disabled auto-export default as real parsed YAML.
	commandReference("source-export-config", "source", source, []string{"--json", "config", "set", "export.auto", "false"}, nil)
	commandReference("source-config", "source", source, []string{"--json", "config", "set", fixture.ConfigKey, "paired-public"}, nil)
	configOutput := commandConfigList("source-config-list", "source", source)
	settings, err := readNativeConfigSettings(configOutput, commandSettings("source-config-sql", "source", source))
	if err != nil {
		t.Fatalf("real config list is invalid: %v %s", err, configOutput)
	}
	configured, found := "", false
	for _, setting := range settings {
		if setting.Key == fixture.ConfigKey {
			configured, found = setting.Value.(string), true // The readback parser requires string settings.
			break
		}
	}
	if !found || configured != "paired-public" {
		t.Fatalf("real config list omitted %s: %s", fixture.ConfigKey, configOutput)
	}
	commandReference("source-remember", "source", source, []string{"--json", "remember", "paired-store memory", "--key", fixture.MemoryKey}, nil)
	commandReference("source-create-root", "source", source, []string{"--json", "create", "--id", "pub-root", "--type", "task", "--title", "Public root", "--description", "relational root", "--metadata", `{"card":"B27","public":true}`, "--labels", "alpha,beta", "--spec-id", "s1/b27"}, nil)
	commandReference("source-create-child", "source", source, []string{"--json", "create", "--type", "bug", "--title", "Public child", "--description", "parented child", "--parent", "pub-root", "--metadata", `{"child":true}`, "--labels", "child,public", "--priority", "1"}, nil)
	commandReference("source-create-dependency", "source", source, []string{"--json", "create", "--id", "pub-dependency", "--type", "task", "--title", "Public dependency", "--description", "dependency row"}, nil)
	commandReference("source-dependency", "source", source, []string{"--json", "dep", "add", publicChildID, "pub-dependency"}, nil)
	commandReference("source-label", "source", source, []string{"--json", "label", "add", "pub-root", "reviewed"}, nil)
	commandReference("source-comment", "source", source, []string{"--json", "comment", "pub-root", "preserve this public comment"}, nil)
	commandReference("source-close", "source", source, []string{"--json", "close", "pub-dependency", "--reason", "dependency complete"}, nil)
	recordNativeScalarOperations(t, run.Root, storeRoot, source, pinned, server, env(source))
	commandReference("source-commit", "source", source, []string{"dolt", "commit"}, nil)

	run.SourceExportPath = filepath.Join(run.Root, "source-export.jsonl")
	commandReference("source-export", "source", source, []string{"--readonly", "export", "--include-memories", "--output", run.SourceExportPath}, nil)
	sourceExport, err := os.ReadFile(run.SourceExportPath)
	if err != nil {
		t.Fatal(err)
	}
	run.SourceExportSHA256 = hashBytes(sourceExport)
	sourceRawProjection, err := projectJSONL(sourceExport, fixture.ProjectionFields, fixture.StatusCategories)
	if err != nil {
		t.Fatalf("source JSONL projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "source-jsonl-projection.json"), sourceRawProjection)
	validateSourceRows(t, sourceRawProjection, fixture)
	sourceConfig := commandConfigList("source-config-readback", "source", source)
	sourceMetadata := commandMetadata("source-metadata-readback", "source", source)
	sourceExpected := observeNativeConfig(t, run.Root, "source", sourceConfig, commandSettings("source-config-sql-readback", "source", source), sourceMetadata, sourceRawProjection)
	if observed, ok := sourceExpected.Config["config/"+fixture.ConfigKey]; !ok || observed.Source.Fields["value"] != configured {
		t.Fatal("source setup config changed before independent archive observation")
	}
	validateInventoryAndArchive(t, &run, commandReference, "source", source, run.Root, fixture)
	recordNativeDeleteOperations(t, run.Root, storeRoot, source, pinned, server, env(source))

	commandGit("target-git-init", "target", target, []string{"-c", "init.defaultBranch=main", "init", "--quiet"})
	commandReference("target-version", "target", target, []string{"version", "--json"}, nil)
	commandReference("target-init", "target", target, nativeInitArgs(target, server.Port), nil)
	recordNativeDatabaseBinding(t, run.Root, "target", target, server)
	commandReference("target-export-config", "target", target, []string{"--json", "config", "set", "export.auto", "false"}, nil)
	commandReference("target-create-keep", "target", target, []string{"--json", "create", "--id", "pub-keep", "--type", "task", "--title", "Keep target row", "--description", "must survive import"}, nil)
	commandReference("target-import", "target", target, []string{"--json", "import", run.SourceExportPath}, nil)
	commandReference("target-commit", "target", target, []string{"dolt", "commit"}, nil)
	run.TargetExportPath = filepath.Join(run.Root, "target-export.jsonl")
	commandReference("target-export", "target", target, []string{"--readonly", "export", "--include-memories", "--output", run.TargetExportPath}, nil)
	targetExport, err := os.ReadFile(run.TargetExportPath)
	if err != nil {
		t.Fatal(err)
	}
	run.TargetExportSHA256 = hashBytes(targetExport)
	targetRawProjection, err := projectJSONL(targetExport, fixture.ProjectionFields, fixture.StatusCategories)
	if err != nil {
		t.Fatalf("target JSONL projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "target-jsonl-projection.json"), targetRawProjection)
	assertTargetContainsSource(t, sourceRawProjection, targetRawProjection, fixture)
	targetConfig := commandConfigList("target-config-readback", "target", target)
	targetMetadata := commandMetadata("target-metadata-readback", "target", target)
	targetExpected := observeNativeConfig(t, run.Root, "target", targetConfig, commandSettings("target-config-sql-readback", "target", target), targetMetadata, targetRawProjection)
	validateInventoryAndArchive(t, &run, commandReference, "target", target, run.Root, fixture)

	sourceArchive, err := dumpformat.ReadFile(run.SourceArchivePath)
	if err != nil {
		t.Fatalf("source plug-dump validation: %v", err)
	}
	targetArchive, err := dumpformat.ReadFile(run.TargetArchivePath)
	if err != nil {
		t.Fatalf("target plug-dump validation: %v", err)
	}
	assertPublicConfigArchive(t, sourceArchive, run.SourceInventoryPath, sourceExpected)
	assertPublicConfigArchive(t, targetArchive, run.TargetInventoryPath, targetExpected)
	sourceArchiveProjection, err := projectArchive(sourceArchive, fixture.ProjectionFields)
	if err != nil {
		t.Fatalf("source archive projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "source-archive-projection.json"), sourceArchiveProjection)
	targetArchiveProjection, err := projectArchive(targetArchive, fixture.ProjectionFields)
	if err != nil {
		t.Fatalf("target archive projection: %v", err)
	}
	writeRecordingBytes(t, filepath.Join(run.Root, "target-archive-projection.json"), targetArchiveProjection)
	if !bytes.Equal(sourceRawProjection, sourceArchiveProjection) || !bytes.Equal(targetRawProjection, targetArchiveProjection) {
		t.Fatalf("independent JSONL/archive projections differ: source=%s/%s target=%s/%s", hashBytes(sourceRawProjection), hashBytes(sourceArchiveProjection), hashBytes(targetRawProjection), hashBytes(targetArchiveProjection))
	}
	run.SourceProjectionSHA = hashBytes(sourceRawProjection)
	run.TargetProjectionSHA = hashBytes(targetRawProjection)
	run.PlugSourceProjection = replayPublicArchive(t, run.Root, storeRoot, "source", sourceArchive, sourceRawProjection, sourceExpected, fixture)
	run.PlugTargetProjection = replayPublicArchive(t, run.Root, storeRoot, "target", targetArchive, targetRawProjection, targetExpected, fixture)
	recordNativeB27Probes(t, run.Root, storeRoot, pinned, server, baseEnv)
	labels := make([]string, len(run.Commands))
	for i, receipt := range run.Commands {
		labels[i] = receipt.Label
		if receipt.Exit != 0 || receipt.StdoutSHA256 == "" || receipt.StderrSHA256 == "" {
			t.Fatalf("command %s did not retain a complete successful receipt", receipt.Label)
		}
	}
	if !equalJSON(labels, fixture.Commands) {
		t.Fatalf("recorded command sequence changed: got=%v want=%v", labels, fixture.Commands)
	}
	writeCommandReceipts(t, run.Root, run.Commands)
	return run
}

// The producer owns the server executable, provisioning and lifecycle. The
// consumer can address only its explicit loopback port and creates fresh,
// per-workspace database names. No external DSN or producer pin is accepted.
func nativeDatabaseName(store string) string { return "b27_" + hashBytes([]byte(store))[:24] }

func nativeInitArgs(store string, port int) []string {
	return []string{"init", "--backend", "dolt", "--server", "--external", "--server-host", "127.0.0.1", "--server-port", strconv.Itoa(port), "--database", nativeDatabaseName(store), "--prefix", "pub", "--quiet", "--skip-hooks", "--skip-agents", "--non-interactive"}
}

func recordNativeDatabaseBinding(t *testing.T, output, label, store string, server publicNativeServer) {
	t.Helper()
	dir := server.provider.RequireDatabaseDir(t, nativeDatabaseName(store))
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("actual Dolt database directory is missing or is a link: %s: %v", dir, err)
	}
	data, err := json.MarshalIndent(map[string]any{"server": server, "database": nativeDatabaseName(store), "database_directory": dir, "mode": info.Mode().String()}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(output, label+"-server-binding.json"), append(data, '\n'))
}

// Replay consumes the exact native producer envelope. It never manufactures an
// archive or treats a converter refusal as success. Snapshot is an explicit
// head-state slice under P5c/P44/P22/P46; this does not qualify source history.
func replayPublicArchive(t *testing.T, output, storeRoot, label string, archive *dumpformat.Archive, expected []byte, config publicConfigExpectation, fixture recordingFixture) map[string]string {
	t.Helper()
	// Reverse explicitly retains source provenance as issue metadata. Compute
	// its expected values from the validated INPUT before any engine runs;
	// never learn an allowance from the reverse result or discard a prefix.
	expected, err := projectPublicReverseExpectation(expected, archive.Records("bead"), fixture.ReverseArchivedFields)
	if err != nil {
		t.Fatalf("%s reverse input expectation: %v", label, err)
	}
	writeRecordingBytes(t, filepath.Join(output, label+"-reverse-expected-projection.json"), expected)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	data := archive.TarBytes()
	inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("%s actual archive cannot enter Plug replay: %v", label, err)
	}
	profile := driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "public-recorder"},
			Authority: "restore-admin", AdmissionDigest: hashBytes([]byte("s1-b27-disposable-public-import")),
		},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import",
		IdempotencyKey: "s1-b27-public-" + label, ArchiveDigest: inspection.ArchiveSHA256,
		DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	hashes := map[string]string{}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		if !t.Run(label+"-plug-"+engine, func(t *testing.T) {
			var store driver.Store
			path := filepath.Join(storeRoot, "plug-"+label+".sqlite")
			var sqlDriver driver.Driver
			dsn := path
			var err error
			if engine == "memory" {
				store, err = memory.NewWithOptions(memory.Options{StoreID: driver.StoreID("s1-b27-public-" + label), Incarnation: "s1-b27-public-" + label})
			} else {
				sqlDriver = sqlite.Driver{}
				if engine == "postgres" {
					sqlDriver = postgres.Driver{}
					dsn = testclock.PostgresDSN(t)
				}
				store, err = sqlDriver.Open(ctx, driver.Config{DSN: dsn})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			if _, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, profile); err != nil {
				t.Fatalf("%s real Plug restore: %v", engine, err)
			}
			compare := func(suffix string) {
				live, err := converterroundtrip.ReverseRestored(ctx, store)
				if err != nil {
					t.Fatalf("%s live Plug projection%s: %v", engine, suffix, err)
				}
				base := filepath.Join(output, label+"-plug-"+engine+suffix)
				writeRecordingBytes(t, base+".jsonl", live.Result.JSONL)
				report, err := json.MarshalIndent(live.Result.Report, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				writeRecordingBytes(t, base+"-report.json", report)
				projection, err := projectJSONL(live.Result.JSONL, fixture.ProjectionFields, fixture.StatusCategories)
				if err != nil {
					t.Fatalf("%s actual Plug JSONL projection%s: %v", engine, suffix, err)
				}
				writeRecordingBytes(t, base+"-projection.json", projection)
				if err := comparePublicReverse(expected, projection, config, live.Result.Report.ConfigReferences, live.Result.Report.ConfigAsMemories); err != nil {
					t.Fatalf("%s Plug replay%s changed recorded semantics: %v", engine, suffix, err)
				}
				hashes[engine+suffix] = hashBytes(projection)
			}
			compare("")
			if sqlDriver != nil {
				err := store.Close()
				store = nil
				if err != nil {
					t.Fatal(err)
				}
				store, err = sqlDriver.Open(ctx, driver.Config{DSN: dsn})
				if err != nil {
					t.Fatal(err)
				}
				compare("-reopen")
			}
			sqlitePath := ""
			if engine == "sqlite" {
				sqlitePath = path
			}
			verifyFault := recordPublicSubscriber(t, ctx, store, filepath.Join(output, label+"-plug-"+engine+"-subscriber.json"), []string{"pub-root", publicChildID, "pub-dependency"}, profile, sqlitePath)
			complete := nativePlugImage(t, ctx, store, profile)
			writeRecordingValue(t, filepath.Join(output, label+"-plug-"+engine+"-post-write-state.json"), complete)
			if sqlDriver != nil {
				err := store.Close()
				store = nil
				if err != nil {
					t.Fatal(err)
				}
				store, err = sqlDriver.Open(ctx, driver.Config{DSN: dsn})
				if err != nil {
					t.Fatal(err)
				}
				reopened := nativePlugImage(t, ctx, store, profile)
				writeRecordingValue(t, filepath.Join(output, label+"-plug-"+engine+"-post-write-reopen-state.json"), reopened)
				if verifyFault != nil {
					verifyFault(store)
				}
				if !equalJSON(complete, reopened) {
					t.Fatal("SQL reopen changed native-origin source, history, derived state or durable receipts")
				}
			}
		}) {
			t.Fatalf("%s Plug replay stopped at %s; no later engine qualifies", label, engine)
		}
	}
	return hashes
}

type publicPostObservation struct {
	Change   port.CommittedChange `json:"change"`
	Snapshot driver.View          `json:"snapshot"`
	Rows     []driver.Record      `json:"rows"`
}

type publicPostRecorder struct {
	mu           sync.Mutex
	Observations []publicPostObservation
}

func (p *publicPostRecorder) After(ctx context.Context, change port.CommittedChange, reader port.SnapshotReader) error {
	view := reader.Snapshot()
	if view.At == nil || view.At.String() != change.Revision.String() || len(change.Changes) != 1 {
		return errors.New("public subscriber did not receive the committed single-row snapshot")
	}
	c := change.Changes[0]
	rows, err := reader.Get(ctx, []model.Ref{c.Resource.Ref})
	if err != nil {
		return err
	}
	if len(rows) != 1 || !bytes.Equal(rows[0].Body, c.After) {
		return errors.New("public subscriber readback differs from the committed after image")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Observations = append(p.Observations, publicPostObservation{change, view, rows})
	return nil
}

// Retain every reader-visible row, version, changeset, derived table and each
// declared operation's durable receipt. This is an observed store image, not
// a counter or an assertion based solely on a subscriber callback.
func nativePlugImage(t *testing.T, ctx context.Context, store driver.Store, profile driver.RestoreImportProfile) map[string]any {
	t.Helper()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || rows.NextCursor != nil {
		t.Fatalf("complete native-origin rows: %v cursor=%v", err, rows.NextCursor)
	}
	versions := map[string][]driver.Version{}
	for _, row := range rows.Items {
		page, err := r.Versions(ctx, row.Ref, driver.PageReq{})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("complete native-origin history: %v", err)
		}
		versions[row.Ref.ID] = page.Items
	}
	// Snapshot restore deliberately begins public changesets strictly after
	// HistoryFloor. Preserve its floor changeset in the bound import receipt;
	// never pretend nil-cursor truncation is a complete empty history.
	var cursor *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		_, belowErr := r.Changesets(ctx, nil, 1000)
		var truncated driver.ErrTruncated
		if !errors.As(belowErr, &truncated) || truncated.Floor != meta.HistoryFloor {
			t.Fatalf("snapshot history-floor refusal changed: %v", belowErr)
		}
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor}
	}
	changes, err := r.Changesets(ctx, cursor, 1000)
	if err != nil || len(changes) >= 1000 {
		t.Fatalf("complete native-origin changesets: %v", err)
	}
	derived := map[driver.DerivedTable][]driver.DerivedRow{}
	derivedObservations := map[driver.DerivedTable][]driver.LockState{}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		derived[table], err = r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		// SQL enumeration returns Key/Value without the stored Index. Retain
		// that projection, then read the full payload at the same pinned view.
		// Observe acquires no locks. Never infer a nil index from enumeration.
		keys := make([]driver.LockKey, len(derived[table]))
		for i, row := range derived[table] {
			keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
		}
		// Table and kind are identical within this call, so bytewise row-key
		// order is the driver key order. Validate before observing any key.
		sort.Slice(keys, func(i, j int) bool { return keys[i].Derived.Key < keys[j].Derived.Key })
		if err := driver.ValidateLockKeys(keys); err != nil {
			t.Fatalf("native-origin derived observation keys for %s: %v", table, err)
		}
		derivedObservations[table], err = r.Observe(ctx, keys)
		if err != nil {
			t.Fatalf("native-origin complete derived observations for %s: %v", table, err)
		}
		// Preserve returned states before the later correspondence assertions;
		// a mismatch must not erase a successfully acquired raw after-image.
	}
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "user", ID: "public-recorder"})
	if err != nil {
		t.Fatal(err)
	}
	receipts := map[string]any{}
	keys := []string{"s1-native-public-0", "s1-native-public-1", "s1-native-public-2", "s1-native-public-3", "s1-native-public-equal-labels", "s1-native-public-refused-aux", "s1-native-public-sqlite-write-fault"}
	for _, key := range keys {
		method := "bead.update"
		if strings.HasSuffix(key, "refused-aux") {
			method = "tx.apply"
		}
		receipt, err := r.IdempotencyByKey(ctx, principal, "", method, hashBytes([]byte(key)))
		if err != nil {
			t.Fatal(err)
		}
		receipts[key] = nil
		if receipt != nil {
			value := map[string]any{"request_hash": receipt.RequestHash, "commit": receipt.Commit, "response_pending": receipt.ResponsePending}
			if receipt.Changeset.Kind != "" {
				value["changeset"] = receipt.Changeset
			}
			receipts[key] = value
		}
	}
	importPrincipal, err := driver.PrincipalKey(profile.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	importReceipt, err := r.IdempotencyByKey(ctx, importPrincipal, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
	requestHash, hashErr := profile.RequestHash()
	if err != nil || hashErr != nil || importReceipt == nil || importReceipt.ResponsePending || importReceipt.RequestHash != requestHash || importReceipt.Changeset.Kind != model.ChangesetRestore || importReceipt.Changeset.Incarnation != meta.Incarnation || importReceipt.Changeset.Revision != meta.HistoryFloor || importReceipt.Commit.Revision != meta.HistoryFloor {
		t.Fatalf("snapshot floor import receipt lost its exact binding: %v / %v", err, hashErr)
	}
	archive, err := r.Blob(ctx, driver.Hash(profile.ArchiveDigest))
	if err != nil || hashBytes(archive) != profile.ArchiveDigest {
		t.Fatalf("snapshot retained source archive changed: %v", err)
	}
	return map[string]any{"meta": meta, "snapshot": r.Snapshot(), "rows": rows.Items, "versions": versions, "changesets": changes, "changeset_cursor": cursor, "import_receipt": importReceipt, "source_archive": archive, "derived": derived, "derived_observations": derivedObservations, "receipts": receipts}
}

// Check the complete captured reader image before indexing it. A map must not
// silently collapse duplicate rows/versions or turn a missing table into an
// empty-table allowance. Physical lock/read-fact tables and unenumerated receipt
// scopes are outside this reader API; this oracle makes no claim about them.
func nativePlugImageRows(t *testing.T, image map[string]any) map[string]driver.Record {
	t.Helper()
	keys := []string{"meta", "snapshot", "rows", "versions", "changesets", "changeset_cursor", "import_receipt", "source_archive", "derived", "derived_observations", "receipts"}
	if len(image) != len(keys) {
		t.Fatal("native-origin image plane census changed")
	}
	for _, key := range keys {
		if _, ok := image[key]; !ok {
			t.Fatalf("native-origin image is missing %s", key)
		}
	}
	meta := image["meta"].(driver.Meta)
	head, headErr := meta.Head.Uint64()
	floor, floorErr := meta.HistoryFloor.Uint64()
	if headErr != nil || floorErr != nil || floor == 0 || head < floor {
		t.Fatal("native-origin image requires its actual nonzero snapshot floor")
	}
	if !reflect.DeepEqual(image["snapshot"], driver.View{Branch: model.Natural64(0), At: &meta.Head, Incarnation: meta.Incarnation}) {
		t.Fatal("native-origin image snapshot is not bound to its complete head")
	}
	index := map[string]driver.Record{}
	versions := image["versions"].(map[string][]driver.Version)
	for _, row := range image["rows"].([]driver.Record) {
		if _, duplicate := index[row.Ref.ID]; duplicate || row.Ref.Validate() != nil || !row.Ref.IsID() || row.Ref.ID == "" || !row.Ref.Branch.IsZero() {
			t.Fatal("native-origin full-reader row identity is duplicate or invalid")
		}
		index[row.Ref.ID] = row
		history, ok := versions[row.Ref.ID]
		if !ok || len(history) == 0 || history[len(history)-1].Revision != row.Revision {
			t.Fatalf("native-origin row/history census differs for %s", row.Ref.ID)
		}
		var previous uint64
		for _, version := range history {
			revision, err := version.Revision.Uint64()
			if err != nil || revision < floor || revision <= previous || revision > head || !reflect.DeepEqual(version.Ref, row.Ref) {
				t.Fatalf("native-origin duplicate, unordered or misbound version for %s", row.Ref.ID)
			}
			previous = revision
		}
	}
	if len(index) == 0 || len(versions) != len(index) {
		t.Fatal("native-origin image lost its complete row/version owner census")
	}
	changes := image["changesets"].([]model.Changeset)
	if uint64(len(changes)) != head-floor {
		t.Fatal("native-origin public history is incomplete above its retained import floor")
	}
	for i, entry := range changes {
		if entry.Revision != model.Natural64(floor+uint64(i)+1) || entry.Incarnation != meta.Incarnation || entry.Branch != model.Natural64(0) {
			t.Fatal("native-origin public history has a duplicate, gap or wrong snapshot binding")
		}
	}
	tables := []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins}
	derived := image["derived"].(map[driver.DerivedTable][]driver.DerivedRow)
	observations := image["derived_observations"].(map[driver.DerivedTable][]driver.LockState)
	if len(derived) != len(tables) || len(observations) != len(tables) {
		t.Fatal("native-origin nine-table derived census changed")
	}
	for _, table := range tables {
		rows, ok := derived[table]
		states, observed := observations[table]
		if !ok || !observed || len(states) != len(rows) {
			t.Fatalf("native-origin derived table/observation census differs: %s", table)
		}
		seen := map[string]bool{}
		for i, row := range rows {
			if seen[row.Key] || i > 0 && rows[i-1].Key >= row.Key {
				t.Fatalf("native-origin duplicate or unordered derived row: %s/%s", table, row.Key)
			}
			seen[row.Key] = true
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: table, Key: row.Key}}
			state := states[i]
			revision, err := state.Revision.Uint64()
			if key.Validate() != nil || !reflect.DeepEqual(state.Key, key) || !state.Present || state.Record != nil || state.Derived == nil || err != nil || revision > head {
				t.Fatalf("native-origin derived observation has wrong key/presence/payload/revision: %s/%s", table, row.Key)
			}
			if state.Derived.Key != row.Key || !reflect.DeepEqual(state.Derived.Value, row.Value) || row.Index != nil && !reflect.DeepEqual(state.Derived.Index, row.Index) {
				t.Fatalf("native-origin full derived observation differs from its enumerated row: %s/%s", table, row.Key)
			}
		}
	}
	receiptKeys := []string{"s1-native-public-0", "s1-native-public-1", "s1-native-public-2", "s1-native-public-3", "s1-native-public-equal-labels", "s1-native-public-refused-aux", "s1-native-public-sqlite-write-fault"}
	receipts := image["receipts"].(map[string]any)
	if len(receipts) != len(receiptKeys) {
		t.Fatal("native-origin declared receipt census changed")
	}
	for _, key := range receiptKeys {
		if _, ok := receipts[key]; !ok {
			t.Fatalf("native-origin declared receipt missing: %s", key)
		}
	}
	return index
}

func assertNativePlugBaseline(t *testing.T, image map[string]any, profile driver.RestoreImportProfile) {
	t.Helper()
	rows := nativePlugImageRows(t, image)
	meta := image["meta"].(driver.Meta)
	if meta.Head != meta.HistoryFloor || len(image["changesets"].([]model.Changeset)) != 0 {
		t.Fatal("native-origin first preimage is not the actual snapshot-import baseline")
	}
	imported := image["import_receipt"].(*driver.IdempotencyRecord)
	versions := image["versions"].(map[string][]driver.Version)
	for id, row := range rows {
		want := driver.Version{Ref: row.Ref, Revision: meta.HistoryFloor, Base: true, Body: row.Body, CreatedAt: imported.Changeset.Time, Principal: imported.Changeset.Principal}
		if row.Revision != meta.HistoryFloor || len(versions[id]) != 1 || !reflect.DeepEqual(versions[id][0], want) {
			t.Fatalf("native-origin baseline is missing its exact imported base for %s", id)
		}
	}
	for key, receipt := range image["receipts"].(map[string]any) {
		if receipt != nil {
			t.Fatalf("native-origin operation receipt predates its recording: %s", key)
		}
	}
	assertNativePlugImportRows(t, image, rows, profile)
}

// Bind the saved baseline to the actual retained input, not just to itself.
// This checks complete raw issue fields and auxiliary source bodies; it does
// not turn retained provenance into proof of every typed interchange semantic.
func assertNativePlugImportRows(t *testing.T, image map[string]any, rows map[string]driver.Record, profile driver.RestoreImportProfile) {
	t.Helper()
	archiveBytes := image["source_archive"].([]byte)
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil || hashBytes(archiveBytes) != profile.ArchiveDigest {
		t.Fatalf("native-origin import baseline lacks its exact retained input: %v", err)
	}
	manifest := archive.Manifest()
	if driver.StoreID(manifest.StoreID) != profile.SourceStoreID || profile.HistoryMode != driver.RestoreHistoryModeSnapshot {
		t.Fatal("native-origin import baseline differs from its admitted snapshot source")
	}
	wantKinds := map[string]string{}
	add := func(id, kind string) {
		if _, duplicate := wantKinds[id]; duplicate {
			t.Fatalf("native-origin input/generated row identity collides: %s", id)
		}
		wantKinds[id] = kind
	}
	// These are the generated records in this exact product's snapshot importer.
	// Additional generated kinds require their own source-defined contract.
	add("restore/archive", "restore-archive")
	add("instance_config/restore-import/binding", "instance_config")
	for _, project := range manifest.Projects {
		add("instance_config/restore-import/project/"+project.Prefix, "instance_config")
	}
	requireJSON := func(id string, got []byte, want any) {
		wantBytes, encodeErr := json.Marshal(want)
		actual, actualErr := readNativeJSON(got)
		expected, expectedErr := readNativeJSON(wantBytes)
		if encodeErr != nil || actualErr != nil || expectedErr != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatalf("native-origin complete retained source payload differs: %s", id)
		}
	}
	for _, kind := range manifest.Kinds {
		sources := archive.Records(kind.Name)
		if len(sources) != kind.Count {
			t.Fatalf("native-origin input kind census differs: %s", kind.Name)
		}
		for _, source := range sources {
			id := source.ID
			if kind.Name != "bead" {
				id = "source/" + kind.Name + "/" + source.ID
			}
			add(id, kind.Name)
			row, present := rows[id]
			if !present || row.Kind != kind.Name {
				t.Fatalf("native-origin input row is absent or has another kind: %s", id)
			}
			if kind.Name == "bead" {
				doc, err := pipeline.DecodeDocument(row.Body)
				if err != nil || doc.ID != source.ID || doc.Project != source.Project {
					t.Fatalf("native-origin source bead identity differs: %s: %v", id, err)
				}
				requireJSON(id+"/beads.source_fields", doc.Metadata["beads.source_fields"], source.Source.Fields)
				continue
			}
			body := make(map[string]any, len(source.Body)+3)
			for key, value := range source.Body {
				body[key] = value
			}
			body["source_id"], body["source_table"], body["source"] = source.ID, source.Source.Table, source.Source.Fields
			if kind.Name == "beads-archive" {
				body["read_only"], body["redactable"] = true, true
			}
			requireJSON(id, row.Body, body)
		}
	}
	if len(rows) != len(wantKinds) {
		t.Fatalf("native-origin complete import row census differs: got %d, want %d", len(rows), len(wantKinds))
	}
	ids := make([]string, 0, len(wantKinds))
	for id, kind := range wantKinds {
		if row, present := rows[id]; !present || row.Kind != kind {
			t.Fatalf("native-origin generated/source row is absent or has another kind: %s", id)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	imported := image["import_receipt"].(*driver.IdempotencyRecord)
	meta := image["meta"].(driver.Meta)
	wantCommit := model.Commit{Revision: meta.HistoryFloor, Results: []model.OpResult{}, Warnings: []model.Warning{}}
	wantChanges := make([]model.ChangeRecord, 0, len(ids))
	for _, id := range ids {
		row := rows[id]
		wantCommit.Results = append(wantCommit.Results, model.OpResult{Ref: row.Ref, Revision: row.Revision, Record: json.RawMessage(row.Body)})
		wantChanges = append(wantChanges, model.ChangeRecord{Ref: row.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}})
	}
	if !reflect.DeepEqual(imported.Commit, wantCommit) || !reflect.DeepEqual(imported.Changeset.Changes, wantChanges) || !reflect.DeepEqual(imported.Changeset.Principal, profile.Admission.Actor) || imported.Changeset.IdempotencyKey != profile.IdempotencyKey {
		t.Fatal("native-origin import receipt does not bind every complete saved row and creation exactly once in order")
	}
}

// The four declared scalar/label updates have no hierarchy edits or mutators.
// Conserve every captured plane, permitting only the requested target body,
// one revision/version/legacy changeset and that operation's finalized receipt.
// The current Writer also publishes the touched bead's rank, source-selection
// rows and eligible ancestor annotations at that same revision.
func assertNativePlugWriteDelta(t *testing.T, store driver.Store, before, after map[string]any, req service.UpdateRequest, meta model.WriteMeta, actor model.PrincipalRef, commit model.Commit, receiptKey string, interval [2]time.Time) {
	t.Helper()
	oldRows, newRows := nativePlugImageRows(t, before), nativePlugImageRows(t, after)
	oldMeta, newMeta := before["meta"].(driver.Meta), after["meta"].(driver.Meta)
	head, err := oldMeta.Head.Uint64()
	if err != nil || head == ^uint64(0) {
		t.Fatal("native-origin preimage head cannot advance")
	}
	revision := model.Natural64(head + 1)
	wantMeta := oldMeta
	wantMeta.Head = revision
	wantView := before["snapshot"].(driver.View)
	wantView.At = &revision
	if !reflect.DeepEqual(newMeta, wantMeta) || !reflect.DeepEqual(after["snapshot"], wantView) || commit.Revision != revision {
		t.Fatal("native-origin successful write changed identity/floor or advanced by other than one revision")
	}
	for _, key := range []string{"changeset_cursor", "import_receipt", "source_archive"} {
		if !reflect.DeepEqual(before[key], after[key]) {
			t.Fatalf("native-origin successful write changed conserved %s", key)
		}
	}
	old, found := oldRows[req.ID.ID]
	actual, present := newRows[req.ID.ID]
	if !found || !present || len(oldRows) != len(newRows) || !reflect.DeepEqual(old.Ref, req.ID) || len(req.Fields) == 0 {
		t.Fatal("native-origin scalar target or complete row census changed")
	}
	for id, row := range oldRows {
		if id != req.ID.ID && !reflect.DeepEqual(row, newRows[id]) {
			t.Fatalf("native-origin successful write changed non-target row %s", id)
		}
	}
	// The existing strict decoder preserves number spelling/types and rejects
	// duplicate members. Never round unpatched metadata/scalars through float64.
	decode := func(data []byte) any {
		value, err := readNativeJSON(data)
		if err != nil {
			t.Fatalf("native-origin successful-write JSON: %v", err)
		}
		return value
	}
	expected, ok := decode(old.Body).(map[string]any)
	if !ok {
		t.Fatal("native-origin scalar target is not an object")
	}
	fields, ok := expected["fields"].(map[string]any)
	if !ok {
		t.Fatal("native-origin scalar target lacks fields")
	}
	setBy := map[string]string{}
	paths := []string{}
	for field, value := range req.Fields {
		// This is the fixed recorder workload, not a permissive general patch
		// emulator: reject an expanded request until its delta is declared.
		switch field {
		case "title", "priority", "description":
		case "labels":
			if !reflect.DeepEqual(value, []string{"native-reviewed"}) {
				t.Fatal("native-origin undeclared label replacement")
			}
			setBy["labels"] = "request"
			paths = append(paths, "/labels")
		default:
			t.Fatalf("native-origin undeclared successful field delta %s", field)
		}
		data, err := json.Marshal(value)
		if err != nil || value == nil {
			t.Fatalf("native-origin undeclared scalar value: %v", err)
		}
		fields[field] = decode(data)
		if field == "labels" {
			expected["labels"] = fields[field]
		}
		setBy["fields."+field] = "request"
		paths = append(paths, "/fields/"+field)
	}
	sort.Strings(paths)
	if !reflect.DeepEqual(expected, decode(actual.Body)) || !reflect.DeepEqual(old.Ref, actual.Ref) || old.Kind != actual.Kind || actual.Revision != revision {
		t.Fatal("native-origin target body/envelope has an undeclared successful-write delta")
	}
	wantCommit := model.Commit{Revision: revision, Results: []model.OpResult{{Ref: old.Ref, Revision: revision, Record: json.RawMessage(actual.Body), SetBy: setBy}}, Warnings: []model.Warning{}}
	if !reflect.DeepEqual(commit, wantCommit) {
		t.Fatal("native-origin returned commit does not describe exactly the declared target delta")
	}
	oldChanges, newChanges := before["changesets"].([]model.Changeset), after["changesets"].([]model.Changeset)
	if len(newChanges) != len(oldChanges)+1 {
		t.Fatal("native-origin successful write did not append exactly one changeset")
	}
	for i, previous := range oldChanges {
		if !reflect.DeepEqual(previous, newChanges[i]) {
			t.Fatal("native-origin successful write rewrote prior public history")
		}
	}
	entry := newChanges[len(oldChanges)]
	_, offset := entry.Time.Zone()
	if entry.Time.IsZero() || offset != 0 || meta.IdempotencyKey != hashBytes([]byte(receiptKey)) {
		t.Fatal("native-origin changeset lacks its actual UTC time or request key")
	}
	wantEntry := model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: oldMeta.Incarnation, Kind: model.ChangesetWrite, Time: entry.Time, Principal: actor, IdempotencyKey: meta.IdempotencyKey, Changes: []model.ChangeRecord{{Ref: old.Ref, Op: model.OpPatch, FromRevision: old.Revision, ChangedPaths: paths}}}
	// PostgreSQL Commit explicitly stores an empty authorization chain when
	// the legacy Writer header supplies nil; memory and SQLite preserve nil.
	// Bind that exact representation to the real engine, never to the observed
	// entry. Keep all captured slices and every other header field unchanged.
	switch store.(type) {
	case *postgres.Store:
		wantEntry.Chain = []string{}
	case *memory.Store, *sqlite.Store:
	default:
		t.Fatalf("native-origin changeset oracle has no declared engine: %T", store)
	}
	if !reflect.DeepEqual(entry, wantEntry) {
		t.Fatal("native-origin new changeset has an undeclared identity/path/event/mutator delta")
	}
	assertNativeReadySortDelta(t, before, after, actual, interval, entry.Time)
	oldVersions, newVersions := before["versions"].(map[string][]driver.Version), after["versions"].(map[string][]driver.Version)
	for id, history := range oldVersions {
		if id != req.ID.ID {
			if !reflect.DeepEqual(history, newVersions[id]) {
				t.Fatalf("native-origin successful write changed non-target history %s", id)
			}
			continue
		}
		next := newVersions[id]
		if len(next) != len(history)+1 || !reflect.DeepEqual(history, next[:len(history)]) {
			t.Fatal("native-origin successful write rewrote target history")
		}
		version := driver.Version{Ref: old.Ref, Revision: revision, CreatedAt: entry.Time, Principal: actor, Base: len(history)%driver.VersionBaseEvery == 0 || history[len(history)-1].Deleted}
		if version.Base {
			version.Body = actual.Body
		} else {
			version.Patch, err = json.Marshal([]map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(actual.Body)}})
			if err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(next[len(history)], version) {
			t.Fatal("native-origin appended version differs from the exact committed replacement/base")
		}
	}
	// Independently lower the named request and its default full-return mode
	// into the pre-gate request-hash contract; do not learn a hash from storage.
	if meta.Return != "" || meta.Method != "" || meta.GateAction != "" || meta.ClaimNextIdentity != "" || meta.DryRun {
		t.Fatal("native-origin successful-write request exceeded its declared metadata scope")
	}
	requestBytes, err := json.Marshal(struct {
		ChangeSet               model.ChangeSet
		Method, Project, Return string
		ClaimNextIdentity       string `json:",omitempty"`
		DryRun                  bool
	}{model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.ID, Fields: req.Fields}}}, Expect: req.Expect}, "bead.update", meta.Project, "full", "", false})
	if err != nil {
		t.Fatal(err)
	}
	oldReceipts, newReceipts := before["receipts"].(map[string]any), after["receipts"].(map[string]any)
	if _, ok := oldReceipts[receiptKey]; !ok || oldReceipts[receiptKey] != nil {
		t.Fatal("native-origin successful-write receipt was not absent in its preimage")
	}
	for key, previous := range oldReceipts {
		if key == receiptKey {
			want := map[string]any{"request_hash": hashBytes(requestBytes), "commit": commit, "response_pending": false, "changeset": entry}
			if !reflect.DeepEqual(newReceipts[key], want) {
				t.Fatal("native-origin successful write did not finalize its one exact durable receipt")
			}
		} else if !reflect.DeepEqual(previous, newReceipts[key]) {
			t.Fatalf("native-origin successful write changed unrelated receipt %s", key)
		}
	}
}

// Validate the Writer's complete source-defined projection delta, including
// full annotations and observed revisions. Expectations come from the already
// conserved target body and the preimage, never from the new derived payloads.
func assertNativeReadySortDelta(t *testing.T, before, after map[string]any, target driver.Record, interval [2]time.Time, committedAt time.Time) {
	t.Helper()
	key := "ready/" + target.Ref.ID
	oldRows := before["derived"].(map[driver.DerivedTable][]driver.DerivedRow)
	newRows := after["derived"].(map[driver.DerivedTable][]driver.DerivedRow)
	oldStates := before["derived_observations"].(map[driver.DerivedTable][]driver.LockState)
	newStates := after["derived_observations"].(map[driver.DerivedTable][]driver.LockState)
	var changed *driver.DerivedRow
	for i := range newRows[driver.DerivedSortKey] {
		row := &newRows[driver.DerivedSortKey][i]
		if row.Key == key {
			changed = row
		}
	}
	if changed == nil {
		t.Fatal("native-origin write omitted its exact ready sort projection")
	}
	id, ranked, err := pipeline.DecodeReadySortKey(*changed)
	if err != nil || id != target.Ref.ID || interval[0].IsZero() || interval[1].Before(interval[0]) || ranked.RankedAt.Before(interval[0]) || ranked.RankedAt.After(committedAt) || committedAt.After(interval[1]) {
		t.Fatalf("native-origin ready sort clock/identity is outside the actual write interval: %v", err)
	}
	doc, err := pipeline.DecodeDocument(target.Body)
	if err != nil || doc.CreatedAt == nil || doc.CreatedAt.IsZero() {
		t.Fatalf("native-origin ready sort projection lacks its original creation instant: %v", err)
	}
	body, err := readNativeJSON(target.Body)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := body.(map[string]any)
	if !ok {
		t.Fatal("native-origin ready sort source is not an object")
	}
	fields, ok := object["fields"].(map[string]any)
	if !ok {
		t.Fatal("native-origin ready sort source lacks fields")
	}
	priority, ok := fields["priority"].(json.Number)
	if !ok {
		t.Fatal("native-origin ready sort priority is not a captured number")
	}
	p, err := priority.Int64()
	if err != nil || p < 0 || p > 4 {
		t.Fatal("native-origin ready sort priority is not a declared integer")
	}
	// B01 uses >= at the 48-hour boundary. Compute the classifier from the
	// verified target document and bounded actual evaluation time, independently
	// of service.ReadySortKey and of the stored bucket/rank values.
	bucket, rank := 0, int(p)
	if doc.CreatedAt.Before(ranked.RankedAt.Add(-48 * time.Hour)) {
		bucket, rank = 1, 999
	}
	value, err := json.Marshal(pipeline.ReadySortKey{Bucket: bucket, Rank: rank, CreatedAt: doc.CreatedAt.UTC(), RankedAt: ranked.RankedAt, Revision: target.Revision})
	if err != nil {
		t.Fatal(err)
	}
	wantRow := driver.DerivedRow{Key: key, Value: value}
	updates := map[driver.DerivedTable]map[string]driver.DerivedRow{}
	add := func(table driver.DerivedTable, row driver.DerivedRow) {
		if updates[table] == nil {
			updates[table] = map[string]driver.DerivedRow{}
		}
		if _, duplicate := updates[table][row.Key]; duplicate {
			t.Fatalf("native-origin duplicate expected projection: %s/%s", table, row.Key)
		}
		updates[table][row.Key] = row
	}
	add(driver.DerivedSortKey, wantRow) // The legacy rank remains unannotated.
	incarnation := before["meta"].(driver.Meta).Incarnation
	ref := model.RefByID(target.Ref.ID, model.Natural64(0))
	indexFor := func(table driver.DerivedTable, row driver.DerivedRow, source driver.Record) *driver.SelectionIndex {
		return &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: driver.DerivedRowKey{Table: table, Key: row.Key},
			Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: hashBytes(row.Value),
			Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: source.Revision, Present: true, Digest: hashBytes(source.Body)},
		}
	}
	// Build the three logical source rows directly from the conserved document.
	// Decode the literal envelope without inferring a legacy parent or filling
	// an omitted link targetKind; those source spellings remain observable.
	var projectionDoc pipeline.Document
	decoder := json.NewDecoder(bytes.NewReader(target.Body))
	decoder.UseNumber()
	if err := decoder.Decode(&projectionDoc); err != nil {
		t.Fatal(err)
	}
	// Hash the independently conserved source fields in the declared Ready leaf
	// wire order; never copy a digest from the observed derived postimage.
	membershipStatus, _ := projectionDoc.Fields["status"].(string)
	membershipValue, err := json.Marshal(struct {
		ID       string         `json:"id"`
		Type     string         `json:"type"`
		Project  string         `json:"project"`
		Status   string         `json:"status"`
		Revision model.Revision `json:"revision"`
		Fields   map[string]any `json:"fields,omitempty"`
	}{
		ID: projectionDoc.ID, Type: projectionDoc.Type, Project: projectionDoc.Project,
		Status: membershipStatus, Revision: target.Revision, Fields: projectionDoc.Fields,
	})
	if err != nil {
		t.Fatal(err)
	}
	bead := driver.SelectionBead{Ref: ref, Type: projectionDoc.Type, Parent: projectionDoc.Parent, MembershipLeaf: hashBytes(membershipValue)}
	if project, present := object["project"]; present {
		name, ok := project.(string)
		if !ok {
			t.Fatal("native-origin projection project is not a declared string")
		}
		bead.Project = &name
	}
	for _, column := range []driver.SelectionColumn{driver.SelectionStatus, driver.SelectionTitle, driver.SelectionTemplate, driver.SelectionPriority} {
		field, present := fields[string(column)]
		if !present {
			bead.Absent = append(bead.Absent, column)
			continue
		}
		valid := false
		switch column {
		case driver.SelectionStatus:
			bead.Status, valid = field.(string)
		case driver.SelectionTitle:
			bead.Title, valid = field.(string)
		case driver.SelectionTemplate:
			bead.Template, valid = field.(bool)
		case driver.SelectionPriority:
			bead.Priority, valid = int64(p), true
		}
		if !valid {
			t.Fatalf("native-origin projection has an undeclared %s value", column)
		}
	}
	labels := driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{}}
	for _, label := range projectionDoc.Labels {
		labels.Rows = append(labels.Rows, driver.SelectionFact{Kind: driver.SelectionLabelFact, Ref: &ref, Name: label, Value: label})
	}
	links := driver.SelectionFacts{Kind: driver.SelectionLinkFact, Rows: []driver.SelectionFact{}}
	for _, link := range projectionDoc.Links {
		if link.TargetKind == model.LinkTargetExternal {
			continue
		}
		local := driver.SourceLink{Kind: link.Kind, To: link.To}
		links.Rows = append(links.Rows, driver.SelectionFact{Kind: driver.SelectionLinkFact, Ref: &ref, Link: &local})
	}
	for _, projection := range []struct {
		table driver.DerivedTable
		value any
		facts *driver.SelectionFacts
	}{
		{driver.DerivedSortKey, bead, nil},
		{driver.DerivedAttribute, labels, &labels},
		{driver.DerivedReferenceIndex, links, &links},
	} {
		logical, err := json.Marshal(projection.value)
		if err != nil {
			t.Fatal(err)
		}
		row := driver.DerivedRow{Key: target.Ref.ID, Value: logical}
		row.Index = indexFor(projection.table, row, target)
		if projection.facts != nil {
			facts := *projection.facts
			// The logical JSON retains Rows:[], while storage-owned empty
			// annotation slices are nil. Keep their expectations separate.
			if len(facts.Rows) == 0 {
				facts.Rows = nil
			}
			row.Index.Facts = &facts
		} else {
			row.Index.Bead = &bead
			canonical, err := json.Marshal(projectionDoc)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(canonical, target.Body) {
				queue := &driver.QueueFacts{Version: driver.QueueFactsVersion, CreatedAt: projectionDoc.CreatedAt.UTC(), LeasePresent: projectionDoc.Lease != nil}
				if blocked, present := fields["blocked"]; present {
					var ok bool
					queue.Blocked, ok = blocked.(bool)
					if !ok {
						t.Fatal("native-origin queue blocked field is not boolean")
					}
				}
				for _, link := range projectionDoc.Links {
					queue.Links = append(queue.Links, driver.QueueLink{Kind: link.Kind, TargetKind: string(link.TargetKind), To: link.To, Metadata: link.Metadata})
				}
				row.Index.Queue = queue
			}
		}
		add(projection.table, row)
	}
	// Scalar/label writes keep the entire target ancestor chain unchanged. Prove
	// that chain from the preimage source records before expecting its refresh;
	// a stale or malformed annotation must never become an accepted new baseline.
	preimage := nativePlugImageRows(t, before)
	oldTarget := preimage[target.Ref.ID]
	chain := map[string]driver.DerivedRow{}
	seen := map[string]bool{}
	for ancestor, depth := ref, uint64(0); ; depth++ {
		if seen[ancestor.ID] {
			t.Fatal("native-origin preimage ancestor chain is cyclic")
		}
		seen[ancestor.ID] = true
		source, present := preimage[ancestor.ID]
		if !present || source.Kind != "bead" || !reflect.DeepEqual(source.Ref, ancestor) {
			t.Fatal("native-origin preimage ancestor source is missing or misbound")
		}
		row, err := pipeline.AncestorRow(ref.ID, ancestor.ID, depth)
		if err != nil {
			t.Fatal(err)
		}
		row.Index = indexFor(driver.DerivedAncestorClosure, row, oldTarget)
		row.Index.Facts = &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &source.Ref, Depth: depth}}}
		chain[row.Key] = row
		parent, err := pipeline.DecodeDocument(source.Body)
		if err != nil {
			t.Fatal(err)
		}
		if parent.Parent == nil {
			break
		}
		ancestor = model.RefByID(parent.Parent.ID, model.Natural64(0))
	}
	for _, state := range oldStates[driver.DerivedAncestorClosure] {
		if state.Derived == nil {
			t.Fatal("native-origin ancestor observation lacks its complete row")
		}
		row := *state.Derived
		descendant, _, _, err := pipeline.DecodeAncestorRow(row)
		if err != nil {
			t.Fatal(err)
		}
		if descendant != ref.ID {
			continue
		}
		expected, present := chain[row.Key]
		if !present || !bytes.Equal(row.Value, expected.Value) || row.Index != nil && !reflect.DeepEqual(row.Index, expected.Index) {
			t.Fatal("native-origin preimage ancestor is not the complete exact old-source projection")
		}
		delete(chain, row.Key)
		expected.Index.Source.Revision, expected.Index.Source.Digest = target.Revision, hashBytes(target.Body)
		add(driver.DerivedAncestorClosure, expected)
	}
	if len(chain) != 0 {
		t.Fatal("native-origin preimage is missing target ancestor rows")
	}
	if len(oldRows) != len(newRows) || len(oldStates) != len(newStates) || len(oldRows) != len(oldStates) {
		t.Fatal("native-origin projection changed the complete derived-table census")
	}
	for table, previous := range oldRows {
		if _, present := newRows[table]; !present {
			t.Fatalf("native-origin projection dropped derived table %s", table)
		}
		if _, present := newStates[table]; !present {
			t.Fatalf("native-origin projection dropped observations for %s", table)
		}
		affected := updates[table]
		if len(affected) == 0 {
			if !reflect.DeepEqual(previous, newRows[table]) || !reflect.DeepEqual(oldStates[table], newStates[table]) {
				t.Fatalf("native-origin projection changed a conserved derived table: %s", table)
			}
			continue
		}
		wantRows := make([]driver.DerivedRow, 0, len(previous)+len(affected))
		wantStates := make([]driver.LockState, 0, len(oldStates[table])+len(affected))
		for _, row := range previous {
			if _, changed := affected[row.Key]; !changed {
				wantRows = append(wantRows, row)
			}
		}
		for _, state := range oldStates[table] {
			if _, changed := affected[state.Key.Derived.Key]; !changed {
				wantStates = append(wantStates, state)
			}
		}
		for _, row := range affected {
			wantRows = append(wantRows, row)
			key := driver.DerivedRowKey{Table: table, Key: row.Key}
			copy := row
			wantStates = append(wantStates, driver.LockState{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &key}, Revision: target.Revision, Derived: &copy, Present: true})
		}
		sort.Slice(wantRows, func(i, j int) bool { return wantRows[i].Key < wantRows[j].Key })
		sort.Slice(wantStates, func(i, j int) bool { return wantStates[i].Key.Derived.Key < wantStates[j].Key.Derived.Key })
		if !reflect.DeepEqual(wantRows, newRows[table]) || !reflect.DeepEqual(wantStates, newStates[table]) {
			t.Fatalf("native-origin projection differs from its complete expected rows/indexes/revisions: %s", table)
		}
	}
}

// These are real public service writes on the just-imported native rows. The
// offline import itself makes no subscriber claim. Evidence is flushed before
// every assertion, including a refused or unexpectedly published operation.
func recordPublicSubscriber(t *testing.T, ctx context.Context, store driver.Store, path string, ids []string, profile driver.RestoreImportProfile, sqlitePath string) func(driver.Store) {
	t.Helper()
	if len(ids) != 3 {
		t.Fatal("public subscriber recording requires exact root, child and dependency IDs")
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	ontologyRows, err := r.DerivedRows(ctx, driver.DerivedOntology, "")
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(ontologyRows) != 1 || ontologyRows[0].Key != "builtin" {
		t.Fatalf("native replay stored ontology: %v / %v", err, closeErr)
	}
	doc, err := ontology.Decode(ontologyRows[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, problems := ontology.Compile(doc, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("native replay stored ontology compilation: %v", problems)
	}
	lease, err := store.Lease(ctx, "s1-native-public-subscriber", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	observer := &publicPostRecorder{}
	rules := service.NewOntology(snapshot.Effective)
	w := pipeline.New(store, lease, pipeline.WithOntology(rules), pipeline.WithPostSubscribers(pipeline.PostBinding{ID: "s1-native-public-observer", Subscriber: observer}))
	domain := service.New(store, lease, w, service.WithOntology(rules))
	actor := model.PrincipalRef{Kind: "user", ID: "public-recorder"}
	read := func() map[string]pipeline.Document {
		meta, err := store.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		refs := []model.Ref{model.RefByID(ids[0], model.Natural64(0)), model.RefByID(ids[1], model.Natural64(0)), model.RefByID(ids[2], model.Natural64(0))}
		rows, err := r.Get(ctx, refs)
		if err != nil || len(rows) != len(refs) {
			t.Fatalf("native public readback: %v", err)
		}
		out := map[string]pipeline.Document{}
		for _, row := range rows {
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			out[doc.ID] = doc
		}
		return out
	}
	var receipts []json.RawMessage
	postCount := func() int {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		return len(observer.Observations)
	}
	appendReceipt := func(v any) {
		writeRawRecordingValue(t, path+fmt.Sprintf(".operation-%d.raw-fields.json", len(receipts)), v)
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, data)
	}
	flush := func() {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		writeRawRecordingValue(t, path+".posts.raw-fields.json", observer.Observations)
		data, err := json.MarshalIndent(map[string]any{"schema": "plug-s1-native-public-subscriber/1", "surface": "service.UpdateWithOutcome", "operations": receipts, "posts": observer.Observations}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		writeRecordingBytes(t, path, append(data, '\n'))
	}
	// Capture each actual return before a reader, assertion or custom JSON
	// marshaler can abort. The later full image is saved before JSON receipts
	// and behavioral assertions, including on unexpected update/replay errors.
	retainReturn := func(label string, outcome port.WriteOutcome, err error) *model.Error {
		var typed *model.Error
		errors.As(err, &typed)
		writeRawRecordingValue(t, path+"."+label+".return.raw-fields.json", map[string]any{"outcome": outcome, "error_type": fmt.Sprintf("%T", err), "error": err, "typed_error": typed})
		return typed
	}
	type rawCommitFields model.Commit
	rawOutcome := func(outcome port.WriteOutcome) map[string]any {
		return map[string]any{"Commit": rawCommitFields(outcome.Commit), "Replayed": outcome.Replayed, "NullResult": outcome.NullResult}
	}
	validateSuccessfulOutcome := func(outcome port.WriteOutcome, err error) {
		if err == nil {
			if _, encodeErr := json.Marshal(outcome); encodeErr != nil {
				t.Fatalf("invalid successful public outcome after raw evidence capture: %v", encodeErr)
			}
		}
	}
	requests := []service.UpdateRequest{
		{ID: model.RefByID(ids[0], model.Natural64(0)), Fields: map[string]any{"title": "Public root after scalar update", "priority": 1, "description": "Plug scalar conservation"}},
		{ID: model.RefByID(ids[1], model.Natural64(0)), Fields: map[string]any{"title": "Public child after scalar update"}},
		{ID: model.RefByID(ids[2], model.Natural64(0)), Fields: map[string]any{"title": "Closed dependency after Plug scalar update"}},
		{ID: model.RefByID(ids[0], model.Natural64(0)), Fields: map[string]any{"labels": []string{"native-reviewed"}}},
	}
	baseline := nativePlugImage(t, ctx, store, profile)
	writeRecordingValue(t, path+".baseline.json", baseline)
	assertNativePlugBaseline(t, baseline, profile)
	previousImage := baseline
	var last port.WriteOutcome
	for i, req := range requests {
		preimage := nativePlugImage(t, ctx, store, profile)
		preimagePath := path + fmt.Sprintf(".before-write-%d.json", i)
		writeRecordingValue(t, preimagePath, preimage)
		if !reflect.DeepEqual(previousImage, preimage) {
			t.Fatal("native-origin complete preimage changed between recorded operations")
		}
		before := read()
		receiptKey := fmt.Sprintf("s1-native-public-%d", i)
		meta := model.WriteMeta{Project: before[req.ID.ID].Project, IdempotencyKey: hashBytes([]byte(receiptKey))}
		startedAt := time.Now().UTC()
		outcome, err := domain.UpdateWithOutcome(ctx, actor, req, meta)
		interval := [2]time.Time{startedAt, time.Now().UTC()}
		typed := retainReturn(fmt.Sprintf("write-%d", i), outcome, err)
		writeRecordingValue(t, path+fmt.Sprintf(".write-%d.interval.json", i), interval)
		complete := nativePlugImage(t, ctx, store, profile)
		writeRecordingValue(t, path+fmt.Sprintf(".write-%d.json", i), complete)
		after := read()
		appendReceipt(map[string]any{"method": "bead.update", "request": req, "meta": meta, "outcome": rawOutcome(outcome), "error": fmt.Sprint(err), "typed_error": typed, "before": before, "after": after, "complete_preimage": preimagePath, "call_interval": interval})
		flush()
		validateSuccessfulOutcome(outcome, err)
		if err != nil || outcome.Replayed || len(outcome.Commit.Warnings) != 0 || postCount() != i+1 {
			t.Fatalf("native public update/subscriber: %+v err=%v", outcome, err)
		}
		assertNativePlugWriteDelta(t, store, preimage, complete, req, meta, actor, outcome.Commit, receiptKey, interval)
		expected := before[req.ID.ID]
		for field, value := range req.Fields {
			expected.Fields[field] = value
			if field == "labels" {
				expected.Labels = value.([]string)
			}
		}
		before[req.ID.ID] = expected
		if !equalJSON(before, after) {
			t.Fatal("native public update changed unpatched fields, metadata, parent, links, comments or close provenance")
		}
		replayed, replayErr := domain.UpdateWithOutcome(ctx, actor, req, meta)
		replayTyped := retainReturn(fmt.Sprintf("replay-%d", i), replayed, replayErr)
		replayImage := nativePlugImage(t, ctx, store, profile)
		writeRecordingValue(t, path+fmt.Sprintf(".replay-%d.json", i), replayImage)
		appendReceipt(map[string]any{"method": "bead.update", "request": req, "meta": meta, "outcome": rawOutcome(replayed), "error": fmt.Sprint(replayErr), "typed_error": replayTyped, "after": read()})
		flush()
		validateSuccessfulOutcome(replayed, replayErr)
		if replayErr != nil || !replayed.Replayed || !equalJSON(outcome.Commit, replayed.Commit) || postCount() != i+1 || !equalJSON(after, read()) || !equalJSON(complete, replayImage) {
			t.Fatal("native public replay changed state, receipt or subscriber count")
		}
		previousImage = replayImage
		last = outcome
	}
	before := read()
	noopMeta := model.WriteMeta{Project: before[ids[0]].Project, IdempotencyKey: hashBytes([]byte("s1-native-public-equal-labels"))}
	noop, err := domain.UpdateWithOutcome(ctx, actor, requests[len(requests)-1], noopMeta)
	noopTyped := retainReturn("noop", noop, err)
	complete := nativePlugImage(t, ctx, store, profile)
	writeRecordingValue(t, path+".noop.json", complete)
	appendReceipt(map[string]any{"method": "bead.update", "request": requests[len(requests)-1], "meta": noopMeta, "outcome": rawOutcome(noop), "error": fmt.Sprint(err), "typed_error": noopTyped, "after": read()})
	flush()
	validateSuccessfulOutcome(noop, err)
	if err != nil || noop.Replayed || noop.Commit.Revision.String() != last.Commit.Revision.String() || postCount() != len(requests) || !equalJSON(before, read()) {
		t.Fatal("native public equal replacement created a revision, subscriber call or changed state")
	}
	closed := model.RefByID(ids[2], model.Natural64(0))
	bad := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: closed, Fields: map[string]any{"title": "must not land", "labels": []string{"must-not-land"}}}},
		{Kind: model.OpLink, Link: &model.LinkOp{From: closed, To: closed, LinkKind: "blocks"}},
	}}
	refusalMeta := model.WriteMeta{Project: before[ids[2]].Project, IdempotencyKey: hashBytes([]byte("s1-native-public-refused-aux"))}
	refused, refusalErr := domain.ApplyWithOutcome(ctx, actor, bad, refusalMeta)
	typed := retainReturn("refusal", refused, refusalErr)
	afterRefusal := nativePlugImage(t, ctx, store, profile)
	writeRecordingValue(t, path+".refusal.json", afterRefusal)
	// A refused operation has no valid public Commit response. Retain its
	// actual zero/nil fields without Commit.MarshalJSON's success validation;
	// never replace nil results/warnings with invented successful arrays.
	appendReceipt(map[string]any{"method": "tx.apply", "request": bad, "outcome": rawOutcome(refused), "outcome_encoding": "raw-failure-fields-not-public-commit", "error": fmt.Sprint(refusalErr), "typed_error": typed, "before": complete, "after": afterRefusal})
	flush()
	if typed == nil || typed.Data.Code != "ontology_guard_failed" || refused.Replayed || refused.NullResult || !refused.Commit.Revision.IsZero() || refused.Commit.Results != nil || refused.Commit.Warnings != nil || !equalJSON(complete, afterRefusal) || postCount() != len(requests) {
		t.Fatalf("native-origin auxiliary refusal changed a source/history/receipt/subscriber plane: %v", refusalErr)
	}
	var verifyFault func(driver.Store)
	if sqlitePath != "" {
		verifyFault = recordNativeSQLiteWriteFault(t, ctx, store, domain, observer, sqlitePath, path+".sqlite-write-fault", requests[0].ID, profile)
	}
	t.Logf("PUBLIC_SUBSCRIBER_RECORDING path=%s posts=%d scalar=3 replacement=1 replay=4 noop=1 refusal=1", path, postCount())
	return verifyFault
}

type nativeSQLiteObject struct {
	Type, Name, Table string
	SQL               any
}

type nativeSQLiteTable struct {
	Name    string
	Columns []string
	Rows    [][]any
}

type nativeSQLiteState struct {
	Schema []nativeSQLiteObject
	Tables []nativeSQLiteTable
}

// Enumerate every persisted SQLite table and schema object in one read
// transaction. SQL values retain their Go/SQL types and bytes; only row order
// is canonicalized, without dropping duplicates or omitting a table family.
func nativeSQLiteImage(t *testing.T, ctx context.Context, path string) nativeSQLiteState {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
	if err != nil {
		t.Fatal(err)
	}
	image := nativeSQLiteState{Schema: []nativeSQLiteObject{}, Tables: []nativeSQLiteTable{}}
	for rows.Next() {
		var object nativeSQLiteObject
		if err := rows.Scan(&object.Type, &object.Name, &object.Table, &object.SQL); err != nil {
			t.Fatal(err)
		}
		image.Schema = append(image.Schema, object)
		if object.Type == "table" {
			image.Tables = append(image.Tables, nativeSQLiteTable{Name: object.Name})
		}
	}
	rowsErr, closeErr := rows.Err(), rows.Close()
	if rowsErr != nil || closeErr != nil || len(image.Tables) == 0 {
		t.Fatalf("complete SQLite schema census: %v / %v", rowsErr, closeErr)
	}
	for i := range image.Tables {
		table := &image.Tables[i]
		rows, err := tx.QueryContext(ctx, `SELECT * FROM "`+strings.ReplaceAll(table.Name, `"`, `""`)+`"`)
		if err != nil {
			t.Fatal(err)
		}
		table.Columns, err = rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		type orderedRow struct {
			key    string
			values []any
		}
		ordered := []orderedRow{}
		for rows.Next() {
			values := make([]any, len(table.Columns))
			pointers := make([]any, len(values))
			for j := range values {
				pointers[j] = &values[j]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for j, value := range values {
				if data, ok := value.([]byte); ok {
					values[j] = bytes.Clone(data)
				}
			}
			key, err := json.Marshal(rawRecordingFields(reflect.ValueOf(values)))
			if err != nil {
				t.Fatal(err)
			}
			ordered = append(ordered, orderedRow{string(key), values})
		}
		rowsErr, closeErr := rows.Err(), rows.Close()
		if rowsErr != nil || closeErr != nil {
			t.Fatalf("complete SQLite table %s: %v / %v", table.Name, rowsErr, closeErr)
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].key < ordered[j].key })
		table.Rows = make([][]any, len(ordered))
		for j, row := range ordered {
			table.Rows[j] = row.values
		}
	}
	return image
}

// A real SQLite changeset INSERT fails only after the attempted target record
// and version are present inside that SQL transaction. The actual error code
// and trigger message distinguish this from prevalidation or a mocked failure.
// Return the assertion closure so the caller first releases its normal lease,
// closes/reopens the real driver and captures complete reopened evidence.
func recordNativeSQLiteWriteFault(t *testing.T, ctx context.Context, store driver.Store, domain *service.Service, observer *publicPostRecorder, dbPath, path string, target model.Ref, profile driver.RestoreImportProfile) func(driver.Store) {
	t.Helper()
	const receiptKey = "s1-native-public-sqlite-write-fault"
	const triggerName = "s1_native_reject_changeset"
	const cause = "s1_native_sqlite_changeset_write_fault"
	actor := model.PrincipalRef{Kind: "user", ID: "public-recorder"}
	before := nativePlugImage(t, ctx, store, profile)
	writeRecordingValue(t, path+".before.json", before)
	rows := nativePlugImageRows(t, before)
	targetRow, found := rows[target.ID]
	if !found {
		t.Fatal("SQLite fault target is absent from the native-origin preimage")
	}
	doc, err := pipeline.DecodeDocument(targetRow.Body)
	if err != nil {
		t.Fatal(err)
	}
	head, err := before["meta"].(driver.Meta).Head.Uint64()
	if err != nil || head == ^uint64(0) {
		t.Fatal("SQLite fault preimage cannot advance")
	}
	key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &target})
	if err != nil {
		t.Fatal(err)
	}
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
	request := service.UpdateRequest{ID: target, Fields: map[string]any{"title": "SQLite fault must roll back", "labels": []string{"sqlite-fault-must-not-land"}}}
	meta := model.WriteMeta{Project: doc.Project, IdempotencyKey: hashBytes([]byte(receiptKey))}
	trigger := fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON changesets
WHEN json_extract(CAST(NEW.data AS TEXT),'$.idempotencyKey')=%s
BEGIN
SELECT CASE WHEN NEW.rev<>%s
 OR NOT EXISTS (SELECT 1 FROM records WHERE key=%s AND json_extract(CAST(data AS TEXT),'$.Revision')=%d)
 OR NOT EXISTS (SELECT 1 FROM versions WHERE key=%s AND rev=NEW.rev)
 THEN RAISE(ABORT,'s1_native_sqlite_record_version_not_staged') END;
SELECT RAISE(ABORT,'%s'); END;`, triggerName, quote(meta.IdempotencyKey), quote(fmt.Sprintf("%020d", head+1)), quote(key), head+1, quote(key), cause)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execSQL := func(label, statement string) error {
		started := time.Now().UTC()
		result, err := db.ExecContext(ctx, statement)
		receipt := map[string]any{"statement": statement, "started_at": started, "finished_at": time.Now().UTC(), "result_present": result != nil, "error_type": fmt.Sprintf("%T", err), "error": err}
		if result != nil {
			// sql.Result wraps a live connection mutex. Record its public
			// returned values, never traverse that unrelated cyclic handle.
			affected, affectedErr := result.RowsAffected()
			insertID, insertErr := result.LastInsertId()
			receipt["rows_affected"], receipt["rows_affected_error"] = affected, affectedErr
			receipt["last_insert_id"], receipt["last_insert_id_error"] = insertID, insertErr
		}
		writeRawRecordingValue(t, path+"."+label+".raw-fields.json", receipt)
		return err
	}
	if err := execSQL("install-trigger", trigger); err != nil {
		t.Fatalf("SQLite fault trigger setup: %v", err)
	}
	beforeSQL := nativeSQLiteImage(t, ctx, dbPath)
	writeRecordingValue(t, path+".before-sql.json", beforeSQL)
	posts := func() []publicPostObservation {
		observer.mu.Lock()
		defer observer.mu.Unlock()
		return append([]publicPostObservation(nil), observer.Observations...)
	}
	beforePosts := posts()
	writeRecordingValue(t, path+".before-posts.json", beforePosts)
	outcome, writeErr := domain.UpdateWithOutcome(ctx, actor, request, meta)
	var typed *model.Error
	errors.As(writeErr, &typed)
	var sqliteError interface{ Code() int }
	hasSQLiteError := errors.As(writeErr, &sqliteError)
	code := 0
	if hasSQLiteError {
		code = sqliteError.Code()
	}
	writeRawRecordingValue(t, path+".return.raw-fields.json", map[string]any{"outcome": outcome, "error_type": fmt.Sprintf("%T", writeErr), "error": writeErr, "typed_error": typed, "sqlite_error_present": hasSQLiteError, "sqlite_code": code})
	after := nativePlugImage(t, ctx, store, profile)
	writeRecordingValue(t, path+".after.json", after)
	afterSQL := nativeSQLiteImage(t, ctx, dbPath)
	writeRecordingValue(t, path+".after-sql.json", afterSQL)
	afterPosts := posts()
	writeRecordingValue(t, path+".after-posts.json", afterPosts)
	type rawCommit model.Commit
	writeRecordingValue(t, path+".operation.json", map[string]any{"surface": "service.UpdateWithOutcome", "request": request, "meta": meta, "commit": rawCommit(outcome.Commit), "replayed": outcome.Replayed, "null_result": outcome.NullResult, "error": fmt.Sprint(writeErr), "typed_error": typed, "sqlite_error_present": hasSQLiteError, "sqlite_code": code, "encoding": "raw-failure-fields-not-public-commit"})
	dropErr := execSQL("remove-trigger", "DROP TRIGGER "+triggerName)
	afterCleanupSQL := nativeSQLiteImage(t, ctx, dbPath)
	writeRecordingValue(t, path+".after-trigger-cleanup-sql.json", afterCleanupSQL)
	return func(reopened driver.Store) {
		t.Helper()
		reopenedImage := nativePlugImage(t, ctx, reopened, profile)
		writeRecordingValue(t, path+".reopened.json", reopenedImage)
		reopenedSQL := nativeSQLiteImage(t, ctx, dbPath)
		writeRecordingValue(t, path+".reopened-sql.json", reopenedSQL)
		// All actual returned fields, typed errors, receipt lookups, full reader
		// and SQL images, and subscriber bodies are saved before these checks.
		if reopened == store || dropErr != nil || writeErr == nil || !hasSQLiteError || code != 1811 || typed != nil || !strings.Contains(writeErr.Error(), cause) || !reflect.DeepEqual(outcome, port.WriteOutcome{}) {
			t.Fatalf("real SQLite staged-publication fault/cleanup/reopen was not proved: write=%v drop=%v", writeErr, dropErr)
		}
		for _, image := range []map[string]any{before, after, reopenedImage} {
			nativePlugImageRows(t, image)
			if image["receipts"].(map[string]any)[receiptKey] != nil {
				t.Fatal("failed SQLite publication stored its idempotency receipt")
			}
		}
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(before, reopenedImage) || !reflect.DeepEqual(beforeSQL, afterSQL) || !reflect.DeepEqual(beforePosts, afterPosts) {
			t.Fatal("SQLite write fault changed a row/history/derived/archive/receipt/SQL/subscriber plane")
		}
		// Cleanup removes exactly our captured trigger. No table, schema or
		// lease difference is hidden behind a family-wide exclusion.
		clean := beforeSQL
		clean.Schema = []nativeSQLiteObject{}
		removed := 0
		for _, object := range beforeSQL.Schema {
			if object.Type == "trigger" && object.Name == triggerName {
				if object.Table != "changesets" || object.SQL != strings.TrimSuffix(trigger, ";") {
					t.Fatal("installed SQLite fault trigger does not match its exact authored SQL")
				}
				removed++
				continue
			}
			clean.Schema = append(clean.Schema, object)
		}
		if removed != 1 || !reflect.DeepEqual(clean, afterCleanupSQL) {
			t.Fatal("SQLite trigger cleanup changed unrelated schema or table contents")
		}
		// The caller's ordinary deferred Release clears only these three
		// columns. Preserve singleton/epoch and every other table exactly.
		clean.Tables = append([]nativeSQLiteTable(nil), clean.Tables...)
		leaseTables := 0
		for i := range clean.Tables {
			table := &clean.Tables[i]
			if table.Name != "active_lease" {
				continue
			}
			leaseTables++
			if !reflect.DeepEqual(table.Columns, []string{"singleton", "epoch", "token", "holder", "expires"}) || len(table.Rows) != 1 || table.Rows[0][0] != int64(1) || table.Rows[0][3] != "s1-native-public-subscriber" {
				t.Fatal("SQLite fault does not have the exact owned active-lease row")
			}
			values := append([]any(nil), table.Rows[0]...)
			if token, ok := values[2].(string); !ok || token == "" {
				t.Fatal("SQLite fault has no captured live lease token")
			}
			values[2], values[3], values[4] = "", "", int64(0)
			table.Rows = [][]any{values}
		}
		if leaseTables != 1 || !reflect.DeepEqual(clean, reopenedSQL) {
			t.Fatal("SQLite reopen changed more than the exact normal lease release")
		}
		t.Logf("PUBLIC_SQLITE_WRITE_FAULT path=%s code=%d cause=%s receipt=absent reopen=conserved", path, code, cause)
	}
}

// This control derives a declared hash-allocator variant of the explicitly
// synthetic archive. The original sequential allocator remains unsupported;
// none of these authored bytes is a native recording or producer evidence.
func authoredSubscriberArchive(t *testing.T, original []byte) []byte {
	t.Helper()
	if hashBytes(original) != "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c" {
		t.Fatal("original synthetic control fixture changed")
	}
	archive, err := dumpformat.Read(bytes.NewReader(original))
	if err != nil {
		t.Fatal(err)
	}
	manifest := archive.Manifest()
	if len(manifest.Projects) != 1 || manifest.Projects[0].IDMode != "sequential" || manifest.Projects[0].HashLength != 0 {
		t.Fatal("original synthetic allocator declaration changed")
	}
	var authoredManifest map[string]json.RawMessage
	if err := json.Unmarshal(archiveMember(t, original, "manifest.json"), &authoredManifest); err != nil {
		t.Fatal(err)
	}
	var projects []map[string]json.RawMessage
	if err := json.Unmarshal(authoredManifest["projects"], &projects); err != nil {
		t.Fatal(err)
	}
	projects[0]["id_mode"], projects[0]["hash_length"] = json.RawMessage(`"hash"`), json.RawMessage(`1`)
	authoredManifest["projects"], err = json.Marshal(projects)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := json.Marshal(authoredManifest)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	reader := tar.NewReader(bytes.NewReader(original))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "manifest.json" {
			data = manifestBytes
		}
		header.Size = int64(len(data))
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dumpformat.Read(bytes.NewReader(output.Bytes())); err != nil {
		t.Fatalf("authored synthetic control validation: %v", err)
	}
	t.Logf("AUTHORED_SUBSCRIBER_INPUT original_sha256=%s authored_sha256=%s only_delta=manifest_allocator_mode_and_width native=false", hashBytes(original), hashBytes(output.Bytes()))
	return output.Bytes()
}

func archiveMember(t *testing.T, data []byte, name string) []byte {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err != nil {
			t.Fatalf("authored fixture member %s: %v", name, err)
		}
		if header.Name == name {
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			return data
		}
	}
}

// Exercise actual restore/service/subscribers on three engines. The manual
// native target selects only TestPublicNativeRecording and excludes this input.
func TestPublicProjectionAuthoredImportPublicSubscriberAcrossDrivers(t *testing.T) {
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = authoredSubscriberArchive(t, data)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			var store driver.Store
			var err error
			sqlitePath := ""
			switch engine {
			case "memory":
				store = memory.New()
			case "sqlite":
				sqlitePath = filepath.Join(t.TempDir(), "control.sqlite")
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: sqlitePath})
			case "postgres":
				store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: testclock.PostgresDSN(t)})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			profile := driver.RestoreImportProfile{
				Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "authored-measurement"}, Authority: "restore-admin", AdmissionDigest: hashBytes([]byte("authored-measurement"))},
				SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: "authored-subscriber-control", ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
			}
			if _, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, profile); err != nil {
				t.Fatal(err)
			}
			output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if output == "" {
				output = t.TempDir()
			}
			verifyFault := recordPublicSubscriber(t, ctx, store, filepath.Join(output, "authored-subscriber-control-"+engine+".json"), []string{"demo-7", "demo-7.4", "demo-8"}, profile, sqlitePath)
			if verifyFault != nil {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: sqlitePath})
				if err != nil {
					t.Fatal(err)
				}
				verifyFault(store)
			}
		})
	}
}

// recordNativeScalarOperations is the card-owned native B25 sequence. It is
// deliberately separate from the canonical source/export command list so the
// operation receipts retain every refusal and auxiliary probe without making a
// fixture list stand in for the actual native run.
func recordNativeScalarOperations(t *testing.T, output, storeRoot, source, pinned string, server publicNativeServer, childEnv []string) {
	t.Helper()
	root := filepath.Join(output, "native-b25-scalar")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	var receipts []commandReceipt
	command := func(label string, argv []string) []byte {
		return runCommand(t, root, &receipts, label, "source", source, childEnv, append([]string{pinned}, argv...), nil)
	}
	expected := func(label string, argv []string, exit int) []byte {
		return runCommandExpected(t, root, &receipts, label, "source", source, childEnv, append([]string{pinned}, argv...), nil, exit)
	}
	show := func(label, id string) map[string]any {
		return decodeNativeIssue(t, command(label, []string{"--json", "show", "--include-comments", id}))
	}
	physicalBefore := recordNativePhysicalImage(t, root, "scalar-before", command)
	currentPhysical := physicalBefore
	scalarWrite := func(label, id, field string, value any, argv []string) {
		command(label, argv)
		after := recordNativePhysicalImage(t, root, label+"-after", command)
		assertNativeScalarPhysical(t, currentPhysical, after)
		assertNativeIssuePatch(t, currentPhysical, after, id, field, value)
		currentPhysical = after
	}
	before := show("b25-scalar-before", "pub-root")
	childBefore := show("b25-child-before", publicChildID)
	if len(nativeComments(before)) == 0 {
		t.Fatal("native B25 before image did not expose full comment bodies")
	}
	scalarWrite("b25-update-title", "pub-root", "title", "Public root title updated natively", []string{"--json", "update", "pub-root", "--title", "Public root title updated natively"})
	scalarWrite("b25-update-priority", "pub-root", "priority", 3, []string{"--json", "update", "pub-root", "--priority", "3"})
	scalarWrite("b25-update-description", "pub-root", "description", "Public root description updated natively", []string{"--json", "update", "pub-root", "--description", "Public root description updated natively"})
	scalarWrite("b25-update-assignee", "pub-root", "assignee", "native-owner", []string{"--json", "update", "pub-root", "--assignee", "native-owner"})
	scalarWrite("b25-update-notes", "pub-root", "notes", "native notes retained", []string{"--json", "update", "pub-root", "--notes", "native notes retained", "--force"})
	scalarWrite("b25-update-child-title", "pub-root.1", "title", "Public child title updated natively", []string{"--json", "update", publicChildID, "--title", "Public child title updated natively"})
	childAfter := show("b25-child-after", publicChildID)
	closedBefore := show("b25-closed-before", "pub-dependency")
	scalarWrite("b25-update-closed-title", "pub-dependency", "title", "Closed dependency updated natively", []string{"--json", "update", "pub-dependency", "--title", "Closed dependency updated natively"})
	closedAfter := show("b25-closed-after", "pub-dependency")
	after := show("b25-scalar-after", "pub-root")
	assertNativeScalarPreservation(t, before, after, map[string]any{
		"title": "Public root title updated natively", "description": "Public root description updated natively",
		"assignee": "native-owner", "notes": "native notes retained", "priority": float64(3),
	})
	assertNativeScalarPreservation(t, childBefore, childAfter, map[string]any{"title": "Public child title updated natively"})
	assertNativeScalarPreservation(t, closedBefore, closedAfter, map[string]any{"title": "Closed dependency updated natively"})
	if closedAfter["status"] != "closed" || closedAfter["close_reason"] != "dependency complete" {
		t.Fatalf("closed-row native scalar update lost close provenance: %s", canonical(closedAfter))
	}
	physicalAfter := currentPhysical
	assertNativeScalarPhysical(t, physicalBefore, physicalAfter)
	refusalBefore := show("b25-refusal-before", "pub-root")
	expected("b25-refused-self-dependency", []string{"--json", "dep", "add", "pub-root", "pub-root"}, 1)
	refusalAfter := show("b25-refusal-after", "pub-root")
	if !equalJSON(nativePreservationImage(refusalBefore), nativePreservationImage(refusalAfter)) {
		t.Fatal("native refused dependency mutation changed the root image")
	}
	refusalPhysical := recordNativePhysicalImage(t, root, "refusal-after", command)
	if !equalJSON(physicalAfter, refusalPhysical) {
		t.Fatal("native dependency refusal changed a physical source/history table")
	}
	expected("b25-refused-scalar-and-type", []string{"--json", "update", "pub-root", "--title", "must not land", "--type", "not-a-declared-type"}, 1)
	typeRefusal := recordNativePhysicalImage(t, root, "type-refusal-after", command)
	if !equalJSON(refusalPhysical, typeRefusal) {
		t.Fatal("native scalar/type refusal wrote a partial patch")
	}
	// Accepted c8 labels.go rejects this before INSERT; it is a length
	// validation refusal, not evidence of an auxiliary SQL write fault.
	expected("b25-label-length-refusal", []string{"--json", "update", "pub-root", "--title", "must roll back", "--add-label", strings.Repeat("x", 256)}, 1)
	labelError, err := os.ReadFile(receipts[len(receipts)-1].StderrPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(labelError), []byte("\n"))
	var failure struct {
		Error  string `json:"error"`
		Failed []struct {
			ID    string `json:"id"`
			Error string `json:"error"`
		} `json:"failed"`
	}
	if err := json.Unmarshal(lines[len(lines)-1], &failure); err != nil || failure.Error != "1 of 1 issues failed to update" || len(failure.Failed) != 1 || failure.Failed[0].ID != "pub-root" || !strings.HasPrefix(failure.Failed[0].Error, "updating issue: ") || !strings.HasSuffix(failure.Failed[0].Error, "field exceeds maximum length: label is 256 characters (max 255)") {
		t.Fatalf("native label length refusal had another cause: %s", labelError)
	}
	faultAfter := recordNativePhysicalImage(t, root, "label-length-refusal-after", command)
	if !equalJSON(typeRefusal, faultAfter) {
		t.Fatal("native label length refusal changed physical source/history state")
	}
	// Exercise the real child counter on the accepted native store. This row is
	// retained in the source archive and its generated identity is checked here,
	// rather than inferred from a fixture or a Plug allocator.
	counter := command("b27-counter-child", []string{"--json", "create", "--type", "bug", "--title", "Native counter child", "--description", "counter edge", "--parent", "pub-root"})
	var counterRow map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(counter), &counterRow); err != nil || fmt.Sprint(counterRow["id"]) != "pub-root.2" {
		t.Fatalf("native child counter did not allocate pub-root.2: %s", counter)
	}
	data, err := json.MarshalIndent(map[string]any{
		"schema": "plug-s1-b25-native-operations/1", "source": source,
		"server": server, "before": before, "after": after, "child_before": childBefore,
		"child_after": childAfter, "closed_before": closedBefore, "closed_after": closedAfter, "refusal_before": refusalBefore,
		"refusal_after": refusalAfter, "counter_child": counterRow,
		"full_comment_bodies": nativeComments(after),
		"physical_before":     physicalBefore, "physical_after": physicalAfter, "refusal_physical": refusalPhysical, "type_refusal_physical": typeRefusal, "label_length_refusal_after": faultAfter,
		"auxiliary_sql_write_fault": "NOT_PROVEN",
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(root, "operations.json"), append(data, '\n'))
	writeCommandReceipts(t, root, receipts)
	_ = storeRoot // retained in the caller's owned physical-store root
}

// recordNativeDeleteOperations covers the native delete policy after the
// source archive snapshot has been captured. It records dry-run, orphan,
// cascade, missing-row, and refusal outcomes with complete before/after rows.
func recordNativeDeleteOperations(t *testing.T, output, storeRoot, source, pinned string, server publicNativeServer, childEnv []string) {
	t.Helper()
	root := filepath.Join(output, "native-b25-delete")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	var receipts []commandReceipt
	command := func(label string, argv []string) []byte {
		return runCommand(t, root, &receipts, label, "source", source, childEnv, append([]string{pinned}, argv...), nil)
	}
	expected := func(label string, argv []string, exit int) []byte {
		return runCommandExpected(t, root, &receipts, label, "source", source, childEnv, append([]string{pinned}, argv...), nil, exit)
	}
	show := func(label, id string) map[string]any {
		return decodeNativeIssue(t, command(label, []string{"--json", "show", "--include-comments", id}))
	}
	command("b25-delete-create-owner", []string{"--json", "create", "--id", "pub-delete-owner", "--type", "task", "--title", "Native delete owner", "--description", "orphan owner"})
	command("b25-delete-create-survivor", []string{"--json", "create", "--id", "pub-delete-survivor", "--type", "task", "--title", "Native delete survivor", "--description", "orphan survivor"})
	command("b25-delete-link", []string{"--json", "dep", "add", "pub-delete-survivor", "pub-delete-owner"})
	command("b25-delete-owner-label", []string{"--json", "label", "add", "pub-delete-owner", "owned-label"})
	command("b25-delete-owner-comment", []string{"--json", "comment", "pub-delete-owner", "owned full comment"})
	command("b25-delete-survivor-comment", []string{"--json", "comment", "pub-delete-survivor", "surviving full comment"})
	orphanBefore := recordNativePhysicalImage(t, root, "orphan-before", command)
	ownerBefore := show("b25-delete-owner-before", "pub-delete-owner")
	survivorBefore := show("b25-delete-survivor-before", "pub-delete-survivor")
	expected("b25-delete-dry-run", []string{"--json", "delete", "pub-delete-owner"}, 1)
	ownerPreview := show("b25-delete-owner-after-dry-run", "pub-delete-owner")
	if !equalJSON(nativePreservationImage(ownerBefore), nativePreservationImage(ownerPreview)) {
		t.Fatal("native unforced delete preview changed the owner")
	}
	previewPhysical := recordNativePhysicalImage(t, root, "preview-after", command)
	if !equalJSON(orphanBefore, previewPhysical) {
		t.Fatal("native default delete refusal changed physical source state")
	}
	command("b25-delete-force-orphan", []string{"--json", "delete", "pub-delete-owner", "--force"})
	expected("b25-delete-owner-missing-after-force", []string{"--json", "show", "--include-comments", "pub-delete-owner"}, 1)
	survivorAfter := show("b25-delete-survivor-after-orphan", "pub-delete-survivor")
	if strings.Contains(canonical(survivorAfter["dependencies"]), "pub-delete-owner") {
		t.Fatal("native orphan delete left a dangling dependency")
	}
	orphanAfter := recordNativePhysicalImage(t, root, "orphan-after", command)
	assertNativeDeletePhysical(t, orphanBefore, orphanAfter, []string{"pub-delete-owner"}, []string{"pub-delete-survivor"})
	command("b25-cascade-create-owner", []string{"--json", "create", "--id", "pub-cascade-owner", "--type", "task", "--title", "Native cascade owner", "--description", "cascade owner"})
	command("b25-cascade-create-child", []string{"--json", "create", "--id", "pub-cascade-child", "--type", "task", "--title", "Native cascade child", "--description", "cascade child"})
	command("b25-cascade-link", []string{"--json", "dep", "add", "pub-cascade-child", "pub-cascade-owner"})
	command("b25-cascade-owner-comment", []string{"--json", "comment", "pub-cascade-owner", "cascade owned full comment"})
	command("b25-cascade-child-label", []string{"--json", "label", "add", "pub-cascade-child", "cascade-label"})
	cascadePhysicalBefore := recordNativePhysicalImage(t, root, "cascade-before", command)
	cascadeBefore := show("b25-cascade-owner-before", "pub-cascade-owner")
	command("b25-delete-cascade-force", []string{"--json", "delete", "pub-cascade-owner", "--cascade", "--force"})
	expected("b25-cascade-owner-missing", []string{"--json", "show", "--include-comments", "pub-cascade-owner"}, 1)
	expected("b25-cascade-child-missing", []string{"--json", "show", "--include-comments", "pub-cascade-child"}, 1)
	cascadePhysicalAfter := recordNativePhysicalImage(t, root, "cascade-after", command)
	assertNativeDeletePhysical(t, cascadePhysicalBefore, cascadePhysicalAfter, []string{"pub-cascade-owner", "pub-cascade-child"}, nil)
	expected("b25-delete-missing-row", []string{"--json", "delete", "pub-never-created", "--force"}, 1)
	data, err := json.MarshalIndent(map[string]any{
		"schema": "plug-s1-b25-native-delete/1", "source": source, "server": server,
		"owner_before": ownerBefore, "owner_after_dry_run": ownerPreview,
		"survivor_before": survivorBefore, "survivor_after_orphan": survivorAfter,
		"cascade_owner_before": cascadeBefore,
		"orphan_before":        orphanBefore, "orphan_after": orphanAfter, "cascade_before": cascadePhysicalBefore, "cascade_after": cascadePhysicalAfter,
		"policy": map[string]string{"default": "dry-run", "force": "orphan", "cascade_force": "cascade"},
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(root, "operations.json"), append(data, '\n'))
	writeCommandReceipts(t, root, receipts)
	_ = storeRoot
}

// recordNativeB27Probes executes two source-specific branches that the main
// archive intentionally cannot represent: missing-target import reporting and
// stored custom-status resolution. The raw importer result is retained before
// any Plug replay is attempted.
func recordNativeB27Probes(t *testing.T, output, storeRoot, pinned string, server publicNativeServer, baseEnv func(string) []string) {
	t.Helper()
	root := filepath.Join(output, "native-b27-probes")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(storeRoot, "b27-probe")
	if err := os.MkdirAll(filepath.Join(workspace, "home", ".config"), 0o700); err != nil {
		t.Fatal(err)
	}
	env := server.provider.ChildEnv(baseEnv(workspace))
	var receipts []commandReceipt
	command := func(label string, argv []string, stdin []byte) []byte {
		return runCommand(t, root, &receipts, label, "b27-probe", workspace, env, append([]string{pinned}, argv...), stdin)
	}
	runCommand(t, root, &receipts, "b27-probe-git-init", "b27-probe", workspace, env, []string{"git", "-c", "init.defaultBranch=main", "init", "--quiet"}, nil)
	command("b27-probe-init", nativeInitArgs(workspace, server.Port), nil)
	recordNativeDatabaseBinding(t, root, "b27-probe", workspace, server)
	command("b27-keep-create", []string{"--json", "create", "--id", "pub-importkeep", "--title", "Keep before missing-target import", "--type", "task"}, nil)
	keepBefore := decodeNativeIssue(t, command("b27-keep-before", []string{"--json", "show", "--include-comments", "pub-importkeep"}, nil))
	missing := []byte(`{"_type":"issue","id":"pub-missing","title":"Missing target import","description":"native skipped dependency","issue_type":"task","priority":1,"status":"open","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","dependencies":[{"issue_id":"pub-missing","depends_on_id":"pub-no-such-target","type":"blocks","created_at":"2026-01-01T00:00:00Z"}]}` + "\n")
	missingPath := filepath.Join(root, "missing-target.jsonl")
	writeRecordingBytes(t, missingPath, missing)
	imported := command("b27-missing-target-import", []string{"--json", "import", missingPath}, nil)
	var importResult map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(imported), &importResult); err != nil {
		t.Fatalf("native missing-target import did not return JSON: %v: %s", err, imported)
	}
	if !equalJSON(importResult["skipped_dependencies"], []string{"pub-missing -> pub-no-such-target: target not found"}) || !equalJSON(importResult["ids"], []string{"pub-missing"}) {
		t.Fatalf("native importer omitted skipped dependency report: %s", imported)
	}
	missingRow := decodeNativeIssue(t, command("b27-missing-target-show", []string{"--json", "show", "--include-comments", "pub-missing"}, nil))
	if missingRow["title"] != "Missing target import" || len(nativeRelationEdges(missingRow["dependencies"])) != 0 {
		t.Fatalf("native missing-target import failed to preserve the issue and omit only the unresolved edge: %s", canonical(missingRow))
	}
	keepAfter := decodeNativeIssue(t, command("b27-keep-after", []string{"--json", "show", "--include-comments", "pub-importkeep"}, nil))
	if !equalJSON(keepBefore, keepAfter) {
		t.Fatal("missing-target import changed its pre-existing survivor")
	}
	command("b27-status-config-set", []string{"--json", "config", "set", "status.custom", "reviewing:active"}, nil)
	statusList := command("b27-status-config-list", []string{"--json", "config", "list"}, nil)
	if !bytes.Contains(statusList, []byte("status.custom")) {
		t.Fatalf("native status registry did not expose stored custom status: %s", statusList)
	}
	command("b27-status-create", []string{"--json", "create", "--id", "pub-statusrow", "--type", "task", "--status", "reviewing", "--title", "Stored custom status", "--description", "status registry probe"}, nil)
	statusRow := command("b27-status-show", []string{"--json", "show", "pub-statusrow"}, nil)
	if row := decodeNativeIssue(t, statusRow); row["id"] != "pub-statusrow" || row["status"] != "reviewing" || row["issue_type"] != "task" {
		t.Fatalf("native stored custom status did not survive create/show: %s", statusRow)
	}
	physicalCommand := func(label string, argv []string) []byte { return command(label, argv, nil) }
	command("b27-config-commit", []string{"dolt", "commit"}, nil)
	beforeRefusal := recordNativePhysicalImage(t, root, "custom-status-before-refusal", physicalCommand)
	if len(beforeRefusal["custom_statuses"]) != 1 {
		t.Fatal("status.custom setup did not materialize its normalized source row")
	}
	sourceConfigBefore := map[string][]byte{}
	for _, name := range []string{"config.yaml", "metadata.json"} {
		data, err := os.ReadFile(filepath.Join(workspace, ".beads", name))
		if err != nil {
			t.Fatal(err)
		}
		sourceConfigBefore[name] = data
		writeRecordingBytes(t, filepath.Join(root, "source-before-"+name), data)
	}
	inventoryPath := filepath.Join(root, "custom-status-inventory.json")
	command("b27-custom-inventory", []string{"--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", inventoryPath}, nil)
	archivePath := filepath.Join(root, "refused-custom-status.tar")
	refusalStdout := runCommandMode(t, root, &receipts, "b27-custom-status-archive-refusal", "b27-probe", workspace, env, []string{pinned, "--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", inventoryPath, "-o", archivePath}, nil, false)
	archiveRefusal := receipts[len(receipts)-1]
	refusalStderr, err := os.ReadFile(archiveRefusal.StderrPath)
	if err != nil {
		t.Fatal(err)
	}
	afterRefusal := recordNativePhysicalImage(t, root, "custom-status-after-refusal", physicalCommand)
	sourceConfigAfter := map[string][]byte{}
	for _, name := range []string{"config.yaml", "metadata.json"} {
		data, err := os.ReadFile(filepath.Join(workspace, ".beads", name))
		if err != nil {
			t.Fatal(err)
		}
		sourceConfigAfter[name] = data
		writeRecordingBytes(t, filepath.Join(root, "source-"+name), data)
	}
	if archiveRefusal.Exit != 1 || len(refusalStdout) != 0 || string(refusalStderr) != "Error: plug-dump/1 refused: unsupported nonempty family custom_statuses (live claims must be resolved)\n" {
		t.Fatalf("native custom status refusal cause changed: exit=%d stdout=%s stderr=%s", archiveRefusal.Exit, refusalStdout, refusalStderr)
	}
	if _, err := os.Stat(archivePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported custom status emitted an archive: %v", err)
	}
	if !equalJSON(beforeRefusal, afterRefusal) || !equalJSON(sourceConfigBefore, sourceConfigAfter) {
		t.Fatal("refused native capture changed physical source/config/history state")
	}
	data, err := json.MarshalIndent(map[string]any{
		"schema": "plug-s1-b27-native-probes/2", "workspace": workspace, "server": server,
		"missing_target_result": importResult, "status_config": json.RawMessage(statusList),
		"status_row":  json.RawMessage(statusRow),
		"missing_row": missingRow, "keep_before": keepBefore, "keep_after": keepAfter,
		"physical_before_refusal": beforeRefusal, "physical_after_refusal": afterRefusal, "archive": "ABSENT_EXPECTED_REFUSAL",
		"refusal_receipt": archiveRefusal, "refusal_stderr": string(refusalStderr), "source_config_before": sourceConfigBefore, "source_config_after": sourceConfigAfter,
		"capability_boundary": "custom_statuses_and_custom_types_are_not_claimed_in_the_c8-native archive",
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(root, "probes.json"), append(data, '\n'))
	writeCommandReceipts(t, root, receipts)
}

func decodeNativeIssue(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var value any
	if err := json.Unmarshal(bytes.TrimSpace(data), &value); err != nil {
		t.Fatalf("native issue JSON is invalid: %v: %s", err, data)
	}
	if object, ok := value.(map[string]any); ok {
		if issue, ok := object["issue"].(map[string]any); ok {
			return issue
		}
		if issues, ok := object["issues"].([]any); ok && len(issues) == 1 {
			if issue, ok := issues[0].(map[string]any); ok {
				return issue
			}
		}
		return object
	}
	if issues, ok := value.([]any); ok && len(issues) == 1 {
		if issue, ok := issues[0].(map[string]any); ok {
			return issue
		}
	}
	t.Fatalf("native show response is not an issue object: %s", data)
	return nil
}

func nativeComments(issue map[string]any) []any {
	comments, _ := issue["comments"].([]any)
	return comments
}

func nativePreservationImage(issue map[string]any) map[string]any {
	image := map[string]any{}
	for _, key := range []string{"labels", "dependencies", "dependents", "metadata", "comments", "parent", "comment_count", "close_reason", "status"} {
		if value, ok := issue[key]; ok {
			if key == "dependencies" || key == "dependents" {
				value = nativeRelationEdges(value)
			}
			image[key] = value
		}
	}
	return image
}

// Beads show embeds the complete endpoint issue in dependency arrays. Scalar
// updates therefore change the embedded endpoint image even when the edge is
// untouched. Preserve the native raw rows in operations.json, but compare the
// stable edge endpoint and kind here.
func nativeRelationEdges(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		edge := map[string]any{}
		for _, key := range []string{"id", "issue_id", "depends_on_id", "depends_on_external", "dependency_type", "type"} {
			if field, present := object[key]; present {
				edge[key] = field
			}
		}
		if len(edge) == 0 {
			edge = object
		}
		result = append(result, edge)
	}
	sort.SliceStable(result, func(i, j int) bool { return canonical(result[i]) < canonical(result[j]) })
	return result
}

func assertNativeScalarPreservation(t *testing.T, before, after map[string]any, expected map[string]any) {
	t.Helper()
	if len(nativeComments(before)) != len(nativeComments(after)) {
		t.Fatalf("native scalar readback changed comment multiplicity: before=%d after=%d", len(nativeComments(before)), len(nativeComments(after)))
	}
	for _, key := range []string{"labels", "dependencies", "dependents", "metadata", "comments", "parent", "comment_count", "close_reason", "status"} {
		beforeValue, afterValue := before[key], after[key]
		if key == "dependencies" || key == "dependents" {
			beforeValue, afterValue = nativeRelationEdges(beforeValue), nativeRelationEdges(afterValue)
		}
		if !equalJSON(beforeValue, afterValue) {
			t.Fatalf("native scalar update changed %s: before=%s after=%s", key, canonical(before[key]), canonical(after[key]))
		}
	}
	for key, want := range expected {
		if !equalJSON(after[key], want) {
			t.Fatalf("native scalar field %s=%s want=%s", key, canonical(after[key]), canonical(want))
		}
	}
}

func nativeHasNonEmptyField(value any, field string) bool {
	switch object := value.(type) {
	case map[string]any:
		if fieldValue, ok := object[field]; ok {
			switch typed := fieldValue.(type) {
			case []any:
				return len(typed) > 0
			case string:
				return typed != ""
			case nil:
				return false
			default:
				return true
			}
		}
		for _, nested := range object {
			if nativeHasNonEmptyField(nested, field) {
				return true
			}
		}
	case []any:
		for _, nested := range object {
			if nativeHasNonEmptyField(nested, field) {
				return true
			}
		}
	}
	return false
}

// The workflow supplies this checkout. Read the immutable commit object rather
// than %P, which suppresses parents at the workflow's shallow boundary. The
// dispatch seal independently compares this observed tuple to its input SHA.
func recordNativeProductIdentity(t *testing.T, output string) map[string]any {
	t.Helper()
	dir := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(dir) {
		t.Fatal("RunsOn did not supply an absolute product checkout")
	}
	root := filepath.Join(output, "product-git")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	var receipts []commandReceipt
	env := []string{"PATH=/usr/bin:/bin", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	head := strings.TrimSpace(string(runCommand(t, root, &receipts, "product-head", "product", dir, env, []string{"git", "rev-parse", "HEAD"}, nil)))
	object := runCommand(t, root, &receipts, "product-object", "product", dir, env, []string{"git", "cat-file", "-p", head}, nil)
	var tree string
	parents := []string{}
	for _, line := range strings.Split(string(object), "\n") {
		if line == "" {
			break
		}
		if strings.HasPrefix(line, "tree ") {
			tree = strings.TrimPrefix(line, "tree ")
		}
		if strings.HasPrefix(line, "parent ") {
			parents = append(parents, strings.TrimPrefix(line, "parent "))
		}
	}
	if len(head) != 40 || !isLowerHex(head) || len(tree) != 40 || !isLowerHex(tree) || len(parents) == 0 {
		t.Fatal("actual product commit object has an incomplete identity")
	}
	result := map[string]any{"commit": head, "tree": tree, "ordered_parents": parents, "raw_commit_sha256": hashBytes(object)}
	writeRecordingBytes(t, filepath.Join(output, "actual-product-source.json"), []byte(canonical(result)+"\n"))
	return result
}

// Default native JSON must remain raw evidence. This decoder rejects duplicate
// fields at every depth and trailing values, without converting the protocol to
// an envelope or changing the existing strict envelope-parser controls.
func readNativeJSON(data []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var read func() (any, error)
	read = func() (any, error) {
		token, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch token {
		case json.Delim('{'):
			object := map[string]any{}
			for d.More() {
				keyToken, err := d.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if _, exists := object[key]; !ok || exists {
					return nil, errors.New("duplicate native JSON field")
				}
				value, err := read()
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return nil, errors.New("unclosed native object")
			}
			return object, nil
		case json.Delim('['):
			array := []any{}
			for d.More() {
				value, err := read()
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return nil, errors.New("unclosed native array")
			}
			return array, nil
		default:
			if _, ok := token.(json.Delim); ok {
				return nil, errors.New("unexpected native delimiter")
			}
			return token, nil
		}
	}
	value, err := read()
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("native JSON has trailing data")
	}
	return value, nil
}

func readNativeKVRows(data []byte, table string) ([]publicConfigObservation, error) {
	value, err := readNativeJSON(data)
	if err != nil {
		return nil, err
	}
	rows, ok := value.([]any)
	if !ok {
		return nil, errors.New("native SQL readback must be an array")
	}
	result := []publicConfigObservation{}
	seen := map[string]bool{}
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok || len(row) != 2 {
			return nil, errors.New("native SQL row must have key and value only")
		}
		key, keyOK := row["key"].(string)
		value, valueOK := row["value"].(string)
		if !keyOK || !valueOK || key == "" || seen[key] {
			return nil, errors.New("invalid or duplicate native SQL key/value")
		}
		seen[key] = true
		result = append(result, publicConfigObservation{Table: table, Key: key, Value: value, Classification: "public"})
	}
	return result, nil
}

func readNativeConfigSettings(list, sqlRows []byte) ([]publicConfigObservation, error) {
	rows, err := readNativeKVRows(sqlRows, "config")
	if err != nil {
		return nil, err
	}
	value, err := readNativeJSON(list)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("default native config list must be an object")
	}
	version, ok := object["schema_version"].(json.Number)
	if !ok || version != "1" {
		return nil, errors.New("default native config schema is not 1")
	}
	want := map[string]any{"schema_version": json.Number("1")}
	settings := []publicConfigObservation{}
	for _, row := range rows {
		if strings.HasPrefix(row.Key, "kv.") {
			continue
		} // CLI config-list's explicit settings plane.
		settings = append(settings, row)
		if row.Key != "schema_version" {
			want[row.Key] = row.Value
		}
	}
	if !equalJSON(want, object) {
		return nil, errors.New("native config list and independent SQL settings disagree")
	}
	return settings, nil
}

func observeNativeConfig(t *testing.T, output, label string, list, sqlRows, metadata, ordinary []byte) publicConfigExpectation {
	t.Helper()
	observations, err := readNativeConfigSettings(list, sqlRows)
	if err != nil {
		t.Fatalf("%s default config readback: %v", label, err)
	}
	metaRows, err := readNativeKVRows(metadata, "metadata")
	if err != nil {
		t.Fatalf("%s default metadata readback: %v", label, err)
	}
	expected, err := expectPublicConfig(append(observations, metaRows...), ordinary)
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(output, label+"-config-expectation.json"), []byte(canonical(expected)+"\n"))
	return expected
}

// Accepted c8 catalog.go's complete physical table census, including all
// columns via SELECT *. Empty families remain observed emptiness only.
var nativePhysicalTables = strings.Fields("schema_migrations ignored_schema_migrations issues wisps dependencies wisp_dependencies labels wisp_labels comments wisp_comments events wisp_events config metadata local_metadata child_counters wisp_child_counters issue_counter issue_snapshots compaction_snapshots repo_mtimes routes interactions federation_peers custom_statuses custom_types provenance_events bd_events_journal bd_events_seq issue_versions store_epoch epoch_minted_addresses leases")

type nativePhysicalImage map[string][]any

func recordNativePhysicalImage(t *testing.T, output, label string, command func(string, []string) []byte) nativePhysicalImage {
	t.Helper()
	image := nativePhysicalImage{}
	for _, table := range nativePhysicalTables {
		raw := command(label+"-"+table, []string{"--readonly", "--json", "sql", "SELECT * FROM `" + table + "`"})
		value, err := readNativeJSON(raw)
		if err != nil {
			t.Fatalf("%s physical %s: %v", label, table, err)
		}
		rows, ok := value.([]any)
		if !ok {
			t.Fatalf("%s physical %s is not an array", label, table)
		}
		sort.SliceStable(rows, func(i, j int) bool { return canonical(rows[i]) < canonical(rows[j]) })
		image[table] = rows
	}
	writeRecordingBytes(t, filepath.Join(output, label+"-physical.json"), []byte(canonical(image)+"\n"))
	return image
}

func assertNativeScalarPhysical(t *testing.T, before, after nativePhysicalImage) {
	t.Helper()
	for _, table := range nativePhysicalTables {
		switch table {
		case "issues", "bd_events_seq": // Changed scalars and the journal cursor are observed in full.
			continue
		case "events", "bd_events_journal", "issue_versions", "provenance_events":
			remaining := map[string]int{}
			for _, row := range after[table] {
				remaining[canonical(row)]++
			}
			for _, row := range before[table] {
				key := canonical(row)
				if remaining[key] == 0 {
					t.Fatalf("native scalar update lost or rewrote prior %s row: %s", table, key)
				}
				remaining[key]--
			}
		default:
			if !equalJSON(before[table], after[table]) {
				t.Fatalf("native scalar update changed unpatched physical table %s", table)
			}
		}
	}
}

func assertNativeIssuePatch(t *testing.T, before, after nativePhysicalImage, id, field string, value any) {
	t.Helper()
	index := func(rows []any) map[string]map[string]any {
		result := map[string]map[string]any{}
		for _, raw := range rows {
			row, ok := raw.(map[string]any)
			if !ok {
				t.Fatal("native issue SQL row is not an object")
			}
			key, ok := row["id"].(string)
			if !ok || result[key] != nil {
				t.Fatal("invalid or duplicate native issue SQL identity")
			}
			result[key] = row
		}
		return result
	}
	a, b := index(before["issues"]), index(after["issues"])
	if len(a) != len(b) || a[id] == nil || b[id] == nil {
		t.Fatal("scalar update changed the native issue census")
	}
	for key, row := range a {
		actual := b[key]
		if len(row) != len(actual) {
			t.Fatal("scalar update changed native issue column census")
		}
		for column, old := range row {
			if key == id {
				if column == field {
					if !equalJSON(actual[column], value) {
						t.Fatalf("native %s patch %s did not land", id, field)
					}
					continue
				}
				if column == "row_lock" {
					// Accepted c8 update.go:491-497 remints this optimistic token
					// on every update; it is not an unpatched business scalar.
					number, ok := actual[column].(json.Number)
					if !ok {
						t.Fatal("native row_lock lost its integer representation")
					}
					n, err := number.Int64()
					if err != nil || n == 0 || equalJSON(old, number) {
						t.Fatal("native update did not remint a nonzero row_lock")
					}
					continue
				}
				if column == "updated_at" || column == "content_hash" || column == "current_revision" {
					continue
				}
			}
			if !equalJSON(old, actual[column]) {
				t.Fatalf("native scalar %s patch changed unpatched %s.%s", field, key, column)
			}
		}
	}
}

func assertNativeDeletePhysical(t *testing.T, before, after nativePhysicalImage, removed, unblocked []string) {
	t.Helper()
	deleted := map[string]bool{}
	for _, id := range removed {
		deleted[id] = true
	}
	// Accepted c8 DeleteResolvedSetInTx recomputes the derived marker after
	// removing inbound blocking edges. This fixture has exactly one such
	// survivor; its token, timestamps and every other column must survive.
	unblock := map[string]bool{}
	for _, id := range unblocked {
		unblock[id] = true
	}
	seenUnblocked := map[string]bool{}
	for _, table := range nativePhysicalTables {
		// Retain these complete raw histories/cursors; their deletion-specific
		// version/address transition is not asserted to equal scalar history.
		switch table {
		case "issue_versions", "bd_events_journal", "bd_events_seq", "epoch_minted_addresses":
			continue
		}
		want := []any{}
		for _, raw := range before[table] {
			row, ok := raw.(map[string]any)
			if !ok {
				t.Fatal("physical SQL row is not an object")
			}
			owned := false
			switch table {
			case "issues":
				owned = deleted[fmt.Sprint(row["id"])]
				if id := fmt.Sprint(row["id"]); unblock[id] {
					if owned || !equalJSON(row["is_blocked"], json.Number("1")) {
						t.Fatal("declared native orphan survivor was not blocked")
					}
					copy := map[string]any{}
					for key, value := range row {
						copy[key] = value
					}
					copy["is_blocked"] = json.Number("0")
					raw = copy
					seenUnblocked[id] = true
				}
			case "child_counters":
				owned = deleted[fmt.Sprint(row["parent_id"])]
			case "dependencies":
				for _, field := range []string{"issue_id", "depends_on_issue_id", "depends_on_wisp_id", "depends_on_external"} {
					owned = owned || deleted[fmt.Sprint(row[field])]
				}
			case "labels", "comments", "events", "issue_snapshots", "compaction_snapshots", "provenance_events", "leases":
				owned = deleted[fmt.Sprint(row["issue_id"])]
			}
			if !owned {
				want = append(want, raw)
			}
		}
		sort.SliceStable(want, func(i, j int) bool { return canonical(want[i]) < canonical(want[j]) })
		if !equalJSON(want, after[table]) {
			t.Fatalf("native delete did not conserve survivors and remove only owned rows in %s", table)
		}
	}
	if !equalJSON(unblock, seenUnblocked) {
		t.Fatal("declared native unblocked survivor was absent")
	}
}

func TestPublicProjectionDefaultNativeProtocol(t *testing.T) {
	// The real CLI overwrites a stored schema_version key. SQL retains it.
	list := []byte(`{"schema_version":1,"custom.public_recording":"paired-public"}`)
	rows := []byte(`[{"key":"schema_version","value":"stored-value"},{"key":"custom.public_recording","value":"paired-public"},{"key":"kv.memory","value":"separate-plane"}]`)
	got, err := readNativeConfigSettings(list, rows)
	if err != nil || len(got) != 2 || got[0].Value != "stored-value" {
		t.Fatalf("schema collision lost source setting: %+v %v", got, err)
	}
	for _, bad := range []string{`{"schema_version":1,"schema_version":1}`, `{"schema_version":1} {}`, `{"schema_version":"1"}`, `{"schema_version":1,"custom.public_recording":"substituted"}`} {
		if _, err := readNativeConfigSettings([]byte(bad), rows); err == nil {
			t.Fatalf("ambiguous or substituted native config accepted: %s", bad)
		}
	}
	for _, bad := range []string{`null`, `[{"key":"x","key":"y","value":"v"}]`, `[{"key":"x","value":null}]`, `[{"key":"x","value":"v"},{"key":"x","value":"v"}]`} {
		if _, err := readNativeKVRows([]byte(bad), "metadata"); err == nil {
			t.Fatalf("ambiguous native SQL accepted: %s", bad)
		}
	}
}

func validateInventoryAndArchive(t *testing.T, run *publicRun, commandReference func(string, string, string, []string, []byte) []byte, label, store, root string, fixture recordingFixture) {
	t.Helper()
	path := filepath.Join(root, label+"-inventory.json")
	commandReference(label+"-inventory", label, store, []string{"--readonly", "export", "--format", "beads-inventory/1", "--plug-capture-public", "-o", path}, nil)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s inventory was not published: %v", label, err)
	}
	if err := validateInventory(data, fixture); err != nil {
		t.Fatalf("%s inventory is not a truthful complete public census: %v", label, err)
	}
	if _, err := os.Stat(path + ".sources"); err != nil {
		t.Fatalf("%s inventory sidecars were not retained: %v", label, err)
	}
	archivePath := filepath.Join(root, label+"-plug-dump.tar")
	commandReference(label+"-dump", label, store, []string{"--readonly", "export", "--format", "plug-dump/1", "--plug-inventory", path, "-o", archivePath}, nil)
	archive, err := dumpformat.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("%s raw plug-dump is invalid: %v", label, err)
	}
	manifest := archive.Manifest()
	if manifest.Format != dumpformat.Format || manifest.Schema != 1 || manifest.Reference != dumpformat.BeadsReference || manifest.Scope != "offline-head-and-source-archives" || len(manifest.Projects) != 1 || len(archive.Records("bead")) < 3 {
		t.Fatalf("%s archive is not the pinned public head-state format: %+v", label, manifest)
	}
	if label == "source" {
		run.SourceInventoryPath, run.SourceArchivePath = path, archivePath
		run.SourceArchiveSHA256 = archive.SHA256()
	} else {
		run.TargetInventoryPath, run.TargetArchivePath = path, archivePath
		run.TargetArchiveSHA256 = archive.SHA256()
	}
}

func validateInventory(data []byte, fixture recordingFixture) error {
	var inventory struct {
		Version         string                           `json:"version"`
		Quiesced        bool                             `json:"quiesced"`
		RemotesMerged   bool                             `json:"remotes_merged"`
		WorkingSetClean bool                             `json:"working_set_clean"`
		History         string                           `json:"history"`
		External        []struct{ Family, State string } `json:"external"`
		ConfigPolicy    map[string]string                `json:"config_policy"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		return err
	}
	if inventory.Version != "beads-inventory/1" || !inventory.Quiesced || !inventory.RemotesMerged || !inventory.WorkingSetClean || (inventory.History != "unavailable" && inventory.History != "available-unverified") {
		return fmt.Errorf("invalid capture declarations: %+v", inventory)
	}
	expected := map[string]bool{}
	for _, family := range fixture.ExternalFamilies {
		expected[family] = true
	}
	if len(inventory.External) != len(expected) {
		return fmt.Errorf("external family count=%d want=%d", len(inventory.External), len(expected))
	}
	for _, entry := range inventory.External {
		if !expected[entry.Family] || (entry.State != "absent" && entry.State != "json-config") {
			return fmt.Errorf("unexpected external entry: %+v", entry)
		}
		delete(expected, entry.Family)
	}
	if len(expected) != 0 || len(inventory.ConfigPolicy) == 0 {
		return fmt.Errorf("external/config policy census is incomplete: missing=%v policy=%v", expected, inventory.ConfigPolicy)
	}
	for key, policy := range inventory.ConfigPolicy {
		if policy != "public" && policy != "secret" {
			return fmt.Errorf("invalid config policy %s=%s", key, policy)
		}
	}
	return nil
}

func observePublicConfig(t *testing.T, output, label string, settings, metadata, ordinary []byte) publicConfigExpectation {
	t.Helper()
	observations, err := readPublicConfigSettings(settings)
	if err != nil {
		t.Fatalf("%s independent config readback: %v", label, err)
	}
	metadataRows, err := readPublicMetadata(metadata)
	if err != nil {
		t.Fatalf("%s independent metadata readback: %v", label, err)
	}
	observations = append(observations, metadataRows...)
	expected, err := expectPublicConfig(observations, ordinary)
	if err != nil {
		t.Fatalf("%s independent config expectations: %v", label, err)
	}
	data, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeRecordingBytes(t, filepath.Join(output, label+"-config-expectation.json"), data)
	return expected
}

// The genuine config-list command returns stored settings in the explicit
// JSON data envelope, excluding the KV plane. Retain every data key and reject
// duplicate keys in either object rather than allowing map decoding to conceal
// them. The envelope's numeric schema_version is protocol metadata, not a
// setting. User memories are observed in ordinary JSONL independently.
// Metadata has its own read-only SQL observation; unobserved entries cannot pass.
func readPublicConfigSettings(data []byte) ([]publicConfigObservation, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("config readback must be an envelope object")
	}
	observations := []publicConfigObservation{}
	seenEnvelope := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || seenEnvelope[key] {
			return nil, errors.New("duplicate or invalid config readback envelope key")
		}
		seenEnvelope[key] = true
		switch key {
		case "schema_version":
			var version any
			if err := decoder.Decode(&version); err != nil {
				return nil, errors.New("unsupported config readback schema_version")
			}
			number, ok := version.(json.Number)
			if !ok || number != "1" {
				return nil, errors.New("unsupported config readback schema_version")
			}
		case "data":
			start, err := decoder.Token()
			if err != nil || start != json.Delim('{') {
				return nil, errors.New("config readback data must be an object")
			}
			seenSettings := map[string]bool{}
			for decoder.More() {
				token, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				setting, ok := token.(string)
				if !ok || setting == "" || seenSettings[setting] || strings.HasPrefix(setting, "kv.") {
					return nil, errors.New("empty, duplicate or non-setting config readback key")
				}
				var rawValue any
				if err := decoder.Decode(&rawValue); err != nil {
					return nil, fmt.Errorf("config readback value must be a string: %w", err)
				}
				value, ok := rawValue.(string)
				if !ok {
					return nil, errors.New("config readback value must be a string")
				}
				seenSettings[setting] = true
				observations = append(observations, publicConfigObservation{Table: "config", Key: setting, Value: value, Classification: "public"})
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return nil, errors.New("config readback data is not closed")
			}
		default:
			return nil, errors.New("unexpected config readback envelope key")
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seenEnvelope["schema_version"] || !seenEnvelope["data"] {
		return nil, errors.New("config readback envelope is incomplete")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("config readback has trailing data")
	}
	return observations, nil
}

// Observe the fresh public corpus through the pinned CLI's read-only SQL
// command, independently of inventory and archive bytes. Retain every metadata
// row and reject ambiguous envelopes, duplicate rows and non-string values.
func readPublicMetadata(data []byte) ([]publicConfigObservation, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("metadata readback must be an envelope object")
	}
	observations := []publicConfigObservation{}
	seenEnvelope, seenKeys := map[string]bool{}, map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || seenEnvelope[key] {
			return nil, errors.New("duplicate or invalid metadata readback envelope key")
		}
		seenEnvelope[key] = true
		switch key {
		case "schema_version":
			var version any
			if err := decoder.Decode(&version); err != nil {
				return nil, err
			}
			if number, ok := version.(json.Number); !ok || number != "1" {
				return nil, errors.New("unsupported metadata readback schema_version")
			}
		case "data":
			start, err := decoder.Token()
			if err != nil || start != json.Delim('[') {
				return nil, errors.New("metadata readback data must be an array")
			}
			for decoder.More() {
				start, err := decoder.Token()
				if err != nil || start != json.Delim('{') {
					return nil, errors.New("metadata readback row must be an object")
				}
				row := map[string]string{}
				for decoder.More() {
					token, err := decoder.Token()
					if err != nil {
						return nil, err
					}
					field, ok := token.(string)
					if _, duplicate := row[field]; !ok || duplicate || (field != "key" && field != "value") {
						return nil, errors.New("duplicate or unexpected metadata readback field")
					}
					var rawValue any
					if err := decoder.Decode(&rawValue); err != nil {
						return nil, err
					}
					value, ok := rawValue.(string)
					if !ok {
						return nil, errors.New("metadata readback fields must be strings")
					}
					row[field] = value
				}
				end, err := decoder.Token()
				if err != nil || end != json.Delim('}') || len(row) != 2 || row["key"] == "" || seenKeys[row["key"]] {
					return nil, errors.New("incomplete or duplicate metadata readback row")
				}
				seenKeys[row["key"]] = true
				observations = append(observations, publicConfigObservation{Table: "metadata", Key: row["key"], Value: row["value"], Classification: "public"})
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return nil, errors.New("metadata readback array is not closed")
			}
		default:
			return nil, errors.New("unexpected metadata readback envelope key")
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seenEnvelope["schema_version"] || !seenEnvelope["data"] {
		return nil, errors.New("metadata readback envelope is incomplete")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("metadata readback has trailing data")
	}
	return observations, nil
}

func expectPublicConfig(observations []publicConfigObservation, ordinary []byte) (publicConfigExpectation, error) {
	expected := publicConfigExpectation{
		Config: map[string]dumpformat.Record{}, UserMemory: map[string]dumpformat.Record{},
		Policy: map[string]string{}, Memories: map[string]any{}, References: []string{},
	}
	projection, err := decodePublicProjection(ordinary)
	if err != nil {
		return expected, err
	}
	for key, value := range projection.Memories {
		if _, ok := value.(string); key == "" || !ok {
			return expected, errors.New("ordinary memory observation requires a key and string value")
		}
		expected.Memories[key] = value
		expected.Policy["config/kv.memory."+key] = "public"
		expected.UserMemory[key] = dumpformat.Record{Kind: "memory", ID: key,
			Body:   dumpformat.Row{"key": key, "value": value, "classification": "public"},
			Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "kv.memory." + key, "value": value}}}
	}
	references := map[string]bool{}
	var collectReferences func(any)
	collectReferences = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if ref, ok := value["$secret"].(string); ok && len(value) == 1 && ref != "" {
				references[ref] = true
			}
			for _, item := range value {
				collectReferences(item)
			}
		case []any:
			for _, item := range value {
				collectReferences(item)
			}
		}
	}
	for _, observed := range observations {
		id := observed.Table + "/" + observed.Key
		if (observed.Table != "config" && observed.Table != "metadata") || observed.Key == "" || (observed.Table == "config" && strings.HasPrefix(observed.Key, "kv.memory.")) {
			return expected, errors.New("unsupported independent config source identity")
		}
		if _, duplicate := expected.Config[id]; duplicate {
			return expected, fmt.Errorf("duplicate config observation %q", id)
		}
		if observed.Classification != "public" && observed.Classification != "secret" {
			return expected, fmt.Errorf("unclassified config observation %q", id)
		}
		if observed.Classification == "secret" && !equalJSON(observed.Value, map[string]any{"$secret": id}) {
			return expected, fmt.Errorf("config observation %q requires its exact secret reference", id)
		}
		value, ok := observed.Value.(string)
		if !ok {
			encoded, err := json.Marshal(observed.Value)
			if err != nil {
				return expected, err
			}
			value = string(encoded)
		}
		key := "plug.config/" + observed.Key
		if _, collision := expected.Memories[key]; collision {
			return expected, fmt.Errorf("reserved config memory-key collision %q", key)
		}
		expected.Memories[key] = value
		expected.Policy[id] = observed.Classification
		expected.Config[id] = dumpformat.Record{Kind: "config", ID: id,
			Body:   dumpformat.Row{"key": id, "value": observed.Value, "classification": observed.Classification},
			Source: dumpformat.Provenance{Table: observed.Table, Fields: dumpformat.Row{"key": observed.Key, "value": observed.Value}}}
		references[id] = true
		collectReferences(observed.Value)
	}
	for ref := range references {
		expected.References = append(expected.References, ref)
	}
	sort.Strings(expected.References)
	return expected, nil
}

func assertPublicConfigArchive(t *testing.T, archive *dumpformat.Archive, inventoryPath string, expected publicConfigExpectation) {
	t.Helper()
	data, err := os.ReadFile(inventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct {
		ConfigPolicy map[string]string `json:"config_policy"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatal(err)
	}
	if err := comparePublicConfigSources(archive.Records("config"), archive.Records("memory"), inventory.ConfigPolicy, expected); err != nil {
		t.Fatal(err)
	}
}

func comparePublicConfigSources(config, memories []dumpformat.Record, policy map[string]string, expected publicConfigExpectation) error {
	for _, group := range []struct {
		kind string
		got  []dumpformat.Record
		want map[string]dumpformat.Record
	}{{"config", config, expected.Config}, {"memory", memories, expected.UserMemory}} {
		seen := map[string]bool{}
		for _, record := range group.got {
			if seen[record.ID] {
				return fmt.Errorf("duplicate archive %s ID %q", group.kind, record.ID)
			}
			seen[record.ID] = true
			want, ok := group.want[record.ID]
			if !ok || !equalJSON(record, want) {
				return fmt.Errorf("archive %s source/body differs from independent observation %q", group.kind, record.ID)
			}
		}
		if len(seen) != len(group.want) {
			return fmt.Errorf("archive %s source census is incomplete", group.kind)
		}
	}
	if !equalJSON(policy, expected.Policy) {
		return errors.New("config policy differs from complete independent observations")
	}
	return nil
}

// This is the declared reverse mapping of validated input provenance, separate
// from independently observed configuration and from the unchanged ordinary
// JSONL/archive comparison. The explicit fixture names the nonnil source
// archive fields for this vector; the protocol catalog names opaque columns.
// Every generated key/value and all original user metadata remain exact.
func projectPublicReverseExpectation(ordinary []byte, records []dumpformat.Record, archivedFields []string) ([]byte, error) {
	projection, err := decodePublicProjection(ordinary)
	if err != nil {
		return nil, err
	}
	declared := map[string]bool{}
	for _, field := range archivedFields {
		if field == "" || declared[field] {
			return nil, fmt.Errorf("invalid or duplicate reverse archive field %q", field)
		}
		declared[field] = true
	}
	if len(declared) == 0 {
		return nil, errors.New("reverse archive field profile is empty")
	}
	seen := map[string]bool{}
	for _, record := range records {
		issue, exists := projection.Issues[record.ID]
		if record.Kind != "bead" || record.Source.Table != "issues" || !exists || seen[record.ID] {
			return nil, fmt.Errorf("reverse input issue census differs at %q", record.ID)
		}
		seen[record.ID] = true
		metadata, ok := issue["metadata"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("ordinary issue %q metadata is not an object", record.ID)
		}
		add := func(key string, value any) error {
			if _, collision := metadata[key]; collision {
				return fmt.Errorf("ordinary issue %q metadata collides with declared reverse key %q", record.ID, key)
			}
			metadata[key] = value
			return nil
		}
		for _, field := range dumpformat.OpaqueIssueFields() {
			value, present := record.Source.Fields[field]
			if !present || declared[field] {
				return nil, fmt.Errorf("issue %q missing or overlapping opaque source field %q", record.ID, field)
			}
			if err := add("beads."+field, value); err != nil {
				return nil, err
			}
		}
		archived := map[string]any{}
		for _, field := range archivedFields {
			value, present := record.Source.Fields[field]
			if !present || value == nil {
				return nil, fmt.Errorf("issue %q missing declared nonnull archive field %q", record.ID, field)
			}
			archived[field] = value
		}
		// The accepted reverse contract also preserves a textual pinned value
		// inside the source archive while mapping its public boolean value.
		if value, textual := record.Source.Fields["pinned"].(string); textual {
			archived["pinned"] = value
		}
		encoded, err := json.Marshal(archived)
		if err != nil {
			return nil, err
		}
		if err := add("_plug_source_fields", string(encoded)); err != nil {
			return nil, err
		}
		if status, _ := record.Source.Fields["status"].(string); status == "" {
			category, ok := record.Body["status_category"].(string)
			if !ok || category == "" {
				return nil, fmt.Errorf("issue %q lacks source status and category", record.ID)
			}
			if err := add("_plug_status_category", category); err != nil {
				return nil, err
			}
		}
		if outcome, present := record.Body["result"]; present && outcome != nil {
			value, ok := outcome.(string)
			if !ok || value == "" {
				return nil, fmt.Errorf("issue %q has invalid result", record.ID)
			}
			if err := add("_plug_status_outcome", value); err != nil {
				return nil, err
			}
		}
	}
	if len(seen) != len(projection.Issues) {
		return nil, errors.New("reverse input issue census is incomplete")
	}
	return json.Marshal(projection)
}

// Ordinary import retains exactly its original user memories. Reverse adds an
// explicitly enumerated config-memory set; no prefix is discarded or treated
// as effective source configuration. Report checks are independent of JSONL.
func comparePublicReverse(ordinary, actual []byte, expected publicConfigExpectation, references []string, configCount int) error {
	want, err := decodePublicProjection(ordinary)
	if err != nil {
		return err
	}
	got, err := decodePublicProjection(actual)
	if err != nil {
		return err
	}
	if !equalJSON(got.Issues, want.Issues) || !equalJSON(got.Edges, want.Edges) || !equalJSON(got.Comments, want.Comments) {
		return errors.New("reverse changed issue/relation/comment semantics")
	}
	if !equalJSON(got.Memories, expected.Memories) {
		return errors.New("reverse config/user-memory set or values differ")
	}
	if configCount != len(expected.Config) || len(references) != len(expected.References) || len(references) > 0 && !equalJSON(references, expected.References) {
		return errors.New("reverse config report differs from independent observations")
	}
	return nil
}

func projectJSONL(data []byte, fields []string, categories map[string]string) ([]byte, error) {
	projection := semanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	first := true
	for {
		var row map[string]any
		if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		if _, header := row["_schema"]; header {
			if !first || len(row) != 2 || row["_schema"] != "beads-jsonl/1" || row["_sort"] != "stable-v1" {
				return nil, fmt.Errorf("unsupported or misplaced JSONL header")
			}
			first = false
			continue
		}
		first = false
		if row["_type"] == "memory" {
			key, _ := row["key"].(string)
			if key == "" {
				return nil, fmt.Errorf("memory row has no key")
			}
			if _, duplicate := projection.Memories[key]; duplicate {
				return nil, fmt.Errorf("duplicate memory key %q", key)
			}
			projection.Memories[key] = row["value"]
			continue
		}
		if row["_type"] != "issue" {
			return nil, fmt.Errorf("unsupported JSONL row type %v", row["_type"])
		}
		id, _ := row["id"].(string)
		if id == "" {
			return nil, fmt.Errorf("issue row has no id")
		}
		item := map[string]any{}
		for _, field := range fields {
			switch field {
			case "status_category":
				item[field] = statusCategory(row["status"], categories)
			case "source_type":
				item[field] = row["issue_type"]
			default:
				value, present := row[field]
				// The pinned CLI omits these typed empty values from JSONL.
				// Normalize only absent keys; explicit null stays observable.
				if !present {
					switch field {
					case "labels":
						value = []any{}
					case "metadata":
						value = map[string]any{}
					case "spec_id", "close_reason":
						value = ""
					}
				}
				item[field] = value
			}
		}
		projection.Issues[id] = item
		projection.Edges = append(projection.Edges, projectedDependencies(row["dependencies"])...)
		if parent, ok := row["parent"].(string); ok && parent != "" {
			projection.Edges = append(projection.Edges, map[string]any{"from": id, "to": parent, "target_kind": "depends_on_issue_id", "type": "parent-child"})
		}
		projection.Comments = append(projection.Comments, projectedNested(row["comments"], []string{"id", "issue_id", "author", "created_by", "text", "body"})...)
	}
	sortProjection(&projection)
	return json.Marshal(projection)
}

func projectArchive(archive *dumpformat.Archive, fields []string) ([]byte, error) {
	projection := semanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
	for _, record := range archive.Records("bead") {
		projection.Issues[record.ID] = projectArchiveBead(record, fields)
	}
	for _, record := range archive.Records("edge") {
		projection.Edges = append(projection.Edges, projectMap(record.Body, []string{"from", "to", "target_kind", "type"}))
	}
	for _, record := range archive.Records("comment") {
		projection.Comments = append(projection.Comments, projectMap(record.Body, []string{"id", "issue_id", "author", "created_by", "text", "body"}))
	}
	for _, record := range archive.Records("memory") {
		key, _ := record.Body["key"].(string)
		if key == "" {
			key = record.ID
		}
		projection.Memories[key] = record.Body["value"]
	}
	sortProjection(&projection)
	return json.Marshal(projection)
}

func projectArchiveBead(record dumpformat.Record, fields []string) map[string]any {
	item := map[string]any{}
	for _, field := range fields {
		if field == "close_reason" {
			// The canonical normalized body omits this column; the exact source
			// provenance retains it. Never turn a real reason into a null match.
			item[field] = record.Source.Fields[field]
		} else {
			item[field] = record.Body[field]
		}
	}
	return item
}

func projectedNested(value any, fields []string) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if ok {
			result = append(result, projectMap(object, fields))
		}
	}
	return result
}

func projectedDependencies(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		edge := map[string]any{"from": object["issue_id"], "type": object["type"]}
		if target, ok := object["depends_on_id"]; ok {
			edge["to"], edge["target_kind"] = target, "depends_on_issue_id"
		} else if target, ok := object["depends_on_external"]; ok {
			edge["to"], edge["target_kind"] = target, "depends_on_external"
		}
		result = append(result, edge)
	}
	return result
}

func projectMap(input map[string]any, fields []string) map[string]any {
	result := map[string]any{}
	for _, field := range fields {
		if value, ok := input[field]; ok {
			result[field] = value
		}
	}
	return result
}

func sortProjection(projection *semanticProjection) {
	sort.SliceStable(projection.Edges, func(i, j int) bool { return canonical(projection.Edges[i]) < canonical(projection.Edges[j]) })
	sort.SliceStable(projection.Comments, func(i, j int) bool { return canonical(projection.Comments[i]) < canonical(projection.Comments[j]) })
}

func validateSourceRows(t *testing.T, projection []byte, fixture recordingFixture) {
	t.Helper()
	var value semanticProjection
	if err := json.Unmarshal(projection, &value); err != nil {
		t.Fatal(err)
	}
	for _, id := range fixture.IDs {
		if id == "pub-keep" {
			continue
		}
		if _, ok := value.Issues[id]; !ok {
			t.Fatalf("source recording omitted issue %s", id)
		}
	}
	if value.Memories[fixture.MemoryKey] != "paired-store memory" {
		t.Fatalf("source recording omitted memory %s: %+v", fixture.MemoryKey, value.Memories)
	}
	wantEdges := []map[string]any{
		{"from": publicChildID, "to": "pub-dependency", "target_kind": "depends_on_issue_id", "type": "blocks"},
		{"from": publicChildID, "to": "pub-root", "target_kind": "depends_on_issue_id", "type": "parent-child"},
	}
	if _, ok := value.Issues["pub-root.2"]; ok {
		wantEdges = append(wantEdges, map[string]any{"from": "pub-root.2", "to": "pub-root", "target_kind": "depends_on_issue_id", "type": "parent-child"})
	}
	if !equalJSON(value.Edges, wantEdges) {
		t.Fatalf("source recording changed dependency/parent relations: got=%s want=%s", canonical(value.Edges), canonical(wantEdges))
	}
	if len(value.Comments) != 1 || value.Comments[0]["issue_id"] != "pub-root" || value.Comments[0]["text"] != "preserve this public comment" {
		t.Fatalf("source recording changed the public comment: %s", canonical(value.Comments))
	}
}

func assertTargetContainsSource(t *testing.T, source, target []byte, fixture recordingFixture) {
	t.Helper()
	if err := comparePublicImport(source, target, fixture); err != nil {
		t.Fatal(err)
	}
}

// This comparison deliberately checks relation contents and multiplicity. Equal
// counts would miss an import that replaces an endpoint, type, author, or body.
func comparePublicImport(source, target []byte, fixture recordingFixture) error {
	left, err := decodePublicProjection(source)
	if err != nil {
		return fmt.Errorf("source projection: %w", err)
	}
	right, err := decodePublicProjection(target)
	if err != nil {
		return fmt.Errorf("target projection: %w", err)
	}
	if _, ok := right.Issues["pub-keep"]; !ok {
		return fmt.Errorf("target import deleted the pre-existing target issue")
	}
	for id, issue := range left.Issues {
		if !equalJSON(issue, right.Issues[id]) {
			return fmt.Errorf("target import changed source issue %s", id)
		}
	}
	for id := range right.Issues {
		if id != "pub-keep" {
			if _, source := left.Issues[id]; !source {
				return fmt.Errorf("target import introduced issue %s", id)
			}
		}
	}
	if !equalJSON(left.Memories, right.Memories) {
		return fmt.Errorf("target import changed source memories")
	}
	if !equalJSON(left.Edges, right.Edges) {
		return fmt.Errorf("target import changed dependency/parent relations: source=%s target=%s", canonical(left.Edges), canonical(right.Edges))
	}
	if !equalJSON(left.Comments, right.Comments) {
		return fmt.Errorf("target import changed comment contents: source=%s target=%s", canonical(left.Comments), canonical(right.Comments))
	}
	for _, id := range fixture.IDs {
		if id != "pub-keep" && right.Issues[id] == nil {
			return fmt.Errorf("target import omitted source issue %s", id)
		}
	}
	return nil
}

func decodePublicProjection(data []byte) (semanticProjection, error) {
	var projection semanticProjection
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&projection); err != nil {
		return projection, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return projection, fmt.Errorf("projection has trailing data: %v", err)
	}
	return projection, nil
}

// Authored measurement controls, not recordings: prove that the import
// comparison rejects substituted relations even when the row counts match.
func TestPublicProjectionRejectsRelationCorruption(t *testing.T) {
	source := semanticProjection{
		Issues: map[string]map[string]any{"pub-root": {"title": "Public root"}, publicChildID: {"title": "Public child"}, "pub-dependency": {"title": "Public dependency"}},
		Edges: []map[string]any{
			{"from": publicChildID, "to": "pub-dependency", "target_kind": "depends_on_issue_id", "type": "blocks"},
			{"from": publicChildID, "to": "pub-root", "target_kind": "depends_on_issue_id", "type": "parent-child"},
		},
		Comments: []map[string]any{{"id": json.Number("9007199254740993"), "issue_id": "pub-root", "author": "public-recorder", "text": "preserve this public comment"}},
		Memories: map[string]any{"public-memory": "paired-store memory"},
	}
	fixture := recordingFixture{IDs: []string{"pub-root", publicChildID, "pub-dependency", "pub-keep"}}
	sourceBytes, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*semanticProjection)
		want   string
	}{
		{"unchanged", func(*semanticProjection) {}, ""},
		{"dependency_endpoint", func(p *semanticProjection) { p.Edges[0]["to"] = "pub-keep" }, "dependency/parent relations"},
		{"dependency_type", func(p *semanticProjection) { p.Edges[0]["type"] = "relates-to" }, "dependency/parent relations"},
		{"parent_endpoint", func(p *semanticProjection) { p.Edges[1]["to"] = "pub-keep" }, "dependency/parent relations"},
		{"duplicate_edge", func(p *semanticProjection) { p.Edges = append(p.Edges, p.Edges[0]) }, "dependency/parent relations"},
		{"extra_issue", func(p *semanticProjection) {
			p.Issues["pub-extra"] = map[string]any{"title": "unexpected target issue"}
		}, "target import introduced issue"},
		{"comment_text", func(p *semanticProjection) { p.Comments[0]["text"] = "replaced" }, "comment contents"},
		{"comment_author", func(p *semanticProjection) { p.Comments[0]["author"] = "another-actor" }, "comment contents"},
		{"comment_issue", func(p *semanticProjection) { p.Comments[0]["issue_id"] = publicChildID }, "comment contents"},
		{"comment_integer_precision", func(p *semanticProjection) { p.Comments[0]["id"] = json.Number("9007199254740992") }, "comment contents"},
		{"duplicate_comment", func(p *semanticProjection) { p.Comments = append(p.Comments, p.Comments[0]) }, "comment contents"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target, err := decodePublicProjection(sourceBytes)
			if err != nil {
				t.Fatal(err)
			}
			target.Issues["pub-keep"] = map[string]any{"title": "Keep target row"}
			tc.mutate(&target)
			targetBytes, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			err = comparePublicImport(sourceBytes, targetBytes, fixture)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("corrupt import error=%v, want %s", err, tc.want)
			}
		})
	}
}

func TestPublicProjectionArchiveCloseReasonAndReverseHeader(t *testing.T) {
	fields := []string{"title", "close_reason"}
	raw := []byte(`{"_type":"issue","id":"pub-dependency","title":"Public dependency","close_reason":"dependency complete"}` + "\n")
	want, err := projectJSONL(raw, fields, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePublicProjection(want)
	if err != nil {
		t.Fatal(err)
	}
	// This is an authored projection control, not an executable recording or
	// an archive delivered to restore. The canonical normalized body omits the
	// close reason while its independent raw source column retains the value.
	record := dumpformat.Record{
		Body:   dumpformat.Row{"title": "Public dependency"},
		Source: dumpformat.Provenance{Fields: dumpformat.Row{"close_reason": "dependency complete"}},
	}
	if got := projectArchiveBead(record, fields); !equalJSON(got, decoded.Issues["pub-dependency"]) {
		t.Fatalf("archive projection lost the retained close reason: %s", canonical(got))
	}
	header := []byte(`{"_schema":"beads-jsonl/1","_sort":"stable-v1"}` + "\n")
	for _, tc := range []struct {
		name string
		data []byte
		ok   bool
	}{
		{"supported_header", append(append([]byte{}, header...), raw...), true},
		{"unknown_schema", []byte(`{"_schema":"unknown/1","_sort":"stable-v1"}` + "\n"), false},
		{"duplicate_header", append(append([]byte{}, header...), header...), false},
		{"late_header", append(append([]byte{}, raw...), header...), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := projectJSONL(tc.data, fields, nil)
			if tc.ok {
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("versioned reverse framing changed semantics: err=%v got=%s", err, got)
				}
			} else if err == nil {
				t.Fatal("unsupported framing was silently discarded")
			}
		})
	}
}

// All config controls below use independently enumerated authored observations
// and output rows. They do not invoke a producer, converter, restore or store.
type publicConfigControl struct {
	Ordinary     []byte
	Observations []publicConfigObservation
	Config       []dumpformat.Record
	Memory       []dumpformat.Record
	Policy       map[string]string
	Rows         []map[string]any
	References   []string
	Count        int
}

func newPublicConfigControl(t *testing.T) publicConfigControl {
	t.Helper()
	ordinary, err := projectJSONL([]byte(`{"_type":"issue","id":"pub-root","title":"Public root"}
{"_type":"memory","key":"public-memory","value":"user value"}
{"_type":"memory","key":"plug.config/user-owned","value":"keep this user memory"}
`), []string{"title"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return publicConfigControl{
		Ordinary: ordinary,
		Observations: []publicConfigObservation{
			{Table: "config", Key: "custom.public_recording", Value: "paired-public", Classification: "public"},
			{Table: "metadata", Key: "api_token", Value: map[string]any{"$secret": "metadata/api_token"}, Classification: "secret"},
		},
		Config: []dumpformat.Record{
			{Kind: "config", ID: "config/custom.public_recording", Body: dumpformat.Row{"key": "config/custom.public_recording", "value": "paired-public", "classification": "public"}, Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "custom.public_recording", "value": "paired-public"}}},
			{Kind: "config", ID: "metadata/api_token", Body: dumpformat.Row{"key": "metadata/api_token", "value": map[string]any{"$secret": "metadata/api_token"}, "classification": "secret"}, Source: dumpformat.Provenance{Table: "metadata", Fields: dumpformat.Row{"key": "api_token", "value": map[string]any{"$secret": "metadata/api_token"}}}},
		},
		Memory: []dumpformat.Record{
			{Kind: "memory", ID: "public-memory", Body: dumpformat.Row{"key": "public-memory", "value": "user value", "classification": "public"}, Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "kv.memory.public-memory", "value": "user value"}}},
			{Kind: "memory", ID: "plug.config/user-owned", Body: dumpformat.Row{"key": "plug.config/user-owned", "value": "keep this user memory", "classification": "public"}, Source: dumpformat.Provenance{Table: "config", Fields: dumpformat.Row{"key": "kv.memory.plug.config/user-owned", "value": "keep this user memory"}}},
		},
		Policy: map[string]string{"config/custom.public_recording": "public", "metadata/api_token": "secret", "config/kv.memory.public-memory": "public", "config/kv.memory.plug.config/user-owned": "public"},
		Rows: []map[string]any{
			{"_type": "issue", "id": "pub-root", "title": "Public root"},
			{"_type": "memory", "key": "public-memory", "value": "user value"},
			{"_type": "memory", "key": "plug.config/user-owned", "value": "keep this user memory"},
			{"_type": "memory", "key": "plug.config/custom.public_recording", "value": "paired-public"},
			{"_type": "memory", "key": "plug.config/api_token", "value": `{"$secret":"metadata/api_token"}`},
		},
		References: []string{"config/custom.public_recording", "metadata/api_token"}, Count: 2,
	}
}

func TestPublicProjectionConfigSources(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*publicConfigControl)
	}{
		{"unchanged", func(*publicConfigControl) {}},
		{"dropped_config", func(c *publicConfigControl) { c.Config = c.Config[1:] }},
		{"extra_config", func(c *publicConfigControl) { r := c.Config[0]; r.ID = "config/extra"; c.Config = append(c.Config, r) }},
		{"duplicate_config_id", func(c *publicConfigControl) { c.Config = append(c.Config, c.Config[0]) }},
		{"same_count_substitution", func(c *publicConfigControl) { c.Config[0].ID = "config/other" }},
		{"classification", func(c *publicConfigControl) { c.Config[0].Body["classification"] = "secret" }},
		{"body_value", func(c *publicConfigControl) { c.Config[0].Body["value"] = "substituted" }},
		{"source_value", func(c *publicConfigControl) { c.Config[0].Source.Fields["value"] = "substituted" }},
		{"body_source_substitution", func(c *publicConfigControl) {
			c.Config[0].Body["value"] = "substituted"
			c.Config[0].Source.Fields["value"] = "substituted"
		}},
		{"source_key", func(c *publicConfigControl) { c.Config[0].Source.Fields["key"] = "other" }},
		{"source_table", func(c *publicConfigControl) { c.Config[0].Source.Table = "metadata" }},
		{"missing_source_relations_identical", func(c *publicConfigControl) { c.Config[0].Source.Fields = nil }},
		{"extra_source_field", func(c *publicConfigControl) { c.Config[0].Source.Fields["extra"] = "hidden" }},
		{"body_key", func(c *publicConfigControl) { c.Config[0].Body["key"] = "config/other" }},
		{"secret_reference", func(c *publicConfigControl) { c.Config[1].Body["value"] = map[string]any{"$secret": "metadata/other"} }},
		{"dropped_user_memory", func(c *publicConfigControl) { c.Memory = c.Memory[1:] }},
		{"duplicate_user_memory", func(c *publicConfigControl) { c.Memory = append(c.Memory, c.Memory[0]) }},
		{"user_memory_value", func(c *publicConfigControl) { c.Memory[0].Body["value"] = "substituted" }},
		{"policy_missing", func(c *publicConfigControl) { delete(c.Policy, "config/custom.public_recording") }},
		{"policy_extra", func(c *publicConfigControl) { c.Policy["config/extra"] = "public" }},
		{"policy_classification", func(c *publicConfigControl) { c.Policy["metadata/api_token"] = "public" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPublicConfigControl(t)
			expected, err := expectPublicConfig(c.Observations, c.Ordinary)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&c)
			err = comparePublicConfigSources(c.Config, c.Memory, c.Policy, expected)
			if (err == nil) != (tc.name == "unchanged") {
				t.Fatalf("source control %s: %v", tc.name, err)
			}
		})
	}
}

func TestPublicProjectionReverseConfigMemories(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*publicConfigControl)
	}{
		{"unchanged", func(*publicConfigControl) {}},
		{"dropped_config", func(c *publicConfigControl) { c.Rows = c.Rows[:4] }},
		{"extra_reserved", func(c *publicConfigControl) {
			c.Rows = append(c.Rows, map[string]any{"_type": "memory", "key": "plug.config/unobserved", "value": "extra"})
		}},
		{"same_count_key_substitution", func(c *publicConfigControl) { c.Rows[3]["key"] = "plug.config/other" }},
		{"same_count_value_substitution", func(c *publicConfigControl) { c.Rows[3]["value"] = "substituted" }},
		{"duplicate_config_memory", func(c *publicConfigControl) { c.Rows = append(c.Rows, c.Rows[3]) }},
		{"duplicate_user_memory", func(c *publicConfigControl) { c.Rows = append(c.Rows, c.Rows[1]) }},
		{"reserved_collision", func(c *publicConfigControl) { c.Rows[3]["key"] = "plug.config/user-owned" }},
		{"changed_user_memory", func(c *publicConfigControl) { c.Rows[1]["value"] = "substituted" }},
		{"dropped_reserved_user_memory", func(c *publicConfigControl) { c.Rows = append(c.Rows[:2], c.Rows[3:]...) }},
		{"noncanonical_secret_string", func(c *publicConfigControl) { c.Rows[4]["value"] = `{ "$secret": "metadata/api_token" }` }},
		{"nonstring_config_memory", func(c *publicConfigControl) { c.Rows[4]["value"] = map[string]any{"$secret": "metadata/api_token"} }},
		{"changed_issue", func(c *publicConfigControl) { c.Rows[0]["title"] = "substituted" }},
		{"wrong_count", func(c *publicConfigControl) { c.Count-- }},
		{"missing_reference", func(c *publicConfigControl) { c.References = c.References[:1] }},
		{"extra_reference", func(c *publicConfigControl) { c.References = append(c.References, "metadata/unobserved") }},
		{"duplicate_reference", func(c *publicConfigControl) { c.References[1] = c.References[0] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPublicConfigControl(t)
			expected, err := expectPublicConfig(c.Observations, c.Ordinary)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&c)
			var data bytes.Buffer
			encoder := json.NewEncoder(&data)
			for _, row := range c.Rows {
				if err := encoder.Encode(row); err != nil {
					t.Fatal(err)
				}
			}
			projection, err := projectJSONL(data.Bytes(), []string{"title"}, nil)
			if err == nil {
				err = comparePublicReverse(c.Ordinary, projection, expected, c.References, c.Count)
			}
			if (err == nil) != (tc.name == "unchanged") {
				t.Fatalf("reverse control %s: %v", tc.name, err)
			}
		})
	}
}

func TestPublicProjectionConfigObservationRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*publicConfigControl)
	}{
		{"duplicate_id", func(c *publicConfigControl) { c.Observations = append(c.Observations, c.Observations[0]) }},
		{"ambiguous_source_keys", func(c *publicConfigControl) {
			c.Observations[1].Key = c.Observations[0].Key
			c.Observations[1].Classification = "public"
		}},
		{"user_reserved_collision", func(c *publicConfigControl) { c.Observations[0].Key = "user-owned" }},
		{"unknown_table", func(c *publicConfigControl) { c.Observations[0].Table = "other" }},
		{"missing_classification", func(c *publicConfigControl) { c.Observations[0].Classification = "" }},
		{"wrong_secret_reference", func(c *publicConfigControl) { c.Observations[1].Value = map[string]any{"$secret": "metadata/other"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPublicConfigControl(t)
			tc.edit(&c)
			if _, err := expectPublicConfig(c.Observations, c.Ordinary); err == nil {
				t.Fatal("ambiguous or unclassified observation admitted")
			}
		})
	}
}

func TestPublicProjectionConfigSettingsReadback(t *testing.T) {
	observations, err := readPublicConfigSettings([]byte(`{"schema_version":1,"data":{"custom.public_recording":"paired-public","schema_version":"stored-version","stored-only":"9007199254740993"}}`))
	if err != nil || len(observations) != 3 || observations[0].Value != "paired-public" || observations[1].Key != "schema_version" || observations[1].Value != "stored-version" || observations[2].Value != "9007199254740993" {
		t.Fatalf("independent stored-settings readback: %v, %+v", err, observations)
	}
	empty, err := readPublicConfigSettings([]byte(`{"data":{},"schema_version":1}`))
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty stored-settings readback: %v, %+v", err, empty)
	}
	for _, data := range []string{
		`[]`, `{"custom.public_recording":"paired-public","schema_version":1}`,
		`{"data":{"a":"x"}}`, `{"schema_version":2,"data":{"a":"x"}}`,
		`{"schema_version":"1","data":{"a":"x"}}`,
		`{"schema_version":{},"data":{}}`,
		`{"schema_version":1,"schema_version":1,"data":{}}`,
		`{"schema_version":1,"data":{},"data":{}}`,
		`{"schema_version":1,"data":{"a":"x"},"other":true}`,
		`{"schema_version":1,"data":[]}`, `{"schema_version":1,"data":null}`,
		`{"schema_version":1,"data":{"a":"x","a":"y"}}`,
		`{"schema_version":1,"data":{"a":null}}`,
		`{"schema_version":1,"data":{"a":42}}`,
		`{"schema_version":1,"data":{"a":false}}`,
		`{"schema_version":1,"data":{"a":{}}}`,
		`{"schema_version":1,"data":{"kv.memory.hidden":"x"}}`,
		`{"schema_version":1,"data":{"a":"x"}} {}`,
	} {
		if _, err := readPublicConfigSettings([]byte(data)); err == nil {
			t.Fatal("invalid/duplicate/non-setting readback admitted")
		}
	}
}

func TestPublicProjectionReverseProvenance(t *testing.T) {
	ordinary := []byte(`{"issues":{"sample":{"title":"Original","metadata":{"user":true,"beads.user":"owned"}}},"edges":[],"comments":[],"memories":{}}`)
	fields := dumpformat.Row{"actor": "observer", "current_revision": json.Number("9007199254740993"), "status": "closed"}
	for _, field := range dumpformat.OpaqueIssueFields() {
		fields[field] = nil
	}
	fields["agent_state"] = "idle"
	record := dumpformat.Record{Kind: "bead", ID: "sample", Body: dumpformat.Row{"status_category": "completed", "result": "success"}, Source: dumpformat.Provenance{Table: "issues", Fields: fields}}
	profile := []string{"actor", "current_revision"}
	expected, err := projectPublicReverseExpectation(ordinary, []dumpformat.Record{record}, profile)
	if err != nil {
		t.Fatal(err)
	}
	want, err := decodePublicProjection(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	metadata := want.Issues["sample"]["metadata"].(map[string]any)
	for _, field := range dumpformat.OpaqueIssueFields() {
		metadata["beads."+field] = nil
	}
	metadata["beads.agent_state"] = "idle"
	metadata["_plug_source_fields"] = `{"actor":"observer","current_revision":9007199254740993}`
	metadata["_plug_status_outcome"] = "success"
	wantBytes, err := json.Marshal(want)
	if err != nil || !bytes.Equal(wantBytes, expected) {
		t.Fatalf("reverse input mapping differs: got=%s want=%s err=%v", expected, wantBytes, err)
	}
	config := publicConfigExpectation{Memories: map[string]any{}}
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"unchanged", func(map[string]any) {}},
		{"changed_user_value", func(m map[string]any) { m["user"] = false }},
		{"dropped_user_prefix", func(m map[string]any) { delete(m, "beads.user") }},
		{"missing_opaque", func(m map[string]any) { delete(m, "beads.agent_state") }},
		{"changed_opaque", func(m map[string]any) { m["beads.agent_state"] = "changed" }},
		{"extra_opaque_prefix", func(m map[string]any) { m["beads.unknown"] = nil }},
		{"changed_source_archive", func(m map[string]any) { m["_plug_source_fields"] = `{"actor":"changed"}` }},
		{"missing_source_archive", func(m map[string]any) { delete(m, "_plug_source_fields") }},
		{"missing_outcome", func(m map[string]any) { delete(m, "_plug_status_outcome") }},
		{"extra_reserved_prefix", func(m map[string]any) { m["_plug_unknown"] = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := decodePublicProjection(wantBytes)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(actual.Issues["sample"]["metadata"].(map[string]any))
			data, err := json.Marshal(actual)
			if err != nil {
				t.Fatal(err)
			}
			err = comparePublicReverse(expected, data, config, nil, 0)
			if (err == nil) != (tc.name == "unchanged") {
				t.Fatalf("unexpected reverse comparison: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name string
		edit func(*dumpformat.Record, *recordingFixture)
	}{
		{"missing_opaque_input", func(r *dumpformat.Record, _ *recordingFixture) { delete(r.Source.Fields, "agent_state") }},
		{"missing_archived_input", func(r *dumpformat.Record, _ *recordingFixture) { delete(r.Source.Fields, "actor") }},
		{"null_archived_input", func(r *dumpformat.Record, _ *recordingFixture) { r.Source.Fields["actor"] = nil }},
		{"extra_issue", func(r *dumpformat.Record, _ *recordingFixture) { r.ID = "unobserved" }},
		{"wrong_table", func(r *dumpformat.Record, _ *recordingFixture) { r.Source.Table = "other" }},
		{"duplicate_field_profile", func(_ *dumpformat.Record, f *recordingFixture) { f.ReverseArchivedFields = []string{"actor", "actor"} }},
		{"overlapping_field_profile", func(_ *dumpformat.Record, f *recordingFixture) { f.ReverseArchivedFields = []string{"agent_state"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			var input dumpformat.Record
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			if err := decoder.Decode(&input); err != nil {
				t.Fatal(err)
			}
			fixture := recordingFixture{ReverseArchivedFields: append([]string(nil), profile...)}
			tc.edit(&input, &fixture)
			if _, err := projectPublicReverseExpectation(ordinary, []dumpformat.Record{input}, fixture.ReverseArchivedFields); err == nil {
				t.Fatal("invalid reverse input was accepted")
			}
		})
	}
	for _, key := range []string{"beads.agent_state", "_plug_source_fields", "_plug_status_outcome"} {
		t.Run("collision_"+key, func(t *testing.T) {
			input, err := decodePublicProjection(ordinary)
			if err != nil {
				t.Fatal(err)
			}
			input.Issues["sample"]["metadata"].(map[string]any)[key] = "original-user-value"
			data, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := projectPublicReverseExpectation(data, []dumpformat.Record{record}, profile); err == nil {
				t.Fatal("original metadata collision was overwritten")
			}
		})
	}
	if _, err := projectPublicReverseExpectation(ordinary, []dumpformat.Record{record, record}, profile); err == nil {
		t.Fatal("duplicate input issue was accepted")
	}
	if _, err := projectPublicReverseExpectation(ordinary, nil, profile); err == nil {
		t.Fatal("missing input issue was accepted")
	}
}

func TestPublicProjectionOmittedTypedDefaults(t *testing.T) {
	fields := []string{"labels", "metadata", "spec_id", "close_reason"}
	project := func(raw string) map[string]any {
		t.Helper()
		data, err := projectJSONL([]byte(raw), fields, nil)
		if err != nil {
			t.Fatal(err)
		}
		projection, err := decodePublicProjection(data)
		if err != nil {
			t.Fatal(err)
		}
		return projection.Issues["sample"]
	}
	omitted := project(`{"_type":"issue","id":"sample"}`)
	if !equalJSON(omitted, map[string]any{"labels": []any{}, "metadata": map[string]any{}, "spec_id": "", "close_reason": ""}) {
		t.Fatalf("omitted CLI defaults differ: %+v", omitted)
	}
	explicitNull := project(`{"_type":"issue","id":"sample","labels":null,"metadata":null,"spec_id":null,"close_reason":null}`)
	for _, field := range fields {
		if explicitNull[field] != nil {
			t.Fatalf("explicit null %s was replaced with a default", field)
		}
	}
	if equalJSON(omitted, explicitNull) {
		t.Fatal("explicit null and omitted typed defaults became equal")
	}
	populated := project(`{"_type":"issue","id":"sample","labels":["kept"],"metadata":{"kept":true},"spec_id":"kept-spec","close_reason":"real reason"}`)
	if !equalJSON(populated, map[string]any{"labels": []any{"kept"}, "metadata": map[string]any{"kept": true}, "spec_id": "kept-spec", "close_reason": "real reason"}) {
		t.Fatal("populated optional fields changed")
	}
}

func TestPublicProjectionMetadataReadback(t *testing.T) {
	observations, err := readPublicMetadata([]byte(`{"schema_version":1,"data":[{"key":"_project_id","value":"9007199254740993"},{"value":"","key":"empty"}]}`))
	if err != nil || len(observations) != 2 || observations[0].Table != "metadata" || observations[0].Key != "_project_id" || observations[0].Value != "9007199254740993" || observations[0].Classification != "public" || observations[1].Value != "" {
		t.Fatalf("independent metadata readback: %v, %+v", err, observations)
	}
	empty, err := readPublicMetadata([]byte(`{"data":[],"schema_version":1}`))
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty metadata readback: %v, %+v", err, empty)
	}
	for _, data := range []string{
		`[]`, `{"data":[]}`, `{"schema_version":1}`,
		`{"schema_version":2,"data":[]}`, `{"schema_version":"1","data":[]}`,
		`{"schema_version":1,"schema_version":1,"data":[]}`,
		`{"schema_version":1,"data":[],"data":[]}`,
		`{"schema_version":1,"data":[],"other":true}`,
		`{"schema_version":1,"data":{}}`, `{"schema_version":1,"data":null}`,
		`{"schema_version":1,"data":[null]}`, `{"schema_version":1,"data":[[]]}`,
		`{"schema_version":1,"data":[{}]}`,
		`{"schema_version":1,"data":[{"key":"a"}]}`,
		`{"schema_version":1,"data":[{"value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"","value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"a","key":"b","value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":"x","value":"y"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":"x","other":"y"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":"x"},{"key":"a","value":"y"}]}`,
		`{"schema_version":1,"data":[{"key":42,"value":"x"}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":42}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":null}]}`,
		`{"schema_version":1,"data":[{"key":"a","value":false}]}`,
		`{"schema_version":1,"data":[]} {}`,
	} {
		if _, err := readPublicMetadata([]byte(data)); err == nil {
			t.Fatal("invalid or ambiguous metadata readback admitted")
		}
	}
}

// Authored build-info controls do not execute or stand in for a reference binary.
func TestPublicProjectionBuildInfoRequiresMainModulePin(t *testing.T) {
	module := "example.invalid/reference"
	pin := hashBytes([]byte(module))
	label := "//cmd/reference:reference"
	mainPath := "\tpath\t" + module + "/cmd/reference\n"
	mainModule := "\tmod\t" + module + "\t(devel)\n"
	for _, tc := range []struct {
		name, info, pin, label string
		ok                     bool
	}{
		{"main_module", "reference: go1.26.10\n" + mainPath + mainModule, pin, label, true},
		{"bazel_main_package", "reference: go1.26.10\n" + mainPath + "\tdep\texample.invalid/dependency\tv1.0.0\n", pin, label, true},
		{"wrong_module", mainPath + "\tmod\texample.invalid/other\t(devel)\n", pin, label, false},
		{"dependency_only", "\tdep\t" + module + "\tv1.0.0\n", pin, label, false},
		{"substring_only", mainPath + "\tmod\t" + module + "-other\t(devel)\n", pin, label, false},
		{"substring_path", "\tpath\t" + module + "-other/cmd/reference\n", pin, label, false},
		{"wrong_package", "\tpath\t" + module + "/cmd/other\n" + mainModule, pin, label, false},
		{"package_suffix", "\tpath\t" + module + "/cmd/reference/other\n" + mainModule, pin, label, false},
		{"missing_path", mainModule, pin, label, false},
		{"malformed_path", mainPath + "\tpath\n", pin, label, false},
		{"duplicate_path", mainPath + mainPath + mainModule, pin, label, false},
		{"malformed_module", mainPath + "\tmod\t" + module + "\n", pin, label, false},
		{"duplicate_module", mainPath + mainModule + mainModule, pin, label, false},
		{"missing_pin", mainPath + mainModule, "", label, false},
		{"invalid_label", mainPath + mainModule, pin, "cmd/reference:reference", false},
		{"extra_label_target", mainPath + mainModule, pin, label + ":other", false},
		{"noncanonical_package", mainPath + mainModule, pin, "//cmd/../cmd/reference:reference", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkPublicBuildInfo([]byte(tc.info), tc.pin, tc.label)
			if (err == nil) != tc.ok {
				t.Fatalf("main package binding: %v", err)
			}
		})
	}
	profile := readRecordingProfile(t)
	fixture := readRecordingFixture(t)
	if len(profile.GoModuleSHA256) != 64 || len(fixture.ExternalFamilies) != 26 || len(fixture.StatusCategories) != 8 {
		t.Fatal("recording runfiles lost the sealed source profile")
	}
}

func copyPinned(t *testing.T, source, destination, expected string, expectedSize int64) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, h), in)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil || n != expectedSize || hex.EncodeToString(h.Sum(nil)) != expected {
		t.Fatalf("pinned executable copy failed: bytes=%d/%d copy=%v close=%v digest=%s/%s", n, expectedSize, copyErr, closeErr, hex.EncodeToString(h.Sum(nil)), expected)
	}
}

// Resolve the fresh workspace from each command CWD. The accepted CLI skips
// local metadata creation for explicit BEADS_DIR with an external data root.
func recordingEnv(dataRoot string) func(string) []string {
	return func(dir string) []string {
		home := filepath.Join(dir, "home")
		return []string{"PATH=/usr/bin:/bin", "HOME=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "BEADS_DOLT_DATA_DIR=" + dataRoot, "LANG=C.UTF-8", "TZ=UTC", "CI=true", "DO_NOT_TRACK=1", "BEADS_NO_DAEMON=1", "BEADS_ACTOR=public-recorder", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Public recorder", "GIT_AUTHOR_EMAIL=public@example.invalid", "GIT_COMMITTER_NAME=Public recorder", "GIT_COMMITTER_EMAIL=public@example.invalid"}
	}
}

func runCommand(t *testing.T, root string, receipts *[]commandReceipt, label, store, dir string, env []string, argv []string, stdin []byte) []byte {
	return runCommandMode(t, root, receipts, label, store, dir, env, argv, stdin, true)
}

// runCommandExpected records a real native command that is expected to refuse.
// Its raw stdout/stderr and nonzero receipt remain evidence; only the caller's
// assertion decides whether the observed exit is the accepted refusal.
func runCommandExpected(t *testing.T, root string, receipts *[]commandReceipt, label, store, dir string, env []string, argv []string, stdin []byte, expectedExit int) []byte {
	before := len(*receipts)
	out := runCommandMode(t, root, receipts, label, store, dir, env, argv, stdin, false)
	if len(*receipts) != before+1 || (*receipts)[before].Exit != expectedExit {
		got := -1
		if len(*receipts) > before {
			got = (*receipts)[before].Exit
		}
		t.Fatalf("native refusal %s exit=%d want=%d", label, got, expectedExit)
	}
	return out
}

func runCommandMode(t *testing.T, root string, receipts *[]commandReceipt, label, store, dir string, env []string, argv []string, stdin []byte, failOnError bool) []byte {
	t.Helper()
	if len(argv) == 0 {
		t.Fatal("empty command argv")
	}
	command := argv[0]
	args := argv[1:]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir, cmd.Env = dir, env
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exit = exitErr.ExitCode()
		}
	}
	n := len(*receipts) + 1
	base := fmt.Sprintf("%03d-%s", n, strings.ReplaceAll(label, "/", "_"))
	stdoutPath, stderrPath := filepath.Join(root, base+".stdout"), filepath.Join(root, base+".stderr")
	if writeErr := os.WriteFile(stdoutPath, stdout.Bytes(), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(stderrPath, stderr.Bytes(), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	receipt := commandReceipt{Label: label, Store: store, Argv: append([]string(nil), argv...), Directory: dir, Environment: append([]string(nil), env...), StdoutSHA256: hashBytes(stdout.Bytes()), StderrSHA256: hashBytes(stderr.Bytes()), Exit: exit, StdoutPath: stdoutPath, StderrPath: stderrPath}
	if stdin != nil {
		receipt.StdinSHA256 = hashBytes(stdin)
	}
	*receipts = append(*receipts, receipt)
	// Flush before failing so the failing command and every preceding command
	// retain their argv, exit status and raw-output hashes in Bazel outputs.
	writeCommandReceipts(t, root, *receipts)
	if err != nil && failOnError {
		t.Fatalf("public command %s failed (exit %d): %v\nstdout=%s\nstderr=%s", label, exit, err, stdout.String(), stderr.String())
	}
	return stdout.Bytes()
}

func writeCommandReceipts(t *testing.T, root string, commands []commandReceipt) {
	t.Helper()
	data, err := json.MarshalIndent(commands, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "commands.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hashFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return hashBytes(data)
}

func canonical(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func equalJSON(left, right any) bool {
	a, err := json.Marshal(left)
	if err != nil {
		return false
	}
	b, err := json.Marshal(right)
	return err == nil && bytes.Equal(a, b)
}

func statusCategory(value any, categories map[string]string) string {
	status := fmt.Sprint(value)
	if category, ok := categories[status]; ok {
		return category
	}
	return status
}

func isLowerHex(value string) bool {
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
