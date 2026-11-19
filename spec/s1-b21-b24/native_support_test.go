package s1b21b24

// Adapted from the independently authored e44a1edbe public-native recorder.
// Nikhil Shinday and the original B25/B27 contributors retain credit.
// Only source helpers are reused; every runtime receipt must be earned here.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const nativeBeadsCommit = "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724"
const nativeBeadsTree = "930405e863284a3955cbd35e73a6097b606b91a5"

type nativeRecordingProfile struct {
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

type nativeRecordingFixture struct {
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

type nativePublicArtifact struct {
	Binary       string
	SHA256       string
	BuildRun     string
	ManifestPath string
	ManifestSHA  string
	Size         int64
}

type nativeCommandReceipt struct {
	StartedUTC   string   `json:"started_utc"`
	EndedUTC     string   `json:"ended_utc"`
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

type nativeSemanticProjection struct {
	Issues   map[string]map[string]any `json:"issues"`
	Edges    []map[string]any          `json:"edges"`
	Comments []map[string]any          `json:"comments"`
	Memories map[string]any            `json:"memories"`
}

type nativePublicConfigObservation struct {
	Table          string `json:"table"`
	Key            string `json:"key"`
	Value          any    `json:"value"`
	Classification string `json:"classification"`
}

type nativePublicConfigExpectation struct {
	Config     map[string]dumpformat.Record `json:"config"`
	UserMemory map[string]dumpformat.Record `json:"user_memory"`
	Policy     map[string]string            `json:"policy"`
	Memories   map[string]any               `json:"reverse_memories"`
	References []string                     `json:"config_references"`
}

func nativeVerifyPublicArtifact(t *testing.T, profile nativeRecordingProfile) nativePublicArtifact {
	t.Helper()
	artifact := nativePublicArtifact{Binary: os.Getenv("PLUG_PUBLIC_BEADS_BINARY"), SHA256: os.Getenv("PLUG_PUBLIC_BEADS_SHA256"), BuildRun: os.Getenv("PLUG_PUBLIC_BEADS_BUILD_RUN"), ManifestPath: os.Getenv("PLUG_PUBLIC_BEADS_MANIFEST")}
	for name, value := range map[string]string{"PLUG_PUBLIC_BEADS_BINARY": artifact.Binary, "PLUG_PUBLIC_BEADS_SHA256": artifact.SHA256, "PLUG_PUBLIC_BEADS_BUILD_RUN": artifact.BuildRun, "PLUG_PUBLIC_BEADS_MANIFEST": artifact.ManifestPath} {
		if value == "" {
			t.Fatalf("required public artifact evidence is missing: %s", name)
		}
	}
	if !filepath.IsAbs(artifact.Binary) || !filepath.IsAbs(artifact.ManifestPath) || len(artifact.SHA256) != 64 || !nativeIsLowerHex(artifact.SHA256) {
		t.Fatal("public artifact paths or digest are invalid")
	}
	if run, err := strconv.ParseUint(artifact.BuildRun, 10, 64); err != nil || run == 0 || strconv.FormatUint(run, 10) != artifact.BuildRun {
		t.Fatal("public artifact build run is not an immutable decimal run ID")
	}
	manifestData, err := os.ReadFile(artifact.ManifestPath)
	if err != nil || len(manifestData) == 0 || len(manifestData) > 1<<20 {
		t.Fatalf("public artifact manifest is unavailable: %v", err)
	}
	artifact.ManifestSHA = nativeHashBytes(manifestData)
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
	if manifest.Schema != profile.Schema || beadsSource.Commit != nativeBeadsCommit || beadsSource.Tree != nativeBeadsTree || target.Label != profile.Exporter.Label || target.Binary != profile.Exporter.Binary || binaryInfo.SHA256 != artifact.SHA256 || binaryInfo.Mode != "0o755" || binaryInfo.Size <= 0 || manifest.Build.RunID != artifact.BuildRun || manifest.Workflow.SHA != profile.Producer.WorkflowSHA || manifest.Workflow.Ref != "refs/heads/"+profile.Producer.WorkflowBranch || manifest.Workflow.Profile != profile.Schema || version.Commit != nativeBeadsCommit || version.Build != "public-"+nativeBeadsCommit {
		t.Fatalf("public artifact identity does not match profile: schema=%q source=%+v target=%+v binary=%+v build=%+v version=%+v", manifest.Schema, beadsSource, target, binaryInfo, manifest.Build, version)
	}
	info, err := os.Lstat(artifact.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 || info.Size() != binaryInfo.Size {
		t.Fatalf("public executable mode or size is not manifest-bound: %v", err)
	}
	artifact.Size = info.Size()
	if got := nativeHashFile(artifact.Binary); got != artifact.SHA256 {
		t.Fatalf("public executable digest changed: got %s want %s", got, artifact.SHA256)
	}
	return artifact
}

func nativeMakeRecordingRoot(t *testing.T) string {
	t.Helper()
	parent := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if parent == "" {
		parent = "/tmp/plug-swarm/work"
	}
	if !filepath.IsAbs(parent) {
		t.Fatal("public recording output directory must be absolute")
	}
	return nativeMakeRecordingDirectory(t, parent, "s1-b21-b24-native-recording-")
}

func nativeMakeRecordingDirectory(t *testing.T, parent, pattern string) string {
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

func nativeWriteRecordingBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func nativeUseBazelGoSDK(t *testing.T) {
	t.Helper()
	binary := os.Getenv("PLUG_TEST_GO_BINARY")
	if binary == "" {
		return
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(binary)))
	if err != nil {
		t.Fatalf("resolve Bazel Go SDK: %v", err)
	}
	root := filepath.Dir(filepath.Dir(resolved))
	t.Setenv("GOROOT", root)
	t.Setenv("PATH", filepath.Join(root, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Logf("B23 tests use Bazel Go SDK: %s", root)
}

func nativeRecordBuildInfo(t *testing.T, root string, artifact nativePublicArtifact, profile nativeRecordingProfile) []byte {
	t.Helper()
	nativeUseBazelGoSDK(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, runErr := exec.CommandContext(ctx, "go", "version", "-m", artifact.Binary).CombinedOutput()
	path := filepath.Join(root, "build-info.txt")
	if err := os.WriteFile(path, output, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("PUBLIC_BUILD_INFO path=%s sha256=%s", path, nativeHashBytes(output))
	if runErr != nil {
		t.Fatalf("pinned Beads build info is unavailable: %v\n%s", runErr, output)
	}
	if err := nativeCheckPublicBuildInfo(output, profile.GoModuleSHA256, profile.Exporter.Label); err != nil {
		t.Fatalf("pinned Beads main package is not profile-bound: %v\n%s", err, output)
	}
	return output
}

// Bazel's Go binary records its main package path without a mod record. Bind
// that complete path to the sealed exporter label and the unchanged module
// hash. Any present mod record must agree; dependencies cannot supply either.

func nativeCheckPublicBuildInfo(data []byte, expectedModuleSHA256, exporterLabel string) error {
	if len(expectedModuleSHA256) != 64 || !nativeIsLowerHex(expectedModuleSHA256) {
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
			if nativeHashBytes([]byte(module)) != expectedModuleSHA256 {
				return errors.New("main package module does not match the sealed source profile")
			}
		case "mod":
			moduleCount++
			if len(fields) < 3 || nativeHashBytes([]byte(fields[1])) != expectedModuleSHA256 {
				return errors.New("main module does not match the sealed source profile")
			}
		}
	}
	if pathCount != 1 || moduleCount > 1 {
		return errors.New("build info must contain exactly one main package and at most one matching module")
	}
	return nil
}

func nativeValidateInventory(data []byte, fixture nativeRecordingFixture) error {
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

// Accepted c8 outputJSON injects schema_version into a flat object when
// BD_JSON_ENVELOPE is absent. Validate that transport control separately from
// stored settings; never coerce numbers/null/objects into stored string values.
func nativeReadPublicConfigSettings(data []byte) ([]nativePublicConfigObservation, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("config readback must be a flat object")
	}
	observations := []nativePublicConfigObservation{}
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || key == "" || seen[key] || strings.HasPrefix(key, "kv.") {
			return nil, errors.New("empty, duplicate or non-setting config readback key")
		}
		seen[key] = true
		var raw any
		if err := decoder.Decode(&raw); err != nil {
			return nil, err
		}
		if key == "schema_version" {
			if number, ok := raw.(json.Number); !ok || number != "1" {
				return nil, errors.New("unsupported flat config readback schema_version")
			}
			continue
		}
		value, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("stored config readback %q must be a string", key)
		}
		observations = append(observations, nativePublicConfigObservation{Table: "config", Key: key, Value: value, Classification: "public"})
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seen["schema_version"] {
		return nil, errors.New("flat config readback is incomplete")
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

func nativeReadPublicMetadata(data []byte) ([]nativePublicConfigObservation, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("metadata readback must be an envelope object")
	}
	observations := []nativePublicConfigObservation{}
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
				observations = append(observations, nativePublicConfigObservation{Table: "metadata", Key: row["key"], Value: row["value"], Classification: "public"})
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

func nativeExpectPublicConfig(observations []nativePublicConfigObservation, ordinary []byte) (nativePublicConfigExpectation, error) {
	expected := nativePublicConfigExpectation{
		Config: map[string]dumpformat.Record{}, UserMemory: map[string]dumpformat.Record{},
		Policy: map[string]string{}, Memories: map[string]any{}, References: []string{},
	}
	projection, err := nativeDecodePublicProjection(ordinary)
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
		if observed.Classification == "secret" && !nativeEqualJSON(observed.Value, map[string]any{"$secret": id}) {
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

func nativeAssertPublicConfigArchive(t *testing.T, archive *dumpformat.Archive, inventoryPath string, expected nativePublicConfigExpectation) {
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
	if err := nativeComparePublicConfigSources(archive.Records("config"), archive.Records("memory"), inventory.ConfigPolicy, expected); err != nil {
		t.Fatal(err)
	}
}

func nativeComparePublicConfigSources(config, memories []dumpformat.Record, policy map[string]string, expected nativePublicConfigExpectation) error {
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
			if !ok || !nativeEqualJSON(record, want) {
				return fmt.Errorf("archive %s source/body differs from independent observation %q", group.kind, record.ID)
			}
		}
		if len(seen) != len(group.want) {
			return fmt.Errorf("archive %s source census is incomplete", group.kind)
		}
	}
	if !nativeEqualJSON(policy, expected.Policy) {
		return errors.New("config policy differs from complete independent observations")
	}
	return nil
}

// This is the declared reverse mapping of validated input provenance, separate
// from independently observed configuration and from the unchanged ordinary
// JSONL/archive comparison. The explicit fixture names the nonnil source
// archive fields for this vector; the protocol catalog names opaque columns.
// Every generated key/value and all original user metadata remain exact.

func nativeProjectPublicReverseExpectation(ordinary []byte, records []dumpformat.Record, archivedFields []string) ([]byte, error) {
	projection, err := nativeDecodePublicProjection(ordinary)
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
			if !present {
				return nil, fmt.Errorf("issue %q missing declared archive field %q", record.ID, field)
			}
			// Complete source provenance retains explicit nulls. The fixed
			// reverse contract archives only nonnull values in metadata.
			if value != nil {
				archived[field] = value
			}
		}
		// The accepted reverse contract also preserves a textual pinned value
		// inside the source archive while mapping its public boolean value.
		if value, textual := record.Source.Fields["pinned"].(string); textual {
			archived["pinned"] = value
		}
		if len(archived) > 0 {
			encoded, err := json.Marshal(archived)
			if err != nil {
				return nil, err
			}
			if err := add("_plug_source_fields", string(encoded)); err != nil {
				return nil, err
			}
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

func nativeComparePublicReverse(ordinary, actual []byte, expected nativePublicConfigExpectation, references []string, configCount int) error {
	want, err := nativeDecodePublicProjection(ordinary)
	if err != nil {
		return err
	}
	got, err := nativeDecodePublicProjection(actual)
	if err != nil {
		return err
	}
	if !nativeEqualJSON(got.Issues, want.Issues) || !nativeEqualJSON(got.Edges, want.Edges) || !nativeEqualJSON(got.Comments, want.Comments) {
		return errors.New("reverse changed issue/relation/comment semantics")
	}
	if !nativeEqualJSON(got.Memories, expected.Memories) {
		return errors.New("reverse config/user-memory set or values differ")
	}
	if configCount != len(expected.Config) || len(references) != len(expected.References) || len(references) > 0 && !nativeEqualJSON(references, expected.References) {
		return errors.New("reverse config report differs from independent observations")
	}
	return nil
}

func nativeProjectJSONL(data []byte, fields []string, categories map[string]string) ([]byte, error) {
	projection := nativeSemanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
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
				item[field] = nativeStatusCategory(row["status"], categories)
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
					case "description", "notes", "spec_id", "close_reason":
						value = ""
					}
				}
				item[field] = value
			}
		}
		projection.Issues[id] = item
		projection.Edges = append(projection.Edges, nativeProjectedDependencies(row["dependencies"])...)
		if parent, ok := row["parent"].(string); ok && parent != "" {
			projection.Edges = append(projection.Edges, map[string]any{"from": id, "to": parent, "target_kind": "depends_on_issue_id", "type": "parent-child"})
		}
		projection.Comments = append(projection.Comments, nativeProjectedNested(row["comments"], []string{"id", "issue_id", "author", "created_by", "text", "body"})...)
	}
	nativeSortProjection(&projection)
	return json.Marshal(projection)
}

func nativeProjectArchive(archive *dumpformat.Archive, fields []string) ([]byte, error) {
	projection := nativeSemanticProjection{Issues: map[string]map[string]any{}, Memories: map[string]any{}, Edges: []map[string]any{}, Comments: []map[string]any{}}
	for _, record := range archive.Records("bead") {
		projection.Issues[record.ID] = nativeProjectArchiveBead(record, fields)
	}
	for _, record := range archive.Records("edge") {
		projection.Edges = append(projection.Edges, nativeProjectMap(record.Body, []string{"from", "to", "target_kind", "type"}))
	}
	for _, record := range archive.Records("comment") {
		projection.Comments = append(projection.Comments, nativeProjectMap(record.Body, []string{"id", "issue_id", "author", "created_by", "text", "body"}))
	}
	for _, record := range archive.Records("memory") {
		key, _ := record.Body["key"].(string)
		if key == "" {
			key = record.ID
		}
		projection.Memories[key] = record.Body["value"]
	}
	nativeSortProjection(&projection)
	return json.Marshal(projection)
}

func nativeProjectArchiveBead(record dumpformat.Record, fields []string) map[string]any {
	item := map[string]any{}
	for _, field := range fields {
		if field == "close_reason" {
			// The nativeCanonical normalized body omits this column; the exact source
			// provenance retains it. Never turn a real reason into a null match.
			item[field] = record.Source.Fields[field]
		} else {
			item[field] = record.Body[field]
		}
	}
	return item
}

func nativeProjectedNested(value any, fields []string) []map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if ok {
			result = append(result, nativeProjectMap(object, fields))
		}
	}
	return result
}

func nativeProjectedDependencies(value any) []map[string]any {
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

func nativeProjectMap(input map[string]any, fields []string) map[string]any {
	result := map[string]any{}
	for _, field := range fields {
		if value, ok := input[field]; ok {
			result[field] = value
		}
	}
	return result
}

func nativeSortProjection(projection *nativeSemanticProjection) {
	sort.SliceStable(projection.Edges, func(i, j int) bool {
		return nativeCanonical(projection.Edges[i]) < nativeCanonical(projection.Edges[j])
	})
	sort.SliceStable(projection.Comments, func(i, j int) bool {
		return nativeCanonical(projection.Comments[i]) < nativeCanonical(projection.Comments[j])
	})
}

func nativeDecodePublicProjection(data []byte) (nativeSemanticProjection, error) {
	var projection nativeSemanticProjection
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

func nativeCopyPinned(t *testing.T, source, destination, expected string, expectedSize int64) {
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

func nativeRecordingEnv(dataRoot string) func(string) []string {
	return func(dir string) []string {
		home := filepath.Join(dir, "home")
		return []string{"PATH=/usr/bin:/bin", "HOME=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "BEADS_DOLT_DATA_DIR=" + dataRoot, "LANG=C.UTF-8", "TZ=UTC", "CI=true", "DO_NOT_TRACK=1", "BEADS_NO_DAEMON=1", "BEADS_ACTOR=public-recorder", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Public recorder", "GIT_AUTHOR_EMAIL=public@example.invalid", "GIT_COMMITTER_NAME=Public recorder", "GIT_COMMITTER_EMAIL=public@example.invalid"}
	}
}

func nativeRunCommand(t *testing.T, root string, receipts *[]nativeCommandReceipt, label, store, dir string, env []string, argv []string, stdin []byte, expectedExit int) []byte {
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
	started := time.Now().UTC()
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
	receipt := nativeCommandReceipt{StartedUTC: started.Format(time.RFC3339Nano), EndedUTC: time.Now().UTC().Format(time.RFC3339Nano), Label: label, Store: store, Argv: append([]string(nil), argv...), Directory: dir, Environment: append([]string(nil), env...), StdoutSHA256: nativeHashBytes(stdout.Bytes()), StderrSHA256: nativeHashBytes(stderr.Bytes()), Exit: exit, StdoutPath: stdoutPath, StderrPath: stderrPath}
	if stdin != nil {
		receipt.StdinSHA256 = nativeHashBytes(stdin)
	}
	*receipts = append(*receipts, receipt)
	// Flush before failing so the failing command and every preceding command
	// retain their argv, exit status and raw-output hashes in Bazel outputs.
	nativeWriteCommandReceipts(t, root, *receipts)
	if exit != expectedExit {
		t.Fatalf("public command %s failed (exit %d): %v\nstdout=%s\nstderr=%s", label, exit, err, stdout.String(), stderr.String())
	}
	return stdout.Bytes()
}

func nativeWriteCommandReceipts(t *testing.T, root string, commands []nativeCommandReceipt) {
	t.Helper()
	data, err := json.MarshalIndent(commands, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "commands.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func nativeHashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func nativeHashFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return nativeHashBytes(data)
}

func nativeCanonical(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func nativeEqualJSON(left, right any) bool {
	a, err := json.Marshal(left)
	if err != nil {
		return false
	}
	b, err := json.Marshal(right)
	return err == nil && bytes.Equal(a, b)
}

func nativeStatusCategory(value any, categories map[string]string) string {
	status := fmt.Sprint(value)
	if category, ok := categories[status]; ok {
		return category
	}
	return status
}

func nativeIsLowerHex(value string) bool {
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
