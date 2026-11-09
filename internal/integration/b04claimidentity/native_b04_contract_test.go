package b04claimidentity_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// This source-only target is safe for ordinary focused/whole gates. It binds
// the immutable current-main package preimage and the exact native command
// contract without starting a native process or claiming a recording.
func TestB04NativeInputContract(t *testing.T) {
	inputs := loadB04NativeInputs(t)
	if inputs.Format != "plug.s1.b04.native-inputs.v1" ||
		inputs.Consumer != "7a43c3c625a5c08d3a4364a321cd47d6357ef30e" ||
		inputs.ConsumerTree != "be93a595dc4c44ef4d94d2333060788fd344dca0" ||
		inputs.ConsumerParent != "5812dd80f0c6e4d1e9136604941c7f54eef3238a" {
		t.Fatalf("native inputs are not bound to the pinned main parent: %+v", inputs)
	}
	if inputs.Package != "internal/integration/b04claimidentity" || inputs.PackageBuildBlob == "" || inputs.PackageBuildSHA256 == "" {
		t.Fatalf("package/BUILD preimage binding is incomplete: %+v", inputs)
	}
	if inputs.Source != "c8eb3ba7d4177e111a6f5c8aebe06b5d0b1d5724" || inputs.SourceTree != "930405e863284a3955cbd35e73a6097b606b91a5" ||
		inputs.HelperSource != "70230ccd92597a96000f90223c5d89b103e25426" || inputs.HelperSHA256 == "" {
		t.Fatalf("producer/source/helper bindings are incomplete: %+v", inputs)
	}
	if inputs.BuildRun != "41800330346" || inputs.Artifact != "13307042737" || inputs.ArtifactSHA256 != "28609d7c072c281a748cb53a361d9a3ffada826d6f2e8866a3b1e62b23d5ffc4" || inputs.ArtifactBytes != 111646601 || inputs.BinarySHA256 != "b734da8ba8b6f50d7fd32239e405b8f8ed9d171b7be5fbbccb63a923d257a123" || inputs.BinaryBytes != 161299592 {
		t.Fatalf("admitted artifact binding changed: %+v", inputs)
	}
	if inputs.Descriptor != "41800330346:13307042737:ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a" ||
		inputs.ManifestSHA256 != "ac028e4b1be9609415dde0d9f2a30e12f37f3be5f3f02e4a89b356749fc4e25a" ||
		inputs.WorkflowSHA != "d758fdf42a426e9e2a4b707dbcebb22ec68b264e" || inputs.Control != "2d2164ba3d8e456c9aba15fbeb8cae4edc755997" {
		t.Fatalf("descriptor/workflow/control binding changed: %+v", inputs)
	}
	wantFlags := []string{
		"--nocache_test_results", "--flaky_test_attempts=1", "--test_env=GO_TEST_WRAP_TESTV=1",
		"--test_output=all", "--test_timeout=900,900,900,9000", "--jobs=2", "--local_resources=memory=8192",
	}
	if !reflect.DeepEqual(inputs.Flags, wantFlags) {
		t.Fatalf("Bazel flags=%v want=%v", inputs.Flags, wantFlags)
	}
	if inputs.Vector.ID != "B04-claim-next-native" || inputs.Vector.DatabasePrefix != "b04_native" || len(inputs.Vector.Runs) != 2 {
		t.Fatalf("native vector/twin stores are incomplete: %+v", inputs.Vector)
	}
	wantIDs := map[string]string{
		"direct": "b04-d00", "customActive": "b04-c00",
		"foreignHeld": "b04-f00", "next": "b04-n00", "empty": "b04-e00",
	}
	if !reflect.DeepEqual(inputs.Vector.SourceIDs, wantIDs) {
		t.Fatalf("source IDs=%v want=%v", inputs.Vector.SourceIDs, wantIDs)
	}
	seen := map[string]bool{}
	for _, id := range inputs.Vector.SourceIDs {
		if !b04NativeSafeID(id) || seen[id] {
			t.Fatalf("unsafe or duplicate native source ID %q", id)
		}
		seen[id] = true
	}
	stores := map[string]bool{}
	for _, run := range inputs.Vector.Runs {
		if run.ID == "" || run.Database == "" || stores[run.Database] {
			t.Fatalf("fresh native store binding is missing or duplicated: %+v", run)
		}
		stores[run.Database] = true
	}
	for _, key := range []string{"direct", "sameOwner", "customActive", "foreign", "claimNext", "emptyFront", "show", "history", "versions", "physical", "inventory", "dump"} {
		if inputs.CommandContract[key] == "" {
			t.Fatalf("command contract omitted %s", key)
		}
	}
	if inputs.Qualification == "" || inputs.ClockScope == "" || len(inputs.Boundaries) < 4 {
		t.Fatal("native qualification boundaries are incomplete")
	}
}

func TestB04NativeInputIsSelfContainedJSON(t *testing.T) {
	var raw any
	if err := json.Unmarshal(b04NativeInputsBytes, &raw); err != nil {
		t.Fatal(err)
	}
	if len(b04NativeInputsBytes) == 0 {
		t.Fatal("native input packet is empty")
	}
	// The file is embedded in the target. This check prevents a future BUILD
	// edit from silently dropping the source packet from the Bazel runfiles.
	if _, err := os.Stat(filepath.Join("internal", "integration", "b04claimidentity", "native_b04_inputs.json")); err == nil {
		t.Log("source packet is available in the checkout")
	}
}
