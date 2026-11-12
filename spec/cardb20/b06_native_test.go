package cardb20

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

func TestB06CanonicalNativeCycles(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute evidence directory required")
	}
	r := &nativeRecorder{t: t, root: root}
	checkout := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	for _, key := range []string{"PLUG_B06_REQUEST_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_WORKFLOW_REF"} {
		if os.Getenv(key) == "" {
			t.Fatal("missing exact request/run binding " + key)
		}
	}
	if !filepath.IsAbs(checkout) {
		t.Fatal("actual candidate checkout required")
	}
	head := strings.TrimSpace(string(r.buildCommand(checkout, "candidate-head", "/usr/bin/git", []string{"rev-parse", "HEAD"})))
	if head != os.Getenv("PLUG_B06_REQUEST_SHA") {
		t.Fatal("request/checkout mismatch")
	}
	protocol, err := os.ReadFile(filepath.Join(checkout, "spec/s1-b09-native/native_inputs.json"))
	if err != nil || hashBytes(protocol) != "a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {
		t.Fatal("existing native build protocol mismatch")
	}
	var input struct {
		ExporterLabel string `json:"exporter_label"`
	}
	if json.Unmarshal(protocol, &input) != nil || input.ExporterLabel == "" {
		t.Fatal("missing existing executable label")
	}
	r.target = input.ExporterLabel
	manifest, err := os.ReadFile(filepath.Join(checkout, "tools/lexicon/source_manifest.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "candidate-source-manifest.txt"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	r.writeJSON(filepath.Join(root, "request-run-binding.json"), map[string]any{"candidate": head, "target": os.Getenv("TEST_TARGET"), "run": os.Getenv("GITHUB_RUN_ID"), "attempt": os.Getenv("GITHUB_RUN_ATTEMPT"), "workflow_sha": os.Getenv("GITHUB_SHA"), "workflow_ref": os.Getenv("GITHUB_WORKFLOW_REF"), "source_manifest_sha256": hashBytes(manifest), "native_source": pinnedSource, "qualification": "PUBLIC_OPERATIONAL_RECORDING_ONLY", "official_admission": "NOT_RUN", "old_pr366_native": "UNCHANGED"})
	binary := r.buildCanonicalSource(pinnedSource)
	var originals [][][]string
	for _, twin := range []string{"a", "b"} {
		t.Run("independent_"+twin, func(t *testing.T) {
			dir := filepath.Join(root, "native-"+twin)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			nr := &b06NativeRecorder{nativeRecorder: &nativeRecorder{t: t, root: dir, target: r.target}}
			server := publicnative.StartDolt(t, dir)
			originals = append(originals, nr.core(binary, server))
			nr.missing(binary, server)
			nr.boundaries(binary, server)
		})
	}
	if len(originals) != 2 || !reflect.DeepEqual(originals[0], originals[1]) {
		t.Fatal("independently initialized original raw path/order projections differ")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("paired_"+engine, func(t *testing.T) {
			plug := b06RunPlug(t, engine)
			if !reflect.DeepEqual(originals[0], plug) {
				t.Fatalf("native/Plug raw ordered durable paths differ native=%v plug=%v", originals[0], plug)
			}
			b06Retain(t, "native-durable-comparison.json", map[string]any{"native_raw_paths": originals[0], "plug_raw_paths": plug, "ids": "unchanged; no sorting or renaming", "meaning": "durable pure-tracks/mixed/default-preservation/rotation/parallel", "physical_missing_counterpart": "OPEN; native originals and Plug policy redaction remain distinct", "mixed_plane_counterpart": "OPEN; uniform capability refusal only", "official_native_admission": "NOT_RUN"})
		})
	}
}
