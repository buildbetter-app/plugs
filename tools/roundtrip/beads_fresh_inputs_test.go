package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFreshCommittedNativeInputsHaveExactCensusAndLimit(t *testing.T) {
	inputs := loadFreshCommittedNativeInputs(t)
	if inputs.CaptureSHA256 == "" || inputs.LedgerSHA256 == "" || inputs.StatesSHA256 == "" || inputs.WorkspaceSHA256 == "" || inputs.RestoreTime == "" {
		t.Fatal("committed native input binding is incomplete")
	}
	for _, mode := range []string{"full", "partial", "snapshot"} {
		path := inputs.Requests[mode]
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var request map[string]any
		if err := strictTypedJSON(data, &request); err != nil {
			t.Fatal(err)
		}
		if request["restore_mode"] != mode || !filepath.IsLocal(request["capture"].(string)) || !filepath.IsLocal(request["oracle"].(string)) || strings.Contains(string(data), "/tmp/") {
			t.Fatalf("%s request is not a repository-relative immutable input", mode)
		}
		if filepath.Join(inputs.ReaderDirectory, filepath.FromSlash(request["capture"].(string))) != inputs.CaptureSet || filepath.Join(inputs.ReaderDirectory, filepath.FromSlash(request["oracle"].(string))) != inputs.Oracle {
			t.Fatalf("%s unchanged request does not resolve to its authenticated input", mode)
		}
		if mode == "partial" {
			if request["restore_first_sequence"] != json.Number("2") {
				t.Fatal("partial source ordinal changed")
			}
		} else if _, supplied := request["restore_first_sequence"]; supplied {
			t.Fatalf("%s acquired a partial source ordinal", mode)
		}
	}
}

func TestFreshCommittedNativeInputsRejectUnboundFixturePath(t *testing.T) {
	for _, path := range []string{"/tmp/plug-swarm/capture-set", "../capture-set", "tools/roundtrip/testdata/fresh-native/id-style-a-inputs/capture-set/../oracle"} {
		if filepath.IsLocal(path) && filepath.ToSlash(filepath.Clean(path)) == path {
			t.Fatalf("fixture path unexpectedly qualifies as local: %s", path)
		}
	}
}

func TestFreshRunfileResolvesWorkspaceQualifiedRlocation(t *testing.T) {
	root := t.TempDir()
	workspace := "_main"
	relative := "bazel-out/k8-fastbuild/bin/internal/integration/converterroundtrip/converterroundtrip_test"
	target := filepath.Join(root, workspace, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("declared runfile"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_SRCDIR", root)
	t.Setenv("TEST_WORKSPACE", workspace)
	for _, name := range []string{relative, workspace + "/" + relative} {
		if got, want := freshRunfile(t, name), target; got != want {
			t.Fatalf("fresh runfile %q resolved to %q, want %q", name, got, want)
		}
	}
}

func TestFreshContinuationPlanRetainsDeclaredCommandsAndRefusesChanges(t *testing.T) {
	inputs := loadFreshCommittedNativeInputs(t)
	data, err := os.ReadFile(filepath.Join(inputs.Root, "requests", "continuation-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	steps, err := decodeFreshContinuationPlan(data)
	if err != nil || strings.Join(steps, ",") != "root,child,comment,comment,child" {
		t.Fatalf("committed five-operation plan refused: %v %v", steps, err)
	}
	for _, name := range []string{"unknown-top-field", "unknown-operation-field", "changed-command", "changed-order", "wrong-output"} {
		t.Run(name, func(t *testing.T) {
			var plan map[string]any
			if err := strictTypedJSON(data, &plan); err != nil {
				t.Fatal(err)
			}
			operations := plan["operations"].([]any)
			switch name {
			case "unknown-top-field":
				plan["unbound"] = true
			case "unknown-operation-field":
				operations[0].(map[string]any)["unbound"] = true
			case "changed-command":
				operations[1].(map[string]any)["native_command"] = []string{"create"}
			case "changed-order":
				operations[0], operations[1] = operations[1], operations[0]
			case "wrong-output":
				plan["generated_request"] = "other.json"
			}
			changed, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeFreshContinuationPlan(changed); err == nil {
				t.Fatal("changed continuation plan admitted")
			}
		})
	}
}
