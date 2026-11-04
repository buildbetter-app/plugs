package s1b01b04

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
)

//go:embed run-2.json
var runTwoBytes []byte

//go:embed record_reference.py
var recorderBytes []byte

const (
	pinnedCommit = "9d6fff760b7a314b476539198504d77ce0959656"
	pinnedTree   = "cde121d6e4119e454857ad1c3ca293e2791be840"
	pinnedBinary = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"
)

func canonical(value any) ([]byte, error) {
	return json.Marshal(value)
}

func stringField(t *testing.T, object map[string]any, path ...string) string {
	t.Helper()
	var value any = object
	for _, key := range path {
		child, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("%s is not an object at %s", strings.Join(path, "."), key)
		}
		value, ok = child[key]
		if !ok {
			t.Fatalf("missing %s", strings.Join(path, "."))
		}
	}
	result, ok := value.(string)
	if !ok || result == "" {
		t.Fatalf("%s is not a nonempty string", strings.Join(path, "."))
	}
	return result
}

func mapField(t *testing.T, object map[string]any, path ...string) map[string]any {
	t.Helper()
	var value any = object
	for _, key := range path {
		child, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("%s is not an object", strings.Join(path, "."))
		}
		value, ok = child[key]
		if !ok {
			t.Fatalf("missing %s", strings.Join(path, "."))
		}
	}
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object", strings.Join(path, "."))
	}
	return result
}

func TestReferenceTwinsHaveStablePinnedProvenance(t *testing.T) {
	if len(manifestBytes) == 0 || len(runOneBytes) == 0 || len(runTwoBytes) == 0 || len(recorderBytes) == 0 {
		t.Fatal("reference artifacts were not embedded")
	}
	manifest := decodeObject(t, manifestBytes)
	if stringField(t, manifest, "format") != "plug.s1-b01-b04.manifest.v1" {
		t.Fatal("manifest format drifted")
	}
	source := mapField(t, manifest, "source")
	if stringField(t, source, "commit") != pinnedCommit || stringField(t, source, "tree") != pinnedTree {
		t.Fatalf("source pin drifted: %+v", source)
	}
	binary := mapField(t, manifest, "binary")
	if stringField(t, binary, "sha256") != pinnedBinary || stringField(t, binary, "version") == "" || stringField(t, binary, "buildCommand") == "" {
		t.Fatalf("binary provenance is incomplete: %+v", binary)
	}
	recorder := mapField(t, manifest, "recorder")
	if stringField(t, recorder, "identity") != "s1-b01-b04-reference" || stringField(t, recorder, "version") != "1" || stringField(t, recorder, "source") != "record_reference.py" || stringField(t, recorder, "sha256") != sha256Hex(recorderBytes) {
		t.Fatalf("recorder provenance is incomplete: %+v", recorder)
	}
	schema := mapField(t, manifest, "schema")
	if stringField(t, schema, "mainDigest") != "c649854f6d67016df0db1c3b324679a41e090b8fad0ae5ac5d94cf72d5e8f0f2" ||
		stringField(t, schema, "ignoredDigest") != "31abcfd3374b7e7ae7b95727785a1591107cac63cbf5db59ea6685350c05b470" {
		t.Fatalf("paired schema digests are incomplete: %+v", schema)
	}
	if fmt.Sprint(schema["mainCursor"]) != "72" || fmt.Sprint(schema["ignoredCursor"]) != "28" {
		t.Fatalf("paired schema cursors drifted: %+v", schema)
	}
	decisions := mapField(t, manifest, "decisionIds")
	for _, card := range []string{"B01", "B02", "B03", "B04", "I5"} {
		values, ok := decisions[card].([]any)
		if !ok || len(values) == 0 {
			t.Fatalf("%s has no decision IDs", card)
		}
	}
	clock := mapField(t, manifest, "clock")
	if stringField(t, clock, "mode") != "controlled-go; sql-driver-clock-unreached" || stringField(t, clock, "seed") == "" {
		t.Fatalf("clock provenance is incomplete: %+v", clock)
	}
	if stringField(t, clock, "limitation") == "" {
		t.Fatalf("clock limitation is not explicit: %+v", clock)
	}
	omitted, ok := clock["omittedFields"].([]any)
	if !ok || len(omitted) < 10 {
		t.Fatalf("runtime omissions are not explicit: %+v", clock)
	}
	if stringField(t, mapField(t, manifest, "i5Evidence"), "target") != "//internal/server/authzrpc:authzrpc_test" {
		t.Fatal("I5 exact-head target is missing")
	}

	if !bytes.Equal(runOneBytes, runTwoBytes) {
		t.Fatal("independently initialized reference vectors differ byte-for-byte")
	}
	twins, ok := manifest["twins"].([]any)
	if !ok || len(twins) != 2 {
		t.Fatalf("twin manifest entries = %#v", manifest["twins"])
	}
	for i, raw := range [][]byte{runOneBytes, runTwoBytes} {
		entry, ok := twins[i].(map[string]any)
		if !ok {
			t.Fatalf("twin %d is not an object", i+1)
		}
		if stringField(t, entry, "freshStore") != "independent-disposable" {
			t.Fatalf("twin %d is not marked fresh", i+1)
		}
		if stringField(t, entry, "artifactSha256") != sha256Hex(raw) {
			t.Fatalf("twin %d artifact digest mismatch", i+1)
		}
	}
}

func TestReferenceVectorsHaveIndependentRawAndSemanticProjections(t *testing.T) {
	for _, data := range [][]byte{runOneBytes, runTwoBytes} {
		artifact := decodeObject(t, data)
		if stringField(t, artifact, "workspaceClass") != "fresh-disposable" {
			t.Fatal("recording does not identify a disposable store")
		}
		raw := []byte(stringField(t, artifact, "rawJSON"))
		if sha256Hex(raw) != stringField(t, artifact, "digest") {
			t.Fatal("raw vector digest mismatch")
		}
		vector := decodeObject(t, raw)
		canonicalRaw, err := canonical(vector)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(canonicalRaw, raw) {
			t.Fatal("raw vector is not canonical JSON")
		}
		cards := mapField(t, vector, "cards")
		keys := make([]string, 0, len(cards))
		for key := range cards {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if fmt.Sprint(keys) != "[B01 B02 B03 B04]" {
			t.Fatalf("card set = %v", keys)
		}
		projection := mapField(t, vector, "projection")
		for _, card := range keys {
			entry := mapField(t, cards, card)
			if _, ok := entry["raw"]; !ok {
				t.Fatalf("%s has no raw command outcomes", card)
			}
			if _, ok := entry["projection"]; !ok {
				t.Fatalf("%s has no independent semantic projection", card)
			}
			if _, ok := projection[card]; !ok {
				t.Fatalf("%s is absent from the independent projection map", card)
			}
			limits, ok := entry["limits"].([]any)
			if !ok || len(limits) == 0 {
				t.Fatalf("%s has no declared limits", card)
			}
		}
		commands, ok := vector["commands"].([]any)
		if !ok || len(commands) < 20 {
			t.Fatalf("command transcript is incomplete: %d", len(commands))
		}
		// Expected refusals are retained as nonzero command outcomes; a skipped
		// refusal would erase the distinction between a typed failure and null.
		refusals := 0
		for _, command := range commands {
			entry := command.(map[string]any)
			if fmt.Sprint(entry["exitCode"]) != "0" {
				refusals++
			}
		}
		if refusals < 3 {
			t.Fatalf("recording lost expected refusal outcomes: %d", refusals)
		}
		if strings.Contains(strings.ToLower(string(raw)), "expectedplug") {
			t.Fatal("reference vector contains authored expected-value marker")
		}
	}
}

func TestReferenceManifestRejectsMissingTwinEvidence(t *testing.T) {
	manifest := decodeObject(t, manifestBytes)
	twins := manifest["twins"].([]any)
	delete(twins[0].(map[string]any), "artifactSha256")
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(data, manifestBytes) {
		t.Fatal("negative manifest mutation did not change bytes")
	}
	if _, ok := twins[0].(map[string]any)["artifactSha256"]; ok {
		t.Fatal("negative mutation failed")
	}
}
