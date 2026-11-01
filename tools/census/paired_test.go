package census

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func testSnapshot() Snapshot {
	s := Snapshot{Scope: PreservationScope, SourceSHA256: strings.Repeat("1", 64), Schema: SchemaPair{72, 28, strings.Repeat("2", 64), strings.Repeat("3", 64)}}
	for _, name := range dimensions {
		s.Metrics = append(s.Metrics, Metric{Dimension: name, Count: 2, SHA256: jsonDigest(name)})
	}
	return s
}

func completeInput() (StoreInventory, PairedInput) {
	i := StoreInventory{Format: PairedFormat, Complete: true, Stores: []StoreAlias{{Alias: "synthetic-one"}}}
	p := PairedInput{Format: PairedFormat}
	for _, stage := range requiredStages {
		s := testSnapshot()
		p.Observations = append(p.Observations, Observation{Alias: "synthetic-one", Stage: stage, Snapshot: &s})
	}
	return i, p
}

func TestPairedCensusAllStagesAndDimensions(t *testing.T) {
	i, p := completeInput()
	r, e := Reconcile(i, p)
	if e != nil || r.Status != "PASS" || len(r.Stores) != 1 || len(r.Stores[0].Stages) != 9 {
		t.Fatalf("complete synthetic reconciliation: %v, %s", e, r.Status)
	}
	for _, stage := range r.Stores[0].Stages {
		if len(stage.Metrics) != 8 {
			t.Fatal("dimension disappeared")
		}
	}
	for n := range p.Observations {
		i, p := completeInput()
		p.Observations = append(p.Observations[:n], p.Observations[n+1:]...)
		r, e := Reconcile(i, p)
		if e != nil || r.Status != "BLOCK" || len(r.Stores[0].Stages) != 9 {
			t.Fatal("missing stage was not accounted for")
		}
	}
	for n := range dimensions {
		i, p := completeInput()
		p.Observations[3].Snapshot.Metrics[n].SHA256 = strings.Repeat("a", 64)
		r, e := Reconcile(i, p)
		if e != nil || r.Status != "BLOCK" || r.Stores[0].Stages[3].Metrics[n].Block != TargetDiffers {
			t.Fatalf("difference in %s disappeared", dimensions[n])
		}
	}
}

func TestPairedCensusRejectsAmbiguityAndOmissions(t *testing.T) {
	cases := []struct {
		name   string
		change func(*StoreInventory, *PairedInput)
	}{
		{"duplicate source", func(i *StoreInventory, p *PairedInput) { i.Stores = append(i.Stores, i.Stores[0]) }},
		{"unknown source", func(i *StoreInventory, p *PairedInput) { p.Observations[0].Alias = "undeclared" }},
		{"two copies counted twice", func(i *StoreInventory, p *PairedInput) { p.Observations = append(p.Observations, p.Observations[0]) }},
		{"unknown stage", func(i *StoreInventory, p *PairedInput) { p.Observations[0].Stage = "skipped" }},
		{"missing dimension", func(i *StoreInventory, p *PairedInput) {
			p.Observations[0].Snapshot.Metrics = p.Observations[0].Snapshot.Metrics[1:]
		}},
		{"duplicate dimension", func(i *StoreInventory, p *PairedInput) {
			p.Observations[0].Snapshot.Metrics[0] = p.Observations[0].Snapshot.Metrics[1]
		}},
		{"unknown dimension", func(i *StoreInventory, p *PairedInput) { p.Observations[0].Snapshot.Metrics[0].Dimension = "other" }},
		{"negative count", func(i *StoreInventory, p *PairedInput) { p.Observations[0].Snapshot.Metrics[0].Count = -1 }},
		{"unpaired cursor", func(i *StoreInventory, p *PairedInput) { p.Observations[0].Snapshot.Schema.Ignored = 26 }},
		{"block and snapshot", func(i *StoreInventory, p *PairedInput) { p.Observations[0].Block = SchemaDrift }},
		{"free text block", func(i *StoreInventory, p *PairedInput) {
			p.Observations[0].Snapshot = nil
			p.Observations[0].Block = "private-error"
		}},
		{"empty inventory", func(i *StoreInventory, p *PairedInput) { i.Stores = nil }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			i, p := completeInput()
			test.change(&i, &p)
			if _, e := Reconcile(i, p); e == nil {
				t.Fatal("invalid census accepted")
			}
		})
	}
}

func TestPairedCensusPreservesTypedBoundariesAndSourceBinding(t *testing.T) {
	i, p := completeInput()
	i.Complete = false
	p.Observations[3].Snapshot.Metrics[3].Block = AllocatorUnproven
	p.Observations[3].Snapshot.Metrics[5].Block = HistoryUnavailable
	p.Observations[7].Snapshot = nil
	p.Observations[7].Block = ReadbackUnavailable
	r, e := Reconcile(i, p)
	if e != nil || r.Status != "BLOCK" || r.InventoryBlock != MissingInventory || r.Stores[0].Stages[7].Block != ReadbackUnavailable {
		t.Fatal("unqualified boundary became PASS")
	}
	i, p = completeInput()
	p.Observations[3].Snapshot.SourceSHA256 = strings.Repeat("f", 64)
	r, e = Reconcile(i, p)
	if e != nil || r.Stores[0].Stages[3].Block != SourceDiffers {
		t.Fatal("stale source artifact was admitted")
	}
	i, p = completeInput()
	p.Observations[3].Snapshot.Schema.MainSHA256 = strings.Repeat("f", 64)
	r, e = Reconcile(i, p)
	if e != nil || r.Stores[0].Stages[3].Block != SchemaDrift {
		t.Fatal("changed migration fingerprint was admitted")
	}
}

func TestPairedJSONAndCLIKeepPrivateErrorsOut(t *testing.T) {
	for _, raw := range []string{
		`{"format":"x","format":"y"}`,
		`{"format":"x","observations":[],"private-record-value":"needle"}`,
		`{"format":"x","observations":[{"alias":"a","snapshot":{"scope":"x","scope":"y"}}]}`,
		`{} {}`,
		`{"format":"plug-paired-census/1","Format":"other","observations":[]}`,
		`{"format":"plug-paired-census/1","observations":null}`,
		`{"format":"plug-paired-census/1"}`,
	} {
		var p PairedInput
		e := DecodePairedJSON(strings.NewReader(raw), &p)
		if e == nil || strings.Contains(e.Error(), "needle") || strings.Contains(e.Error(), "private-record") {
			t.Fatal("JSON refusal was not sanitized")
		}
	}
	var out, errOut bytes.Buffer
	code := RunPaired([]string{"-mode", "archive", "-archive", filepath.Join(t.TempDir(), "private-path-needle")}, &out, &errOut)
	if code != 1 || out.Len() != 0 || strings.Contains(errOut.String(), "private-path-needle") {
		t.Fatal("filesystem refusal exposed its path")
	}
	i, p := completeInput()
	i.Complete = false
	dir := t.TempDir()
	write := func(name string, v any) string {
		b, _ := json.Marshal(v)
		file := filepath.Join(dir, name)
		if e := os.WriteFile(file, b, 0600); e != nil {
			t.Fatal(e)
		}
		return file
	}
	out.Reset()
	errOut.Reset()
	code = RunPaired([]string{"-mode", "reconcile", "-inventory", write("inventory.json", i), "-observations", write("observations.json", p)}, &out, &errOut)
	var r PairedResult
	if code != 3 || errOut.Len() != 0 || json.Unmarshal(out.Bytes(), &r) != nil || r.InventoryBlock != MissingInventory {
		t.Fatal("CLI did not preserve BLOCK status")
	}
}

func behaviorFS() fstest.MapFS {
	files := fstest.MapFS{}
	var cards strings.Builder
	for i := 1; i <= 28; i++ {
		fmt.Fprintf(&cards, "## B%02d — synthetic card\n", i)
	}
	files["engdocs/design/plug/spec/behaviors/work.md"] = &fstest.MapFile{Data: []byte(cards.String())}
	for _, name := range []string{"content", "history", "surfaces"} {
		files["engdocs/design/plug/spec/behaviors/"+name+".md"] = &fstest.MapFile{}
	}
	for _, file := range []string{"spec/cardb01/reference-run-1.json", "spec/cardb01/reference-run-2.json", "spec/recorded/extension/extension.json", "spec/recorded/goldens.json", "spec/cardb19/reference.json", "spec/cardb19/source-binding.json", "spec/recorded/b24_untimed_defer_deviation.json", "spec/cardb28/reference/provenance.json", "spec/cardb28/reference/run-a.json", "spec/cardb28/reference/run-b.json"} {
		files[file] = &fstest.MapFile{Data: []byte("{}\n")}
	}
	files["spec/scenarios_test.go"] = &fstest.MapFile{Data: []byte("package spec\nimport \"testing\"\nfunc TestB01B02(t *testing.T) {}\nfunc TestUnassigned(t *testing.T) {}\n// func TestB03(t *testing.T) {}\n")}
	return files
}

func TestBehaviorCensusParsesDeclarationsWithoutClaimingRuntime(t *testing.T) {
	files := behaviorFS()
	r, e := MeasureBehaviors(files)
	if e != nil || len(r.Cards) != 28 || len(r.Cards[0].Declarations) != 1 || len(r.Cards[1].Declarations) != 1 || len(r.Cards[2].Declarations) != 0 || r.UnassignedDeclarations != 1 {
		t.Fatal("behavior declaration inventory differs")
	}
	for _, card := range r.Cards {
		if card.Block != FullCardUnqualified {
			t.Fatal("declaration/reference became a full-card certificate")
		}
	}
	again, e := MeasureBehaviors(files)
	if e != nil || r.SHA256 != again.SHA256 {
		t.Fatal("behavior measurement is not reproducible")
	}
	delete(files, "spec/cardb01/reference-run-2.json")
	if _, e = MeasureBehaviors(files); e == nil {
		t.Fatal("deleted reference disappeared silently")
	}
	files = behaviorFS()
	files["spec/unknown_test.go"] = &fstest.MapFile{Data: []byte("package spec\nimport \"testing\"\nfunc TestB29(t *testing.T) {}")}
	if _, e = MeasureBehaviors(files); e == nil {
		t.Fatal("unknown card was accepted")
	}
	files = behaviorFS()
	files["engdocs/design/plug/spec/behaviors/work.md"].Data = []byte("## B01 — only one\n")
	if _, e = MeasureBehaviors(files); e == nil {
		t.Fatal("removed cards disappeared silently")
	}
}

func TestBehaviorCensusOnCheckout(t *testing.T) {
	root := os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if root == "" {
		t.Fatal("RunsOn source root is required for the complete behavior census")
	}
	r, e := MeasureBehaviors(os.DirFS(root))
	if e != nil || len(r.Cards) != 28 {
		t.Fatalf("checkout behavior census: %v", e)
	}
	withReferences := 0
	for _, card := range r.Cards {
		if len(card.ReferenceArtifacts) > 0 {
			withReferences++
		}
		if card.Block != FullCardUnqualified {
			t.Fatal("full card was overqualified")
		}
	}
	if withReferences != 7 {
		t.Fatal("retained reference slice inventory changed")
	}
	writeSanitized(t, "behavior-census.json", r)
	t.Logf("28 catalog cards; %d have retained reference slices; all full cards remain explicitly unqualified", withReferences)
}

func writeSanitized(t *testing.T, name string, value any) {
	t.Helper()
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
		data, e := json.MarshalIndent(value, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
