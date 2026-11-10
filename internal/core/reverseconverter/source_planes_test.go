package reverseconverter

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

func TestReverseDefaultFiltersOnlyEphemeralAndAllPreservesPlanes(t *testing.T) {
	dump := cloneDump(fixtureDump())
	dump.Manifest.SourcePlaneAddressContract = dumpformat.SourcePlaneAddressContract
	// A non-ephemeral, no-history row really belongs to the active plane. Its
	// flag is independent of physical placement, as in the pinned exporter.
	active := cloneMap(dump.Records["bead"][0].Source.Fields)
	active["id"], active["ephemeral"], active["no_history"], active["storage_class"] = "demo-active", false, true, "ephemeral"
	dump.Records["beads-archive"] = append(dump.Records["beads-archive"], Record{Kind: "beads-archive", ID: "active-origin", Project: "demo", Body: map[string]any{"read_only": true}, Source: Provenance{Table: dumpformat.EphemeralEntityTable, Fields: active}})
	dump.Records["bead"][1].Source.Fields["ephemeral"] = json.Number("1")
	for i := range dump.Manifest.Kinds {
		dump.Manifest.Kinds[i].Count = len(dump.Records[dump.Manifest.Kinds[i].Name])
	}
	result, err := Convert(dump)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Report.OmittedEphemeralRows) != 1 || !strings.Contains(result.Report.OmittedEphemeralRows[0], "demo-1") {
		t.Fatal("missing per-row omission", result.Report)
	}
	if bytes.Contains(result.JSONL, []byte(`"id":"demo-1"`)) || !bytes.Contains(result.JSONL, []byte(`"id":"demo-active"`)) || !bytes.Contains(result.JSONL, []byte(`"`+dumpformat.EphemeralPlaneWireField+`":true`)) {
		t.Fatal("default did not preserve non-ephemeral active plane", string(result.JSONL))
	}
	all, err := ConvertWithOptions(dump, Options{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Report.OmittedEphemeralRows) != 0 || !bytes.Contains(all.JSONL, []byte(`"id":"demo-1"`)) || !bytes.Contains(all.JSONL, []byte(`"ephemeral":true`)) {
		t.Fatal("explicit all omitted ephemeral row", string(all.JSONL))
	}
	if len(result.Projection.Issues) != 3 || len(all.Projection.Issues) != 4 {
		t.Fatal("wrong physical entity counts")
	}
}

func TestReverseBothPhysicalPlanesWithEqualRawIDRefusesWithoutOutput(t *testing.T) {
	dump := cloneDump(fixtureDump())
	dump.Manifest.SourcePlaneAddressContract = dumpformat.SourcePlaneAddressContract
	raw := cloneMap(dump.Records["bead"][0].Source.Fields)
	raw["ephemeral"], raw["no_history"], raw["storage_class"] = true, true, "ephemeral"
	dump.Records["beads-archive"] = append(dump.Records["beads-archive"], Record{Kind: "beads-archive", ID: "active-origin", Project: "demo", Body: map[string]any{"read_only": true}, Source: Provenance{Table: dumpformat.EphemeralEntityTable, Fields: raw}})
	comment := map[string]any{"id": "active-comment", "issue_id": raw["id"], "author": "unverified", "text": "physical active comment", "created_at": "2026-11-05T00:00:00Z"}
	dump.Records["beads-archive"] = append(dump.Records["beads-archive"], Record{Kind: "beads-archive", ID: "comment-origin", Project: "demo", Body: map[string]any{"read_only": true}, Source: Provenance{Table: dumpformat.EphemeralCommentTable, Fields: comment}})
	for i := range dump.Manifest.Kinds {
		dump.Manifest.Kinds[i].Count = len(dump.Records[dump.Manifest.Kinds[i].Name])
	}
	for _, all := range []bool{false, true} {
		result, err := ConvertWithOptions(dump, Options{All: all})
		var refusal *Refusal
		if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "source-plane-contract") || len(result.JSONL) != 0 {
			t.Fatalf("all=%v collision escaped: %v %s", all, err, result.JSONL)
		}
	}
}

func TestReverseReportsEveryOmittedRowAndRetainsOutgoingTarget(t *testing.T) {
	dump := cloneDump(fixtureDump())
	dump.Records["bead"][0].Source.Fields["ephemeral"] = true
	dump.Records["bead"][1].Source.Fields["ephemeral"] = json.Number("1")
	result, err := Convert(dump)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Report.OmittedEphemeralRows) != 2 || len(result.Projection.Issues) != 1 || len(result.Projection.Edges) != 1 || result.Projection.Edges[0].To != "demo-1" {
		t.Fatal("default did not mirror selected-source outgoing relation export", result.Report, result.Projection)
	}
	if result.Report.OmittedEphemeralRows[0] == result.Report.OmittedEphemeralRows[1] || !strings.Contains(result.Report.OmittedEphemeralRows[0], "demo-1") || !strings.Contains(result.Report.OmittedEphemeralRows[1], "demo-3") {
		t.Fatal("per-row omission report lost physical identities", result.Report)
	}
}
