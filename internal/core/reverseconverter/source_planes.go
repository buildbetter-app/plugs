package reverseconverter

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/ontologymigration"
)

// Options selects the pinned default export or its explicit all-planes form.
// Directives retain the existing explicit ontology type-admission contract.
type Options struct {
	All        bool
	Directives []ontologymigration.Directive
}

func sourceFlag(fields map[string]any, name string) (bool, bool) {
	value, exists := fields[name]
	if !exists || value == nil {
		return false, true
	}
	return parsePinned(value)
}

func omissionLine(row Record) string {
	tuple, _ := json.Marshal([]string{row.Project, row.Source.Table, row.ID})
	return string(tuple) + ": omitted Ephemeral=true by pinned default export"
}

func collisionReason(id, left, right string) string {
	if left != right {
		return fmt.Sprintf("source-plane-contract: raw ID %q is present in both entity planes", id)
	}
	return fmt.Sprintf("duplicate bead id %q", id)
}

// The pinned importer consumes this exact explicit plane marker. Keep it in
// the protocol catalog; it is never used to rename a source ID or table.
func (line issueLine) MarshalJSON() ([]byte, error) {
	type plain issueLine
	raw, err := json.Marshal(plain(line))
	if err != nil {
		return nil, err
	}
	if line.SourcePlane != dumpformat.EphemeralEntityTable {
		return raw, nil
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	fields[dumpformat.EphemeralPlaneWireField] = json.RawMessage("true")
	return json.Marshal(fields)
}

// SourcePlaneEntityRecord projects an admitted physical active row into the
// existing issue converter. Source.Table and raw ID remain the original tuple.
func SourcePlaneEntityRecord(project string, fields map[string]any) Record {
	source := cloneMap(fields)
	body := map[string]any{"type": "issue", "source_type": source["issue_type"], "labels": []any{}}
	for key := range supportedBeadBodyFields {
		switch key {
		case "type", "source_type", "labels", "status_category", "result":
			continue
		}
		if value, present := source[key]; present {
			body[key] = cloneValue(value)
		}
	}
	status := text(source["status"])
	body["status_category"] = categoryForStatus(status)
	if status == "closed" {
		body["result"] = "success"
	}
	return Record{Kind: "bead", ID: text(source["id"]), Project: project, Body: body, Source: Provenance{Table: dumpformat.EphemeralEntityTable, Fields: source}}
}

func physicalExportRecords(dump Dump) (entities, edges, comments []Record, reasons []string) {
	entities = append(entities, dump.Records["bead"]...)
	edges = append(edges, dump.Records["edge"]...)
	comments = append(comments, dump.Records["comment"]...)
	active := map[string]*Record{}
	for _, row := range sortedRecords(dump.Records["beads-archive"]) {
		if row.Source.Table != dumpformat.EphemeralEntityTable {
			continue
		}
		if dump.Manifest.SourcePlaneAddressContract != dumpformat.SourcePlaneAddressContract {
			reasons = append(reasons, "active source requires the explicit physical address contract")
			continue
		}
		project := row.Project
		if project == "" {
			id := text(row.Source.Fields["id"])
			for _, p := range dump.Manifest.Projects {
				if strings.HasPrefix(id, p.Prefix+"-") {
					project = p.Prefix
					break
				}
			}
		}
		if project == "" {
			reasons = append(reasons, "active source lacks declared project")
			continue
		}
		record := SourcePlaneEntityRecord(project, row.Source.Fields)
		if active[record.ID] != nil {
			reasons = append(reasons, fmt.Sprintf("duplicate active id %q", record.ID))
			continue
		}
		active[record.ID] = &record
	}
	for _, row := range sortedRecords(dump.Records["beads-archive"]) {
		fields := row.Source.Fields
		switch row.Source.Table {
		case dumpformat.EphemeralLabelTable:
			issue := active[text(fields["issue_id"])]
			if issue == nil {
				reasons = append(reasons, "active label lacks physical owner")
				continue
			}
			issue.Body["labels"] = append(issue.Body["labels"].([]any), fields["label"])
		case dumpformat.EphemeralCommentTable:
			issue := active[text(fields["issue_id"])]
			if issue == nil {
				reasons = append(reasons, "active comment lacks physical owner")
				continue
			}
			comments = append(comments, Record{Kind: "comment", ID: text(fields["id"]), Project: issue.Project, Body: cloneMap(fields), Source: Provenance{Table: row.Source.Table, Fields: cloneMap(fields)}})
		}
	}
	for _, row := range active {
		entities = append(entities, *row)
	}
	return
}
