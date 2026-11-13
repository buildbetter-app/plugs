package graphcertfixture

import (
	"bytes"
	"encoding/json"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func commitment(raw []byte, presence string) driver.SourcePlaneBytesV1 {
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(raw)), SHA256: Hash(raw)}
}
func fields(t *testing.T, record driver.SourcePlaneRecordV1) []driver.SourcePlaneFieldV1 {
	var wire struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	Decode(t, record.Raw, &wire)
	result := []driver.SourcePlaneFieldV1{}
	for name, raw := range wire.Source.Fields {
		presence := "null"
		switch bytes.TrimSpace(raw)[0] {
		case '{':
			presence = "object"
		case '[':
			presence = "array"
		case '"':
			presence = "text"
			var text string
			Decode(t, raw, &text)
			raw = []byte(text)
		case 't', 'f':
			presence = "boolean"
		case 'n':
		default:
			presence = "number"
		}
		result = append(result, driver.SourcePlaneFieldV1{Name: name, Value: commitment(raw, presence)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Plan is explicit test intent for task rows in the fixed public source.
// It never infers target authority or manufactures a physical recording.
func Plan(t *testing.T, archive *dumpformat.Archive, lifetime driver.CheckpointLifetime) driver.SourcePlanePlanV1 {
	t.Helper()
	principal := JSON(t, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	p := driver.SourcePlanePlanV1{Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: archive.Manifest().StoreID, ArchiveSHA256: archive.SHA256(), Target: driver.SourcePlaneTargetV1{RequireEmpty: true, Lifetime: lifetime, RuntimeContract: driver.SourcePlaneRuntimeContractV1}, Principals: []driver.SourcePlanePrincipalV1{{Key: "owner", Source: "explicit authored scale fixture", Authority: "trusted-explicit/1", PrincipalJSON: bytes.TrimSpace(principal)}}, Providers: []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, OriginAdapter: "beads-workflow-origins/1"}}}
	d := driver.SourcePlaneDispositionV1{Key: "durable", SourceType: "task", StorageClass: "", SinkKind: "bead", Schema: "bead/1", Lifetime: "core", RefusalPolicy: "exact-only"}
	for _, s := range archive.Manifest().Projects[0].Statuses {
		d.Statuses = append(d.Statuses, driver.SourcePlaneStringPairV1{From: s.Name, To: s.Name})
	}
	sort.Slice(d.Statuses, func(i, j int) bool { return d.Statuses[i].From < d.Statuses[j].From })
	rows, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range rows {
		var row dumpformat.Record
		Decode(t, raw.Raw, &row)
		origin, census := commitment(raw.Raw, "object"), fields(t, raw)
		if row.Kind == "bead" {
			p.Entities = append(p.Entities, driver.SourcePlaneEntityV1{Source: driver.SourceEntityKey{Project: row.Project, Plane: row.Source.Table, ID: row.ID}, TargetKind: "bead", TargetID: row.ID, DispositionKey: "durable", PrincipalKey: "owner", Route: driver.SourcePlaneRouteV1{Project: row.Project, SourceID: row.ID, DurableTargetID: row.ID, Preference: "active-first"}, Origin: origin, Fields: census})
			if d.Fields == nil {
				for _, f := range census {
					rule := driver.SourcePlaneFieldRuleV1{Source: f.Name, Disposition: "origin-only"}
					switch f.Name {
					case "id", "title", "status", "metadata", "created_at", "priority", "description":
						rule.Target, rule.Disposition = f.Name, "exact-map"
					case "issue_type":
						rule.Target, rule.Disposition = "type", "exact-map"
					}
					d.Fields = append(d.Fields, rule)
				}
			}
		} else {
			project := row.Project
			if row.Source.Identity != nil {
				project = row.Source.Identity.Project
			}
			p.Origins = append(p.Origins, driver.SourcePlaneOriginV1{Project: project, Table: row.Source.Table, ID: row.ID, Action: "inert-origin", Raw: origin, Fields: census, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}})
		}
	}
	p.Dispositions = []driver.SourcePlaneDispositionV1{d}
	sidecars, err := archive.SourcePlaneSidecarOrigins()
	if err != nil {
		t.Fatal(err)
	}
	p.Origins = append(p.Origins, sidecars...)
	sort.Slice(p.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(p.Entities[i].Source, p.Entities[j].Source) < 0
	})
	sort.Slice(p.Origins, func(i, j int) bool {
		a, b := p.Origins[i], p.Origins[j]
		left, right := []string{a.Project, a.Table, a.ID, a.Path, a.Layer}, []string{b.Project, b.Table, b.ID, b.Path, b.Layer}
		for k := range left {
			if left[k] != right[k] {
				return left[k] < right[k]
			}
		}
		return false
	})
	return p
}
