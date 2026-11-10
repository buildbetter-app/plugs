package b09native

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func b09SourceBytes(data []byte, presence string) driver.SourcePlaneBytesV1 {
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: hashBytes(data)}
}

// Public fixture policy: these recordings contain durable tasks in one declared
// project. The owner and every mapping are explicit host inputs. Raw creator
// fields are conserved as origins and never grant target authority.
func bindB09SourcePlan(t *testing.T, data []byte, target *b09ReplayTarget, profile driver.RestoreImportProfile) driver.RestoreImportProfile {
	t.Helper()
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if archive.Manifest().SourcePlaneAddressContract == "" {
		return profile
	}
	if archive.Manifest().SourcePlaneAddressContract != driver.SourcePlaneAddressContractV1 || len(archive.Manifest().Projects) != 1 || archive.Manifest().Projects[0].Prefix != "b09" {
		t.Fatal("public fixture policy requires exactly the declared b09 source project")
	}
	ctx := context.Background()
	meta, err := target.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(target.store, target.lease)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := json.Marshal(profile.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	plan := driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1,
		SourceStoreID: archive.Manifest().StoreID, ArchiveSHA256: archive.SHA256(),
		Target:       driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: capability.WorkflowCheckpointLifetime(), RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Principals:   []driver.SourcePlanePrincipalV1{{Key: "owner", Source: "explicit public fixture owner", Authority: "trusted-explicit/1", PrincipalJSON: principal}},
		Providers:    []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: capability.WorkflowCheckpointLifetime(), OriginAdapter: "beads-workflow-origins/1"}},
		Dispositions: []driver.SourcePlaneDispositionV1{{Key: "durable", SourceType: "task", StorageClass: "", SinkKind: "bead", Schema: "bead/1", Lifetime: "core", Statuses: []driver.SourcePlaneStringPairV1{{From: "closed", To: "done"}, {From: "in_progress", To: "in_progress"}, {From: "open", To: "todo"}}, RefusalPolicy: "exact-only"}},
	}
	records, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		var row dumpformat.Record
		decoder := json.NewDecoder(bytes.NewReader(record.Raw))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			t.Fatal(err)
		}
		fields := b09SourceFields(t, record.Raw)
		origin := b09SourceBytes(record.Raw, "object")
		if record.Table == dumpformat.DurableEntityTable {
			storage, storagePresent := row.Source.Fields["storage_class"]
			if record.Project != "b09" || row.Source.Fields["issue_type"] != "task" || !plan.Dispositions[0].MatchesStorageClass(storage, storagePresent) || row.Source.Fields["ephemeral"] != json.Number("0") || row.Source.Fields["no_history"] != json.Number("0") {
				t.Fatal("unmapped entity outside the durable-task public fixture policy")
			}
			key := driver.SourceEntityKey{Project: record.Project, Plane: record.Table, ID: row.ID}
			plan.Entities = append(plan.Entities, driver.SourcePlaneEntityV1{Source: key, TargetKind: "bead", TargetID: key.ID, DispositionKey: "durable", PrincipalKey: "owner", Route: driver.SourcePlaneRouteV1{Project: key.Project, SourceID: key.ID, DurableTargetID: key.ID, Preference: "active-first"}, Origin: origin, Fields: fields})
			var rules []driver.SourcePlaneFieldRuleV1
			for _, field := range fields {
				rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
				switch field.Name {
				case "id", "title", "description", "priority", "status", "metadata", "created_at":
					rule.Target, rule.Disposition = field.Name, "exact-map"
				case "issue_type":
					rule.Target, rule.Disposition = "type", "exact-map"
				}
				rules = append(rules, rule)
			}
			if plan.Dispositions[0].Fields == nil {
				plan.Dispositions[0].Fields = rules
			} else if !reflect.DeepEqual(plan.Dispositions[0].Fields, rules) {
				t.Fatal("source field census differs within fixture disposition")
			}
			continue
		}
		if record.Kind == "edge" {
			fromID, fromOK := row.Source.Fields["issue_id"].(string)
			toID, toOK := row.Body["to"].(string)
			kind, kindOK := row.Body["type"].(string)
			if !fromOK || !toOK || !kindOK || record.Project != "b09" || record.Table != "dependencies" {
				t.Fatal("unmapped public fixture relation")
			}
			predicate, err := dumpformat.SourceEdgeMetadata(3, record.ID, kind, row.Source.Fields["metadata"])
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(predicate)
			if err != nil {
				t.Fatal(err)
			}
			to := driver.SourceEntityKey{Project: record.Project, Plane: dumpformat.DurableEntityTable, ID: toID}
			plan.Relations = append(plan.Relations, driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: record.Project, Table: record.Table, ID: record.ID}, From: driver.SourceEntityKey{Project: record.Project, Plane: dumpformat.DurableEntityTable, ID: fromID}, To: &to, Kind: kind, PredicateContract: "beads-typed-metadata/1", PredicateJSON: encoded, Origin: origin, Fields: fields})
			continue
		}
		if record.Table == dumpformat.EphemeralEntityTable {
			t.Fatal("public allocator fixture does not map active entities")
		}
		plan.Origins = append(plan.Origins, driver.SourcePlaneOriginV1{Project: record.Project, Table: record.Table, ID: record.ID, Action: "inert-origin", Raw: origin, Fields: fields, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}})
	}
	sidecars, err := archive.SourcePlaneSidecarOrigins()
	if err != nil {
		t.Fatal(err)
	}
	plan.Origins = append(plan.Origins, sidecars...)
	sort.Slice(plan.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Entities[i].Source, plan.Entities[j].Source) < 0
	})
	sort.Slice(plan.Relations, func(i, j int) bool {
		return driver.CompareSourceEdgeKeys(plan.Relations[i].Source, plan.Relations[j].Source) < 0
	})
	sort.Slice(plan.Origins, func(i, j int) bool {
		a, b := plan.Origins[i], plan.Origins[j]
		left, right := []string{a.Project, a.Table, a.ID, a.Path, a.Layer}, []string{b.Project, b.Table, b.ID, b.Path, b.Layer}
		for k := range left {
			if left[k] != right[k] {
				return left[k] < right[k]
			}
		}
		return false
	})
	// Joint projection includes scoped source mirror identities. The unbound
	// inspection digest is retained separately; this verifies the actual plan.
	profile.DerivedDigest = ""
	profile, err = converterroundtrip.BindSourceImport(ctx, bytes.NewReader(data), profile, plan)
	if err != nil {
		t.Fatal("complete explicit public fixture source plan", err)
	}
	target.sourcePlan = profile.SourcePlanes
	(&recorder{t: t}).writeJSON(target.output+"/source-import-profile.json", profile)
	return profile
}

func b09SourceFields(t *testing.T, raw []byte) []driver.SourcePlaneFieldV1 {
	t.Helper()
	var wire struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	var fields []driver.SourcePlaneFieldV1
	for key, data := range wire.Source.Fields {
		presence := "null"
		switch bytes.TrimSpace(data)[0] {
		case '{':
			presence = "object"
		case '[':
			presence = "array"
		case '"':
			presence = "text"
			var value string
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			data = []byte(value)
		case 't', 'f':
			presence = "boolean"
		case 'n':
		default:
			presence = "number"
		}
		fields = append(fields, driver.SourcePlaneFieldV1{Name: key, Value: b09SourceBytes(data, presence)})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return fields
}

// Derive the immutable commit record from the admitted plan and independently
// loaded checkpoint bytes, including its exact initial origin and logical hash.
func b09ExpectedSourceCommit(t *testing.T, profile driver.RestoreImportProfile, stage b09ReplayStage) driver.SourcePlaneCommitBindingV1 {
	t.Helper()
	plan, err := profile.SourcePlanes.Plan()
	if err != nil {
		t.Fatal(err)
	}
	request, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	binding := driver.SourcePlaneCommitBindingV1{Version: driver.SourcePlaneCommitVersionV1, JointPlanDigest: profile.SourcePlanes.JointPlanDigest, RequestHash: request, StoreID: stage.Meta.StoreID, Incarnation: stage.Meta.Incarnation, Revision: stage.Meta.Head}
	if len(stage.Checkpoints) != len(plan.Providers) {
		t.Fatal("source checkpoint census differs from plan")
	}
	for _, provider := range plan.Providers {
		cp, ok := stage.Checkpoints[provider.Namespace]
		if !ok {
			t.Fatal("source checkpoint missing")
		}
		entry := driver.SourcePlaneProviderCommitV1{Provider: provider, Origin: driver.SourcePlaneOriginBindingV1{Version: driver.SourcePlaneOriginVersionV1, JointPlanDigest: binding.JointPlanDigest, RequestHash: request, StoreID: binding.StoreID, Incarnation: binding.Incarnation, Namespace: provider.Namespace, InitialGeneration: provider.ExpectedGeneration + 1, LogicalImageSHA256: provider.LogicalImageSHA256, Lifetime: provider.Lifetime}, EnvelopeSHA256: hashBytes(cp.Payload)}
		if err := driver.ValidateSourcePlaneInitialCheckpoint(cp, entry); err != nil {
			t.Fatal(err)
		}
		binding.Providers = append(binding.Providers, entry)
	}
	if err := binding.Validate(profile, stage.Meta); err != nil {
		t.Fatal(err)
	}
	return binding
}
