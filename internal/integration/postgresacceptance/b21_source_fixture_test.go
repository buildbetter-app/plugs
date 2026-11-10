package postgresacceptance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// These explicit input records exercise the private provider contract. They
// are test data, not an admitted producer archive or native qualification.
type b21SourceFixture struct {
	plan driver.SourcePlanePlanV1
	raw  []driver.SourcePlaneRecordV1
}

var b21SourceActor = model.PrincipalRef{Kind: "user", ID: "source-owner"}

func b21Bytes(data []byte, presence string) driver.SourcePlaneBytesV1 {
	h := sha256.Sum256(data)
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: hex.EncodeToString(h[:])}
}

func b21Record(t *testing.T, kind, table, id string, fields map[string]any) (driver.SourcePlaneRecordV1, driver.SourcePlaneBytesV1, []driver.SourcePlaneFieldV1) {
	t.Helper()
	wireProject := project
	if kind == "beads-archive" {
		wireProject = ""
	}
	data, err := json.Marshal(map[string]any{"kind": kind, "id": id, "project": wireProject, "body": map[string]any{"fixture": true}, "source": map[string]any{"table": table, "fields": fields}})
	if err != nil {
		t.Fatal(err)
	}
	// Retain significant raw formatting as well as numbers and string bytes.
	data = append([]byte(" "), data...)
	commitments := []driver.SourcePlaneFieldV1{}
	for key, value := range fields {
		var data []byte
		presence := "object"
		if text, ok := value.(string); ok {
			data, presence = []byte(text), "text"
		} else {
			data, err = json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			switch value.(type) {
			case nil:
				presence = "null"
			case bool:
				presence = "boolean"
			case json.Number, int:
				presence = "number"
			}
		}
		commitments = append(commitments, driver.SourcePlaneFieldV1{Name: key, Value: b21Bytes(data, presence)})
	}
	sort.Slice(commitments, func(i, j int) bool { return commitments[i].Name < commitments[j].Name })
	return driver.SourcePlaneRecordV1{Kind: kind, Project: wireProject, Table: table, ID: id, Raw: data}, b21Bytes(data, "object"), commitments
}

func b21Fixture(t *testing.T, meta driver.Meta, lifetime driver.CheckpointLifetime) b21SourceFixture {
	t.Helper()
	const same = "same-id"
	core := driver.SourceEntityKey{Project: project, Plane: dumpformat.DurableEntityTable, ID: same}
	parent := driver.SourceEntityKey{Project: project, Plane: dumpformat.EphemeralEntityTable, ID: same}
	child := driver.SourceEntityKey{Project: project, Plane: dumpformat.EphemeralEntityTable, ID: "child"}
	principal, _ := json.Marshal(b21SourceActor)
	f := b21SourceFixture{plan: driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: "b21-source", ArchiveSHA256: strings.Repeat("a", 64),
		Target:     driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: lifetime, RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Principals: []driver.SourcePlanePrincipalV1{{Key: "owner", Source: "explicit-test-owner", Authority: "trusted-explicit/1", PrincipalJSON: principal}},
		Runs: []driver.SourcePlaneRunV1{
			{Project: project, Key: "child", TargetID: "workflow:run/source-child", Template: "declared", Schema: workflowplugin.SourceStateSchema, Mode: "ephemeral", State: "active", PrincipalKey: "owner", Members: []driver.SourceEntityKey{child}},
			{Project: project, Key: "parent", TargetID: "workflow:run/source-parent", Template: "declared", Schema: workflowplugin.SourceStateSchema, Mode: "ephemeral", State: "active", PrincipalKey: "owner", Members: []driver.SourceEntityKey{parent}},
		},
		Providers: []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, LogicalImageSHA256: strings.Repeat("0", 64), OriginAdapter: "beads-workflow-origins/1"}},
	}}
	rules := []driver.SourcePlaneFieldRuleV1{}
	for _, field := range []string{"created_at", "ephemeral", "id", "issue_type", "metadata", "no_history", "status", "storage_class", "title"} {
		target := field
		disposition := "exact-map"
		switch field {
		case "issue_type":
			target = "type"
		case "ephemeral", "no_history", "storage_class":
			target = ""
			disposition = "origin-only"
		}
		rules = append(rules, driver.SourcePlaneFieldRuleV1{Source: field, Target: target, Disposition: disposition})
	}
	f.plan.Dispositions = []driver.SourcePlaneDispositionV1{
		{Key: "active", SourceType: "task", StorageClass: "ephemeral", Ephemeral: true, NoHistory: true, SinkKind: "workflow:item", Schema: workflowplugin.SourceStateSchema, Lifetime: string(lifetime), Statuses: []driver.SourcePlaneStringPairV1{{From: "open", To: "open"}}, Fields: rules, RefusalPolicy: "exact-only"},
		{Key: "durable", SourceType: "task", StorageClass: "durable", SinkKind: "bead", Schema: "bead/1", Lifetime: "core", RefusalPolicy: "exact-only"},
	}
	for _, key := range []driver.SourceEntityKey{core, child, parent} {
		active := key.Plane == dumpformat.EphemeralEntityTable
		storage := "durable"
		if active {
			storage = "ephemeral"
		}
		fields := map[string]any{"id": key.ID, "title": "source " + key.Plane + " " + key.ID, "issue_type": "task", "status": "open", "storage_class": storage, "ephemeral": active, "no_history": active, "created_at": "2026-11-01T12:00:00Z", "metadata": ` {"large":9007199254740993,"negative":-9007199254740993,"nested":[null,"9007199254740993"]} `}
		kind, id := "issue", key.ID
		if active {
			kind, id = "beads-archive", dumpformat.EphemeralEntityTable+"/"+key.ID
		}
		record, origin, census := b21Record(t, kind, key.Plane, id, fields)
		entity := driver.SourcePlaneEntityV1{Source: key, TargetKind: "bead", TargetID: "pl-source-shadow", DispositionKey: "durable", PrincipalKey: "owner", Origin: origin, Fields: census}
		if active {
			entity.TargetKind = "workflow:item"
			entity.DispositionKey = "active"
			entity.RunKey = "parent"
			entity.TargetID = "workflow:item/source-parent"
			if key.ID == "child" {
				entity.RunKey = "child"
				entity.TargetID = "workflow:item/source-child"
			}
		}
		entity.Route = driver.SourcePlaneRouteV1{Project: project, SourceID: key.ID, ActiveTargetID: "workflow:item/source-parent", DurableTargetID: "pl-source-shadow", Preference: "active-first"}
		if key.ID == "child" {
			entity.Route.ActiveTargetID = "workflow:item/source-child"
			entity.Route.DurableTargetID = ""
		}
		f.plan.Entities = append(f.plan.Entities, entity)
		f.raw = append(f.raw, record)
	}
	addEdge := func(table, id, kind string, from, to driver.SourceEntityKey) {
		fields := map[string]any{"id": id, "issue_id": from.ID, "depends_on_issue_id": nil, dumpformat.EphemeralDependencyTargetField: nil, "depends_on_external": nil, "type": kind, "created_at": "2026-11-01T12:00:00Z", "created_by": "unverified-author", "metadata": nil, "thread_id": "raw-thread"}
		arm := "depends_on_issue_id"
		if to.Plane == dumpformat.EphemeralEntityTable {
			arm = dumpformat.EphemeralDependencyTargetField
		}
		fields[arm] = to.ID
		record, origin, census := b21Record(t, "edge", table, id, fields)
		f.raw = append(f.raw, record)
		f.plan.Relations = append(f.plan.Relations, driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: project, Table: table, ID: id}, From: from, To: &to, Kind: kind, PredicateContract: "beads-typed-metadata/1", PredicateJSON: []byte("null"), Origin: origin, Fields: census})
	}
	addEdge(dumpformat.DurableDependencyTable, "same-edge", "related", core, parent)
	addEdge(dumpformat.EphemeralDependencyTable, "block-child", "blocks", child, parent)
	addEdge(dumpformat.EphemeralDependencyTable, "same-edge", "related", parent, core)
	for _, aux := range []struct {
		table, id string
		fields    map[string]any
	}{
		{dumpformat.EphemeralChildCounterTable, "counter", map[string]any{"parent_id": same, "last_child": 13}},
		{dumpformat.EphemeralCommentTable, "comment", map[string]any{"id": "physical-comment", "issue_id": same, "text": "original body", "author": "unverified-author", "created_at": "2026-11-01T12:00:00Z"}},
		{dumpformat.EphemeralLabelTable, "label", map[string]any{"issue_id": same, "label": "source-label"}},
	} {
		record, origin, census := b21Record(t, "beads-archive", aux.table, aux.table+"/"+aux.id, aux.fields)
		f.raw = append(f.raw, record)
		f.plan.Origins = append(f.plan.Origins, driver.SourcePlaneOriginV1{Project: project, Table: aux.table, ID: record.ID, Action: "exact-map", Owner: &parent, Raw: origin, Fields: census, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}})
	}
	if err := f.plan.Validate(); err != nil {
		t.Fatal(err)
	}
	return f
}
