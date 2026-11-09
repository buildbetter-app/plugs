package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const sourcePlaneFixtureActiveEventTable = "wisp_events"

func sourcePlaneTestBytes(data string, presence string) SourcePlaneBytesV1 {
	h := sha256.Sum256([]byte(data))
	return SourcePlaneBytesV1{Presence: presence, Size: uint64(len(data)), SHA256: hex.EncodeToString(h[:])}
}

func sourcePlaneTestPlan() SourcePlanePlanV1 {
	durable := SourceEntityKey{Project: "p", Plane: "issues", ID: "same-id"}
	active := SourceEntityKey{Project: "p", Plane: "wisps", ID: "same-id"}
	route := SourcePlaneRouteV1{Project: "p", SourceID: "same-id", ActiveTargetID: "workflow:item/imported", DurableTargetID: "durable-id", Preference: "active-first"}
	origin := sourcePlaneTestBytes("{\"exact\":9007199254740993}", "object")
	field := []SourcePlaneFieldV1{{Name: "metadata", Value: sourcePlaneTestBytes(" {\"gate\":\"\"} ", "text")}}
	return SourcePlanePlanV1{
		Version: SourcePlanePlanVersionV1, AddressContract: SourcePlaneAddressContractV1, SourceStoreID: "source", ArchiveSHA256: strings.Repeat("a", 64),
		Target: SourcePlaneTargetV1{StoreID: "target", Incarnation: "old-incarnation", RequireEmpty: true, Lifetime: CheckpointLifetimeProcess, RuntimeContract: SourcePlaneRuntimeContractV1},
		Dispositions: []SourcePlaneDispositionV1{
			{Key: "durable", SourceType: "task", StorageClass: "durable", SinkKind: "bead", Schema: "bead/1", Lifetime: "core", Statuses: []SourcePlaneStringPairV1{{From: "open", To: "open"}}, RefusalPolicy: "exact-only"},
			{Key: "operational", SourceType: "task", StorageClass: "ephemeral", Ephemeral: true, NoHistory: true, SinkKind: "workflow:item", Schema: "workflow/source-state/1", Lifetime: "process", Statuses: []SourcePlaneStringPairV1{{From: "open", To: "open"}}, RefusalPolicy: "exact-only"},
		},
		Principals: []SourcePlanePrincipalV1{{Key: "owner", Source: "source-author", Authority: "trusted-explicit/1", PrincipalJSON: []byte(`{"kind":"user","id":"owner"}`)}},
		Runs:       []SourcePlaneRunV1{{Project: "p", Key: "run", TargetID: "workflow:run/imported", Template: "explicit", Schema: "workflow/source-state/1", Mode: "ephemeral", State: "active", PrincipalKey: "owner", Members: []SourceEntityKey{active}}},
		Entities: []SourcePlaneEntityV1{
			{Source: durable, TargetKind: "bead", TargetID: "durable-id", DispositionKey: "durable", PrincipalKey: "owner", Route: route, Origin: origin, Fields: field},
			{Source: active, TargetKind: "workflow:item", TargetID: "workflow:item/imported", DispositionKey: "operational", PrincipalKey: "owner", RunKey: "run", Route: route, Origin: origin, Fields: field},
		},
		Relations: []SourcePlaneRelationV1{
			{Source: SourceEdgeKey{Project: "p", Table: "dependencies", ID: "same-edge"}, From: durable, To: &active, Kind: "relates-to", PredicateContract: "source-metadata/1", PredicateJSON: []byte(`{}`), Origin: origin, Fields: field},
			{Source: SourceEdgeKey{Project: "p", Table: sourcePlaneEphemeralDependencyTable, ID: "same-edge"}, From: active, To: &durable, Kind: "relates-to", PredicateContract: "source-metadata/1", PredicateJSON: []byte(`{}`), Origin: origin, Fields: field},
		},
		Origins:   []SourcePlaneOriginV1{{Project: "p", Table: sourcePlaneFixtureActiveEventTable, ID: "history", Action: "inert-origin", Owner: &active, Raw: origin, Parsed: SourcePlaneBytesV1{Presence: "absent"}, Precedence: []string{"stored", "yaml"}}},
		Providers: []SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: SourcePlaneRuntimeContractV1, Lifetime: CheckpointLifetimeProcess, LogicalImageSize: 2, LogicalImageSHA256: sourcePlaneTestBytes("{}", "object").SHA256, OriginAdapter: "beads-workflow-origins/1"}},
	}
}

func sourcePlaneTestProfile() RestoreImportProfile {
	return RestoreImportProfile{Admission: RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("b", 64)}, SourceStoreID: "source", Method: "offline.restore.import", IdempotencyKey: "import-source-planes-1", ArchiveDigest: strings.Repeat("a", 64), DerivedDigest: strings.Repeat("c", 64)}
}

func TestSourcePlaneCanonicalPlanExactBindingAndDetachedBytes(t *testing.T) {
	plan := sourcePlaneTestPlan()
	binding, err := NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	got, err := binding.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if got.Entities[0].Source.ID != got.Entities[1].Source.ID || got.Relations[0].Source.ID != got.Relations[1].Source.ID || got.Relations[0].Source.Table == got.Relations[1].Source.Table {
		t.Fatal("physical cross-plane identities lost")
	}
	if got.Entities[0].Origin != plan.Entities[0].Origin || got.Entities[0].Fields[0].Value != plan.Entities[0].Fields[0].Value || !bytes.Equal(got.Relations[0].PredicateJSON, plan.Relations[0].PredicateJSON) {
		t.Fatal("raw commitments/predicate changed")
	}
	var input bytes.Buffer
	for _, part := range [][]byte{[]byte(sourcePlaneDigestDomain), binding.CanonicalPlan} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		input.Write(n[:])
		input.Write(part)
	}
	want := sha256.Sum256(input.Bytes())
	if binding.JointPlanDigest != hex.EncodeToString(want[:]) {
		t.Fatal("digest domain or framing differs from protocol")
	}
	detached, err := binding.Detached()
	if err != nil {
		t.Fatal(err)
	}
	binding.CanonicalPlan[0] ^= 1
	if _, err := detached.Plan(); err != nil {
		t.Fatalf("detached plan aliased caller: %v", err)
	}
	if _, err := binding.Plan(); err == nil {
		t.Fatal("tampered canonical bytes accepted")
	}
	got.Relations[0].PredicateJSON[0] = '!'
	if _, err := detached.Plan(); err != nil {
		t.Fatal("decoded plan aliased immutable bytes")
	}
}

func TestSourcePlaneCanonicalPlanBoundsAndRefusals(t *testing.T) {
	data, err := EncodeSourcePlanePlanV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		if _, err := DecodeSourcePlanePlanV1(data[:i]); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(data), 0), bytes.Repeat([]byte{255}, 8), nil} {
		if _, err := DecodeSourcePlanePlanV1(bad); err == nil {
			t.Fatal("accepted trailing/oversized/empty bytes")
		}
	}
	// Reach the first set count after the four top-level strings and target.
	var prefix bytes.Buffer
	p := sourcePlaneTestPlan()
	for _, s := range []string{p.Version, p.AddressContract, p.SourceStoreID, p.ArchiveSHA256, p.Target.StoreID, p.Target.Incarnation} {
		_ = writeSourcePlaneBytes(&prefix, []byte(s))
	}
	prefix.Write(make([]byte, 8))
	prefix.WriteByte(1)
	_ = writeSourcePlaneBytes(&prefix, []byte(p.Target.Lifetime))
	_ = writeSourcePlaneBytes(&prefix, []byte(p.Target.RuntimeContract))
	bad := bytes.Clone(data)
	binary.BigEndian.PutUint64(bad[prefix.Len():], ^uint64(0))
	if _, err := DecodeSourcePlanePlanV1(bad); err == nil {
		t.Fatal("oversized set count accepted")
	}
	cases := map[string]func(*SourcePlanePlanV1){
		"unknown version":      func(p *SourcePlanePlanV1) { p.Version = "future" },
		"unsorted entities":    func(p *SourcePlanePlanV1) { p.Entities[0], p.Entities[1] = p.Entities[1], p.Entities[0] },
		"duplicate edge tuple": func(p *SourcePlanePlanV1) { p.Relations[1].Source = p.Relations[0].Source },
		"dangling local arm": func(p *SourcePlanePlanV1) {
			p.Relations[0].To = &SourceEntityKey{Project: "p", Plane: "wisps", ID: "missing"}
		},
		"multi arm":          func(p *SourcePlanePlanV1) { s := "external"; p.Relations[0].External = &s },
		"wrong source plane": func(p *SourcePlanePlanV1) { p.Relations[0].From = p.Entities[1].Source },
		"noninjective target": func(p *SourcePlanePlanV1) {
			p.Entities = append(p.Entities, p.Entities[1])
			p.Entities[2].Source.ID = "z"
		},
		"missing principal": func(p *SourcePlanePlanV1) { p.Entities[1].PrincipalKey = "missing" },
		"unverified principal": func(p *SourcePlanePlanV1) {
			p.Principals[0].UnverifiedOriginOnly = true
			p.Principals[0].PrincipalJSON = nil
		},
		"unknown raw tag":           func(p *SourcePlanePlanV1) { p.Entities[1].Origin.Presence = "unknown" },
		"raw absent alias":          func(p *SourcePlanePlanV1) { p.Entities[1].Origin.Presence = "absent" },
		"unknown provider lifetime": func(p *SourcePlanePlanV1) { p.Providers[0].Lifetime = "forever" },
		"duplicate namespace":       func(p *SourcePlanePlanV1) { p.Providers = append(p.Providers, p.Providers[0]) },
		"nonzero generation":        func(p *SourcePlanePlanV1) { p.Providers[0].ExpectedGeneration = 1 },
		"invalid UTF8":              func(p *SourcePlanePlanV1) { p.SourceStoreID = string([]byte{255}) },
		"duplicate null storage tuple": func(p *SourcePlanePlanV1) {
			p.Dispositions[0].StorageClass = ""
			p.Dispositions[1].StorageClass = ""
			p.Dispositions[1].Ephemeral, p.Dispositions[1].NoHistory = false, false
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			p := sourcePlaneTestPlan()
			change(&p)
			_, err := EncodeSourcePlanePlanV1(p)
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("refusal=%v", err)
			}
		})
	}
}

func TestSourcePlaneRequestHashBindsEveryLogicalComponent(t *testing.T) {
	profile := sourcePlaneTestProfile()
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	profile.SourcePlanes = &binding
	baseline, err := profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	changes := map[string]func(*SourcePlanePlanV1){
		"target precondition": func(p *SourcePlanePlanV1) { p.Target.Incarnation = "other" },
		"disposition":         func(p *SourcePlanePlanV1) { p.Dispositions[0].RefusalPolicy = "exact-only/2" },
		"null storage class":  func(p *SourcePlanePlanV1) { p.Dispositions[0].StorageClass = "" },
		"status":              func(p *SourcePlanePlanV1) { p.Dispositions[0].Statuses[0].To = "in-progress" },
		"principal":           func(p *SourcePlanePlanV1) { p.Principals[0].PrincipalJSON = []byte(`{"kind":"user","id":"other"}`) },
		"run":                 func(p *SourcePlanePlanV1) { p.Runs[0].Template = "other" },
		"entity origin":       func(p *SourcePlanePlanV1) { p.Entities[0].Origin = sourcePlaneTestBytes("other", "text") },
		"entity field raw null": func(p *SourcePlanePlanV1) {
			p.Entities[0].Fields = []SourcePlaneFieldV1{{Name: "metadata", Value: sourcePlaneTestBytes("null", "null")}}
		},
		"relation":           func(p *SourcePlanePlanV1) { p.Relations[0].Kind = "tracks" },
		"predicate":          func(p *SourcePlanePlanV1) { p.Relations[0].PredicateJSON = []byte(`{"gate":""}`) },
		"sidecar":            func(p *SourcePlanePlanV1) { p.Origins[0].Raw = sourcePlaneTestBytes("other", "bytes") },
		"precedence":         func(p *SourcePlanePlanV1) { p.Origins[0].Precedence = []string{"yaml", "stored"} },
		"provider namespace": func(p *SourcePlanePlanV1) { p.Providers[0].Namespace = "plugin/workflow/other/durable" },
		"provider image":     func(p *SourcePlanePlanV1) { p.Providers[0].LogicalImageSHA256 = strings.Repeat("d", 64) },
		"provider schema":    func(p *SourcePlanePlanV1) { p.Providers[0].Schema = "workflow/source-state/2" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			p := sourcePlaneTestPlan()
			change(&p)
			b, err := NewSourcePlaneImportBindingV1(p)
			if err != nil {
				t.Fatal(err)
			}
			candidate := profile
			candidate.SourcePlanes = &b
			got, err := candidate.RequestHash()
			if err != nil || got == baseline {
				t.Fatalf("changed-plan identity=%s err=%v", got, err)
			}
			if candidate.DerivedDigest != profile.DerivedDigest || candidate.ArchiveDigest != profile.ArchiveDigest {
				t.Fatal("core/archive identity repurposed")
			}
		})
	}
}

func TestSourcePlaneProfileLegacyAbsenceAndStrictPresence(t *testing.T) {
	p := sourcePlaneTestProfile()
	detached, err := p.Detached()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(detached)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("sourcePlanes")) {
		t.Fatal("legacy bytes gained binding")
	}
	want := sha256.Sum256(data)
	got, err := p.RequestHash()
	if err != nil || got != hex.EncodeToString(want[:]) {
		t.Fatal("legacy RequestHash changed")
	}
	for _, field := range []string{`null`, `{}`, `{"version":"future","canonicalPlan":"","jointPlanDigest":""}`} {
		var read RestoreImportProfile
		bad := append(bytes.Clone(data[:len(data)-1]), []byte(`,"sourcePlanes":`+field+`}`)...)
		if json.Unmarshal(bad, &read) == nil {
			t.Fatalf("accepted explicit %s", field)
		}
	}
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	p.SourcePlanes = &binding
	data, err = json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, replace := range []struct{ old, new string }{{`"jointPlanDigest":`, `"unknown":`}, {`"version":`, `"version":"beads-joint-import-plan/1","version":`}} {
		var read RestoreImportProfile
		if json.Unmarshal(bytes.Replace(data, []byte(replace.old), []byte(replace.new), 1), &read) == nil {
			t.Fatal("accepted duplicate/unknown binding field")
		}
	}
	detached, err = p.Detached()
	if err != nil {
		t.Fatal(err)
	}
	p.SourcePlanes.CanonicalPlan[0] ^= 1
	if _, err := detached.RequestHash(); err != nil {
		t.Fatal("detached profile aliases caller plan")
	}
}

func TestSourcePlaneComponentOrderingHasNoDelimiterAliases(t *testing.T) {
	a := SourceEdgeKey{Project: "a", Table: sourcePlaneEphemeralDependencyTable, ID: "z"}
	b := SourceEdgeKey{Project: "a\x00" + sourcePlaneEphemeralDependencyTable, Table: "dependencies", ID: "z"}
	if CompareSourceEdgeKeys(a, b) >= 0 || CompareSourceEdgeKeys(a, a) != 0 {
		t.Fatal("tuple order collapsed components")
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	c := a
	c.Table = "dependencies"
	if CompareSourceEdgeKeys(c, a) >= 0 {
		t.Fatal("table component not ordered before physical ID")
	}
}

func TestSourcePlaneRouteAlternativesRequireExactSourceInverse(t *testing.T) {
	// The same-ID cross-plane route in the complete plan is the positive
	// control. Each negative keeps all target refs globally available.
	if _, err := EncodeSourcePlanePlanV1(sourcePlaneTestPlan()); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"different-id", "different-project"} {
		t.Run(scope, func(t *testing.T) {
			p := sourcePlaneTestPlan()
			other := p.Entities[0]
			other.Source.ID = "z-other"
			other.TargetID = "other-durable"
			other.Route = SourcePlaneRouteV1{Project: "p", SourceID: "z-other", DurableTargetID: other.TargetID, Preference: "active-first"}
			if scope == "different-project" {
				other.Source.Project = "z"
				other.Source.ID = "same-id"
				other.Route.Project = "z"
				other.Route.SourceID = "same-id"
			}
			p.Entities = append(p.Entities, other)
			for i := 0; i < 2; i++ {
				p.Entities[i].Route.DurableTargetID = other.TargetID
			}
			// Remove the original durable entity so its own selected route arm is
			// valid; the active entity's alternate points at the unrelated target.
			p.Entities = p.Entities[1:]
			p.Relations = nil
			sort.Slice(p.Entities, func(i, j int) bool { return CompareSourceEntityKeys(p.Entities[i].Source, p.Entities[j].Source) < 0 })
			if _, err := EncodeSourcePlanePlanV1(p); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("unrelated alternate accepted: %v", err)
			}
		})
	}
}

func TestSourcePlaneFoundationRefusesCoreOnlyExecution(t *testing.T) {
	p := sourcePlaneTestProfile()
	if _, err := p.DetachedCoreOnly(); err != nil {
		t.Fatal("legacy nil profile refused", err)
	}
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	p.SourcePlanes = &binding
	if _, err := p.Detached(); err != nil {
		t.Fatal("checked preparation profile refused", err)
	}
	if _, err := p.DetachedCoreOnly(); !errors.Is(err, ErrUnsupported) {
		t.Fatal("core-only execution accepted joint plan", err)
	}
}
