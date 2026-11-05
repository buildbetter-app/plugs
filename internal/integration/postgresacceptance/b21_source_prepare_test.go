package postgresacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func b21Logical(t *testing.T, f *b21SourceFixture) []byte {
	t.Helper()
	prepared, err := (&workflowplugin.Provider{}).PrepareSourceRestore(context.Background(), f.plan, f.raw)
	if err != nil {
		t.Fatal(err)
	}
	logical := prepared.LogicalImage()
	f.plan.Providers[0].LogicalImageSHA256 = prepared.SHA256()
	f.plan.Providers[0].LogicalImageSize = uint64(len(logical))
	return logical
}

func TestB21SourcePreparationExactOriginsAndNumbers(t *testing.T) {
	f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
	wantRaw := append([]driver.SourcePlaneRecordV1(nil), f.raw...)
	logical := b21Logical(t, &f)
	// The image does not contain its own final commitment, new incarnation or
	// a clock-derived ID. Finalizing the joint plan cannot create a hash cycle.
	prepared, err := (&workflowplugin.Provider{}).PrepareSourceRestore(context.Background(), f.plan, f.raw)
	if err != nil || !bytes.Equal(logical, prepared.LogicalImage()) {
		t.Fatalf("final commitment changed image: %v", err)
	}
	for i, j := 0, len(f.raw)-1; i < j; i, j = i+1, j-1 {
		f.raw[i], f.raw[j] = f.raw[j], f.raw[i]
	}
	again, err := (&workflowplugin.Provider{}).PrepareSourceRestore(context.Background(), f.plan, f.raw)
	if err != nil || !bytes.Equal(logical, again.LogicalImage()) {
		t.Fatalf("file traversal changed logical bytes: %v", err)
	}
	copy := prepared.LogicalImage()
	copy[0] = '!'
	if !bytes.Equal(logical, prepared.LogicalImage()) {
		t.Fatal("logical accessor aliased bytes")
	}
	var image struct {
		Items  map[string]workflowplugin.ItemView `json:"items"`
		Source struct {
			Records   []driver.SourcePlaneRecordV1   `json:"records"`
			Relations []driver.SourcePlaneRelationV1 `json:"relations"`
		} `json:"source"`
	}
	decoder := json.NewDecoder(bytes.NewReader(logical))
	decoder.UseNumber()
	if err := decoder.Decode(&image); err != nil {
		t.Fatal(err)
	}
	parent := image.Items["workflow:item/source-parent"]
	if parent.ID != "workflow:item/source-parent" || parent.Title != "source "+dumpformat.EphemeralEntityTable+" same-id" || parent.Metadata["large"] != json.Number("9007199254740993") || parent.Metadata["negative"] != json.Number("-9007199254740993") || !reflect.DeepEqual(parent.Metadata["nested"], []any{nil, "9007199254740993"}) {
		t.Fatalf("active raw mapping lost: %+v", parent)
	}
	// Stored item schema and public ItemView use different counter spellings;
	// verify the physical logical image independently of the public decoder.
	var stored map[string]json.RawMessage
	if err := json.Unmarshal(logical, &stored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(stored["items"], []byte(`"childCounter":13`)) || !bytes.Contains(stored["items"], []byte(`"source-label"`)) {
		t.Fatal("auxiliary mapping absent")
	}
	if len(parent.Comments) != 1 || parent.Comments[0].Source == nil || parent.Comments[0].Source.ID != "physical-comment" || parent.Comments[0].Source.Author != "unverified-author" || parent.Comments[0].Principal.Kind != "" {
		t.Fatalf("source comment granted authority or lost origin: %+v", parent.Comments)
	}
	comment, _ := json.Marshal(parent.Comments[0])
	if bytes.Contains(comment, []byte(`"principal"`)) {
		t.Fatal("unverified author serialized as authenticated principal")
	}
	if len(image.Source.Records) != len(wantRaw) || len(image.Source.Relations) != 3 {
		t.Fatal("full physical census lost")
	}
	for _, raw := range wantRaw {
		found := false
		for _, retained := range image.Source.Records {
			if reflect.DeepEqual(raw, retained) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("original record bytes missing: %s/%s", raw.Table, raw.ID)
		}
	}
	if image.Source.Relations[0].Source.ID != image.Source.Relations[2].Source.ID || image.Source.Relations[0].Source.Table == image.Source.Relations[2].Source.Table {
		t.Fatal("equal physical edge IDs collapsed across planes")
	}
}

func b21ChangeEdge(t *testing.T, f *b21SourceFixture, index int, kind string) {
	t.Helper()
	r := &f.plan.Relations[index]
	for i, raw := range f.raw {
		if b21Bytes(raw.Raw, "object").SHA256 != r.Origin.SHA256 {
			continue
		}
		var wire struct {
			Source struct {
				Fields map[string]any `json:"fields"`
			} `json:"source"`
		}
		d := json.NewDecoder(bytes.NewReader(raw.Raw))
		d.UseNumber()
		if err := d.Decode(&wire); err != nil {
			t.Fatal(err)
		}
		wire.Source.Fields["type"] = kind
		record, origin, census := b21Record(t, raw.Kind, raw.Table, raw.ID, wire.Source.Fields)
		f.raw[i] = record
		r.Origin = origin
		r.Fields = census
		r.Kind = kind
		return
	}
	t.Fatal("missing edge fixture")
}

func TestB21SourcePreparationTypedMixedParents(t *testing.T) {
	f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
	b21ChangeEdge(t, &f, 0, "parent-child") // ordinary child -> active parent
	b21ChangeEdge(t, &f, 1, "parent-child") // active child -> active parent
	logical := b21Logical(t, &f)
	var image struct {
		Items map[string]struct {
			ID      string                        `json:"id"`
			Parent  *model.Ref                    `json:"parent"`
			Inbound []workflowplugin.RelationView `json:"inbound"`
		} `json:"items"`
	}
	if err := json.Unmarshal(logical, &image); err != nil {
		t.Fatal(err)
	}
	child := image.Items["workflow:item/source-child"]
	if child.Parent == nil || child.Parent.ID != "workflow:item/source-parent" || !child.Parent.Branch.IsZero() {
		t.Fatalf("typed parent was rewritten or lost: %+v", child.Parent)
	}
	parent := image.Items["workflow:item/source-parent"]
	found := false
	for _, link := range parent.Inbound {
		if link.From == "pl-source-shadow" && link.To.ID == parent.ID && link.Kind == "parent-child" {
			found = true
		}
	}
	if !found {
		t.Fatal("ordinary child's workflow parent was lost from the mixed index")
	}
}

func TestB21SourcePreparationCausalRefusals(t *testing.T) {
	changes := map[string]func(*b21SourceFixture){
		"missing original":      func(f *b21SourceFixture) { f.raw = f.raw[1:] },
		"DTO mirror":            func(f *b21SourceFixture) { f.raw[0].Table = dumpformat.EphemeralEntityTable },
		"trailing record":       func(f *b21SourceFixture) { f.raw[0].Raw = append(f.raw[0].Raw, []byte(` {}`)...) },
		"field commitment":      func(f *b21SourceFixture) { f.plan.Entities[1].Fields[0].Value = b21Bytes([]byte("wrong"), "text") },
		"missing field rule":    func(f *b21SourceFixture) { f.plan.Dispositions[0].Fields = f.plan.Dispositions[0].Fields[1:] },
		"required unsupported":  func(f *b21SourceFixture) { f.plan.Dispositions[0].Fields[0].Disposition = "refuse-required" },
		"status correspondence": func(f *b21SourceFixture) { f.plan.Dispositions[0].Statuses[0].To = "in-progress" },
		"lifetime":              func(f *b21SourceFixture) { f.plan.Dispositions[0].Lifetime = "restart" },
		"unverified principal": func(f *b21SourceFixture) {
			f.plan.Principals[0].UnverifiedOriginOnly = true
			f.plan.Principals[0].PrincipalJSON = nil
		},
		"run membership":         func(f *b21SourceFixture) { f.plan.Runs[0].Members[0] = f.plan.Entities[0].Source },
		"TEN field census":       func(f *b21SourceFixture) { f.plan.Relations[0].Fields = f.plan.Relations[0].Fields[1:] },
		"predicate mismatch":     func(f *b21SourceFixture) { f.plan.Relations[0].PredicateJSON = []byte(`{}`) },
		"auxiliary owner":        func(f *b21SourceFixture) { f.plan.Origins[0].Owner = &f.plan.Entities[1].Source },
		"mixed scheduling cycle": func(f *b21SourceFixture) { b21ChangeEdge(t, f, 0, "blocks"); b21ChangeEdge(t, f, 2, "blocks") },
		"mixed parent cycle": func(f *b21SourceFixture) {
			b21ChangeEdge(t, f, 0, "parent-child")
			b21ChangeEdge(t, f, 2, "parent-child")
		},
		"mixed ancestor waits": func(f *b21SourceFixture) {
			b21ChangeEdge(t, f, 0, "parent-child")
			b21ChangeEdge(t, f, 2, "waits-for")
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
			change(&f)
			prepared, err := (&workflowplugin.Provider{}).PrepareSourceRestore(context.Background(), f.plan, f.raw)
			if err == nil || len(prepared.LogicalImage()) != 0 {
				t.Fatalf("refusal published prepared state: %v", err)
			}
		})
	}
	f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if prepared, err := (&workflowplugin.Provider{}).PrepareSourceRestore(ctx, f.plan, f.raw); err != context.Canceled || len(prepared.LogicalImage()) != 0 {
		t.Fatalf("canceled preparation=%v", err)
	}
}
