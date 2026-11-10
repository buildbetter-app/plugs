package postgresacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

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

func TestB21SourceReverseImageDetachedCensus(t *testing.T) {
	f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
	logical := b21Logical(t, &f)
	original := bytes.Clone(logical)
	image, err := workflowplugin.InspectSourceReverseImage(logical)
	if err != nil {
		t.Fatal(err)
	}
	parent := driver.SourceEntityKey{Project: project, Plane: dumpformat.EphemeralEntityTable, ID: "same-id"}
	durable := driver.SourceEntityKey{Project: project, Plane: dumpformat.DurableEntityTable, ID: "same-id"}
	entry := image.Entities[parent]
	if len(image.Entities) != 2 || len(image.Relations) != 3 || !entry.Present || entry.Terminal || entry.Item == nil || entry.TargetID != "workflow:item/source-parent" || entry.Item.Metadata["large"] != json.Number("9007199254740993") || entry.Item.Metadata["negative"] != json.Number("-9007199254740993") || entry.Item.ChildCounter != 13 {
		t.Fatalf("incomplete source-keyed current census: %+v", image)
	}
	if _, aliased := image.Entities[durable]; aliased {
		t.Fatal("equal physical IDs aliased durable and active source planes")
	}
	if len(entry.Item.Comments) != 1 || entry.Item.Comments[0].Source == nil || entry.Item.Comments[0].Source.Author != "unverified-author" || entry.Item.Comments[0].Principal.Kind != "" {
		t.Fatalf("inspection changed source attribution: %+v", entry.Item.Comments)
	}
	want, err := workflowplugin.InspectSourceReverseImage(logical)
	if err != nil {
		t.Fatal(err)
	}
	entry.Item.Title = "changed detached view"
	entry.Item.Metadata["large"] = "changed"
	entry.Item.Metadata["nested"].([]any)[1] = "changed"
	entry.Item.Comments[0].Source.Author = "changed"
	image.Relations[0].From = "changed"
	delete(image.Entities, parent)
	again, err := workflowplugin.InspectSourceReverseImage(logical)
	if err != nil || !reflect.DeepEqual(want, again) || !bytes.Equal(logical, original) {
		t.Fatalf("reverse inspection aliased or changed its input: %v", err)
	}
}

func TestB21SourceReverseImagePreservesStoredClaims(t *testing.T) {
	f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
	logical := b21Logical(t, &f)
	baseline, err := workflowplugin.InspectSourceReverseImage(logical)
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		Items map[string]json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(logical, &value); err != nil {
		t.Fatal(err)
	}
	original := value.Items["workflow:item/source-parent"]
	if bytes.Count(original, []byte(`"status":"open"`)) != 1 || bytes.Count(original, []byte(`"generation":1`)) != 1 || bytes.Count(logical, original) != 1 {
		t.Fatal("missing canonical current-item precondition")
	}
	for name, expiry := range map[string]time.Time{
		"expired": time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		"live":    time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		t.Run(name, func(t *testing.T) {
			// Match the provider's stored claim field order and number encoding;
			// keep every unrelated canonical image byte and origin unchanged.
			storedClaim, err := json.Marshal(struct {
				Principal  model.PrincipalRef `json:"principal"`
				Agent      string             `json:"agent,omitempty"`
				ExpiresAt  time.Time          `json:"expiresAt"`
				Generation uint64             `json:"generation"`
			}{b21SourceActor, "stored-agent", expiry, 1})
			if err != nil {
				t.Fatal(err)
			}
			item := bytes.Replace(original, []byte(`"status":"open"`), []byte(`"status":"claimed"`), 1)
			item = bytes.Replace(item, []byte(`"generation":1`), append([]byte(`"generation":1,"claim":`), storedClaim...), 1)
			current := bytes.Replace(logical, original, item, 1)
			before := bytes.Clone(current)
			image, err := workflowplugin.InspectSourceReverseImage(current)
			if err != nil {
				t.Fatal(err)
			}
			key := driver.SourceEntityKey{Project: project, Plane: dumpformat.EphemeralEntityTable, ID: "same-id"}
			entry := image.Entities[key]
			want := *baseline.Entities[key].Item
			want.Status = "claimed"
			want.Claim = &workflowplugin.ClaimView{Principal: b21SourceActor, Agent: "stored-agent", ExpiresAt: expiry, Generation: model.Natural64(1)}
			if !entry.Present || entry.Terminal || !reflect.DeepEqual(entry.Item, &want) || len(image.Entities) != len(baseline.Entities) || !reflect.DeepEqual(image.Relations, baseline.Relations) || image.OriginSHA256 != baseline.OriginSHA256 || !bytes.Equal(current, before) {
				t.Fatalf("claimed inspection changed stored claim, census, origin or input: %+v", image)
			}
		})
	}
}

func TestB21SourceReverseOriginBindsEveryImmutableComponent(t *testing.T) {
	f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
	logical := b21Logical(t, &f)
	baseline, err := workflowplugin.InspectSourceReverseImage(logical)
	if err != nil {
		t.Fatal(err)
	}
	var original struct {
		Source json.RawMessage `json:"source"`
	}
	if err := json.Unmarshal(logical, &original); err != nil {
		t.Fatal(err)
	}
	if baseline.OriginSHA256 != b21Bytes(original.Source, "object").SHA256 {
		t.Fatal("origin digest does not bind the complete canonical immutable source")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(original.Source, &fields); err != nil {
		t.Fatal(err)
	}
	// Reorder each immutable component without changing any record commitment
	// or current item/relation. Internal consistency and the active census alone
	// cannot distinguish these images from the admitted initial preparation.
	for _, component := range []string{"entities", "relations", "origins", "records"} {
		t.Run(component, func(t *testing.T) {
			var entries []json.RawMessage
			if err := json.Unmarshal(fields[component], &entries); err != nil || len(entries) < 2 {
				t.Fatalf("missing immutable-component precondition: %v", err)
			}
			entries[0], entries[len(entries)-1] = entries[len(entries)-1], entries[0]
			reordered, err := json.Marshal(entries)
			if err != nil || bytes.Equal(fields[component], reordered) || bytes.Count(original.Source, fields[component]) != 1 {
				t.Fatalf("invalid component-only replacement: %v", err)
			}
			source := bytes.Replace(original.Source, fields[component], reordered, 1)
			changed := bytes.Replace(logical, original.Source, source, 1)
			image, err := workflowplugin.InspectSourceReverseImage(changed)
			if err != nil {
				t.Fatal(err)
			}
			if image.OriginSHA256 == "" || image.OriginSHA256 == baseline.OriginSHA256 || image.OriginSHA256 != b21Bytes(source, "object").SHA256 {
				t.Fatal("changed immutable source was accepted as the admitted origin")
			}
			if !reflect.DeepEqual(baseline.Entities, image.Entities) || !reflect.DeepEqual(baseline.Relations, image.Relations) {
				t.Fatal("component-only change altered the current census")
			}
		})
	}
}

func TestB21SourceReverseImageRefusesMalformedAndUnmapped(t *testing.T) {
	f := b21Fixture(t, driver.Meta{StoreID: "target", Incarnation: "before"}, driver.CheckpointLifetimeProcess)
	logical := b21Logical(t, &f)
	replace := func(before, after string) []byte {
		t.Helper()
		if !bytes.Contains(logical, []byte(before)) {
			t.Fatalf("missing causal image precondition: %s", before)
		}
		return bytes.Replace(logical, []byte(before), []byte(after), 1)
	}
	// Keep the writer's struct/map order and all unrelated bytes unchanged so
	// each changed known field reaches the corresponding identity/index guard.
	inputs := map[string][]byte{
		"empty": nil, "null": []byte("null"), "truncated": logical[:len(logical)-1],
		"trailing":                    append(bytes.Clone(logical), []byte("{}")...),
		"duplicate JSON member":       append([]byte(`{"runs":null,`), logical[1:]...),
		"unsupported source schema":   replace(`"schema":"workflow/source-state/1"`, `"schema":"unsupported/1"`),
		"unmapped current item":       replace(`"workflow:item/source-parent":`, `"workflow:item/unmapped":`),
		"mismatched current identity": replace(`"id":"workflow:item/source-parent"`, `"id":"workflow:item/other"`),
		"missing current run":         replace(`"workflow:run/source-parent":`, `"workflow:run/unmapped":`),
		"missing run member":          replace(`"itemIds":["workflow:item/source-parent"]`, `"itemIds":[]`),
		"duplicate run member":        replace(`"itemIds":["workflow:item/source-parent"]`, `"itemIds":["workflow:item/source-parent","workflow:item/source-parent"]`),
		"zero item generation":        replace(`"generation":1`, `"generation":0`),
		"unsupported current status":  replace(`"status":"open"`, `"status":"unsupported"`),
		"claimed without claim":       replace(`"status":"open"`, `"status":"claimed"`),
		"malformed current claim":     replace(`"generation":1`, `"generation":1,"claim":{"principal":{},"expiresAt":"0001-01-01T00:00:00Z","generation":0}`),
		"mismatched relation indexes": replace(`"kind":"blocks"`, `"kind":"relates-to"`),
		"unmapped relation":           replace(`"from":"workflow:item/source-parent"`, `"from":"workflow:item/unmapped"`),
		"unknown item field":          replace(`"generation":1`, `"generation":1,"future":true`),
		"malformed source comment":    replace(`"id":"physical-comment"`, `"id":""`),
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			before := bytes.Clone(input)
			image, err := workflowplugin.InspectSourceReverseImage(input)
			if !errors.Is(err, driver.ErrUnsupported) || image.OriginSHA256 != "" || len(image.Entities) != 0 || len(image.Relations) != 0 || !bytes.Equal(input, before) {
				t.Fatalf("malformed image returned partial state or changed input: %+v %v", image, err)
			}
		})
	}
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
