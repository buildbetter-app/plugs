package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// These port tests exercise reader ordering independently of persistence.
// The server package separately proves the actual storage boundary.
type planLog []model.Changeset

func (l planLog) Head(context.Context) (Head, error) {
	return Head{Incarnation: "store", Revision: l[len(l)-1].Revision}, nil
}
func (l planLog) Scan(_ context.Context, after Position, limit int) ([]model.Changeset, error) {
	var result []model.Changeset
	for _, set := range l {
		cmp, _ := CompareNatural(set.Revision, after.Revision)
		if cmp >= 0 && len(result) < limit {
			result = append(result, set)
		}
	}
	return result, nil
}

type planMaterializer struct{}

func (planMaterializer) Materialize(_ context.Context, set model.Changeset, i int) (model.DurableEvent, error) {
	e := model.DurableEvent{Type: model.EventBeadUpdated, Resource: "bead/" + set.Changes[0].Ref.ID, Changed: []string{}, Principal: set.Principal, Chain: []string{}, SetBy: map[string]string{}, Cause: model.EventCause{Root: "root", Chain: []string{}}}
	e.Before = map[string]any{"title": "record", "nested": map[string]any{"value": json.Number("9007199254740993")}}
	e.After = map[string]any{"title": "record", "nested": map[string]any{"value": json.Number("9007199254740993")}}
	if set.Events != nil {
		d := set.Events[i]
		e.Type, e.Resource = d.Type, d.Resource.Kind+"/"+d.Resource.Ref.ID
		e.SetBy, e.Cause, e.Chain, e.Details = d.Provenance.SetBy, d.Provenance.Cause, d.Provenance.Chain, d.Details
		if d.Details != nil {
			e.Before, e.After = nil, nil
		}
	} else {
		e.Resource = "bead/" + set.Changes[i].Ref.ID
	}
	return e, nil
}

type noticeAuthorization struct {
	deny, redact                     string
	primaryDeny, primaryMask, mutate bool
	fail                             error
	resources                        []string
	retained                         map[string]any
	retainedDetails                  *model.ConditionUnmetDetails
}

func (a *noticeAuthorization) ReadResource(_ context.Context, _ model.PrincipalRef, r model.AuditResource) (ResourceVisibility, error) {
	key := r.Kind + "/" + r.Ref.ID
	a.resources = append(a.resources, key)
	if a.fail != nil {
		return ResourceVisibility{}, a.fail
	}
	return ResourceVisibility{Readable: key != a.deny, Redacted: key == a.redact}, nil
}

func (a *noticeAuthorization) Read(_ context.Context, _ model.PrincipalRef, e model.DurableEvent) (Visibility, error) {
	v := Visibility{Readable: true, Before: e.Before, After: e.After}
	if e.Type == model.EventDependencyConditionUnmet {
		if a.primaryDeny {
			return Visibility{PreviouslyVisible: true}, nil
		}
		if a.primaryMask {
			v.After = map[string]any{"title": "dependent"}
		}
		if a.mutate {
			e.Details.Ontology.Project.ID = "tampered"
			e.Details.After.Status = "tampered"
			e.SetBy["status"] = "tampered"
			e.Cause.Root = "tampered"
		}
		a.retainedDetails = e.Details
	} else {
		a.retained = e.After
	}
	return v, nil
}

func TestNoticeAuthorizationProtectsEveryResourceAndSuppressesMarkers(t *testing.T) {
	for _, action := range []string{"deny", "redact"} {
		for _, resource := range []string{"bead/blocker", "bead/dependent-a", "bead/dependent-b", "ontology/ontology", "project/project"} {
			t.Run(action+"/"+resource, func(t *testing.T) {
				a := &noticeAuthorization{}
				if action == "deny" {
					a.deny = resource
				} else {
					a.redact = resource
				}
				r := Reader{Log: planLog{readerPlan()}, Materializer: planMaterializer{}, Authorization: a}
				page, err := r.Read(context.Background(), ReadRequest{Limit: 1000})
				want := 2
				if strings.Contains(resource, "dependent-") {
					want = 3
				}
				if err != nil || len(page.Events()) != want || page.Position.Index.String() != "3" {
					t.Fatalf("resource masking: %+v, %v", page, err)
				}
				for _, e := range page.Events() {
					if e.Control != nil {
						t.Fatal("hidden notice leaked a visibility marker")
					}
					if e.Durable.Details != nil && "bead/"+e.Durable.Details.Dependent.Ref.ID == resource {
						t.Fatal("hidden dependent leaked")
					}
				}
			})
		}
	}
	for _, mask := range []bool{false, true} {
		a := &noticeAuthorization{primaryDeny: !mask, primaryMask: mask}
		r := Reader{Log: planLog{readerPlan()}, Materializer: planMaterializer{}, Authorization: a}
		page, err := r.Read(context.Background(), ReadRequest{Limit: 1000})
		if err != nil || len(page.Events()) != 2 {
			t.Fatalf("primary denied/masked notice leaked: %+v, %v", page, err)
		}
		for _, e := range page.Events() {
			if e.Control != nil || e.Durable.Details != nil {
				t.Fatal("notice-specific existence leak")
			}
		}
	}
	set := readerPlan()
	a := &noticeAuthorization{}
	r := Reader{Log: planLog{set}, Materializer: planMaterializer{}, Authorization: a}
	if _, err := r.Read(context.Background(), ReadRequest{Limit: 1000}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.resources, []string{"bead/blocker", "bead/dependent-a", "ontology/ontology", "project/project", "bead/blocker", "bead/dependent-b", "ontology/ontology", "project/project"}) {
		t.Fatalf("protection set=%v", a.resources)
	}
	r.Authorization = primaryOnlyAuthorization{}
	if _, err := r.Read(context.Background(), ReadRequest{Limit: 1000}); !errors.Is(err, ErrResourceAuthorization) {
		t.Fatalf("missing capability: %v", err)
	}
	denied := errors.New("policy unavailable")
	r.Authorization = &noticeAuthorization{fail: denied}
	if _, err := r.Read(context.Background(), ReadRequest{Limit: 1000}); !errors.Is(err, denied) {
		t.Fatalf("policy error swallowed: %v", err)
	}
}

func TestNoticeValuesDoNotAliasLogPolicyOrReturnedPages(t *testing.T) {
	set := readerPlan()
	before, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	a := &noticeAuthorization{mutate: true}
	r := Reader{Log: planLog{set}, Materializer: planMaterializer{}, Authorization: a}
	page, err := r.Read(context.Background(), ReadRequest{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	a.retained["title"] = "retained policy mutation"
	a.retainedDetails.After.Status = "retained policy mutation"
	e := page.Events()[2].Durable
	if e.Details.After.Status != "done" || e.Details.Ontology.Project.ID != "project" || e.SetBy["status"] != "request" || e.Cause.Root != "root" || e.Before != nil || e.After != nil || page.Events()[3].Durable.After["title"] != "record" {
		t.Fatalf("policy mutated committed envelope: %+v", e)
	}
	e.Details.After.Status = "returned page mutation"
	e.Details.Ontology.Project.ID = "returned page mutation"
	e.SetBy["status"] = "returned page mutation"
	page.Events()[3].Durable.Before["nested"].(map[string]any)["value"] = "returned page mutation"
	after, _ := json.Marshal(set)
	if string(before) != string(after) {
		t.Fatal("event materialization/masking mutated log data")
	}
	page, err = r.Read(context.Background(), ReadRequest{Limit: 1000})
	if err != nil || page.Events()[2].Durable.Details.After.Status != "done" || page.Events()[3].Durable.Before["nested"].(map[string]any)["value"].(json.Number).String() != "9007199254740993" {
		t.Fatalf("replay changed: %+v, %v", page, err)
	}
}

type alteredMaterializer struct{ legacy bool }

func (m alteredMaterializer) Materialize(ctx context.Context, set model.Changeset, i int) (model.DurableEvent, error) {
	if m.legacy {
		set = readerPlan()
		i = 1
	}
	e, err := (planMaterializer{}).Materialize(ctx, set, i)
	if !m.legacy && e.Details != nil {
		copy := *e.Details
		copy.Ontology.Digest = strings.Repeat("cd", 32)
		e.Details = &copy
	}
	return e, err
}

func TestReaderRefusesSynthesizedOrReinterpretedNotices(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		set := readerPlan()
		if legacy {
			set.Events = nil
		}
		r := Reader{Log: planLog{set}, Materializer: alteredMaterializer{legacy: legacy}, Authorization: AllowAll{}}
		if _, err := r.Read(context.Background(), ReadRequest{Limit: 1000}); err == nil {
			t.Fatal("uncommitted or reinterpreted decision accepted")
		}
	}
}

func readerPlan() model.Changeset {
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	p := model.EventProvenance{Method: "tx.apply", Slot: "close", SetBy: map[string]string{"status": "request"}, Cause: model.EventCause{Root: "root", Chain: []string{}}, Chain: []string{}}
	set := model.Changeset{Incarnation: "store", Revision: "2", Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "user", ID: "author"}, Chain: []string{}, Changes: []model.ChangeRecord{{Ref: ref("blocker"), Op: model.OpPatch, FromRevision: "1"}, {Ref: ref("other"), Op: model.OpPatch, FromRevision: "1"}}, Events: []model.EventDescriptor{{Type: model.EventBeadClosed, Resource: model.AuditResource{Kind: "bead", Ref: ref("blocker")}, Provenance: p}}}
	for i, id := range []string{"dependent-a", "dependent-b"} {
		project := ref("project")
		d := &model.ConditionUnmetDetails{Dependent: model.EventImage{Ref: ref(id), Revision: "1"}, BlockerBefore: model.EventImage{Ref: ref("blocker"), Revision: "1"}, BlockerAfter: model.EventImage{Ref: ref("blocker"), Revision: "2"}, Edge: model.ConditionalEdge{Dependent: ref(id), Blocker: ref("blocker"), Kind: "conditional-blocks", Revision: "1"}, Ontology: model.OntologyPin{Ref: ref("ontology"), Revision: "1", Digest: strings.Repeat("ab", 32), Project: &project}, Before: model.ConditionDecision{Status: "working", Category: "started"}, After: model.ConditionDecision{Status: "done", Category: "completed", Outcome: "success", Terminal: true}}
		set.Events = append(set.Events, model.EventDescriptor{Index: model.Natural64(uint64(i + 1)), Type: model.EventDependencyConditionUnmet, Resource: model.AuditResource{Kind: "bead", Ref: ref(id)}, Provenance: p, Details: d})
	}
	p.Slot = ""
	set.Events = append(set.Events, model.EventDescriptor{Index: "3", ChangeIndex: "1", Type: model.EventBeadUpdated, Resource: model.AuditResource{Kind: "bead", Ref: ref("other")}, Provenance: p})
	return set
}

func TestReaderLogicalPlanAndLegacyResume(t *testing.T) {
	set := readerPlan()
	if err := set.ValidateEvents(); err != nil {
		t.Fatal(err)
	}
	legacy := set
	legacy.Revision, legacy.Events = "3", nil
	r := Reader{Log: planLog{set, legacy}, Materializer: planMaterializer{}, Authorization: AllowAll{}}
	position := Position{}
	var seen []string
	for range 7 {
		page, err := r.Read(context.Background(), ReadRequest{After: position, Limit: 1, Changesets: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Events() {
			seen = append(seen, e.Durable.Revision.String()+":"+e.Durable.Index.String())
		}
		position = page.Position
	}
	if got := strings.Join(seen, ","); got != "2:0,2:1,2:2,2:3,3:0,3:1" {
		t.Fatalf("logical/legacy order = %s", got)
	}
	page, err := r.Read(context.Background(), ReadRequest{After: Position{Incarnation: "store", Revision: "2", Index: "1"}, Limit: 1000, Changesets: 1})
	if err != nil || len(page.Events()) != 4 || page.Events()[0].Durable.Index.String() != "2" {
		t.Fatalf("index1 continuation = %+v, %v", page, err)
	}
	for _, index := range []string{"4", "999999999999999999999999"} {
		if _, err := r.Read(context.Background(), ReadRequest{After: Position{Incarnation: "store", Revision: "2", Index: model.Natural(index)}, Limit: 1}); err == nil {
			t.Fatal("accepted impossible logical index", index)
		}
	}
	whole, err := r.Read(context.Background(), ReadRequest{Limit: 1, WholeChangesets: true, OneChangeset: true})
	if err != nil || len(whole.Groups) != 1 || len(whole.Events()) != 4 || whole.Position.Index.String() != "3" {
		t.Fatalf("projection truncated logical revision: %+v, %v", whole, err)
	}
	whole, err = r.Read(context.Background(), ReadRequest{After: whole.Position, Limit: 1, WholeChangesets: true, OneChangeset: true, Changesets: 1})
	if err != nil || len(whole.Events()) != 2 || whole.Position.Revision.String() != "3" || whole.Position.Index.String() != "1" {
		t.Fatalf("projection failed legacy successor progress: %+v, %v", whole, err)
	}
}

type primaryOnlyAuthorization struct{}

func (primaryOnlyAuthorization) Read(_ context.Context, _ model.PrincipalRef, e model.DurableEvent) (Visibility, error) {
	return Visibility{Readable: true, Before: e.Before, After: e.After}, nil
}

func TestReaderRefusesUnvalidatedPlansAndUnavailableResourceAuthorization(t *testing.T) {
	for _, name := range []string{"empty", "missing ordinary", "wrong index", "resource authorization"} {
		t.Run(name, func(t *testing.T) {
			set := readerPlan()
			switch name {
			case "empty":
				set.Events = []model.EventDescriptor{}
			case "missing ordinary":
				set.Events = set.Events[:3]
			case "wrong index":
				set.Events[1].Index = "9"
			}
			r := Reader{Log: planLog{set}, Materializer: planMaterializer{}, Authorization: primaryOnlyAuthorization{}}
			if _, err := r.Read(context.Background(), ReadRequest{Limit: 1000}); err == nil {
				t.Fatal(fmt.Sprintf("accepted %s", name))
			}
		})
	}
}

type principalMutationAuthorization struct {
	want     []byte
	retained []model.PrincipalRef
}

func (a *principalMutationAuthorization) checkAndMutate(principal model.PrincipalRef) error {
	raw, err := json.Marshal(principal)
	if err != nil || string(raw) != string(a.want) {
		return fmt.Errorf("authorization received a principal mutated by a previous callback: %s %v", raw, err)
	}
	a.retained = append(a.retained, principal)
	principal.Sponsor.ID = "callback mutation"
	principal.Caveats["scope"].(map[string]any)["project"] = "callback mutation"
	return nil
}

func (a *principalMutationAuthorization) Read(_ context.Context, p model.PrincipalRef, e model.DurableEvent) (Visibility, error) {
	return Visibility{Readable: true, Before: e.Before, After: e.After}, a.checkAndMutate(p)
}

func (a *principalMutationAuthorization) ReadResource(_ context.Context, p model.PrincipalRef, _ model.AuditResource) (ResourceVisibility, error) {
	return ResourceVisibility{Readable: true}, a.checkAndMutate(p)
}

func TestAuthorizationPrincipalArgumentsDoNotAliasRequestOrOtherChecks(t *testing.T) {
	principal := model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "reader"}, Caveats: map[string]any{"scope": map[string]any{"project": "project"}, "number": json.Number("9007199254740993")}}
	want, err := json.Marshal(principal)
	if err != nil {
		t.Fatal(err)
	}
	a := &principalMutationAuthorization{want: want}
	r := Reader{Log: planLog{readerPlan()}, Materializer: planMaterializer{}, Authorization: a}
	if _, err := r.Read(context.Background(), ReadRequest{Principal: principal, Limit: 1000}); err != nil {
		t.Fatal(err)
	}
	for _, p := range a.retained {
		p.Sponsor.ID = "retained mutation"
		p.Caveats["scope"].(map[string]any)["project"] = "retained mutation"
	}
	got, err := json.Marshal(principal)
	if err != nil || string(got) != string(want) {
		t.Fatalf("authorization mutated caller principal: %s %v", got, err)
	}
	if _, err := r.Read(context.Background(), ReadRequest{Principal: principal, Limit: 1000}); err != nil {
		t.Fatalf("retained policy arguments changed later authorization: %v", err)
	}
}
