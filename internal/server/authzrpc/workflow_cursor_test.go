package authzrpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

func newWorkflowCursorFixture(t *testing.T) *workflowClaimNextFixture {
	t.Helper()
	f := newWorkflowClaimNextFixture(t)
	f.by = model.PrincipalRef{Kind: "user", ID: "user:sponsor"}
	// CP7 has no Read/Project permit. Install the existing explicit test-only
	// policy, using real current registry/grant facts and the HTTP principal.
	f.app.Checker.chain.Rules = append(f.app.Checker.chain.Rules, &workflowReadRules{namespace: f.app.Namespace})
	return f
}

type workflowContinuation struct {
	position *service.PagePosition
	token    string
}

func (p workflowContinuation) more() bool { return p.position != nil || p.token != "" }

type workflowReadyObservation struct {
	ids      []string
	next     workflowContinuation
	snapshot *service.WorkflowPageSnapshot
}

func workflowCursorRead(t *testing.T, f *workflowClaimNextFixture, mode string, filter *service.Filter, limit int, previous workflowContinuation, project string) (workflowReadyObservation, error) {
	t.Helper()
	if mode == "http" {
		params := map[string]any{"limit": limit}
		if filter != nil {
			selector := map[string]any{}
			if len(filter.Types) != 0 {
				selector["types"] = filter.Types
			}
			if len(filter.Resources) != 0 {
				selector["resources"] = filter.Resources
			}
			params["filter"] = selector
		}
		if previous.token != "" {
			params["cursor"] = previous.token
		}
		reply := workflowWriteCall(t, f, "bead.ready", params, 500, project)
		if reply.Error != nil {
			if len(reply.Result) != 0 {
				t.Fatal("cursor refusal disclosed a partial page")
			}
			return workflowReadyObservation{}, &model.Error{Data: reply.Error.Data}
		}
		var page struct {
			Items      []struct{ ID string }
			NextCursor string
		}
		if err := json.Unmarshal(reply.Result, &page); err != nil {
			t.Fatal(err)
		}
		out := workflowReadyObservation{next: workflowContinuation{token: page.NextCursor}}
		for _, item := range page.Items {
			out.ids = append(out.ids, item.ID)
		}
		if page.NextCursor != "" {
			raw, err := base64.RawURLEncoding.DecodeString(strings.Split(page.NextCursor, ".")[0])
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				WorkflowSnapshot *service.WorkflowPageSnapshot
			}
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatal(err)
			}
			out.snapshot = payload.WorkflowSnapshot
		}
		return out, nil
	}
	read := f.domain.Ready
	if mode == "guarded" {
		read = f.app.Service.Ready
	}
	page, err := read(context.Background(), f.by, service.ReadyRequest{Project: project, Agent: "client-label", Filter: filter, Limit: limit, Position: previous.position})
	if err != nil {
		if len(page.Items) != 0 {
			t.Fatal("cursor refusal disclosed a partial domain page")
		}
		return workflowReadyObservation{}, err
	}
	out := workflowReadyObservation{next: workflowContinuation{position: page.Next}, snapshot: page.WorkflowSnapshot}
	for _, item := range page.Items {
		out.ids = append(out.ids, item.ID)
	}
	return out, nil
}

func workflowCursorError(t *testing.T, err error, code model.ErrorCode) {
	t.Helper()
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func workflowCursorIDs(items []workflowplugin.ItemView) []string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}
	sort.Strings(ids)
	return ids
}

func workflowCursorCore(t *testing.T, f *workflowClaimNextFixture) []string {
	t.Helper()
	ids := []string{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"}
	// These core items are created at the fixture's declared clock.
	createdAt := f.now.UTC()
	changes := make([]conformance.FixtureChange, len(ids))
	for i, id := range ids {
		changes[i] = conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate,
			Body: []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":"project","createdAt":%q,"fields":{"title":"core item","status":"todo","priority":0}}`, id, createdAt.Format(time.RFC3339Nano)))}
	}
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Time: createdAt}, changes); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestWorkflowReadyStableTraversalAndResources(t *testing.T) {
	for _, mode := range []string{"domain", "guarded", "http"} {
		for _, filtered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resources=%v", mode, filtered), func(t *testing.T) {
				f := newWorkflowCursorFixture(t)
				core := workflowCursorCore(t, f)
				ids := workflowCursorIDs(f.invoke(t, 501, 4))
				want := append(append([]string(nil), core...), ids...)
				filter := &service.Filter{Types: []string{"issue", "task"}}
				if filtered {
					filter.Resources = []string{ids[3], core[1], ids[1], ids[3], "missing-item"}
					want = []string{core[1], ids[1], ids[3]}
				}
				var previous workflowContinuation
				var got []string
				for pass := 0; ; pass++ {
					if pass > len(want) {
						t.Fatal("ready traversal did not terminate")
					}
					page, err := workflowCursorRead(t, f, mode, filter, 1, previous, "project")
					if err != nil || len(page.ids) != 1 {
						t.Fatalf("page=%+v err=%v", page, err)
					}
					got = append(got, page.ids...)
					if mode != "domain" && page.next.more() && page.snapshot == nil {
						t.Fatal("authorization aggregate lost the workflow binding")
					}
					if mode == "guarded" && page.next.position != nil && page.next.position.Workflow {
						t.Fatal("authorized aggregate reused a raw phase offset")
					}
					if !page.next.more() {
						break
					}
					previous = page.next
				}
				if !slices.Equal(got, want) {
					t.Fatalf("skipped, duplicated or out-of-selector rows: got=%v want=%v", got, want)
				}
				// An unmatched resource and a mismatching type are genuinely empty.
				for _, empty := range []*service.Filter{{Resources: []string{"missing-item"}}, {Types: []string{"absent"}}} {
					page, err := workflowCursorRead(t, f, mode, empty, 1, workflowContinuation{}, "project")
					if err != nil || len(page.ids) != 0 || page.next.more() {
						t.Fatalf("empty selector returned content: %+v err=%v", page, err)
					}
				}
			})
		}
	}
}

func TestWorkflowReadyContinuationRefusesMutationAndTimeOnlyExpiry(t *testing.T) {
	for _, mode := range []string{"domain", "guarded", "http"} {
		for _, mutation := range []string{"invoke", "comment", "claim", "lease-expiry"} {
			t.Run(mode+"/"+mutation, func(t *testing.T) {
				f := newWorkflowCursorFixture(t)
				ids := workflowCursorIDs(f.invoke(t, 510, 4))
				if mutation == "lease-expiry" {
					if _, err := f.domain.ClaimWithOutcome(context.Background(), f.by, service.ClaimRequest{ID: model.RefByID(ids[0], model.Natural64(0))}, workflowClaimNextMeta("bead.claim", 511)); err != nil {
						t.Fatal(err)
					}
				}
				first, err := workflowCursorRead(t, f, mode, nil, 1, workflowContinuation{}, "project")
				if err != nil || !first.next.more() || first.snapshot == nil {
					t.Fatalf("first page=%+v err=%v", first, err)
				}
				before := workflowWriteCheckpoint(t, f)
				switch mutation {
				case "invoke":
					f.invoke(t, 512, 1)
				case "comment":
					_, err = f.domain.CommentWithOutcome(context.Background(), f.by, service.CommentRequest{ID: model.RefByID(ids[0], model.Natural64(0)), Body: "changed"}, workflowClaimNextMeta("bead.comment", 512))
				case "claim":
					_, err = f.domain.ClaimWithOutcome(context.Background(), f.by, service.ClaimRequest{ID: model.RefByID(ids[0], model.Natural64(0))}, workflowClaimNextMeta("bead.claim", 512))
				case "lease-expiry":
					if first.snapshot.ValidUntil == nil {
						t.Fatal("excluded live claim did not bind the continuation deadline")
					}
					f.now = f.now.Add(2 * workflowplugin.ClaimTTL)
				}
				if err != nil {
					t.Fatal(err)
				}
				_, err = workflowCursorRead(t, f, mode, nil, 1, first.next, "project")
				workflowCursorError(t, err, "cursor_invalid")
				if mutation == "lease-expiry" {
					after := workflowWriteCheckpoint(t, f)
					if before.Generation != after.Generation || !slices.Equal(before.Payload, after.Payload) {
						t.Fatal("time-only expiry test mutated the durable generation")
					}
				}
				fresh, err := workflowCursorRead(t, f, mode, nil, 100, workflowContinuation{}, "project")
				if err != nil || len(fresh.ids) < 3 || fresh.next.more() {
					t.Fatalf("fresh restart did not expose the current set: %+v err=%v", fresh, err)
				}
			})
		}
	}
}

func TestWorkflowReadyHTTPPrincipalProjectFilterBinding(t *testing.T) {
	f := newWorkflowCursorFixture(t)
	ids := workflowCursorIDs(f.invoke(t, 520, 3))
	filter := &service.Filter{Types: []string{"task"}}
	first, err := workflowCursorRead(t, f, "http", filter, 1, workflowContinuation{}, "project")
	if err != nil || !first.next.more() {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	for _, changed := range []string{"principal", "project", "types", "resources", "signature"} {
		t.Run(changed, func(t *testing.T) {
			by := f.by
			defer func() { f.by = by }()
			project := "project"
			selector := *filter
			next := first.next
			switch changed {
			case "principal":
				f.by.ID = "user:other-reader"
			case "project":
				project = "other-project"
			case "types":
				selector.Types = []string{"issue"}
			case "resources":
				selector.Resources = []string{ids[0]}
			case "signature":
				next.token = strings.Split(next.token, ".")[0] + ".AA"
			}
			_, err := workflowCursorRead(t, f, "http", &selector, 1, next, project)
			workflowCursorError(t, err, "cursor_invalid")
		})
	}
	last, err := workflowCursorRead(t, f, "http", filter, 2, first.next, "project")
	if err != nil || !slices.Equal(last.ids, ids[1:]) || last.next.more() {
		t.Fatalf("bound cursor did not permit page-size change: %+v err=%v", last, err)
	}
}

func TestWorkflowReadyDomainRejectsUnboundAndInvalidPhase(t *testing.T) {
	f := newWorkflowCursorFixture(t)
	f.invoke(t, 530, 3)
	first, err := f.domain.Ready(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 1})
	if err != nil || first.Next == nil || first.Next.Workflow || first.Next.WorkflowSnapshot == nil || first.Next.ReadyMembership == "" {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	for _, invalid := range []string{"unbound", "negative", "past-end", "incarnation", "revision", "deadline", "limit"} {
		t.Run(invalid, func(t *testing.T) {
			position := *first.Next
			binding := *position.WorkflowSnapshot
			position.WorkflowSnapshot = &binding
			req := service.ReadyRequest{Project: "project", Limit: 1, Position: &position}
			code := model.ErrorCode("cursor_invalid")
			switch invalid {
			case "unbound":
				position.WorkflowSnapshot = nil
			case "negative":
				position.Offset = -1
			case "past-end":
				position.Offset = 100
			case "incarnation":
				position.AsOf.Incarnation = "other-incarnation"
				code = "incarnation_changed"
			case "revision":
				binding.Revision = model.Natural("-1")
			case "deadline":
				zero := time.Time{}
				binding.ValidUntil = &zero
			case "limit":
				req.Limit = 1001
				code = "invalid_params"
			}
			_, err := f.domain.Ready(context.Background(), f.by, req)
			workflowCursorError(t, err, code)
		})
	}
}

func TestWorkflowClaimNextLivePagerRestartsChangedProviderOffset(t *testing.T) {
	f := newWorkflowCursorFixture(t)
	ids := workflowCursorIDs(f.invoke(t, 540, 3))
	first, err := f.domain.ClaimNextReadyPage(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 1})
	if err != nil || first.Next == nil || first.Items[0].ID != ids[0] {
		t.Fatalf("first candidates=%+v err=%v", first, err)
	}
	if _, err := f.domain.ClaimWithOutcome(context.Background(), f.by, service.ClaimRequest{ID: model.RefByID(ids[0], model.Natural64(0))}, workflowClaimNextMeta("bead.claim", 541)); err != nil {
		t.Fatal(err)
	}
	next, err := f.domain.ClaimNextReadyPage(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 1, Position: first.Next})
	if err != nil || len(next.Items) != 1 || next.Items[0].ID != ids[1] || first.WorkflowSnapshot.Equal(next.WorkflowSnapshot) {
		t.Fatalf("live walker skipped the new first candidate: %+v err=%v", next, err)
	}
}

func TestWorkflowReadyGuardedRetainsBindingAcrossProviderPages(t *testing.T) {
	f := newWorkflowCursorFixture(t)
	var want []string
	for i := 0; i < 11; i++ {
		want = append(want, workflowCursorIDs(f.invoke(t, 550+i, 100))...)
	}
	sort.Strings(want)
	first, err := workflowCursorRead(t, f, "guarded", nil, 1000, workflowContinuation{}, "project")
	if err != nil || !slices.Equal(first.ids, want[:1000]) || first.snapshot == nil || first.next.position == nil || first.next.position.Workflow {
		t.Fatalf("guarded drain lost its captured provider state: count=%d err=%v", len(first.ids), err)
	}
	last, err := workflowCursorRead(t, f, "guarded", nil, 1000, first.next, "project")
	if err != nil || !slices.Equal(last.ids, want[1000:]) || last.next.more() || !first.snapshot.Equal(last.snapshot) {
		t.Fatalf("guarded continuation skipped the drained tail: count=%d err=%v", len(last.ids), err)
	}
}

func TestWorkflowGuardedCursorRefusesCoreMembershipChange(t *testing.T) {
	f := newWorkflowCursorFixture(t)
	coreID := "00000000-0000-4000-8000-000000000009"
	createdAt := f.now.UTC()
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Time: createdAt}, []conformance.FixtureChange{{
		Ref: model.RefByID(coreID, model.Natural64(0)), Kind: "bead", Op: model.OpCreate,
		Body: []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":"project","createdAt":%q,"fields":{"title":"core item","status":"todo","priority":0}}`, coreID, createdAt.Format(time.RFC3339Nano))),
	}}); err != nil {
		t.Fatal(err)
	}
	workflow := workflowCursorIDs(f.invoke(t, 560, 2))
	first, err := workflowCursorRead(t, f, "guarded", nil, 2, workflowContinuation{}, "project")
	if err != nil || !slices.Equal(first.ids, []string{coreID, workflow[0]}) || !first.next.more() {
		t.Fatalf("mixed page=%+v err=%v", first, err)
	}
	// Change only fixture content. This is a read-pagination regression;
	// the read-only caller and fixed CP7 policy do not permit bead.close.
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Time: createdAt}, []conformance.FixtureChange{{
		Ref: model.RefByID(coreID, model.Natural64(0)), Kind: "bead", Op: model.OpPatch,
		Body: []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":"project","createdAt":%q,"fields":{"title":"core item","status":"done","priority":0}}`, coreID, createdAt.Format(time.RFC3339Nano))),
	}}); err != nil {
		t.Fatal(err)
	}
	fresh, err := workflowCursorRead(t, f, "guarded", nil, 1, workflowContinuation{}, "project")
	if err != nil || !slices.Equal(fresh.ids, workflow[:1]) || !fresh.next.more() {
		t.Fatalf("core fixture did not leave Ready: page=%+v err=%v", fresh, err)
	}
	if !first.snapshot.Equal(fresh.snapshot) || first.next.position.AuthorizationRevision.String() != fresh.next.position.AuthorizationRevision.String() {
		t.Fatal("core fixture changed the workflow snapshot or current ACL generation")
	}
	_, err = workflowCursorRead(t, f, "guarded", nil, 2, first.next, "project")
	workflowCursorError(t, err, "cursor_invalid")
	last, err := workflowCursorRead(t, f, "guarded", nil, 1, fresh.next, "project")
	if err != nil || !slices.Equal(last.ids, workflow[1:]) || last.next.more() {
		t.Fatalf("fresh traversal lost the final workflow item: page=%+v err=%v", last, err)
	}
}

func TestWorkflowReadySignedRPCBindsCompiledWorkflowSnapshot(t *testing.T) {
	t.Run("non-nil revision change", func(t *testing.T) {
		f := newWorkflowCursorFixture(t)
		ids := workflowCursorIDs(f.invoke(t, 570, 3))
		filter := &service.Filter{Types: []string{"task"}}
		first, err := workflowCursorRead(t, f, "http", filter, 1, workflowContinuation{}, "project")
		if err != nil || len(first.ids) != 1 || first.snapshot == nil || !first.next.more() {
			t.Fatalf("signed first page=%+v err=%v", first, err)
		}
		if first.snapshot.Revision.IsZero() {
			t.Fatal("signed Ready omitted its nonnil workflow revision")
		}
		body, err := base64.RawURLEncoding.DecodeString(strings.Split(first.next.token, ".")[0])
		if err != nil {
			t.Fatal(err)
		}
		var payload struct {
			Version         int    `json:"version"`
			ReadyMembership string `json:"readyMembership"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Version != 3 || !strings.HasPrefix(payload.ReadyMembership, "v3:") {
			t.Fatalf("signed Ready did not use the compiled membership binding: %+v", payload)
		}
		// The original signed token must reach the guarded compiled Ready path,
		// not merely be accepted by the transport codec.
		last, err := workflowCursorRead(t, f, "http", filter, 2, first.next, "project")
		if err != nil || !slices.Equal(last.ids, ids[1:]) || last.next.more() {
			t.Fatalf("valid signed continuation=%+v err=%v", last, err)
		}

		_, err = f.domain.CommentWithOutcome(context.Background(), f.by, service.CommentRequest{
			ID: model.RefByID(ids[0], model.Natural64(0)), Body: "revision changed",
		}, workflowClaimNextMeta("bead.comment", 571))
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := workflowCursorRead(t, f, "http", filter, 1, workflowContinuation{}, "project")
		if err != nil || fresh.snapshot == nil || fresh.snapshot.Revision.IsZero() || fresh.snapshot.Revision.String() == first.snapshot.Revision.String() {
			t.Fatalf("fresh signed Ready did not bind a new nonnil workflow revision: first=%+v fresh=%+v err=%v", first.snapshot, fresh.snapshot, err)
		}
		_, err = workflowCursorRead(t, f, "http", filter, 1, first.next, "project")
		workflowCursorError(t, err, "cursor_invalid")
	})

	t.Run("clock-only ValidUntil expiry", func(t *testing.T) {
		f := newWorkflowCursorFixture(t)
		ids := workflowCursorIDs(f.invoke(t, 580, 3))
		if _, err := f.domain.ClaimWithOutcome(context.Background(), f.by, service.ClaimRequest{
			ID: model.RefByID(ids[0], model.Natural64(0)),
		}, workflowClaimNextMeta("bead.claim", 581)); err != nil {
			t.Fatal(err)
		}
		before := workflowWriteCheckpoint(t, f)
		filter := &service.Filter{Types: []string{"task"}}
		first, err := workflowCursorRead(t, f, "http", filter, 1, workflowContinuation{}, "project")
		if err != nil || first.snapshot == nil || first.snapshot.ValidUntil == nil || !first.next.more() {
			t.Fatalf("signed Ready did not bind the excluded claim deadline: %+v err=%v", first, err)
		}
		f.now = f.now.Add(2 * workflowplugin.ClaimTTL)
		_, err = workflowCursorRead(t, f, "http", filter, 1, first.next, "project")
		workflowCursorError(t, err, "cursor_invalid")
		after := workflowWriteCheckpoint(t, f)
		if before.Generation != after.Generation || !slices.Equal(before.Payload, after.Payload) {
			t.Fatal("clock-only cursor expiry changed the durable workflow checkpoint")
		}
		fresh, err := workflowCursorRead(t, f, "http", filter, 1, workflowContinuation{}, "project")
		if err != nil || fresh.snapshot == nil || fresh.snapshot.Revision.String() != first.snapshot.Revision.String() || fresh.snapshot.Equal(first.snapshot) {
			t.Fatalf("fresh Ready did not preserve revision while changing only the deadline: first=%+v fresh=%+v err=%v", first.snapshot, fresh.snapshot, err)
		}
	})
}

func TestWorkflowReadySignedRPCRefusesClockOnlyCrossPlaneReorder(t *testing.T) {
	f := newWorkflowCursorFixture(t)
	base := f.now.UTC()
	// Use the real SQLite store, provider and signed HTTP route. Only their
	// shared ordering clock is controlled; no page or source image is replaced.
	service.WithClock(func() time.Time { return f.now })(f.domain)
	f.now = base.Add(-time.Hour)
	workflow := workflowCursorIDs(f.invoke(t, 590, 1))
	f.now = base

	coreID := "00000000-0000-4000-8000-000000000099"
	created := base.Add(-48*time.Hour + time.Second)
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{Time: base}, []conformance.FixtureChange{{
		Ref: model.RefByID(coreID, model.Natural64(0)), Kind: "bead", Op: model.OpCreate,
		Body: []byte(fmt.Sprintf(`{"id":%q,"type":"issue","project":"project","createdAt":%q,"fields":{"title":"cross-plane cutoff","status":"todo","priority":0}}`, coreID, created.Format(time.RFC3339Nano))),
	}}); err != nil {
		t.Fatal(err)
	}
	beforeMeta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	beforeCheckpoint := workflowWriteCheckpoint(t, f)

	first, err := workflowCursorRead(t, f, "http", nil, 1, workflowContinuation{}, "project")
	if err != nil || !slices.Equal(first.ids, []string{coreID}) || !first.next.more() || first.snapshot == nil || first.snapshot.ValidUntil != nil {
		t.Fatalf("mixed signed first page=%+v err=%v", first, err)
	}
	version, membership := signedReadyPayload(t, first.next.token)
	if version != 3 || !strings.HasPrefix(membership, "v3:") {
		t.Fatalf("mixed signed page omitted ordered membership: version=%d membership=%q", version, membership)
	}
	stable, err := workflowCursorRead(t, f, "http", nil, 1, first.next, "project")
	if err != nil || !slices.Equal(stable.ids, workflow) || stable.next.more() {
		t.Fatalf("unchanged signed continuation=%+v err=%v", stable, err)
	}

	// The released v2 digest cannot distinguish the two interleavings. A
	// mixed v2 continuation is safely refused even before the rank crossing.
	full, err := f.app.Service.Ready(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 100})
	if err != nil || len(full.Items) != 2 {
		t.Fatalf("complete authorized mixed domain=%+v err=%v", full, err)
	}
	oldMembership, err := readyMembershipV2(full.Items)
	if err != nil {
		t.Fatal(err)
	}
	firstDirect, err := f.app.Service.Ready(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 1})
	if err != nil || firstDirect.Next == nil {
		t.Fatalf("direct signed-position source=%+v err=%v", firstDirect, err)
	}
	oldPosition := *firstDirect.Next
	oldPosition.ReadyMembership = oldMembership
	_, err = f.app.Service.Ready(context.Background(), f.by, service.ReadyRequest{Project: "project", Limit: 1, Position: &oldPosition})
	workflowCursorError(t, err, "cursor_invalid")

	// Cross both the 48-hour boundary and any one-minute persisted-order lag.
	// The same source, checkpoint and authority now yield [workflow, core].
	f.now = base.Add(2 * time.Minute)
	_, err = workflowCursorRead(t, f, "http", nil, 1, first.next, "project")
	workflowCursorError(t, err, "cursor_invalid")
	fresh, err := workflowCursorRead(t, f, "http", nil, 1, workflowContinuation{}, "project")
	if err != nil || !slices.Equal(fresh.ids, workflow) || !fresh.next.more() || !first.snapshot.Equal(fresh.snapshot) {
		t.Fatalf("fresh post-cutoff first page=%+v err=%v", fresh, err)
	}
	tail, err := workflowCursorRead(t, f, "http", nil, 1, fresh.next, "project")
	if err != nil || !slices.Equal(tail.ids, []string{coreID}) || tail.next.more() {
		t.Fatalf("fresh post-cutoff continuation omitted or duplicated a row: %+v err=%v", tail, err)
	}
	afterMeta, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	afterCheckpoint := workflowWriteCheckpoint(t, f)
	if beforeMeta.Incarnation != afterMeta.Incarnation || beforeMeta.Head.String() != afterMeta.Head.String() ||
		beforeCheckpoint.Generation != afterCheckpoint.Generation || !slices.Equal(beforeCheckpoint.Payload, afterCheckpoint.Payload) {
		t.Fatal("clock-only order crossing changed the source head or workflow checkpoint")
	}
}
