package s1b01b04

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
)

// Each category mapping is declared independently of either observed result.
// Native custom active maps to Plug unstarted; wip maps to started, done to
// completed, and frozen/pinned to backlog. This does not inject a native clock.
type nativeMembershipStep struct {
	Name, Method, Role, NativeStatus, PlugStatus, Refusal string
	Ready                                                 []string
}

func nativeMembershipSteps() []nativeMembershipStep {
	withoutFree := []string{"parent", "child", "grandchild"}
	return []nativeMembershipStep{
		{"status-pinned", "status", "free", "pinned", "backlog", "", withoutFree},
		{"status-started", "status", "free", "in_progress", "in_progress", "", withoutFree},
		{"status-active", "status", "free", "b01active", "b01active", "", nativeRoles},
		{"status-wip", "status", "free", "b01wip", "b01wip", "", withoutFree},
		{"status-done", "status", "free", "b01done", "b01done", "", withoutFree},
		{"status-frozen", "status", "free", "b01frozen", "b01frozen", "", withoutFree},
		{"status-closed", "close", "free", "closed", "done", "", withoutFree},
		{"status-open", "reopen", "free", "open", "todo", "", nativeRoles},
		{"status-deferred", "defer", "free", "deferred", "backlog", "", withoutFree},
		{"status-resumed", "resume", "free", "open", "todo", "", nativeRoles},
		{"close-parent-refused", "close", "parent", "open", "todo", "open child issue(s); close children first", nativeRoles},
		{"direct-leaf-blocked", "link", "grandchild", "open", "todo", "", []string{"parent", "child", "free"}},
		{"close-blocked-leaf-refused", "close", "grandchild", "open", "todo", "cannot close blocked issue", []string{"parent", "child", "free"}},
		{"direct-leaf-unblocked", "unlink", "grandchild", "open", "todo", "", nativeRoles},
		{"ancestor-blocked", "link", "parent", "open", "todo", "", []string{"free"}},
		{"close-under-blocked-ancestor", "close", "grandchild", "closed", "done", "", []string{"free"}},
		{"repeat-close-under-blocked-ancestor", "close", "grandchild", "closed", "done", "", []string{"free"}},
		{"reopen-under-blocked-ancestor", "reopen", "grandchild", "open", "todo", "", []string{"free"}},
		{"ancestor-unblocked", "unlink", "parent", "open", "todo", "", nativeRoles},
	}
}

type nativeMembershipNode struct {
	ID, Title, Status string
	Priority          int
	Parent            string
}

type nativeMembershipPhase struct {
	Nodes        map[string]nativeMembershipNode
	Ready        []string
	TypeAll      []string
	TypeEmpty    []string
	ExplainReady []nativeExplained
	TotalReady   int
	TotalBlocked int
}

func recordNativeMembership(t *testing.T, c *nativeCommand, vector nativeVector) map[string]nativeMembershipPhase {
	t.Helper()
	c.run("membership-config", "config", "set", "status.custom", "b01active:active,b01wip:wip,b01done:done,b01frozen:frozen")
	phases := map[string]nativeMembershipPhase{}
	for _, step := range nativeMembershipSteps() {
		if !t.Run("membership/"+step.Name, func(t *testing.T) {
			c.t = t
			before := nativeMembershipShows(t, c, vector, step.Name+"-before")
			id := vector.SourceIDs[step.Role]
			var args []string
			switch step.Method {
			case "status":
				args = []string{"update", id, "--status", step.NativeStatus, "--json"}
			case "resume":
				args = []string{"update", id, "--status", "open", "--defer", "", "--json"}
			case "link":
				args = []string{"dep", "add", id, vector.SourceIDs["free"], "--type", "blocks", "--json"}
			case "unlink":
				args = []string{"dep", "remove", id, vector.SourceIDs["free"], "--json"}
			case "defer":
				args = []string{"defer", id, "--reason", "native category control", "--json"}
			default:
				args = []string{step.Method, id, "--json"}
			}
			if step.Refusal != "" {
				c.refuse("membership-"+step.Name, step.Refusal, args...)
			} else {
				c.run("membership-"+step.Name, args...)
			}
			phase := nativeMembershipPhase{Nodes: map[string]nativeMembershipNode{}}
			after := nativeMembershipShows(t, c, vector, step.Name+"-after")
			for _, role := range nativeRoles {
				var issues []struct {
					ID, Title, Status string
					Priority          int
					Parent            *string
				}
				nativeDecode(t, after[role], &issues)
				if len(issues) != 1 || issues[0].ID != vector.SourceIDs[role] {
					t.Fatal("category show lost exact native identity")
				}
				v := issues[0]
				parent := ""
				if v.Parent != nil {
					parent = *v.Parent
				}
				phase.Nodes[role] = nativeMembershipNode{v.ID, v.Title, v.Status, v.Priority, parent}
				if role != step.Role {
					var prior []struct {
						ID, Title, Status string
						Priority          int
						Parent            *string
					}
					nativeDecode(t, before[role], &prior)
					if len(prior) != 1 || prior[0].ID != v.ID || prior[0].Title != v.Title || prior[0].Status != v.Status || prior[0].Priority != v.Priority || !reflect.DeepEqual(prior[0].Parent, v.Parent) {
						t.Fatal("native write changed unrelated identity/title/status/priority/parent")
					}
				}
				if (step.Refusal != "" || step.Name == "repeat-close-under-blocked-ancestor") && !bytes.Equal(before[role], after[role]) {
					t.Fatalf("%s changed refused/settled native %s show bytes", step.Name, role)
				}
			}
			if phase.Nodes[step.Role].Status != step.NativeStatus {
				t.Fatalf("native status=%s want=%s", phase.Nodes[step.Role].Status, step.NativeStatus)
			}
			phase.Ready = nativeMembershipReady(t, c, "membership-"+step.Name)
			phase.TypeAll = nativeMembershipReady(t, c, "membership-"+step.Name+"-type-task", "--type", "task")
			phase.TypeEmpty = nativeMembershipReady(t, c, "membership-"+step.Name+"-type-bug", "--type", "bug")
			want := make([]string, 0, len(step.Ready))
			for _, role := range step.Ready {
				want = append(want, vector.SourceIDs[role])
			}
			if !reflect.DeepEqual(phase.Ready, want) || !reflect.DeepEqual(phase.TypeAll, want) || len(phase.TypeEmpty) != 0 {
				t.Fatalf("actual category/type membership differs: %+v want=%v", phase, want)
			}
			var explanation struct {
				Ready   []nativeExplained
				Blocked []nativeBlocked
				Summary struct {
					Ready   *int `json:"total_ready"`
					Blocked *int `json:"total_blocked"`
				}
			}
			data := c.run("membership-"+step.Name+"-explain", "ready", "--json", "--sort", "hybrid", "--explain", "--limit", "0")
			nativeDecode(t, data, &explanation)
			if explanation.Summary.Ready == nil || explanation.Summary.Blocked == nil || *explanation.Summary.Ready != len(explanation.Ready) || *explanation.Summary.Blocked != len(explanation.Blocked) {
				t.Fatal("category explain totals disagree with original arrays")
			}
			var explained []string
			for _, v := range explanation.Ready {
				if v.Reason == "" {
					t.Fatal("category explain omitted its reason")
				}
				explained = append(explained, v.ID)
			}
			if !reflect.DeepEqual(explained, want) {
				t.Fatal("category explain differs from unbounded actual membership")
			}
			phase.ExplainReady, phase.TotalReady, phase.TotalBlocked = explanation.Ready, *explanation.Summary.Ready, *explanation.Summary.Blocked
			phases[step.Name] = phase
			nativeJSON(t, filepath.Join(c.output, "membership-"+step.Name+"-projection.json"), phase)
			t.Logf("B01_NATIVE_MEMBERSHIP phase=%s vector=%s ready=%v", step.Name, vector.ID, phase.Ready)
		}) {
			c.t = t
			t.Fatalf("native membership %s failed; originals retained", step.Name)
		}
		c.t = t
	}
	return phases
}

func nativeMembershipShows(t *testing.T, c *nativeCommand, vector nativeVector, label string) map[string][]byte {
	t.Helper()
	rows := map[string][]byte{}
	for _, role := range nativeRoles {
		rows[role] = c.run("membership-"+label+"-show-"+role, "show", vector.SourceIDs[role], "--json")
	}
	return rows
}

func nativeMembershipReady(t *testing.T, c *nativeCommand, label string, filters ...string) []string {
	t.Helper()
	args := append([]string{"ready", "--json", "--sort", "hybrid", "--limit", "0"}, filters...)
	var rows []struct{ ID string }
	nativeDecode(t, c.run(label, args...), &rows)
	ids := []string{}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.ID == "" || seen[row.ID] {
			t.Fatal("native membership has an empty or duplicate identity")
		}
		seen[row.ID] = true
		ids = append(ids, row.ID)
	}
	return ids
}

func (r *nativeReplay) replayMembershipContinuations(capture nativeCapture, coverage *nativeReplayCoverage) {
	parent := r.f.t
	for _, step := range nativeMembershipSteps() {
		if !parent.Run("membership/"+step.Name, func(t *testing.T) {
			r.f.t = t
			defer func() { r.f.t = parent }()
			if coverage != nil {
				coverage.Membership[step.Name] = true
			}
			r.configureMembershipSlots(step)
			observed, ok := capture.Membership[step.Name]
			if !ok || len(observed.Nodes) != len(nativeRoles) {
				t.Fatal("missing genuine native membership phase")
			}
			if step.Method == "status" {
				before := r.snapshot()
				r.refuseMembership("membership-"+step.Name+"-direct-status-refused", "bead.update", map[string]any{
					"id": r.refs[step.Role], "fields": map[string]any{"status": step.PlugStatus},
				}, "ontology_use_slot_verb")
				after := r.snapshot()
				if !bytes.Equal(before, after) {
					t.Fatal("direct status refusal changed complete source/derived/history/receipt state")
				}
				nativeWrite(t, filepath.Join(r.output, "membership-"+step.Name+"-status-refusal-before.json"), before)
				nativeWrite(t, filepath.Join(r.output, "membership-"+step.Name+"-status-refusal-after.json"), after)
			}
			// Claim only accepts an unstarted source. Restore the deferred
			// pinned mapping through its real reopen slot before claiming it.
			if step.Name == "status-started" {
				r.membershipReopenBeforeClaim(step)
			}
			before := r.snapshot()
			beforeDocs := map[string]json.RawMessage{}
			for _, role := range nativeRoles {
				beforeDocs[role] = nativeDocumentBytes(t, r.rawHierarchyDocument("membership-"+step.Name+"-before", role))
			}
			method := "bead." + step.Method
			params := map[string]any{"id": r.refs[step.Role]}
			switch step.Method {
			case "status":
				// Status keys are selected by the configured canonical slots,
				// never by an ordinary caller-supplied status patch.
				switch step.Name {
				case "status-pinned", "status-frozen":
					method, params["reason"] = "bead.defer", "native category control"
				case "status-started", "status-wip":
					method = "bead.claim"
				case "status-active":
					method = "bead.reopen"
				case "status-done":
					method = "bead.close"
				default:
					t.Fatal("category phase lacks its explicit lifecycle route")
				}
			case "resume":
				method = "bead.reopen"
			case "link", "unlink":
				params = map[string]any{"from": r.refs[step.Role], "to": r.refs["free"], "kind": "blocks"}
			case "defer":
				params["reason"] = "native category control"
			}
			if step.Refusal != "" {
				r.refuseMembership("membership-"+step.Name, method, params, "ontology_guard_failed")
				if !bytes.Equal(before, r.snapshot()) {
					t.Fatal("refused close changed complete source/derived/history/receipt state")
				}
			} else {
				commit := r.write("membership-"+step.Name, method, params)
				if step.Name == "repeat-close-under-blocked-ancestor" {
					r.assertMembershipSettlement(before, commit)
				} else {
					r.assertHierarchyWrite("membership-"+step.Name, step.Role, before, commit)
				}
			}
			for _, role := range nativeRoles {
				after := r.rawHierarchyDocument("membership-"+step.Name+"-after", role)
				if role != step.Role || step.Refusal != "" || step.Name == "repeat-close-under-blocked-ancestor" {
					if !bytes.Equal(beforeDocs[role], nativeDocumentBytes(t, after)) {
						t.Fatal("category write changed unrelated/refused/settled document")
					}
				} else {
					var prior pipeline.Document
					nativeDecode(t, beforeDocs[role], &prior)
					r.assertMembershipNonTargetFields(step.Name, method, params, prior, after)
				}
			}
			for _, suffix := range []string{"", "-reopened"} {
				if suffix != "" {
					if r.f.backend == "memory" {
						break
					}
					first := r.call("membership-"+step.Name+"-reopen-first", "bead.ready", map[string]any{"limit": 1}, false)
					if len(step.Ready) > 1 {
						if cursor, _ := first["nextCursor"].(string); cursor == "" {
							t.Fatal("membership multirow result lacks its real signed cursor")
						}
					}
					r.reopen("membership-" + step.Name)
					if !reflect.DeepEqual(r.drain("membership-"+step.Name+"-across-reopen", first), r.wantIDs(step.Ready)) {
						t.Fatal("membership signed cursor changed across physical SQL reopen")
					}
				}
				r.observeMembership(capture, step, observed, "membership-"+step.Name+suffix)
			}
			t.Logf("B01_NATIVE_MEMBERSHIP_REPLAY phase=%s engine=%s roles=%v", step.Name, r.f.backend, step.Ready)
		}) {
			parent.Fatalf("membership replay %s failed; originals retained", step.Name)
		}
	}
}

// Custom categories use real workflow slot configuration. Each custom target
// retains the corresponding built-in slot's guards; claim retains its explicit
// unstarted source restriction. No production validation is bypassed.
func (r *nativeReplay) configureMembershipSlots(step nativeMembershipStep) {
	base := ontology.DefaultDocument()
	workflow := base.Workflows["issue"]
	for _, entry := range []struct {
		name   string
		status ontology.Status
	}{
		{"b01active", ontology.Status{Name: "B01 active", Category: ontology.Unstarted}},
		{"b01wip", ontology.Status{Name: "B01 work in progress", Category: ontology.Started}},
		{"b01done", ontology.Status{Name: "B01 completed", Category: ontology.Completed, Outcome: ontology.Success}},
		{"b01frozen", ontology.Status{Name: "B01 frozen", Category: ontology.Backlog}},
	} {
		base.Statuses[entry.name] = entry.status
		workflow.Statuses = append(workflow.Statuses, entry.name)
	}
	for _, binding := range []struct {
		from, to string
		slot     ontology.Slot
	}{
		{"todo", "b01active", ontology.Reopen},
		{"in_progress", "b01wip", ontology.Claim},
		{"done", "b01done", ontology.Close},
		{"backlog", "b01frozen", ontology.Defer},
	} {
		for _, original := range ontology.DefaultDocument().Workflows["issue"].Transitions {
			if original.To != binding.from {
				continue
			}
			custom := original
			custom.To = binding.to
			if binding.slot == ontology.Claim {
				custom.From = []string{"b01active"}
			}
			workflow.Transitions = append(workflow.Transitions, custom)
		}
		switch step.Name {
		case "status-active", "status-wip", "status-done", "status-frozen":
			workflow.Slots[binding.slot] = binding.to
		}
	}
	base.Workflows["issue"] = workflow
	compiled, problems := ontology.Compile(base, nil, nil)
	if len(problems) != 0 {
		r.f.t.Fatalf("explicit category ontology: %+v", problems)
	}
	r.f.ontology.Refresh(compiled)
	nativeJSON(r.f.t, filepath.Join(r.output, "membership-"+step.Name+"-ontology-slots.json"), base)
}

func (r *nativeReplay) membershipReopenBeforeClaim(step nativeMembershipStep) {
	before := r.snapshot()
	beforeImages := r.sourceImages()
	prior := r.rawHierarchyDocument("membership-"+step.Name+"-prepare-before", step.Role)
	params := map[string]any{"id": r.refs[step.Role]}
	commit := r.write("membership-"+step.Name+"-prepare-reopen", "bead.reopen", params)
	r.assertHierarchyWrite("membership-"+step.Name+"-prepare-reopen", step.Role, before, commit)
	after := r.rawHierarchyDocument("membership-"+step.Name+"-prepare-after", step.Role)
	if after.Fields["status"] != "todo" || after.Lease != nil {
		r.f.t.Fatal("claim preparation did not enter its genuine unstarted reopen slot")
	}
	r.assertMembershipNonTargetFields(step.Name+"-prepare-reopen", "bead.reopen", params, prior, after)
	afterImages := r.sourceImages()
	for role, image := range beforeImages {
		if role != step.Role && image != afterImages[role] {
			r.f.t.Fatal("claim preparation changed an unrelated source image")
		}
	}
}

func (r *nativeReplay) assertMembershipNonTargetFields(label, method string, params map[string]any, before, after pipeline.Document) {
	r.assertLifecycleCloseReason(label, method, params, before, after)
	prior, current := map[string]any{}, map[string]any{}
	nativeDecode(r.f.t, nativeDocumentBytes(r.f.t, before), &prior)
	nativeDecode(r.f.t, nativeDocumentBytes(r.f.t, after), &current)
	if method == "bead.link" || method == "bead.unlink" {
		delete(prior, "links")
		delete(current, "links")
	} else {
		delete(prior["fields"].(map[string]any), "status")
		delete(current["fields"].(map[string]any), "status")
		if reason, _ := params["reason"].(string); reason != "" {
			if len(after.Comments) != len(before.Comments)+1 ||
				after.Comments[len(before.Comments)].Body != reason {
				r.f.t.Fatal("category reason did not append exactly its one unchanged-prefix comment")
			}
			for i, comment := range before.Comments {
				if !reflect.DeepEqual(comment, after.Comments[i]) {
					r.f.t.Fatal("category reason changed an existing comment")
				}
			}
			delete(prior, "comments")
			delete(current, "comments")
		}
		if method == "bead.claim" {
			if before.Lease != nil || after.Lease == nil || after.Lease.Agent != owner ||
				!reflect.DeepEqual(after.Lease.Principal, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}) ||
				!after.Lease.ExpiresAt.Equal(r.f.clock.Now().Add(5*time.Minute)) ||
				after.Lease.TtlMs != model.Natural64(300000) || after.Lease.Epoch != model.Natural64(r.f.lease.Epoch()) {
				r.f.t.Fatal("membership claim lacks its exact ordinary durable lease binding")
			}
		} else if after.Lease != nil {
			r.f.t.Fatal("non-claim membership lifecycle retained a lease")
		}
		delete(prior, "lease")
		delete(current, "lease")
		for _, key := range []string{"startedAt", "closedAt", "closeReason"} {
			delete(prior, key)
			delete(current, key)
		}
	}
	if !reflect.DeepEqual(prior, current) {
		r.f.t.Fatalf("%s changed non-target fields/project/type/identity", label)
	}
	nativeJSON(r.f.t, filepath.Join(r.output, "membership-"+label+"-non-target-fields.json"), map[string]any{
		"method": method, "before": before, "after": after, "conservedProjection": prior,
	})
}

// A first close stores its exact reason, including an explicit empty reason.
// Genuine reopen/release/defer transitions clear it; settlement and unrelated
// operations preserve the original value, including the distinction from nil.
func (r *nativeReplay) assertLifecycleCloseReason(label, method string, params map[string]any, before, after pipeline.Document) {
	r.f.t.Helper()
	want := before.CloseReason
	if before.Fields["status"] != after.Fields["status"] {
		switch method {
		case "bead.close":
			reason, _ := params["reason"].(string)
			want = &reason
		case "bead.reopen", "bead.release", "bead.defer":
			want = nil
		}
	}
	if !reflect.DeepEqual(after.CloseReason, want) {
		r.f.t.Fatalf("%s %s closeReason=%s want=%s", label, method,
			nativeDocumentBytes(r.f.t, after.CloseReason), nativeDocumentBytes(r.f.t, want))
	}
}

func nativeDocumentBytes(t *testing.T, doc any) []byte {
	t.Helper()
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// RPC results pass through a map before Commit is decoded, so a raw record's
// object keys can be reordered. Compare the complete JSON values: UseNumber
// retains number tokens, and DeepEqual retains array order and null/empty.
func nativeCommitEqual(t *testing.T, receipt, response model.Commit) bool {
	t.Helper()
	var prior, current any
	nativeDecode(t, nativeDocumentBytes(t, receipt), &prior)
	nativeDecode(t, nativeDocumentBytes(t, response), &current)
	return reflect.DeepEqual(prior, current)
}

func (r *nativeReplay) observeMembership(capture nativeCapture, step nativeMembershipStep, observed nativeMembershipPhase, label string) {
	before, epoch := r.snapshot(), r.f.lease.Epoch()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-before.json"), before)
	for _, role := range nativeRoles {
		value := r.call(label+"-get-"+role, "bead.get", map[string]any{"id": r.refs[role]}, false)
		bead, ok := value["bead"].(map[string]any)
		actual := observed.Nodes[role]
		wantStatus := map[string]string{"open": "todo", "in_progress": "in_progress", "closed": "done", "deferred": "backlog", "pinned": "backlog", "b01active": "b01active", "b01wip": "b01wip", "b01done": "b01done", "b01frozen": "b01frozen"}[actual.Status]
		if !ok || wantStatus == "" || bead["id"] != r.refs[role].ID || bead["status"] != wantStatus || bead["project"] != project || bead["type"] != "issue" || bead["fields"].(map[string]any)["title"] != actual.Title {
			r.f.t.Fatalf("%s category/public identity differs for %s: %+v", label, role, bead)
		}
	}
	queries := []struct {
		Name   string
		Filter map[string]any
		Want   []string
	}{
		{"all", nil, step.Ready},
		{"type-positive", map[string]any{"types": []string{"issue"}}, step.Ready},
		{"type-empty", map[string]any{"types": []string{"absent-type"}}, []string{}},
		{"resource-negative-prefix", map[string]any{"resources": []string{r.refs["parent"].ID, r.refs["free"].ID}}, nil},
		{"intersection-empty", map[string]any{"types": []string{"absent-type"}, "resources": []string{r.refs["free"].ID}}, []string{}},
	}
	for _, role := range step.Ready {
		if role == "parent" || role == "free" {
			queries[3].Want = append(queries[3].Want, role)
		}
	}
	if queries[3].Want == nil {
		queries[3].Want = []string{}
	}
	actualQueries := map[string][]string{}
	for _, query := range queries {
		params := map[string]any{"limit": 1000}
		if query.Filter != nil {
			params["filter"] = query.Filter
		}
		full := r.call(label+"-"+query.Name+"-full", "bead.ready", params, false)
		params["limit"] = 1
		first := r.call(label+"-"+query.Name+"-one", "bead.ready", params, false)
		actualQueries[query.Name] = r.ids(full)
		if !reflect.DeepEqual(r.ids(full), r.wantIDs(query.Want)) || !reflect.DeepEqual(r.drain(label+"-"+query.Name, first, query.Filter), r.wantIDs(query.Want)) {
			r.f.t.Fatalf("%s/%s filters or page cut changed membership", label, query.Name)
		}
	}
	after := r.snapshot()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-after.json"), after)
	if !bytes.Equal(before, after) || epoch != r.f.lease.Epoch() {
		r.f.t.Fatal("category/filter reads changed complete source/derived/history/receipts/authority")
	}
	select {
	case <-r.f.lease.Lost():
		r.f.t.Fatal("category/filter reads lost authority")
	default:
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-membership-comparison.json"), map[string]any{
		"actualNative": observed, "expectedRoles": step.Ready, "actualPlugQueryIDs": actualQueries,
		"membershipParity": true, "nativeType": "task", "plugType": "issue", "categoryMapping": "explicit declared category mapping",
		"filterScope":  "Native task/bug type filters; Plug issue/absent type and additional resource/intersection filters; no native resource API equivalence claim",
		"nativeVector": capture.Vector.ID, "nativeRun": capture.Run.ID,
	})
}

func (r *nativeReplay) refuseMembership(label, method string, params map[string]any, code string) {
	key := r.f.key(label, method, owner)
	params["_meta"] = map[string]any{"project": project, "agent": owner, "idempotencyKey": key}
	request := nativeDocumentBytes(r.f.t, map[string]any{"jsonrpc": "2.0", "id": len(r.calls) + 1, "method": method, "params": params})
	stem := filepath.Join(r.output, fmt.Sprintf("%03d-%s", len(r.calls)+1, label))
	nativeWrite(r.f.t, stem+".request.json", request)
	started := time.Now().UTC()
	response, err := r.f.transport.RoundTrip(r.f.ctx, request)
	nativeWrite(r.f.t, stem+".response.json", response)
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	nativeJSON(r.f.t, stem+".receipt.json", map[string]any{
		"method": method, "requestSHA256": sha256Hex(request), "requestBytes": len(request),
		"responseSHA256": sha256Hex(response), "responseBytes": len(response),
		"startedUTC": started, "endedUTC": time.Now().UTC(), "transportError": errorText, "expectedErrorCode": code,
	})
	r.calls = append(r.calls, nativeRPCScope{Method: method, Key: key})
	var envelope struct {
		ID    int `json:"id"`
		Error *struct {
			Data struct {
				Code string `json:"code"`
			} `json:"data"`
		} `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	nativeDecode(r.f.t, response, &envelope)
	if err != nil || envelope.ID != len(r.calls) || envelope.Error == nil || envelope.Error.Data.Code != code || len(envelope.Result) != 0 {
		r.f.t.Fatalf("exact typed refusal %s: %v %s", label, err, response)
	}
}

func (r *nativeReplay) assertMembershipSettlement(before []byte, commit model.Commit) {
	r.f.t.Helper()
	var old, after struct {
		Meta     map[string]any             `json:"meta"`
		Versions map[string]json.RawMessage `json:"versions"`
		Source   string                     `json:"sourceAndChangesetSHA256"`
		Derived  map[string]json.RawMessage `json:"allNineDerivedTables"`
		Facts    json.RawMessage            `json:"graphValidityFact"`
		Receipts map[string]json.RawMessage `json:"writeReceipts"`
	}
	nativeDecode(r.f.t, before, &old)
	afterSnapshot := r.snapshot()
	// Preserve both complete states and the response even if a later oracle
	// assertion fails. These observations alone do not certify settlement.
	base := filepath.Join(r.output, "membership-repeat-close-under-blocked-ancestor-settlement")
	nativeWrite(r.f.t, base+"-before.json", before)
	nativeWrite(r.f.t, base+"-after.json", afterSnapshot)
	nativeJSON(r.f.t, base+"-returned-commit.json", commit)
	nativeDecode(r.f.t, afterSnapshot, &after)
	if len(r.calls) == 0 {
		r.f.t.Fatal("quiet settlement lacks its actual RPC scope")
	}
	call := r.calls[len(r.calls)-1]
	key := call.Method + "/" + call.Key
	if !call.Write || call.Method != "bead.close" || call.Key == "" || old.Receipts == nil || after.Receipts == nil {
		r.f.t.Fatal("quiet settlement lacks its exact close method/key or receipt maps")
	}
	if _, exists := old.Receipts[key]; exists {
		r.f.t.Fatal("quiet settlement key already had a receipt before this call")
	}
	added, exists := after.Receipts[key]
	if !exists || len(after.Receipts) != len(old.Receipts)+1 {
		r.f.t.Fatal("quiet settlement must add precisely its one method/key receipt")
	}
	for priorKey, prior := range old.Receipts {
		if !bytes.Equal(prior, after.Receipts[priorKey]) {
			r.f.t.Fatalf("quiet settlement changed prior receipt %s", priorKey)
		}
	}
	var receipt nativeReceiptEvidence
	nativeWrite(r.f.t, base+"-receipt.json", added)
	nativeDecode(r.f.t, added, &receipt)
	commitEqual := nativeCommitEqual(r.f.t, receipt.Commit, commit)
	nativeJSON(r.f.t, base+"-observation.json", map[string]any{
		"method": call.Method, "idempotencyKey": call.Key, "receiptKey": key,
		"completedReceipt": receipt, "returnedCommit": commit,
		"commitJSONValuesEqual": commitEqual,
		"commitJSONBytesEqual":  bytes.Equal(nativeDocumentBytes(r.f.t, receipt.Commit), nativeDocumentBytes(r.f.t, commit)),
	})
	if receipt.ResponsePending || len(receipt.RequestHash) != 64 ||
		!commitEqual ||
		!receipt.ZeroChangeset || receipt.Changeset != nil {
		r.f.t.Fatal("quiet settlement receipt is not a completed no-op bound to the returned commit")
	}
	// Conserve every prior receipt above and allow only the new scoped receipt;
	// the remaining snapshot must stay byte-equivalent in every source plane.
	old.Receipts, after.Receipts = nil, nil
	if !reflect.DeepEqual(old, after) || fmt.Sprint(old.Meta["Head"]) != commit.Revision.String() {
		r.f.t.Fatal("repeated close changed source/history/derived facts/head instead of settling quietly")
	}
	nativeJSON(r.f.t, filepath.Join(r.output, "membership-repeat-close-under-blocked-ancestor-settlement.json"), map[string]any{
		"before": json.RawMessage(before), "after": json.RawMessage(afterSnapshot),
		"method": call.Method, "idempotencyKey": call.Key, "receiptKey": key,
		"completedReceipt": receipt, "returnedCommit": commit,
	})
}

// Local hierarchy writes may change Parent/Links, and lifecycle operations may
// change their status, core timestamps, and the asserted close reason. Every
// remaining source field stays exact, including project/type/title/createdAt/metadata.
func (r *nativeReplay) assertHierarchyNonTargetFields(label, method string, before, after pipeline.Document) {
	r.assertLifecycleCloseReason(label, method, nil, before, after)
	prior, current := map[string]any{}, map[string]any{}
	nativeDecode(r.f.t, nativeDocumentBytes(r.f.t, before), &prior)
	nativeDecode(r.f.t, nativeDocumentBytes(r.f.t, after), &current)
	if method == "bead.link" || method == "bead.unlink" || method == "tx.apply" {
		for _, key := range []string{"parent", "links"} {
			delete(prior, key)
			delete(current, key)
		}
	} else {
		delete(prior["fields"].(map[string]any), "status")
		delete(current["fields"].(map[string]any), "status")
		for _, key := range []string{"startedAt", "closedAt", "closeReason"} {
			delete(prior, key)
			delete(current, key)
		}
	}
	if !reflect.DeepEqual(prior, current) {
		r.f.t.Fatalf("%s changed non-target source fields", label)
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-non-target-fields.json"), map[string]any{"method": method, "before": before, "after": after, "conservedProjection": prior})
}
