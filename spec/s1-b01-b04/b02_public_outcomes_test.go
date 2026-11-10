package s1b01b04

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// This is public Plug acceptance. No native capture is constructed, loaded or
// credited. The physical recorder uses the same write, receipt, event and
// reopen verifiers only after obtaining its required real native observations.
func TestB02PublicConditionalOutcomeReplayAndReopenAcrossStores(t *testing.T) {
	h := newPostgresHarness(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			dsn := ""
			if engine == "postgres" {
				dsn = h.dsn(t)
			}
			roles := []string{"blocker", "ordinary", "conditional", "unrelated"}
			ids := []string{"b02public-blocker", "b02public-ordinary", "b02public-conditional", "b02public-unrelated"}
			f := newFixture(t, engine, dsn, compileOntology(t, customDocument(), nil), ids)
			root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if root == "" {
				root = t.TempDir()
			}
			output, err := os.MkdirTemp(root, "b02-public-outcomes-"+engine+"-")
			if err != nil {
				t.Fatal(err)
			}
			r := &b02Replay{f: f, output: output, refs: map[string]model.Ref{}, statuses: map[string]string{}}
			nativeJSON(t, filepath.Join(output, "public-scope.json"), map[string]any{
				"engine": engine, "ontology": customDocument(),
				"qualification": "public Plug outcome/event/replay/reopen acceptance; native recording NOT_RUN",
			})
			for i, role := range roles {
				commit := r.write("create-"+role, "bead.create", map[string]any{
					"type": "issue", "fields": map[string]any{"title": "B02 public " + role, "priority": i},
				}, ids[i])
				if len(commit.Results) != 1 || commit.Results[0].Ref.ID != ids[i] || !commit.Results[0].Ref.Branch.IsZero() {
					t.Fatal("public create did not return the allocated durable ref")
				}
				r.refs[role], r.statuses[role] = commit.Results[0].Ref, "queued"
			}
			for _, edge := range []struct{ role, kind string }{{"ordinary", "blocks"}, {"conditional", "conditional-blocks"}} {
				r.write("link-"+edge.role, "bead.link", map[string]any{
					"from": r.refs[edge.role], "to": r.refs["blocker"], "kind": edge.kind,
				}, r.refs[edge.role].ID)
			}
			r.refuse("blocked-dependent-close", "bead.close", map[string]any{"id": r.refs["conditional"]}, "ontology_guard_failed")
			b02ObservePublicOutcome(t, r, "initial", roles, []string{"blocker", "unrelated"})
			steps := []struct {
				name, method, status string
				ready                []string
				terminal             bool
			}{
				{"already-open-noop", "bead.reopen", "queued", []string{"blocker", "unrelated"}, false},
				{"success", "bead.close", "finished", []string{"ordinary", "unrelated"}, true},
				{"reopen-success", "bead.reopen", "queued", []string{"blocker", "unrelated"}, false},
				{"failure", "bead.fail", "broken", []string{"ordinary", "conditional", "unrelated"}, true},
				{"reopen-failure", "bead.reopen", "queued", []string{"blocker", "unrelated"}, false},
				{"cancel", "bead.cancel", "abandoned", []string{"ordinary", "unrelated"}, true},
				{"reopen-cancel", "bead.reopen", "queued", []string{"blocker", "unrelated"}, false},
				{"backlog", "bead.defer", "parked", []string{"unrelated"}, false},
				{"reopen-backlog", "bead.reopen", "queued", []string{"blocker", "unrelated"}, false},
			}
			for _, step := range steps {
				parent := t
				if !t.Run(step.name, func(t *testing.T) {
					r.f.t = t
					defer func() { r.f.t = parent }()
					commit := r.write(step.name, step.method, map[string]any{
						"id": r.refs["blocker"], "reason": "B02 public " + step.name,
					}, r.refs["blocker"].ID)
					r.statuses["blocker"] = step.status
					if step.terminal {
						r.assertTerminalEvents(step.name, step.method, commit)
					}
					b02ObservePublicOutcome(t, r, step.name, roles, step.ready)
					if step.terminal {
						// Re-read the exact event plan after physical SQL reopen.
						r.assertTerminalEvents(step.name+"-retained", step.method, commit)
					}
				}) {
					t.Fatalf("public B02 %s failed; later outcomes NOT_RUN", step.name)
				}
			}
			t.Logf("B02_PUBLIC_OUTCOMES engine=%s phases=%d terminal_event_plans=3 same_key_replays=15 native=NOT_RUN", engine, 1+len(steps))
		})
	}
}

func b02ObservePublicOutcome(t *testing.T, r *b02Replay, label string, roles, want []string) {
	t.Helper()
	r.renew()
	before := r.snapshot()
	nativeJSON(t, filepath.Join(r.output, label+"-public-reads-before.json"), before)
	for _, role := range roles {
		value := r.read(label+"-get-"+role, "bead.get", map[string]any{"id": r.refs[role]})
		bead, ok := value["bead"].(map[string]any)
		fields, fieldsOK := bead["fields"].(map[string]any)
		if !ok || !fieldsOK || bead["id"] != r.refs[role].ID || bead["status"] != r.statuses[role] || fields["title"] != "B02 public "+role {
			t.Fatalf("public %s identity, status or title differs: %v", role, bead)
		}
	}
	full := r.read(label+"-ready-full", "bead.ready", map[string]any{"limit": 1000})
	page := r.read(label+"-ready-first", "bead.ready", map[string]any{"limit": 1})
	all, drained := r.readyRoles(full), r.readyRoles(page)
	wantSnapshot := map[string]any{"revision": before.Meta.Head, "incarnation": before.Meta.Incarnation}
	if !bytes.Equal(b02Bytes(t, full["asOf"]), b02Bytes(t, wantSnapshot)) ||
		!bytes.Equal(b02Bytes(t, full["asOf"]), b02Bytes(t, page["asOf"])) {
		t.Fatal("public ready pages do not bind the actual head and incarnation")
	}
	if next, _ := full["nextCursor"].(string); next != "" {
		t.Fatal("four-record public full page is truncated")
	}
	if len(all) > 1 {
		if next, _ := page["nextCursor"].(string); next == "" {
			t.Fatal("public one-row page lacks the required continuation")
		}
	}
	if r.f.backend != "memory" {
		r.reopen(label)
	}
	seen := map[string]bool{}
	for index := 0; ; index++ {
		cursor, _ := page["nextCursor"].(string)
		if cursor == "" {
			break
		}
		if seen[cursor] || index >= len(roles) {
			t.Fatal("public continuation repeated or exceeded the complete finite graph")
		}
		seen[cursor] = true
		page = r.read(fmt.Sprintf("%s-ready-next-%d", label, index), "bead.ready", map[string]any{"limit": 1, "cursor": cursor})
		if !bytes.Equal(b02Bytes(t, full["asOf"]), b02Bytes(t, page["asOf"])) {
			t.Fatal("public continuation changed its snapshot across reopen")
		}
		drained = append(drained, r.readyRoles(page)...)
	}
	if !reflect.DeepEqual(all, drained) || !reflect.DeepEqual(sortedStrings(all), sortedStrings(want)) {
		t.Fatalf("public full/paged/expected membership = %v / %v / %v", all, drained, want)
	}
	after := r.snapshot()
	nativeJSON(t, filepath.Join(r.output, label+"-public-reads-after.json"), after)
	if !bytes.Equal(b02Bytes(t, before), b02Bytes(t, after)) {
		t.Fatal("public reads or reopen changed records/history/versions/receipts/derived/facts")
	}
	t.Logf("B02_PUBLIC_PHASE engine=%s phase=%s ready=%v retained_state=conserved", r.f.backend, label, all)
}
