package cardb20

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

type b24PublicOutcome struct {
	Selected             []string  `json:"selected_stale_ids"`
	NonpositiveRefused   int       `json:"nonpositive_ages_refused"`
	Reason               string    `json:"exact_reason"`
	BlankRefused         bool      `json:"blank_reason_refused"`
	FutureExcluded       bool      `json:"future_deferred_excluded"`
	ExplicitReopenReady  bool      `json:"explicit_reopen_ready"`
	TimedRequestsRefused int       `json:"target_time_requests_refused"`
	Target               time.Time `json:"comparison_target"`
	Reopen               string    `json:"persistence_scope"`
}

func b24Retain(t *testing.T, name string, value any) {
	t.Helper()
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("absolute declared output directory required")
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func b24ReadyContains(t *testing.T, f *fixture, label, id string) bool {
	t.Helper()
	got := f.call("bead.ready", label, map[string]any{"limit": 100})
	if got.Error != nil {
		t.Fatalf("ready refusal: %+v", got.Error)
	}
	var page struct {
		Items []struct{ ID string } `json:"items"`
	}
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatal(err)
	}
	for _, item := range page.Items {
		if item.ID == id {
			return true
		}
	}
	return false
}

func b24RunPublicCounterpart(t *testing.T, engine string, target time.Time) b24PublicOutcome {
	t.Helper()
	f := newFixture(t, engine)
	f.ids = []string{"b24-old", "b24-closed", "b24-younger", "b24-deferred"}
	application := testclock.New(t, target.Add(-48*time.Hour))
	b24BindApplicationClock(t, f, application)
	origin := application.Now()
	tick := time.Nanosecond
	if engine == "postgres" {
		tick = time.Microsecond
	}
	old, _ := f.create(t, "b24-public-old", "old")
	closed, _ := f.create(t, "b24-public-closed", "closed")
	commit(t, f.call("bead.close", "b24-public-close", map[string]any{"id": closed, "reason": "completed"}))
	b24StepClocks(t, f, application, 2*tick, 0)
	f.create(t, "b24-public-younger", "younger")
	for _, boundary := range []struct {
		name  string
		delta time.Duration
		want  []string
	}{
		{"before", 24*time.Hour - 3*tick, nil},
		{"at", tick, nil},
		{"after", tick, []string{old.ID}},
	} {
		b24StepClocks(t, f, application, boundary.delta, 0)
		b24StaleRead(t, f, "b24-public-stale-"+boundary.name, boundary.want)
		b24ObserveClocks(t, f, application, "b24-public-stale-"+boundary.name)
		before := f.state(t)
		b24ReopenClocks(t, f, application)
		sameState(t, before, f.state(t))
		b24StaleRead(t, f, "b24-public-stale-"+boundary.name+"-reopened", boundary.want)
	}
	if !application.Now().Equal(origin.Add(24*time.Hour + tick)) {
		t.Fatal("strict stale cutoff clock drift")
	}
	outcome := b24PublicOutcome{Selected: []string{old.ID}, Target: target, Reopen: "SAME_STORE_MEMORY_ONLY"}
	if engine != "memory" {
		outcome.Reopen = "ACTUAL_SQL_CLOSE_OPEN"
	}
	for _, age := range []int{0, -1} {
		b24ConservedRead(t, f, fmt.Sprintf("b24-public-invalid-age-%d", age), func() {
			got := f.call("view.run", fmt.Sprintf("invalid-age-%d", age), map[string]any{
				"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": age},
			})
			if got.Error == nil || got.Error.Data.Code != "invalid_params" {
				t.Fatalf("nonpositive age=%d response=%+v", age, got)
			}
			outcome.NonpositiveRefused++
		})
	}
	ref, _ := f.create(t, "b24-public-defer-target", "defer preserves title")
	commit(t, f.call("bead.comment", "b24-public-existing-notes", map[string]any{"id": ref, "body": "existing notes"}))
	prior := f.state(t)
	blank := func() {
		got := f.call("bead.defer", "b24-public-blank", map[string]any{"id": ref, "reason": " "})
		if got.Error == nil || got.Error.Data.Code != "invalid_params" {
			t.Fatalf("blank reason response=%+v", got)
		}
		outcome.BlankRefused = true
	}
	b24ConservedRead(t, f, "b24-public-blank-reason", blank)
	reason := "waiting for API"
	request := f.request("bead.defer", "b24-public-defer", map[string]any{"id": ref, "reason": reason})
	accepted := commit(t, f.dispatch(request))
	if len(accepted.Results) != 2 {
		t.Fatal("defer must publish one patch and one reason comment")
	}
	for _, result := range accepted.Results {
		doc, err := pipeline.DecodeDocument(result.Record)
		if err != nil || doc.Fields["status"] != "backlog" || doc.Fields["title"] != "defer preserves title" || len(doc.Comments) != 2 || doc.Comments[0].Body != "existing notes" || doc.Comments[1].Body != reason {
			t.Fatalf("defer did not preserve exact ordered reason/comments: %+v %v", doc, err)
		}
	}
	parked := f.state(t)
	if len(parked.Versions[ref.ID]) != len(prior.Versions[ref.ID])+1 || len(parked.Changesets) != len(prior.Changesets)+1 {
		t.Fatal("accepted defer must use one ordinary version/ChangeSet; no native history-off flag is adopted")
	}
	outcome.Reason = reason
	b24ConservedRead(t, f, "b24-public-defer-replay", func() {
		replayed := f.dispatch(request)
		if !replayed.Meta.Replayed || !reflect.DeepEqual(commit(t, replayed), accepted) {
			t.Fatal("defer replay changed commit or lost receipt")
		}
	})
	for _, boundary := range []struct {
		name string
		at   time.Time
	}{
		{"before", target.Add(-tick)}, {"at", target}, {"after", target.Add(tick)},
	} {
		b24StepClocks(t, f, application, boundary.at.Sub(application.Now()), 0)
		b24ObserveClocks(t, f, application, "b24-public-defer-"+boundary.name)
		for readback := 0; readback < 2; readback++ {
			if readback == 1 {
				before := f.state(t)
				b24ReopenClocks(t, f, application)
				sameState(t, before, f.state(t))
			}
			b24ConservedRead(t, f, fmt.Sprintf("b24-public-untimed-%s-%d", boundary.name, readback), func() {
				if b24ReadyContains(t, f, fmt.Sprintf("untimed-%s-%d", boundary.name, readback), ref.ID) {
					t.Fatal("clock advance silently woke untimed Plug defer")
				}
			})
		}
	}
	outcome.FutureExcluded = true
	for _, until := range []time.Time{target.Add(time.Hour), target.Add(-time.Hour)} {
		b24ConservedRead(t, f, "b24-public-target-refusal", func() {
			got := f.call("bead.defer", fmt.Sprintf("until-%d", until.Unix()), map[string]any{"id": ref, "reason": reason, "until": until})
			if got.Error == nil || got.Error.Data.Code != "invalid_params" {
				t.Fatalf("unsupported timed defer response=%+v", got)
			}
			outcome.TimedRequestsRefused++
		})
	}
	commit(t, f.call("bead.reopen", "b24-public-reopen", map[string]any{"id": ref, "reason": "API available"}))
	b24ConservedRead(t, f, "b24-public-reopened-ready", func() {
		outcome.ExplicitReopenReady = b24ReadyContains(t, f, "explicit-reopen-ready", ref.ID)
		if !outcome.ExplicitReopenReady {
			t.Fatal("normal category/derived path did not restore readiness")
		}
	})
	before := f.state(t)
	b24ReopenClocks(t, f, application)
	sameState(t, before, f.state(t))
	b24ConservedRead(t, f, "b24-public-final-ready", func() {
		if !b24ReadyContains(t, f, "final-ready", ref.ID) {
			t.Fatal("explicit reopen lost after real SQL reopen")
		}
	})
	b24Retain(t, "public-"+engine+"-outcome.json", outcome)
	t.Logf("B24_PUBLIC_COUNTERPART engine=%s stale=%v blank_refusal=%t exact_reason=%q target=%s persistence=%s native_clock_equality=NOT_MEASURED", engine, outcome.Selected, outcome.BlankRefused, reason, target.Format(time.RFC3339Nano), outcome.Reopen)
	return outcome
}

func TestB24PublicStaleDeferBoundaryCounterparts(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			b24RunPublicCounterpart(t, engine, time.Date(2026, 11, 13, 12, 0, 0, 123456000, time.UTC))
		})
	}
}
