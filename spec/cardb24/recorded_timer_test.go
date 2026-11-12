package cardb20

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// These are unchanged public native originals. The independent expectation
// file was authored by a91 before this consumer and is never inferred from
// either native result membership or the Plug results below.
type b24RecordedTimers struct {
	root     string
	expected struct {
		Stale struct {
			UpdatedAt time.Time   `json:"stored_updated_at"`
			SQL       time.Time   `json:"sql_fixed"`
			Go        []time.Time `json:"go_points"`
			Open      [][]string  `json:"open_ids"`
			Deferred  [][]string  `json:"explicit_deferred_ids"`
			TickNS    int64       `json:"sample_tick_ns"`
			Subsecond string      `json:"subsecond_cutoff_precision"`
		} `json:"stale"`
		Timer struct {
			Notes string `json:"notes"`
		} `json:"timer"`
		Wake struct {
			Steps []struct {
				Name string    `json:"name"`
				Go   time.Time `json:"go"`
				SQL  time.Time `json:"sql"`
			} `json:"steps"`
		} `json:"wake"`
	}
}

func b24RecordedRead(t *testing.T, root, name string, value any) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(value); err != nil {
			t.Fatalf("recorded %s: %v", name, err)
		}
	}
	return data
}

func b24RecordedFixture(t *testing.T) b24RecordedTimers {
	t.Helper()
	r := b24RecordedTimers{root: filepath.Join("testdata", "recorded-timers")}
	var provenance struct {
		Files []struct {
			Path, SHA256 string
			Size         int64
		}
	}
	data := b24RecordedRead(t, r.root, "provenance.json", &provenance)
	if fmt.Sprintf("%x", sha256.Sum256(data)) != "a60c9c2aaac6f168e44ad2cd7302075057237143b49f0cf2e4ab4ffce29d2aa0" || len(provenance.Files) != 67 {
		t.Fatal("recorded native input manifest changed")
	}
	for _, file := range provenance.Files {
		if filepath.IsAbs(file.Path) || filepath.Clean(file.Path) != file.Path || strings.HasPrefix(file.Path, "..") {
			t.Fatal("recorded input path is not a fixture child")
		}
		data := b24RecordedRead(t, r.root, file.Path, nil)
		if int64(len(data)) != file.Size || fmt.Sprintf("%x", sha256.Sum256(data)) != file.SHA256 {
			t.Fatalf("recorded original %s changed", file.Path)
		}
	}
	b24RecordedRead(t, r.root, "independent-expected-8e91.json", &r.expected)
	if len(r.expected.Stale.Go) != 3 || len(r.expected.Stale.Open) != 3 || len(r.expected.Stale.Deferred) != 3 || r.expected.Stale.TickNS != int64(time.Second) || len(r.expected.Wake.Steps) != 3 {
		t.Fatal("independent typed B24 clock expectations missing")
	}
	return r
}

type b24RecordedCommand struct {
	Argv     []string `json:"argv"`
	ExitCode int      `json:"exit_code"`
	Stdout   string   `json:"stdout"`
	Stderr   string   `json:"stderr"`
}

func (r b24RecordedTimers) command(t *testing.T, number, exit int, argv []string) b24RecordedCommand {
	t.Helper()
	var command b24RecordedCommand
	b24RecordedRead(t, r.root, fmt.Sprintf("native8e91/command-%03d.json", number), &command)
	if command.ExitCode != exit || !reflect.DeepEqual(command.Argv, argv) {
		t.Fatalf("recorded public argv/exit %d: %+v", number, command)
	}
	return command
}

func b24RecordedIDs(t *testing.T, stdout string) []string {
	t.Helper()
	var rows []struct{ ID string }
	if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	sort.Strings(ids)
	return ids
}

type b24RecordedSQLImage struct {
	Tables map[string]struct {
		Columns []string         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
	} `json:"tables"`
	Graph any `json:"source_graph"`
}

func (r b24RecordedTimers) sql(t *testing.T, name string) b24RecordedSQLImage {
	t.Helper()
	var image b24RecordedSQLImage
	b24RecordedRead(t, r.root, "native8e91/"+name+"-sql-state.json", &image)
	if len(image.Tables) != 33 || len(image.Tables["issue_versions"].Columns) == 0 || image.Graph == nil {
		t.Fatal("native complete SQL/graph image missing")
	}
	return image
}

func (r b24RecordedTimers) unchanged(t *testing.T, before, after string) {
	t.Helper()
	if !reflect.DeepEqual(r.sql(t, before), r.sql(t, after)) {
		t.Fatalf("recorded native read/refusal mutated complete SQL/graph: %s/%s", before, after)
	}
}

func b24RecordedRow(t *testing.T, image b24RecordedSQLImage, table, id string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, row := range image.Tables[table].Rows {
		if row["id"] == id {
			if found != nil {
				t.Fatalf("duplicate native %s/%s", table, id)
			}
			found = row
		}
	}
	if found == nil {
		t.Fatalf("native %s/%s missing", table, id)
	}
	return found
}

func (r b24RecordedTimers) clock(t *testing.T, phase string, goTime, sqlTime time.Time) {
	t.Helper()
	var observation struct {
		Clock struct {
			Input struct {
				GoTime  time.Time `json:"go_time"`
				SQLTime time.Time `json:"sql_time"`
			} `json:"input"`
			GoObserved time.Time `json:"go_observed"`
			SQLResult  struct {
				Rows []map[string]string
			} `json:"sql_result"`
		} `json:"capture_clock"`
	}
	b24RecordedRead(t, r.root, "native8e91/"+phase+"-environment-and-flags.json", &observation)
	c := observation.Clock
	if !c.Input.GoTime.Equal(goTime) || !c.Input.SQLTime.Equal(sqlTime) || !c.GoObserved.Equal(goTime) || len(c.SQLResult.Rows) != 1 {
		t.Fatal("native input and independent clock observations disagree")
	}
	for _, column := range []string{"utc_microseconds", "utc_default"} {
		observed, err := time.Parse(time.RFC3339Nano, c.SQLResult.Rows[0][column])
		if err != nil || !observed.Equal(sqlTime) {
			t.Fatalf("native actual %s=%q want=%s", column, c.SQLResult.Rows[0][column], sqlTime)
		}
	}
}

// Save complete before/after images as well as the byte-RPC trace. These
// observations belong to live Plug calls, not to replaying the native parser.
func b24RecordedConserved(t *testing.T, f *fixture, phase string, call func()) {
	t.Helper()
	before := b24ReadSnapshot(t, f)
	call()
	after := b24ReadSnapshot(t, f)
	b24Retain(t, f.engine+"-"+phase+"-images.json", map[string]any{"before": before, "after": after})
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("public %s changed logical or physical state", phase)
	}
}

func (r b24RecordedTimers) stale(t *testing.T, engine string) {
	t.Helper()
	f := newFixture(t, engine)
	f.ids = []string{"clockdefer-stale", "clockdefer-stale-deferred", "clockdefer-closed"}
	application := testclock.New(t, r.expected.Stale.UpdatedAt)
	b24BindApplicationClock(t, f, application)
	f.create(t, "recorded-old", "strict stale ordinary")
	deferred, _ := f.create(t, "recorded-deferred", "explicit deferred stale")
	commit(t, f.call("bead.defer", "recorded-seed-defer", map[string]any{"id": deferred, "reason": "independent stale fixture"}))
	closed, _ := f.create(t, "recorded-closed", "closed stale exclusion")
	commit(t, f.call("bead.close", "recorded-close", map[string]any{"id": closed, "reason": "completed"}))
	b24StepClocks(t, f, application, 0, r.expected.Stale.SQL.Sub(f.clock.Now()))
	for index, phase := range []string{"stale-before", "stale-at", "stale-after"} {
		r.clock(t, phase, r.expected.Stale.Go[index], r.expected.Stale.SQL)
		b24StepClocks(t, f, application, r.expected.Stale.Go[index].Sub(application.Now()), 0)
		b24ObserveClocks(t, f, application, "recorded-"+phase)
		for _, selection := range []struct {
			status string
			want   []string
		}{
			{"todo", r.expected.Stale.Open[index]},
			{"backlog", r.expected.Stale.Deferred[index]},
		} {
			label := "recorded-" + phase + "-" + selection.status
			b24RecordedConserved(t, f, label, func() {
				query := map[string]any{"definition": staleDefinition("status=" + selection.status), "stale": map[string]any{"days": 1, "status": selection.status}}
				ids, cursor := staleIDs(t, f.call("view.run", label, query))
				query["countOnly"] = true
				count := staleCount(t, f.call("view.run", label+"-count", query))
				if cursor != "" || len(ids) != len(selection.want) || strings.Join(ids, "\n") != strings.Join(selection.want, "\n") || count != len(selection.want) {
					t.Fatalf("public/source strict stale %s IDs=%v count=%d want=%v", label, ids, count, selection.want)
				}
			})
		}
		b24TimerReopen(t, f, application, "recorded-clock", phase)
	}
	for _, days := range []int{0, -1} {
		label := fmt.Sprintf("recorded-age-%d", days)
		b24RecordedConserved(t, f, label, func() {
			got := f.call("view.run", label, map[string]any{"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": days}})
			if got.Error == nil || got.Error.Data.Code != "invalid_params" {
				t.Fatalf("nonpositive public stale age did not refuse: %+v", got)
			}
		})
	}
}

func (r b24RecordedTimers) nativePrefixes(t *testing.T) {
	t.Helper()
	for index, phase := range []string{"stale-before", "stale-at", "stale-after"} {
		r.clock(t, phase, r.expected.Stale.Go[index], r.expected.Stale.SQL)
		command := r.command(t, []int{12, 15, 18}[index], 0, []string{"stale", "--days", "1", "--label", "stale-clock", "--limit", "1000", "--json"})
		deferred := r.command(t, []int{13, 16, 19}[index], 0, []string{"stale", "--days", "1", "--status", "deferred", "--label", "stale-deferred-clock", "--json"})
		if !reflect.DeepEqual(b24RecordedIDs(t, command.Stdout), r.expected.Stale.Open[index]) || !reflect.DeepEqual(b24RecordedIDs(t, deferred.Stdout), r.expected.Stale.Deferred[index]) {
			t.Fatal("actual native stale result disagrees with independent input oracle")
		}
		r.unchanged(t, phase+"-before", phase+"-after")
		deferredPhase := strings.Replace(phase, "stale-", "stale-deferred-", 1)
		r.unchanged(t, deferredPhase+"-before", deferredPhase+"-after")
		for _, id := range []string{"clockdefer-stale", "clockdefer-stale-deferred", "clockdefer-closed"} {
			row := b24RecordedRow(t, r.sql(t, phase+"-before"), "issues", id)
			b24TimerValue(t, "native stale stored updated_at", row["updated_at"], &r.expected.Stale.UpdatedAt)
		}
	}
	for i, age := range []string{"0", "-1"} {
		command := r.command(t, 20+i, 1, []string{"stale", "--days", age, "--json"})
		var refusal struct {
			Error string `json:"error"`
		}
		if json.Unmarshal([]byte(command.Stdout), &refusal) != nil || refusal.Error != "--days must be at least 1" {
			t.Fatal("native nonpositive-age refusal changed")
		}
		r.unchanged(t, "stale-age-"+age+"-before", "stale-age-"+age+"-refusal-after")
	}
	appended := r.command(t, 6, 0, []string{"defer", "clockdefer-timer", "--reason", "  waiting deliberately  ", "--json"})
	var rows []map[string]any
	if json.Unmarshal([]byte(appended.Stdout), &rows) != nil || len(rows) != 1 || rows[0]["notes"] != r.expected.Timer.Notes {
		t.Fatal("native exact reason/notes append disagrees with independent oracle")
	}
	blank := r.command(t, 7, 1, []string{"defer", "clockdefer-timer", "--reason", " \t ", "--json"})
	if blank.Stdout != "" || blank.Stderr != "Error: reason cannot be empty\n" {
		t.Fatal("native blank-reason refusal changed")
	}
	r.unchanged(t, "timer-retained", "blank-reason-refusal-after")
	past := r.command(t, 8, 0, []string{"defer", "clockdefer-past", "--until", "2026-11-03T11:59:59Z", "--reason", "past deliberately"})
	if !strings.Contains(past.Stderr, "in the past") {
		t.Fatal("native deliberate past write did not preserve its warning")
	}
	written := b24RecordedRow(t, r.sql(t, "past-written-before-wake"), "issues", "clockdefer-past")
	if written["status"] != "deferred" || written["defer_until"] != "2026-11-03T11:59:59Z" {
		t.Fatal("native past warning was not followed by its deliberate write")
	}
	pastReady := r.command(t, 9, 0, []string{"ready", "--label", "past-clock", "--json"})
	if !reflect.DeepEqual(b24RecordedIDs(t, pastReady.Stdout), []string{"clockdefer-past"}) {
		t.Fatal("native expired past target did not wake through public ready")
	}
	history := r.command(t, 2, 0, []string{"config", "get", "versioned-history.enabled"})
	if history.Stdout != "false\n" {
		t.Fatal("native effective history-off configuration missing")
	}
	var inputs struct {
		Dated, Active, Indefinite string
		IndefiniteActive          string `json:"indefinite_active"`
	}
	b24RecordedRead(t, r.root, "native8e91/wake-operation-inputs.json", &inputs)
	// The generated active ID is bound to the actual public defer INPUTS, not
	// learned from ready's answer. No Plug ephemeral parity is claimed here.
	r.command(t, 23, 0, []string{"defer", inputs.Dated, inputs.Active, "--until", "2026-11-03T12:00:01Z", "--json"})
	if inputs.Dated != "clockdefer-dated" || inputs.Indefinite != "clockdefer-indefinite" || inputs.Active == "" || inputs.IndefiniteActive == "" {
		t.Fatal("independent wake fixture identity binding missing")
	}
	for index, phase := range []string{"wake-go-only-past", "wake-sql-at"} {
		step := r.expected.Wake.Steps[index]
		r.clock(t, phase, step.Go, step.SQL)
		command := r.command(t, 26+2*index, 0, []string{"ready", "--include-ephemeral", "--label", "wake-clock", "--limit", "1000", "--json"})
		wanted := []string{}
		if index == 1 {
			wanted = []string{inputs.Dated, inputs.Active}
			sort.Strings(wanted)
		}
		if !reflect.DeepEqual(b24RecordedIDs(t, command.Stdout), wanted) {
			t.Fatal("native split Go/SQL wake boundary disagrees with independent inputs")
		}
	}
	r.unchanged(t, "wake-sql-before-before", "wake-sql-before-after")
	before, after := r.sql(t, "wake-at-before"), r.sql(t, "wake-at-after")
	if !reflect.DeepEqual(before.Tables["issue_versions"], after.Tables["issue_versions"]) || len(after.Tables["issue_versions"].Rows) != 0 {
		t.Fatal("native history-off wake minted or changed application version rows")
	}
	for table, ids := range map[string][]string{"issues": {inputs.Dated, inputs.Indefinite}, "wisps": {inputs.Active, inputs.IndefiniteActive}} {
		prior := b24RecordedRow(t, before, table, ids[0])
		now := b24RecordedRow(t, after, table, ids[0])
		if prior["status"] != "deferred" || prior["defer_until"] != "2026-11-03T12:00:01Z" || now["status"] != "open" || now["defer_until"] != nil || now["updated_at"] != "2026-11-03T12:00:20Z" || !reflect.DeepEqual(b24RecordedRow(t, before, table, ids[1]), b24RecordedRow(t, after, table, ids[1])) {
			t.Fatal("native wake changed the wrong boundary, timestamp or indefinite row")
		}
	}
	// The source fixture failed later on graph decoration. It supplied no
	// repeat-after or after-wake physical capture, and no operation read trace.
	// Validating a reached prefix never changes that original RED disposition.
	t.Log("B24_RECORDED_NATIVE prefix=STALE_3_POINTS_WAKE_BEFORE_AT original=RED_PARTIAL after_wake_physical=NOT_REACHED repeat_after=NOT_REACHED version_read=NOT_MEASURED subsecond_stale=NOT_PROVED")
}

func (r b24RecordedTimers) deferCounterpart(t *testing.T, engine string) {
	t.Helper()
	f := newFixture(t, engine)
	f.ids = []string{"b24-recorded-defer"}
	application := testclock.New(t, r.expected.Stale.Go[1])
	b24BindApplicationClock(t, f, application)
	b24StepClocks(t, f, application, 0, r.expected.Wake.Steps[0].SQL.Sub(f.clock.Now()))
	ref, _ := f.create(t, "recorded-timer", "independent native timer comparison")
	commit(t, f.call("bead.comment", "recorded-existing-notes", map[string]any{"id": ref, "body": "existing notes"}))
	b24RecordedConserved(t, f, "recorded-blank", func() {
		got := f.call("bead.defer", "recorded-blank", map[string]any{"id": ref, "reason": " \t "})
		if got.Error == nil || got.Error.Data.Code != "invalid_params" {
			t.Fatal("public blank defer reason did not refuse")
		}
	})
	prior := f.state(t)
	request := f.request("bead.defer", "recorded-reason", map[string]any{"id": ref, "reason": "waiting deliberately"})
	accepted := commit(t, f.dispatch(request))
	if len(accepted.Results) != 2 {
		t.Fatal("Plug defer must publish the ordinary patch and comment")
	}
	for _, result := range accepted.Results {
		doc, err := pipeline.DecodeDocument(result.Record)
		if err != nil || doc.Fields["status"] != "backlog" || len(doc.Comments) != 2 || doc.Comments[0].Body != "existing notes" || doc.Comments[1].Body != "waiting deliberately" {
			t.Fatal("adopted public comment/status projection differs")
		}
	}
	parked := f.state(t)
	if len(parked.Versions[ref.ID]) != len(prior.Versions[ref.ID])+1 || len(parked.Changesets) != len(prior.Changesets)+1 {
		t.Fatal("Plug accepted defer did not publish one always-on version/ChangeSet")
	}
	b24Retain(t, engine+"-recorded-defer-write.json", map[string]any{"before": prior, "after": parked, "native_notes": r.expected.Timer.Notes, "plug_comments": []string{"existing notes", "waiting deliberately"}})
	for index, step := range r.expected.Wake.Steps {
		b24StepClocks(t, f, application, step.Go.Sub(application.Now()), step.SQL.Sub(f.clock.Now()))
		b24ObserveClocks(t, f, application, "recorded-"+step.Name)
		label := fmt.Sprintf("recorded-wake-%d", index)
		b24RecordedConserved(t, f, label, func() {
			if b24ReadyContains(t, f, label, ref.ID) {
				t.Fatal("native SQL timestamp silently enabled a Plug scheduler")
			}
		})
		b24TimerReopen(t, f, application, "recorded-defer", step.Name)
		b24RecordedConserved(t, f, label+"-reopened", func() {
			if b24ReadyContains(t, f, label+"-reopened", ref.ID) {
				t.Fatal("SQL reopen silently woke untimed Plug defer")
			}
		})
	}
	for _, until := range []string{"2026-11-03T11:59:59Z", "2099-06-15T00:00:00Z"} {
		b24RecordedConserved(t, f, "recorded-until-"+until, func() {
			got := f.call("bead.defer", "recorded-until-"+until, map[string]any{"id": ref, "reason": "past deliberately", "until": until})
			if got.Error == nil || got.Error.Data.Code != "invalid_params" {
				t.Fatal("Plug must refuse the unadopted timed request without mutation")
			}
		})
	}
	b24RecordedConserved(t, f, "recorded-reason-replay", func() {
		replayed := f.dispatch(request)
		if !replayed.Meta.Replayed || !reflect.DeepEqual(commit(t, replayed), accepted) {
			t.Fatal("exact public defer replay lost its receipt or minted state")
		}
	})
	commit(t, f.call("bead.reopen", "recorded-explicit-reopen", map[string]any{"id": ref}))
	b24TimerReopen(t, f, application, "recorded-defer", "explicit-reopen")
	b24RecordedConserved(t, f, "recorded-explicit-ready", func() {
		if !b24ReadyContains(t, f, "recorded-explicit-ready", ref.ID) {
			t.Fatal("explicit Plug reopen did not restore ordinary derived readiness")
		}
	})
	b24Retain(t, engine+"-recorded-clock-limits.json", map[string]any{
		"native_input_source": "8e91a0d223e5e9d61cf4827af06c79a75f255f2b", "native_original": "RED_PARTIAL_PREFIX_ONLY",
		"native_version_write": "NO_MINT_RECORDED_ROWS", "native_version_read": "NOT_MEASURED",
		"native_after_wake_physical": "NOT_REACHED", "native_repeat_after": "NOT_REACHED", "native_subsecond_stale": "NOT_PROVED",
		"plug_contract": "UNTIMED_DEFER_EXPLICIT_REOPEN_ALWAYS_ON_CHANGESETS", "plug_clock_steps": r.expected.Wake.Steps,
	})
}

func TestB24RecordedNativeClockComparisons(t *testing.T) {
	r := b24RecordedFixture(t)
	r.nativePrefixes(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run("stale-"+engine, func(t *testing.T) { r.stale(t, engine) })
		t.Run("defer-"+engine, func(t *testing.T) { r.deferCounterpart(t, engine) })
	}
}

func TestB24RecordedCanonicalNonNullTimersDurable(t *testing.T) {
	r := b24RecordedFixture(t)
	data := b24RecordedRead(t, r.root, "canonical904/timer-capture/timer-native-dump.tar", nil)
	// Due/defer inputs were independently named in the retained B24 source
	// requirements before the native create/update/export and this consumer.
	due := time.Date(2099, 1, 15, 0, 0, 0, 0, time.UTC)
	deferred := time.Date(2099, 6, 15, 0, 0, 0, 0, time.UTC)
	want := b24TimerOracle{ID: "b24-timer", Project: "b24", Title: "Scheduled task (updated)", Status: "backlog", Due: &due, Defer: &deferred}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b24RunTimerImport(t, engine, data, want) })
	}
}
