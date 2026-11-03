package s1b17b20

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	project   = "b18"
	agent     = "b18-acceptance"
	cursorKey = "b18-recorded-slice-cursor-key-32-bytes"
)

type wireClient struct {
	runtime *composition.Runtime
	next    int
}

func newWireClient(t *testing.T, runtime *composition.Runtime) *wireClient {
	t.Helper()
	return &wireClient{runtime: runtime}
}

func (c *wireClient) call(t *testing.T, method string, params map[string]any) map[string]any {
	t.Helper()
	c.next++
	params = cloneMap(params)
	params["_meta"] = map[string]any{
		"agent":          agent,
		"project":        project,
		"idempotencyKey": digestKey(fmt.Sprintf("%s/%d", method, c.next)),
	}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": c.next, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	response := c.runtime.RPC.Dispatch(context.Background(), request)
	var envelope map[string]any
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("decode %s response: %v; bytes=%s", method, err, response)
	}
	if failure, ok := envelope["error"]; ok {
		t.Fatalf("%s refused: %#v", method, failure)
	}
	return envelope["result"].(map[string]any)
}

func digestKey(seed string) string {
	sum := sha256.Sum256([]byte("b18/" + seed))
	return hex.EncodeToString(sum[:])
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func create(t *testing.T, c *wireClient, title string) (string, model.Revision) {
	t.Helper()
	result := c.call(t, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": title}})
	rows, ok := result["results"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("create results=%#v", result["results"])
	}
	row := rows[0].(map[string]any)
	ref := row["ref"].(map[string]any)
	id := ref["id"].(string)
	return id, natural(t, result["revision"])
}

func natural(t *testing.T, value any) model.Revision {
	t.Helper()
	switch v := value.(type) {
	case json.Number:
		return model.Natural(v.String())
	case string:
		return model.Natural(v)
	case float64:
		return model.Natural(fmt.Sprint(int64(v)))
	default:
		t.Fatalf("revision has type %T: %#v", value, value)
		return model.Natural("0")
	}
}

func revisionParam(t *testing.T, revision model.Revision) json.Number {
	t.Helper()
	return json.Number(revision.String())
}

func open(t *testing.T, driverName, dsn string) *composition.Runtime {
	t.Helper()
	runtime, err := composition.Open(context.Background(), composition.Config{
		Driver:      driverName,
		DSN:         dsn,
		Project:     project,
		AuthMode:    "anonymous",
		CursorKey:   []byte(cursorKey),
		LeaseHolder: "b18-acceptance-holder",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return runtime
}

func versions(t *testing.T, runtime *composition.Runtime, id string) ([]driver.Version, []model.Changeset) {
	t.Helper()
	meta, err := runtime.Store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := runtime.Store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	page, err := reader.Versions(context.Background(), model.RefByID(id, model.Natural64(0)), driver.PageReq{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if page.NextCursor != nil {
		t.Fatal("version page unexpectedly truncated")
	}
	changes, err := reader.Changesets(context.Background(), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return page.Items, changes
}

func TestRevisionAndVersionShareTheCommittedIdentity(t *testing.T) {
	runtime := open(t, "memory", "")
	client := newWireClient(t, runtime)
	id, createdRevision := create(t, client, "first")
	updated := client.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "second"}})
	updatedRevision := natural(t, updated["revision"])
	if updatedRevision.String() == createdRevision.String() {
		t.Fatal("changing update reused the create revision")
	}
	gotVersions, changes := versions(t, runtime, id)
	if len(gotVersions) != 2 || len(changes) != 2 {
		t.Fatalf("versions=%d changesets=%d, want one per accepted durable mutation", len(gotVersions), len(changes))
	}
	for i, version := range gotVersions {
		want := []model.Revision{createdRevision, updatedRevision}[i]
		if version.Ref.ID != id || version.Revision.String() != want.String() || changes[i].Revision.String() != want.String() {
			t.Fatalf("identity mismatch at %d: version=%+v changeset=%+v want=%s", i, version, changes[i], want)
		}
	}
	if !gotVersions[0].Base || len(gotVersions[0].Patch) != 0 || len(gotVersions[1].Patch) == 0 {
		t.Fatalf("creation/change base-patch shape=%+v", gotVersions)
	}
}

func TestPinnedAsOfContentRemainsAfterLaterByteRPCWrites(t *testing.T) {
	runtime := open(t, "memory", "")
	client := newWireClient(t, runtime)
	id, firstRevision := create(t, client, "original")
	client.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "second"}})
	client.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "third"}})
	historical := client.call(t, "history.query", map[string]any{"asOf": map[string]any{"revision": revisionParam(t, firstRevision)}})
	items := historical["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("historical items=%#v", historical["items"])
	}
	item := items[0].(map[string]any)
	fields := item["fields"].(map[string]any)
	if fields["title"] != "original" || natural(t, item["revision"]).String() != firstRevision.String() {
		t.Fatalf("pinned history substituted later state: %#v", item)
	}
	current := client.call(t, "bead.get", map[string]any{"id": map[string]any{"id": id}})
	if current["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "third" {
		t.Fatalf("current state=%#v", current)
	}
}

func TestSQLiteNoOpAndChangeSurviveSameStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b18.db")
	firstRuntime := open(t, "sqlite", path)
	firstClient := newWireClient(t, firstRuntime)
	id, createdRevision := create(t, firstClient, "stable")
	noop := firstClient.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "stable"}})
	if natural(t, noop["revision"]).String() != createdRevision.String() {
		t.Fatalf("same-value update advanced revision: %#v", noop)
	}
	changed := firstClient.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "changed"}})
	changedRevision := natural(t, changed["revision"])
	if changedRevision.String() == createdRevision.String() {
		t.Fatal("changing update did not advance revision")
	}
	if err := firstRuntime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	secondRuntime := open(t, "sqlite", path)
	secondClient := newWireClient(t, secondRuntime)
	historical := secondClient.call(t, "history.query", map[string]any{"asOf": map[string]any{"revision": revisionParam(t, createdRevision)}})
	item := historical["items"].([]any)[0].(map[string]any)
	if item["fields"].(map[string]any)["title"] != "stable" {
		t.Fatalf("reopen lost historical body: %#v", item)
	}
	current := secondClient.call(t, "bead.get", map[string]any{"id": map[string]any{"id": id}})
	if current["bead"].(map[string]any)["fields"].(map[string]any)["title"] != "changed" {
		t.Fatalf("reopen lost current body: %#v", current)
	}
	gotVersions, changes := versions(t, secondRuntime, id)
	if len(gotVersions) != 2 || len(changes) != 2 {
		t.Fatalf("reopened versions=%d changesets=%d, want no-op suppressed and one change retained", len(gotVersions), len(changes))
	}
	if gotVersions[0].Revision.String() != createdRevision.String() || gotVersions[1].Revision.String() != changedRevision.String() {
		t.Fatalf("reopened revision identity=%v, want [%s %s]", gotVersions, createdRevision, changedRevision)
	}
	if !reflect.DeepEqual(gotVersions[0].Principal, gotVersions[1].Principal) {
		t.Fatalf("reopened version attribution changed: %v / %v", gotVersions[0].Principal, gotVersions[1].Principal)
	}
}

type referenceCommand struct {
	Name   string   `json:"name"`
	Args   []string `json:"args"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	Stderr string   `json:"stderr"`
}

type referenceRun struct {
	Status     string             `json:"status"`
	Commands   []referenceCommand `json:"commands"`
	Projection map[string]any     `json:"projection"`
	Provenance struct {
		RawCommandsSHA256 string `json:"rawCommandsSha256"`
		ProjectionSHA256  string `json:"projectionSha256"`
	} `json:"provenance"`
}

type stableObservation struct {
	VersionCounts       [3]int
	LocalRevisions      [3]int
	NoOpPreserved       bool
	ChangingWriteMoved  bool
	HistoricalTitle     string
	CurrentTitle        string
	HistoryTitles       [3]string
	SeparateIdentities  bool
	ReopenedProcessRuns int
}

func referencePath(name string) string {
	return filepath.Join(os.Getenv("TEST_SRCDIR"), "_main", "spec", "s1-b17-b20", "reference", "b18", name)
}

func readReference(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(referencePath(name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func fileSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func loadReferenceRun(t *testing.T, name string) referenceRun {
	t.Helper()
	var run referenceRun
	if err := json.Unmarshal(readReference(t, name), &run); err != nil {
		t.Fatal(err)
	}
	if run.Status != "RECORDED" || len(run.Commands) != 15 {
		t.Fatalf("%s status/commands=%q/%d", name, run.Status, len(run.Commands))
	}
	for i, command := range run.Commands {
		if command.Exit != 0 {
			t.Fatalf("%s command %d (%s) exit=%d stderr=%q", name, i, command.Name, command.Exit, command.Stderr)
		}
	}
	return run
}

func commandNamed(t *testing.T, run referenceRun, name string) referenceCommand {
	t.Helper()
	for _, command := range run.Commands {
		if command.Name == name {
			return command
		}
	}
	t.Fatalf("reference command %q is absent", name)
	return referenceCommand{}
}

func decodeCommand(t *testing.T, command referenceCommand, target any) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(command.Stdout))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode %s: %v; stdout=%q", command.Name, err, command.Stdout)
	}
}

func oneReferenceIssue(t *testing.T, run referenceRun, name string) map[string]any {
	t.Helper()
	var issues []map[string]any
	decodeCommand(t, commandNamed(t, run, name), &issues)
	if len(issues) != 1 {
		t.Fatalf("%s issues=%d", name, len(issues))
	}
	return issues[0]
}

func referenceObservation(t *testing.T, run referenceRun) stableObservation {
	t.Helper()
	created := oneReferenceIssue(t, run, "show-created")
	noop := oneReferenceIssue(t, run, "show-noop")
	current := oneReferenceIssue(t, run, "show-current")
	historical := oneReferenceIssue(t, run, "show-as-of-create")
	type versionsPayload struct {
		Versions []map[string]any `json:"versions"`
	}
	var createdVersions, noopVersions, finalVersions versionsPayload
	decodeCommand(t, commandNamed(t, run, "versions-created"), &createdVersions)
	decodeCommand(t, commandNamed(t, run, "versions-noop"), &noopVersions)
	decodeCommand(t, commandNamed(t, run, "versions-final"), &finalVersions)
	if len(finalVersions.Versions) != 3 {
		t.Fatalf("reference final versions=%d", len(finalVersions.Versions))
	}
	local := [3]int{}
	separate := true
	for i, version := range finalVersions.Versions {
		revision, err := version["local_revision"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("reference local revision: %v", err)
		}
		local[i] = int(revision)
		_, hasIssueRevisionName := version["revision"]
		separate = separate && !hasIssueRevisionName
	}
	var history []struct {
		CommitHash string `json:"CommitHash"`
		Issue      struct {
			Title string `json:"title"`
		} `json:"Issue"`
	}
	decodeCommand(t, commandNamed(t, run, "history-final"), &history)
	if len(history) != 3 {
		t.Fatalf("reference history=%d", len(history))
	}
	asOf := commandNamed(t, run, "show-as-of-create")
	if len(asOf.Args) != 5 || asOf.Args[2] != "--as-of" || asOf.Args[3] != history[2].CommitHash {
		t.Fatalf("as-of command does not use the retained original full hash: %#v", asOf.Args)
	}
	return stableObservation{
		VersionCounts:       [3]int{len(createdVersions.Versions), len(noopVersions.Versions), len(finalVersions.Versions)},
		LocalRevisions:      local,
		NoOpPreserved:       created["revision"] == noop["revision"],
		ChangingWriteMoved:  created["revision"] != current["revision"],
		HistoricalTitle:     historical["title"].(string),
		CurrentTitle:        current["title"].(string),
		HistoryTitles:       [3]string{history[0].Issue.Title, history[1].Issue.Title, history[2].Issue.Title},
		SeparateIdentities:  separate,
		ReopenedProcessRuns: len(run.Commands),
	}
}

func TestRecordedReferenceProjectionMatchesPlugWithAdoptedIdentityDeviation(t *testing.T) {
	var recording struct {
		Status           string `json:"status"`
		ComparisonStatus string `json:"comparisonStatus"`
		SourceCommit     string `json:"sourceCommit"`
		Runs             []struct {
			Path              string `json:"path"`
			ArtifactSHA256    string `json:"artifactSha256"`
			ProjectionSHA256  string `json:"projectionSha256"`
			RawCommandsSHA256 string `json:"rawCommandsSha256"`
		} `json:"runs"`
		Comparison struct {
			VersionIdentity struct {
				Result    string   `json:"result"`
				Decisions []string `json:"decisions"`
			} `json:"versionAndRevisionIdentity"`
			AsOf struct {
				Result string `json:"result"`
			} `json:"asOfAfterLaterWrites"`
		} `json:"comparison"`
		Historical struct {
			Checkpoint string `json:"checkpoint"`
			Attempts   []struct {
				Status         string `json:"status"`
				EvidenceSHA256 string `json:"evidenceSha256"`
			} `json:"attempts"`
		} `json:"historicalUnrun"`
	}
	if err := json.Unmarshal(readReference(t, "recording.json"), &recording); err != nil {
		t.Fatal(err)
	}
	if recording.Status != "RECORDED" || recording.ComparisonStatus != "PASS_WITH_ADOPTED_DEVIATION" || recording.SourceCommit != "9d6fff760b7a314b476539198504d77ce0959656" || len(recording.Runs) != 2 {
		t.Fatalf("recording header is not bound: %#v", recording)
	}
	if recording.Comparison.VersionIdentity.Result != "ADOPTED_DEVIATION" || !reflect.DeepEqual(recording.Comparison.VersionIdentity.Decisions, []string{"P6", "P7", "P30"}) || recording.Comparison.AsOf.Result != "MATCHING_STABLE_MEANING_WITH_ADOPTED_ADDRESS_DEVIATION" {
		t.Fatalf("reference difference is not tied to the adopted decisions: %#v", recording.Comparison)
	}
	if recording.Historical.Checkpoint != "f52e85b235864137be001d5dda00d441c4d4366b" || len(recording.Historical.Attempts) != 2 {
		t.Fatalf("historical UNRUN evidence was not preserved: %#v", recording.Historical)
	}
	for i, attempt := range recording.Historical.Attempts {
		if attempt.Status != "UNRUN" || attempt.EvidenceSHA256 != "5eb21c5a4a1aff92c4c21819492be687b77eea1d17fec433011ca447035a0d46" {
			t.Fatalf("historical attempt %d changed: %#v", i, attempt)
		}
	}

	runs := make([]referenceRun, 0, 2)
	observations := make([]stableObservation, 0, 2)
	for _, pinned := range recording.Runs {
		name := filepath.Base(pinned.Path)
		data := readReference(t, name)
		if got := fileSHA256(data); got != pinned.ArtifactSHA256 {
			t.Fatalf("%s sha256=%s, want %s", name, got, pinned.ArtifactSHA256)
		}
		run := loadReferenceRun(t, name)
		if run.Provenance.ProjectionSHA256 != pinned.ProjectionSHA256 || run.Provenance.RawCommandsSHA256 != pinned.RawCommandsSHA256 {
			t.Fatalf("%s provenance digests do not match recording", name)
		}
		runs = append(runs, run)
		observations = append(observations, referenceObservation(t, run))
	}
	if !reflect.DeepEqual(runs[0].Projection, runs[1].Projection) || !reflect.DeepEqual(observations[0], observations[1]) {
		t.Fatalf("fresh reference projections differ:\n%#v\n%#v", observations[0], observations[1])
	}
	wantReference := stableObservation{
		VersionCounts:       [3]int{1, 1, 3},
		LocalRevisions:      [3]int{3, 2, 1},
		NoOpPreserved:       true,
		ChangingWriteMoved:  true,
		HistoricalTitle:     "original",
		CurrentTitle:        "third",
		HistoryTitles:       [3]string{"third", "second", "original"},
		SeparateIdentities:  true,
		ReopenedProcessRuns: 15,
	}
	if !reflect.DeepEqual(observations[0], wantReference) {
		t.Fatalf("reference observation=%#v, want %#v", observations[0], wantReference)
	}

	runtime := open(t, "memory", "")
	client := newWireClient(t, runtime)
	id, createdRevision := create(t, client, "original")
	createdVersions, _ := versions(t, runtime, id)
	noop := client.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "original"}})
	noopVersions, _ := versions(t, runtime, id)
	client.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "second"}})
	client.call(t, "bead.update", map[string]any{"id": map[string]any{"id": id}, "fields": map[string]any{"title": "third"}})
	finalVersions, changes := versions(t, runtime, id)
	historical := client.call(t, "history.query", map[string]any{"asOf": map[string]any{"revision": revisionParam(t, createdRevision)}})
	historicalTitle := historical["items"].([]any)[0].(map[string]any)["fields"].(map[string]any)["title"].(string)
	current := client.call(t, "bead.get", map[string]any{"id": map[string]any{"id": id}})
	currentTitle := current["bead"].(map[string]any)["fields"].(map[string]any)["title"].(string)
	if len(createdVersions) != observations[0].VersionCounts[0] || len(noopVersions) != observations[0].VersionCounts[1] || len(finalVersions) != observations[0].VersionCounts[2] || natural(t, noop["revision"]).String() != createdRevision.String() || historicalTitle != observations[0].HistoricalTitle || currentTitle != observations[0].CurrentTitle {
		t.Fatalf("Plug/reference stable meaning differs: versions=%d/%d/%d noop=%s/%s titles=%s/%s reference=%#v", len(createdVersions), len(noopVersions), len(finalVersions), natural(t, noop["revision"]), createdRevision, historicalTitle, currentTitle, observations[0])
	}
	for i, version := range finalVersions {
		if version.Revision.String() != changes[i].Revision.String() {
			t.Fatalf("Plug version/changeset identity differs at %d: %+v / %+v", i, version, changes[i])
		}
	}
}
