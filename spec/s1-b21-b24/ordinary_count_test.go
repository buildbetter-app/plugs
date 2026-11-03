package s1b21b24

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// The two count recordings are fresh public runs of the same pinned Beads
// executable in independent disposable managed-proxied-Dolt stores.  They
// are intentionally separate from reference-a/b, whose older binary and
// ordered-batch vector remain retained unchanged.
//
//go:embed count-reference-first.json count-reference-repeat.json record_ordinary_count.py
var ordinaryCountReferenceFiles embed.FS

const (
	ordinaryCountBeadsCommit = "9d6fff760b7a314b476539198504d77ce0959656"
	ordinaryCountBeadsTree   = "cde121d6e4119e454857ad1c3ca293e2791be840"
	ordinaryCountBeadsBinary = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"
	ordinaryCountDescriptor  = "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2"
	ordinaryCountDoltVersion = "2.2.0"
	ordinaryCountDoltBinary  = "72352f1d882472a7a8d6124e9c4dc12f80e2c2359ea95f1424c032e9250e35e6"
)

type ordinaryCountReference struct {
	Schema            string                  `json:"schema"`
	Card              string                  `json:"card"`
	Recording         string                  `json:"recording"`
	Source            ordinaryCountSource     `json:"source"`
	Inputs            map[string]any          `json:"inputs"`
	InputSHA256       string                  `json:"inputSha256"`
	Projection        ordinaryCountProjection `json:"projection"`
	ProjectionSHA256  string                  `json:"projectionSha256"`
	RawJSON           string                  `json:"rawJSON"`
	RawSHA256         string                  `json:"rawSha256"`
	Streams           []ordinaryCountStream   `json:"streams"`
	RuntimeConfig     map[string]any          `json:"runtimeConfiguration"`
	Workspace         string                  `json:"workspace"`
	Limitations       []string                `json:"limitations"`
	RecorderSHA256    string                  `json:"recorderSha256"`
	ShutdownVerified  bool                    `json:"shutdownVerified"`
	WorkspaceRetained bool                    `json:"workspaceRetained"`
	Failure           json.RawMessage         `json:"failure"`
}

type ordinaryCountStream struct {
	Label       string   `json:"label"`
	Argv        []string `json:"argv"`
	ExitCode    *int     `json:"exitCode"`
	Outcome     string   `json:"outcome"`
	StdoutB64   string   `json:"stdoutBase64"`
	StderrB64   string   `json:"stderrBase64"`
	StdoutSHA   string   `json:"stdoutSha256"`
	StderrSHA   string   `json:"stderrSha256"`
	StdoutBytes int      `json:"stdoutBytes"`
	StderrBytes int      `json:"stderrBytes"`
}

type ordinaryCountSource struct {
	Commit       string `json:"commit"`
	Tree         string `json:"tree"`
	Profile      string `json:"profile"`
	Descriptor   string `json:"descriptor"`
	BinarySHA256 string `json:"binarySha256"`
	BinaryMode   string `json:"binaryMode"`
	DoltVersion  string `json:"doltVersion"`
	DoltSHA256   string `json:"doltSha256"`
	DoltMode     string `json:"doltMode"`
	RuntimeMode  string `json:"runtimeMode"`
}

type ordinaryCountProjection struct {
	OpenTaskCount                  int `json:"openTaskCount"`
	OpenNeedleTaskCount            int `json:"openNeedleTaskCount"`
	ClosedTaskCount                int `json:"closedTaskCount"`
	AbsentNeedleTaskCount          int `json:"absentNeedleTaskCount"`
	OpenTaskStatusTotal            int `json:"openTaskStatusTotal"`
	AfterUpdateOpenNeedleTaskCount int `json:"afterUpdateOpenNeedleTaskCount"`
	AfterUpdateChangedTaskCount    int `json:"afterUpdateChangedTaskCount"`
}

type ordinaryCountCommand struct {
	Step   string   `json:"step"`
	Argv   []string `json:"argv"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	Stderr string   `json:"stderr"`
}

type ordinaryCountReply struct {
	Items      []json.RawMessage `json:"items"`
	Count      *int              `json:"count"`
	NextCursor string            `json:"nextCursor"`
	AsOf       map[string]any    `json:"asOf"`
}

func ordinaryCountSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ordinaryCountCanonical(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func ordinaryCountProjectionCanonical(projection ordinaryCountProjection) []byte {
	// The recorder uses Python's sort_keys=True.  Marshal a map so Go's JSON
	// encoder applies the same lexicographic key ordering instead of the Go
	// struct declaration order.
	return ordinaryCountCanonical(map[string]any{
		"afterUpdateChangedTaskCount":    projection.AfterUpdateChangedTaskCount,
		"afterUpdateOpenNeedleTaskCount": projection.AfterUpdateOpenNeedleTaskCount,
		"absentNeedleTaskCount":          projection.AbsentNeedleTaskCount,
		"closedTaskCount":                projection.ClosedTaskCount,
		"openNeedleTaskCount":            projection.OpenNeedleTaskCount,
		"openTaskCount":                  projection.OpenTaskCount,
		"openTaskStatusTotal":            projection.OpenTaskStatusTotal,
	})
}

func loadOrdinaryCountReference(t *testing.T, name string) ordinaryCountReference {
	t.Helper()
	raw, err := ordinaryCountReferenceFiles.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var reference ordinaryCountReference
	if err := json.Unmarshal(raw, &reference); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	if reference.Schema != "plug.s1-b23.ordinary-count-reference.v2" || reference.Card != "B23" || reference.Workspace == "" {
		t.Fatalf("ordinary count schema/provenance changed: %+v", reference)
	}
	recorder, err := ordinaryCountReferenceFiles.ReadFile("record_ordinary_count.py")
	if err != nil || ordinaryCountSHA(recorder) != reference.RecorderSHA256 {
		t.Fatal("ordinary count recorder source does not match the recorded profile")
	}
	if !reference.ShutdownVerified || reference.WorkspaceRetained || string(reference.Failure) != "null" {
		t.Fatal("ordinary count recording did not complete verified public shutdown")
	}
	source := reference.Source
	if source.Commit != ordinaryCountBeadsCommit || source.Tree != ordinaryCountBeadsTree || source.Profile != "s1-b23-public9d6-cgo0-managed-proxied-dolt220-v2" || source.Descriptor != ordinaryCountDescriptor || source.BinarySHA256 != ordinaryCountBeadsBinary || source.BinaryMode != "0o755" || source.DoltVersion != ordinaryCountDoltVersion || source.DoltSHA256 != ordinaryCountDoltBinary || source.DoltMode != "0o700" || source.RuntimeMode != "managed-proxied-server" {
		t.Fatalf("ordinary count source profile changed: %+v", source)
	}
	for _, digest := range []string{reference.InputSHA256, reference.ProjectionSHA256, reference.RawSHA256} {
		if len(digest) != 64 {
			t.Fatalf("ordinary count digest is not SHA-256: %q", digest)
		}
	}
	if ordinaryCountSHA([]byte(reference.RawJSON)) != reference.RawSHA256 {
		t.Fatalf("ordinary count raw transcript digest changed")
	}
	if ordinaryCountSHA(ordinaryCountCanonical(reference.Inputs)) != reference.InputSHA256 {
		t.Fatalf("ordinary count input digest changed")
	}
	if ordinaryCountSHA(ordinaryCountProjectionCanonical(reference.Projection)) != reference.ProjectionSHA256 {
		t.Fatalf("ordinary count projection digest changed")
	}
	if len(reference.Limitations) < 3 {
		t.Fatalf("ordinary count limits were dropped: %+v", reference.Limitations)
	}
	wantSteps := []string{"git-init", "dolt-config-probe", "dolt-version", "init", "create-b23-alpha", "create-b23-zeta", "create-b23-secret", "create-b23-done", "create-b23-other", "close-secret", "open-task-count", "open-needle-count", "closed-task-count", "absent-needle-count", "open-task-by-status", "update-zeta", "after-update-needle-count", "after-update-changed-count", "cleanup-stop"}
	if len(reference.Streams) != len(wantSteps) {
		t.Fatalf("ordinary count raw stream envelope has %d entries, want %d", len(reference.Streams), len(wantSteps))
	}
	for i, stream := range reference.Streams {
		if stream.Label != wantSteps[i] || stream.ExitCode == nil || *stream.ExitCode != 0 || stream.Outcome != "exited" {
			t.Fatalf("ordinary count stream %d provenance changed: %+v", i, stream)
		}
		stdout, err := base64.StdEncoding.DecodeString(stream.StdoutB64)
		if err != nil || ordinaryCountSHA(stdout) != stream.StdoutSHA || len(stdout) != stream.StdoutBytes {
			t.Fatalf("ordinary count stdout stream %d digest changed", i)
		}
		stderr, err := base64.StdEncoding.DecodeString(stream.StderrB64)
		if err != nil || ordinaryCountSHA(stderr) != stream.StderrSHA || len(stderr) != stream.StderrBytes {
			t.Fatalf("ordinary count stderr stream %d digest changed", i)
		}
	}
	if reference.RuntimeConfig["directoryMode"] != "0o700" || reference.RuntimeConfig["fileMode"] != "0o600" || reference.RuntimeConfig["relativePath"] != ".dolt/config_global.json" {
		t.Fatalf("ordinary count runtime configuration changed: %+v", reference.RuntimeConfig)
	}
	var commands []ordinaryCountCommand
	if err := json.Unmarshal([]byte(reference.RawJSON), &commands); err != nil {
		t.Fatalf("decode ordinary count transcript: %v", err)
	}
	if len(commands) != len(wantSteps) {
		t.Fatalf("ordinary count transcript has %d commands, want %d", len(commands), len(wantSteps))
	}
	for i, command := range commands {
		if command.Step != wantSteps[i] || command.Exit != 0 || len(command.Argv) < 2 {
			t.Fatalf("ordinary count command %d provenance changed: %+v", i, command)
		}
		stream := reference.Streams[i]
		stdout, _ := base64.StdEncoding.DecodeString(stream.StdoutB64)
		stderr, _ := base64.StdEncoding.DecodeString(stream.StderrB64)
		if command.Stdout != string(stdout) || command.Stderr != string(stderr) || !reflect.DeepEqual(command.Argv, stream.Argv) {
			t.Fatal("count command text or argv differs from the retained raw stream")
		}
		if command.Step == "git-init" && !reflect.DeepEqual(command.Argv, []string{"/usr/bin/git", "init", "-q"}) {
			t.Fatal("count recording lacks fresh Git initialization")
		}
		if command.Step == "dolt-config-probe" && !reflect.DeepEqual(command.Argv[1:], []string{"config", "--global", "--get", "versioncheck.disabled"}) {
			t.Fatalf("ordinary count config probe changed: %+v", command.Argv)
		}
		if command.Step == "dolt-version" && !reflect.DeepEqual(command.Argv[1:], []string{"version"}) {
			t.Fatalf("ordinary count version probe changed: %+v", command.Argv)
		}
		if strings.HasPrefix(command.Step, "create-") || strings.HasSuffix(command.Step, "-count") || command.Step == "open-task-by-status" || command.Step == "init" || command.Step == "close-secret" || command.Step == "update-zeta" || command.Step == "cleanup-stop" {
			if len(command.Argv) < 5 || !reflect.DeepEqual(command.Argv[1:5], []string{"--sandbox", "--actor", "cardb23", "--json"}) {
				t.Fatalf("ordinary count actor/json mode changed: %+v", command.Argv)
			}
		}
	}
	// The stored summary is never sufficient: derive every count directly
	// from the byte-verified stdout of the actual command in each twin.
	byStep := make(map[string][]byte, len(reference.Streams))
	for _, stream := range reference.Streams {
		byStep[stream.Label], _ = base64.StdEncoding.DecodeString(stream.StdoutB64)
	}
	readCount := func(step string) int {
		var value struct {
			Count *int `json:"count"`
		}
		if err := json.Unmarshal(byStep[step], &value); err != nil || value.Count == nil || *value.Count < 0 {
			t.Fatalf("raw count %s is not a nonnegative integer: %s", step, byStep[step])
		}
		return *value.Count
	}
	var grouped struct {
		Total  int `json:"total"`
		Groups []struct {
			Group string `json:"group"`
			Count int    `json:"count"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(byStep["open-task-by-status"], &grouped); err != nil || len(grouped.Groups) != 1 || grouped.Groups[0].Group != "open" || grouped.Groups[0].Count != grouped.Total {
		t.Fatalf("raw grouped count differs: %s", byStep["open-task-by-status"])
	}
	derived := ordinaryCountProjection{
		OpenTaskCount: readCount("open-task-count"), OpenNeedleTaskCount: readCount("open-needle-count"),
		ClosedTaskCount: readCount("closed-task-count"), AbsentNeedleTaskCount: readCount("absent-needle-count"),
		OpenTaskStatusTotal: grouped.Total, AfterUpdateOpenNeedleTaskCount: readCount("after-update-needle-count"),
		AfterUpdateChangedTaskCount: readCount("after-update-changed-count"),
	}
	if derived != reference.Projection || derived != (ordinaryCountProjection{4, 3, 1, 0, 4, 2, 1}) {
		t.Fatalf("count vector differs from raw stdout: derived=%+v recorded=%+v", derived, reference.Projection)
	}
	var stopped struct {
		Stopped  bool `json:"stopped"`
		Verified bool `json:"verified"`
	}
	if err := json.Unmarshal(byStep["cleanup-stop"], &stopped); err != nil || !stopped.Stopped || !stopped.Verified {
		t.Fatal("raw public shutdown is not verified")
	}
	return reference
}

func ordinaryCountFromReply(t *testing.T, got reply, want int) (map[string]any, []byte) {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("ordinary count refusal: %+v", got.Error)
	}
	var page ordinaryCountReply
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatal(err)
	}
	if page.Count == nil || *page.Count != want || len(page.Items) != 0 || page.NextCursor != "" || page.AsOf["revision"] == nil || page.AsOf["incarnation"] == nil {
		t.Fatalf("ordinary count response=%s want count=%d and current asOf/no items", got.Result, want)
	}
	return page.AsOf, append([]byte(nil), got.Result...)
}

func ordinaryCountDefinition(query string) map[string]any {
	return map[string]any{"filter": map[string]any{"query": query}}
}

func TestS1B23OrdinaryExactCountMatchesPinnedTwinsAcrossStores(t *testing.T) {
	first := loadOrdinaryCountReference(t, "count-reference-first.json")
	repeat := loadOrdinaryCountReference(t, "count-reference-repeat.json")
	if first.Recording == repeat.Recording || first.Workspace == repeat.Workspace || reflect.DeepEqual(first.RawJSON, repeat.RawJSON) {
		t.Fatal("ordinary count twins did not use independent raw recordings")
	}
	if !reflect.DeepEqual(first.Projection, repeat.Projection) {
		t.Fatalf("ordinary count twin projections differ: %+v vs %+v", first.Projection, repeat.Projection)
	}
	want := first.Projection

	for _, engine := range append(engines(), "postgres") {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			alpha, _ := f.create(t, "b23-count-alpha", "needle-alpha")
			zeta, _ := f.create(t, "b23-count-zeta", "needle-zeta")
			secret, _ := f.create(t, "b23-count-secret", "needle-secret")
			f.create(t, "b23-count-done", "done-needle")
			f.create(t, "b23-count-other", "other-beta")
			commit(t, f.call("bead.close", "b23-count-close", map[string]any{"id": secret}))

			ordinaryCountFromReply(t, f.call("view.run", "b23-count-open", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("status=todo AND type=issue")}), want.OpenTaskCount)
			initialAsOf, initial := ordinaryCountFromReply(t, f.call("view.run", "b23-count-needle", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("title=needle AND status=todo AND type=issue")}), want.OpenNeedleTaskCount)
			if !bytes.Contains(initial, []byte(`"count":3`)) {
				t.Fatalf("initial count response lost exact count: %s", initial)
			}
			ordinaryCountFromReply(t, f.call("view.run", "b23-count-closed", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("status=done AND type=issue")}), want.ClosedTaskCount)
			ordinaryCountFromReply(t, f.call("view.run", "b23-count-absent", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("title=absent AND status=todo AND type=issue")}), want.AbsentNeedleTaskCount)

			commit(t, f.call("bead.update", "b23-count-update", map[string]any{"id": zeta, "fields": map[string]any{"title": "changed-zeta"}}))
			ordinaryCountFromReply(t, f.call("view.run", "b23-count-current", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("title=needle AND status=todo AND type=issue")}), want.AfterUpdateOpenNeedleTaskCount)
			ordinaryCountFromReply(t, f.call("view.run", "b23-count-changed", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("title=changed AND status=todo AND type=issue")}), want.AfterUpdateChangedTaskCount)
			historicalAsOf, _ := ordinaryCountFromReply(t, f.call("view.run", "b23-count-historical", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("title=needle AND status=todo AND type=issue"), "asOf": initialAsOf}), want.OpenNeedleTaskCount)
			if !reflect.DeepEqual(historicalAsOf, initialAsOf) {
				t.Fatalf("historical count changed its selected snapshot: initial=%v historical=%v", initialAsOf, historicalAsOf)
			}
			if engine == "sqlite" {
				f.reopen(t)
				ordinaryCountFromReply(t, f.call("view.run", "b23-count-reopen", map[string]any{"countOnly": true, "definition": ordinaryCountDefinition("title=needle AND status=todo AND type=issue"), "asOf": initialAsOf}), want.OpenNeedleTaskCount)
			}
			if alpha.ID == "" {
				t.Fatal("count fixture lost its first reference")
			}
			t.Logf("B23 ordinary exact count PASS: engine=%s twin vector open=%d needle=%d closed=%d absent=%d; current and historical counts retained", engine, want.OpenTaskCount, want.OpenNeedleTaskCount, want.ClosedTaskCount, want.AbsentNeedleTaskCount)
		})
	}
}

func TestS1B23OrdinaryCountReferenceProfileIsExplicit(t *testing.T) {
	reference := loadOrdinaryCountReference(t, "count-reference-first.json")
	if !strings.Contains(reference.Source.Profile, "managed-proxied") || reference.Source.RuntimeMode != "managed-proxied-server" {
		t.Fatalf("count reference profile was presented as a different engine: %+v", reference.Source)
	}
	t.Logf("B23 reference profile retained: commit=%s tree=%s executable=%s dolt=%s/%s descriptor=%s", reference.Source.Commit, reference.Source.Tree, reference.Source.BinarySHA256, reference.Source.DoltVersion, reference.Source.DoltSHA256, reference.Source.Descriptor)
}
