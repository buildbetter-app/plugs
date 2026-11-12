package b01readyorder_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

type b01CapturedReadyPoint struct {
	Name         string              `json:"name"`
	GoTime       time.Time           `json:"go_time"`
	SQLTime      time.Time           `json:"sql_time"`
	ClockSHA256  string              `json:"clock_sha256"`
	Default      []string            `json:"default_roles"`
	Hybrid       []string            `json:"hybrid_roles"`
	Observations map[string][]string `json:"actual_ids"`
}

type b01CapturedReadyInput struct {
	Archive    *dumpformat.Archive
	Members    map[string][]byte
	Roles      map[string]string
	Points     []b01CapturedReadyPoint
	Tables     map[string]dumpformat.NativeTable
	CrossPlane b01CapturedChildCounter
}

type b01CapturedChildCounter struct {
	ParentID, ChildID      string
	ChildNumber, HighWater uint64
}

type b01CapturedReadyCommand struct {
	Argv   []string `json:"argv"`
	Exit   int      `json:"exit_code"`
	Stdout string   `json:"stdout_sha256"`
	Stderr string   `json:"stderr_sha256"`
}

// Only committed card testdata can enter this reader. Authentication includes
// the complete compressed input, every inner member, and the existing native
// capture-set root/head/part stream. A partial trace cannot satisfy this path.
func b01ReadCapturedReady(t *testing.T, fixturePath, wantSHA string) *b01CapturedReadyInput {
	t.Helper()
	if !strings.HasPrefix(fixturePath, "testdata/") || path.Clean(fixturePath) != fixturePath || len(wantSHA) != 64 {
		t.Fatal("B01 requires a committed card fixture and exact public SHA256")
	}
	raw, err := os.ReadFile(fixturePath)
	if err != nil || len(raw) > 20971520 || b01CaptureHash(raw) != wantSHA {
		t.Fatal("complete B01 native fixture absent or authentication differs", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	members := map[string][]byte{}
	reader, decoded := tar.NewReader(gz), int64(0)
	for {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, segment := range strings.Split(h.Name, "/") {
			if segment == "poll.log" || segment == ".artifact" {
				t.Fatal("excluded native member before read")
			}
		}
		if h.Typeflag != tar.TypeReg || !strings.HasPrefix(h.Name, "b01-ready/") || path.Clean(h.Name) != h.Name || h.Size < 0 || h.Size > 536870912-decoded {
			t.Fatal("noncanonical or oversized B01 native input member")
		}
		name := strings.TrimPrefix(h.Name, "b01-ready/")
		if _, duplicate := members[name]; duplicate {
			t.Fatal("duplicate native fixture member", name)
		}
		data, err := io.ReadAll(io.LimitReader(reader, h.Size+1))
		if err != nil || int64(len(data)) != h.Size {
			t.Fatal("native fixture member length differs", err)
		}
		members[name], decoded = data, decoded+h.Size
	}
	var hashes map[string]string
	b01CaptureDecode(t, members["INNER-HASHES.json"], &hashes)
	if len(hashes) != len(members)-1 {
		t.Fatal("native inner hash census differs")
	}
	for name, data := range members {
		if name != "INNER-HASHES.json" && hashes[name] != b01CaptureHash(data) {
			t.Fatal("native inner authentication differs", name)
		}
	}
	prefix := "physical/capture-set/"
	archive, err := dumpformat.ReadNativeCaptureSet(context.Background(), bytes.NewReader(members[prefix+"capture-set.json"]), bytes.NewReader(members[prefix+"head.tar"]), func(_ context.Context, n uint64) (io.ReadCloser, error) {
		data, ok := members[prefix+fmt.Sprintf("parts/%020d.part", n)]
		if !ok {
			return nil, fmt.Errorf("complete native part %d missing", n)
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}, dumpformat.NativeCaptureSetOptions{PrepareReceiving: true})
	if err != nil {
		t.Fatal("authenticate full native capture stream", err)
	}
	t.Cleanup(archive.ReleaseNativeDecoded)
	if len(archive.Manifest().Projects) != 1 || archive.Manifest().Projects[0].Prefix != b01CaptureProject || archive.Manifest().SourcePlaneAddressContract != dumpformat.SourcePlaneAddressContract || archive.Manifest().SourceFieldsContract != dumpformat.IssueSourceFieldsContract {
		t.Fatal("native B01 source project/plane contract differs")
	}
	var observed struct {
		Format string                  `json:"format"`
		Roles  map[string]string       `json:"roles"`
		Points []b01CapturedReadyPoint `json:"points"`
	}
	b01CaptureDecode(t, members["observations.json"], &observed)
	if observed.Format != "b01-native-ready-observations/1" || len(observed.Points) != 7 {
		t.Fatal("native independent clock observations incomplete")
	}
	var final struct {
		Tables map[string]dumpformat.NativeTable `json:"tables"`
	}
	b01CaptureDecode(t, members["states/final-before-capture.json"], &final)
	input := &b01CapturedReadyInput{Archive: archive, Members: members, Roles: observed.Roles, Points: observed.Points, Tables: final.Tables}
	input.authenticateSQLRows(t)
	input.authenticateRoles(t)
	input.authenticateCrossPlaneChild(t)
	input.authenticateClockReadback(t)
	input.authenticateOrders(t)
	if !reflect.DeepEqual(input.nativeState(t, "final-before-capture"), input.nativeState(t, "final-after-capture")) {
		t.Fatal("physical capture changed original complete native tables or clean graph")
	}
	return input
}

func (in *b01CapturedReadyInput) command(t *testing.T, name string) []byte {
	t.Helper()
	receipt, stdout := in.commandOriginal(t, name)
	if receipt.Argv[1] == "ready" && !reflect.DeepEqual(in.nativeState(t, name+"-ready-before"), in.nativeState(t, name+"-ready-after")) {
		t.Fatal("original native ready changed complete tables, schema, ignore or clean graph", name)
	}
	return stdout
}

func (in *b01CapturedReadyInput) nativeState(t *testing.T, name string) map[string]any {
	t.Helper()
	var state map[string]any
	b01CaptureDecode(t, in.Members["states/"+name+".json"], &state)
	log, ok := state["commit_log"].(map[string]any)
	if !ok || log["Clean"] != true || log["Head"] == nil || log["Head"] == "" {
		t.Fatal("original native state lacks its actual clean source graph", name)
	}
	return state
}

func (in *b01CapturedReadyInput) commandOriginal(t *testing.T, name string) (b01CapturedReadyCommand, []byte) {
	t.Helper()
	var paths []string
	for filename := range in.Members {
		if strings.HasPrefix(filename, "commands/") && strings.HasSuffix(filename, "-"+name+".json") {
			paths = append(paths, filename)
		}
	}
	if len(paths) != 1 {
		t.Fatalf("native command %s has %d receipts", name, len(paths))
	}
	var receipt b01CapturedReadyCommand
	b01CaptureDecode(t, in.Members[paths[0]], &receipt)
	stem := strings.TrimSuffix(paths[0], ".json")
	stdout, stderr := in.Members[stem+".stdout"], in.Members[stem+".stderr"]
	if receipt.Exit != 0 || len(receipt.Argv) < 2 || receipt.Stdout != b01CaptureHash(stdout) || receipt.Stderr != b01CaptureHash(stderr) {
		t.Fatal("native command actual status/original bytes differ", name)
	}
	return receipt, stdout
}

func (in *b01CapturedReadyInput) clock(t *testing.T, name string, goTime, sqlTime time.Time, wantSHA string) {
	t.Helper()
	clockBytes := in.Members["clocks/"+name+"/clock.json"]
	if len(clockBytes) == 0 || wantSHA != "" && b01CaptureHash(clockBytes) != wantSHA {
		t.Fatal("native clock file is absent or its exact hash differs", name)
	}
	type clockInput struct {
		Version string    `json:"version"`
		Step    string    `json:"step"`
		Go      time.Time `json:"go_time"`
		SQL     time.Time `json:"sql_time"`
	}
	var input clockInput
	b01CaptureDecode(t, clockBytes, &input)
	var observedClock struct {
		Clock struct {
			Input      clockInput             `json:"input"`
			GoObserved time.Time              `json:"go_observed"`
			GoTick     int64                  `json:"go_input_resolution_ns"`
			SQLTick    int64                  `json:"sql_input_resolution_ns"`
			DefaultSQL int64                  `json:"sql_default_utc_timestamp_resolution_ns"`
			SQLResult  dumpformat.NativeTable `json:"sql_result"`
			Columns    dumpformat.NativeTable `json:"column_precision"`
		} `json:"capture_clock"`
	}
	b01CaptureDecode(t, in.Members["clocks/"+name+"/actual-clock-sidecar.json"], &observedClock)
	actual := observedClock.Clock
	if input.Version != "beads-capture-clock/1" || input.Step != name || !input.Go.Equal(goTime) || !input.SQL.Equal(sqlTime) || !reflect.DeepEqual(actual.Input, input) || !actual.GoObserved.Equal(goTime) || actual.GoTick != 1 || actual.SQLTick != 1000 || actual.DefaultSQL != 1000 || len(actual.SQLResult.Rows) != 1 || len(actual.Columns.Rows) != 6 {
		t.Fatal("native actual independent clock readback differs", name)
	}
	stamp := sqlTime.Format("2006-01-02T15:04:05.000000Z")
	if actual.SQLResult.Rows[0]["utc_microseconds"] != stamp || actual.SQLResult.Rows[0]["utc_default"] != stamp {
		t.Fatal("genuine SQL expression readback differs from independent input", name)
	}
	var direct map[string]string
	b01CaptureDecode(t, in.Members["clocks/"+name+"/actual-sql.json"], &direct)
	if direct["utc_timestamp_6"] != stamp || direct["utc_timestamp"] != stamp {
		t.Fatal("direct native SQL clock differs", name)
	}
}

func (in *b01CapturedReadyInput) authenticateClockReadback(t *testing.T) {
	t.Helper()
	boundary := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	for _, sample := range []struct {
		name string
		goAt time.Time
	}{
		{"create-old", boundary.Add(-72 * time.Hour)},
		{"create-aging", boundary.Add(-48 * time.Hour)},
		{"create-tie-z", boundary.Add(-time.Hour)},
		{"create-tie-a", boundary.Add(-time.Hour)},
		{"create-a-later", boundary.Add(-time.Hour + time.Second)},
		{"active-create", boundary.Add(-time.Hour)},
		{"active-at", boundary},
	} {
		in.clock(t, sample.name, sample.goAt, boundary.Add(10*time.Second), "")
	}
	for _, sample := range []struct {
		name  string
		delta time.Duration
	}{{"48h-before", -time.Microsecond}, {"48h-at", 0}, {"48h-after", time.Microsecond}, {"60s-before", time.Minute - time.Nanosecond}, {"60s-at", time.Minute}, {"60s-after", time.Minute + time.Nanosecond}} {
		in.clock(t, "go-only-"+sample.name, boundary.Add(sample.delta), boundary.Add(10*time.Second), "")
	}
	for _, sample := range []struct {
		name  string
		delta time.Duration
	}{{"before", -time.Microsecond}, {"at", 0}, {"after", time.Microsecond}} {
		in.clock(t, "sql-only-"+sample.name, boundary, boundary.Add(sample.delta), "")
	}
	for _, point := range in.Points {
		in.clock(t, point.Name, point.GoTime, point.SQLTime, point.ClockSHA256)
	}
}

func (in *b01CapturedReadyInput) authenticateOrders(t *testing.T) {
	t.Helper()
	boundary := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	priority := []string{"aging", "tie-a", "tie-z", "a-later", "old"}
	for _, sample := range []struct {
		name  string
		delta time.Duration
	}{{"48h-before", -time.Microsecond}, {"48h-at", 0}, {"48h-after", time.Microsecond}, {"60s-before", time.Minute - time.Nanosecond}, {"60s-at", time.Minute}, {"60s-after", time.Minute + time.Nanosecond}} {
		hybrid := priority
		if sample.delta > 0 {
			hybrid = []string{"tie-a", "tie-z", "a-later", "old", "aging"}
		}
		for _, limit := range []int{0, 1, 2, 1000} {
			for _, policy := range []struct {
				name string
				want []string
			}{{"default", priority}, {"priority", priority}, {"hybrid", hybrid}, {"oldest", []string{"old", "aging", "tie-a", "tie-z", "a-later"}}} {
				in.nativeIDs(t, fmt.Sprintf("%s-%s-%d", sample.name, policy.name, limit), policy.want, limit)
			}
		}
	}
	for _, name := range []string{"before", "at", "after"} {
		in.nativeIDs(t, "sql-only-"+name, priority, 1000)
	}
	expectedPoints := []struct {
		name        string
		goAt, sqlAt time.Time
	}{
		{"mixed-before", boundary.Add(-time.Microsecond), boundary.Add(7 * time.Hour)},
		{"mixed-at", boundary, boundary.Add(7 * time.Hour)},
		{"mixed-after", boundary.Add(time.Microsecond), boundary.Add(7 * time.Hour)},
		{"mixed-sql-only", boundary.Add(time.Microsecond), boundary.Add(7*time.Hour + time.Second)},
		{"mixed-60s-before", boundary.Add(time.Minute - time.Nanosecond), boundary.Add(7*time.Hour + time.Second)},
		{"mixed-60s-at", boundary.Add(time.Minute), boundary.Add(7*time.Hour + time.Second)},
		{"mixed-60s-after", boundary.Add(time.Minute + time.Nanosecond), boundary.Add(7*time.Hour + time.Second)},
	}
	for i, want := range expectedPoints {
		point := in.Points[i]
		if point.Name != want.name || !point.GoTime.Equal(want.goAt) || !point.SQLTime.Equal(want.sqlAt) {
			t.Fatal("native clock point differs from independent exact input", want.name)
		}
		hybrid := []string{"aging", "wisp", "control"}
		if want.goAt.After(boundary) {
			hybrid = []string{"wisp", "control", "aging"}
		}
		for _, limit := range []int{0, 1, 2, 1000} {
			in.nativeIDs(t, fmt.Sprintf("%s-default-%d", want.name, limit), []string{"aging", "wisp", "control"}, limit)
			in.nativeIDs(t, fmt.Sprintf("%s-hybrid-%d", want.name, limit), hybrid, limit)
			in.nativeIDs(t, fmt.Sprintf("%s-durable-default-%d", want.name, limit), []string{"aging", "control"}, limit)
			durable := []string{"aging", "control"}
			if want.goAt.After(boundary) {
				durable = []string{"control", "aging"}
			}
			in.nativeIDs(t, fmt.Sprintf("%s-durable-hybrid-%d", want.name, limit), durable, limit)
		}
	}
	for _, limit := range []int{0, 1, 2, 1000} {
		in.nativeIDs(t, fmt.Sprintf("ordinary-child-of-active-%d", limit), []string{"ordinary_child"}, limit)
		in.nativeIDs(t, fmt.Sprintf("active-child-of-ordinary-%d", limit), []string{"active_child"}, limit)
		in.nativeIDs(t, fmt.Sprintf("active-unblocked-%d", limit), []string{"aging", "blocked", "wisp", "control"}, limit)
	}
	in.nativeIDs(t, "unknown-parent", []string{}, 0)
	in.nativeIDs(t, "active-reblocked", []string{"aging", "wisp", "control"}, 0)
}

func (in *b01CapturedReadyInput) authenticateRoles(t *testing.T) {
	t.Helper()
	seen := map[string]bool{}
	for _, role := range []string{"old", "aging", "tie-z", "tie-a", "a-later", "wisp", "blocked", "control", "blocker", "ordinary_child", "active_child"} {
		var created struct {
			ID string `json:"id"`
		}
		b01CaptureDecode(t, in.command(t, "create-"+role), &created)
		if created.ID == "" || created.ID != in.Roles[role] || seen[created.ID] {
			t.Fatal("native source identity lacks its unique public create receipt", role)
		}
		seen[created.ID] = true
	}
	for role, want := range map[string]string{"old": "clockready-old", "aging": "clockready-aging", "tie-z": "clockready-tie-z", "tie-a": "clockready-tie-a", "a-later": "clockready-a-later", "control": "clockready-control", "blocker": "clockready-blocker"} {
		if in.Roles[role] != want {
			t.Fatal("native explicit public input identity differs", role)
		}
	}
}

// These values come only from authenticated native create originals and SQL
// rows. The allocation high-water mark is independent of the retained child's
// suffix: a capture may retain child .3 while its parent's counter is 17.
func (in *b01CapturedReadyInput) authenticateCrossPlaneChild(t *testing.T) {
	t.Helper()
	parent, child := in.Roles["wisp"], in.Roles["ordinary_child"]
	receipt, _ := in.commandOriginal(t, "create-ordinary_child")
	wantArgv := []string{"create", "ordinary child of active parent", "--json", "--parent", parent, "--no-inherit-labels", "--labels", "active-child"}
	if !reflect.DeepEqual(receipt.Argv[1:], wantArgv) {
		t.Fatal("durable child lacks its exact public active-parent create inputs")
	}
	if !strings.HasPrefix(child, parent+".") {
		t.Fatal("native child ID does not retain its original active-parent prefix")
	}
	suffix := strings.TrimPrefix(child, parent+".")
	childNumber, err := strconv.ParseUint(suffix, 10, 64)
	if err != nil || childNumber == 0 || strconv.FormatUint(childNumber, 10) != suffix {
		t.Fatal("native child ID lacks its original canonical positive child number")
	}
	for _, entry := range []struct{ family, id string }{{"issues", child}, {"wisps", parent}} {
		count := 0
		for _, row := range in.Tables[entry.family].Rows {
			if row["id"] == entry.id {
				count++
			}
		}
		if count != 1 {
			t.Fatal("native cross-plane identity lacks its unique physical SQL row", entry)
		}
	}
	for _, entry := range []struct{ family, id string }{{"wisps", child}, {"issues", parent}} {
		for _, row := range in.Tables[entry.family].Rows {
			if row["id"] == entry.id {
				t.Fatal("native cross-plane identity was substituted into the other plane", entry)
			}
		}
	}
	edges := 0
	for _, row := range in.Tables["dependencies"].Rows {
		if row["issue_id"] != child || row["type"] != "parent-child" {
			continue
		}
		if b01CaptureText(t, row, "depends_on_wisp_id") != parent {
			t.Fatal("native durable child points to a different active parent")
		}
		for _, arm := range []string{"depends_on_issue_id", "depends_on_external"} {
			if value, present := row[arm]; !present || value != nil {
				t.Fatal("native cross-plane parent edge lacks its typed SQL NULL arm", arm)
			}
		}
		edges++
	}
	counters, highWater := 0, uint64(0)
	for _, row := range in.Tables["wisp_child_counters"].Rows {
		if row["parent_id"] != parent {
			continue
		}
		number, present := row["last_child"].(json.Number)
		if !present {
			t.Fatal("native active-parent counter is absent or not a lossless SQL integer")
		}
		highWater, err = strconv.ParseUint(number.String(), 10, 64)
		if err != nil || strconv.FormatUint(highWater, 10) != number.String() || highWater < childNumber {
			t.Fatal("native active-parent high-water mark does not preserve its allocated child number")
		}
		counters++
	}
	projectCounter, present := in.Archive.Manifest().Projects[0].ChildCounters[parent]
	if edges != 1 || counters != 1 || !present || projectCounter != highWater {
		t.Fatal("complete capture lost the original child edge or exact active-parent high-water mark")
	}
	in.CrossPlane = b01CapturedChildCounter{ParentID: parent, ChildID: child, ChildNumber: childNumber, HighWater: highWater}
	t.Logf("B01_CAPTURE_NATIVE_CHILD_COUNTER parent=%s child=%s child_number=%d high_water=%d", parent, child, childNumber, highWater)
}

func b01CaptureCanonicalRows(t *testing.T, rows []dumpformat.Row) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(raw))
	}
	sort.Strings(out)
	return out
}

func (in *b01CapturedReadyInput) authenticateSQLRows(t *testing.T) {
	t.Helper()
	raw, err := in.Archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string][]dumpformat.Row{}
	for _, record := range raw {
		var row dumpformat.Record
		b01CaptureDecode(t, record.Raw, &row)
		rows[record.Table] = append(rows[record.Table], row.Source.Fields)
	}
	for _, family := range []string{"issues", "dependencies", "wisps", "wisp_dependencies", "wisp_comments", "wisp_labels", "wisp_events", "wisp_child_counters"} {
		if err := b01CapturedSourceRowsMatch(family, rows[family], in.Tables[family].Rows); len(in.Tables[family].Rows) == 0 || err != nil {
			t.Fatal("complete authenticated capture differs from actual SQL under its explicit source-fields contract", family, err)
		}
	}
}
