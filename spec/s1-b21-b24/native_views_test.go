package s1b21b24

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func nativeRecordViewsAndDefer(t *testing.T, root, work, binary, corpus string, env []string, commands *[]nativeCommandReceipt, run func(*testing.T, string, ...string) []byte, sql func(*testing.T, string, string) []map[string]any, census func(*testing.T, string) map[string]any) {
	t.Helper()
	decodeRows := func(t *testing.T, label string, b []byte) []map[string]any {
		var rows []map[string]any
		if err := json.Unmarshal(b, &rows); err != nil {
			t.Fatalf("%s invalid native rows: %v %s", label, err, b)
		}
		return rows
	}
	contains := func(rows []map[string]any, id string) bool {
		for _, r := range rows {
			if r["id"] == nativeID(id) {
				return true
			}
		}
		return false
	}
	t.Run("B23-full-predicate-count-labels-and-ready", func(t *testing.T) {
		run(t, "label-alpha", "--json", "label", "add", nativeID("alphaa"), "Alpha")
		run(t, "label-beta", "--json", "label", "add", nativeID("betaaa"), "alpha")
		rows := decodeRows(t, "native-list", run(t, "list-page", "--json", "list", "--all", "--title", "Needle", "--limit", "1", "--sort", "id"))
		var count struct {
			Count int `json:"count"`
		}
		raw := run(t, "count-full", "--json", "count", "--title", "Needle")
		if err := json.Unmarshal(raw, &count); err != nil || count.Count != 2 || len(rows) != 1 {
			t.Fatalf("native count was page limited: list=%v count=%s %v", rows, raw, err)
		}
		for _, pair := range []struct{ label, id string }{{"Alpha", "alphaa"}, {"alpha", "betaaa"}} {
			list := decodeRows(t, "native-label-list", run(t, "list-label-"+pair.label, "--json", "list", "--all", "--label", pair.label, "--limit", "0"))
			if len(list) != 1 || !contains(list, pair.id) {
				t.Fatalf("native label collation %s=%v", pair.label, list)
			}
		}
		run(t, "block-beta", "--json", "dep", "add", nativeID("betaaa"), nativeID("alphaa"))
		ready := decodeRows(t, "native-ready", run(t, "ready-blocked", "--json", "ready", "--limit", "0"))
		if !contains(ready, "alphaa") || contains(ready, "betaaa") {
			t.Fatal("native ready did not honor blocker")
		}
		nativeWriteJSON(t, filepath.Join(root, "native-view-observations.json"), map[string]any{"native_count": count.Count, "limited_page": rows, "ready": ready, "label_matching": "exact-case-sensitive", "plug_numeric_revision_projection": "verified separately on imported live records"})
	})
	if t.Failed() {
		return
	}
	t.Run("B24-native-untimed-timed-and-reason", func(t *testing.T) {
		before := census(t, "blank-reason-before")
		nativeRunCommand(t, root, commands, "blank-defer-reason", corpus, work, env, []string{binary, "--json", "defer", nativeID("untimed"), "--reason", " "}, nil, 1)
		if !reflect.DeepEqual(before, census(t, "blank-reason-after")) {
			t.Fatal("blank native reason changed physical state")
		}
		run(t, "defer-indefinite", "--json", "defer", nativeID("untimed"), "--reason", "waiting explicitly")
		run(t, "defer-future", "--json", "defer", nativeID("timedaa"), "--until", "2100-01-01", "--reason", "future timed")
		ready := decodeRows(t, "native-ready-future", run(t, "ready-future", "--json", "ready", "--limit", "0"))
		if contains(ready, "untimed") || contains(ready, "timedaa") {
			t.Fatal("native deferred rows appeared ready")
		}
		// A real past-date native invocation must emit its warning. It is not a
		// simulated clock and does not qualify equality at a timer boundary.
		run(t, "defer-past-warning", "defer", nativeID("timedaa"), "--until", "2001-01-01", "--reason", "past timed")
		if len(*commands) == 0 {
			t.Fatal("missing command receipt")
		}
		last := (*commands)[len(*commands)-1]
		if last.StderrSHA256 == nativeHashBytes(nil) {
			t.Fatal("native past-date defer omitted warning")
		}
		ready = decodeRows(t, "native-ready-wake", run(t, "ready-auto-wake", "--json", "ready", "--limit", "0"))
		if !contains(ready, "timedaa") || contains(ready, "untimed") {
			t.Fatal("native timed auto-wake/indefinite distinction lost")
		}
		rows := sql(t, "defer-notes-readback", "SELECT id,status,notes,defer_until FROM issues WHERE id IN ('"+nativeID("untimed")+"','"+nativeID("timedaa")+"') ORDER BY id")
		if len(rows) != 2 {
			t.Fatal("missing native defer rows")
		}
		for _, r := range rows {
			if r["id"] == nativeID("untimed") && (!strings.Contains(r["notes"].(string), "waiting explicitly") || r["status"] != "deferred") {
				t.Fatal("native indefinite reason/status lost")
			}
		}
		before = census(t, "stale-zero-before")
		nativeRunCommand(t, root, commands, "stale-days-zero", corpus, work, env, []string{binary, "--json", "stale", "--days", "0"}, nil, 1)
		if !reflect.DeepEqual(before, census(t, "stale-zero-after")) {
			t.Fatal("invalid native stale read changed state")
		}
		// The native engine has no clock override. Seed old timestamps through
		// its real public maintenance command, recording this setup separately;
		// this is not evidence of an exact native timer equality boundary.
		run(t, "stale-age-setup", "--json", "sql", "UPDATE issues SET created_at='2000-01-01 00:00:00', updated_at='2000-01-02 00:00:00' WHERE id IN ('"+nativeID("alphaa")+"','"+nativeID("closednow")+"','"+nativeID("untimed")+"')")
		run(t, "stale-age-settle", "dolt", "commit")
		before = census(t, "stale-read-before")
		stale := decodeRows(t, "native-stale", run(t, "stale-read", "--readonly", "--json", "stale", "--days", "1", "--limit", "0"))
		deferred := decodeRows(t, "native-stale-deferred", run(t, "stale-read-deferred", "--readonly", "--json", "stale", "--days", "1", "--status", "deferred", "--limit", "0"))
		if len(stale) != 1 || !contains(stale, "alphaa") || len(deferred) != 1 || !contains(deferred, "untimed") {
			t.Fatalf("native stale age/status membership: default=%v deferred=%v", stale, deferred)
		}
		if !reflect.DeepEqual(before, census(t, "stale-read-after")) {
			t.Fatal("successful native stale reads changed physical state")
		}
		nativeWriteJSON(t, filepath.Join(root, "native-defer-observations.json"), map[string]any{"ready_after_wake": ready, "stored_rows": rows, "stale_default": stale, "stale_deferred": deferred, "stale_age_setup": "real native public SQL maintenance command; no clock override", "native": "timed auto-wake; indefinite remains deferred; supplied blank reason refuses", "plug_adopted": "untimed defer with reason and explicit reopen; no automatic wake", "exact_timer_boundary": "not represented by a native clock override"})
	})
}

// nativeRecordHistoryOffWake runs before the exchange corpus is exported, so
// the actual allocated task and disabled setting participate in the complete
// archive and every import. The role-only refusal corpus remains separate.
// History is disabled through the real public config command, then a real CLI
// create/defer/ready path wakes one expired dated defer. The complete censuses
// are retained around the wake;
// every table whose name carries version semantics must remain byte-equal.
// That proves no version row was minted.  A table census cannot prove whether
// the native implementation read a version row and then discarded it, so the
// no-version-read half of the upstream B24 pin remains explicitly unqualified.
func nativeRecordHistoryOffWake(t *testing.T, root string, run func(*testing.T, string, ...string) []byte, sql func(*testing.T, string, string) []map[string]any, census func(*testing.T, string) map[string]any) nativeHistoryOffObservation {
	t.Helper()
	var observation nativeHistoryOffObservation
	if !t.Run("B24-native-history-off-expired-wake", func(t *testing.T) {
		run(t, "history-off-disable", "config", "set", "versioned-history.enabled", "false")
		settingsRaw := run(t, "history-off-config-readback", "--json", "config", "list")
		settings, err := nativeReadPublicConfigSettings(settingsRaw)
		if err != nil {
			t.Fatalf("history-off config readback is not a valid public settings object: %v", err)
		}
		historyDisabled := false
		for _, setting := range settings {
			if setting.Key == "versioned-history.enabled" && setting.Value == "false" {
				historyDisabled = true
			}
		}
		if !historyDisabled {
			t.Fatalf("history-off config readback did not retain versioned-history.enabled=false: %v", settings)
		}
		createdRaw := run(t, "history-off-create", "--json", "create", "--title", "B24 history-off wake probe", "--type", "task")
		var created struct {
			ID   string `json:"id"`
			Data *struct {
				ID string `json:"id"`
			} `json:"data,omitempty"`
		}
		if err := json.Unmarshal(createdRaw, &created); err != nil {
			t.Fatalf("history-off create did not return one real ref: %v %s", err, createdRaw)
		}
		target := created.ID
		if target == "" && created.Data != nil {
			target = created.Data.ID
		}
		if target == "" {
			t.Fatalf("history-off create did not return a real native task id: %s", createdRaw)
		}
		run(t, "history-off-defer", "--json", "defer", target, "--until", "2001-01-01", "--reason", "history-off wake")
		before := census(t, "history-off-wake-before")
		rows := sql(t, "history-off-deferred-row", "SELECT id,issue_type,status,defer_until FROM issues WHERE id='"+target+"'")
		if len(rows) != 1 || rows[0]["issue_type"] != "task" || rows[0]["status"] != "deferred" {
			t.Fatalf("history-off target was not a dated defer before wake: %v", rows)
		}
		readyRaw := run(t, "history-off-ready-wake", "--json", "ready", "--limit", "0")
		var ready []map[string]any
		if err := json.Unmarshal(readyRaw, &ready); err != nil {
			t.Fatalf("history-off ready response is not a row array: %v %s", err, readyRaw)
		}
		visible := false
		for _, row := range ready {
			if row["id"] == target {
				visible = true
				break
			}
		}
		if !visible {
			t.Fatalf("history-off wake did not return the expired target: %s", target)
		}
		after := census(t, "history-off-wake-after")
		woken := sql(t, "history-off-woken-row", "SELECT id,issue_type,status,defer_until FROM issues WHERE id='"+target+"'")
		if len(woken) != 1 || woken[0]["issue_type"] != "task" || woken[0]["status"] != "open" || woken[0]["defer_until"] != nil {
			t.Fatalf("history-off wake did not clear the defer: %v", woken)
		}
		sourceRow := nativeHistoryOffSourceRow(t, after, target)
		sourceColumns := sql(t, "history-off-source-columns", "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='issues' ORDER BY ORDINAL_POSITION")
		typedSourceRow, err := nativePhysicalSourceFields(sourceRow, sourceColumns)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range woken[0] {
			actual, present := sourceRow[key]
			if !present || !nativeEqualJSON(actual, value) {
				t.Fatalf("full history-off source census differs from direct wake read at %s", key)
			}
		}
		versionTables := map[string]bool{}
		for table, initial := range before {
			if strings.Contains(strings.ToLower(table), "version") {
				versionTables[table] = true
				if !reflect.DeepEqual(initial, after[table]) {
					t.Fatalf("history-off wake minted or rewrote %s rows", table)
				}
			}
		}
		if len(versionTables) == 0 {
			t.Fatal("history-off census exposed no version-bearing table to compare")
		}
		// Only the wake's documented physical planes may move.  Every unrelated
		// table remains exactly equal, including source/config/auxiliary planes.
		mutable := map[string]bool{
			"issues": true, "events": true, "ready_issues": true,
			"bd_events_journal": true, "bd_events_seq": true,
			"local_metadata": true, "@dolt_log": true,
		}
		for table, initial := range before {
			if !mutable[table] && !reflect.DeepEqual(initial, after[table]) {
				t.Fatalf("history-off wake changed unrelated physical table %s", table)
			}
		}
		secondBefore := census(t, "history-off-second-ready-before")
		run(t, "history-off-second-ready", "--json", "ready", "--limit", "0")
		secondAfter := census(t, "history-off-second-ready-after")
		if !reflect.DeepEqual(secondBefore, secondAfter) {
			t.Fatal("history-off ready re-woke or rewrote state after the first wake")
		}
		issueRows := len(after["issues"].([]map[string]any))
		observation = nativeHistoryOffObservation{Target: target, Settings: settings, WokenRow: sourceRow, Columns: sourceColumns, IssueRows: issueRows}
		nativeWriteJSON(t, filepath.Join(root, "native-history-off-wake.json"), map[string]any{
			"config": settings, "target": target, "ready_after_wake": ready, "deferred_row": rows,
			"woken_row": woken, "before": before, "after": after,
			"full_source_row": sourceRow, "full_source_row_census": "history-off-wake-after/issues",
			"source_columns": sourceColumns, "full_typed_source_row": typedSourceRow,
			"source_issue_rows": issueRows,
			"version_tables":    versionTables, "version_rows_unchanged": true,
			"unrelated_tables_unchanged": true, "second_ready_noop": true,
			"no_version_read_claim": "UNQUALIFIED: physical before/after proves no version mint only",
			"clock_boundary":        "native wall clock; exact SQL/Go equality is not claimed",
		})
	}) {
		t.Fatal("native history-off wake failed; archive and replay are not attempted")
	}
	return observation
}
