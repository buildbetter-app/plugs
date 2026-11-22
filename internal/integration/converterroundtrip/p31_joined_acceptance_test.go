package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func p31SourceJSONL(t *testing.T, database string) []byte {
	t.Helper()
	a, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), joinedPhysicalDirectory(t, database), dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.ReleaseNativeDecoded()
	dump, err := reverseconverter.AdaptValidatedArchive(a)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reverseconverter.ConvertWithOptions(dump, reverseconverter.Options{All: true})
	if err != nil {
		t.Fatal(err)
	}
	return result.JSONL
}

func p31ProjectJSONL(t *testing.T, input []byte) map[string][]byte {
	t.Helper()
	byProject := map[string][][]byte{}
	for _, line := range bytes.Split(bytes.TrimSpace(input), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		if _, header := row["_schema"]; header {
			continue
		}
		if typ, ok := row["_type"].(string); ok && typ != "issue" {
			continue
		}
		project, _ := row["project"].(string)
		if project == "" {
			if id, ok := row["id"].(string); ok {
				switch {
				case strings.HasPrefix(id, "ops-"):
					project = "ops"
				case strings.HasPrefix(id, "sv2-"):
					project = "sv2"
				}
			}
			if key, ok := row["key"].(string); ok {
				switch {
				case strings.Contains(key, "/ops/"):
					project = "ops"
				case strings.Contains(key, "/sv2/"):
					project = "sv2"
				}
			}
		}
		byProject[project] = append(byProject[project], append([]byte(nil), line...))
	}
	result := map[string][]byte{}
	for project, lines := range byProject {
		sort.Slice(lines, func(i, j int) bool { return string(lines[i]) < string(lines[j]) })
		result[project] = append(bytes.Join(lines, []byte{'\n'}), '\n')
	}
	return result
}

const p31PinnedBD = "/tmp/plug-swarm/work/restore-roundtrip-r2-20261031/bd"

func p31BDComparable(value map[string]any) map[string]any {
	copy := map[string]any{}
	for key, item := range value {
		switch key {
		case "_type", "comment_count", "comments_omitted", "dependency_count", "dependent_count", "revision", "dependencies", "parent", "comments":
			continue
		}
		copy[key] = item
	}
	return copy
}

func p31BD(t *testing.T, directory string, stdin []byte, args ...string) []byte {
	t.Helper()
	cmd := exec.Command(p31PinnedBD, args...)
	cmd.Dir = directory
	cmd.Stdin = bytes.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bd %v: %v\n%s", args, err, out)
	}
	return out
}

func p31BDSourceReadback(t *testing.T, project string, source, expected []byte) {
	t.Helper()
	directory := t.TempDir()
	p31BD(t, directory, nil, "init", "--prefix", project, "--non-interactive", "--quiet")
	p31BD(t, directory, source, "import", "-")
	var issues []map[string]any
	if err := json.Unmarshal(p31BD(t, directory, nil, "list", "--all", "--json"), &issues); err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(expected), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if json.Unmarshal(line, &row) != nil {
			t.Fatal("invalid source jsonl")
		}
		if row["_type"] != "issue" {
			continue
		}
		id, _ := row["id"].(string)
		want[id] = row
	}
	if len(issues) != len(want) {
		t.Fatalf("bd list count=%d source=%d", len(issues), len(want))
	}
	for _, got := range issues {
		id, _ := got["id"].(string)
		if !reflect.DeepEqual(p31BDComparable(got), p31BDComparable(want[id])) {
			gb, _ := json.Marshal(got)
			wb, _ := json.Marshal(want[id])
			t.Fatalf("bd list readback differs id=%s got=%s want=%s", id, gb, wb)
		}
		var shown any
		if err := json.Unmarshal(p31BD(t, directory, nil, "show", id, "--json"), &shown); err != nil {
			t.Fatal(err)
		}
		shownMap, _ := shown.(map[string]any)
		if shownMap == nil {
			if list, ok := shown.([]any); ok && len(list) == 1 {
				shownMap, _ = list[0].(map[string]any)
			}
		}
		if !reflect.DeepEqual(p31BDComparable(shownMap), p31BDComparable(want[id])) {
			gb, _ := json.Marshal(shownMap)
			wb, _ := json.Marshal(want[id])
			t.Fatalf("bd show readback differs id=%s got=%s want=%s", id, gb, wb)
		}
		var deps any
		if err := json.Unmarshal(p31BD(t, directory, nil, "dep", "list", id, "--json"), &deps); err != nil {
			t.Fatal(err)
		}
		wantDeps := want[id]["dependencies"]
		if wantDeps == nil {
			wantDeps = []any{}
		}
		gotDeps, _ := deps.([]any)
		wantDepList, _ := wantDeps.([]any)
		if len(gotDeps) != len(wantDepList) {
			t.Fatalf("bd dep readback differs id=%s got=%v want=%v", id, deps, wantDeps)
		}
		for _, raw := range gotDeps {
			dep, _ := raw.(map[string]any)
			if dep["id"] == nil {
				continue
			}
			matched := false
			for _, expected := range wantDepList {
				e, _ := expected.(map[string]any)
				if dep["id"] == e["depends_on_id"] && dep["dependency_type"] == e["type"] {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("bd dep readback differs id=%s got=%v want=%v", id, deps, wantDeps)
			}
		}
	}
}

func TestP31JoinedCurrentReceiverReverseDirectAndNativeRestoredAllDrivers(t *testing.T) {
	want := map[string][]byte{}
	for _, project := range []string{"ops", "sv2"} {
		want[project] = p31ProjectJSONL(t, p31SourceJSONL(t, project))[project]
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := dumpformat.WithNativeDeltaBudget(context.Background(), dumpformat.NewNativeDeltaBudget(0))
			ledger := newJoinedOriginalLedger(t)
			store, initial, reopen := openIdentityTarget(t, engine)
			if err := initial.Release(ctx); err != nil {
				t.Fatal(err)
			}
			op, lease, release, err := HoldRestoreLease(ctx, store, "p31-joined")
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			profile := joinedPhysicalProfile(t, ctx, ledger, store, lease)
			report, err := RestoreNativeCaptureCollectionImportWithLease(op, ledger.binding, ledger.sources(), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			report.Close()
			check := func(label string, current driver.Store) {
				t.Helper()
				got, err := ReverseRestoredWithOptions(ctx, current, reverseconverter.Options{All: true})
				if err != nil {
					t.Fatalf("%s reverse: %v", label, err)
				}
				actual := p31ProjectJSONL(t, got.Result.JSONL)
				if actual["ops"] == nil {
					t.Logf("actual project keys=%v first=%s", func() []string {
						k := make([]string, 0, len(actual))
						for x := range actual {
							k = append(k, x)
						}
						return k
					}(), string(got.Result.JSONL[:min(300, len(got.Result.JSONL))]))
				}
				for project, expected := range want {
					if !bytes.Equal(actual[project], expected) {
						gotLines, wantLines := bytes.Split(bytes.TrimSpace(actual[project]), []byte{'\n'}), bytes.Split(bytes.TrimSpace(expected), []byte{'\n'})
						t.Logf("line counts project=%s got=%d want=%d", project, len(gotLines), len(wantLines))
						for i := 0; i < len(gotLines) && i < len(wantLines); i++ {
							if !bytes.Equal(gotLines[i], wantLines[i]) {
								t.Logf("first diff project=%s line=%d got=%s want=%s", project, i, gotLines[i], wantLines[i])
								break
							}
						}
						t.Fatalf("%s project=%s source-equal mismatch got=%d want=%d", label, project, len(actual[project]), len(expected))
					}
				}
				for project, expected := range want {
					p31BDSourceReadback(t, project, actual[project], expected)
				}
			}
			check("direct", store)
			nativeBytes, manifest, err := restore.Dump(ctx, store, "head")
			if err != nil {
				t.Fatal(err)
			}
			target, targetLease, targetReopen := openIdentityTarget(t, engine)
			restoreNativeHeadForTest(t, ctx, target, targetLease, nativeBytes, manifest)
			for i := 0; i < 2; i++ {
				target, targetLease = targetReopen()
			}
			check("native-restored", target)
			_ = reopen
		})
	}
}
