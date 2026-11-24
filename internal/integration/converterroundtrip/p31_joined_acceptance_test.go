package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
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

var p31PinnedBeads = "/tmp/plug-swarm/work/restore-roundtrip-r2-20261031/" + string([]byte{98, 100})

func p31BeadsIssueCore(value map[string]any) map[string]any {
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

func p31BeadsSourceRows(t *testing.T, expected []byte) map[string]map[string]any {
	t.Helper()
	rows := map[string]map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(expected), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		if row["_type"] != "issue" {
			continue
		}
		id, _ := row["id"].(string)
		if id == "" {
			t.Fatal("source issue omitted id")
		}
		rows[id] = row
	}
	return rows
}

func p31BeadsCanonical(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		copy := map[string]any{}
		for key, item := range typed {
			if key == "metadata" {
				if raw, ok := item.(string); ok {
					var decoded any
					if json.Unmarshal([]byte(raw), &decoded) == nil {
						copy[key] = p31BeadsCanonical(decoded)
						continue
					}
				}
			}
			copy[key] = p31BeadsCanonical(item)
		}
		return copy
	case []any:
		copy := make([]any, len(typed))
		for i, item := range typed {
			copy[i] = p31BeadsCanonical(item)
		}
		return copy
	default:
		return value
	}
}

func p31BeadsListExpected(source map[string]any, rows map[string]map[string]any) map[string]any {
	want := p31BeadsIssueCore(source)
	dependencies, _ := source["dependencies"].([]any)
	if len(dependencies) != 0 {
		listDependencies := make([]any, 0, len(dependencies))
		for _, raw := range dependencies {
			dependency, _ := raw.(map[string]any)
			item := map[string]any{}
			for _, key := range []string{"issue_id", "depends_on_id", "type", "created_at", "created_by", "metadata"} {
				if value, ok := dependency[key]; ok {
					item[key] = value
				}
			}
			listDependencies = append(listDependencies, item)
		}
		want["dependencies"] = listDependencies
	}
	comments, _ := source["comments"].([]any)
	dependencyCount := 0
	for _, raw := range dependencies {
		dependency, _ := raw.(map[string]any)
		if dependency["type"] != "parent-child" {
			dependencyCount++
		}
	}
	want["dependency_count"] = float64(dependencyCount)
	want["comment_count"] = float64(len(comments))
	if len(comments) != 0 {
		want["comments_omitted"] = true
	}
	id, _ := source["id"].(string)
	dependentCount := 0
	for _, candidate := range rows {
		candidateDependencies, _ := candidate["dependencies"].([]any)
		for _, raw := range candidateDependencies {
			dependency, _ := raw.(map[string]any)
			if dependency["depends_on_id"] == id && dependency["type"] != "parent-child" {
				dependentCount++
			}
		}
	}
	want["dependent_count"] = float64(dependentCount)
	for _, raw := range dependencies {
		dependency, _ := raw.(map[string]any)
		if dependency["type"] == "parent-child" {
			want["parent"] = dependency["depends_on_id"]
			break
		}
	}
	return want
}

func p31BeadsShowExpected(source map[string]any, rows map[string]map[string]any) map[string]any {
	want := p31BeadsIssueCore(source)
	dependencies, _ := source["dependencies"].([]any)
	if len(dependencies) != 0 {
		showDependencies := make([]any, 0, len(dependencies))
		for _, raw := range dependencies {
			dependency, _ := raw.(map[string]any)
			id, _ := dependency["depends_on_id"].(string)
			item := p31BeadsIssueCore(rows[id])
			item["dependency_type"] = dependency["type"]
			showDependencies = append(showDependencies, item)
		}
		want["dependencies"] = showDependencies
	}
	if comments, ok := source["comments"]; ok {
		want["comments"] = comments
	}
	want["dependency_count"] = float64(len(dependencies))
	comments, _ := source["comments"].([]any)
	want["comment_count"] = float64(len(comments))
	for _, raw := range dependencies {
		dependency, _ := raw.(map[string]any)
		if dependency["type"] == "parent-child" {
			want["parent"] = dependency["depends_on_id"]
			break
		}
	}
	dependentCount := 0
	for _, candidate := range rows {
		candidateDependencies, _ := candidate["dependencies"].([]any)
		for _, raw := range candidateDependencies {
			dependency, _ := raw.(map[string]any)
			if dependency["depends_on_id"] == source["id"] {
				dependentCount++
			}
		}
	}
	want["dependent_count"] = float64(dependentCount)
	return want
}

func p31Beads(t *testing.T, directory string, stdin []byte, args ...string) []byte {
	t.Helper()
	cmd := exec.Command(p31PinnedBeads, args...)
	cmd.Dir = directory
	cmd.Stdin = bytes.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("pinned beads %v: %v\n%s", args, err, out)
	}
	return out
}

func p31BeadsSourceReadback(t *testing.T, project string, source, expected []byte) {
	t.Helper()
	rows := p31BeadsSourceRows(t, expected)
	directory := t.TempDir()
	p31Beads(t, directory, nil, "init", "--prefix", project, "--non-interactive", "--quiet")
	p31Beads(t, directory, source, "import", "-")
	var issues []map[string]any
	if err := json.Unmarshal(p31Beads(t, directory, nil, "list", "--all", "--json"), &issues); err != nil {
		t.Fatal(err)
	}
	if len(issues) != len(rows) {
		t.Fatalf("beads list count=%d source=%d", len(issues), len(rows))
	}
	for _, got := range issues {
		id, _ := got["id"].(string)
		sourceRow := rows[id]
		if sourceRow == nil {
			t.Fatalf("beads list returned unknown source id=%s", id)
		}
		listComparable := func(value map[string]any) map[string]any {
			copy := map[string]any{}
			for key, item := range value {
				if key != "revision" {
					copy[key] = item
				}
			}
			return copy
		}
		if !reflect.DeepEqual(p31BeadsCanonical(listComparable(got)), p31BeadsCanonical(p31BeadsListExpected(sourceRow, rows))) {
			gb, _ := json.Marshal(got)
			wb, _ := json.Marshal(p31BeadsListExpected(sourceRow, rows))
			t.Fatalf("beads list readback differs id=%s got=%s want=%s", id, gb, wb)
		}
		var shown any
		if err := json.Unmarshal(p31Beads(t, directory, nil, "show", id, "--json", "--include-comments"), &shown); err != nil {
			t.Fatal(err)
		}
		shownMap, _ := shown.(map[string]any)
		if shownMap == nil {
			if list, ok := shown.([]any); ok && len(list) == 1 {
				shownMap, _ = list[0].(map[string]any)
			}
		}
		if !reflect.DeepEqual(p31BeadsCanonical(listComparable(shownMap)), p31BeadsCanonical(p31BeadsShowExpected(sourceRow, rows))) {
			gb, _ := json.Marshal(shownMap)
			wb, _ := json.Marshal(p31BeadsShowExpected(sourceRow, rows))
			t.Fatalf("beads show readback differs id=%s got=%s want=%s", id, gb, wb)
		}
		var deps any
		if err := json.Unmarshal(p31Beads(t, directory, nil, "dep", "list", id, "--json"), &deps); err != nil {
			t.Fatal(err)
		}
		wantDeps := sourceRow["dependencies"]
		if wantDeps == nil {
			wantDeps = []any{}
		}
		gotDeps, _ := deps.([]any)
		wantDepList, _ := wantDeps.([]any)
		if len(gotDeps) != len(wantDepList) {
			t.Fatalf("beads dependency readback differs id=%s got=%v want=%v", id, deps, wantDeps)
		}
		for _, raw := range gotDeps {
			dep, _ := raw.(map[string]any)
			matched := false
			for _, expected := range wantDepList {
				e, _ := expected.(map[string]any)
				depID, _ := e["depends_on_id"].(string)
				if dep["id"] == depID && dep["dependency_type"] == e["type"] && reflect.DeepEqual(p31BeadsIssueCore(depWithoutDependencyType(dep)), p31BeadsIssueCore(rows[depID])) {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("beads dependency readback differs id=%s got=%v want=%v", id, deps, wantDeps)
			}
		}
	}
}

func depWithoutDependencyType(value map[string]any) map[string]any {
	copy := map[string]any{}
	for key, item := range value {
		if key != "dependency_type" {
			copy[key] = item
		}
	}
	return copy
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
			ledger.assertTraversals(t, 0, 1, true)
			report, err := RestoreNativeCaptureCollectionImportWithLease(op, ledger.binding, ledger.sources(), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			report.Close()
			ledger.assertTraversals(t, 0, 2, true)
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
					p31BeadsSourceReadback(t, project, actual[project], expected)
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
			tampered := tamperNativeEvidenceRootForTest(t, nativeBytes)
			tamperedArchive, err := dumpformat.Read(bytes.NewReader(tampered))
			if err != nil {
				t.Fatal(err)
			}
			badTarget, badLease, _ := openIdentityTarget(t, engine)
			restoreNativeHeadForTest(t, ctx, badTarget, badLease, tampered, tamperedArchive.Manifest())
			_, err = ReverseRestoredWithOptions(ctx, badTarget, reverseconverter.Options{All: true})
			var refusal ReverseRefusal
			if !errors.As(err, &refusal) || refusal.Reason != "source_receipt_archive_or_origin_mismatch" {
				t.Fatalf("tampered native evidence was accepted: %v", err)
			}
			_ = reopen
		})
	}
}

func TestStreamedJoinedReversePreservesProfilesAndOriginLoss(t *testing.T) {
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), dumpformat.NewNativeDeltaBudget(0))
	ledger := newJoinedOriginalLedger(t)
	source, initial, _ := openIdentityTarget(t, "sqlite")
	if err := initial.Release(ctx); err != nil {
		t.Fatal(err)
	}
	op, lease, release, err := HoldRestoreLease(ctx, source, "reverse-stream-projection")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	profile := joinedPhysicalProfile(t, ctx, ledger, source, lease)
	report, err := RestoreNativeCaptureCollectionImportWithLease(op, ledger.binding, ledger.sources(), source, lease, profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := report.Close(); err != nil {
		t.Fatal(err)
	}
	options := reverseconverter.Options{All: true, BeadsDestination: true}
	direct, err := ReverseRestoredWithOptions(ctx, source, options)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	manifest, err := restore.WriteReverseHead(ctx, source, &out)
	if err != nil {
		t.Fatal(err)
	}
	projected := manifest.NativeReverseProjection
	if projected == nil || projected.ProfileCopies != 2 || projected.DiscardedOrigins == nil ||
		projected.DiscardedOrigins.Records == 0 || projected.DiscardedOrigins.Fields < projected.DiscardedOrigins.Records {
		t.Fatalf("missing profile and origin projection: %+v", projected)
	}
	if _, err := dumpformat.Read(bytes.NewReader(out.Bytes())); err == nil {
		t.Fatal("ordinary restore admitted the projected trial archive")
	}
	target, targetLease, reopen := openIdentityTarget(t, "sqlite")
	presence := restore.Presence{"mode": "presence_check", "challenge": "stream-projection-test", "proof": "stream-projection-test"}
	manager := restore.NewManager(target, targetLease, restore.PresenceVerifierFunc(func(context.Context, string, string, restore.Presence) error { return nil }))
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.RestoreReverseHead(ctx, bytes.NewReader(out.Bytes()), int64(out.Len()), hashBytes(manifestRaw), hashBytes(out.Bytes()),
		model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, model.WriteMeta{}, presence); err != nil {
		t.Fatal(err)
	}
	target, _ = reopen()
	restored, err := ReverseRestoredWithOptions(ctx, target, options)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.Result.JSONL, direct.Result.JSONL) {
		t.Fatal("streamed native restore changed reverse JSONL")
	}
	losses := func(report reverseconverter.Report) map[string]reverseconverter.StateLoss {
		result := map[string]reverseconverter.StateLoss{}
		for _, loss := range report.CoreKindLosses {
			if loss.Kind == "beads-archive" || loss.Kind == "instance_config" {
				result[loss.Kind] = loss
			}
		}
		return result
	}
	if a, b := losses(direct.Result.Report), losses(restored.Result.Report); len(a) != 2 || !reflect.DeepEqual(a, b) {
		t.Fatalf("streamed native restore changed core losses: direct=%+v restored=%+v", a, b)
	}
}
