package compaction_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// B12 card pairing. Every vector recorded from the pinned native Beads build
// (//spec/cardb12:native_compaction_record_test) is paired here with Plug's
// public behavior on memory, SQLite and PostgreSQL. Each pair is labeled SAME
// or ADOPTED DEVIATION; deviations name the binding ruling.
//
// Native AI provider stubbed via public ai.base_url; summary content not under
// test; Beads source unmodified (pinned 9d6fff76 source build).
const b12NativeVectors = "../../cardb12/testdata/b12-native-vectors.json"

// Literal supplied summaries, identical to the native recorder's files/stub.
const (
	b12PairTierOne    = "Tier one supplied summary."
	b12PairTierTwo    = "T2 summary."
	b12PairStub       = "Stub summary: closed public fixture."
	b12PairLongMarker = "b12-autolong"
)

var b12PairNotSmaller = strings.Repeat("Supplied summary that is not smaller than the original. ", 20)
var b12PairLongStub = strings.Repeat("Deliberately longer stub summary. ", 80)

type b12NativeIssue struct {
	ID                 string `json:"id"`
	Status             string `json:"status"`
	Description        string `json:"description"`
	Design             string `json:"design"`
	Notes              string `json:"notes"`
	AcceptanceCriteria string `json:"acceptance_criteria"`
	CompactionLevel    string `json:"compaction_level"`
	Compacted          bool   `json:"compacted_at_present"`
	OriginalSize       string `json:"original_size"`
}

type b12NativeVector struct {
	Name           string           `json:"name"`
	Exit           int              `json:"exit"`
	Stderr         string           `json:"stderr"`
	Before         []b12NativeIssue `json:"before"`
	After          []b12NativeIssue `json:"after"`
	SnapshotsAfter []struct {
		IssueID         string         `json:"issue_id"`
		CompactionLevel string         `json:"compaction_level"`
		Content         map[string]any `json:"snapshot"`
	} `json:"snapshots_after"`
	CommentsAdded []struct {
		IssueID string `json:"issue_id"`
		Text    string `json:"comment"`
	} `json:"comments_added"`
	StubRequests     []json.RawMessage `json:"stub_requests"`
	FullImageChanged bool              `json:"full_image_changed"`
}

func loadB12NativeVectors(t *testing.T) map[string]b12NativeVector {
	t.Helper()
	raw, err := os.ReadFile(b12NativeVectors)
	if err != nil {
		t.Fatal(err)
	}
	var vectors []b12NativeVector
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&vectors); err != nil {
		t.Fatal(err)
	}
	out := map[string]b12NativeVector{}
	for _, v := range vectors {
		out[v.Name] = v
	}
	if len(out) != len(vectors) || len(out) == 0 {
		t.Fatalf("native vectors absent or duplicated: %d", len(vectors))
	}
	return out
}

// b12Pair builds a fresh Plug store holding the native fixture record(s),
// closed at the fixture clock, with every native text field and a zero level.
type b12Pair struct {
	*fixture
	t         *testing.T
	authority *presenceAuthority
	summaries atomic.Int32
	summary   func(service.CompactionInput) string
	closedAt  time.Time
}

func newB12Pair(t *testing.T, engine string, summary func(service.CompactionInput) string) *b12Pair {
	p := &b12Pair{t: t, authority: &presenceAuthority{}, summary: summary}
	config := service.CompactionConfig{VerifyPresence: p.authority.verify}
	if summary != nil {
		config.Summary = service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
			p.summaries.Add(1)
			return p.summary(in), nil
		})
	}
	p.fixture = newFixture(t, engine, service.WithCompaction(config))
	p.extendedOntology = true
	p.bind()
	return p
}

func (p *b12Pair) record(key string, native b12NativeIssue, closed bool) map[string]any {
	p.t.Helper()
	fields := map[string]any{"title": "B12 native pairing " + native.ID, "description": native.Description, "design": native.Design, "notes": native.Notes, "acceptance_criteria": native.AcceptanceCriteria, "compaction_level": 0}
	made := p.call("bead.create", key+"-create", map[string]any{"type": "issue", "fields": fields}, "")
	ref := made["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
	if closed {
		p.call("bead.close", key+"-close", map[string]any{"id": ref}, "")
		p.closedAt = p.clock
	}
	return ref
}

func (p *b12Pair) age(d time.Duration) { p.clock = p.closedAt.Add(d) }

func (p *b12Pair) fields(ref map[string]any) map[string]any {
	p.t.Helper()
	return p.call("bead.get", fmt.Sprintf("get-%s-%d", ref["id"], len(p.trace)), map[string]any{"id": ref}, "")["bead"].(map[string]any)["fields"].(map[string]any)
}

// assertText compares Plug's four text fields and level to a native issue.
func (p *b12Pair) assertText(ref map[string]any, want b12NativeIssue) {
	p.t.Helper()
	got := p.fields(ref)
	for name, value := range map[string]string{"description": want.Description, "design": want.Design, "notes": want.Notes, "acceptance_criteria": want.AcceptanceCriteria} {
		if got[name] != value {
			p.t.Fatalf("Plug %s=%q native=%q", name, got[name], value)
		}
	}
	if level := got["compaction_level"]; level == nil || level.(json.Number).String() != want.CompactionLevel {
		p.t.Fatalf("Plug compaction_level=%v native=%s", level, want.CompactionLevel)
	}
}

func (p *b12Pair) refuse(key string) {
	p.t.Helper()
	before := p.meta()
	p.call("admin.compact", key, map[string]any{"dryRun": false}, "compaction_unavailable")
	p.assertSameHead(before)
	p.assertNoReceipt("admin.compact", key)
}

func (p *b12Pair) compact(key string, ref map[string]any) map[string]any {
	p.t.Helper()
	commit := p.call("admin.compact", key, map[string]any{"dryRun": false}, "")
	assertCommitTarget(p.t, commit, ref)
	assertKind(p.t, p.fixture, commit["revision"], model.ChangesetWrite)
	return commit
}

func (p *b12Pair) revert(key string, ref map[string]any, base any) map[string]any {
	p.t.Helper()
	params := map[string]any{"ref": ref, "baseRevision": base, "expect": []any{}}
	params["humanPresence"] = p.authority.issue(p.fixture, params, key)
	commit := p.call("history.revert", key, params, "")
	assertKind(p.t, p.fixture, commit["revision"], model.ChangesetRevert)
	return commit
}

func nativeRefusedUnchanged(t *testing.T, v b12NativeVector) {
	t.Helper()
	if v.Exit == 0 || v.FullImageChanged || !reflect.DeepEqual(v.Before, v.After) {
		t.Fatalf("native %s is not a no-write refusal: exit=%d changed=%t", v.Name, v.Exit, v.FullImageChanged)
	}
}

func nativeSucceeded(t *testing.T, v b12NativeVector, level string) b12NativeIssue {
	t.Helper()
	if v.Exit != 0 || !v.FullImageChanged || len(v.After) == 0 || v.After[0].CompactionLevel != level {
		t.Fatalf("native %s did not compact to level %s: exit=%d after=%+v", v.Name, level, v.Exit, v.After)
	}
	return v.After[0]
}

const (
	b12Same      = "SAME"
	b12Deviation = "ADOPTED DEVIATION"
	b12KnownGap  = "KNOWN GAP (head-of-line), pending decision"
)

type b12PairCase struct {
	name, label, reason string
	run                 func(t *testing.T, engine string, v b12NativeVector, all map[string]b12NativeVector)
}

func b12PairCases() []b12PairCase {
	tierOne := func(in service.CompactionInput) string {
		if in.Tier == 2 {
			return b12PairTierTwo
		}
		return b12PairTierOne
	}
	return []b12PairCase{
		{"apply-open-refused", b12Same, "an open record is ineligible and nothing is written", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("open", v.Before[0], false)
			p.clock = p.clock.Add(2000 * 24 * time.Hour)
			p.refuse("open-refused")
			p.assertText(ref, v.Before[0])
		}},
		{"apply-recent-refused", b12Same, "closed under 30 days is ineligible; exact 30/90-day boundaries use Plug's controlled clock (TestB12PublicTierBoundariesHistoryReplayReversalAndReopen)", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("recent", v.Before[0], true)
			p.age(time.Hour)
			p.refuse("recent-refused")
			p.assertText(ref, v.Before[0])
		}},
		{"apply-summary-not-smaller-refused", b12Same, "a summary that is not smaller is refused with the original intact", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			p := newB12Pair(t, engine, func(service.CompactionInput) string { return b12PairNotSmaller })
			ref := p.record("not-smaller", v.Before[0], true)
			p.age(2000 * 24 * time.Hour)
			p.refuse("not-smaller-refused")
			p.assertText(ref, v.Before[0])
		}},
		{"apply-tier-one", b12Deviation, "snapshot before overwrite: Plug keeps the original as a permanent history version (P45/P73); native manual --apply writes no snapshot row and appends an audit comment", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			after := nativeSucceeded(t, v, "1")
			if len(v.SnapshotsAfter) != 0 || len(v.CommentsAdded) != 1 {
				t.Fatalf("native manual apply snapshot/comment changed: %+v %+v", v.SnapshotsAfter, v.CommentsAdded)
			}
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("tier-one", v.Before[0], true)
			closed := p.meta().Head
			p.age(31 * 24 * time.Hour)
			p.compact("tier-one", ref)
			p.assertText(ref, after)
			assertHistoricalText(t, p.fixture, ref, closed, v.Before[0].Description)
		}},
		{"apply-tier-one-repeat-refused", b12Same, "an already tier-one record is not compacted again at tier one (Plug derives the tier from the level; shown inside the 30-90 day window)", func(t *testing.T, engine string, v b12NativeVector, all map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("repeat", all["apply-tier-one"].Before[0], true)
			p.age(31 * 24 * time.Hour)
			p.compact("repeat-first", ref)
			p.refuse("repeat-refused")
			p.assertText(ref, v.After[0])
		}},
		{"restore-after-apply-no-snapshot", b12Deviation, "native cannot restore a manual --apply (no snapshot); Plug restores from permanent history through D32-checked history.revert", func(t *testing.T, engine string, v b12NativeVector, all map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			p := newB12Pair(t, engine, tierOne)
			original := all["apply-tier-one"].Before[0]
			ref := p.record("restore-manual", original, true)
			p.age(31 * 24 * time.Hour)
			commit := p.compact("restore-manual-compact", ref)
			p.revert("restore-manual-revert", ref, commit["revision"])
			p.assertText(ref, original)
		}},
		{"apply-tier-two-level-zero-refused", b12Deviation, "the pinned native CLI refuses every --tier 2 request as not implemented; Plug has no caller-selected tier (tier = level + 1), so an old level-zero record is compacted at tier one", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			b12AssertNativeTierTwoUnimplemented(t, v)
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("tier-order", v.Before[0], true)
			p.age(2000 * 24 * time.Hour)
			p.compact("tier-order", ref)
			want := v.Before[0]
			want.Description, want.Design, want.Notes, want.AcceptanceCriteria, want.CompactionLevel = b12PairTierOne, "", "", "", "1"
			p.assertText(ref, want)
		}},
		{"apply-tier-two-level-one-refused", b12Deviation, "the pinned native CLI refuses tier two as not implemented, although its storage eligibility (CheckEligibilityInTx) defines it; Plug implements the card's tier two: a level-one record closed 90+ days is compacted again with a smaller summary", func(t *testing.T, engine string, v b12NativeVector, all map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			b12AssertNativeTierTwoUnimplemented(t, v)
			if v.Before[0].CompactionLevel != "1" {
				t.Fatalf("native tier-two vector did not start at level one: %+v", v.Before)
			}
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("tier-two", all["apply-tier-one"].Before[0], true)
			p.age(31 * 24 * time.Hour)
			first := p.compact("tier-two-first", ref)
			p.age(90*24*time.Hour - time.Nanosecond)
			p.refuse("tier-two-early")
			p.age(90 * 24 * time.Hour)
			p.compact("tier-two", ref)
			want := v.Before[0]
			want.Description, want.CompactionLevel = b12PairTierTwo, "2"
			p.assertText(ref, want)
			assertHistoricalText(t, p.fixture, ref, first["revision"], b12PairTierOne)
		}},
		{"apply-dry-run", b12Deviation, "dry-run never writes in Plug (05:25Z ruling); the native --apply --dry-run vector is recorded as observed", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			b12AssertNativeDryRunApply(t, v)
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("dry", v.Before[0], true)
			p.age(31 * 24 * time.Hour)
			before := p.meta()
			dry := p.call("admin.compact", "dry", map[string]any{"dryRun": true}, "")
			assertCommitTarget(t, dry, ref)
			if dry["wouldCommit"] != true || p.summaries.Load() != 0 {
				t.Fatalf("Plug dry run wrote or summarized: %v calls=%d", dry, p.summaries.Load())
			}
			p.assertSameHead(before)
			p.assertNoReceipt("admin.compact", "dry")
			p.assertText(ref, v.Before[0])
		}},
		{"auto-missing-key-refused", b12Same, "no summary source means no compaction and no write (Plug core has no outbound key path, CP6)", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			nativeRefusedUnchanged(t, v)
			if len(v.StubRequests) != 0 {
				t.Fatal("native missing-key vector reached the provider")
			}
			p := newB12Pair(t, engine, nil)
			ref := p.record("no-provider", v.Before[0], true)
			p.age(31 * 24 * time.Hour)
			p.refuse("no-provider")
			p.assertText(ref, v.Before[0])
		}},
		{"auto-dry-run", b12Same, "dry-run names the candidate without calling the provider or writing", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			if v.Exit != 0 || v.FullImageChanged || len(v.StubRequests) != 0 || !reflect.DeepEqual(v.Before, v.After) {
				t.Fatalf("native auto dry-run wrote or called the provider: %+v", v)
			}
			p := newB12Pair(t, engine, tierOne)
			ref := p.record("auto-dry", v.Before[0], true)
			p.age(31 * 24 * time.Hour)
			before := p.meta()
			dry := p.call("admin.compact", "auto-dry", map[string]any{"dryRun": true}, "")
			assertCommitTarget(t, dry, ref)
			if p.summaries.Load() != 0 {
				t.Fatal("Plug dry run called the summary provider")
			}
			p.assertSameHead(before)
		}},
		{"auto-tier-one", b12Same, "the provider is called once and its smaller summary is applied; the original is retained (native snapshot row, Plug permanent history version)", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			after := nativeSucceeded(t, v, "1")
			if len(v.StubRequests) != 1 || len(v.SnapshotsAfter) != 1 || v.SnapshotsAfter[0].Content["description"] != v.Before[0].Description {
				t.Fatalf("native auto compaction did not snapshot the original once: %+v", v.SnapshotsAfter)
			}
			p := newB12Pair(t, engine, func(service.CompactionInput) string { return b12PairStub })
			ref := p.record("auto", v.Before[0], true)
			closed := p.meta().Head
			p.age(31 * 24 * time.Hour)
			p.compact("auto", ref)
			if p.summaries.Load() != 1 {
				t.Fatalf("Plug provider calls=%d", p.summaries.Load())
			}
			p.assertText(ref, after)
			assertHistoricalText(t, p.fixture, ref, closed, v.Before[0].Description)
		}},
		{"auto-summary-not-smaller-refused", b12Deviation, "both keep the original text; native also appends a 'compaction skipped' warning comment, while a Plug refusal writes nothing", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			if v.Exit == 0 || !reflect.DeepEqual(v.Before, v.After) || len(v.StubRequests) != 1 || len(v.SnapshotsAfter) != 0 || len(v.CommentsAdded) != 1 || !strings.Contains(v.CommentsAdded[0].Text, "not shorter") {
				t.Fatalf("native auto not-smaller vector changed: %+v", v)
			}
			p := newB12Pair(t, engine, func(service.CompactionInput) string { return b12PairLongStub })
			ref := p.record("auto-long", v.Before[0], true)
			p.age(31 * 24 * time.Hour)
			p.refuse("auto-long")
			p.assertText(ref, v.Before[0])
		}},
		{"restore-preview", b12Same, "the pre-compaction text is readable without writing (native restore preview, Plug history.get)", func(t *testing.T, engine string, v b12NativeVector, all map[string]b12NativeVector) {
			if v.Exit != 0 || v.FullImageChanged {
				t.Fatalf("native restore preview wrote: %+v", v)
			}
			original := all["auto-tier-one"].Before[0]
			p := newB12Pair(t, engine, func(service.CompactionInput) string { return b12PairStub })
			ref := p.record("preview", original, true)
			closed := p.meta().Head
			p.age(31 * 24 * time.Hour)
			p.compact("preview-compact", ref)
			before := p.meta()
			assertHistoricalText(t, p.fixture, ref, closed, original.Description)
			p.assertSameHead(before)
		}},
		{"restore-apply-snapshot", b12Same, "restore returns the original text and level zero while retaining the audit (native snapshot row kept; Plug versions kept). Plug additionally requires D32 human presence", func(t *testing.T, engine string, v b12NativeVector, all map[string]b12NativeVector) {
			if v.Exit != 0 || len(v.After) != 1 || v.After[0].CompactionLevel != "0" || v.After[0].Description != all["auto-tier-one"].Before[0].Description || len(v.SnapshotsAfter) != 1 {
				t.Fatalf("native snapshot restore changed: %+v", v)
			}
			p := newB12Pair(t, engine, func(service.CompactionInput) string { return b12PairStub })
			ref := p.record("restore", all["auto-tier-one"].Before[0], true)
			p.age(31 * 24 * time.Hour)
			commit := p.compact("restore-compact", ref)
			versions := len(p.versions(ref))
			p.revert("restore-revert", ref, commit["revision"])
			p.assertText(ref, v.After[0])
			if len(p.versions(ref)) != versions+1 {
				t.Fatal("Plug restore removed audit history")
			}
		}},
		{"auto-batch-mixed", b12KnownGap, "Option A (binding 2026-11-02) compacts one deterministic record per admin.compact call and a refusal publishes nothing; a record whose summary is never smaller stays first in line, so later records are not reached, while the native sweep continues past it. Root took the public API shape to Nikhil on 2026-11-23", func(t *testing.T, engine string, v b12NativeVector, _ map[string]b12NativeVector) {
			b12AssertNativeBatch(t, v)
			b12PlugRepeatedCalls(t, engine, v)
		}},
	}
}

func TestB12NativeVectorPairing(t *testing.T) {
	vectors := loadB12NativeVectors(t)
	cases := b12PairCases()
	if len(cases) != len(vectors) {
		t.Fatalf("pairing covers %d of %d native vectors", len(cases), len(vectors))
	}
	for _, c := range cases {
		v, ok := vectors[c.name]
		if !ok {
			t.Fatalf("no native vector %s", c.name)
		}
		t.Run(c.name, func(t *testing.T) {
			t.Logf("B12_PAIR %s: %s — %s", c.name, c.label, c.reason)
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) { c.run(t, engine, v, vectors) })
			}
		})
	}
}

func b12AssertNativeTierTwoUnimplemented(t *testing.T, v b12NativeVector) {
	t.Helper()
	if !strings.Contains(v.Stderr, "Tier 2 compaction is not yet implemented") {
		t.Fatalf("native tier-two refusal changed: %q", v.Stderr)
	}
}

// b12AssertNativeDryRunApply pins the native `--apply --dry-run` vector as
// observed. It is a native vector, not a Beads defect to repair here.
func b12AssertNativeDryRunApply(t *testing.T, v b12NativeVector) {
	t.Helper()
	if v.Exit != 0 || !v.FullImageChanged || len(v.After) != 1 || v.After[0].CompactionLevel != "1" || v.After[0].Description != b12PairTierOne {
		t.Fatalf("native --apply --dry-run vector changed: %+v", v)
	}
}

// b12AssertNativeBatch pins the native single-call sweep: every tier-one
// candidate is attempted in one command, and a not-smaller summary for one
// record does not stop the others.
func b12AssertNativeBatch(t *testing.T, v b12NativeVector) {
	t.Helper()
	if v.Exit != 0 || !v.FullImageChanged {
		t.Fatalf("native batch did not complete: %+v", v)
	}
	before := map[string]b12NativeIssue{}
	for _, issue := range v.Before {
		before[issue.ID] = issue
	}
	for _, issue := range v.After {
		was := before[issue.ID]
		switch {
		case was.CompactionLevel != "0":
			if !reflect.DeepEqual(was, issue) {
				t.Fatalf("native batch touched a non-candidate %s", issue.ID)
			}
		case strings.Contains(was.Description, b12PairLongMarker):
			if !reflect.DeepEqual(was, issue) {
				t.Fatalf("native batch overwrote the not-smaller record %s", issue.ID)
			}
		default:
			if issue.CompactionLevel != "1" || issue.Description != b12PairStub {
				t.Fatalf("native batch skipped eligible record %s: %+v", issue.ID, issue)
			}
		}
	}
}

// b12PlugRepeatedCalls runs the same candidate set (native level-zero records)
// through repeated Plug calls. Selection is closed time, then ID; a record
// whose summary is never smaller stays first in line, so later records are
// not reached by further calls. Each refusal publishes nothing.
func b12PlugRepeatedCalls(t *testing.T, engine string, v b12NativeVector) {
	t.Helper()
	p := newB12Pair(t, engine, func(in service.CompactionInput) string {
		if strings.Contains(in.Text["description"], b12PairLongMarker) {
			return b12PairLongStub
		}
		return b12PairStub
	})
	refs := map[string]map[string]any{}
	order := []string{}
	for _, issue := range v.Before {
		if issue.CompactionLevel != "0" {
			continue
		}
		refs[issue.ID] = p.record("batch-"+issue.ID, issue, true)
		order = append(order, issue.ID)
	}
	if len(order) < 3 || order[1] != b12PairLongMarker {
		t.Fatalf("batch fixture order changed: %v", order)
	}
	p.clock = p.closedAt.Add(31 * 24 * time.Hour)
	p.compact("batch-call-1", refs[order[0]])
	for i := 2; i <= len(order); i++ {
		p.refuse(fmt.Sprintf("batch-call-%d", i))
	}
	for _, id := range order[1:] {
		for _, issue := range v.Before {
			if issue.ID == id {
				p.assertText(refs[id], issue)
			}
		}
	}
	nativeCompacted := 0
	for i, issue := range v.After {
		if v.Before[i].CompactionLevel == "0" && issue.CompactionLevel == "1" {
			nativeCompacted++
		}
	}
	t.Logf("B12_PAIR_BATCH engine=%s candidates=%d native_compacted=%d plug_compacted=1 plug_refused=%d head_of_line=%s", engine, len(order), nativeCompacted, len(order)-1, order[1])
}
