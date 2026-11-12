package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// This is the predecessor hash preimage: normalize history mode, perform the
// complete JSON/UseNumber round trip, then marshal once more. It deliberately
// does not call RequestHash or the new streaming encoder.
func predecessorImportHashJSON(t *testing.T, p RestoreImportProfile) []byte {
	t.Helper()
	mode, err := ParseRestoreHistoryMode(string(p.HistoryMode))
	if err != nil {
		t.Fatal(err)
	}
	p.HistoryMode = mode
	first, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var detached RestoreImportProfile
	d := json.NewDecoder(bytes.NewReader(first))
	d.UseNumber()
	if err := d.Decode(&detached); err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(detached)
	if err != nil {
		t.Fatal(err)
	}
	return want
}

func TestRestoreImportProfileStreamPreservesPredecessorHashBytes(t *testing.T) {
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func(*RestoreImportProfile)
	}{
		{"legacy-full-default", func(p *RestoreImportProfile) {}},
		{"joint-full-default", func(p *RestoreImportProfile) { p.SourcePlanes = &binding }},
		{"snapshot-alias", func(p *RestoreImportProfile) { p.HistoryMode = RestoreHistoryHead }},
		{"partial", func(p *RestoreImportProfile) { p.HistoryMode = RestoreHistoryModePartial }},
		{"native-partial-floor", func(p *RestoreImportProfile) {
			p.SourcePlanes = &binding
			p.HistoryMode = RestoreHistoryModePartial
			p.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat, FirstSequence: 2}
		}},
		{"native-snapshot", func(p *RestoreImportProfile) {
			p.SourcePlanes = &binding
			p.HistoryMode = RestoreHistoryModeSnapshot
			p.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat}
		}},
		{"history-output", func(p *RestoreImportProfile) {
			p.SourcePlanes = &binding
			p.HistoryDigest = strings.Repeat("d", 64)
			p.HistoryTime = time.Date(2100, 2, 3, 4, 5, 6, 123456789, time.UTC).Format(time.RFC3339Nano)
		}},
		{"exact-caveats", func(p *RestoreImportProfile) {
			p.SourcePlanes = &binding
			p.Admission.Actor.Sponsor = &model.PrincipalRef{Kind: "user", ID: "sponsor"}
			p.Admission.Actor.Caveats = map[string]any{
				"raw":            json.RawMessage(` { "z": 9007199254740993, "a": [1e+100, null, "<&>\u2028"] } `),
				"number":         json.Number("9007199254740993"),
				"bytes":          []byte{0, 1, 2, 253, 254, 255},
				"ordered-fields": struct{ Z, A string }{"last", "first"},
			}
		}},
		{"anonymous-scope", func(p *RestoreImportProfile) {
			p.Admission.Mode = "anonymous-local"
			p.Admission.Authority = "trusted-human-presence"
			p.Admission.Actor = model.PrincipalRef{Kind: "anonymous", ID: "local"}
			p.Admission.Scope = "local-agent"
		}},
		{"legacy-invalid-utf8-normalization", func(p *RestoreImportProfile) {
			p.IdempotencyKey += string([]byte{0xff})
			p.SourceStoreID = StoreID("source" + string([]byte{0xff}))
			p.Admission.Actor.ID += string([]byte{0xfe})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := sourcePlaneTestProfile()
			test.edit(&p)
			want := predecessorImportHashJSON(t, p)
			detached, err := p.Detached()
			if err != nil {
				t.Fatal(err)
			}
			var got bytes.Buffer
			if err := writeRestoreImportProfileJSON(&got, detached, RestoreImportProfileAccounting{}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("stream changed predecessor JSON\nwant %s\n got %s", want, got.Bytes())
			}
			sum := sha256.Sum256(want)
			hash, err := p.RequestHash()
			if err != nil || hash != hex.EncodeToString(sum[:]) {
				t.Fatalf("stream changed predecessor request hash: %s %v", hash, err)
			}
		})
	}
}

func TestRestoreImportProfileStreamPreservesValidationRefusals(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*RestoreImportProfile)
	}{
		{"source", func(p *RestoreImportProfile) { p.SourceStoreID = "other" }},
		{"archive", func(p *RestoreImportProfile) { p.ArchiveDigest = strings.Repeat("d", 64) }},
		{"binding-version", func(p *RestoreImportProfile) { p.SourcePlanes.Version = "future" }},
		{"binding-digest", func(p *RestoreImportProfile) { p.SourcePlanes.JointPlanDigest = strings.Repeat("d", 64) }},
		{"canonical-trailing-bytes", func(p *RestoreImportProfile) {
			p.SourcePlanes.CanonicalPlan = append(p.SourcePlanes.CanonicalPlan, 0)
			p.SourcePlanes.JointPlanDigest = sourcePlanePlanDigest(p.SourcePlanes.CanonicalPlan)
		}},
		{"history-mode", func(p *RestoreImportProfile) { p.HistoryMode = "future" }},
		{"native-full-policy", func(p *RestoreImportProfile) {
			p.HistoryMode = RestoreHistoryModeFull
			p.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat}
		}},
		{"native-partial-zero-floor", func(p *RestoreImportProfile) {
			p.HistoryMode = RestoreHistoryModePartial
			p.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat}
		}},
		{"native-policy-without-plan", func(p *RestoreImportProfile) {
			p.SourcePlanes = nil
			p.HistoryMode = RestoreHistoryModeSnapshot
			p.NativeDeltaHistory = &NativeDeltaHistoryPolicy{Format: NativeDeltaHistoryPolicyFormat}
		}},
		{"history-digest", func(p *RestoreImportProfile) { p.HistoryDigest = "invalid" }},
		{"history-time", func(p *RestoreImportProfile) { p.HistoryTime = "2100-01-01T00:00:00+00:00" }},
		{"authority", func(p *RestoreImportProfile) { p.Admission.Authority = "other" }},
		{"authenticated-scope", func(p *RestoreImportProfile) { p.Admission.Scope = "other" }},
		{"actor", func(p *RestoreImportProfile) { p.Admission.Actor.Kind = "future" }},
		{"invalid-number", func(p *RestoreImportProfile) {
			p.Admission.Actor.Caveats = map[string]any{"number": json.Number("not-a-number")}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
			if err != nil {
				t.Fatal(err)
			}
			p := sourcePlaneTestProfile()
			p.SourcePlanes = &binding
			test.edit(&p)
			_, want := p.Detached()
			got, err := p.RequestHash()
			if want == nil || err == nil || got != "" || err.Error() != want.Error() || errors.Is(err, ErrUnsupported) != errors.Is(want, ErrUnsupported) {
				t.Fatalf("validation changed: hash=%s got=%v want=%v", got, err, want)
			}
		})
	}
}

func TestRestoreImportProfileStreamPreservesBase64Tails(t *testing.T) {
	seen := map[int]bool{}
	for extra := 0; extra < 3; extra++ {
		plan := sourcePlaneTestPlan()
		plan.Principals[0].Source += strings.Repeat("x", extra)
		binding, err := NewSourcePlaneImportBindingV1(plan)
		if err != nil {
			t.Fatal(err)
		}
		seen[len(binding.CanonicalPlan)%3] = true
		p := sourcePlaneTestProfile()
		p.SourcePlanes = &binding
		detached, err := p.Detached()
		if err != nil {
			t.Fatal(err)
		}
		var got bytes.Buffer
		if err := writeRestoreImportProfileJSON(&got, detached, RestoreImportProfileAccounting{}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Bytes(), predecessorImportHashJSON(t, p)) {
			t.Fatalf("base64 tail changed for canonical length %d", len(binding.CanonicalPlan))
		}
	}
	if len(seen) != 3 {
		t.Fatal("did not exercise all three base64 tails", seen)
	}
}

type restoreHashSink struct {
	bytes, largest int
	hash           io.Writer
}

func (w *restoreHashSink) Write(p []byte) (int, error) {
	w.bytes += len(p)
	if len(p) > w.largest {
		w.largest = len(p)
	}
	return w.hash.Write(p)
}

func TestRestoreImportProfileStreamHasBoundedHashWorkingSet(t *testing.T) {
	plan := sourcePlaneTestPlan()
	plan.Principals[0].Source = strings.Repeat("x", 1<<20)
	binding, err := NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	p := sourcePlaneTestProfile()
	p.SourcePlanes = &binding
	detached, err := p.Detached()
	if err != nil {
		t.Fatal(err)
	}
	want := predecessorImportHashJSON(t, p)
	wantSHA := sha256.Sum256(want)
	h := sha256.New()
	sink := &restoreHashSink{hash: h}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = writeRestoreImportProfileJSON(sink, detached, RestoreImportProfileAccounting{})
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(detached)
	if err != nil || !bytes.Equal(h.Sum(nil), wantSHA[:]) || sink.bytes != len(want) {
		t.Fatalf("large profile bytes/hash differ: %d/%d %v", sink.bytes, len(want), err)
	}
	// The admitted plan already exists. Hash output scratch must stay bounded
	// independently of its size; this margin includes metadata and SHA/base64
	// state. A materialized 1.4 MiB JSON hash preimage fails both assertions.
	if sink.largest > 32<<10 || after.TotalAlloc-before.TotalAlloc > 256<<10 {
		t.Fatalf("hash buffered canonical plan: largest_write=%d allocation=%d", sink.largest, after.TotalAlloc-before.TotalAlloc)
	}
	t.Logf("STREAM_PROFILE_HASH plan_bytes=%d wire_bytes=%d largest_write=%d temporary_allocated_bytes=%d margin_bytes=%d", len(binding.CanonicalPlan), sink.bytes, sink.largest, after.TotalAlloc-before.TotalAlloc, 256<<10)
}

type restoreHashFailingWriter struct{ remaining int }

var errRestoreHashWrite = errors.New("profile hash sink failed")

func (w *restoreHashFailingWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, errRestoreHashWrite
	}
	w.remaining -= len(p)
	return len(p), nil
}

func TestRestoreImportProfileStreamReturnsWriteErrors(t *testing.T) {
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	p := sourcePlaneTestProfile()
	p.SourcePlanes = &binding
	detached, err := p.Detached()
	if err != nil {
		t.Fatal(err)
	}
	want := predecessorImportHashJSON(t, p)
	for _, offset := range []int{0, len(want) / 2, len(want) - 1} {
		err := writeRestoreImportProfileJSON(&restoreHashFailingWriter{remaining: offset}, detached, RestoreImportProfileAccounting{})
		if !errors.Is(err, errRestoreHashWrite) {
			t.Fatalf("writer failure at %d lost: %v", offset, err)
		}
	}
}

type restoreHashAccountingEntry struct {
	phase string
	bytes uint64
}

type restoreHashAccountingTrace struct {
	t       *testing.T
	live    map[int]restoreHashAccountingEntry
	calls   []restoreHashAccountingEntry
	failAt  int
	held    uint64
	peak    uint64
	refusal error
}

func (a *restoreHashAccountingTrace) reserve(phase string, n uint64) (func(), error) {
	a.t.Helper()
	entry := restoreHashAccountingEntry{phase: phase, bytes: n}
	a.calls = append(a.calls, entry)
	id := len(a.calls)
	if id == a.failAt {
		return nil, a.refusal
	}
	if a.live == nil {
		a.live = map[int]restoreHashAccountingEntry{}
	}
	a.live[id] = entry
	a.held += n
	a.peak = max(a.peak, a.held)
	return func() {
		if _, ok := a.live[id]; !ok {
			a.t.Fatalf("candidate released twice: %s", phase)
		}
		delete(a.live, id)
		a.held -= n
	}, nil
}

func (a *restoreHashAccountingTrace) accounting() RestoreImportProfileAccounting {
	return RestoreImportProfileAccounting{
		Bytes: a.reserve,
		JSON: func(phase string, value any) (func(), error) {
			// Test-only counting may materialize the wire candidate. Production
			// uses the archive's nonallocating logical-size counter.
			wire, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			return a.reserve(phase, uint64(len(wire)))
		},
	}
}

func TestRestoreImportProfileHashAccountingReleasesEveryCandidate(t *testing.T) {
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	p := sourcePlaneTestProfile()
	p.SourcePlanes = &binding
	want, err := p.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(binding.CanonicalPlan)
	trace := restoreHashAccountingTrace{t: t}
	got, err := p.RequestHashWithAccounting(trace.accounting())
	if err != nil || got != want || !bytes.Equal(original, binding.CanonicalPlan) {
		t.Fatal("accounting changed the checked hash or borrowed plan", got, want, err)
	}
	if trace.held != 0 || len(trace.live) != 0 {
		t.Fatal("hash retained a scratch candidate after return", trace.held, trace.live)
	}
	seenBase64 := false
	for _, entry := range trace.calls {
		if entry.phase == "restore_profile_hash_base64" {
			seenBase64 = true
			if entry.bytes != 1024 {
				t.Fatal("base64 reservation differs from actual fixed buffer", entry)
			}
		}
	}
	if !seenBase64 {
		t.Fatal("canonical bytes were not streamed through the admitted buffer")
	}
	// Refuse each actual phase in turn. Every earlier acquisition must unwind,
	// and no failed request may expose a partial digest.
	for failAt := 1; failAt <= len(trace.calls); failAt++ {
		t.Run(trace.calls[failAt-1].phase, func(t *testing.T) {
			refusal := errors.New("actual profile candidate refused")
			denied := restoreHashAccountingTrace{t: t, failAt: failAt, refusal: refusal}
			got, err := p.RequestHashWithAccounting(denied.accounting())
			if got != "" || !errors.Is(err, refusal) {
				t.Fatal("candidate refusal changed or leaked a digest", got, err)
			}
			if denied.held != 0 || len(denied.live) != 0 {
				t.Fatal("failed hash retained an earlier candidate", denied.held, denied.live)
			}
		})
	}
}

type restoreHashShortWriter struct{}

func TestRestoreImportProfileHashAccountingAcceptsNilRelease(t *testing.T) {
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	p := sourcePlaneTestProfile()
	p.SourcePlanes = &binding
	wire := predecessorImportHashJSON(t, p)
	sum := sha256.Sum256(wire)
	want := hex.EncodeToString(sum[:])
	for _, test := range []struct {
		name        string
		bytes, json bool
	}{{"bytes", true, false}, {"json", false, true}, {"both", true, true}} {
		t.Run(test.name, func(t *testing.T) {
			var accounting RestoreImportProfileAccounting
			byteCalls, jsonCalls := 0, 0
			if test.bytes {
				accounting.Bytes = func(string, uint64) (func(), error) {
					byteCalls++
					return nil, nil
				}
			}
			if test.json {
				accounting.JSON = func(string, any) (func(), error) {
					jsonCalls++
					return nil, nil
				}
			}
			got, err := p.RequestHashWithAccounting(accounting)
			if err != nil || got != want {
				t.Fatal("nil cleanup changed the predecessor hash", got, want, err)
			}
			if test.bytes && byteCalls == 0 || test.json && jsonCalls == 0 {
				t.Fatal("accounting callbacks were not reached", byteCalls, jsonCalls)
			}
		})
	}
}

func (restoreHashShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestRestoreImportProfileStreamReleasesCandidatesAfterSinkFailure(t *testing.T) {
	binding, err := NewSourcePlaneImportBindingV1(sourcePlaneTestPlan())
	if err != nil {
		t.Fatal(err)
	}
	p := sourcePlaneTestProfile()
	p.SourcePlanes = &binding
	detached, err := p.Detached()
	if err != nil {
		t.Fatal(err)
	}
	want := predecessorImportHashJSON(t, p)
	for _, offset := range []int{0, len(want) / 2, len(want) - 1} {
		trace := restoreHashAccountingTrace{t: t}
		err := writeRestoreImportProfileJSON(&restoreHashFailingWriter{remaining: offset}, detached, trace.accounting())
		if !errors.Is(err, errRestoreHashWrite) || trace.held != 0 || len(trace.live) != 0 {
			t.Fatal("sink failure retained a candidate or changed cause", offset, err, trace.live)
		}
	}
	trace := restoreHashAccountingTrace{t: t}
	err = writeRestoreHashBase64(restoreHashShortWriter{}, binding.CanonicalPlan, trace.accounting())
	if !errors.Is(err, io.ErrShortWrite) || trace.held != 0 || len(trace.live) != 0 {
		t.Fatal("short base64 write lost its error or reservation", err, trace.live)
	}
}
