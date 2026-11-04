package s1b21b24

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func staleDefinition(query string) map[string]any {
	return map[string]any{
		"filter":       map[string]any{"query": query},
		"fields":       []string{"id", "status", "project"},
		"sort":         []string{"id"},
		"budgetTokens": 1,
	}
}

func staleIDs(t *testing.T, got reply) (ids []string, cursor string) {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("stale query: %+v", got.Error)
	}
	var page struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(got.Result, &page); err != nil {
		t.Fatalf("stale result %s: %v", got.Result, err)
	}
	for _, item := range page.Items {
		ids = append(ids, item.ID)
	}
	return ids, page.NextCursor
}

func staleCount(t *testing.T, got reply) int {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("stale count: %+v", got.Error)
	}
	var result struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(got.Result, &result); err != nil {
		t.Fatalf("stale count result %s: %v", got.Result, err)
	}
	return result.Count
}

func assertNoWriteAfterClockAdvance(t *testing.T, before, after storedState) {
	t.Helper()
	// IdempotencyByKey is intentionally visibility-windowed. A clock advance
	// may hide old receipts from the reader, but it must not alter any durable
	// record, version, derived, changeset, or metadata plane or create a new
	// receipt. Keep that expiry observation separate from the durable census.
	beforeReceipts := before.Receipts
	afterReceipts := after.Receipts
	for key := range afterReceipts {
		if _, ok := beforeReceipts[key]; !ok {
			t.Fatalf("clock advance exposed a new receipt %q", key)
		}
	}
	if len(afterReceipts) > len(beforeReceipts) {
		t.Fatalf("clock advance increased visible receipts: before=%d after=%d", len(beforeReceipts), len(afterReceipts))
	}
	before.Receipts = nil
	after.Receipts = nil
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("clock advance changed durable state: before=%+v after=%+v", before, after)
	}
}

func latestVersion(t *testing.T, state storedState, ref model.Ref) driver.Version {
	t.Helper()
	versions := state.Versions[ref.ID]
	if len(versions) == 0 {
		t.Fatalf("no persisted versions for %s", ref.ID)
	}
	var record *driver.Record
	for i := range state.Records {
		if state.Records[i].Ref.ID == ref.ID {
			record = &state.Records[i]
			break
		}
	}
	if record == nil {
		t.Fatalf("no persisted record for %s", ref.ID)
	}
	latest := versions[len(versions)-1]
	if latest.Revision != record.Revision || latest.CreatedAt.IsZero() || latest.Deleted {
		t.Fatalf("latest persisted version=%+v record=%+v", latest, *record)
	}
	return latest
}

func TestS1B24StaleUsesPersistedStrictCutoffAndStablePaginationAcrossStores(t *testing.T) {
	for _, engine := range append(engines(), "postgres") {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			beforeRef, _ := f.create(t, "stale-before", "before")
			secondRef, _ := f.create(t, "stale-second", "second")
			atRef, _ := f.create(t, "stale-at", "at")
			afterRef, _ := f.create(t, "stale-after", "after")
			f.advance(t, 24*time.Hour)
			cutoff := f.clock.Now()
			if got := f.call("bead.update", "stale-at-update", map[string]any{"id": atRef, "fields": map[string]any{"title": "at-cutoff"}}); got.Error != nil {
				t.Fatalf("at-cutoff update: %+v", got.Error)
			}
			f.advance(t, time.Microsecond)
			if got := f.call("bead.update", "stale-after-update", map[string]any{"id": afterRef, "fields": map[string]any{"title": "after-cutoff"}}); got.Error != nil {
				t.Fatalf("after-cutoff update: %+v", got.Error)
			}
			f.advance(t, 24*time.Hour-time.Microsecond)
			before := f.state(t)
			wantOld := map[string]bool{beforeRef.ID: true, secondRef.ID: true}
			wantNew := map[string]bool{atRef.ID: true, afterRef.ID: true}
			first, paramsCursor := staleIDs(t, f.call("view.run", "stale-first-page", map[string]any{
				"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
			}))
			if len(first) != 1 || paramsCursor == "" || !wantOld[first[0]] {
				t.Fatalf("first stale page ids=%v cursor=%q want one old row", first, paramsCursor)
			}
			count := staleCount(t, f.call("view.run", "stale-count-before-advance", map[string]any{
				"countOnly": true, "definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
			}))
			if count != 2 {
				t.Fatalf("strict stale count=%d want 2", count)
			}
			if got := f.state(t); !reflect.DeepEqual(before, got) {
				t.Fatalf("stale list/count changed durable state")
			}

			// Reopen the real SQL stores before changing the clock. The same
			// persisted Version.CreatedAt boundary must produce the same count.
			if engine == "sqlite" || engine == "postgres" {
				f.reopen(t)
				reopened := staleCount(t, f.call("view.run", "stale-count-reopen", map[string]any{
					"countOnly": true, "definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
				}))
				if reopened != count {
					t.Fatalf("SQL reopen stale count=%d want %d", reopened, count)
				}
				if got := f.state(t); !reflect.DeepEqual(before, got) {
					t.Fatalf("SQL reopen changed durable state")
				}
			}

			f.advance(t, 72*time.Hour)
			continuation, _ := staleIDs(t, f.call("view.run", "stale-second-page", map[string]any{
				"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1}, "cursor": paramsCursor,
			}))
			if len(continuation) != 1 || !wantOld[continuation[0]] || continuation[0] == first[0] {
				t.Fatalf("fixed-cutoff continuation ids=%v first=%v", continuation, first)
			}
			for _, id := range append(append([]string{}, first...), continuation...) {
				if wantNew[id] {
					t.Fatalf("row %s crossed the cutoff between pages", id)
				}
				delete(wantOld, id)
			}
			if len(wantOld) != 0 {
				t.Fatalf("old rows missing from paged membership: %v", wantOld)
			}
			assertNoWriteAfterClockAdvance(t, before, f.state(t))

			latestBefore := latestVersion(t, before, beforeRef)
			latestSecond := latestVersion(t, before, secondRef)
			latestAt := latestVersion(t, before, atRef)
			latestAfter := latestVersion(t, before, afterRef)
			if !latestBefore.CreatedAt.Before(cutoff) || !latestSecond.CreatedAt.Before(cutoff) {
				t.Fatalf("old timestamps before=%s second=%s cutoff=%s", latestBefore.CreatedAt, latestSecond.CreatedAt, cutoff)
			}
			if !latestAt.CreatedAt.Equal(cutoff) {
				t.Fatalf("at-boundary timestamp=%s cutoff=%s", latestAt.CreatedAt, cutoff)
			}
			if !latestAfter.CreatedAt.After(cutoff) {
				t.Fatalf("after-boundary timestamp=%s cutoff=%s", latestAfter.CreatedAt, cutoff)
			}
		})
	}
}

func TestS1B24StaleRechecksCurrentAuthorityAcrossStores(t *testing.T) {
	for _, engine := range append(engines(), "postgres") {
		t.Run(engine, func(t *testing.T) {
			denied := map[string]bool{}
			f := newFixtureWithConfig(t, engine, nil, func(_ context.Context, _ model.PrincipalRef, ref model.ResourceRef, _ string) error {
				if denied[ref.Ref.ID] {
					return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
				}
				return nil
			})
			firstRef, _ := f.create(t, "stale-authority-first", "first")
			secondRef, _ := f.create(t, "stale-authority-second", "second")
			f.advance(t, 49*time.Hour)
			before := f.state(t)
			first, cursor := staleIDs(t, f.call("view.run", "authority-first-page", map[string]any{
				"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
			}))
			if len(first) != 1 || cursor == "" || first[0] != firstRef.ID {
				t.Fatalf("authority first page=%v cursor=%q want %s", first, cursor, firstRef.ID)
			}
			denied[secondRef.ID] = true
			second, _ := staleIDs(t, f.call("view.run", "authority-second-page", map[string]any{
				"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1}, "cursor": cursor,
			}))
			if len(second) != 0 {
				t.Fatalf("revoked current authority leaked continuation=%v", second)
			}
			count := staleCount(t, f.call("view.run", "authority-count", map[string]any{
				"countOnly": true, "definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
			}))
			if count != 1 {
				t.Fatalf("current-authority count=%d want 1", count)
			}
			if got := f.state(t); !reflect.DeepEqual(before, got) {
				t.Fatalf("authority-only stale reads changed durable state")
			}
		})
	}
}

func TestS1B24ContinuationRevokingEmittedRowDoesNotSkipUnread(t *testing.T) {
	denied := map[string]bool{}
	f := newFixtureWithConfig(t, "memory", nil, func(_ context.Context, _ model.PrincipalRef, ref model.ResourceRef, _ string) error {
		if denied[ref.Ref.ID] {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	})
	a, _ := f.create(t, "stale-authority-a", "a")
	b, _ := f.create(t, "stale-authority-b", "b")
	c, _ := f.create(t, "stale-authority-c", "c")
	f.advance(t, 49*time.Hour)
	definition := staleDefinition("status=todo")
	first, cursor := staleIDs(t, f.call("view.run", "authority-revoke-first", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1},
	}))
	if len(first) != 1 || first[0] != a.ID || cursor == "" {
		t.Fatalf("first authority page=%v cursor=%q want [%s]", first, cursor, a.ID)
	}
	denied[a.ID] = true
	second, _ := staleIDs(t, f.call("view.run", "authority-revoke-continuation", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1}, "cursor": cursor,
	}))
	if len(second) != 1 || second[0] != b.ID {
		t.Fatalf("revoked emitted row shifted continuation=%v want [%s] (third=%s)", second, b.ID, c.ID)
	}
}

func TestS1B24DeniedRowsDoNotCreateOpaqueStaleContinuation(t *testing.T) {
	denied := map[string]bool{}
	f := newFixtureWithConfig(t, "memory", nil, func(_ context.Context, _ model.PrincipalRef, ref model.ResourceRef, _ string) error {
		if denied[ref.Ref.ID] {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	})
	visible, _ := f.create(t, "stale-opaque-visible", "visible")
	hidden, _ := f.create(t, "stale-opaque-hidden", "hidden")
	later, _ := f.create(t, "stale-opaque-later", "later")
	denied[hidden.ID] = true
	f.advance(t, 49*time.Hour)
	ids, cursor := staleIDs(t, f.call("view.run", "stale-opaque-denied-tail", map[string]any{
		"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
	}))
	if len(ids) != 1 || ids[0] != visible.ID {
		t.Fatalf("denied-tail stale page=%v want [%s]", ids, visible.ID)
	}
	if cursor == "" {
		t.Fatal("visible unread row did not receive a continuation cursor")
	}
	codec, err := viewrpc.NewCursorCodec(fixedCursorKey)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(cursor)
	if err != nil {
		t.Fatal(err)
	}
	visibleDigest := sha256.Sum256([]byte(visible.ID))
	if decoded.Index != 0 || decoded.StaleResumeDigest != hex.EncodeToString(visibleDigest[:]) {
		t.Fatalf("stale cursor exposed numeric/hidden position: %+v", decoded)
	}
	denied[later.ID] = true
	_, tailCursor := staleIDs(t, f.call("view.run", "stale-opaque-only-denied-tail", map[string]any{
		"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
	}))
	if tailCursor != "" {
		t.Fatalf("denied-only tail exposed a continuation cursor %q", tailCursor)
	}
}

func TestS1B24AcceptedRepeatedSortKeysKeepCursorBounded(t *testing.T) {
	f := newFixture(t, "memory")
	firstRef, _ := f.create(t, "stale-many-sort-first", "first")
	secondRef, _ := f.create(t, "stale-many-sort-second", "second")
	f.advance(t, 49*time.Hour)
	before := f.state(t)

	// Repeated supported keys are admitted by the definition contract. The
	// previous cursor copied all 1,000 values and exceeded the 16 KiB token
	// bound even though this legitimate budget-one page has only two rows.
	sortKeys := make([]string, 1000)
	for i := range sortKeys {
		sortKeys[i] = "id"
	}
	definition := map[string]any{
		"filter":       map[string]any{"query": "status=todo"},
		"fields":       []string{"id"},
		"sort":         sortKeys,
		"budgetTokens": 1,
	}
	first, cursor := staleIDs(t, f.call("view.run", "stale-many-sort-page-one", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1},
	}))
	if len(first) != 1 || first[0] != firstRef.ID || cursor == "" || len(cursor) > 16*1024 {
		t.Fatalf("first repeated-sort page ids=%v cursor-bytes=%d want [%s] and bounded continuation", first, len(cursor), firstRef.ID)
	}
	codec, err := viewrpc.NewCursorCodec(fixedCursorKey)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(cursor)
	if err != nil {
		t.Fatal(err)
	}
	firstDigest := sha256.Sum256([]byte(firstRef.ID))
	if decoded.Index != 0 || decoded.StaleResumeDigest != hex.EncodeToString(firstDigest[:]) {
		t.Fatalf("repeated-sort cursor did not bind only the emitted row: %+v", decoded)
	}
	unknown := *decoded
	unknown.StaleResumeDigest = strings.Repeat("0", sha256.Size*2)
	unknownToken, err := codec.Encode(&unknown)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.call("view.run", "stale-many-sort-unknown-resume", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1}, "cursor": unknownToken,
	}); got.Error == nil || got.Error.Data.Code != "cursor_invalid" {
		t.Fatalf("unknown signed resume key result=%+v want cursor_invalid", got)
	}
	indexed := *decoded
	indexed.Index = 1
	indexedToken, err := codec.Encode(&indexed)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.call("view.run", "stale-many-sort-indexed-resume", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1}, "cursor": indexedToken,
	}); got.Error == nil || got.Error.Data.Code != "cursor_invalid" {
		t.Fatalf("signed numeric stale position result=%+v want cursor_invalid", got)
	}
	second, end := staleIDs(t, f.call("view.run", "stale-many-sort-page-two", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1}, "cursor": cursor,
	}))
	if len(second) != 1 || second[0] != secondRef.ID || end != "" {
		t.Fatalf("second repeated-sort page ids=%v cursor=%q want [%s] and end", second, end, secondRef.ID)
	}
	if after := f.state(t); !reflect.DeepEqual(before, after) {
		t.Fatal("repeated-sort stale pagination changed durable state")
	}
}

func TestS1B24GroupedStaleContinuationUsesVisibleResumeKey(t *testing.T) {
	denied := map[string]bool{}
	f := newFixtureWithConfig(t, "memory", nil, func(_ context.Context, _ model.PrincipalRef, ref model.ResourceRef, _ string) error {
		if denied[ref.Ref.ID] {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	})
	a, _ := f.create(t, "stale-group-a", "a")
	b, _ := f.create(t, "stale-group-b", "b")
	c, _ := f.create(t, "stale-group-c", "c")
	f.advance(t, 49*time.Hour)
	definition := map[string]any{
		"filter":       map[string]any{"query": "status=todo"},
		"fields":       []string{"id", "type"},
		"groupBy":      []string{"type"},
		"sort":         []string{"id"},
		"budgetTokens": 1,
	}
	first := f.call("view.run", "stale-group-first", map[string]any{"definition": definition, "stale": map[string]any{"days": 1}})
	if first.Error != nil {
		t.Fatal(first.Error)
	}
	var firstPage struct {
		Groups []struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		} `json:"groups"`
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(first.Result, &firstPage); err != nil {
		t.Fatalf("grouped first result=%s: %v", first.Result, err)
	}
	if len(firstPage.Groups) != 1 || len(firstPage.Groups[0].Items) != 1 || firstPage.Groups[0].Items[0].ID != a.ID || firstPage.NextCursor == "" {
		t.Fatalf("grouped first page=%s want item %s and continuation", first.Result, a.ID)
	}
	denied[a.ID] = true
	second := f.call("view.run", "stale-group-revoked", map[string]any{"definition": definition, "stale": map[string]any{"days": 1}, "cursor": firstPage.NextCursor})
	if second.Error != nil {
		t.Fatal(second.Error)
	}
	var secondPage struct {
		Groups []struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(second.Result, &secondPage); err != nil {
		t.Fatalf("grouped continuation result=%s: %v", second.Result, err)
	}
	if len(secondPage.Groups) != 1 || len(secondPage.Groups[0].Items) != 1 || secondPage.Groups[0].Items[0].ID != b.ID {
		t.Fatalf("grouped continuation=%s want unread %s (then %s)", second.Result, b.ID, c.ID)
	}
}

func TestS1B24RefusesMutableNoPluginRefreshAcrossContinuation(t *testing.T) {
	f := newFixture(t, "memory")
	f.create(t, "refresh-first", "first")
	f.create(t, "refresh-second", "second")
	f.advance(t, 49*time.Hour)
	definition := staleDefinition("status=todo")
	first := f.call("view.run", "refresh-first-page", map[string]any{"definition": definition, "stale": map[string]any{"days": 1}})
	if first.Error != nil {
		t.Fatal(first.Error)
	}
	var page struct {
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(first.Result, &page); err != nil || page.NextCursor == "" {
		t.Fatalf("refresh first page=%s err=%v", first.Result, err)
	}
	custom := ontology.DefaultDocument()
	custom.Statuses["todo"] = ontology.Status{Name: "Todo (refreshed)", Category: ontology.Unstarted}
	snapshot, problems := ontology.Compile(custom, nil, nil)
	if len(problems) != 0 {
		t.Fatalf("custom refresh compile=%v", problems)
	}
	f.ontology.Refresh(snapshot)
	second := f.call("view.run", "refresh-second-page", map[string]any{"definition": definition, "stale": map[string]any{"days": 1}, "cursor": page.NextCursor})
	if second.Error == nil || second.Error.Data.Code != "invalid_params" {
		t.Fatalf("mutable ontology continuation was not refused: %+v", second)
	}
}

func TestS1B24ExplicitStatusPresenceUsesExactSelection(t *testing.T) {
	f := newFixture(t, "memory")
	deferred, _ := f.create(t, "stale-backlog", "backlog")
	if got := f.call("bead.defer", "stale-backlog-defer", map[string]any{"id": deferred, "reason": "parked"}); got.Error != nil {
		t.Fatal(got.Error)
	}
	closed, _ := f.create(t, "stale-closed", "closed")
	if got := f.call("bead.close", "stale-closed-close", map[string]any{"id": closed}); got.Error != nil {
		t.Fatal(got.Error)
	}
	f.advance(t, 49*time.Hour)
	definition := staleDefinition("")
	var result struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	defaultQuery := f.call("view.run", "stale-default-status", map[string]any{"definition": definition, "stale": map[string]any{"days": 1}})
	if defaultQuery.Error != nil {
		t.Fatal(defaultQuery.Error)
	}
	if err := json.Unmarshal(defaultQuery.Result, &result); err != nil {
		t.Fatal(err)
	}
	for _, item := range result.Items {
		if item.ID == deferred.ID || item.ID == closed.ID {
			t.Fatalf("default stale membership included non-active item %q", item.ID)
		}
	}
	for _, status := range []string{"does-not-exist", ""} {
		got := f.call("view.run", "stale-explicit-"+status, map[string]any{
			"definition": definition, "stale": map[string]any{"days": 1, "status": status},
		})
		if got.Error != nil {
			t.Fatalf("explicit status %q refusal=%+v", status, got.Error)
		}
		if err := json.Unmarshal(got.Result, &result); err != nil || len(result.Items) != 0 {
			t.Fatalf("explicit exact status %q result=%s err=%v", status, got.Result, err)
		}
	}
	terminal := f.call("view.run", "stale-explicit-terminal", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1, "status": "done"},
	})
	if terminal.Error != nil {
		t.Fatal(terminal.Error)
	}
	if err := json.Unmarshal(terminal.Result, &result); err != nil || len(result.Items) != 1 || result.Items[0].ID != closed.ID {
		t.Fatalf("explicit terminal status=%s err=%v", terminal.Result, err)
	}
	deferredQuery := f.call("view.run", "stale-explicit-deferred", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 1, "status": "backlog"},
	})
	if deferredQuery.Error != nil {
		t.Fatal(deferredQuery.Error)
	}
	if err := json.Unmarshal(deferredQuery.Result, &result); err != nil || len(result.Items) != 1 || result.Items[0].ID != deferred.ID {
		t.Fatalf("explicit deferred status=%s err=%v", deferredQuery.Result, err)
	}
	invalid := f.call("view.run", "stale-invalid-days", map[string]any{
		"definition": definition, "stale": map[string]any{"days": 0},
	})
	if invalid.Error == nil || invalid.Error.Data.Code != "invalid_params" {
		t.Fatalf("non-positive stale age was accepted: %+v", invalid)
	}
}

func TestS1B24ProtectedOntologyIsPinnedPerProjectAndMissingSourceRefuses(t *testing.T) {
	sourceRef := model.RefByID("ontology/s1-stale", model.Natural64(0))
	source := service.OntologySource{
		Base: ontology.DefaultDocument(),
		Overlays: map[string]ontology.Document{"custom": {
			Statuses: map[string]ontology.Status{"queued": {Name: "Queued", Category: ontology.Unstarted}},
		}},
	}
	f := newFixtureWithStaleSource(t, "memory", service.StaticOntologySource(sourceRef))
	sourceBody, err := service.MarshalOntologySource(source)
	if err != nil {
		t.Fatal(err)
	}
	customBody, err := json.Marshal(pipeline.Document{ID: "stale-custom", Project: "custom", Type: "issue", Fields: map[string]any{"title": "custom", "status": "queued"}, Labels: []string{"needle"}})
	if err != nil {
		t.Fatal(err)
	}
	defaultBody, err := json.Marshal(pipeline.Document{ID: "stale-default", Project: "default", Type: "issue", Fields: map[string]any{"title": "default", "status": "todo"}, Labels: []string{"needle"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock.Now(), Principal: anonymous}, []conformance.FixtureChange{
		{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: sourceBody},
		{Ref: model.RefByID("stale-custom", model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: customBody},
		{Ref: model.RefByID("stale-default", model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: defaultBody},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.advance(t, 49*time.Hour)
	custom := f.call("view.run", "stale-custom-ontology", map[string]any{
		"definition": staleDefinition("label=needle"), "stale": map[string]any{"days": 1, "status": "queued"},
		"_meta": map[string]any{"project": "custom"},
	})
	if custom.Error != nil || !json.Valid(custom.Result) {
		t.Fatalf("custom ontology query=%s err=%+v", custom.Result, custom.Error)
	}
	var result struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(custom.Result, &result); err != nil || len(result.Items) != 1 || result.Items[0].ID != "stale-custom" {
		t.Fatalf("custom ontology membership=%s err=%v", custom.Result, err)
	}
	defaultProject := f.call("view.run", "stale-default-ontology", map[string]any{
		"definition": staleDefinition("label=needle"), "stale": map[string]any{"days": 1, "status": "queued"},
		"_meta": map[string]any{"project": "default"},
	})
	if defaultProject.Error != nil {
		t.Fatal(defaultProject.Error)
	}
	if err := json.Unmarshal(defaultProject.Result, &result); err != nil || len(result.Items) != 0 {
		t.Fatalf("default project used custom ontology=%s err=%v", defaultProject.Result, err)
	}
	missing := newFixtureWithStaleSource(t, "memory", service.StaticOntologySource(model.RefByID("ontology/missing", model.Natural64(0))))
	missing.create(t, "stale-missing-source", "missing")
	missing.advance(t, 49*time.Hour)
	refused := missing.call("view.run", "stale-missing-refusal", map[string]any{
		"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
	})
	if refused.Error == nil || refused.Error.Data.Code != "invalid_params" {
		t.Fatalf("missing protected source was not refused: %+v", refused)
	}
	malformed := newFixtureWithStaleSource(t, "memory", service.StaticOntologySource(model.RefByID("ontology/malformed", model.Natural64(0))))
	malformed.create(t, "stale-malformed-source", "malformed")
	// The storage record itself must be admitted as valid JSON; the refusal
	// under test is the ontology decoder/compiler rejecting this empty source.
	if _, err := conformance.Seed(malformed.ctx, malformed.store, malformed.lease, driver.ChangesetHeader{Time: malformed.clock.Now(), Principal: anonymous}, []conformance.FixtureChange{{Ref: model.RefByID("ontology/malformed", model.Natural64(0)), Kind: "ontology", Op: model.OpCreate, Body: []byte(`{"base":{"statuses":[]}}`)}}); err != nil {
		t.Fatal(err)
	}
	malformed.advance(t, 49*time.Hour)
	bad := malformed.call("view.run", "stale-malformed-refusal", map[string]any{
		"definition": staleDefinition("status=todo"), "stale": map[string]any{"days": 1},
	})
	if bad.Error == nil || bad.Error.Data.Code != "invalid_params" {
		t.Fatalf("malformed protected source was not refused: %+v", bad)
	}
}
