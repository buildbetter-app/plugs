package s1b21b24

import (
	"bytes"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestS1B21BatchReceiptWindowAcrossStores(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			anchor, _ := f.create(t, "clock-anchor", "anchor")
			request := f.request("tx.apply", "clock-batch", map[string]any{"changeSet": map[string]any{"ops": []any{
				map[string]any{"kind": "patch", "target": anchor, "fields": map[string]any{"title": "first"}},
				map[string]any{"kind": "comment", "target": anchor, "body": "clock batch"},
			}}})
			first := f.dispatch(request)
			initial := commit(t, first)
			if initial.Revision.String() != "2" || len(initial.Results) != 2 {
				t.Fatalf("batch=%+v", initial)
			}
			tick := time.Nanosecond
			if engine == "postgres" {
				tick = time.Microsecond
			}
			f.advance(t, driver.IdempotencyWindow-tick)
			before := f.state(t)
			replay := f.dispatch(request)
			if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(replay.Result, first.Result) {
				t.Fatalf("before expiry batch replay=%+v", replay)
			}
			sameState(t, before, f.state(t))
			f.advance(t, tick)
			exact := f.dispatch(request)
			if exact.Error != nil || exact.Meta.Replayed || commit(t, exact).Revision.String() != "3" {
				t.Fatalf("exact expiry did not execute fresh batch=%+v", exact)
			}
			f.advance(t, driver.IdempotencyWindow+tick)
			after := f.dispatch(request)
			if after.Error != nil || after.Meta.Replayed || commit(t, after).Revision.String() != "4" {
				t.Fatalf("after expiry did not execute fresh batch=%+v", after)
			}
			t.Logf("B21 engine=%s before=stable-replay exactly=fresh-batch after=fresh-batch 24h_window_unchanged=true tick=%s", engine, tick)
		})
	}
}
