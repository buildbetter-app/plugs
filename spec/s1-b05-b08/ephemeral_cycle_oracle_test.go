package s1b05b08

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// Live provider items have no graph relationships under the adopted contract.
// These are authored current-contract assertions, not mixed-plane positives or
// paired reference observations. The existing oracle independently enumerates
// shortest return paths; this projection uses an uncached document lookup.
func currentCycleReportOracle(rows []cycleSeedRow, tracks bool, limit int) service.CyclesResult {
	edges := []cycleOracleEdge{}
	for _, row := range rows {
		for _, link := range row.links {
			edges = append(edges, cycleOracleEdge{from: row.id, to: link.To.ID, kind: link.Kind})
		}
	}
	result := service.CyclesResult{Items: []service.Cycle{}}
	count := uint64(0)
	for _, path := range cycleReportOracle(edges, tracks) {
		item := service.Cycle{Path: []service.CycleMember{}, MemberCount: model.Natural64(uint64(len(path)))}
		anchored, hidden := false, uint64(0)
		for _, id := range path {
			visible := false
			for _, row := range rows {
				if row.id == id && row.project == "visible" {
					visible = true
				}
			}
			if visible {
				ref := model.RefByID(id, model.Natural64(0))
				item.Path = append(item.Path, service.CycleMember{Ref: &ref})
				anchored = true
			} else {
				item.Path = append(item.Path, service.CycleMember{Unreadable: true})
				hidden++
			}
		}
		if anchored {
			count++
			item.Partial, item.UnreadableCount = hidden != 0, model.Natural64(hidden)
			if len(result.Items) < limit {
				result.Items = append(result.Items, item)
			}
		}
	}
	result.CycleCount, result.Truncated = model.Natural64(count), count > uint64(len(result.Items))
	return result
}

func TestB06LivePersistentWorkflowKeepsCurrentReportAndRefusalContract(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newEphemeralCycleFixture(t, engine)
			items := f.invoke(t, "current-contract-invoke", 2)
			before := f.state(t)
			if before.Checkpoint.Generation == 0 || len(before.Checkpoint.Payload) == 0 || bytes.Contains(before.Archive, []byte("private ephemeral title")) {
				t.Fatal("fixture lacks genuine provider state or leaked its body into durable history")
			}
			meta, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, tracks := range []bool{false, true} {
				for _, limit := range []int{1, 2, 100} {
					got := f.report(t, false, tracks, limit)
					want := currentCycleReportOracle(defaultCycleRPCRows(), tracks, limit)
					if !reflect.DeepEqual(got.Items, want.Items) || got.CycleCount != want.CycleCount || got.Truncated != want.Truncated {
						t.Fatalf("tracks=%v limit=%d live provider changed independent durable projection: got=%+v want=%+v", tracks, limit, got, want)
					}
					if got.AsOf.Revision != meta.Head || got.AsOf.Incarnation != meta.Incarnation {
						t.Fatalf("report lost pinned durable snapshot: %+v meta=%+v", got.AsOf, meta)
					}
					encoded, err := json.Marshal(got)
					if err != nil {
						t.Fatal(err)
					}
					for _, ref := range items {
						if bytes.Contains(encoded, []byte(ref.ID)) {
							t.Fatal("ordinary report exposed a workflow identity without an adopted graph contract")
						}
					}
				}
			}
			got := f.request(t, "bead.cycles", map[string]any{"includeEphemeral": true}, "")
			if got.Error == nil || got.Error.Data.Code != "plugin_durable_state_unavailable" {
				t.Fatalf("persistent workflow graph refusal=%+v", got.Error)
			}
			direct, err := f.domain.Cycles(context.Background(), f.by, service.CyclesRequest{Project: "visible", IncludeEphemeral: true})
			var refused *model.Error
			if !errors.As(err, &refused) || refused.Data.Code != "plugin_durable_state_unavailable" || !reflect.DeepEqual(direct, service.CyclesResult{}) {
				t.Fatalf("direct service fabricated a workflow graph: result=%+v error=%v", direct, err)
			}
			if after := f.state(t); !reflect.DeepEqual(before, after) {
				t.Fatal("report/refusal changed durable graph/history/receipts, provider checkpoint/audit or sentinel")
			}
			full := f.report(t, false, true, 100)
			f.reopen(t)
			if got := f.report(t, false, true, 100); !reflect.DeepEqual(got, full) {
				t.Fatalf("reopen changed current report identity/path/count: got=%+v before=%+v", got, full)
			}
			item, err := f.domain.Get(context.Background(), f.by, service.GetRequest{ID: items[0], Project: "visible"})
			if err != nil || item.Bead.Plane != "ephemeral" || item.Bead.ID != items[0].ID || item.Bead.Fields["title"] != "private ephemeral title" {
				t.Fatalf("graph refusal damaged real persisted workflow state: item=%+v error=%v", item, err)
			}
			if after := f.state(t); !reflect.DeepEqual(before, after) {
				t.Fatal("reopen/report/read changed provider or durable state")
			}
		})
	}
}
