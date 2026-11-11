package cardb20

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

// These fault controls exercise the new count walker at its page-provider
// boundary. They supplement the actual public multi-page acceptance; an error
// after a successful first page must never publish that page's partial total.
func TestB23ReadyCountLatePageFailureAndSourceBinding(t *testing.T) {
	at := service.AsOf{Revision: model.Natural64(17), Incarnation: "b23-ready-count-source"}
	provider := &service.WorkflowPageSnapshot{Revision: model.Natural64(9)}
	first := func() service.BeadPage {
		return service.BeadPage{
			Items: []service.Bead{{ID: "b23-first"}}, AsOf: at,
			ReadyMembership: "complete-ready-membership", WorkflowSnapshot: provider,
			Next: &service.PagePosition{Offset: 1, AsOf: at},
		}
	}
	second := func() service.BeadPage {
		return service.BeadPage{Items: []service.Bead{{ID: "b23-second"}}, AsOf: at,
			ReadyMembership: "complete-ready-membership", WorkflowSnapshot: provider}
	}
	lateError := errors.New("B23 late page source unavailable")
	for _, name := range []string{"complete", "late-error", "revision", "incarnation", "membership", "provider", "provider-missing", "repeated-item", "empty-nonterminal", "repeated-position", "backward-position"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			got, err := service.CountReadyPages(context.Background(), model.PrincipalRef{}, service.ReadyRequest{Project: project, Limit: 1}, func(_ context.Context, _ model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
				calls++
				if req.Limit != 1000 || req.Project != project {
					t.Fatal("count lost the validated predicate or internal page size")
				}
				if calls == 1 {
					if req.Position != nil {
						t.Fatal("first request unexpectedly has a position")
					}
					return first(), nil
				}
				if calls != 2 || req.Position == nil || req.Position.Offset != 1 || req.Position.AsOf != at {
					t.Fatal("walker lost the first page's exact continuation", req.Position)
				}
				p := second()
				switch name {
				case "late-error":
					return service.BeadPage{}, lateError
				case "revision":
					p.AsOf.Revision = model.Natural64(18)
				case "incarnation":
					p.AsOf.Incarnation = "different-source"
				case "membership":
					p.ReadyMembership = "different-membership"
				case "provider":
					p.WorkflowSnapshot = &service.WorkflowPageSnapshot{Revision: model.Natural64(10)}
				case "provider-missing":
					p.WorkflowSnapshot = nil
				case "repeated-item":
					p.Items[0].ID = "b23-first"
				case "empty-nonterminal":
					p.Items = nil
					p.Next = &service.PagePosition{Offset: 2, AsOf: at}
				case "repeated-position":
					p.Next = &service.PagePosition{Offset: 1, AsOf: at}
				case "backward-position":
					p.Next = &service.PagePosition{Offset: 0, AsOf: at}
				}
				return p, nil
			})
			if calls != 2 {
				t.Fatalf("late-page control did not reach its second page: %d", calls)
			}
			if name == "complete" {
				if err != nil || got.Count != 2 || got.AsOf != at {
					t.Fatalf("complete count=%+v error=%v", got, err)
				}
				return
			}
			if !reflect.DeepEqual(got, service.ReadyCount{}) {
				t.Fatalf("failure published partial result: %+v", got)
			}
			if name == "late-error" {
				if !errors.Is(err, lateError) {
					t.Fatal("late source error was lost", err)
				}
			} else {
				var typed *model.Error
				if !errors.As(err, &typed) || typed.Data.Code != "cursor_invalid" {
					t.Fatalf("source/progress refusal=%v", err)
				}
			}
		})
	}
}
