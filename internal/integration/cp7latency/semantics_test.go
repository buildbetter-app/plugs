package cp7latency_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
)

func TestQ5RawGroupSeededRotationBeforeAuthorization(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 33)
			req := service.ClaimNextRequest{Project: projectName}
			identity, err := service.ClaimNextRequestIdentity(req)
			if err != nil {
				t.Fatal(err)
			}
			// All 33 raw ready issues share priority zero. Only the 16 even
			// identities are permitted. Find a deterministic distinguishing key.
			var key string
			var rawOffset, rawWinner, authorizedWinner int
			for i := 0; i < 128; i++ {
				key = requestKey("raw-pivot", 2, "case", i)
				seed := sha256.Sum256([]byte(identity + "\x00" + key))
				group := sha256.Sum256([]byte(fmt.Sprintf("%x", seed) + "\x000"))
				number := binary.BigEndian.Uint64(group[:8])
				rawOffset = int(number % 33)
				for step := 0; step < 33; step++ {
					candidate := (rawOffset+step)%33 + 1
					if candidate%2 == 0 {
						rawWinner = candidate
						break
					}
				}
				authorizedWinner = 2 * (int(number%16) + 1)
				if rawWinner != authorizedWinner {
					break
				}
			}
			if rawWinner == 0 || rawWinner == authorizedWinner {
				t.Fatal("no distinguishing raw-group pivot case")
			}
			p := f.agent(2, false)
			meta := model.WriteMeta{Project: projectName, Return: "full", IdempotencyKey: key}
			out, err := f.app.Service.ClaimNextWithOutcome(context.Background(), p, req, meta)
			if err != nil {
				t.Fatal(err)
			}
			f.checkClaim(t, p, out, meta, 2)
			if got := out.Commit.Results[0].Ref.ID; got != beadRef(rawWinner).ID {
				t.Fatalf("selected %s; raw rotation requires %s, authorized-only rotation would select %s", got, beadRef(rawWinner).ID, beadRef(authorizedWinner).ID)
			}
			t.Logf("CP7_RAW_PIVOT raw_group=33 authorized_group=16 raw_offset=%d winner=%s authorized_only_winner=%s key=%s", rawOffset, beadRef(rawWinner).ID, beadRef(authorizedWinner).ID, key)
		})
	}
}

func TestReadyCursorBindsAuthorizedTailBeyondFirstPage(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine, 220)
			ctx := context.Background()
			p := f.user(1)
			req := service.ReadyRequest{Project: projectName, Limit: pageSize}
			first, err := f.app.Service.Ready(ctx, p, req)
			if err != nil || first.Next == nil {
				t.Fatalf("first page: %v", err)
			}
			f.checkPage(t, first, 1)
			req.Position = first.Next
			control, err := f.app.Service.Ready(ctx, p, req)
			if err != nil || len(control.Items) != pageSize || control.Items[0].ID != beadRef(101).ID {
				t.Fatalf("unchanged continuation: %v", err)
			}
			// The authorized row is outside both the first and second pages;
			// it remains ready and keeps its identity, priority and parent.
			tail := document(220)
			tail.Fields["title"] = "CP7 tail changed after cursor issuance"
			encoded, err := json.Marshal(tail)
			if err != nil {
				t.Fatal(err)
			}
			f.patchSource(t, "ready_tail", beadRef(220), encoded, []string{"/fields/title"})
			req.Position = nil
			fresh, err := f.app.Service.Ready(ctx, p, req)
			if err != nil || fresh.Next == nil {
				t.Fatalf("fresh first page: %v", err)
			}
			if !reflect.DeepEqual(first.Items, fresh.Items) {
				t.Fatal("tail change altered first-page content")
			}
			if !first.Next.AuthorizationBound || !fresh.Next.AuthorizationBound || first.Next.AuthorizationRevision.String() != fresh.Next.AuthorizationRevision.String() {
				t.Fatal("test changed authorization generation")
			}
			if first.Next.ReadyMembership == "" || first.Next.ReadyMembership == fresh.Next.ReadyMembership {
				t.Fatal("complete authorized membership binding ignored tail change")
			}
			req.Position = first.Next
			page, err := f.app.Service.Ready(ctx, p, req)
			requireCode(t, err, "cursor_invalid")
			var typed *model.Error
			if !errors.As(err, &typed) || !strings.Contains(typed.Message, "ready membership changed") || len(page.Items) != 0 || page.Next != nil {
				t.Fatal("cursor refused for a different reason or emitted a partial page")
			}
			t.Log("CP7_READY_TAIL first_page_unchanged=true authorization_generation_unchanged=true complete_membership_changed=true continuation=cursor_invalid")
		})
	}
}
