package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestQueueProjectionFactsRequireExactCanonicalSource(t *testing.T) {
	ref := model.RefByID("queue-source", model.Natural64(0))
	doc := Document{ID: ref.ID, Type: "issue", Project: "p", Fields: map[string]any{"status": "todo", "priority": 0, "blocked": true},
		Links: []Link{{Kind: "waits-for", To: model.RefByID("spawner", model.Natural64(0)), Metadata: &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}}}}
	createdAt := time.Date(2026, 11, 4, 7, 0, 0, 0, time.UTC)
	startedAt, closedAt := createdAt.Add(time.Minute), createdAt.Add(2*time.Minute)
	doc.CreatedAt, doc.StartedAt, doc.ClosedAt = &createdAt, &startedAt, &closedAt
	canonical, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	rows := func(body []byte) []driver.DerivedRow {
		t.Helper()
		plan, err := PrepareSourceRecord(driver.Record{Ref: ref, Kind: "bead", Body: body})
		if err != nil {
			t.Fatal(err)
		}
		out, err := plan.SelectionRows("queue-source-inc", model.Natural64(1))
		if err != nil || len(out) != 3 {
			t.Fatalf("projection: %d %v", len(out), err)
		}
		return out
	}
	control := rows(canonical)
	queue := control[0].Index.Queue
	if queue == nil || queue.LeasePresent || queue.Version != driver.QueueFactsVersion || !queue.Blocked || !queue.CreatedAt.Equal(createdAt) || len(queue.Links) != 1 || queue.Links[0].Metadata.Gate != "any-children" || !queue.Links[0].Metadata.AlsoBlocks {
		t.Fatalf("canonical source lost scheduling facts: %+v", queue)
	}
	for name, raw := range map[string]string{
		"explicit_null":    strings.TrimSuffix(string(canonical), "}") + `,"lease":null}`,
		"duplicate_lease":  strings.TrimSuffix(string(canonical), "}") + `,"lease":null,"lease":null}`,
		"miscased_lease":   strings.TrimSuffix(string(canonical), "}") + `,"Lease":null}`,
		"unknown_property": strings.TrimSuffix(string(canonical), "}") + `,"futureLease":false}`,
		"noncanonical":     " " + string(canonical),
		"duplicate_time":   strings.TrimSuffix(string(canonical), "}") + `,"createdAt":"2026-11-04T07:00:00Z"}`,
		"miscased_time":    strings.TrimSuffix(string(canonical), "}") + `,"CreatedAt":"2026-11-04T07:00:00Z"}`,
	} {
		t.Run(name, func(t *testing.T) {
			got := rows([]byte(raw))
			if got[0].Index == nil || got[0].Index.Queue != nil {
				t.Fatal("unknown/legacy lease syntax certified absent")
			}
			for i := range got {
				if !bytes.Equal(control[i].Value, got[i].Value) {
					t.Fatal("optional queue annotation changed logical projection bytes")
				}
			}
		})
	}
	withoutCreation := doc
	withoutCreation.CreatedAt = nil
	rawWithoutCreation, err := json.Marshal(withoutCreation)
	if err != nil {
		t.Fatal(err)
	}
	if got := rows(rawWithoutCreation); got[0].Index == nil || got[0].Index.Queue == nil || !got[0].Index.Queue.CreatedAt.IsZero() {
		t.Fatal("source without a canonical creation instant changed its legacy queue envelope")
	}
	wrongBlocked := doc
	wrongBlocked.Fields = map[string]any{"status": "todo", "priority": 0, "blocked": "yes"}
	rawWrongBlocked, err := json.Marshal(wrongBlocked)
	if err != nil {
		t.Fatal(err)
	}
	if got := rows(rawWrongBlocked); got[0].Index == nil || got[0].Index.Queue != nil {
		t.Fatal("nonboolean blocked source entered queue acceleration")
	}
	doc.Lease = &ClaimLease{Principal: model.PrincipalRef{Kind: "user", ID: "lease-owner"}, ExpiresAt: time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC), TtlMs: model.Natural64(60000), Epoch: model.Natural64(1)}
	leased, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	withLease := rows(leased)
	if withLease[0].Index.Queue == nil || !withLease[0].Index.Queue.LeasePresent {
		t.Fatal("canonical live lease attested absent")
	}
	// Lease-only publications refresh the source digest even though existing
	// logical sort/link values do not change.
	if control[0].Index.Source.Digest == withLease[0].Index.Source.Digest || !bytes.Equal(control[0].Value, withLease[0].Value) {
		t.Fatal("lease-only source binding was lost or changed existing sort bytes")
	}
	withLease[0].Index.Queue.Links[0].Metadata.Gate = "all-children"
	if control[0].Index.Queue.Links[0].Metadata.Gate != "any-children" || doc.Links[0].Metadata.Gate != "any-children" {
		t.Fatal("queue producer aliases source or another publication")
	}
}

func TestQueueProjectionSurvivesCanonicalAgentClaim(t *testing.T) {
	h := setup(t)
	w := h.writer()
	created := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := created.Results[0].Ref
	agent := model.PrincipalRef{Kind: "agent", ID: "queue-agent", Sponsor: &by,
		Caveats: map[string]any{"refs": []model.Ref{model.RefByID(ref.ID, model.Natural64(0))}}}
	m := meta(2)
	m.Method = "bead.claim"
	claimed, err := w.Apply(context.Background(), agent, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{})}}, m)
	if err != nil {
		t.Fatal(err)
	}
	r := h.read(t)
	records, err := r.Get(context.Background(), []model.Ref{ref})
	if err != nil || len(records) != 1 || records[0].Revision.String() != claimed.Revision.String() {
		t.Fatalf("committed claim source: %+v %v", records, err)
	}
	doc, err := DecodeDocument(records[0].Body)
	if err != nil || doc.Lease == nil || doc.Lease.Principal.ID != agent.ID {
		t.Fatalf("actual claim lease: %+v %v", doc.Lease, err)
	}
	rows, err := r.DerivedRows(context.Background(), driver.DerivedSortKey, ref.ID)
	if err != nil || len(rows) != 1 || rows[0].Index == nil || rows[0].Index.Queue == nil || !rows[0].Index.Queue.LeasePresent ||
		rows[0].Index.Source.Digest != driver.SelectionDigest(records[0].Body) || rows[0].Index.Source.Revision.String() != records[0].Revision.String() {
		t.Fatalf("canonical Writer claim lost exact queue lease/source binding: %+v %v", rows, err)
	}
}
