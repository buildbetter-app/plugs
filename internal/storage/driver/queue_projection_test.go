package driver

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func queueContractInput(t *testing.T) (View, []QueueSourceHeader, []DerivedSelectionRow) {
	t.Helper()
	at := model.Natural64(7)
	view := View{Incarnation: "queue-contract", Branch: model.Natural64(0), At: &at}
	var headers []QueueSourceHeader
	var rows []DerivedSelectionRow
	for _, id := range []string{"a", "hidden"} {
		ref := model.RefByID(id, model.Natural64(0))
		key := LockKey{Kind: LockRecord, Ref: &ref}
		encoded, _ := EncodeLockKey(key)
		source := SelectionSource{Key: key, Revision: at, Present: true, Digest: SelectionDigest([]byte(id))}
		headers = append(headers, QueueSourceHeader{Key: encoded, Revision: at, Digest: source.Digest})
		project := "p"
		bead := &SelectionBead{Ref: ref, Type: "issue", Project: &project, Status: "todo"}
		for _, table := range []DerivedTable{DerivedSortKey, DerivedReferenceIndex} {
			index := &SelectionIndex{Version: SelectionIndexVersion, Incarnation: view.Incarnation, Branch: view.Branch,
				Row: DerivedRowKey{Table: table, Key: id}, Source: source}
			var value any
			if table == DerivedSortKey {
				index.Bead, index.Queue = bead, &QueueFacts{Version: QueueFactsVersion}
				value = bead
			} else {
				index.Facts = &SelectionFacts{Kind: SelectionLinkFact, Rows: []SelectionFact{}}
				value = index.Facts
			}
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			index.ValueDigest = SelectionDigest(raw)
			rows = append(rows, DerivedSelectionRow{Table: table, Row: DerivedRow{Key: id, Value: raw, Index: index}})
		}
	}
	return view, headers, rows
}

func TestQueueProjectionRequiresIndependentCompleteInputs(t *testing.T) {
	for name, mutate := range map[string]func(*View, *[]QueueSourceHeader, *[]DerivedSelectionRow){
		"missing_hidden_both_families": func(_ *View, _ *[]QueueSourceHeader, rows *[]DerivedSelectionRow) { *rows = (*rows)[:2] },
		"missing_zero_link_envelope":   func(_ *View, _ *[]QueueSourceHeader, rows *[]DerivedSelectionRow) { *rows = (*rows)[:3] },
		"duplicate_source":             func(_ *View, h *[]QueueSourceHeader, _ *[]DerivedSelectionRow) { *h = append(*h, (*h)[0]) },
		"missing_source":               func(_ *View, h *[]QueueSourceHeader, _ *[]DerivedSelectionRow) { *h = (*h)[:1] },
		"duplicate_row":                func(_ *View, _ *[]QueueSourceHeader, r *[]DerivedSelectionRow) { *r = append(*r, (*r)[0].Clone()) },
		"legacy":                       func(_ *View, _ *[]QueueSourceHeader, r *[]DerivedSelectionRow) { (*r)[0].Row.Index.Queue = nil },
		"unknown_version":              func(_ *View, _ *[]QueueSourceHeader, r *[]DerivedSelectionRow) { (*r)[0].Row.Index.Queue.Version++ },
		"untyped":                      func(_ *View, _ *[]QueueSourceHeader, r *[]DerivedSelectionRow) { (*r)[0].Row.Index = nil },
		"stale_revision": func(_ *View, _ *[]QueueSourceHeader, r *[]DerivedSelectionRow) {
			(*r)[0].Row.Index.Source.Revision = model.Natural64(6)
		},
		"future_source": func(_ *View, h *[]QueueSourceHeader, _ *[]DerivedSelectionRow) { (*h)[0].Revision = model.Natural64(8) },
		"wrong_digest": func(_ *View, h *[]QueueSourceHeader, _ *[]DerivedSelectionRow) {
			(*h)[0].Digest = SelectionDigest([]byte("changed"))
		},
		"wrong_value":       func(_ *View, _ *[]QueueSourceHeader, r *[]DerivedSelectionRow) { (*r)[0].Row.Value = []byte(`{}`) },
		"wrong_incarnation": func(v *View, _ *[]QueueSourceHeader, _ *[]DerivedSelectionRow) { v.Incarnation = "other" },
		"missing_snapshot":  func(v *View, _ *[]QueueSourceHeader, _ *[]DerivedSelectionRow) { v.At = nil },
		"extra_queue_link": func(_ *View, _ *[]QueueSourceHeader, r *[]DerivedSelectionRow) {
			(*r)[0].Row.Index.Queue.Links = []QueueLink{{Kind: "blocks", To: model.RefByID("hidden", model.Natural64(0))}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			v, headers, rows := queueContractInput(t)
			mutate(&v, &headers, &rows)
			got, err := CompleteQueueProjection(context.Background(), v, true, headers, rows)
			if !errors.Is(err, ErrSelectionUnavailable) || len(got.Beads) != 0 {
				t.Fatalf("incomplete input authorized a projection: %+v %v", got, err)
			}
		})
	}
	v, headers, rows := queueContractInput(t)
	got, err := CompleteQueueProjection(context.Background(), v, true, headers, rows)
	if err != nil || len(got.Beads) != 2 || got.Beads[1].Bead.Ref.ID != "hidden" {
		t.Fatalf("complete control: %+v %v", got, err)
	}
	if _, err := CompleteQueueProjection(context.Background(), v, false, headers, rows); !errors.Is(err, ErrSelectionUnavailable) {
		t.Fatal("uncertified control admitted")
	}
	if empty, err := CompleteQueueProjection(context.Background(), v, true, nil, nil); err != nil || len(empty.Beads) != 0 {
		t.Fatalf("certified empty domain: %+v %v", empty, err)
	}
}

func TestQueueFactsOwnershipAndWireRoundtrip(t *testing.T) {
	_, _, rows := queueContractInput(t)
	row := rows[0].Row
	row.Index.Queue.LeasePresent = true
	row.Index.Queue.Links = []QueueLink{{Kind: "waits-for", To: model.RefByID("hidden", model.Natural64(0)), Metadata: &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}}}
	if err := row.Index.Validate(DerivedSortKey, row); err != nil {
		t.Fatal(err)
	}
	for _, codec := range []string{"json", "gob"} {
		t.Run(codec, func(t *testing.T) {
			var copy DerivedRow
			if codec == "json" {
				raw, err := json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(raw, &copy); err != nil {
					t.Fatal(err)
				}
			} else {
				var raw bytes.Buffer
				if err := gob.NewEncoder(&raw).Encode(row); err != nil {
					t.Fatal(err)
				}
				if err := gob.NewDecoder(&raw).Decode(&copy); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(row, copy) {
				t.Fatalf("%s lost queue metadata", codec)
			}
			copy = copy.Clone()
			copy.Index.Queue.Links[0].Metadata.Gate = "all-children"
			copy.Index.Queue.Links[0].To.ID = "changed"
			if row.Index.Queue.Links[0].Metadata.Gate != "any-children" || row.Index.Queue.Links[0].To.ID != "hidden" || !bytes.Equal(copy.Value, row.Value) {
				t.Fatal("queue clone aliases input or changes logical values")
			}
		})
	}
}
