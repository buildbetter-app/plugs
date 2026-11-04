package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// QueueProjectionReader is an internal candidate-source capability, not an
// authorized list, Select mode, ready predicate, or counted/ranked ready tree.
// It returns the COMPLETE current committed thin graph. Core evaluates actual
// readiness before rotating priority groups and still authorizes/fences claims.
// A WriteTx requires Head and reads committed state, excluding its overlay.
// Meta, temporal coverage, all live source headers and all logical sort/link
// annotations must share one snapshot (one statement for READ COMMITTED).
// Every source and row is revalidated; a ready marker alone is insufficient.
// Unknown/legacy metadata refuses acceleration, permitting the exact old path.
// This O(N) census bounds no executor work and changes no public selection cap.
type QueueProjectionReader interface {
	QueueProjection(context.Context) (QueueProjection, error)
	// Images uses the SAME committed view and excludes transaction overlays.
	// Callers verify each returned source token/digest against the projection.
	QueueProjectionImages(context.Context, View, []model.Ref) ([]Record, error)
}

const QueueFactsVersion = 1

// QueueFacts is optional, independently versioned annotation metadata. It is
// absent on legacy rows and never changes their logical Key/Value. Trusted core
// produces it only from an exact canonical source. Absence is NOT lease=false.
type QueueFacts struct {
	Version      uint8
	LeasePresent bool
	Links        []QueueLink
}

type QueueLink struct {
	Kind     string
	To       model.Ref
	Metadata *model.LinkMetadata
}

type QueueBead struct {
	Bead   SelectionBead
	Source SelectionSource
	Facts  QueueFacts
}

type QueueProjection struct {
	View  View
	Beads []QueueBead
}

// QueueSourceHeader comes from a complete, independent live-record census,
// with the actual record token checked against its thin source header. Storage
// does not reconstruct source bodies to supply it.
type QueueSourceHeader struct {
	Key      string
	Revision model.Revision
	Digest   string
}

func QueueUnavailable(reason string) error {
	return fmt.Errorf("%w: queue projection %s", ErrSelectionUnavailable, reason)
}

// QueueProjectionRanges declares the two whole logical input domains without
// parsing opaque keys or inventing a source-boundary authorization token.
func QueueProjectionRanges() (lower, upper []string) {
	for _, table := range []DerivedTable{DerivedSortKey, DerivedReferenceIndex} {
		key, _ := EncodeLockKey(LockKey{Kind: LockDerived, Derived: &DerivedRowKey{Table: table, Key: "!"}})
		key = strings.TrimSuffix(key, "!")
		end := []byte(key)
		for i := len(end) - 1; i >= 0; i-- {
			if end[i] < 255 {
				end[i]++
				end = end[:i+1]
				break
			}
		}
		lower, upper = append(lower, key), append(upper, string(end))
	}
	return
}

// CompleteQueueProjection proves a bidirectional census, including zero-link
// envelopes and hidden/deleted/duplicate sources. It consumes only snapshot
// inputs; no candidate, output page or source-image scan certifies coverage.
func CompleteQueueProjection(ctx context.Context, view View, ready bool, headers []QueueSourceHeader, rows []DerivedSelectionRow) (QueueProjection, error) {
	zero := QueueProjection{}
	if !ready || view.At == nil || view.At.Validate() != nil || view.Incarnation == "" || !view.Branch.IsZero() {
		return zero, QueueUnavailable("complete current temporal coverage unavailable")
	}
	at, err := view.At.Uint64()
	if err != nil {
		return zero, QueueUnavailable("unsupported snapshot token")
	}
	sources := make(map[string]QueueSourceHeader, len(headers))
	for _, header := range headers {
		ReportReadWork(ctx, ReadSelectorKeys, 1)
		revision, err := header.Revision.Uint64()
		if _, duplicate := sources[header.Key]; duplicate || header.Key == "" || err != nil || revision > at || header.Digest == "" {
			return zero, QueueUnavailable("incomplete or duplicate live source header")
		}
		sources[header.Key] = header
	}
	beads, links := map[string]*SelectionIndex{}, map[string]*SelectionIndex{}
	seen := map[DerivedRowKey]bool{}
	for _, row := range rows {
		ReportReadWork(ctx, ReadSelectorKeys, 1)
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		index := row.Row.Index
		if index == nil {
			return zero, QueueUnavailable("unannotated logical input")
		}
		if err := row.Validate(); err != nil || index.Incarnation != view.Incarnation || seen[index.Row] {
			return zero, QueueUnavailable("invalid or duplicate logical row envelope")
		}
		seen[index.Row] = true
		if index.Source.Key.Kind != LockRecord {
			continue // independent derived-source facts are not bead graph rows
		}
		id, err := EncodeLockKey(index.Source.Key)
		header, live := sources[id]
		if err != nil || !live || !index.Source.Present || header.Revision.String() != index.Source.Revision.String() || header.Digest != index.Source.Digest {
			return zero, QueueUnavailable("source revision, presence or digest mismatch")
		}
		var value any
		if index.Bead != nil {
			if beads[id] != nil || index.Queue == nil || index.Queue.Version != QueueFactsVersion {
				return zero, QueueUnavailable("duplicate bead or unknown queue facts")
			}
			beads[id], value = index, index.Bead
		} else if index.Facts.Kind == SelectionLinkFact {
			if links[id] != nil {
				return zero, QueueUnavailable("duplicate complete link envelope")
			}
			links[id], value = index, index.Facts
		} else {
			return zero, QueueUnavailable("unexpected logical input family")
		}
		encoded, err := json.Marshal(value)
		if err != nil || !bytes.Equal(encoded, row.Row.Value) {
			return zero, QueueUnavailable("annotation and logical value disagree")
		}
	}
	if len(beads) != len(sources) || len(links) != len(sources) {
		return zero, QueueUnavailable("incomplete bead or link source census")
	}
	result := QueueProjection{View: view, Beads: make([]QueueBead, 0, len(beads))}
	for id, index := range beads {
		ReportReadWork(ctx, ReadSelectorKeys, 1)
		facts := links[id].Facts.Rows
		if len(index.Queue.Links) != len(facts) {
			return zero, QueueUnavailable("queue links differ from complete envelope")
		}
		for i, fact := range facts {
			ReportReadWork(ctx, ReadSelectorKeys, 1)
			link := index.Queue.Links[i]
			if fact.Ref == nil || !SelectionSameRef(*fact.Ref, index.Bead.Ref) || fact.Link == nil || fact.Link.Kind != link.Kind || !SelectionSameRef(fact.Link.To, link.To) {
				return zero, QueueUnavailable("queue link source or target mismatch")
			}
		}
		copy := (DerivedRow{Index: index}).Clone().Index
		result.Beads = append(result.Beads, QueueBead{Bead: *copy.Bead, Source: copy.Source, Facts: *copy.Queue})
	}
	sort.Slice(result.Beads, func(i, j int) bool {
		ReportReadWork(ctx, ReadSelectorKeys, 1)
		return result.Beads[i].Bead.Ref.ID < result.Beads[j].Bead.Ref.ID
	})
	return result, ctx.Err()
}
