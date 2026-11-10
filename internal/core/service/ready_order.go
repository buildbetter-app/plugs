package service

import (
	"context"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/readyorder"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	readyOrderMaxLag = time.Minute
	readyOrderPrefix = "ready/"
)

// The consumer uses the pipeline-owned persisted shape and codec. Semantic
// bucket/rank classification and P74 freshness remain B01's responsibility.
type readyOrderKey = pipeline.ReadySortKey

// ReadySortKey supplies the pipeline-owned rank producer with the pure B01
// bucket/rank rule. Unknown legacy creation time remains unknown; membership
// and persistence are the caller's responsibility. The supplied instant is
// the producer's existing clock, never an independent wall-clock read.
func ReadySortKey(doc pipeline.Document, at time.Time) (bucket int, rank int, known bool, err error) {
	if at.IsZero() {
		return 0, 0, false, failure("internal_error", "ready order requires a rank clock")
	}
	createdAt, known := doc.CreationTime()
	if !known {
		return 0, 0, false, nil
	}
	p, err := priority(doc)
	if err != nil {
		return 0, 0, false, err
	}
	key := liveReadyOrderKey(createdAt, at, p, model.Natural64(0))
	return key.Bucket, key.Rank, true, nil
}

// ReadySortKey also exposes the pure rule through the existing ontology
// adapter. Pipeline can consume this optional capability without importing
// service and creating a package cycle.
func (o *Ontology) ReadySortKey(doc pipeline.Document, at time.Time) (bucket int, rank int, known bool, err error) {
	return ReadySortKey(doc, at)
}

func liveReadyOrderKey(createdAt, at time.Time, priority int, revision model.Revision) readyOrderKey {
	key := readyorder.Classify(priority, createdAt, at)
	return readyOrderKey{Bucket: key.Bucket, Rank: key.Rank, CreatedAt: key.CreatedAt, RankedAt: at, Revision: revision}
}

func readyOrderValue(key readyOrderKey) readyorder.Key {
	return readyorder.Key{Bucket: key.Bucket, Rank: key.Rank, CreatedAt: key.CreatedAt}
}

// readyOrderKeys reads the same pinned snapshot as the complete graph. Missing
// or outdated projections are repaired in the answer from real creation time;
// this read never publishes a derived changeset or invents a creation instant.
func readyOrderKeys(ctx context.Context, r driver.Reader, graph map[string]storedBead, now time.Time) (map[string]readyOrderKey, error) {
	stored := map[string]readyOrderKey{}
	rows, err := r.DerivedRows(ctx, driver.DerivedSortKey, readyOrderPrefix)
	if err != nil {
		return nil, mapped(err)
	}
	for _, row := range rows {
		id, key, err := pipeline.DecodeReadySortKey(row)
		if err != nil || (key.Bucket != 0 && key.Bucket != 1) {
			return nil, failure("internal_error", "invalid persisted ready order")
		}
		if _, exists := stored[id]; exists {
			return nil, failure("internal_error", "duplicate persisted ready order")
		}
		stored[id] = key
	}
	keys := make(map[string]readyOrderKey, len(graph))
	for id, row := range graph {
		createdAt, known := row.doc.CreationTime()
		if !known {
			return nil, &model.Error{Message: "ready order requires persisted creation time", Data: model.ErrorData{Code: "internal_error", Details: map[string]any{"reason": "ready_creation_time_unavailable"}}}
		}
		p, err := priority(row.doc)
		if err != nil {
			return nil, err
		}
		key, exists := stored[id]
		if exists && key.CreatedAt.Equal(createdAt) && key.Revision.String() == row.record.Revision.String() && !key.RankedAt.After(now) && now.Sub(key.RankedAt) <= readyOrderMaxLag {
			// Freshness alone cannot bless a corrupt bucket/rank pair. Validate
			// it at the stored clock before permitting P74's bounded lag.
			expected := liveReadyOrderKey(createdAt, key.RankedAt, p, row.record.Revision)
			if key.Bucket != expected.Bucket || key.Rank != expected.Rank {
				return nil, failure("internal_error", "persisted ready order disagrees with its source")
			}
			keys[id] = key
			continue
		}
		keys[id] = liveReadyOrderKey(createdAt, now, p, row.record.Revision)
	}
	return keys, nil
}

func sortReadyIDs(ids []string, keys map[string]readyOrderKey) {
	sort.Slice(ids, func(i, j int) bool {
		a, b := keys[ids[i]], keys[ids[j]]
		return readyorder.Compare(readyOrderValue(a), ids[i], readyOrderValue(b), ids[j]) < 0
	})
}
