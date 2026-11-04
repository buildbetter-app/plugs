package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadySortKey is an atomic Writer projection, not a readiness/permission fact.
// Revision is the bead mutation's allocated commit revision. RankedAt states
// when the pure classifier evaluated the moving bucket; it is not a promise
// that an unimplemented periodic refresh has run.
type ReadySortKey struct {
	Bucket    int            `json:"bucket"`
	Rank      int            `json:"rank"`
	CreatedAt time.Time      `json:"createdAt"`
	RankedAt  time.Time      `json:"rankedAt"`
	Revision  model.Revision `json:"revision"`
}

// readySortSource is trusted, pure ontology lowering. It cannot supply a
// creation instant or a revision and never receives a transaction or writer.
type readySortSource interface {
	ReadySortKey(Document, time.Time) (bucket int, rank int, known bool, err error)
}

func ReadySortKeyRowKey(id string) (string, error) {
	if _, err := driver.NewBeadSentinel(id); err != nil {
		return "", err
	}
	return "ready/" + id, nil
}

// DecodeReadySortKey is a read-only codec. A reader must bind the returned ID,
// CreatedAt and Revision to its bead in the same pinned snapshot, and apply its
// adopted staleness policy to RankedAt. This function never refreshes a row.
func DecodeReadySortKey(row driver.DerivedRow) (string, ReadySortKey, error) {
	var wire struct {
		Bucket    *int            `json:"bucket"`
		Rank      *int            `json:"rank"`
		CreatedAt *time.Time      `json:"createdAt"`
		RankedAt  *time.Time      `json:"rankedAt"`
		Revision  *model.Revision `json:"revision"`
	}
	id, prefixed := strings.CutPrefix(row.Key, "ready/")
	key, err := ReadySortKeyRowKey(id)
	if !prefixed || err != nil || key != row.Key {
		return "", ReadySortKey{}, fmt.Errorf("invalid ready sort key")
	}
	dec := json.NewDecoder(bytes.NewReader(row.Value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil {
		return "", ReadySortKey{}, err
	}
	if err := dec.Decode(new(any)); err != io.EOF || wire.Bucket == nil || wire.Rank == nil || wire.CreatedAt == nil || wire.RankedAt == nil || wire.Revision == nil {
		return "", ReadySortKey{}, fmt.Errorf("incomplete ready sort row")
	}
	if *wire.Bucket < 0 || *wire.Rank < 0 || wire.CreatedAt.IsZero() || wire.RankedAt.IsZero() {
		return "", ReadySortKey{}, fmt.Errorf("invalid ready sort row")
	}
	if revision, err := wire.Revision.Uint64(); err != nil || revision == 0 {
		return "", ReadySortKey{}, fmt.Errorf("invalid ready sort revision")
	}
	return id, ReadySortKey{Bucket: *wire.Bucket, Rank: *wire.Rank, CreatedAt: wire.CreatedAt.UTC(), RankedAt: wire.RankedAt.UTC(), Revision: *wire.Revision}, nil
}

type readySortMutation struct {
	key   driver.DerivedRowKey
	value *ReadySortKey
}

type readySortPlan []readySortMutation

func (w *Writer) prepareReadySortKeys(p *prepared) error {
	source, supported := w.cfg.ontology.(readySortSource)
	ids := make([]string, 0, len(p.changed))
	for id := range p.changed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	stamp := w.cfg.now().UTC()
	for _, id := range ids {
		value := p.writes[id]
		if value.kind == "memory" || !supported && !value.deleted {
			continue
		}
		key, err := ReadySortKeyRowKey(id)
		if err != nil {
			return err
		}
		mutation := readySortMutation{key: driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: key}}
		if !value.deleted {
			// A by-value Document still contains maps and pointers. Detach it
			// before calling the classifier so it cannot edit the pending body.
			doc, err := clone(value.doc)
			if err != nil {
				return err
			}
			bucket, rank, known, err := source.ReadySortKey(doc, stamp)
			if err != nil {
				return err
			}
			if known {
				createdAt, exists := value.doc.CreationTime()
				if !exists || stamp.IsZero() || bucket < 0 || rank < 0 {
					return failure("invalid_params", "ready sort projection requires a genuine creation instant and valid rank")
				}
				mutation.value = &ReadySortKey{Bucket: bucket, Rank: rank, CreatedAt: createdAt, RankedAt: stamp}
			}
		}
		p.readySort = append(p.readySort, mutation)
	}
	return nil
}

func (p readySortPlan) locks() []driver.LockKey {
	keys := make([]driver.LockKey, 0, len(p))
	for _, mutation := range p {
		key := mutation.key
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	return keys
}

// publish runs after Head allocated the record revision and before the same
// transaction's Commit. Errors roll back body, history, derived rows and receipt.
func (p readySortPlan) publish(ctx context.Context, tx driver.WriteTx, revision model.Revision) error {
	if len(p) == 0 {
		return nil
	}
	remove, add := []driver.DerivedRow{}, []driver.DerivedRow{}
	for _, mutation := range p {
		row := driver.DerivedRow{Key: mutation.key.Key}
		if mutation.value == nil {
			remove = append(remove, row)
			continue
		}
		value := *mutation.value
		value.Revision = revision
		var err error
		row.Value, err = json.Marshal(value)
		if err != nil {
			return err
		}
		add = append(add, row)
	}
	return tx.PutDerived(ctx, driver.DerivedSortKey, remove, add)
}
