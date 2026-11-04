package readyindex

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (d *Delta) publication(revision model.Revision, removed, added []driver.DerivedRow) (driver.ReadyOrderPublication, error) {
	data := driver.ReadyOrderPublicationData{Incarnation: d.coverage.Incarnation, Base: *d.t.r.Snapshot().At, Revision: revision, Removed: removed, Added: added}
	ids := make([]string, 0, len(d.mutations))
	for id := range d.mutations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		data.Mutations = append(data.Mutations, d.mutations[id])
	}
	keys := make([]string, 0, len(d.t.before))
	for key := range d.t.before {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		data.Before = append(data.Before, d.t.before[key])
	}
	return driver.NewReadyOrderPublication(data)
}

type publicationImage struct {
	data driver.ReadyOrderPublicationData
	rows map[string]driver.LockState
}

func (r publicationImage) Snapshot() driver.View {
	base := r.data.Base
	return driver.View{Incarnation: r.data.Incarnation, At: &base, Branch: model.Natural64(0)}
}

func (r publicationImage) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		encoded, err := driver.EncodeLockKey(key)
		if err != nil {
			return nil, err
		}
		state, found := r.rows[encoded]
		if !found {
			return nil, unavailable("proof omitted a predeclared path")
		}
		states[i] = state
	}
	return states, nil
}

// Publication is a verified bounded transition. The SQL engine must also
// compare its sources with the real source/staging overlay, lock observations
// and fenced physical generation. This result alone is not a certificate.
type Publication struct {
	Data                               driver.ReadyOrderPublicationData
	Root                               driver.ReadyCommitment
	MarkerKey, RootKey, EligibilityKey string
	MarkerDigest, EligibilityDigest    string
	Digest                             string
}

// StagedSource describes the final staged source, retaining the old kind for
// deletions. Keys use the driver's canonical record lock encoding.
type StagedSource struct {
	Record *driver.Record
	Kind   string
}

// ValidateOverlay proves that the replay covers every staged bead mutation and
// exactly the Ready rows in the final transaction overlay. Other record kinds
// carry authority/control state at the same target Head; they cannot silently
// introduce a bead into this lifetime.
func (p Publication) ValidateOverlay(sources map[string]StagedSource, derived map[string]driver.LockState, acquired driver.LockSet, bumps map[string]driver.LockKey) error {
	mutations := map[string]driver.ReadyOrderMutation{}
	for _, m := range p.Data.Mutations {
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &m.Ref})
		if err != nil {
			return err
		}
		w, ok := sources[key]
		if !ok || w.Kind != "bead" || (m.Item == nil) != (w.Record == nil) || w.Record != nil && driver.SelectionDigest(w.Record.Body) != m.SourceDigest {
			return unavailable("source image differs from staged image")
		}
		mutations[key] = m
	}
	for key, w := range sources {
		if w.Kind == "bead" {
			if _, ok := mutations[key]; !ok {
				return unavailable("unmaintained staged bead")
			}
		}
	}
	expected := map[string]driver.LockState{}
	for _, row := range p.Data.Removed {
		key := lock(row.Key)
		id, _ := driver.EncodeLockKey(key)
		expected[id] = driver.LockState{Key: key}
	}
	for _, row := range p.Data.Added {
		key := lock(row.Key)
		id, _ := driver.EncodeLockKey(key)
		copy := row.Clone()
		expected[id] = driver.LockState{Key: key, Present: true, Derived: &copy}
	}
	count := 0
	for id, actual := range derived {
		if actual.Key.Kind != driver.LockDerived || actual.Key.Derived == nil || actual.Key.Derived.Table != driver.DerivedReadyState {
			continue
		}
		want, ok := expected[id]
		if !ok || want.Present != actual.Present || want.Present && (actual.Derived == nil || actual.Derived.Index != nil || !bytes.Equal(want.Derived.Value, actual.Derived.Value)) {
			return unavailable("untyped or altered Ready row")
		}
		count++
	}
	if count != len(expected) {
		return unavailable("incomplete staged Ready rows")
	}
	locks := map[string]bool{}
	for _, request := range acquired {
		id, _ := driver.EncodeLockKey(request.Key)
		locks[id] = request.Mode != driver.LockShare
	}
	for _, before := range p.Data.Before {
		id, _ := driver.EncodeLockKey(before.Key)
		if !locks[id] {
			return unavailable("undeclared Ready observation")
		}
	}
	global, _ := driver.NewReadySentinel("*", "*")
	globalKey := driver.LockKey{Kind: driver.LockSentinel, Sentinel: global}
	id, _ := driver.EncodeLockKey(globalKey)
	if !locks[id] {
		return unavailable("missing Ready publication fence")
	}
	if _, ok := bumps[id]; !ok {
		return unavailable("unbumped Ready publication fence")
	}
	return nil
}

// FinalImages includes unchanged declared paths so the SQL publisher checks
// the actual root and eligibility census, including a control-only commit.
func (p Publication) FinalImages() []driver.LockState {
	rows := map[string]driver.LockState{}
	for _, state := range p.Data.Before {
		rows[state.Key.Derived.Key] = state
	}
	for _, row := range p.Data.Removed {
		rows[row.Key] = driver.LockState{Key: lock(row.Key)}
	}
	for _, row := range p.Data.Added {
		copy := row.Clone()
		rows[row.Key] = driver.LockState{Key: lock(row.Key), Present: true, Derived: &copy}
	}
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]driver.LockState, 0, len(rows))
	for _, key := range keys {
		out = append(out, rows[key])
	}
	return out
}

// ValidatePublication independently replays the canonical delta using ONLY
// the declared before-images. It cannot fetch another path or trust proposed
// aggregate counts. This work scales with changed paths, never the store size.
func ValidatePublication(ctx context.Context, publication driver.ReadyOrderPublication) (Publication, error) {
	data, err := publication.Data()
	if err != nil {
		return Publication{}, err
	}
	image := publicationImage{data: data, rows: map[string]driver.LockState{}}
	for _, state := range data.Before {
		if state.Key.Kind != driver.LockDerived || state.Key.Derived == nil || state.Key.Derived.Table != driver.DerivedReadyState {
			return Publication{}, unavailable("proof contains a non-Ready observation")
		}
		key, err := driver.EncodeLockKey(state.Key)
		if err != nil {
			return Publication{}, err
		}
		if _, duplicate := image.rows[key]; duplicate {
			return Publication{}, unavailable("duplicate proof observation")
		}
		image.rows[key] = state
	}
	replay, err := Prepare(ctx, image, data.Mutations)
	if err != nil {
		return Publication{}, err
	}
	if !replay.Available() {
		return Publication{}, unavailable("proof predecessor is uncovered")
	}
	removed, added, err := replay.materialize(data.Revision)
	if err != nil {
		return Publication{}, err
	}
	same := func(a, b []driver.DerivedRow) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i].Key != b[i].Key || a[i].Index != nil || b[i].Index != nil || !bytes.Equal(a[i].Value, b[i].Value) {
				return false
			}
		}
		return true
	}
	if !same(removed, data.Removed) || !same(added, data.Added) || len(replay.t.before) != len(data.Before) {
		return Publication{}, unavailable("proposed Ready rows differ from canonical transition")
	}
	scope := scopeID(driver.ReadyOrderScope{})
	root := replay.t.nodes[nodeKey(scope, "")]
	if root == nil {
		return Publication{}, unavailable("proof omitted semantic root")
	}
	markerRow := replay.t.rows[markerKey]
	if markerRow == nil {
		return Publication{}, unavailable("proof omitted coverage marker")
	}
	var eligibility []byte
	if header := replay.t.rows[headerKey(scope)]; header != nil {
		eligibility = header.Value
	}
	// Decode independently to enforce the exact target binding even on a
	// control-only transition whose root remains byte-for-byte unchanged.
	var coverage marker
	if json.Unmarshal(markerRow.Value, &coverage) != nil || coverage.Incarnation != data.Incarnation || coverage.Through != data.Revision {
		return Publication{}, unavailable("proof target binding")
	}
	return Publication{Data: data, Root: root.Commitment, MarkerKey: markerKey, RootKey: nodeKey(scope, ""), EligibilityKey: headerKey(scope), MarkerDigest: driver.SelectionDigest(markerRow.Value), EligibilityDigest: driver.SelectionDigest(eligibility), Digest: publication.Digest()}, nil
}
