package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// SourceProjectionPlan owns the final source image and its complete projection
// write census. Prepare it before Lock/Head, merge LockKeys into the transaction
// census, Publish in that transaction, and bump Sentinels before Commit. A
// rolled-back publication leaves neither a source nor a projection behind.
type SourceProjectionPlan struct {
	record   driver.Record
	deleted  bool
	doc      *Document
	presence projectionPresence
	links    []driver.SourceLink
	keys     []driver.DerivedRowKey
}

// SourceSelectionRows are stable opaque identities. Changing a sort value does
// not change its row identity; both empty fact sets are explicitly represented.
func SourceSelectionRows(ref model.Ref) ([]driver.DerivedRowKey, error) {
	if err := driver.ValidateHierarchyRef(ref); err != nil {
		return nil, err
	}
	key := ref.ID
	return []driver.DerivedRowKey{
		{Table: driver.DerivedSortKey, Key: key},
		{Table: driver.DerivedAttribute, Key: key},
		{Table: driver.DerivedReferenceIndex, Key: key},
	}, nil
}

// projectionProject records exact source presence in the same JSON decode as
// the document. Document.Project's compatibility string alone cannot separate
// an omitted property from an explicit empty string. Null is accepted by the
// ordinary document decoder, but is not a certified string value.
type projectionProject struct {
	value          string
	present, valid bool
}

func (p *projectionProject) UnmarshalJSON(raw []byte) error {
	p.present = true
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		p.valid = false
		return nil
	}
	if err := json.Unmarshal(raw, &p.value); err != nil {
		return err
	}
	p.valid = true
	return nil
}

type projectionPresence struct {
	project             *string
	bead, labels, links bool
}

func decodeProjectionDocument(body []byte) (Document, projectionPresence, error) {
	var doc Document
	var project projectionProject
	presence := projectionPresence{bead: true, labels: true, links: true}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return Document{}, presence, fmt.Errorf("invalid bead document")
	}
	// Decode each property once, preserving the ordinary Document decoder's
	// case-insensitive/duplicate-field acceptance. Such spellings cannot attest
	// the exact, case-sensitive source properties used by selection predicates.
	properties := []struct {
		name        string
		destination any
	}{
		{"id", &doc.ID}, {"type", &doc.Type}, {"project", &project},
		{"parent", &doc.Parent}, {"fields", &doc.Fields}, {"metadata", &doc.Metadata},
		{"labels", &doc.Labels}, {"comments", &doc.Comments}, {"links", &doc.Links},
		{"attachments", &doc.Attachments}, {"lease", &doc.Lease},
		{"createdAt", &doc.CreatedAt}, {"startedAt", &doc.StartedAt}, {"closedAt", &doc.ClosedAt},
	}
	seen := map[string]bool{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return Document{}, presence, err
		}
		name, ok := key.(string)
		if !ok {
			return Document{}, presence, fmt.Errorf("invalid bead property")
		}
		var ignored json.RawMessage
		var destination any = &ignored
		for _, property := range properties {
			if !strings.EqualFold(name, property.name) {
				continue
			}
			destination = property.destination
			if name != property.name || seen[property.name] {
				switch property.name {
				case "id", "type", "project", "parent", "fields":
					presence.bead = false
				case "labels":
					presence.labels = false
				case "links":
					presence.links = false
				}
			}
			seen[property.name] = true
			break
		}
		if err := decoder.Decode(destination); err != nil {
			return Document{}, presence, err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return Document{}, presence, err
	}
	doc.Project = project.value
	if doc.ID == "" || doc.Type == "" || doc.Fields == nil {
		return Document{}, presence, fmt.Errorf("invalid bead document")
	}
	if err := doc.validateLifecycleTimes(); err != nil {
		return Document{}, presence, err
	}
	if err := validateLifecycleTimeMembers(body); err != nil {
		return Document{}, presence, err
	}
	if err := validateParent(doc.Parent); err != nil {
		return Document{}, presence, err
	}
	for _, comment := range doc.Comments {
		if err := comment.Validate(); err != nil {
			return Document{}, presence, err
		}
	}
	if project.valid {
		value := project.value
		presence.project = &value
	}
	presence.bead = presence.bead && (!project.present || project.valid)
	return doc, presence, nil
}

func PrepareSourceRecord(record driver.Record) (*SourceProjectionPlan, error) {
	record.Body = bytes.Clone(record.Body)
	p := &SourceProjectionPlan{record: record}
	if record.Kind != "bead" {
		return p, nil
	}
	doc, presence, err := decodeProjectionDocument(record.Body)
	if err != nil {
		return nil, failure("ontology_guard_failed", "invalid hierarchy source document")
	}
	doc, links, err := graphSourceMetadata(record, doc)
	if err != nil {
		return nil, err
	}
	p.doc, p.presence, p.links = &doc, presence, links
	p.keys, err = SourceSelectionRows(record.Ref)
	return p, err
}

func PrepareSourceDelete(ref model.Ref, kind string) (*SourceProjectionPlan, error) {
	p := &SourceProjectionPlan{record: driver.Record{Ref: ref, Kind: kind}, deleted: true}
	if kind != "bead" {
		return p, nil
	}
	var err error
	p.keys, err = SourceSelectionRows(ref)
	return p, err
}

func (p *SourceProjectionPlan) Sentinels() []driver.Sentinel {
	if len(p.keys) == 0 {
		return nil
	}
	bead, _ := driver.NewBeadSentinel(p.record.Ref.ID)
	closure, _ := driver.NewClosureSentinel(p.record.Ref.ID)
	return []driver.Sentinel{bead, closure, driver.SentinelGraphBlocking}
}

func (p *SourceProjectionPlan) LockKeys() []driver.LockKey {
	ref := p.record.Ref
	keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}}
	for _, row := range p.keys {
		row := row
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &row})
	}
	for _, sentinel := range p.Sentinels() {
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	return keys
}

// PutSelectionDerived preserves typed annotations when the driver advertises
// W3's optional writer. Otherwise it publishes ordinary logical bytes without
// certifying selector coverage. It acquires no locks or transaction of its own.
func PutSelectionDerived(ctx context.Context, tx driver.WriteTx, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	// Deletes carry exact row identities, not potentially stale annotations.
	removed := make([]driver.DerivedRow, len(del))
	for i, row := range del {
		removed[i] = driver.DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
	}
	if typed, ok := tx.(driver.SelectionIndexWriter); ok {
		return typed.PutSelectionDerived(ctx, table, removed, add)
	}
	ordinary := make([]driver.DerivedRow, len(add))
	for i, row := range add {
		ordinary[i] = driver.DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
	}
	return tx.PutDerived(ctx, table, removed, ordinary)
}

func (p *SourceProjectionPlan) Publish(ctx context.Context, tx driver.WriteTx) error {
	return p.PublishInIncarnation(ctx, tx, tx.Snapshot().Incarnation)
}

// PublishInIncarnation is for an admitted restore's returned metadata. Storage
// still owns admission; a transaction snapshot retains its old incarnation.
func (p *SourceProjectionPlan) PublishInIncarnation(ctx context.Context, tx driver.WriteTx, incarnation string) error {
	if p.deleted {
		if err := tx.Delete(ctx, p.record.Ref); err != nil {
			return err
		}
		for _, key := range p.keys {
			if err := PutSelectionDerived(ctx, tx, key.Table, []driver.DerivedRow{{Key: key.Key}}, nil); err != nil {
				return err
			}
		}
		return nil
	}
	if p.doc == nil {
		return tx.Put(ctx, p.record)
	}
	// Capture the committed revision/miss before staging. The final bytes are
	// the prepared owned image; drivers recheck the complete overlay at Commit
	// and restamp a changed source to the actual publication revision H+1.
	states, err := tx.Observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &p.record.Ref}})
	if err != nil {
		return err
	}
	if len(states) != 1 {
		return fmt.Errorf("source publication observation was incomplete")
	}
	if err := putDecodedGraphSource(ctx, tx, p.record, *p.doc, p.links); err != nil {
		return err
	}
	rows, err := p.SelectionRows(incarnation, states[0].Revision)
	if err != nil {
		return err
	}
	revision, err := sourcePublicationRevision(tx)
	if err != nil {
		return err
	}
	if err := BindReadySelectionRows(*p.doc, revision, rows); err != nil {
		return err
	}
	for i, row := range rows {
		if err := PutSelectionDerived(ctx, tx, p.keys[i].Table, nil, []driver.DerivedRow{row}); err != nil {
			return err
		}
	}
	return nil
}

// SelectionRows is the startup/restore decoder seam. It returns independently
// owned metadata for the exact prepared source image, never inferred history.
func (p *SourceProjectionPlan) SelectionRows(incarnation string, revision model.Revision) ([]driver.DerivedRow, error) {
	if p.doc == nil || p.deleted {
		return nil, nil
	}
	doc := p.doc
	// Drivers store the resolved branch explicitly. Use that same wire shape
	// for logical projection values before and after a startup decode; an
	// omitted zero branch must not become an apparent logical-value rewrite.
	ref := model.RefByID(p.record.Ref.ID, model.Natural64(0))
	source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: revision, Present: true, Digest: driver.SelectionDigest(p.record.Body)}
	bead := driver.SelectionBead{Ref: ref, Type: doc.Type}
	if doc.Parent != nil {
		parent := *doc.Parent
		bead.Parent = &parent
	}
	if p.presence.project != nil {
		project := *p.presence.project
		bead.Project = &project
	}
	// Preserve presence from the exact decoded source. Null and wrong types
	// withhold the bead annotation; accepted source publication remains valid.
	typed := p.presence.bead
	for _, field := range []struct {
		name   string
		column driver.SelectionColumn
	}{
		{"status", driver.SelectionStatus}, {"title", driver.SelectionTitle},
		{"template", driver.SelectionTemplate}, {"priority", driver.SelectionPriority},
	} {
		value, present := doc.Fields[field.name]
		if !present {
			bead.Absent = append(bead.Absent, field.column)
			continue
		}
		switch field.column {
		case driver.SelectionStatus:
			v, ok := value.(string)
			bead.Status, typed = v, typed && ok
		case driver.SelectionTitle:
			v, ok := value.(string)
			bead.Title, typed = v, typed && ok
		case driver.SelectionTemplate:
			v, ok := value.(bool)
			bead.Template, typed = v, typed && ok
		case driver.SelectionPriority:
			v, ok := value.(json.Number)
			n, err := v.Int64()
			if err != nil && ok {
				// Kernel-valid integral JSON spellings such as 1.0 and 1e0
				// are exact integers; never round via float64.
				if u, e := model.Natural(v.String()).Uint64(); e == nil && u <= uint64(1<<63-1) {
					n, err = int64(u), nil
				}
			}
			bead.Priority, typed = n, typed && ok && err == nil
		}
	}
	labels := driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{}}
	for _, label := range doc.Labels {
		ref := ref
		labels.Rows = append(labels.Rows, driver.SelectionFact{Kind: driver.SelectionLabelFact, Ref: &ref, Name: label, Value: label})
	}
	links := driver.SelectionFacts{Kind: driver.SelectionLinkFact, Rows: []driver.SelectionFact{}}
	for _, link := range p.links {
		ref, link := ref, link
		links.Rows = append(links.Rows, driver.SelectionFact{Kind: driver.SelectionLinkFact, Ref: &ref, Link: &link})
	}
	values := []any{bead, labels, links}
	rows := make([]driver.DerivedRow, len(values))
	for i, value := range values {
		body, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		row := driver.DerivedRow{Key: p.keys[i].Key, Value: body}
		index := &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: p.keys[i], Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(body), Source: source}
		switch i {
		case 0:
			if typed {
				index.Bead = &bead
				// Only byte-exact canonical documents attest queue facts. Legacy
				// aliases, duplicate/miscased lease properties, nulls, unknown
				// members or noncanonical envelopes keep Queue nil. Queue ordering
				// also needs a real creation instant and a boolean blocked field;
				// legacy/malformed values therefore remain on the exact fallback.
				// Ordinary projections and their logical Values remain unchanged.
				canonical, err := json.Marshal(doc)
				if err == nil && bytes.Equal(canonical, p.record.Body) {
					blocked := false
					queueValid := true
					if raw, present := doc.Fields["blocked"]; present {
						var ok bool
						blocked, ok = raw.(bool)
						if !ok {
							queueValid = false
						}
					}
					queue := &driver.QueueFacts{Version: driver.QueueFactsVersion, LeasePresent: doc.Lease != nil, Blocked: blocked}
					if doc.CreatedAt != nil {
						queue.CreatedAt = doc.CreatedAt.UTC()
					}
					if queueValid {
						for _, link := range doc.Links {
							targetKind, targetErr := projectionLinkTargetKind(link)
							if targetErr != nil {
								queueValid = false
								break
							}
							value := driver.QueueLink{Kind: link.Kind, TargetKind: targetKind, To: link.To}
							if link.Metadata != nil {
								metadata := *link.Metadata
								value.Metadata = &metadata
							}
							queue.Links = append(queue.Links, value)
						}
					}
					if queueValid {
						index.Queue = queue
					}
				}
				row.Index = index
			}
		case 1:
			if p.presence.labels {
				index.Facts = &labels
				row.Index = index
			}
		case 2:
			if p.presence.links {
				index.Facts = &links
				row.Index = index
			}
		}
		if err := row.Index.Validate(p.keys[i].Table, row); err != nil {
			// Optional representation limits never become source-admission
			// limits. Publishing the logical row without an annotation keeps
			// the write valid and leaves selection coverage unavailable.
			row.Index = nil
		}
		rows[i] = row.Clone()
	}
	if err := BindReadySelectionRows(*doc, revision, rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// projectionLinkTargetKind is deliberately decoded through the canonical link
// JSON. The W3 source branch predates the typed target-kind member, while the
// main composition carries it on pipeline.Link; this adapter preserves the
// field when present without teaching the storage projection a second link
// schema or accepting a caller-supplied value.
func projectionLinkTargetKind(link Link) (string, error) {
	raw, err := json.Marshal(link)
	if err != nil {
		return "", err
	}
	var wire struct {
		TargetKind string `json:"targetKind"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return "", err
	}
	return wire.TargetKind, nil
}

type projectionDelta struct {
	sources     []*SourceProjectionPlan
	locks       []driver.LockKey
	sentinels   []driver.Sentinel
	remove, add []driver.DerivedRow
}

// Read the exact union once instead of making an optional closure read for
// every changed bead. AncestorPrefix ends in a separator outside its encoded
// alphabet, so these descendant ranges cannot overlap or select other sources.
// A missing capability or failed union keeps the established per-source path:
// optional coverage must not turn a supported ordinary write into a refusal.
func (p *prepared) projectionAncestorRows(ctx context.Context, ids []string) map[string][]driver.DerivedRow {
	reader, ok := p.snapshot.reader.(driver.DerivedPrefixReader)
	if !ok {
		return nil
	}
	prefixes := make([]string, 0, len(ids))
	byPrefix := make(map[string]string, len(ids))
	groups := make(map[string][]driver.DerivedRow, len(ids))
	for _, id := range ids {
		if p.writes[id].kind != "bead" {
			continue
		}
		prefix, err := AncestorPrefix(id)
		if err != nil {
			return nil // Preserve validation at the original per-source boundary.
		}
		prefixes = append(prefixes, prefix)
		byPrefix[prefix], groups[id] = id, nil
	}
	if len(prefixes) < 2 {
		return nil
	}
	sort.Strings(prefixes)
	rows, err := reader.DerivedRowsForPrefixes(ctx, driver.DerivedAncestorClosure, prefixes)
	if err != nil {
		return nil
	}
	previous := ""
	for i, row := range rows {
		encoded, _, separated := strings.Cut(row.Key, ".")
		id, selected := byPrefix[encoded+"."]
		if !separated || !selected || i > 0 && row.Key <= previous {
			return nil // An invalid optional union cannot certify a refresh.
		}
		previous = row.Key
		groups[id] = append(groups[id], row)
	}
	return groups
}

// prepareProjections observes old rows AND new misses before the complete lock
// census is acquired. Every changed source refreshes all source-bound rows,
// even if only comment/metadata/lease bytes changed and logical sort did not.
func (p *prepared) prepareProjections(ctx context.Context) error {
	delta := &projectionDelta{}
	keys := map[string]driver.LockKey{}
	sentinels := map[driver.Sentinel]bool{}
	addKey := func(key driver.LockKey) error {
		encoded, err := driver.EncodeLockKey(key)
		if err == nil {
			keys[encoded] = key
		}
		return err
	}
	old, final := map[string]driver.DerivedRow{}, map[string]driver.DerivedRow{}
	ids := make([]string, 0, len(p.changed))
	for id := range p.changed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ancestorRows := p.projectionAncestorRows(ctx, ids)
	for _, id := range ids {
		pending := p.writes[id]
		var plan *SourceProjectionPlan
		var err error
		if pending.deleted {
			plan, err = PrepareSourceDelete(pending.ref, pending.kind)
		} else {
			body, e := pending.body()
			if e != nil {
				return e
			}
			plan, err = PrepareSourceRecord(driver.Record{Ref: pending.ref, Kind: pending.kind, Body: body})
		}
		if err != nil {
			return err
		}
		delta.sources = append(delta.sources, plan)
		for _, key := range plan.LockKeys() {
			if err := addKey(key); err != nil {
				return err
			}
		}
		for _, sentinel := range plan.Sentinels() {
			sentinels[sentinel] = true
		}
		if pending.kind != "bead" {
			continue
		}
		prefix, err := AncestorPrefix(id)
		if err != nil {
			return err
		}
		rows, batched := ancestorRows[id]
		if batched {
			if err := ctx.Err(); err != nil {
				return err
			}
		} else {
			rows, err = p.snapshot.reader.DerivedRows(ctx, driver.DerivedAncestorClosure, prefix)
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Optional coverage cannot introduce a new ordinary-write refusal.
			// The changed source makes any unrefreshed annotation unavailable.
			continue
		}
		// pending.before/revision are the actual old snapshot image loaded
		// during preparation and rechecked under its source lock before Head.
		before := driver.Record{Ref: pending.ref, Kind: pending.kind, Revision: pending.revision, Body: pending.before}
		legacy, err := p.refreshableLegacyAncestorSet(ctx, before, rows)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if !legacy && !canRefreshAncestorProjection(row, before, p.snapshot.Snapshot()) {
				continue
			}
			old[row.Key] = row
			if !pending.deleted {
				final[row.Key] = row
			}
		}
	}
	if p.hierarchyDelta != nil {
		for _, row := range p.hierarchyDelta.remove {
			old[row.Key] = row
			delete(final, row.Key)
		}
		// The hierarchy planner already proved these exact affected chains.
		// Retained self/ancestor rows need the same final source annotation as
		// newly introduced pairs, including on unchanged cold descendants.
		for _, row := range p.hierarchyDelta.complete {
			final[row.Key] = row
		}
	}
	for key, row := range old {
		if _, ok := final[key]; !ok {
			delta.remove = append(delta.remove, row)
		}
	}
	for _, row := range final {
		delta.add = append(delta.add, row)
	}
	for _, rows := range [][]driver.DerivedRow{delta.remove, delta.add} {
		for _, row := range rows {
			descendant, _, _, err := DecodeAncestorRow(row)
			if err != nil {
				return err
			}
			ref := model.RefByID(descendant, model.Natural64(0))
			if err := addKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}); err != nil {
				return err
			}
			if err := addKey(ancestorLock(row.Key)); err != nil {
				return err
			}
			sentinel, err := driver.NewClosureSentinel(descendant)
			if err != nil {
				return err
			}
			sentinels[sentinel] = true
		}
	}
	for sentinel := range sentinels {
		if err := addKey(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}); err != nil {
			return err
		}
		delta.sentinels = append(delta.sentinels, sentinel)
	}
	delta.locks = sortedKeys(keys)
	observed := make([]driver.LockKey, 0, len(delta.locks))
	for _, key := range delta.locks {
		// Publication serializes and bumps graph/blocking without reading its
		// old snapshot value. Keep the update lock, and retain any genuine
		// semantic graph dependency already recorded during preparation.
		if key.Kind == driver.LockSentinel && key.Sentinel == driver.SentinelGraphBlocking {
			continue
		}
		observed = append(observed, key)
	}
	if _, err := p.snapshot.observe(ctx, observed); err != nil {
		return err
	}
	sort.Slice(delta.sentinels, func(i, j int) bool { return delta.sentinels[i] < delta.sentinels[j] })
	sort.Slice(delta.remove, func(i, j int) bool { return delta.remove[i].Key < delta.remove[j].Key })
	sort.Slice(delta.add, func(i, j int) bool { return delta.add[i].Key < delta.add[j].Key })
	p.projections = delta
	return nil
}

// canRefreshAncestorProjection permits only an already-complete projection
// bound to this exact OLD snapshot source. A graph-only rewrite can leave a
// non-nil but stale annotation; its logical membership must never be recertified
// by an unrelated comment/lease write. Unsupported envelopes remain untouched.
func canRefreshAncestorProjection(row driver.DerivedRow, source driver.Record, view driver.View) bool {
	index := row.Index
	if index == nil || index.Validate(driver.DerivedAncestorClosure, row) != nil ||
		index.Incarnation != view.Incarnation || index.Branch.String() != view.Branch.String() ||
		source.Kind != "bead" || len(source.Body) == 0 ||
		!index.Source.Present || index.Source.Key.Kind != driver.LockRecord || index.Source.Key.Ref == nil ||
		!driver.SelectionSameRef(*index.Source.Key.Ref, source.Ref) ||
		index.Source.Revision.String() != source.Revision.String() ||
		index.Source.Digest != driver.SelectionDigest(source.Body) ||
		index.Facts == nil || index.Facts.Kind != driver.SelectionAncestorFact || len(index.Facts.Rows) != 1 {
		return false
	}
	descendant, ancestor, depth, err := DecodeAncestorRow(row)
	if err != nil || descendant != source.Ref.ID {
		return false
	}
	fact := index.Facts.Rows[0]
	target := model.RefByID(ancestor, model.Natural64(0))
	return fact.Kind == driver.SelectionAncestorFact && fact.Ref != nil && fact.Target != nil &&
		driver.SelectionSameRef(*fact.Ref, source.Ref) && driver.SelectionSameRef(*fact.Target, target) && fact.Depth == depth
}

// PutAncestorProjections retains the established closure Key/Value encoding.
// The caller predeclares every closure key and descendant source lock. Physical
// reverse indexes remain driver-owned; this is not a second graph database.
func PutAncestorProjections(ctx context.Context, tx driver.WriteTx, remove, add []driver.DerivedRow) error {
	return PutAncestorProjectionsInIncarnation(ctx, tx, tx.Snapshot().Incarnation, remove, add)
}

func PutAncestorProjectionsInIncarnation(ctx context.Context, tx driver.WriteTx, incarnation string, remove, add []driver.DerivedRow) error {
	keys := map[string]driver.LockKey{}
	for _, row := range add {
		descendant, _, _, err := DecodeAncestorRow(row)
		if err != nil {
			return err
		}
		ref := model.RefByID(descendant, model.Natural64(0))
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	ordered := sortedKeys(keys)
	states, err := tx.Observe(ctx, ordered)
	if err != nil {
		return err
	}
	if len(states) != len(ordered) {
		return fmt.Errorf("closure source observation was incomplete")
	}
	sources := map[string]driver.LockState{}
	for i, state := range states {
		sources[ordered[i].Ref.ID] = state
	}
	rows := make([]driver.DerivedRow, len(add))
	for i, row := range add {
		descendant, ancestor, depth, err := DecodeAncestorRow(row)
		if err != nil {
			return err
		}
		state := sources[descendant]
		if !state.Present || state.Record == nil || state.Record.Kind != "bead" {
			return fmt.Errorf("closure projection has no live source")
		}
		ref, target := state.Record.Ref, model.RefByID(ancestor, model.Natural64(0))
		copy := driver.DerivedRow{Key: row.Key, Value: bytes.Clone(row.Value)}
		copy.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}, Incarnation: incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(row.Value), Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Revision: state.Revision, Present: true, Digest: driver.SelectionDigest(state.Record.Body)}, Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &target, Depth: depth}}}}
		if err := copy.Index.Validate(driver.DerivedAncestorClosure, copy); err != nil {
			return err
		}
		rows[i] = copy
	}
	return PutSelectionDerived(ctx, tx, driver.DerivedAncestorClosure, remove, rows)
}
