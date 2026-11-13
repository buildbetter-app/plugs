package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// A declaration belongs to exactly the image in t.records. Its presence means
// an explicit Parent; graph also distinguishes complete-empty Links from absent.
type sourceDeclaration struct {
	parentKey string
	graph     bool
	links     []sourceLink
}

func hierarchyReady(h *hierarchyIndex) bool    { return h != nil && len(h.roots) != 0 }
func sourceLinksReady(h *sourceLinkIndex) bool { return h != nil && len(h.roots) != 0 }

func (t *transaction) validateSourcePut(record driver.Record) (string, error) {
	key, err := recordKey(record.Ref)
	if err != nil {
		return "", err
	}
	if err = t.writable(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref}); err != nil {
		return "", err
	}
	if record.Kind == "" || !json.Valid(record.Body) {
		return "", fmt.Errorf("memory: record requires kind and valid JSON body")
	}
	if kind, exists := t.base.kinds[key]; exists && kind != record.Kind {
		return "", fmt.Errorf("memory: changing a record kind is unsupported")
	}
	if previous, ok := t.records[key]; ok && previous != nil && previous.Kind != record.Kind {
		return "", fmt.Errorf("memory: changing a staged record kind is unsupported")
	}
	return key, nil
}

func (t *transaction) PutHierarchyRecord(ctx context.Context, record driver.Record, parent *model.Ref) error {
	return t.putSourceRecord(ctx, record, parent, nil, false)
}

func (t *transaction) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	return t.putSourceRecord(ctx, record, parent, links, true)
}

func (t *transaction) putSourceRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink, graph bool) error {
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := driver.ValidateGraphSourceRecord(record, parent, links); err != nil {
		return err
	}
	key, err := t.validateSourcePut(record)
	if err != nil {
		return err
	}
	if err = t.writable(driver.GraphBlockingSentinelLock); err != nil {
		return err
	}
	if !graph && sourceLinksReady(t.base.sourceLinks) {
		return fmt.Errorf("%w: certified outgoing links require a complete graph source declaration", driver.ErrUnsupported)
	}
	declaration := sourceDeclaration{graph: graph}
	if parent != nil {
		declaration.parentKey, err = recordKey(*parent)
		if err != nil {
			return err
		}
	}
	// Validate and detach all arguments before replacing any staged component.
	declaration.links = make([]sourceLink, len(links))
	for i, link := range links {
		declaration.links[i] = sourceLink{kind: link.Kind, target: link.To}
	}
	t.stageSourceRecord(key, record, &declaration)
	return nil
}

func (t *transaction) stageSourceRecord(key string, record driver.Record, declaration *sourceDeclaration) {
	record = copyRecord(record)
	record.Ref.Branch = model.Natural64(0)
	t.replaceSourceDeclaration(key, declaration)
	t.records[key] = &record
}

func removeSourceKey(keys []string, key string) []string {
	if i, found := slices.BinarySearch(keys, key); found {
		return slices.Delete(keys, i, i+1)
	}
	return keys
}

func insertSourceKey(keys []string, key string) []string {
	i, found := slices.BinarySearch(keys, key)
	if found {
		return keys
	}
	return slices.Insert(keys, i, key)
}

// These buckets are transaction-owned. Readers do not rebuild a whole root or
// scan all staged images to find matching sources; they seek requested buckets.
func (t *transaction) replaceSourceDeclaration(key string, next *sourceDeclaration) {
	if old, ok := t.sourceDeclarations[key]; ok {
		if old.parentKey != "" {
			t.stagedParents[old.parentKey] = removeSourceKey(t.stagedParents[old.parentKey], key)
		}
		for _, link := range old.links {
			bucket, _ := sourceLinkKey(link)
			t.stagedIncoming[bucket] = removeSourceKey(t.stagedIncoming[bucket], key)
		}
		delete(t.sourceDeclarations, key)
	}
	if next == nil {
		return
	}
	if t.sourceDeclarations == nil {
		t.sourceDeclarations = make(map[string]sourceDeclaration)
		t.stagedParents = make(map[string][]string)
		t.stagedIncoming = make(map[sourceLinkBucket][]string)
	}
	t.sourceDeclarations[key] = *next
	if next.parentKey != "" {
		t.stagedParents[next.parentKey] = insertSourceKey(t.stagedParents[next.parentKey], key)
	}
	for _, link := range next.links {
		bucket, _ := sourceLinkKey(link)
		t.stagedIncoming[bucket] = insertSourceKey(t.stagedIncoming[bucket], key)
	}
}

// Called against the private next state, under Head and before the final lease
// check/publication. A failure leaves all committed records and roots untouched.
func (t *transaction) publishSourceIndexes(next *state) error {
	current := t.store.state
	head, err := current.meta.Head.Uint64()
	if err != nil {
		return err
	}
	at, err := next.meta.Head.Uint64()
	if err != nil {
		return err
	}
	h, links := current.hierarchy, current.sourceLinks
	hReady, linksReady := hierarchyReady(h), sourceLinksReady(links)
	parentComplete, graphComplete := true, true
	parentDeclared, graphDeclared := false, false
	puts := make([]hierarchySource, 0, len(t.sourceDeclarations))
	linkPuts := make([]sourceLinkSource, 0, len(t.sourceDeclarations))
	deletes := make([]string, 0)
	for key, record := range t.records {
		kind := current.kinds[key]
		if record != nil {
			kind = record.Kind
		}
		if kind != "bead" {
			continue
		}
		declaration, typed := t.sourceDeclarations[key]
		if typed || hReady || linksReady {
			if err = t.writable(driver.GraphBlockingSentinelLock); err != nil {
				return err
			}
			if _, bumped := t.bumps[keyID(driver.GraphBlockingSentinelLock)]; !bumped {
				return fmt.Errorf("memory: typed source publication requires graph/blocking bump")
			}
		}
		if record == nil {
			deletes = append(deletes, key)
			continue
		}
		if !typed {
			parentComplete, graphComplete = false, false
			continue
		}
		parentDeclared = true
		puts = append(puts, hierarchySource{parentKey: declaration.parentKey, record: next.records[key]})
		if declaration.graph {
			graphDeclared = true
			linkPuts = append(linkPuts, sourceLinkSource{record: next.records[key], links: declaration.links})
		} else {
			graphComplete = false
		}
	}
	if hReady && !parentComplete || linksReady && !graphComplete {
		return fmt.Errorf("%w: incomplete typed source publication", driver.ErrUnsupported)
	}
	// Source-free history (notably ontology bootstrap) is not unknown bead
	// coverage. Prove current source absence under Head; never infer historical
	// completeness or certify a populated graph from its staged final images.
	empty := true
	if !hReady || !linksReady {
		for _, record := range current.records {
			if record.Kind == "bead" {
				empty = false
				break
			}
		}
	}
	activateParent := !hReady && empty && parentComplete && parentDeclared
	activateLinks := !linksReady && empty && graphComplete && graphDeclared
	if t.restore != nil {
		// Both snapshot and history imports certify only the complete current
		// image at H. Older imported revisions keep the unbuilt fallback.
		h, links = newHierarchyIndex(next.meta.Incarnation), newSourceLinkIndex(next.meta.Incarnation)
		hReady, linksReady = false, false
	}
	if hReady || activateParent {
		var root *hierarchyRoot
		if hReady {
			root, err = h.rootAt(current.meta.Incarnation, 0, head)
		} else {
			root = newHierarchyRoot(at)
			if t.restore == nil {
				root = newHierarchyRoot(head)
				h, err = h.certify(root)
			}
		}
		if err != nil {
			return err
		}
		root, err = applyHierarchyPublication(root, at, puts, deletes)
		if err != nil {
			return err
		}
		h, err = h.certify(root)
		if err != nil {
			return err
		}
	}
	if linksReady || activateLinks {
		var root *sourceLinkRoot
		if linksReady {
			root, err = links.rootAt(current.meta.Incarnation, 0, head)
		} else {
			root = newSourceLinkRoot(at)
			if t.restore == nil {
				root = newSourceLinkRoot(head)
				links, err = links.certify(root)
			}
		}
		if err != nil {
			return err
		}
		root, err = applyLinkPublication(root, at, linkPuts, deletes)
		if err != nil {
			return err
		}
		links, err = links.certify(root)
		if err != nil {
			return err
		}
	}
	next.hierarchy, next.sourceLinks = h, links
	return nil
}

var _ driver.HierarchySourceWriter = (*transaction)(nil)
var _ driver.GraphSourceWriter = (*transaction)(nil)

// Group a publication's changes before replacing buckets. A large restore
// sorts once per affected bucket instead of shifting it once per source.
func applyHierarchyPublication(root *hierarchyRoot, at uint64, puts []hierarchySource, deletes []string) (*hierarchyRoot, error) {
	next := &hierarchyRoot{revision: at, sources: maps.Clone(root.sources), children: maps.Clone(root.children)}
	changed := make(map[string]bool, len(puts)+len(deletes))
	affected := make(map[string][]string)
	remove := func(key string) {
		changed[key] = true
		if old, found := next.sources[key]; found && old.parentKey != "" {
			if _, exists := affected[old.parentKey]; !exists {
				affected[old.parentKey] = nil
			}
		}
		delete(next.sources, key)
	}
	for _, key := range deletes {
		remove(key)
	}
	for _, source := range puts {
		key, err := recordKey(source.record.Ref)
		revision, revisionErr := source.record.Revision.Uint64()
		if err != nil || revisionErr != nil || revision > at || driver.ValidateHierarchyRecord(source.record, nil) != nil || source.parentKey == key {
			return nil, fmt.Errorf("%w: invalid hierarchy publication image", driver.ErrUnsupported)
		}
		remove(key)
		source.record = copyRecord(source.record)
		next.sources[key] = source
		if source.parentKey != "" {
			affected[source.parentKey] = append(affected[source.parentKey], key)
		}
	}
	for parent, added := range affected {
		keys := make([]string, 0, len(root.children[parent])+len(added))
		for _, previous := range root.children[parent] {
			key, err := recordKey(previous.record.Ref)
			if err != nil {
				return nil, err
			}
			if !changed[key] {
				keys = append(keys, key)
			}
		}
		keys = append(keys, added...)
		sort.Strings(keys)
		bucket := make([]hierarchySource, len(keys))
		for i, key := range keys {
			bucket[i] = next.sources[key]
		}
		if len(bucket) == 0 {
			delete(next.children, parent)
		} else {
			next.children[parent] = bucket
		}
	}
	return next, nil
}

func applyLinkPublication(root *sourceLinkRoot, at uint64, puts []sourceLinkSource, deletes []string) (*sourceLinkRoot, error) {
	next := &sourceLinkRoot{revision: at, sources: maps.Clone(root.sources), incoming: maps.Clone(root.incoming)}
	changed := make(map[string]bool, len(puts)+len(deletes))
	affected := make(map[sourceLinkBucket][]string)
	remove := func(key string) {
		changed[key] = true
		if old, found := next.sources[key]; found {
			for _, link := range old.links {
				bucket, _ := sourceLinkKey(link)
				if _, exists := affected[bucket]; !exists {
					affected[bucket] = nil
				}
			}
		}
		delete(next.sources, key)
	}
	for _, key := range deletes {
		remove(key)
	}
	for _, source := range puts {
		key, err := recordKey(source.record.Ref)
		revision, revisionErr := source.record.Revision.Uint64()
		if err != nil || revisionErr != nil || revision > at || driver.ValidateHierarchyRecord(source.record, nil) != nil {
			return nil, fmt.Errorf("%w: invalid outgoing publication image", driver.ErrUnsupported)
		}
		buckets := make(map[sourceLinkBucket]bool, len(source.links))
		for _, link := range source.links {
			bucket, err := sourceLinkKey(link)
			if err != nil {
				return nil, err
			}
			buckets[bucket] = true
		}
		remove(key)
		source.record = copyRecord(source.record)
		source.links = slices.Clone(source.links)
		next.sources[key] = source
		for bucket := range buckets {
			affected[bucket] = append(affected[bucket], key)
		}
	}
	for bucket, added := range affected {
		keys := make([]string, 0, len(root.incoming[bucket])+len(added))
		for _, key := range root.incoming[bucket] {
			if !changed[key] {
				keys = append(keys, key)
			}
		}
		keys = append(keys, added...)
		sort.Strings(keys)
		if len(keys) == 0 {
			delete(next.incoming, bucket)
		} else {
			next.incoming[bucket] = keys
		}
	}
	return next, nil
}
