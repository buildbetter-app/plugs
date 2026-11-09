package memory

import (
	"context"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *Store) buildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder) error {
	if decode == nil {
		return fmt.Errorf("source index backfill requires a core decoder")
	}
	value, err := s.Write(ctx, lease)
	if err != nil {
		return err
	}
	defer value.Close()
	if _, err = value.Lock(ctx, driver.LockSet{{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate}}); err != nil {
		return err
	}
	if _, err = value.Head(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	t := value.(*transaction)
	if err = t.check(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	current := s.state
	s.mu.Unlock()
	// The committed snapshot is immutable and graph/Head remain held by t.
	// Decode and sort private replacement roots without Store.mu so the normal
	// lease heartbeat (and expiry notification) can progress throughout a build.
	head, err := current.meta.Head.Uint64()
	if err != nil {
		return err
	}
	// Keep fresh native-restore targets durably empty.
	if head == 0 {
		return nil
	}
	h, links := current.hierarchy, current.sourceLinks
	hReady, linksReady := hierarchyReady(h), sourceLinksReady(links)
	if hReady {
		if _, err := h.rootAt(current.meta.Incarnation, 0, head); err != nil || h.through != head {
			return fmt.Errorf("%w: gapped hierarchy backfill", driver.ErrUnsupported)
		}
	}
	if linksReady {
		if _, err := links.rootAt(current.meta.Incarnation, 0, head); err != nil || links.through != head {
			return fmt.Errorf("%w: gapped outgoing-link backfill", driver.ErrUnsupported)
		}
	}
	if hReady && linksReady {
		return nil
	}
	parentRoot, linkRoot := newHierarchyRoot(head), newSourceLinkRoot(head)
	// These maps ARE the new immutable index representation, not an additional
	// corpus/decoded-document buffer. Only one decoder image is live at a time.
	for id, record := range current.records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Kind != "bead" {
			continue
		}
		metadata, err := decode(copyRecord(record))
		if err != nil {
			return err
		}
		if err := driver.ValidateGraphSourceRecord(record, metadata.Parent, metadata.Links); err != nil {
			return err
		}
		key, err := recordKey(record.Ref)
		revision, revisionErr := record.Revision.Uint64()
		if err != nil || key != id || revisionErr != nil || revision > head {
			return fmt.Errorf("%w: backfill source envelope mismatch", driver.ErrUnsupported)
		}
		if !hReady {
			source := hierarchySource{record: copyRecord(record)}
			if metadata.Parent != nil {
				source.parentKey, _ = recordKey(*metadata.Parent)
				parentRoot.children[source.parentKey] = append(parentRoot.children[source.parentKey], source)
			}
			parentRoot.sources[id] = source
		}
		if !linksReady {
			source := sourceLinkSource{record: copyRecord(record)}
			seen := map[sourceLinkBucket]bool{}
			for _, link := range metadata.Links {
				fact := sourceLink{kind: link.Kind, target: link.To}
				bucket, _ := sourceLinkKey(fact)
				if !seen[bucket] {
					source.links = append(source.links, fact)
					linkRoot.incoming[bucket] = append(linkRoot.incoming[bucket], id)
					seen[bucket] = true
				}
			}
			linkRoot.sources[id] = source
		}
	}
	if !hReady {
		for _, bucket := range parentRoot.children {
			sort.Slice(bucket, func(i, j int) bool {
				a, _ := recordKey(bucket[i].record.Ref)
				b, _ := recordKey(bucket[j].record.Ref)
				return a < b
			})
		}
		h, err = newHierarchyIndex(current.meta.Incarnation).certify(parentRoot)
		if err != nil {
			return err
		}
	}
	if !linksReady {
		for _, bucket := range linkRoot.incoming {
			sort.Strings(bucket)
		}
		links, err = newSourceLinkIndex(current.meta.Incarnation).certify(linkRoot)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return err
	}
	if s.head != t || s.state.meta != current.meta {
		return fmt.Errorf("%w: source backfill head changed", driver.ErrUnsupported)
	}
	// Response finalization can replace state without advancing Head. Preserve
	// such concurrent metadata rather than restoring the snapshot's older copy.
	next := *s.state
	next.hierarchy, next.sourceLinks = h, links
	s.state = &next
	return nil
}

var _ driver.GraphSourceIndexBuilder = (*Store)(nil)
