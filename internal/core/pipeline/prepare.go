package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type pending struct {
	ref       model.Ref
	kind      string
	doc       Document
	memory    *MemoryDocument
	rawMemory json.RawMessage
	before    json.RawMessage
	revision  model.Revision
	deleted   bool
	setBy     map[string]string
	replaced  *bool
	// allocatorPrefix/Expected are set only for IDs previewed from a
	// transaction-capable allocator. A parent with no prefix uses its internal
	// parent-only counter. The expected value is checked
	// again after the allocator fact lock is acquired before publication.
	allocatorPrefix   string
	allocatorExpected string
	allocatorChild    string
	allocatorHash     *driver.HashInput
}

func (p *pending) body() (json.RawMessage, error) {
	if p.kind == "memory" {
		if p.rawMemory != nil {
			return append(json.RawMessage(nil), p.rawMemory...), nil
		}
		if p.memory == nil {
			return nil, failure("internal_error", "memory write has no body")
		}
		body, err := json.Marshal(p.memory)
		return body, err
	}
	body, err := json.Marshal(p.doc)
	return body, err
}

func memoryMethod(method string) bool {
	return method == "memory.save" || method == "memory.delete"
}

type prepared struct {
	population *populationCache
	snapshot   *snapshotReader
	writes     map[string]*pending
	proposal   port.ProposedChange
	// Ordered images are private core input for typed close guard validation.
	ordered []port.Change
	closes  map[string]bool
	// refusal is deliberately kept out of ProposedChange. It may contain a
	// current revision token and can be released only after AuthorizeBefore has
	// covered every disclosed expectation target.
	refusal error
	// leaseRefusal is a lease refusal (live foreign claim; missing, foreign or
	// expired lease). Preparation stops at it, and it is released only after
	// AuthorizeBefore admits the operations prepared so far, so an
	// unauthorized caller learns nothing about the claim state of a bead it
	// may not modify.
	leaseRefusal      error
	release           *claimReleaseMode
	warnings          []model.Warning
	mutators          []string
	refs              []model.Ref
	changed           map[string]bool
	audit             *auditPlan
	allocatorCursors  map[string]uint64
	allocatorSeen     map[string]bool
	allocatorCounts   map[string]uint64
	beads             []driver.Record
	beadsLoaded       bool
	createdBeadIDs    []string
	maxHierarchyDepth int
	hierarchyEdits    []hierarchyEdit
	hierarchyDelta    *hierarchyDelta
	hierarchySources  *hierarchySources
	graphValidated    bool
	graphPreserved    bool
	checkpointReads   map[string]uint64
	readySort         readySortPlan
	source            *SourcePublication
	external          map[int]SourceOperation
}

func failure(code model.ErrorCode, message string) error {
	return &model.Error{Message: message, Data: model.ErrorData{Code: code, Retryable: code == "write_conflict" || code == "subscriber_unavailable" || code == "busy"}}
}

// preconditionFailure describes the selected CAS arm. Preparation keeps this
// refusal private until AuthorizeBefore covers its expectation target.
func preconditionFailure(reason, message string, refusing ...model.Ref) *model.Error {
	err := &model.Error{Message: message, Data: model.ErrorData{Code: "precondition_failed", Details: map[string]any{"reason": reason}}}
	if len(refusing) != 0 {
		err.Data.Details["ref"] = refusing[0]
	}
	return err
}

// preconditionRevisionFailure adds the token from the pinned pre-read only
// when the expected record exists.
func preconditionRevisionFailure(current model.Revision, refusing ...model.Ref) error {
	err := preconditionFailure("revision", "precondition failed", refusing...)
	err.Data.Details["currentRevision"] = current
	return err
}

func fieldName(path string) (string, error) {
	name, ok := strings.CutPrefix(path, "fields.")
	if !ok || name == "" || strings.ContainsAny(name, "./\x00") {
		return "", failure("plugin_patch_out_of_scope", "patch path must be fields.<name>")
	}
	return name, nil
}

func protectedName(name string) bool {
	switch name {
	case "ontology", "plugins", "plugin", "plugin_set", "plugin_lock", "plugin_config", "instance_config", "lock":
		return true
	}
	for _, prefix := range []string{"ontology/", "plugins/", "plugin/", "plugin_set/", "plugin_lock/", "plugin_config/", "instance_config/"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (p *prepared) resolve(ref model.Ref, aliases map[string]model.Ref) (model.Ref, error) {
	if err := ref.Validate(); err != nil {
		return model.Ref{}, failure("invalid_params", err.Error())
	}
	if !ref.IsID() {
		var ok bool
		ref, ok = aliases[ref.Key]
		if !ok {
			return model.Ref{}, failure("invalid_params", "reference key must name an earlier create")
		}
	}
	if ref.ID == "" || !ref.Branch.IsZero() {
		return model.Ref{}, failure("invalid_params", "only nonempty IDs on branch zero are supported")
	}
	if protectedName(ref.ID) {
		return model.Ref{}, failure("protected_record", "ordinary changes cannot target protected records")
	}
	return ref, nil
}

func (p *prepared) load(ctx context.Context, ref model.Ref) (*pending, error) {
	if prior, ok := p.writes[ref.ID]; ok {
		if prior.deleted {
			return nil, failure("not_found", "record was deleted in this batch")
		}
		return prior, nil
	}
	records, err := p.snapshot.Get(ctx, []model.Ref{ref})
	if err != nil {
		return nil, err
	}
	if len(records) != 1 {
		return nil, failure("not_found", "bead not found")
	}
	r := records[0]
	if protectedName(r.Kind) {
		return nil, failure("protected_record", "ordinary changes cannot target protected records")
	}
	if r.Kind != "bead" {
		return nil, failure("invalid_params", "unsupported record kind")
	}
	doc, err := DecodeDocument(r.Body)
	if err != nil {
		return nil, err
	}
	if doc.ID != ref.ID {
		return nil, failure("internal_error", "record identity differs from body")
	}
	pending := &pending{ref: ref, kind: "bead", doc: doc, before: append(json.RawMessage(nil), r.Body...), revision: r.Revision, setBy: map[string]string{}}
	p.writes[ref.ID] = pending
	return pending, nil
}

func (w *Writer) prepare(ctx context.Context, reader driver.Reader, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta, release *claimReleaseMode) (*prepared, error) {
	snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	if _, err := snapshot.observe(ctx, []driver.LockKey{driver.OntologySentinelLock, driver.PluginsSentinelLock}); err != nil {
		return nil, err
	}
	p := &prepared{population: &w.population, snapshot: snapshot, writes: map[string]*pending{}, release: release, proposal: port.ProposedChange{RequestID: meta.IdempotencyKey, Principal: by, BaseRevision: *reader.Snapshot().At, DryRun: meta.DryRun, Meta: meta, Changes: []port.Change{}}, warnings: []model.Warning{}, allocatorCursors: map[string]uint64{}, allocatorSeen: map[string]bool{}, allocatorCounts: map[string]uint64{}, maxHierarchyDepth: w.cfg.hierarchyMaxDepth}
	aliases := map[string]model.Ref{}
	for _, op := range cs.Ops {
		if op.Kind == model.OpClose {
			if meta.Method != "tx.apply" && !(meta.Method == "bead.close" && ctx.Value(sourceHandlerKey{}) != nil) {
				return nil, failure("invalid_params", "close operation requires tx.apply")
			}
			p.closes = map[string]bool{}
			break
		}
	}
	slotRef := ""
	if len(cs.Ops) == 0 {
		if meta.Method != "bead.claimNext" {
			return nil, failure("invalid_params", "empty change set is only valid for claimNext")
		}
	} else if isSlot(meta.Method) && !(meta.Method == "bead.close" && len(cs.Ops) == 1 && cs.Ops[0].Kind == model.OpClose && ctx.Value(sourceHandlerKey{}) != nil) {
		// A method is host-supplied lowering context, not a batch-wide bypass.
		if len(cs.Ops) < 1 || cs.Ops[0].Kind != model.OpPatch {
			return nil, failure("invalid_params", "slot method requires a patch first")
		}
		if len(cs.Ops) > 2 || (len(cs.Ops) == 2 && cs.Ops[1].Kind != model.OpComment) {
			return nil, failure("invalid_params", "slot method supports one patch and an optional reason comment")
		}
	}
	for _, op := range cs.Ops {
		if op.Kind == model.OpClose {
			value := *op.Close
			value.Expect = append([]model.Precondition{}, value.Expect...)
			op.Close = &value
		}
		var target model.Ref
		var current *pending
		var before json.RawMessage
		var err error
		if handle, _ := ctx.Value(sourceHandlerKey{}).(SourceOperationHandler); handle != nil {
			var active *SourceOperation
			op, active, err = handle(ctx, op, p.sourceCoreImage)
			if err != nil {
				return nil, err
			}
			if active != nil {
				if p.external == nil {
					p.external = map[int]SourceOperation{}
				}
				p.external[len(p.refs)] = *active
				p.refs = append(p.refs, active.Result.Ref)
				p.proposal.Changes = append(p.proposal.Changes, active.Change)
				if p.closes != nil {
					p.ordered = append(p.ordered, active.Change)
				}
				continue
			}
		}
		if op.Kind == model.OpCreate {
			c := op.Create
			if protectedName(c.Type) {
				return nil, failure("protected_record", "protected resources require their plan/apply path")
			}
			parent, err := p.resolveParent(ctx, c.Parent, aliases, "")
			if err != nil {
				return nil, err
			}
			c.Parent = parent
			if c.Type == "" {
				return nil, failure("invalid_params", "create requires a type")
			}
			if c.Key != "" {
				if _, exists := aliases[c.Key]; exists {
					return nil, failure("invalid_params", "duplicate create key")
				}
			}
			project := c.Project
			if project == "" {
				project = meta.Project
			}
			var id, allocatorPrefix, allocatorExpected, allocatorChild string
			var allocatorHash *driver.HashInput
			var e error
			createdAt := w.cfg.now().UTC()
			if parent != nil {
				id, allocatorPrefix, allocatorExpected, allocatorChild, e = p.nextChildAllocatorID(ctx, project, parent.ID, w.cfg.newID, w.cfg.newAllocatorChildID)
			} else {
				creator, principalErr := driver.PrincipalKey(by)
				if principalErr != nil {
					return nil, failure("invalid_params", principalErr.Error())
				}
				input := driver.HashInput{Title: hashField(c.Fields, "title"), Description: hashField(c.Fields, "description"), Creator: creator, Timestamp: createdAt}
				id, allocatorPrefix, allocatorExpected, allocatorHash, e = p.nextAllocatorID(ctx, project, input, w.cfg.newID, w.cfg.newAllocatorID)
			}
			if e != nil {
				return nil, e
			}
			if _, e := driver.NewBeadSentinel(id); e != nil {
				return nil, failure("invalid_params", "ID generator returned an invalid bead ID")
			}
			target = model.Ref{ID: id}
			if _, exists := p.writes[id]; exists {
				return nil, failure("invalid_params", "ID generator returned a duplicate batch ID")
			}
			if _, e := snapshot.Get(ctx, []model.Ref{target}); e != nil {
				return nil, e
			}
			// A generated collision is retried from a fresh attempt, never overwritten.
			key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &target})
			if snapshot.reads.states[key].Present {
				return nil, errStale
			}
			current = &pending{ref: target, kind: "bead", doc: Document{ID: id, Type: c.Type, Project: project, Parent: parent, Fields: map[string]any{}, CreatedAt: &createdAt}, setBy: map[string]string{"createdAt": "core"}, allocatorPrefix: allocatorPrefix, allocatorExpected: allocatorExpected, allocatorChild: allocatorChild, allocatorHash: allocatorHash}
			p.writes[id] = current
			p.createdBeadIDs = append(p.createdBeadIDs, id)
			if parent != nil {
				current.setBy["parent"] = "request"
			}
			p.hierarchyEdits = append(p.hierarchyEdits, hierarchyEdit{id: id, parent: parentID(parent), create: true})
			if err = applyFields(current, c.Fields, "request"); err != nil {
				return nil, err
			}
			if c.TargetStatus != nil {
				if _, present := c.Fields["status"]; present {
					return nil, failure("invalid_params", "targetStatus cannot be combined with fields.status")
				}
				selector, supported := w.cfg.ontology.(createStatuses)
				if !supported {
					return nil, failure("invalid_params", "ontology does not support targetStatus")
				}
				target, ok := selector.CreateStatus(project, c.Type, c.TargetStatus)
				if !ok {
					return nil, failure("invalid_params", "create target must name a declared nonterminal workflow status")
				}
				current.doc.Fields["status"] = target
				current.setBy["fields.status"] = "request"
			}
			if err = p.inheritLabels(ctx, current); err != nil {
				return nil, err
			}
			if c.Key != "" {
				aliases[c.Key] = target
			}
		} else {
			switch op.Kind {
			case model.OpPatch:
				target = op.Patch.Target
			case model.OpClose:
				target = op.Close.Target
			case model.OpComment:
				target = op.Comment.Target
			case model.OpLink, model.OpUnlink:
				target = op.Link.From
			case model.OpDelete:
				target = op.Delete.Target
			case model.OpSetMeta:
				target = op.SetMeta.Target
			case model.OpAttach:
				target = op.Attach.Target
			case model.OpReparent:
				target = op.Reparent.Target
			default:
				return nil, failure("invalid_params", "unsupported operation")
			}
			target, err = p.resolve(target, aliases)
			if err != nil {
				return nil, err
			}
			if (op.Kind == model.OpLink || op.Kind == model.OpUnlink) && strings.HasPrefix(target.ID, "workflow:") {
				return nil, failure("invalid_params", "workflow references require a supported workflow item operation")
			}
			if op.Kind == model.OpClose && canonicalWorkflowCloseTarget(target) {
				return nil, failure(model.CodeUnsupported, "workflow items cannot participate in durable close changesets")
			}
			current, err = p.load(ctx, target)
			if err != nil {
				return nil, err
			}
			before, err = json.Marshal(current.doc)
			if err != nil {
				return nil, err
			}
			var leaseErr error
			if release != nil {
				leaseErr = w.checkClaimRelease(ctx, p, current.doc, by, meta, release)
			} else {
				leaseErr = w.checkLease(ctx, current.doc, by, meta)
			}
			if leaseErr != nil {
				if !deferredLeaseRefusal(leaseErr) {
					return nil, leaseErr
				}
				// Stop exactly where the refusal used to be returned, keeping
				// the operations prepared so far plus this target for
				// AuthorizeBefore. refresh gives every change its committed
				// pre-image and staged final image, as ordinary preparation
				// does; the refused target is loaded but unchanged, so its
				// final image is its committed body. Authorizers that read
				// final images (hierarchy routes) therefore see a well-formed
				// proposal. The caller releases the refusal only if that
				// authorization admits it.
				p.leaseRefusal = leaseErr
				if op.Kind == model.OpReparent {
					// Hierarchy authorizers compare a reparent's final parent
					// with its operation, so present the requested transition.
					// A reparent that cannot be staged keeps the unchanged
					// image; the refusal released is still the lease refusal.
					_ = p.stageReparent(ctx, current, op, target, aliases)
				}
				p.proposal.Changes = append(p.proposal.Changes, port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: target}, Op: op, Before: before})
				if err := p.refresh(); err != nil {
					return nil, err
				}
				return p, nil
			}
			liveLease := current.doc.Lease
			if overlayStore, ok := w.store.(driver.CheckpointStore); ok && liveLease != nil {
				liveLease, err = EffectiveLease(ctx, overlayStore, current.doc.ID, liveLease)
				if err != nil {
					return nil, err
				}
			}
			if isClaim(meta.Method) && liveLease != nil && w.cfg.now().Before(liveLease.ExpiresAt) {
				if len(cs.Ops) != 1 || op.Kind != model.OpPatch || len(op.Patch.Fields) != 0 {
					return nil, failure("invalid_params", "an already-owned claim requires one empty patch")
				}
				p.proposal.SameClaim = true
			}
			if isSlot(meta.Method) {
				if slotRef == "" {
					slotRef = target.ID
				}
				if slotRef != target.ID {
					return nil, failure("invalid_params", "slot reason must target the same bead")
				}
			}
			switch op.Kind {
			case model.OpReparent:
				if err := p.stageReparent(ctx, current, op, target, aliases); err != nil {
					return nil, err
				}
			case model.OpPatch:
				if err = applyFields(current, op.Patch.Fields, "request"); err != nil {
					return nil, err
				}
			case model.OpClose:
				op.Close.Target = target
				before, err = w.prepareClose(ctx, p, current, op.Close, aliases)
				if err != nil {
					return nil, err
				}
			case model.OpComment:
				if strings.TrimSpace(op.Comment.Body) == "" {
					return nil, failure("invalid_params", "comment body must not be empty")
				}
				// Named close keeps its historical patch+reason lowering and hash.
				// Its optional reason is subject to the same first-close identity
				// rule as typed CloseOp; ordinary explicit comments are unchanged.
				if meta.Method == "bead.close" && w.closedDocument(current.doc) && current.doc.CloseReason != nil && *current.doc.CloseReason == op.Comment.Body {
					break
				}
				stamp := w.cfg.now().UTC()
				var newest time.Time
				for _, comment := range current.doc.Comments {
					if comment.Time.After(newest) {
						newest = comment.Time
					}
				}
				if !newest.Before(stamp) {
					stamp = newest.UTC().Add(time.Second)
				}
				current.doc.Comments = append(current.doc.Comments, Comment{Body: op.Comment.Body, Principal: by, Time: stamp})
				current.setBy["comments"] = "request"
			case model.OpAttach:
				if err = validateAttachmentPayload(op.Attach.Attachment); err != nil {
					return nil, err
				}
				current.doc.Attachments = append(current.doc.Attachments, op.Attach.Attachment)
				current.setBy["attachments"] = "request"
			case model.OpLink, model.OpUnlink:
				kind := op.Link.LinkKind
				if !validDependencyKind(kind) {
					return nil, failure("invalid_params", "unsupported link kind")
				}
				targetKind, e := op.Link.TargetKind.Normalized()
				if e != nil {
					return nil, failure("invalid_params", e.Error())
				}
				to := op.Link.To
				if targetKind != model.LinkTargetExternal {
					to, e = p.resolve(to, aliases)
					if e != nil {
						return nil, e
					}
				}
				if e := model.ValidateLinkTarget(targetKind, to); e != nil {
					return nil, failure("invalid_params", e.Error())
				}
				if op.Kind == model.OpLink && targetKind == model.LinkTargetExternal && kind == "parent-child" && strings.HasPrefix(to.ID, "external:") {
					return nil, failure("invalid_params", "external capability dependencies cannot use parent-child edges")
				}
				op.Link.TargetKind = targetKind
				if targetKind != model.LinkTargetExternal && to.ID == target.ID {
					return nil, failure("ontology_guard_failed", "self-links are refused")
				}
				if e := p.loadLinkTarget(ctx, to, targetKind); e != nil {
					return nil, e
				}
				localParentChild := kind == "parent-child" && targetKind != model.LinkTargetExternal
				if localParentChild && op.Kind == model.OpLink {
					parent, e := HierarchyParent(current.doc)
					if e != nil {
						return nil, e
					}
					if parent != nil && parent.ID != to.ID {
						return nil, failure("invalid_params", "conflicting parent representations")
					}
				}
				// The endpoint is read, not written, unless another op targets it.
				found := -1
				conflictingKind := ""
				for i, link := range current.doc.Links {
					storedTargetKind, e := link.TargetKind.Normalized()
					if e != nil {
						return nil, failure("ontology_guard_failed", "invalid stored link target kind")
					}
					if storedTargetKind != targetKind || link.To.ID != to.ID {
						continue
					}
					if link.Kind == kind {
						found = i
						continue
					}
					conflictingKind = link.Kind
					// All dependency kinds share one typed endpoint identity (B07/B11).
					// A type change requires an explicit unlink before the link;
					// nonblocking kinds still retain their independent graph effects.
					if op.Kind == model.OpLink {
						return nil, failure("ontology_guard_failed", fmt.Sprintf("dependency endpoints already have link kind %s", link.Kind))
					}
				}
				if op.Kind == model.OpUnlink && found < 0 && conflictingKind != "" {
					return nil, failure("not_found", "link of the requested kind does not exist")
				}
				var metadata *model.LinkMetadata
				if op.Link.Metadata != nil {
					if op.Kind != model.OpLink {
						return nil, failure("invalid_params", "metadata is only valid on a link")
					}
					value, err := op.Link.Metadata.ForKind(kind)
					if err != nil {
						return nil, failure("invalid_params", err.Error())
					}
					metadata = &value
				}
				if op.Kind == model.OpLink {
					// Plain legacy links retain their existing representation. Only
					// metadata-bearing canonical parent edges use ParentMetadata.
					if found < 0 && localParentChild && parentID(current.doc.Parent) == to.ID && (metadata != nil || current.doc.ParentMetadata != nil) {
						if metadata != nil {
							value := metadata.Retaining(current.doc.ParentMetadata)
							current.doc.ParentMetadata = &value
						}
					} else if found < 0 {
						current.doc.Links = append(current.doc.Links, Link{To: to, Kind: kind, Metadata: metadata, TargetKind: targetKind})
					} else if metadata != nil {
						// Omission preserves the stored predicate. An explicit
						// replacement participates in ordinary final-image CAS,
						// no-op detection and the single history/version commit.
						value := metadata.Retaining(current.doc.Links[found].Metadata)
						current.doc.Links[found].Metadata = &value
					}
				}
				if op.Kind == model.OpUnlink && found >= 0 {
					if localParentChild {
						// Remove every matching legacy copy before detaching Parent.
						links := current.doc.Links[:0]
						for _, link := range current.doc.Links {
							storedTargetKind, e := link.TargetKind.Normalized()
							if e != nil {
								return nil, failure("ontology_guard_failed", "invalid stored link target kind")
							}
							if storedTargetKind != targetKind || link.Kind != kind || link.To.ID != to.ID {
								links = append(links, link)
							}
						}
						current.doc.Links = links
					} else {
						current.doc.Links = append(current.doc.Links[:found], current.doc.Links[found+1:]...)
					}
				}
				if localParentChild {
					if op.Kind == model.OpLink {
						parent, e := p.resolveParent(ctx, &to, aliases, target.ID)
						if e != nil {
							return nil, e
						}
						current.doc.Parent = parent
					} else if parentID(current.doc.Parent) == to.ID {
						current.doc.Parent = nil
						current.doc.ParentMetadata = nil
					}
					current.setBy["parent"] = "request"
					p.hierarchyEdits = append(p.hierarchyEdits, hierarchyEdit{id: target.ID, parent: parentID(current.doc.Parent)})
				}
				current.setBy["links"] = "request"
			case model.OpDelete:
				current.deleted = true
				p.hierarchyEdits = append(p.hierarchyEdits, hierarchyEdit{id: target.ID, deleted: true})
			case model.OpSetMeta:
				if op.SetMeta.Key == "" || strings.HasPrefix(op.SetMeta.Key, "_plug") || strings.HasPrefix(op.SetMeta.Key, "core.") {
					return nil, failure("protected_record", "reserved core metadata cannot be changed")
				}
				if current.doc.Metadata == nil {
					current.doc.Metadata = map[string]json.RawMessage{}
				}
				if bytes.Equal(bytes.TrimSpace(op.SetMeta.Value), []byte("null")) {
					delete(current.doc.Metadata, op.SetMeta.Key)
				} else {
					// Equal metadata targets keep the committed token spelling. In
					// particular, object order/whitespace cannot mint a version, and
					// numeric literal differences must remain real changes.
					equal := false
					if previous, present := current.doc.Metadata[op.SetMeta.Key]; present {
						before, beforeErr := canonicalMetadataJSON(previous)
						after, afterErr := canonicalMetadataJSON(op.SetMeta.Value)
						if beforeErr != nil || afterErr != nil {
							return nil, failure("invalid_params", "metadata comparison requires valid JSON")
						}
						equal = bytes.Equal(before, after)
					}
					if !equal {
						current.doc.Metadata[op.SetMeta.Key] = append(json.RawMessage(nil), op.SetMeta.Value...)
					}
				}
				current.setBy["metadata."+op.SetMeta.Key] = "request"
			}
		}
		p.refs = append(p.refs, target)
		change := port.Change{Resource: model.ResourceRef{Kind: "bead", Ref: target}, Op: op, Before: before}
		if p.closes != nil {
			if !current.deleted {
				change.After, err = current.body()
				if err != nil {
					return nil, err
				}
			}
			p.ordered = append(p.ordered, change)
		}
		p.proposal.Changes = append(p.proposal.Changes, change)
	}
	// Only actual operation targets belong to the write set. Reads of link
	// endpoints remain guarded but do not gain spurious versions or revisions.
	targets := map[string]bool{}
	for _, ref := range p.refs {
		targets[ref.ID] = true
	}
	for id := range p.writes {
		if !targets[id] {
			delete(p.writes, id)
		}
	}
	for _, expect := range cs.Expect {
		ref, err := p.resolve(expect.Ref, aliases)
		if err != nil {
			return nil, err
		}
		canonical := expect
		canonical.Ref = ref
		p.proposal.Expect = append(p.proposal.Expect, canonical)
		records, err := snapshot.Get(ctx, []model.Ref{ref})
		if err != nil {
			return nil, err
		}
		if len(records) != 1 {
			if p.refusal == nil {
				reason := "hash"
				if expect.Revision != nil {
					reason = "revision"
				}
				p.refusal = preconditionFailure(reason, "expected record is absent", ref)
			}
			continue
		}
		if expect.Revision != nil {
			if expect.Field != "" || expect.Hash != "" {
				return nil, failure("invalid_params", "precondition must select revision or field hash")
			}
			if expect.Revision.String() != records[0].Revision.String() {
				if p.refusal == nil {
					p.refusal = preconditionRevisionFailure(records[0].Revision, ref)
				}
			}
		} else {
			if expect.Field == "" || expect.Hash == "" {
				return nil, failure("invalid_params", "field precondition requires field and hash")
			}
			doc, err := DecodeDocument(records[0].Body)
			if err != nil {
				return nil, err
			}
			hash, err := expectationHash(doc, expect.Field, ref)
			if err != nil {
				if p.refusal == nil {
					p.refusal = err
				}
				continue
			}
			if hash != expect.Hash {
				if p.refusal == nil {
					p.refusal = preconditionFailure("hash", "field hash changed", ref)
				}
			}
		}
	}
	if err := p.refresh(); err != nil {
		return nil, err
	}
	return p, nil
}

// Attachments use the ordinary bead write and receipt boundary. URI evidence
// needs no content store; content-backed evidence cannot be accepted until a
// store can verify and retain the referenced bytes.
func validateAttachmentPayload(attachment model.Attachment) error {
	if strings.TrimSpace(attachment.MediaType) == "" {
		return failure("invalid_params", "attachment requires a mediaType")
	}
	switch attachment.Kind {
	case "link", "commit":
		if strings.TrimSpace(attachment.URI) == "" || attachment.ContentRef != "" {
			return failure("invalid_params", "link and commit attachments require uri without contentRef")
		}
	case "blob", "log":
		if strings.TrimSpace(attachment.ContentRef) == "" || attachment.URI != "" {
			return failure("invalid_params", "blob and log attachments require contentRef without uri")
		}
		return &model.Error{Message: "attachment content store is not implemented", Data: model.ErrorData{Code: model.CodeUnsupported, Details: map[string]any{"reason": "attachment_content_store_unavailable", "kind": attachment.Kind}}}
	default:
		return failure("invalid_params", "unsupported attachment kind")
	}
	return nil
}

// loadLinkTarget reads only local endpoints. An explicitly external target
// never aliases a same-spelled local bead or a local deletion in this batch.
// Missing local targets remain not_found; workflow references retain their
// separate typed admission boundary.
func (p *prepared) loadLinkTarget(ctx context.Context, ref model.Ref, kind model.LinkTargetKind) error {
	if kind == model.LinkTargetExternal {
		return nil
	}
	if prior, ok := p.writes[ref.ID]; ok && prior.deleted {
		return failure("not_found", "record was deleted in this batch")
	}
	if _, err := p.load(ctx, ref); err == nil {
		return nil
	} else {
		var typed *model.Error
		if !errors.As(err, &typed) || typed == nil || typed.Data.Code != "not_found" {
			return err
		}
		if strings.HasPrefix(ref.ID, "workflow:") {
			return failure("invalid_params", "workflow references require a supported workflow item operation")
		}
		return err
	}
}

// prepareMemory lowers memory.save/delete into the same prepared transaction
// used by bead writes. The memory body is deliberately independent of the
// bead Document and never enters ontology normalization or graph maintenance.
func (w *Writer) prepareMemory(ctx context.Context, reader driver.Reader, by model.PrincipalRef, cs model.ChangeSet, meta model.WriteMeta) (*prepared, error) {
	snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	if _, err := snapshot.observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock}); err != nil {
		return nil, err
	}
	p := &prepared{snapshot: snapshot, writes: map[string]*pending{}, proposal: port.ProposedChange{RequestID: meta.IdempotencyKey, Principal: by, BaseRevision: *reader.Snapshot().At, DryRun: meta.DryRun, Meta: meta, Changes: []port.Change{}}, warnings: []model.Warning{}}
	if len(cs.Ops) != 1 {
		return nil, failure("invalid_params", "memory writes require exactly one operation")
	}
	op := cs.Ops[0]
	if meta.Method == "memory.save" {
		if op.Kind != model.OpCreate || op.Create == nil || op.Create.Type != "memory" {
			return nil, failure("invalid_params", "memory.save requires a memory create operation")
		}
		key, ok := op.Create.Fields["key"].(string)
		if !ok || strings.TrimSpace(key) == "" {
			return nil, failure("invalid_params", "memory key must be nonempty")
		}
		content, ok := op.Create.Fields["content"].(string)
		if !ok {
			return nil, failure("invalid_params", "memory content must be a string")
		}
		if strings.TrimSpace(content) == "" {
			return nil, failure("invalid_params", "memory content must be nonempty")
		}
		metadata := map[string]any{}
		if raw, exists := op.Create.Fields["metadata"]; exists && raw != nil {
			var valid bool
			metadata, valid = raw.(map[string]any)
			if !valid {
				return nil, failure("invalid_params", "memory metadata must be an object")
			}
		}
		page, err := findMemoryByKey(ctx, snapshot, key)
		if err != nil {
			return nil, err
		}
		if len(page) > 1 {
			return nil, failure("internal_error", "duplicate memory key")
		}
		replaced := false
		if len(page) == 1 {
			_, document, _, err := ResolveMemoryRecord(ctx, snapshot, page[0])
			if err != nil {
				return nil, failure("internal_error", "invalid persisted memory")
			}
			replaced = document.Overlay == nil || !document.Overlay.Deleted
		}
		var pendingRecord *pending
		changeOp := op
		if len(page) == 1 {
			record, doc, source, err := ResolveMemoryRecord(ctx, snapshot, page[0])
			if err != nil {
				return nil, failure("internal_error", "invalid persisted memory")
			}
			if source != nil {
				pendingRecord, changeOp, err = w.prepareMemoryOverlay(ctx, p, by, source, record, doc, content, metadata, false)
				if err != nil {
					return nil, err
				}
			} else {
				doc.Content, doc.Metadata = content, cloneAnyMap(metadata)
				rows, err := snapshot.Get(ctx, []model.Ref{record.Ref})
				if err != nil {
					return nil, err
				}
				if len(rows) != 1 {
					return nil, failure("internal_error", "memory disappeared during snapshot")
				}
				pendingRecord = &pending{ref: record.Ref, kind: "memory", memory: &doc, before: append(json.RawMessage(nil), record.Body...), revision: record.Revision, setBy: map[string]string{"content": "request", "metadata": "request"}}
				changeOp = model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: record.Ref, Fields: map[string]any{"content": content, "metadata": metadata}}}
			}
		} else {
			id, err := w.cfg.newID()
			if err != nil {
				return nil, err
			}
			ref := model.RefByID(id, model.Natural64(0))
			if _, err := snapshot.Get(ctx, []model.Ref{ref}); err != nil {
				return nil, err
			}
			if state, exists := snapshot.reads.states[recordLockKey(ref)]; exists && state.Present {
				return nil, errStale
			}
			doc := MemoryDocument{ID: id, Key: key, Content: content, Metadata: cloneAnyMap(metadata)}
			pendingRecord = &pending{ref: ref, kind: "memory", memory: &doc, setBy: map[string]string{"key": "request", "content": "request", "metadata": "request"}}
		}
		pendingRecord.replaced = &replaced
		p.writes[pendingRecord.ref.ID], p.refs = pendingRecord, append(p.refs, pendingRecord.ref)
		p.proposal.Changes = append(p.proposal.Changes, port.Change{Resource: model.ResourceRef{Kind: "memory", Ref: pendingRecord.ref}, Op: changeOp, Before: pendingRecord.before})
	} else if meta.Method == "memory.delete" {
		if op.Kind != model.OpDelete || op.Delete == nil {
			return nil, failure("invalid_params", "memory.delete requires a delete operation")
		}
		ref, err := p.resolve(op.Delete.Target, nil)
		if err != nil {
			return nil, err
		}
		rows, err := snapshot.Get(ctx, []model.Ref{ref})
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || rows[0].Kind != "memory" {
			return nil, failure("not_found", "memory was not found")
		}
		effective, doc, source, err := ResolveMemoryRecord(ctx, snapshot, rows[0])
		if err != nil {
			return nil, failure("internal_error", "invalid persisted memory")
		}
		if source != nil && !source.Visible || doc.Overlay != nil && doc.Overlay.Deleted {
			return nil, failure("not_found", "memory was not found")
		}
		var pendingRecord *pending
		changeOp := op
		if source != nil {
			pendingRecord, changeOp, err = w.prepareMemoryOverlay(ctx, p, by, source, effective, doc, "", nil, true)
			if err != nil {
				return nil, err
			}
		} else {
			pendingRecord = &pending{ref: ref, kind: "memory", memory: &doc, before: append(json.RawMessage(nil), rows[0].Body...), revision: rows[0].Revision, deleted: true, setBy: map[string]string{}}
		}
		p.writes[pendingRecord.ref.ID], p.refs = pendingRecord, append(p.refs, pendingRecord.ref)
		p.proposal.Changes = append(p.proposal.Changes, port.Change{Resource: model.ResourceRef{Kind: "memory", Ref: pendingRecord.ref}, Op: changeOp, Before: pendingRecord.before})
	} else {
		return nil, failure("invalid_params", "unsupported memory method")
	}
	for _, expect := range cs.Expect {
		ref, err := p.resolve(expect.Ref, nil)
		if err != nil {
			return nil, err
		}
		canonical := expect
		canonical.Ref = ref
		p.proposal.Expect = append(p.proposal.Expect, canonical)
		rows, err := snapshot.Get(ctx, []model.Ref{ref})
		if err != nil {
			return nil, err
		}
		if len(rows) == 1 {
			effective, document, _, resolveErr := ResolveMemoryRecord(ctx, snapshot, rows[0])
			if resolveErr != nil {
				return nil, failure("internal_error", "invalid persisted memory")
			}
			if document.Overlay != nil && document.Overlay.Deleted {
				rows = nil
			} else {
				rows = []driver.Record{effective}
			}
		}
		if len(rows) != 1 {
			if p.refusal == nil {
				reason := "hash"
				if expect.Revision != nil {
					reason = "revision"
				}
				p.refusal = preconditionFailure(reason, "expected record is absent", ref)
			}
			continue
		}
		if expect.Revision != nil {
			if expect.Field != "" || expect.Hash != "" {
				return nil, failure("invalid_params", "precondition must select revision or field hash")
			}
			if expect.Revision.String() != rows[0].Revision.String() {
				if p.refusal == nil {
					p.refusal = preconditionRevisionFailure(rows[0].Revision, ref)
				}
			}
			continue
		}
		if expect.Field == "" || expect.Hash == "" {
			return nil, failure("invalid_params", "precondition requires revision or field hash")
		}
		doc, _, err := DecodeMemoryRecord(rows[0])
		if err != nil {
			return nil, failure("internal_error", "invalid persisted memory")
		}
		var value any
		switch {
		case expect.Field == "key":
			value = doc.Key
		case expect.Field == "content":
			value = doc.Content
		case strings.HasPrefix(expect.Field, "metadata."):
			value = doc.Metadata[strings.TrimPrefix(expect.Field, "metadata.")]
		default:
			return nil, failure("invalid_params", "memory precondition field is unsupported")
		}
		hash, err := FieldHash(value)
		if err != nil || hash != expect.Hash {
			if p.refusal == nil {
				p.refusal = preconditionFailure("hash", "field hash changed", ref)
			}
		}
	}
	if err := p.refresh(); err != nil {
		return nil, err
	}
	return p, nil
}

func recordLockKey(ref model.Ref) string {
	key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	return key
}

type memoryKeyReader interface {
	memoryRecordReader
	Query(context.Context, driver.Query) (driver.Page[driver.Record], error)
}

func findMemoryByKey(ctx context.Context, snapshot memoryKeyReader, key string) ([]driver.Record, error) {
	found := make([]driver.Record, 0, 1)
	seen := map[string]bool{}
	var cursor *driver.Cursor
	scanned := 0
	for {
		page, err := snapshot.Query(ctx, driver.Query{Filter: map[string]any{"kind": "memory"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		scanned += len(page.Items)
		if scanned > auditScanLimit {
			return nil, failure("scope_too_large", "memory key scan exceeds 20000 records")
		}
		for _, record := range page.Items {
			effective, document, source, err := ResolveMemoryRecord(ctx, snapshot, record)
			if err != nil {
				return nil, failure("internal_error", "invalid persisted memory")
			}
			if document.Key != key {
				continue
			}
			if source != nil && !source.Visible {
				return nil, failure("invalid_params", "hidden source memory cannot be replaced")
			}
			if !seen[effective.Ref.ID] {
				found = append(found, effective)
				seen[effective.Ref.ID] = true
			}
		}
		if page.NextCursor == nil {
			return found, nil
		}
		if len(page.Items) == 0 {
			return nil, failure("internal_error", "memory key scan did not advance")
		}
		cursor = page.NextCursor
	}
}

func cloneAnyMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func hashField(fields map[string]any, name string) string {
	value, ok := fields[name]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(raw)
}

func (p *prepared) loadBeads(ctx context.Context) error {
	if p.beadsLoaded {
		return nil
	}
	var cursor *driver.Cursor
	for {
		page, err := p.snapshot.Query(ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return err
		}
		p.beads = append(p.beads, page.Items...)
		if page.NextCursor == nil {
			p.beadsLoaded = true
			return nil
		}
		if len(page.Items) == 0 {
			return failure("internal_error", "allocator scan did not advance")
		}
		cursor = page.NextCursor
	}
}

// allocatorObservations returns the top-level count and occupied IDs for a
// project, plus the largest observed direct numeric child for the requested
// parent. It reads the immutable graph once and layers batch-created records
// over that snapshot, so staged hash collisions and stale child counters are
// both visible before lock acquisition.
func (p *prepared) allocatorObservations(ctx context.Context, prefix, parent string) (uint64, map[string]bool, uint64, error) {
	if err := p.loadBeads(ctx); err != nil {
		return 0, nil, 0, err
	}
	count := uint64(0)
	occupied := map[string]bool{}
	maxChild := uint64(0)
	add := func(id string, project string, deleted bool) error {
		// A number previewed by an earlier create remains consumed within
		// this batch even if a later operation deletes that pending child.
		if parent != "" {
			n, err := driver.ChildNumber(parent, id)
			if errors.Is(err, driver.ErrUnsupported) {
				return err
			}
			if err == nil && n > maxChild {
				maxChild = n
			}
		}
		if deleted {
			return nil
		}
		occupied[id] = true
		if prefix != "" && strings.HasPrefix(id, prefix+"-") {
			tail := strings.TrimPrefix(id, prefix+"-")
			if tail != "" && !strings.ContainsRune(tail, '.') {
				count++
			}
		}
		return nil
	}
	for _, record := range p.beads {
		if _, replaced := p.writes[record.Ref.ID]; replaced {
			continue
		}
		driver.ReportReadWork(ctx, driver.ReadAllocatorDecodes, 1)
		doc, err := DecodeDocument(record.Body)
		if err != nil {
			return 0, nil, 0, err
		}
		if err := add(record.Ref.ID, doc.Project, false); err != nil {
			return 0, nil, 0, err
		}
	}
	for id, pending := range p.writes {
		if pending.kind != "bead" {
			continue
		}
		if err := add(id, pending.doc.Project, pending.deleted); err != nil {
			return 0, nil, 0, err
		}
	}
	return count, occupied, maxChild, nil
}

// Candidate identity is global, including other kinds/projects and tombstones.
// Observe also puts a miss in the read set, so a concurrent insert invalidates
// preparation before the allocator or any records can commit.
func (p *prepared) allocatorOccupied(ctx context.Context, id string) (bool, error) {
	if allocatorReserved(ctx, id) {
		return true, nil
	}
	if _, staged := p.writes[id]; staged {
		return true, nil
	}
	ref := model.RefByID(id, model.Natural64(0))
	states, err := p.snapshot.observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}})
	if err != nil {
		return false, err
	}
	return states[0].Present || !states[0].Revision.IsZero(), nil
}

// Child numbering consumes identity metadata, not existing child documents.
// Every previously live ID remains consumed even if deleted in this batch,
// exactly as allocatorObservations' add-before-deleted check. Only new creates
// can add an identity to that base set; read-only parents and field edits cannot.
func (p *prepared) observedChildNumber(ctx context.Context, parent string) (uint64, error) {
	reader, ok := p.snapshot.reader.(driver.NumericChildReader)
	if !ok {
		_, _, maximum, err := p.allocatorObservations(ctx, "", parent)
		return maximum, err
	}
	if _, err := p.snapshot.observe(ctx, []driver.LockKey{driver.GraphBlockingSentinelLock}); err != nil {
		return 0, err
	}
	driver.ReportReadWork(ctx, driver.ReadChildIDLookups, 1)
	maximum, err := reader.MaxChildNumber(ctx, parent, nil)
	if err != nil {
		return 0, err
	}
	for _, id := range p.createdBeadIDs {
		driver.ReportReadWork(ctx, driver.ReadChildIDCandidates, 1)
		n, err := driver.ChildNumber(parent, id)
		if errors.Is(err, driver.ErrUnsupported) {
			return 0, err
		}
		if err == nil && n > maximum {
			maximum = n
		}
	}
	return maximum, nil
}

func (p *prepared) fallbackChildID(ctx context.Context, parent string) (string, error) {
	incarnation := p.snapshot.Snapshot().Incarnation
	key, err := driver.ParentCounterLock(incarnation, parent)
	if err != nil {
		return "", err
	}
	if _, err = p.snapshot.observe(ctx, []driver.LockKey{key}); err != nil {
		return "", err
	}
	reader, ok := p.snapshot.reader.(driver.ParentCounterReader)
	if !ok {
		return "", driver.ErrUnsupported
	}
	state, err := reader.LoadParentCounter(ctx, parent)
	if errors.Is(err, driver.ErrNotFound) {
		state = driver.ParentCounter{Incarnation: incarnation, Parent: parent}
	} else if err != nil {
		return "", err
	}
	observed, err := p.observedChildNumber(ctx, parent)
	if err != nil {
		return "", err
	}
	for {
		id, err := state.Next(observed)
		if err != nil {
			return "", err
		}
		used, err := p.allocatorOccupied(ctx, id)
		if err != nil || !used {
			return id, err
		}
		state, err = state.Consume(id)
		if err != nil {
			return "", err
		}
	}
}

// nextAllocatorID previews a restored allocator on the immutable preparation
// snapshot. Hash candidates are content-bound and collision-checked against
// committed plus batch-staged IDs; sequential candidates retain their legacy
// cursor behavior. The matching allocator fact is upgraded to an update lock
// in transact, where the expected ID is consumed atomically with records.
func (p *prepared) nextAllocatorID(ctx context.Context, project string, input driver.HashInput, fallback func() (string, error), mint func(driver.OfflineAllocator) (string, error)) (string, string, string, *driver.HashInput, error) {
	if project == "" {
		id, err := fallback()
		return id, "", "", nil, err
	}
	reader, ok := p.snapshot.reader.(driver.AllocatorReader)
	if !ok {
		id, err := fallback()
		return id, "", "", nil, err
	}
	if !p.allocatorSeen[project] {
		key, err := driver.AllocatorLock(project)
		if err != nil {
			return "", "", "", nil, err
		}
		if _, err = p.snapshot.observe(ctx, []driver.LockKey{key}); err != nil {
			return "", "", "", nil, err
		}
		p.allocatorSeen[project] = true
	}
	state, err := reader.LoadAllocator(ctx, project)
	if errors.Is(err, driver.ErrNotFound) {
		id, fallbackErr := fallback()
		return id, "", "", nil, fallbackErr
	}
	if err != nil {
		return "", "", "", nil, err
	}
	switch state.Mode {
	case driver.AllocatorRandom:
		id, err := mint(state)
		if err != nil {
			return "", "", "", nil, err
		}
		if err := state.ValidateCandidate("", id); err != nil {
			return "", "", "", nil, err
		}
		return id, project, id, nil, nil
	case driver.AllocatorHash:
		count, err := p.hashPopulation(ctx, project)
		if err != nil {
			return "", "", "", nil, err
		}
		id, err := state.MintHashID(input, count, func(candidate string) (bool, error) {
			for _, pending := range p.writes {
				if !pending.deleted && pending.ref.ID == candidate {
					return true, nil
				}
			}
			return p.allocatorOccupied(ctx, candidate)
		})
		if err != nil {
			return "", "", "", nil, err
		}
		return id, project, id, &input, nil
	default:
		if cursor, exists := p.allocatorCursors[project]; exists {
			state.LastID = cursor
		}
		id, err := state.NextID()
		if err != nil {
			return "", "", "", nil, err
		}
		if state.LastID == ^uint64(0) {
			return "", "", "", nil, fmt.Errorf("%w: allocator counter exhausted", driver.ErrUnsupported)
		}
		next := state.LastID + 1
		p.allocatorCursors[project] = next
		return id, project, id, nil, nil
	}
}

// nextChildAllocatorID keeps the dotted ID contract even when no restored
// allocator exists by reconciling a durable parent-only counter and direct
// IDs. A restored random allocator retains its opaque suffix; sequential/hash
// allocators repair a
// stale counter to the observed maximum before consuming the candidate.
func (p *prepared) nextChildAllocatorID(ctx context.Context, project, parent string, fallback func() (string, error), mint func(driver.OfflineAllocator, string) (string, error)) (string, string, string, string, error) {
	if project == "" {
		id, err := p.fallbackChildID(ctx, parent)
		return id, "", id, parent, err
	}
	reader, ok := p.snapshot.reader.(driver.AllocatorReader)
	if !ok {
		id, err := p.fallbackChildID(ctx, parent)
		return id, "", id, parent, err
	}
	if !p.allocatorSeen[project] {
		key, err := driver.AllocatorLock(project)
		if err != nil {
			return "", "", "", "", err
		}
		if _, err = p.snapshot.observe(ctx, []driver.LockKey{key}); err != nil {
			return "", "", "", "", err
		}
		p.allocatorSeen[project] = true
	}
	state, err := reader.LoadAllocator(ctx, project)
	if errors.Is(err, driver.ErrNotFound) {
		id, fallbackErr := p.fallbackChildID(ctx, parent)
		return id, "", id, parent, fallbackErr
	}
	if err != nil {
		return "", "", "", "", err
	}
	if state.Mode == driver.AllocatorRandom {
		id, err := mint(state, parent)
		if err != nil {
			return "", "", "", "", err
		}
		if err := state.ValidateCandidate(parent, id); err != nil {
			return "", "", "", "", err
		}
		return id, project, id, parent, nil
	}
	observed, err := p.observedChildNumber(ctx, parent)
	if err != nil {
		return "", "", "", "", err
	}
	current := state.Children[parent]
	if observed > current {
		current = observed
	}
	if current == ^uint64(0) {
		return "", "", "", "", fmt.Errorf("%w: child counter exhausted", driver.ErrUnsupported)
	}
	for current < ^uint64(0) {
		current++
		id := fmt.Sprintf("%s.%d", parent, current)
		used, err := p.allocatorOccupied(ctx, id)
		if err != nil {
			return "", "", "", "", err
		}
		if !used {
			return id, project, id, parent, nil
		}
	}
	return "", "", "", "", fmt.Errorf("%w: child counter exhausted", driver.ErrUnsupported)
}

func applyFields(p *pending, fields map[string]any, setter string) error {
	for name, value := range fields {
		if name == "" || strings.ContainsAny(name, "./\x00") {
			return failure("invalid_params", "field names must be nonempty unqualified keys")
		}
		if name == "labels" {
			if err := setLabels(p, value, setter); err != nil {
				return err
			}
			continue
		}
		if value == nil {
			delete(p.doc.Fields, name)
		} else {
			p.doc.Fields[name] = value
		}
		p.setBy["fields."+name] = setter
	}
	return nil
}

func (p *prepared) refresh() error {
	for i := range p.proposal.Changes {
		if _, active := p.external[i]; active {
			continue
		}
		c := &p.proposal.Changes[i]
		pending := p.writes[c.Resource.Ref.ID]
		// All operations on a record describe the same atomic transition.
		// A later patch of a batch-created record therefore still has no
		// committed pre-image, even after ontology defaults are supplied.
		c.Before = append(json.RawMessage(nil), pending.before...)
		if pending.deleted {
			c.After = nil
		} else {
			body, err := pending.body()
			if err != nil {
				return err
			}
			c.After = body
		}
		c.SetBy = cloneMap(pending.setBy)
	}
	return nil
}

func (p *prepared) patches(patches []port.Patch, owner string, owns []string) error {
	allowed := map[string]bool{}
	for _, path := range owns {
		allowed[path] = true
	}
	for _, patch := range patches {
		ref := patch.Resource.Ref
		if patch.Resource.Kind != "bead" || !ref.IsID() || !ref.Branch.IsZero() {
			return failure("plugin_patch_out_of_scope", "patch resource is outside the proposed beads")
		}
		pending := p.writes[ref.ID]
		if pending == nil || pending.deleted {
			return failure("plugin_patch_out_of_scope", "patch may only change a proposed live record")
		}
		for path, raw := range patch.Paths {
			name, err := fieldName(path)
			if err != nil {
				return err
			}
			if owns != nil && !allowed[path] {
				return failure("plugin_patch_out_of_scope", "mutator wrote outside its owned fields")
			}
			var value any
			if err := decode(raw, &value); err != nil {
				return failure("invalid_params", "mutator returned invalid JSON")
			}
			if err := applyFields(pending, map[string]any{name: value}, owner); err != nil {
				return err
			}
		}
	}
	return p.refresh()
}

func cloneMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func FieldHash(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

// expectationHash keeps the long-standing bare/fields.<name> hash contract
// byte-for-byte, while making metadata.<literal-key> an explicit selector for
// the committed document metadata. Metadata is decoded and canonicalized only
// for this comparison; the stored RawMessage and exported FieldHash remain
// untouched.
func expectationHash(doc Document, selector string, refusing ...model.Ref) (string, error) {
	if key, ok := strings.CutPrefix(selector, "metadata."); ok {
		raw, present := doc.Metadata[key]
		if !present {
			// The admitted string hash arm has no absence discriminator. Never
			// treat a missing metadata key as the digest of JSON null.
			return "", preconditionFailure("hash", "metadata key is absent", refusing...)
		}
		canonical, err := canonicalMetadataJSON(raw)
		if err != nil {
			return "", preconditionFailure("hash", "metadata value is invalid JSON", refusing...)
		}
		digest := sha256.Sum256(canonical)
		return hex.EncodeToString(digest[:]), nil
	}
	name := strings.TrimPrefix(selector, "fields.")
	return FieldHash(doc.Fields[name])
}

// canonicalMetadataJSON is local to metadata expectations. Object keys are
// sorted while arrays, types and numeric literal spellings stay intact, as
// required by B17. UseNumber avoids both float64 rounding and conflating 1
// with 1.0. Comparison never rewrites the stored RawMessage.
func canonicalMetadataJSON(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("metadata must contain exactly one JSON value")
	}
	return json.Marshal(value)
}

func isSlot(method string) bool {
	switch method {
	case "bead.claim", "bead.claimNext", "bead.close", "bead.fail", "bead.cancel", "bead.reopen", "bead.defer", "bead.release":
		return true
	}
	return false
}

// stageReparent applies a reparent operation to its loaded target. It changes
// the pending record only after every check has passed.
func (p *prepared) stageReparent(ctx context.Context, current *pending, op model.Op, target model.Ref, aliases map[string]model.Ref) error {
	if project := op.Reparent.Project; project != "" && project != current.doc.Project {
		return failure("invalid_params", "reparent cannot move a bead between projects")
	}
	parent, err := p.resolveParent(ctx, op.Reparent.Parent, aliases, target.ID)
	if err != nil {
		return err
	}
	next := current.doc
	next.Parent = parent
	if _, err := HierarchyParent(next); err != nil {
		return err
	}
	op.Reparent.Target, op.Reparent.Parent = target, parent
	if parentID(current.doc.Parent) != parentID(parent) {
		current.doc.ParentMetadata = nil
	}
	current.doc.Parent = parent
	current.setBy["parent"] = "request"
	p.hierarchyEdits = append(p.hierarchyEdits, hierarchyEdit{id: target.ID, parent: parentID(parent)})
	return nil
}

func isClaim(method string) bool { return method == "bead.claim" || method == "bead.claimNext" }

// deferredLeaseRefusal selects the typed lease refusals that depend on the
// bead's current claim state (claim, release, update, close and their tx.apply
// forms all pass the same check). Storage, staleness and capability errors
// still fail preparation immediately.
func deferredLeaseRefusal(err error) bool {
	var typed *model.Error
	if !errors.As(err, &typed) || typed == nil {
		return false
	}
	switch typed.Data.Code {
	case "busy", "lease_not_held", "lease_expired":
		return true
	}
	return false
}

func (w *Writer) checkLease(ctx context.Context, doc Document, by model.PrincipalRef, meta model.WriteMeta) error {
	if isGateMethod(meta.Method) && meta.Method == "gate."+meta.GateAction {
		// Approval and resolver authority are independent of claim ownership.
		// Normalization and authorization still validate the gate operation;
		// claims preserves a pending lease and clears it on terminal resolution.
		return nil
	}
	lease := doc.Lease
	if overlayStore, ok := w.store.(driver.CheckpointStore); ok && lease != nil {
		effective, err := EffectiveLease(ctx, overlayStore, doc.ID, lease)
		if err != nil {
			return err
		}
		lease = effective
	}
	if lease == nil {
		return nil
	}
	if isClaim(meta.Method) {
		if w.cfg.now().Before(lease.ExpiresAt) {
			principal, _ := driver.PrincipalKey(by)
			holder, _ := driver.PrincipalKey(lease.Principal)
			if principal != holder || by.Kind == "anonymous" && meta.AgentLabel != lease.Agent {
				return failure("busy", "bead already has a live claim")
			}
		}
		return nil
	}
	principal, _ := driver.PrincipalKey(by)
	holder, _ := driver.PrincipalKey(lease.Principal)
	if principal != holder || (by.Kind == "anonymous" && meta.AgentLabel != lease.Agent) {
		return failure("lease_not_held", "bead is claimed by another principal")
	}
	if !w.cfg.now().Before(lease.ExpiresAt) {
		return failure("lease_expired", "bead claim has expired")
	}
	return nil
}

func (w *Writer) claims(p *prepared, by model.PrincipalRef, meta model.WriteMeta) error {
	stamp := w.cfg.now().UTC()
	if err := w.lifecycleTimes(p, stamp); err != nil {
		return err
	}
	if isClaim(meta.Method) && !p.proposal.SameClaim {
		ttl := model.Natural64(uint64(w.cfg.claimTTL / time.Millisecond))
		for _, pending := range p.writes {
			pending.doc.Lease = &ClaimLease{Principal: by, Agent: meta.AgentLabel, ExpiresAt: stamp.Add(w.cfg.claimTTL), TtlMs: ttl, Epoch: model.Natural64(w.lease.Epoch())}
			pending.setBy["lease"] = "request"
		}
	} else {
		for _, pending := range p.writes {
			method := p.lifecycleMethod(pending)
			if !isSlot(method) || isClaim(method) {
				continue
			}
			pending.doc.Lease = nil
			pending.setBy["lease"] = "request"
		}
	}
	return p.refresh()
}

func (p *prepared) sentinels() []driver.Sentinel {
	set := map[driver.Sentinel]bool{}
	if p.hierarchyDelta != nil {
		for _, s := range p.hierarchyDelta.sentinels {
			set[s] = true
		}
	}
	for _, pending := range p.writes {
		if !p.changed[pending.ref.ID] {
			continue
		}
		set[driver.SentinelGraphBlocking] = true
		if pending.kind != "memory" {
			if s, err := driver.NewBeadSentinel(pending.ref.ID); err == nil {
				set[s] = true
			}
		}
	}
	result := make([]driver.Sentinel, 0, len(set))
	for s := range set {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (p *prepared) detectChanges() error {
	p.changed = map[string]bool{}
	for id, pending := range p.writes {
		if len(pending.before) == 0 {
			if !pending.deleted {
				p.changed[id] = true
			}
			continue
		}
		if pending.deleted {
			p.changed[id] = true
			continue
		}
		if pending.rawMemory != nil {
			if !bytes.Equal(pending.before, pending.rawMemory) {
				return failure("internal_error", "immutable memory guard changed")
			}
			continue
		}
		var oldBody json.RawMessage
		var err error
		if pending.kind == "memory" {
			memory, decodeErr := DecodeMemoryDocument(pending.before)
			if decodeErr != nil {
				return decodeErr
			}
			oldBody, err = json.Marshal(memory)
		} else {
			before, decodeErr := DecodeDocument(pending.before)
			if decodeErr != nil {
				return decodeErr
			}
			oldBody, err = json.Marshal(before)
		}
		if err != nil {
			return err
		}
		body, err := pending.body()
		if err != nil {
			return err
		}
		if !bytes.Equal(oldBody, body) {
			p.changed[id] = true
		}
	}
	return nil
}

func (p *prepared) graph(ctx context.Context) error {
	docs, err := p.hierarchy(ctx)
	if err != nil {
		return err
	}
	need := docs != nil
	for _, edit := range p.hierarchyEdits {
		// Indexed hierarchy facts are deliberately partial. Retain the complete
		// graph checks for legacy hierarchy links and scheduling relations.
		need = need || !edit.create || edit.parent != ""
	}
	for _, pending := range p.writes {
		need = need || len(p.hierarchyEdits) != 0 && len(pending.before) != 0 && pending.doc.Parent != nil
	}
	for _, c := range p.proposal.Changes {
		if c.Op.Kind == model.OpLink || c.Op.Kind == model.OpUnlink || c.Op.Kind == model.OpDelete {
			need = true
		}
	}
	if !need {
		// These operations preserve any valid graph, including one advanced by
		// unrelated writers. Consult its certificate only after acquiring the
		// publication locks, not as an unnecessary whole-graph pre-read fact.
		p.graphPreserved = true
		return nil
	}
	if docs == nil {
		indexed, err := p.observeIndexedGraphIncoming(ctx)
		if err != nil {
			return err
		}
		if indexed {
			certified, err := p.graphIsCertified(ctx)
			if err != nil {
				return err
			}
			if certified {
				p.graphValidated = true
				return nil
			}
		}
	}
	if docs == nil {
		docs, err = p.documents(ctx)
		if err != nil {
			return err
		}
	}
	for id, pending := range p.writes {
		if pending.deleted {
			delete(docs, id)
		} else {
			docs[id] = pending.doc
		}
	}

	// A deleted bead's outgoing edges would disappear silently; require explicit
	// unlink operations first so the change log represents each removed edge.
	for _, pending := range p.writes {
		if pending.deleted && len(pending.doc.Links) != 0 {
			return failure("ontology_guard_failed", "unlink outgoing edges before deletion")
		}
	}
	if err := ValidateGraphDocuments(ctx, docs); err != nil {
		return err
	}
	p.graphValidated, err = p.graphCoverageActive(ctx)
	return err
}
