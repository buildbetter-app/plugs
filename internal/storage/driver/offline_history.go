package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// OfflineHistoryTx is an optional trusted-host capability. The caller validates
// source evidence before admission, locks the complete final image and calls
// Head. Preparation binds a detached payload to that admission. Commit publishes
// every imported version, changeset, head record, floor, gap marker and receipt
// together, or none. Ordinary Commit must refuse a prepared history import.
// Imported revisions describe import order, never inferred source transactions.
type OfflineHistoryTx interface {
	PrepareOfflineHistory(context.Context, RestoreImportProfile, OfflineHistory, time.Time) (Meta, error)
	CommitOfflineHistory(context.Context, ChangesetHeader) (model.Changeset, error)
}

const OfflineHistoryEvidenceID = "plug-restore-history"

// SourceHistoryGap is an inclusive unavailable source sequence range. A partial
// replay retains a contiguous suffix; every earlier sequence is accounted for
// explicitly, including known history deliberately left outside that suffix.
type SourceHistoryGap struct {
	First  uint64 `json:"first"`
	Last   uint64 `json:"last"`
	Reason string `json:"reason"`
}

// SourceHistoryChange carries complete converted images. Nil Before means a
// proven creation, nil After a proven deletion; two nil images are invalid.
// Missing images cannot be filled from the archive's current head.
type SourceHistoryChange struct {
	Before *Record `json:"before,omitempty"`
	After  *Record `json:"after,omitempty"`
}

// SourceHistoryBatch is an explicitly ordered import unit, not an assertion
// about source SQL transactions. Event and commit IDs are untrusted provenance.
type SourceHistoryBatch struct {
	SourceStoreID  string                `json:"sourceStoreId,omitempty"`
	SourceSequence uint64                `json:"sourceSequence,omitempty"`
	Project        string                `json:"project,omitempty"`
	Sequence       uint64                `json:"sequence"`
	EventIDs       []string              `json:"eventIds"`
	CommitIDs      []string              `json:"commitIds,omitempty"`
	Changes        []SourceHistoryChange `json:"changes"`
}

// OfflineHistory is trusted source verification output, never an RPC authority
// object. Verified must be established by the source adapter's range/image
// verifier, not an operator flag. Baseline is a complete converted state just
// before First; it is required for a partial suffix and is never synthesized.
type OfflineHistory struct {
	Source           string               `json:"source"`
	Verified         bool                 `json:"verified"`
	Complete         bool                 `json:"complete"`
	First            uint64               `json:"first"`
	Head             uint64               `json:"head"`
	BaselineVerified bool                 `json:"baselineVerified"`
	Baseline         []Record             `json:"baseline,omitempty"`
	Gaps             []SourceHistoryGap   `json:"gaps"`
	Batches          []SourceHistoryBatch `json:"batches"`
}

type SourceImportRevision struct {
	SourceStoreID  string         `json:"sourceStoreId,omitempty"`
	SourceSequence uint64         `json:"sourceSequence,omitempty"`
	Project        string         `json:"project,omitempty"`
	Sequence       uint64         `json:"sequence"`
	Revision       model.Revision `json:"revision"`
	EventIDs       []string       `json:"eventIds"`
	CommitIDs      []string       `json:"commitIds,omitempty"`
}
type HistoryImportEvidence struct {
	Source   string                 `json:"source"`
	Mode     RestoreHistoryMode     `json:"mode"`
	Complete bool                   `json:"complete"`
	First    uint64                 `json:"first"`
	Head     uint64                 `json:"head"`
	Floor    model.Revision         `json:"floor"`
	Gaps     []SourceHistoryGap     `json:"gaps"`
	Mapping  []SourceImportRevision `json:"mapping"`
}

// OfflineHistoryPlan is detached validation output. Drivers build it again
// from the bound payload at publication; callers cannot submit a prepared plan.
type OfflineHistoryRow struct {
	Record  Record
	Deleted bool
}

type OfflineHistoryPlan struct {
	Meta     Meta
	Records  []Record
	Rows     []OfflineHistoryRow
	Versions []Version
	Entries  []model.Changeset
	Evidence HistoryImportEvidence
}

func historyRecordKey(r Record) (string, error) {
	if err := r.Ref.Validate(); err != nil {
		return "", err
	}
	if !r.Ref.Branch.IsZero() || strings.TrimSpace(r.Kind) != r.Kind || r.Kind == "" || !json.Valid(r.Body) {
		return "", fmt.Errorf("invalid history record image")
	}
	return EncodeLockKey(LockKey{Kind: LockRecord, Ref: &r.Ref})
}
func sameHistoryRecord(a, b Record) bool {
	return a.Ref == b.Ref && a.Kind == b.Kind && bytes.Equal(a.Body, b.Body)
}

// BuildOfflineHistory checks coverage, exact preimages and final-image equality
// before allocating any driver state. All principals/times are the admitted
// restore actor/time; source attribution stays in the retained source archive.
func BuildOfflineHistory(current Meta, profile RestoreImportProfile, input OfflineHistory, when time.Time, tail []Record) (OfflineHistoryPlan, error) {
	return buildOfflineHistory(current, profile, input, when, tail, false)
}

// BuildSourcePlaneHistory is the joined, full-history adapter. It retains the
// complete source plan in the admission hash; the core-only API still refuses
// source namespaces. Target lifetime is checked again by transaction preparation.
func BuildSourcePlaneHistory(current Meta, profile RestoreImportProfile, input OfflineHistory, when time.Time, tail []Record) (OfflineHistoryPlan, error) {
	if profile.SourcePlanes == nil || profile.HistoryMode != RestoreHistoryModeFull {
		return OfflineHistoryPlan{}, sourcePlaneError("joint history requires full policy and a complete source plan")
	}
	return buildOfflineHistory(current, profile, input, when, tail, true)
}

func buildOfflineHistory(current Meta, profile RestoreImportProfile, input OfflineHistory, when time.Time, tail []Record, joint bool) (OfflineHistoryPlan, error) {
	if !current.Head.IsZero() || !current.HistoryFloor.IsZero() {
		return OfflineHistoryPlan{}, ErrRestoreTargetNotEmpty
	}
	detach := profile.DetachedCoreOnly
	if joint {
		detach = profile.Detached
	}
	p, err := detach()
	if err != nil {
		return OfflineHistoryPlan{}, err
	}
	refuse := func(reason RestoreHistoryReason) (OfflineHistoryPlan, error) {
		return OfflineHistoryPlan{}, ErrRestoreHistoryRefused{Mode: p.HistoryMode, History: RestoreHistoryAvailableUnverified, Reason: reason}
	}
	if p.HistoryMode == RestoreHistoryModeSnapshot {
		return refuse(RestoreHistoryReasonUnmappable)
	}
	if !input.Verified {
		return refuse(RestoreHistoryReasonAvailableUnverified)
	}
	if input.Source == "" || strings.TrimSpace(input.Source) != input.Source || strings.ContainsRune(input.Source, '\x00') || when.IsZero() {
		return refuse(RestoreHistoryReasonUnmappable)
	}
	// Copy every body, principal and provenance slice before validation binds it.
	data, err := json.Marshal(input)
	if err != nil {
		return OfflineHistoryPlan{}, err
	}
	if len(data) > 128<<20 {
		return refuse(RestoreHistoryReasonUnmappable)
	}
	var h OfflineHistory
	if err = json.Unmarshal(data, &h); err != nil {
		return OfflineHistoryPlan{}, err
	}
	if h.First == 0 || h.Head < h.First || h.Head-h.First != uint64(len(h.Batches))-1 || len(h.Batches) == 0 || len(h.Batches) > 100000 {
		return refuse(RestoreHistoryReasonNonContiguous)
	}
	partial := h.First > 1
	if p.HistoryMode == RestoreHistoryModeFull && (!h.Complete || partial || len(h.Gaps) != 0 || len(h.Baseline) != 0) {
		return refuse(RestoreHistoryReasonNonContiguous)
	}
	if !partial && (!h.Complete || len(h.Gaps) != 0 || len(h.Baseline) != 0) {
		return refuse(RestoreHistoryReasonNonContiguous)
	}
	if partial {
		if h.Complete || !h.BaselineVerified || len(h.Gaps) == 0 {
			return refuse(RestoreHistoryReasonNonContiguous)
		}
		next := uint64(1)
		for _, g := range h.Gaps {
			if g.First != next || g.Last < g.First || g.Last >= h.First || strings.TrimSpace(g.Reason) == "" {
				return refuse(RestoreHistoryReasonNonContiguous)
			}
			next = g.Last + 1
		}
		if next != h.First {
			return refuse(RestoreHistoryReasonNonContiguous)
		}
	}
	meta, err := NewOfflineRestoreMeta(current, p.SourceStoreID)
	if err != nil {
		return OfflineHistoryPlan{}, err
	}
	meta.HistoryFloor = model.Natural64(0)
	plan := OfflineHistoryPlan{Meta: meta, Evidence: HistoryImportEvidence{Source: h.Source, Mode: p.HistoryMode, Complete: h.Complete, First: h.First, Head: h.Head, Gaps: append([]SourceHistoryGap{}, h.Gaps...), Mapping: []SourceImportRevision{}}}
	state := map[string]Record{}
	counts := map[string]int{}
	last := map[string]model.Revision{}
	deleted := map[string]bool{}
	kinds := map[string]string{}
	var rev uint64
	evidenceRef := model.RefByID(OfflineHistoryEvidenceID, model.Natural64(0))
	evidenceKey, _ := EncodeLockKey(LockKey{Kind: LockRecord, Ref: &evidenceRef})
	emit := func(changes []SourceHistoryChange, key string) error {
		if rev == math.MaxUint64 {
			return fmt.Errorf("history revision exhausted")
		}
		rev++
		revision := model.Natural64(rev)
		entry := model.Changeset{Revision: revision, Branch: model.Natural64(0), Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Time: when, Principal: p.Admission.Actor, Chain: []string{}, IdempotencyKey: key, Changes: []model.ChangeRecord{}}
		seen := map[string]bool{}
		for _, c := range changes {
			var record Record
			if c.After != nil {
				record = *c.After
			} else if c.Before != nil {
				record = *c.Before
			} else {
				return fmt.Errorf("empty source delta")
			}
			id, e := historyRecordKey(record)
			if e != nil {
				return e
			}
			if seen[id] {
				return fmt.Errorf("duplicate history delta")
			}
			seen[id] = true
			if previousKind, ok := kinds[id]; ok && previousKind != record.Kind {
				return fmt.Errorf("history changes record kind")
			}
			kinds[id] = record.Kind
			before, present := state[id]
			if c.Before == nil && present || c.Before != nil && (!present || !sameHistoryRecord(before, *c.Before)) {
				return fmt.Errorf("unproven history preimage")
			}
			if c.Before != nil {
				if _, e = historyRecordKey(*c.Before); e != nil {
					return e
				}
				if c.Before.Ref != record.Ref || c.Before.Kind != record.Kind {
					return fmt.Errorf("history delta changes identity/kind")
				}
			}
			if c.Before != nil && c.After != nil && sameHistoryRecord(*c.Before, *c.After) {
				return fmt.Errorf("source delta has no durable change")
			}
			if c.After != nil {
				if e = p.ValidateRecord(*c.After, when); e != nil {
					return e
				}
			}
			v := Version{Ref: record.Ref, Revision: revision, CreatedAt: when, Principal: p.Admission.Actor, Deleted: c.After == nil, Base: counts[id]%VersionBaseEvery == 0 || deleted[id]}
			op := model.OpPatch
			if c.Before == nil {
				op = model.OpCreate
			}
			if c.After == nil {
				op = model.OpDelete
				delete(state, id)
			} else {
				record.Revision = revision
				record.Body = bytes.Clone(record.Body)
				state[id] = record
				if v.Base {
					v.Body = bytes.Clone(record.Body)
				} else {
					v.Patch, e = json.Marshal([]map[string]any{{"op": "replace", "path": "", "value": json.RawMessage(record.Body)}})
					if e != nil {
						return e
					}
				}
			}
			from := last[id]
			if from == "" {
				from = model.Natural64(0)
			}
			entry.Changes = append(entry.Changes, model.ChangeRecord{Ref: record.Ref, Op: op, FromRevision: from, ChangedPaths: []string{""}})
			row := record
			row.Revision = revision
			row.Body = bytes.Clone(row.Body)
			if c.After == nil {
				row.Body = nil
			}
			plan.Rows = append(plan.Rows, OfflineHistoryRow{Record: row, Deleted: c.After == nil})
			plan.Versions = append(plan.Versions, v)
			counts[id]++
			last[id] = revision
			deleted[id] = c.After == nil
		}
		plan.Entries = append(plan.Entries, entry)
		return nil
	}
	if partial {
		base := []SourceHistoryChange{}
		for i := range h.Baseline {
			id, e := historyRecordKey(h.Baseline[i])
			if e != nil || id == evidenceKey {
				return refuse(RestoreHistoryReasonUnmappable)
			}
			base = append(base, SourceHistoryChange{After: &h.Baseline[i]})
		}
		if err = emit(base, ""); err != nil {
			return refuse(RestoreHistoryReasonUnmappable)
		}
		plan.Meta.HistoryFloor = model.Natural64(1)
	}
	type scope struct{ store, project string }
	type eventKey struct {
		scope
		id string
	}
	seenEvents := map[eventKey]bool{}
	sourceSequences := map[scope]uint64{}
	for i, b := range h.Batches {
		if b.Sequence != h.First+uint64(i) {
			return refuse(RestoreHistoryReasonNonContiguous)
		}
		origin := scope{b.SourceStoreID, b.Project}
		if b.SourceStoreID != "" || b.SourceSequence != 0 {
			if b.SourceStoreID == "" || b.Project == "" || strings.TrimSpace(b.SourceStoreID) != b.SourceStoreID || strings.TrimSpace(b.Project) != b.Project || strings.ContainsRune(b.SourceStoreID+b.Project, '\x00') {
				return refuse(RestoreHistoryReasonUnmappable)
			}
			previous, present := sourceSequences[origin]
			if !present {
				previous = h.First - 1
			}
			if b.SourceSequence != previous+1 {
				return refuse(RestoreHistoryReasonNonContiguous)
			}
			sourceSequences[origin] = b.SourceSequence
		}
		// A captured Dolt commit may contain no source audit event or no
		// portable business change. Preserve that explicit source unit without
		// inventing an event, mutation or grouping. A provenance-free no-op is
		// never admissible.
		if len(b.EventIDs)+len(b.CommitIDs) == 0 || len(b.Changes) == 0 && len(b.CommitIDs) == 0 {
			return refuse(RestoreHistoryReasonUnmappable)
		}
		for _, id := range b.EventIDs {
			key := eventKey{origin, id}
			if id == "" || strings.TrimSpace(id) != id || strings.ContainsRune(id, '\x00') || seenEvents[key] {
				return refuse(RestoreHistoryReasonUnmappable)
			}
			seenEvents[key] = true
		}
		for _, id := range b.CommitIDs {
			if id == "" || strings.TrimSpace(id) != id || strings.ContainsRune(id, '\x00') {
				return refuse(RestoreHistoryReasonUnmappable)
			}
		}
		for _, c := range b.Changes {
			var r *Record
			if c.After != nil {
				r = c.After
			} else {
				r = c.Before
			}
			if r == nil {
				return refuse(RestoreHistoryReasonUnmappable)
			}
			id, e := historyRecordKey(*r)
			if e != nil || id == evidenceKey {
				return refuse(RestoreHistoryReasonUnmappable)
			}
		}
		if err = emit(b.Changes, ""); err != nil {
			return refuse(RestoreHistoryReasonUnmappable)
		}
		plan.Evidence.Mapping = append(plan.Evidence.Mapping, SourceImportRevision{SourceStoreID: b.SourceStoreID, SourceSequence: b.SourceSequence, Project: b.Project, Sequence: b.Sequence, Revision: model.Natural64(rev), EventIDs: b.EventIDs, CommitIDs: b.CommitIDs})
	}
	// Tail is the independently rebuilt head. Historical records must agree;
	// non-historical records become explicit restore-time creates, never history.
	extra := []SourceHistoryChange{}
	tailSeen := map[string]bool{}
	for i, r := range tail {
		id, e := historyRecordKey(r)
		if e != nil || id == evidenceKey || tailSeen[id] {
			return refuse(RestoreHistoryReasonUnmappable)
		}
		tailSeen[id] = true
		if old, ok := state[id]; ok {
			if !sameHistoryRecord(old, r) {
				return refuse(RestoreHistoryReasonUnmappable)
			}
		} else {
			if _, known := last[id]; known || r.Kind == "bead" || r.Kind == "memory" {
				return refuse(RestoreHistoryReasonUnmappable)
			}
			extra = append(extra, SourceHistoryChange{After: &tail[i]})
		}
	}
	if tail != nil {
		for id := range state {
			if !tailSeen[id] {
				return refuse(RestoreHistoryReasonUnmappable)
			}
		}
	}
	plan.Evidence.Floor = plan.Meta.HistoryFloor
	marker, err := json.Marshal(plan.Evidence)
	if err != nil {
		return OfflineHistoryPlan{}, err
	}
	extra = append(extra, SourceHistoryChange{After: &Record{Ref: evidenceRef, Kind: "beads-archive", Body: marker}})
	if err = emit(extra, p.IdempotencyKey); err != nil {
		return refuse(RestoreHistoryReasonUnmappable)
	}
	plan.Meta.Head = model.Natural64(rev)
	keys := make([]string, 0, len(state))
	for id := range state {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		plan.Records = append(plan.Records, state[id])
	}
	return plan, nil
}

// PreparedOfflineHistory owns detached payload and admission state. Only the
// driver stores it; neither callers nor commit headers can retarget its binding.
type PreparedOfflineHistory struct {
	current Meta
	meta    Meta
	profile RestoreImportProfile
	history OfflineHistory
	when    time.Time
	joint   bool
}

func OfflineHistoryDigest(h OfflineHistory) (string, error) {
	data, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func PrepareOfflineHistoryBinding(current Meta, p RestoreImportProfile, h OfflineHistory, when time.Time) (*PreparedOfflineHistory, error) {
	return prepareOfflineHistoryBinding(current, p, h, when, false)
}

// PrepareSourcePlaneHistoryBinding verifies the real empty target and provider
// lifetime before binding full replay. No partial or snapshot fallback exists.
func PrepareSourcePlaneHistoryBinding(current Meta, p RestoreImportProfile, h OfflineHistory, when time.Time, lifetime CheckpointLifetime) (*PreparedOfflineHistory, error) {
	bound, err := PrepareSourcePlaneHistoryProfile(p, current, lifetime)
	if err != nil {
		return nil, err
	}
	return prepareOfflineHistoryBinding(current, bound, h, when, true)
}

func prepareOfflineHistoryBinding(current Meta, p RestoreImportProfile, h OfflineHistory, when time.Time, joint bool) (*PreparedOfflineHistory, error) {
	detach := p.DetachedCoreOnly
	build := BuildOfflineHistory
	if joint {
		detach, build = p.Detached, BuildSourcePlaneHistory
	}
	bound, err := detach()
	if err != nil {
		return nil, err
	}
	if bound.HistoryTime != "" && when.UTC().Format(time.RFC3339Nano) != bound.HistoryTime {
		return nil, fmt.Errorf("offline history time differs from admitted binding")
	}
	digest, err := OfflineHistoryDigest(h)
	if err != nil {
		return nil, err
	}
	if bound.HistoryDigest == "" || digest != bound.HistoryDigest {
		return nil, fmt.Errorf("offline history payload differs from admitted digest")
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	var detached OfflineHistory
	if err = json.Unmarshal(raw, &detached); err != nil {
		return nil, err
	}
	plan, err := build(current, bound, detached, when, nil)
	if err != nil {
		return nil, err
	}
	return &PreparedOfflineHistory{current: current, meta: plan.Meta, profile: bound, history: detached, when: when, joint: joint}, nil
}
func (b *PreparedOfflineHistory) Meta() Meta { return b.meta }
func (b *PreparedOfflineHistory) Finalize(h ChangesetHeader, tail []Record, locks LockSet, bumps []LockKey) (OfflineHistoryPlan, error) {
	if err := b.profile.ValidateHeader(h); err != nil {
		return OfflineHistoryPlan{}, err
	}
	if h.Incarnation != b.meta.Incarnation || !h.Time.Equal(b.when) || !h.Branch.IsZero() {
		return OfflineHistoryPlan{}, fmt.Errorf("history commit differs from prepared lifetime/time")
	}
	if err := ValidateHeaderLocks(h.LockKeys, locks); err != nil {
		return OfflineHistoryPlan{}, err
	}
	build := BuildOfflineHistory
	if b.joint {
		build = BuildSourcePlaneHistory
	}
	plan, err := build(b.current, b.profile, b.history, b.when, tail)
	if err != nil {
		return OfflineHistoryPlan{}, err
	}
	plan.Meta.Incarnation = b.meta.Incarnation
	for i := range plan.Entries {
		plan.Entries[i].Incarnation = b.meta.Incarnation
	}
	held := map[string]bool{}
	bumped := map[string]bool{}
	for _, r := range locks {
		if r.Mode != LockShare {
			id, _ := EncodeLockKey(r.Key)
			held[id] = true
		}
	}
	for _, k := range bumps {
		id, _ := EncodeLockKey(k)
		bumped[id] = true
	}
	for _, row := range plan.Rows {
		r := row.Record
		key, _ := EncodeLockKey(LockKey{Kind: LockRecord, Ref: &r.Ref})
		if !held[key] {
			return OfflineHistoryPlan{}, fmt.Errorf("history publication requires complete record update locks")
		}
		if r.Kind == "bead" {
			if !r.Ref.IsID() {
				return OfflineHistoryPlan{}, fmt.Errorf("history bead requires resolved id")
			}
			s, err := NewBeadSentinel(r.Ref.ID)
			if err != nil {
				return OfflineHistoryPlan{}, err
			}
			key, _ := EncodeLockKey(LockKey{Kind: LockSentinel, Sentinel: s})
			if !held[key] || !bumped[key] {
				return OfflineHistoryPlan{}, fmt.Errorf("history bead requires its exact sentinel lock and bump")
			}
		}
	}
	return plan, nil
}
