package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// CompactionInput is detached input to a host-supplied summary. The core has
// no outbound client, credential, or implicit summarizer. Text contains only
// the fields that compaction will replace; title and unrelated data stay out.
type CompactionInput struct {
	Ref      model.Ref
	Revision model.Revision
	Tier     int
	ClosedAt time.Time
	Text     map[string]string
}

type CompactionSummaryProvider interface {
	Summary(context.Context, CompactionInput) (string, error)
}

type CompactionSummaryFunc func(context.Context, CompactionInput) (string, error)

func (f CompactionSummaryFunc) Summary(ctx context.Context, input CompactionInput) (string, error) {
	if f == nil {
		return "", compactionUnavailable("a supplied summary provider is required")
	}
	return f(ctx, input)
}

// CompactionConfig is internal host configuration, never RPC input. An absent
// threshold uses the adopted 30/90-day default; an explicit nonpositive value
// is invalid. VerifyPresence must validate an issued D32 proof bound to the
// method and digest; merely receiving a client-shaped object is insufficient.
type CompactionConfig struct {
	Summary        CompactionSummaryProvider
	ThresholdDays  map[int]int
	VerifyPresence func(context.Context, string, string, map[string]any) error
}

func WithCompaction(config CompactionConfig) Option {
	thresholds := make(map[int]int, len(config.ThresholdDays))
	for tier, days := range config.ThresholdDays {
		thresholds[tier] = days
	}
	config.ThresholdDays = thresholds
	return func(s *Service) { s.compaction = config }
}

// CompactionRequest selects the next eligible record in (closed time, ID)
// order. AfterID is an optional exclusive cursor: selection resumes strictly
// after the named record's position in that order. The named record must be a
// closed record in the caller's project; its own current eligibility does not
// matter, so the ID that a refusal names can always be passed back.
type CompactionRequest struct {
	BeforeRevision *model.Revision
	AfterID        *string
	DryRun         bool
}

// CompactionRevertRequest implements only reversal of an actual compaction
// version. It does not expose a generic body/ontology/plugin restore path.
type CompactionRevertRequest struct {
	Ref           model.Ref
	BaseRevision  model.Revision
	Expect        []model.Precondition
	HumanPresence map[string]any
}

const compactionMetadata = "plug.compaction"

var compactionTextFields = [...]string{"description", "design", "notes", "acceptance_criteria"}

// Only pointers into ordinary permanent history are retained here. Original
// text is never duplicated into an auxiliary snapshot or discarded on revert.
type compactionState struct {
	Format         string          `json:"format"`
	Tier           int             `json:"tier"`
	SourceRevision model.Revision  `json:"sourceRevision"`
	BeforeRevision *model.Revision `json:"beforeRevision,omitempty"`
	AfterID        *string         `json:"afterId,omitempty"`
	Time           time.Time       `json:"time"`
	OriginalSize   int             `json:"originalSize"`
}

type compactionCandidate struct {
	row      storedBead
	tier     int
	closedAt time.Time
	text     map[string]string
	size     int
}

func compactionUnavailable(hint string) error { return failure("compaction_unavailable", hint) }

// compactionRefused names the selected record so a caller can pass its ID as
// afterId on the next call. A refusal never writes, so the record is unchanged.
func compactionRefused(candidate compactionCandidate, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: "compaction_unavailable", Hint: hint, Details: map[string]any{
		"id": candidate.row.doc.ID, "tier": candidate.tier,
	}}}
}

// CompactWithOutcome selects one record, in closed-time/ID order. Repeating
// calls is the non-atomic batch; a failed item never publishes another item.
// beforeRevision excludes a selected row at or above the boundary.
func (s *Service) CompactWithOutcome(ctx context.Context, by model.PrincipalRef, req CompactionRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	if req.BeforeRevision != nil && req.BeforeRevision.Validate() != nil {
		return port.WriteOutcome{}, failure("invalid_params", "invalid compaction revision boundary")
	}
	if req.AfterID != nil && strings.TrimSpace(*req.AfterID) != *req.AfterID || req.AfterID != nil && *req.AfterID == "" {
		return port.WriteOutcome{}, failure("invalid_params", "afterId must be a nonempty record ID")
	}
	meta.Method, meta.DryRun = "admin.compact", meta.DryRun || req.DryRun
	if !meta.DryRun {
		cs, found, err := s.compactionReplay(ctx, by, req, meta)
		if err != nil {
			return port.WriteOutcome{}, err
		}
		if found {
			return s.apply(ctx, by, meta.Method, cs, meta, true)
		}
		if s.compaction.Summary == nil {
			return port.WriteOutcome{}, compactionUnavailable("a supplied summary provider is required")
		}
	}
	candidate, err := s.nextCompaction(ctx, by, req, meta.Project)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	probe, err := s.authorizeCompaction(ctx, by, candidate.row.record.Ref, meta)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	if meta.DryRun {
		// This is a selection preview, not a generated or committed summary.
		// The actual Writer has authorized the original row; no provider ran.
		probe.Commit.WouldCommit = true
		return probe, nil
	}
	text := make(map[string]string, len(candidate.text))
	for name, value := range candidate.text {
		text[name] = value
	}
	summary, err := s.compaction.Summary.Summary(ctx, CompactionInput{Ref: candidate.row.record.Ref, Revision: candidate.row.record.Revision, Tier: candidate.tier, ClosedAt: candidate.closedAt, Text: text})
	if err != nil {
		return port.WriteOutcome{}, compactionRefused(candidate, "supplied summary failed")
	}
	if !utf8.ValidString(summary) || len(summary) >= candidate.size {
		return port.WriteOutcome{}, compactionRefused(candidate, "summary must be valid text smaller than the original")
	}
	state := compactionState{Format: "plug-compaction/1", Tier: candidate.tier, SourceRevision: candidate.row.record.Revision, BeforeRevision: req.BeforeRevision, AfterID: req.AfterID, Time: s.now().UTC(), OriginalSize: candidate.size}
	raw, err := json.Marshal(state)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	cs := compactionChangeSet(candidate.row, summary, raw, candidate.tier)
	return s.apply(ctx, by, meta.Method, cs, meta, true)
}

func (s *Service) authorizeCompaction(ctx context.Context, by model.PrincipalRef, ref model.Ref, meta model.WriteMeta) (port.WriteOutcome, error) {
	// Authorization runs in the canonical pipeline before text is supplied to
	// the provider, and again with the actual changes and CAS before commit.
	meta.DryRun = true
	digest := sha256.Sum256([]byte(meta.Method + "\x00compaction-preflight\x00" + meta.IdempotencyKey))
	meta.IdempotencyKey = hex.EncodeToString(digest[:])
	return s.apply(ctx, by, meta.Method, model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: map[string]any{}}}}}, meta, true)
}

func compactionChangeSet(row storedBead, summary string, state json.RawMessage, tier int) model.ChangeSet {
	fields := map[string]any{"description": summary}
	// Preserve an existing configured/imported level field as the same atomic
	// transition. Default records gain no new public field.
	if _, present := row.doc.Fields["compaction_level"]; present {
		fields["compaction_level"] = tier
	}
	for _, name := range compactionTextFields[1:] {
		if _, exists := row.doc.Fields[name]; exists {
			fields[name] = ""
		}
	}
	// Omitted branch and explicit branch zero name the same record, but
	// must produce one request digest across current and historical readers.
	ref := model.RefByID(row.record.Ref.ID, model.Natural64(0))
	return model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref, Fields: fields}},
		{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: compactionMetadata, Value: state}},
	}, Expect: []model.Precondition{model.PreconditionByRevision(ref, row.record.Revision)}}
}

// compactionBefore is the selection order: closed time, then record ID.
func compactionBefore(aAt time.Time, aID string, bAt time.Time, bID string) bool {
	if aAt.Equal(bAt) {
		return aID < bID
	}
	return aAt.Before(bAt)
}

func (s *Service) nextCompaction(ctx context.Context, by model.PrincipalRef, req CompactionRequest, project string) (compactionCandidate, error) {
	if project == "" {
		return compactionCandidate{}, failure("project_required", "supply a project")
	}
	thresholds := map[int]time.Duration{1: 30 * 24 * time.Hour, 2: 90 * 24 * time.Hour}
	for tier, days := range s.compaction.ThresholdDays {
		if (tier != 1 && tier != 2) || days <= 0 || uint64(days) > uint64(math.MaxInt64/int64(24*time.Hour)) {
			return compactionCandidate{}, compactionUnavailable("invalid compaction threshold")
		}
		thresholds[tier] = time.Duration(days) * 24 * time.Hour
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return compactionCandidate{}, err
	}
	defer r.Close()
	graph, err := scanBeads(ctx, r)
	if err != nil {
		return compactionCandidate{}, err
	}
	candidates := []compactionCandidate{}
	now := s.now().UTC()
	for _, id := range sortedIDs(graph) {
		row := graph[id]
		if row.doc.Project != project || req.BeforeRevision != nil && compareNatural(row.record.Revision, *req.BeforeRevision) >= 0 {
			continue
		}
		category, ok := s.ontology.effective(project).Category(state(row.doc).Status)
		if !ok || !category.Completed() {
			continue
		}
		level, err := compactionLevel(row.doc)
		if err != nil {
			continue
		}
		if level >= 2 {
			continue
		}
		closedAt, err := s.compactionClosedAt(ctx, by, r, row)
		if err != nil {
			return compactionCandidate{}, err
		}
		if closedAt.IsZero() || now.Before(closedAt) || now.Sub(closedAt) < thresholds[level+1] {
			continue
		}
		text, size, err := compactionText(row.doc)
		if err != nil {
			return compactionCandidate{}, err
		}
		if size == 0 {
			continue
		}
		candidates = append(candidates, compactionCandidate{row: row, tier: level + 1, closedAt: closedAt, text: text, size: size})
	}
	if req.AfterID != nil {
		// The cursor's position is the named record's own (closed time, ID),
		// resolved from the same snapshot whether or not it is still eligible.
		cursor, ok := graph[*req.AfterID]
		cursorAt := time.Time{}
		if ok && cursor.doc.Project == project {
			category, known := s.ontology.effective(project).Category(state(cursor.doc).Status)
			if known && category.Completed() {
				if cursorAt, err = s.compactionClosedAt(ctx, by, r, cursor); err != nil {
					return compactionCandidate{}, err
				}
			}
		}
		if cursorAt.IsZero() {
			return compactionCandidate{}, failure("invalid_params", "afterId must name a closed record in this project")
		}
		after := candidates[:0]
		for _, candidate := range candidates {
			if compactionBefore(cursorAt, *req.AfterID, candidate.closedAt, candidate.row.doc.ID) {
				after = append(after, candidate)
			}
		}
		candidates = after
	}
	if len(candidates) == 0 {
		return compactionCandidate{}, compactionUnavailable("no eligible record before the revision boundary and after the cursor")
	}
	sort.Slice(candidates, func(i, j int) bool {
		return compactionBefore(candidates[i].closedAt, candidates[i].row.doc.ID, candidates[j].closedAt, candidates[j].row.doc.ID)
	})
	return candidates[0], nil
}

func compactionText(doc pipeline.Document) (map[string]string, int, error) {
	text, size := map[string]string{}, 0
	for _, name := range compactionTextFields {
		value, exists := doc.Fields[name]
		if !exists {
			continue
		}
		valueText, ok := value.(string)
		if !ok || !utf8.ValidString(valueText) || len(valueText) > math.MaxInt-size {
			return nil, 0, compactionUnavailable("compaction text is not valid")
		}
		text[name], size = valueText, size+len(valueText)
	}
	return text, size, nil
}

func readCompactionState(doc pipeline.Document) (compactionState, bool, error) {
	raw, present := doc.Metadata[compactionMetadata]
	if !present {
		return compactionState{}, false, nil
	}
	var value compactionState
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF || value.Format != "plug-compaction/1" || value.Tier < 1 || value.Tier > 2 || value.SourceRevision.Validate() != nil || value.Time.IsZero() || value.OriginalSize <= 0 {
		return compactionState{}, false, compactionUnavailable("invalid retained compaction state")
	}
	return value, true, nil
}

func compactionLevel(doc pipeline.Document) (int, error) {
	value, present, err := readCompactionState(doc)
	if err != nil {
		return 0, err
	}
	if present {
		return value.Tier, nil
	}
	if level, exists := doc.Fields["compaction_level"]; exists {
		raw, err := json.Marshal(level)
		var n int
		if err != nil || json.Unmarshal(raw, &n) != nil || n < 0 || n > 2 {
			return 0, compactionUnavailable("invalid source compaction level")
		}
		return n, nil
	}
	return 0, nil
}

func (s *Service) compactionRecordAt(ctx context.Context, by model.PrincipalRef, ref model.Ref, revision model.Revision, incarnation string) (storedBead, error) {
	r, err := s.reader(ctx, by, &AsOf{Revision: revision, Incarnation: incarnation})
	if err != nil {
		return storedBead{}, historyGone(err)
	}
	defer r.Close()
	record, err := historyRecord(ctx, r, ref)
	if err != nil {
		return storedBead{}, err
	}
	if record == nil || record.Kind != "bead" {
		return storedBead{}, failure("not_found", "compaction version was not found")
	}
	doc, err := decodeDocument(record.Body)
	if err != nil || doc.ID != ref.ID {
		return storedBead{}, failure("internal_error", "invalid compaction history record")
	}
	return storedBead{record: *record, doc: doc}, nil
}

func (s *Service) compactionClosedAt(ctx context.Context, by model.PrincipalRef, r driver.Reader, row storedBead) (time.Time, error) {
	// Imports may carry an explicit close timestamp. A missing/invalid imported
	// timestamp is never fabricated from restore time or the row's last edit.
	if raw, present := row.doc.Fields["closed_at"]; present {
		text, ok := raw.(string)
		if !ok {
			return time.Time{}, nil
		}
		at, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return time.Time{}, nil
		}
		return at.UTC(), nil
	}
	// Ordinary Plug closes have a proven transition timestamp in permanent
	// history. It is the header clock, bound into SQL, not database wall time.
	// A terminal first/base image alone does not prove when a close happened.
	versions, err := historyVersions(ctx, r, row.record.Ref)
	if err != nil {
		return time.Time{}, err
	}
	var closedAt time.Time
	known, wasClosed := false, false
	for _, version := range versions {
		if version.Deleted {
			known, wasClosed, closedAt = false, false, time.Time{}
			continue
		}
		prior, err := s.compactionRecordAt(ctx, by, row.record.Ref, version.Revision, r.Snapshot().Incarnation)
		if err != nil {
			return time.Time{}, err
		}
		category, ok := s.ontology.effective(prior.doc.Project).Category(state(prior.doc).Status)
		closed := ok && category.Completed()
		if !closed {
			closedAt = time.Time{}
		} else if known && !wasClosed {
			closedAt = version.CreatedAt.UTC()
		}
		known, wasClosed = true, closed
	}
	return closedAt, nil
}

func (s *Service) compactionReceipt(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) (*driver.IdempotencyRecord, string, error) {
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return nil, "", err
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		return nil, "", failure("invalid_params", "invalid principal")
	}
	agent := meta.AgentLabel
	if agent == "" {
		agent = meta.Agent
	}
	if by.Kind != "anonymous" {
		agent = ""
	}
	entry, err := r.IdempotencyByKey(ctx, principal, agent, meta.Method, meta.IdempotencyKey)
	return entry, r.Snapshot().Incarnation, mapped(err)
}

func (s *Service) compactionReplay(ctx context.Context, by model.PrincipalRef, req CompactionRequest, meta model.WriteMeta) (model.ChangeSet, bool, error) {
	entry, incarnation, err := s.compactionReceipt(ctx, by, meta)
	if err != nil || entry == nil {
		return model.ChangeSet{}, false, err
	}
	if len(entry.Commit.Results) != 2 || !historyRefEqual(entry.Commit.Results[0].Ref, entry.Commit.Results[1].Ref) {
		return model.ChangeSet{}, true, failure("internal_error", "invalid compaction receipt")
	}
	after, err := s.compactionRecordAt(ctx, by, entry.Commit.Results[0].Ref, entry.Commit.Revision, incarnation)
	if err != nil {
		return model.ChangeSet{}, true, err
	}
	value, present, err := readCompactionState(after.doc)
	if err != nil || !present {
		return model.ChangeSet{}, true, compactionUnavailable("compaction receipt has no retained source")
	}
	if (value.BeforeRevision == nil) != (req.BeforeRevision == nil) || value.BeforeRevision != nil && value.BeforeRevision.String() != req.BeforeRevision.String() {
		return model.ChangeSet{}, true, failure("idempotency_key_reused", "compaction revision boundary changed")
	}
	if (value.AfterID == nil) != (req.AfterID == nil) || value.AfterID != nil && *value.AfterID != *req.AfterID {
		return model.ChangeSet{}, true, failure("idempotency_key_reused", "compaction afterId cursor changed")
	}
	before, err := s.compactionRecordAt(ctx, by, after.record.Ref, value.SourceRevision, incarnation)
	if err != nil {
		return model.ChangeSet{}, true, err
	}
	summary, ok := after.doc.Fields["description"].(string)
	if !ok {
		return model.ChangeSet{}, true, compactionUnavailable("compaction receipt summary is unavailable")
	}
	// History reconstruction may reorder JSON object members. Re-encode the
	// typed state exactly as on first admission before hashing the Writer request.
	raw, err := json.Marshal(value)
	if err != nil {
		return model.ChangeSet{}, true, err
	}
	return compactionChangeSet(before, summary, raw, value.Tier), true, nil
}

// CompactionRevertDigest binds D32 proof to this store incarnation and exact
// request. Proof bytes are excluded so a retry can use a fresh issued proof.
func CompactionRevertDigest(req CompactionRevertRequest, meta model.WriteMeta, incarnation string) (string, error) {
	raw, err := json.Marshal(struct {
		Ref                          model.Ref
		Base                         model.Revision
		Expect                       []model.Precondition
		Incarnation, Project, Return string
		DryRun                       bool
	}{req.Ref, req.BaseRevision, req.Expect, incarnation, meta.Project, meta.Return, meta.DryRun})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func (s *Service) RevertCompactionWithOutcome(ctx context.Context, by model.PrincipalRef, req CompactionRevertRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if err := namedRef(req.Ref); err != nil {
		return port.WriteOutcome{}, err
	}
	if req.BaseRevision.Validate() != nil || req.Expect == nil {
		return port.WriteOutcome{}, failure("invalid_params", "base revision and expectations are required")
	}
	if err := s.admit(ctx, by); err != nil {
		return port.WriteOutcome{}, err
	}
	storeMeta, err := s.store.Meta(ctx)
	if err != nil {
		return port.WriteOutcome{}, mapped(err)
	}
	digest, err := CompactionRevertDigest(req, meta, storeMeta.Incarnation)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", "invalid revert expectations")
	}
	if s.compaction.VerifyPresence == nil || s.compaction.VerifyPresence(ctx, "history.revert", digest, req.HumanPresence) != nil {
		return port.WriteOutcome{}, failure("human_presence_required", "a trusted digest-bound human presence proof is required")
	}
	meta.Method = "history.revert"
	if _, err := s.authorizeCompaction(ctx, by, req.Ref, meta); err != nil {
		return port.WriteOutcome{}, err
	}
	base, err := s.compactionRecordAt(ctx, by, req.Ref, req.BaseRevision, storeMeta.Incarnation)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	value, present, err := readCompactionState(base.doc)
	if err != nil || !present {
		return port.WriteOutcome{}, compactionUnavailable("selected base is not a reversible compaction version")
	}
	original, err := s.compactionRecordAt(ctx, by, req.Ref, value.SourceRevision, storeMeta.Incarnation)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	if original.doc.Project != meta.Project || base.doc.Project != meta.Project || original.doc.Type != base.doc.Type {
		return port.WriteOutcome{}, compactionUnavailable("compaction source identity changed")
	}
	// The selected row must be the immediate successor of the retained source,
	// rather than an arbitrary later user edit which happens to keep metadata.
	r, err := s.reader(ctx, by, &AsOf{Revision: req.BaseRevision, Incarnation: storeMeta.Incarnation})
	if err != nil {
		return port.WriteOutcome{}, historyGone(err)
	}
	versions, versionsErr := historyVersions(ctx, r, req.Ref)
	_ = r.Close()
	if versionsErr != nil {
		return port.WriteOutcome{}, versionsErr
	}
	found := false
	for i := 1; i < len(versions); i++ {
		if versions[i-1].Revision.String() == value.SourceRevision.String() && versions[i].Revision.String() == base.record.Revision.String() {
			found = !versions[i-1].Deleted && !versions[i].Deleted
			break
		}
	}
	if !found {
		return port.WriteOutcome{}, compactionUnavailable("selected version is not the compaction successor")
	}
	fields := map[string]any{}
	expects := append([]model.Precondition{}, req.Expect...)
	revertFields := append(append([]string{}, compactionTextFields[:]...), "compaction_level")
	for _, name := range revertFields {
		old, oldPresent := original.doc.Fields[name]
		after, afterPresent := base.doc.Fields[name]
		oldHash, err := pipeline.FieldHash(old)
		if err != nil {
			return port.WriteOutcome{}, err
		}
		afterHash, err := pipeline.FieldHash(after)
		if err != nil {
			return port.WriteOutcome{}, err
		}
		if oldPresent == afterPresent && oldHash == afterHash {
			continue
		}
		fields[name] = old
		expects = append(expects, model.PreconditionByHash(req.Ref, "fields."+name, afterHash))
	}
	var metadata any
	decoder := json.NewDecoder(bytes.NewReader(base.doc.Metadata[compactionMetadata]))
	decoder.UseNumber()
	if decoder.Decode(&metadata) != nil {
		return port.WriteOutcome{}, compactionUnavailable("compaction metadata is unavailable")
	}
	hash, err := pipeline.FieldHash(metadata)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	expects = append(expects, model.PreconditionByHash(req.Ref, "metadata."+compactionMetadata, hash))
	priorState := original.doc.Metadata[compactionMetadata]
	if len(priorState) == 0 {
		priorState = json.RawMessage("null")
	}
	cs := model.ChangeSet{Ops: []model.Op{
		{Kind: model.OpPatch, Patch: &model.PatchOp{Target: req.Ref, Fields: fields}},
		{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: req.Ref, Key: compactionMetadata, Value: priorState}},
	}, Expect: expects}
	outcome, err := s.apply(ctx, by, meta.Method, cs, meta, true)
	var typed *model.Error
	if errors.As(err, &typed) && typed.Data.Code == "precondition_failed" {
		details := map[string]any{"reason": "revert"}
		for name, value := range typed.Data.Details {
			if name != "reason" {
				details[name] = value
			}
		}
		return port.WriteOutcome{}, &model.Error{Data: model.ErrorData{Code: "precondition_failed", Details: details}}
	}
	return outcome, err
}
