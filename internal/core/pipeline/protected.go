package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ProtectedRecordChange is the detached source/row image used by a protected
// management operation. The caller supplies the complete before image so the
// Writer can prove that the plan was built from the records it is about to
// replace. The driver still assigns the committed revision.
type ProtectedRecordChange struct {
	Ref          model.Ref
	Kind         string
	FromRevision model.Revision
	Before       []byte
	After        []byte
	ChangedPaths []string
	Operation    model.OpKind
}

// ProtectedApplyRequest is intentionally narrower than ChangeSet. It is only
// usable by a host-controlled protected method after its plan and proof have
// been admitted by the service layer.
type ProtectedApplyRequest struct {
	PlanDigest   string
	BaseRevision model.Revision
	Source       ProtectedRecordChange
	Rows         []ProtectedRecordChange
}

// ProtectedApplier is the small seam consumed by ontology and other future
// host-owned management services. It remains on the canonical Writer so
// source, row, receipt, history and lease publication share one transaction.
type ProtectedApplier interface {
	ApplyProtected(context.Context, model.PrincipalRef, ProtectedApplyRequest, model.WriteMeta) (port.WriteOutcome, error)
}

var _ ProtectedApplier = (*Writer)(nil)

// ProtectedRequestHash is the durable request identity used by the Writer's
// idempotency receipt. Human proof is deliberately excluded: callers may
// retry the same digest-bound operation with a newly obtained live proof.
func ProtectedRequestHash(req ProtectedApplyRequest, meta model.WriteMeta) (string, error) {
	value := struct {
		Request         ProtectedApplyRequest
		Method, Project string
		Return          string
		DryRun          bool
	}{req, meta.Method, meta.Project, meta.Return, meta.DryRun}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (w *Writer) ApplyProtected(ctx context.Context, by model.PrincipalRef, req ProtectedApplyRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	if w.err != nil {
		return port.WriteOutcome{}, failure("invalid_params", w.err.Error())
	}
	if meta.Method == "" {
		meta.Method = "ontology.apply"
	}
	if meta.Method != "ontology.apply" {
		return port.WriteOutcome{}, failure("invalid_params", "protected apply requires ontology.apply")
	}
	if meta.Return == "" {
		meta.Return = "full"
	}
	if meta.Return != "full" && meta.Return != "minimal" {
		return port.WriteOutcome{}, failure("invalid_params", "return must be full or minimal")
	}
	if !validKey(meta.IdempotencyKey) {
		return port.WriteOutcome{}, failure("invalid_params", "idempotency key must encode at least 128 bits as UUIDv4, ULID, hex or base64url")
	}
	if err := validateProtectedRequest(req); err != nil {
		return port.WriteOutcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return port.WriteOutcome{}, err
	}
	select {
	case <-w.lease.Lost():
		return port.WriteOutcome{}, failure("lease_not_held", "active storage lease has been lost")
	default:
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", err.Error())
	}
	if meta.AgentLabel == "" {
		meta.AgentLabel = meta.Agent
	}
	if by.Kind != "anonymous" {
		meta.AgentLabel = ""
	}
	hash, err := ProtectedRequestHash(req, meta)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", err.Error())
	}
	if meta.DryRun {
		commit, err := w.dryRunProtected(ctx, by, req, meta)
		return port.WriteOutcome{Commit: commit}, err
	}
	for attempt := 0; attempt <= 3; attempt++ {
		reader, err := w.reader(ctx)
		if err != nil {
			return port.WriteOutcome{}, err
		}
		if _, err := w.authorizeProtected(ctx, port.AuthorizeReplay, by, req, meta, reader); err != nil {
			_ = reader.Close()
			return port.WriteOutcome{}, err
		}
		result, found, replayErr := protectedReplay(ctx, reader, principal, meta, hash)
		_ = reader.Close()
		if found || replayErr != nil {
			return port.WriteOutcome{Commit: result, Replayed: found && replayErr == nil}, replayErr
		}

		commit, err := w.applyProtectedOnce(ctx, by, req, meta, principal, hash)
		if errors.Is(err, errStale) || errors.Is(err, driver.ErrConflict) {
			continue
		}
		if err == nil {
			return port.WriteOutcome{Commit: commit}, nil
		}
		var indeterminate driver.ErrIndeterminate
		if errors.As(err, &indeterminate) {
			fresh, readErr := w.reader(ctx)
			if readErr == nil {
				if _, authErr := w.authorizeProtected(ctx, port.AuthorizeReplay, by, req, meta, fresh); authErr != nil {
					_ = fresh.Close()
					return port.WriteOutcome{}, authErr
				}
				result, found, replayErr := protectedReplay(ctx, fresh, principal, meta, hash)
				_ = fresh.Close()
				if found || replayErr != nil {
					return port.WriteOutcome{Commit: result, Replayed: found && replayErr == nil}, replayErr
				}
			}
		}
		return port.WriteOutcome{}, mapProtectedError(err)
	}
	return port.WriteOutcome{}, failure("write_conflict", "protected plan became stale during all four commit attempts")
}

func (w *Writer) dryRunProtected(ctx context.Context, by model.PrincipalRef, req ProtectedApplyRequest, meta model.WriteMeta) (model.Commit, error) {
	reader, err := w.reader(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	defer reader.Close()
	if _, err := w.authorizeProtected(ctx, port.AuthorizeBefore, by, req, meta, reader); err != nil {
		return model.Commit{}, err
	}
	if reader.Snapshot().At == nil || reader.Snapshot().At.String() != req.BaseRevision.String() {
		return model.Commit{}, failure("ontology_plan_stale", "protected plan base revision changed")
	}
	changes := protectedRecordChanges(req)
	refs := make([]model.Ref, 0, 1+len(req.Rows))
	refs = append(refs, req.Source.Ref)
	for _, row := range req.Rows {
		refs = append(refs, row.Ref)
	}
	records, err := reader.Get(ctx, refs)
	if err != nil {
		return model.Commit{}, err
	}
	if len(records) != len(refs) {
		return model.Commit{}, failure("ontology_plan_stale", "protected plan record inventory changed")
	}
	for _, change := range append([]ProtectedRecordChange{req.Source}, req.Rows...) {
		found := false
		for _, record := range records {
			if record.Ref.ID == change.Ref.ID && record.Ref.Branch.String() == change.Ref.Branch.String() {
				if record.Kind != change.Kind || record.Revision.String() != change.FromRevision.String() || !bytes.Equal(record.Body, change.Before) {
					return model.Commit{}, failure("ontology_plan_stale", "protected plan no longer matches the stored source or rows")
				}
				found = true
				break
			}
		}
		if !found {
			return model.Commit{}, failure("ontology_plan_stale", "protected plan record inventory changed")
		}
	}
	head := *reader.Snapshot().At
	revision := head
	if len(changes) != 0 {
		n, err := head.Uint64()
		if err != nil || n == math.MaxUint64 {
			return model.Commit{}, failure("write_conflict", "driver revision space is exhausted")
		}
		revision = model.Natural64(n + 1)
		if _, err := protectedEvents(revision, changes, protectedChangeRecords(changes), meta); err != nil {
			return model.Commit{}, err
		}
	}
	response := model.Commit{Revision: revision, Results: []model.OpResult{}, Warnings: []model.Warning{}, WouldCommit: len(changes) != 0}
	for _, change := range changes {
		result := model.OpResult{Ref: change.Ref, Revision: revision}
		if meta.Return != "minimal" {
			result.Record = append([]byte(nil), change.After...)
		}
		response.Results = append(response.Results, result)
	}
	if _, err := w.authorizeProtected(ctx, port.AuthorizeAfter, by, req, meta, reader); err != nil {
		return model.Commit{}, err
	}
	return response, nil
}

func validateProtectedRequest(req ProtectedApplyRequest) error {
	if req.PlanDigest == "" || len(req.PlanDigest) != sha256.Size*2 || !isLowerHex(req.PlanDigest) {
		return failure("invalid_params", "protected apply requires a canonical plan digest")
	}
	if req.BaseRevision == "" || req.BaseRevision.Validate() != nil {
		return failure("ontology_plan_stale", "protected plan has no valid base revision")
	}
	if err := validateProtectedChange(req.Source, "ontology", true); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, row := range req.Rows {
		if err := validateProtectedChange(row, "bead", false); err != nil {
			return err
		}
		before, err := DecodeDocument(row.Before)
		if err != nil || before.ID != row.Ref.ID {
			return failure("invalid_params", "protected plan contains an invalid bead before image")
		}
		after, err := DecodeDocument(row.After)
		if err != nil || after.ID != row.Ref.ID {
			return failure("invalid_params", "protected plan contains an invalid bead after image")
		}
		if seen[row.Ref.ID] {
			return failure("invalid_params", "protected plan contains a duplicate bead row")
		}
		seen[row.Ref.ID] = true
	}
	return nil
}

func validateProtectedChange(change ProtectedRecordChange, kind string, source bool) error {
	if change.Kind != kind || !change.Ref.IsID() || !change.Ref.Branch.IsZero() || change.Ref.ID == "" || change.FromRevision.Validate() != nil || len(change.Before) == 0 || len(change.After) == 0 {
		return failure("invalid_params", "protected plan contains an invalid record image")
	}
	if !json.Valid(change.Before) || !json.Valid(change.After) {
		return failure("invalid_params", "protected plan contains invalid JSON record bytes")
	}
	if source {
		if change.Ref.ID != "ontology" && !strings.HasPrefix(change.Ref.ID, "ontology/") {
			return failure("protected_record", "protected source must use an ontology ID")
		}
	}
	if change.Operation != "" && change.Operation != model.OpPatch {
		return failure("invalid_params", "ontology migration rows must be patch operations")
	}
	return nil
}

func isLowerHex(value string) bool {
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func protectedReplay(ctx context.Context, reader driver.Reader, principal string, meta model.WriteMeta, hash string) (model.Commit, bool, error) {
	entry, err := reader.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey)
	if err != nil {
		return model.Commit{}, false, err
	}
	if entry == nil {
		return model.Commit{}, false, nil
	}
	if entry.RequestHash != hash {
		return model.Commit{}, true, failure("idempotency_key_reused", "idempotency key was used with different parameters")
	}
	if entry.ResponsePending {
		return model.Commit{}, true, failure("busy", "protected apply receipt is still pending")
	}
	return entry.Commit, true, nil
}

func (w *Writer) applyProtectedOnce(ctx context.Context, by model.PrincipalRef, req ProtectedApplyRequest, meta model.WriteMeta, principal, hash string) (model.Commit, error) {
	reader, err := w.reader(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	defer reader.Close()
	tracked := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	authority, err := w.authorizeProtected(ctx, port.AuthorizeBefore, by, req, meta, tracked)
	if err != nil {
		return model.Commit{}, err
	}
	if reader.Snapshot().At == nil || reader.Snapshot().At.String() != req.BaseRevision.String() {
		return model.Commit{}, failure("ontology_plan_stale", "protected plan base revision changed")
	}
	recordChanges := protectedRecordChanges(req)
	locks, err := tracked.locks(nil, nil, lockKeys(protectedLocks(req))...)
	if err != nil {
		return model.Commit{}, err
	}
	changes := protectedChangeRecords(recordChanges)
	tx, err := w.store.Write(ctx, w.lease)
	if err != nil {
		return model.Commit{}, err
	}
	defer tx.Close()
	defer tx.Rollback(context.WithoutCancel(ctx))
	states, err := tx.Lock(ctx, locks)
	if err != nil {
		return model.Commit{}, err
	}
	if len(states) != len(locks) {
		return model.Commit{}, fmt.Errorf("driver failed to acquire the complete protected lock set")
	}
	if err := tracked.check(states); err != nil {
		return model.Commit{}, err
	}
	for _, change := range append([]ProtectedRecordChange{req.Source}, req.Rows...) {
		index := recordStateIndex(locks, change.Ref)
		if index < 0 {
			return model.Commit{}, errStale
		}
		state := states[index]
		if !state.Present || state.Record == nil || state.Revision.String() != change.FromRevision.String() || !bytes.Equal(state.Record.Body, change.Before) {
			return model.Commit{}, errStale
		}
	}
	head, err := tx.Head(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	if head.String() != req.BaseRevision.String() {
		return model.Commit{}, errStale
	}
	if prior, err := tx.IdempotencyByKey(ctx, principal, meta.AgentLabel, meta.Method, meta.IdempotencyKey); err != nil {
		return model.Commit{}, err
	} else if prior != nil {
		return model.Commit{}, errStale
	}
	// The current policy/source decision is guarded by every recorded lock,
	// including ACL/context generations and absence. Recheck the live login
	// before staging publication, without inverting the host source's lock order.
	if err := w.revalidateProtectedContext(ctx, by); err != nil {
		return model.Commit{}, err
	}
	if len(changes) == 0 {
		noop, ok := tx.(driver.NoopCommitter)
		if !ok {
			return model.Commit{}, driver.ErrUnsupported
		}
		response := model.Commit{Revision: head, Results: []model.OpResult{}, Warnings: []model.Warning{}}
		keys := lockKeys(locks)
		return noop.CommitNoop(ctx, driver.ChangesetHeader{Incarnation: reader.Snapshot().Incarnation, Kind: model.ChangesetMigration, Time: w.cfg.now().UTC(), Principal: by, Chain: authority.Chain, IdempotencyKey: meta.IdempotencyKey, Method: meta.Method, AgentLabel: meta.AgentLabel, RequestHash: hash, Response: response, LockKeys: keys})
	}
	n, err := head.Uint64()
	if err != nil || n == math.MaxUint64 {
		return model.Commit{}, failure("write_conflict", "driver revision space is exhausted")
	}
	revision := model.Natural64(n + 1)
	events, err := protectedEvents(revision, recordChanges, changes, meta)
	if err != nil {
		return model.Commit{}, err
	}
	for i := range events {
		events[i].Provenance.Chain = append([]string{}, authority.Chain...)
		events[i].Provenance.Cause = authority.Cause
	}
	if err := (model.Changeset{Revision: revision, Changes: changes, Chain: authority.Chain, Events: events}).ValidateEvents(); err != nil {
		return model.Commit{}, failure("invalid_params", "invalid protected authorization provenance: "+err.Error())
	}
	response := model.Commit{Revision: revision, Results: []model.OpResult{}, Warnings: []model.Warning{}}
	for _, change := range recordChanges {
		if err := PutSourceRecord(ctx, tx, driver.Record{Ref: change.Ref, Kind: change.Kind, Body: append([]byte(nil), change.After...)}); err != nil {
			return model.Commit{}, err
		}
		result := model.OpResult{Ref: change.Ref, Revision: revision}
		if meta.Return != "minimal" {
			result.Record = append([]byte(nil), change.After...)
		}
		response.Results = append(response.Results, result)
	}
	sentinels := protectedSentinels(req)
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		return model.Commit{}, err
	}
	entry, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: reader.Snapshot().Incarnation, Kind: model.ChangesetMigration, Time: w.cfg.now().UTC(), Principal: by, Chain: append([]string{}, authority.Chain...), IdempotencyKey: meta.IdempotencyKey, Method: meta.Method, AgentLabel: meta.AgentLabel, RequestHash: hash, Response: response, Changes: changes, Events: events, LockKeys: lockKeys(locks)})
	if err != nil {
		return model.Commit{}, err
	}
	if entry.Revision.String() != revision.String() {
		return model.Commit{}, driver.ErrIndeterminate{IdempotencyKey: meta.IdempotencyKey}
	}
	return response, nil
}

// A protected migration has actual source/row images and an admitted method,
// so its complete event plan belongs in the same atomic commit. A reader must
// never infer slot provenance from broad /fields patches in a legacy log row.
func protectedEvents(revision model.Revision, records []ProtectedRecordChange, changes []model.ChangeRecord, meta model.WriteMeta) ([]model.EventDescriptor, error) {
	if len(records) > model.MaxChangesetEvents {
		return nil, failure("scope_too_large", "complete atomic event plan exceeds 1000 descriptors")
	}
	events := make([]model.EventDescriptor, 0, len(records))
	for index, record := range records {
		kind := model.EventBeadUpdated
		if record.Kind == "ontology" {
			kind = model.EventOntologyApplied
		}
		owners := map[string]string{}
		for _, path := range record.ChangedPaths {
			owners[path] = "core"
		}
		events = append(events, model.EventDescriptor{
			Index: model.Natural64(uint64(index)), ChangeIndex: model.Natural64(uint64(index)),
			Type: kind, Resource: model.AuditResource{Kind: record.Kind, Ref: record.Ref},
			Provenance: model.EventProvenance{
				Method: meta.Method, SetBy: owners, Chain: []string{},
				Cause: model.EventCause{Root: meta.IdempotencyKey, Depth: model.Natural64(0), Chain: []string{}},
			},
		})
	}
	entry := model.Changeset{Revision: revision, Changes: changes, Chain: []string{}, Events: events}
	if err := entry.ValidateEvents(); err != nil {
		return nil, failure("invalid_params", "invalid protected event plan: "+err.Error())
	}
	return events, nil
}

func protectedRecordChanges(req ProtectedApplyRequest) []ProtectedRecordChange {
	changes := make([]ProtectedRecordChange, 0, 1+len(req.Rows))
	if !bytes.Equal(req.Source.Before, req.Source.After) {
		changes = append(changes, req.Source)
	}
	rows := append([]ProtectedRecordChange(nil), req.Rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Ref.ID < rows[j].Ref.ID })
	for _, row := range rows {
		if !bytes.Equal(row.Before, row.After) {
			changes = append(changes, row)
		}
	}
	return changes
}

func protectedLocks(req ProtectedApplyRequest) driver.LockSet {
	refs := make([]model.Ref, 0, 1+len(req.Rows))
	refs = append(refs, req.Source.Ref)
	for _, row := range req.Rows {
		refs = append(refs, row.Ref)
	}
	seen := map[string]bool{}
	requests := make([]driver.LockRequest, 0, 2+len(refs)*2)
	add := func(key driver.LockKey) {
		encoded, _ := driver.EncodeLockKey(key)
		if seen[encoded] {
			return
		}
		seen[encoded] = true
		requests = append(requests, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
	}
	for _, ref := range refs {
		ref := ref
		add(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	}
	add(driver.OntologySentinelLock)
	if len(changedRows(req)) != 0 {
		add(driver.GraphBlockingSentinelLock)
	}
	for _, row := range changedRows(req) {
		sentinel, _ := driver.NewBeadSentinel(row.Ref.ID)
		add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	sort.Slice(requests, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(requests[i].Key)
		b, _ := driver.EncodeLockKey(requests[j].Key)
		return a < b
	})
	return requests
}

func protectedChangeRecords(changes []ProtectedRecordChange) []model.ChangeRecord {
	out := make([]model.ChangeRecord, 0, len(changes))
	for _, change := range changes {
		paths := append([]string(nil), change.ChangedPaths...)
		sort.Strings(paths)
		out = append(out, model.ChangeRecord{Ref: change.Ref, Op: model.OpPatch, FromRevision: change.FromRevision, ChangedPaths: paths})
	}
	return out
}

func protectedSentinels(req ProtectedApplyRequest) []driver.Sentinel {
	set := map[driver.Sentinel]bool{driver.SentinelOntology: true}
	if len(changedRows(req)) != 0 {
		set[driver.SentinelGraphBlocking] = true
	}
	for _, row := range changedRows(req) {
		if sentinel, err := driver.NewBeadSentinel(row.Ref.ID); err == nil {
			set[sentinel] = true
		}
	}
	out := make([]driver.Sentinel, 0, len(set))
	for sentinel := range set {
		out = append(out, sentinel)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func changedRows(req ProtectedApplyRequest) []ProtectedRecordChange {
	rows := make([]ProtectedRecordChange, 0, len(req.Rows))
	for _, row := range req.Rows {
		if !bytes.Equal(row.Before, row.After) {
			rows = append(rows, row)
		}
	}
	return rows
}

func recordStateIndex(locks driver.LockSet, ref model.Ref) int {
	for i, lock := range locks {
		if lock.Key.Kind == driver.LockRecord && lock.Key.Ref != nil && lock.Key.Ref.ID == ref.ID {
			return i
		}
	}
	return -1
}

func lockKeys(locks driver.LockSet) []driver.LockKey {
	out := make([]driver.LockKey, len(locks))
	for i, lock := range locks {
		out[i] = lock.Key
	}
	return out
}

func mapProtectedError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errStale) {
		return failure("ontology_plan_stale", "protected plan no longer matches the stored source or rows")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return failure("busy", "protected apply was canceled or exceeded its deadline")
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		return &model.Error{Data: model.ErrorData{Code: code, Details: details}}
	}
	return err
}
