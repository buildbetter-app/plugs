package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	// A version lookup normally scans one page. The cap prevents a malformed
	// or unexpectedly hot record from turning a read into an unbounded scan.
	maxHistoryVersionScan = 10000
	maxDiffOperations     = 1000
)

func historyGone(err error) error {
	var truncated driver.ErrTruncated
	if errors.As(err, &truncated) {
		return &model.Error{Data: model.ErrorData{Code: "history_gone", Details: map[string]any{"floor": truncated.Floor}}}
	}
	var truncatedPtr *driver.ErrTruncated
	if errors.As(err, &truncatedPtr) && truncatedPtr != nil {
		return &model.Error{Data: model.ErrorData{Code: "history_gone", Details: map[string]any{"floor": truncatedPtr.Floor}}}
	}
	var future driver.ErrFutureRevision
	if errors.As(err, &future) {
		return futureHistoryNotFound(future)
	}
	var futurePtr *driver.ErrFutureRevision
	if errors.As(err, &futurePtr) && futurePtr != nil {
		return futureHistoryNotFound(*futurePtr)
	}
	return mapped(err)
}

func futureHistoryNotFound(future driver.ErrFutureRevision) error {
	return &model.Error{Data: model.ErrorData{Code: "not_found", Details: map[string]any{
		"requestedRevision": future.Requested,
		"headRevision":      future.Head,
	}}}
}

// historyInstantRevision scans every committed page through this reader's head.
// Commit timestamps need not increase; revision order breaks timestamp ties and
// a later page can contain the latest eligible commit. A missing prefix or an
// interior hole never permits selecting a nearby retained state.
func historyInstantRevision(ctx context.Context, r driver.Reader, instant time.Time) (model.Revision, error) {
	snapshot := r.Snapshot()
	if snapshot.At == nil || snapshot.Incarnation == "" {
		return "", failure("internal_error", "history snapshot is not pinned")
	}
	head, err := snapshot.At.Uint64()
	if err != nil {
		return "", failure("internal_error", "invalid history snapshot revision")
	}
	selected := model.Natural64(0)
	var previous uint64
	cursor := &driver.Cursor{Incarnation: snapshot.Incarnation, Revision: model.Natural64(0)}
	for previous < head {
		entries, err := r.Changesets(ctx, cursor, 1000)
		if err != nil {
			return "", err
		}
		if len(entries) == 0 {
			return "", failure("internal_error", "history log is incomplete at its pinned head")
		}
		for _, entry := range entries {
			revision, err := entry.Revision.Uint64()
			if err != nil || previous == head || revision != previous+1 || revision > head ||
				entry.Incarnation != snapshot.Incarnation || entry.Branch.String() != snapshot.Branch.String() || entry.Time.IsZero() {
				return "", failure("internal_error", "history log is not a complete committed sequence")
			}
			if !entry.Time.After(instant) {
				selected = entry.Revision
			}
			previous = revision
		}
		cursor.Revision = entries[len(entries)-1].Revision
	}
	return selected, nil
}

// historyMemoryAccess records the current-memory boundary observed before a
// historical result is assembled. A history reader is pinned to the requested
// revision, while memory authorization is always evaluated against a fresh
// head read. Only authoritative record kinds and retained tombstones establish
// identity; neither historical JSON fields nor a periodic base identify a lifetime.
type historyMemoryAccess struct {
	kind        string
	installed   bool
	incarnation string
	revisions   []model.Revision
}

func historyMemoryNotFound() error {
	return failure("not_found", "memory was not found")
}

// Missing retained identity is different from a requested historical revision
// being unavailable. Do not expose history diagnostics while classifying an
// absent current record or proving a memory's lifetime.
func historyIdentityError(err error) error {
	err = historyGone(err)
	var typed *model.Error
	if errors.As(err, &typed) {
		switch typed.Data.Code {
		case "history_gone", "cursor_expired", "not_found", "incarnation_changed":
			return historyMemoryNotFound()
		}
	}
	return err
}

func historyRecord(ctx context.Context, r driver.Reader, ref model.Ref) (*driver.Record, error) {
	rows, err := r.Get(ctx, []driver.Ref{ref})
	if err != nil {
		return nil, historyGone(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) != 1 || !historyRefEqual(rows[0].Ref, ref) || rows[0].Revision.Validate() != nil ||
		compareNatural(rows[0].Revision, *r.Snapshot().At) > 0 {
		return nil, failure("internal_error", "persisted history record does not match its reference")
	}
	return &rows[0], nil
}

func historyAuthzError(s *Service, before bool, err error) error {
	if err == nil {
		return nil
	}
	var typed *model.Error
	if errors.As(err, &typed) && typed.Data.Code == "busy" {
		return err
	}
	if before != s.memoryAuthorizationInstalled() {
		return failure("authz_unavailable", "authorization installation changed during history read")
	}
	return err
}

func (s *Service) beginHistoryMemoryAccess(ctx context.Context, by model.PrincipalRef, ref model.Ref) (historyMemoryAccess, error) {
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return historyMemoryAccess{}, err
	}
	defer r.Close()
	record, err := historyRecord(ctx, r, ref)
	if err != nil {
		return historyMemoryAccess{}, err
	}
	present := record != nil
	if !present {
		// Get omits tombstones. Resolve kind from the last retained live record,
		// at its own revision and this head's incarnation. Drivers preserve kind
		// for a reference even across deletion; never authorize this old body.
		versions, err := historyVersions(ctx, r, ref)
		if err != nil {
			return historyMemoryAccess{}, historyIdentityError(err)
		}
		record, err = s.lastLiveHistoryRecord(ctx, by, ref, r.Snapshot(), versions)
		if err != nil {
			return historyMemoryAccess{}, historyIdentityError(err)
		}
	}
	if record == nil || record.Kind == "" || record.Kind == "memory" && !present {
		return historyMemoryAccess{}, historyMemoryNotFound()
	}
	access := historyMemoryAccess{kind: record.Kind, incarnation: r.Snapshot().Incarnation}
	if access.kind != "memory" {
		return access, nil
	}
	memory, err := decodeMemoryRecord(*record)
	if err != nil {
		return historyMemoryAccess{}, err
	}
	installed, err := s.memoryAuthorizationState(by)
	if err != nil {
		return historyMemoryAccess{}, err
	}
	allowed, err := s.authorizeMemory(ctx, by, memory)
	if err != nil {
		return historyMemoryAccess{}, historyAuthzError(s, installed, err)
	}
	if installed != s.memoryAuthorizationInstalled() {
		return historyMemoryAccess{}, failure("authz_unavailable", "authorization installation changed during history read")
	}
	if !allowed {
		return historyMemoryAccess{}, historyMemoryNotFound()
	}
	access.installed = installed
	return access, nil
}

func (s *Service) finishHistoryMemoryAccess(ctx context.Context, by model.PrincipalRef, ref model.Ref, access historyMemoryAccess) error {
	if access.kind != "memory" {
		return nil
	}
	installed, err := s.memoryAuthorizationState(by)
	if err != nil {
		if access.installed != s.memoryAuthorizationInstalled() {
			return failure("authz_unavailable", "authorization installation changed during history read")
		}
		return err
	}
	if installed != access.installed {
		return failure("authz_unavailable", "authorization installation changed during history read")
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return err
	}
	defer r.Close()
	record, err := historyRecord(ctx, r, ref)
	if err != nil {
		return err
	}
	if record == nil || record.Kind != "memory" || r.Snapshot().Incarnation != access.incarnation {
		return historyMemoryNotFound()
	}
	versions, err := historyVersions(ctx, r, ref)
	if err != nil {
		return historyIdentityError(err)
	}
	if !sameMemoryLifetime(versions, record.Revision, access.revisions) {
		return historyMemoryNotFound()
	}
	memory, err := decodeMemoryRecord(*record)
	if err != nil {
		return err
	}
	allowed, err := s.authorizeMemory(ctx, by, memory)
	if err != nil {
		return historyAuthzError(s, access.installed, err)
	}
	if access.installed != s.memoryAuthorizationInstalled() {
		return failure("authz_unavailable", "authorization installation changed during history read")
	}
	if !allowed {
		return historyMemoryNotFound()
	}
	return nil
}

func historyVersions(ctx context.Context, r driver.Reader, ref model.Ref) ([]driver.Version, error) {
	versions := make([]driver.Version, 0, 32)
	var cursor *driver.Cursor
	for {
		page, err := r.Versions(ctx, ref, driver.PageReq{Cursor: cursor, Limit: 1000})
		if err != nil {
			return nil, historyGone(err)
		}
		if page.AsOf.String() != r.Snapshot().At.String() {
			return nil, historyMemoryNotFound()
		}
		for _, version := range page.Items {
			if !historyRefEqual(version.Ref, ref) || version.Revision.Validate() != nil ||
				compareNatural(version.Revision, *r.Snapshot().At) > 0 ||
				len(versions) != 0 && compareNatural(versions[len(versions)-1].Revision, version.Revision) >= 0 {
				return nil, historyMemoryNotFound()
			}
			versions = append(versions, version)
		}
		if len(versions) > maxHistoryVersionScan {
			return nil, failure("scope_too_large", "history version scan exceeds the bounded read limit")
		}
		if page.NextCursor == nil {
			return versions, nil
		}
		if len(page.Items) == 0 {
			return nil, historyMemoryNotFound()
		}
		cursor = page.NextCursor
	}
}

func (s *Service) lastLiveHistoryRecord(ctx context.Context, by model.PrincipalRef, ref model.Ref, snapshot driver.View, versions []driver.Version) (*driver.Record, error) {
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Deleted {
			continue
		}
		r, err := s.reader(ctx, by, &AsOf{Revision: versions[i].Revision, Incarnation: snapshot.Incarnation})
		if err != nil {
			return nil, historyGone(err)
		}
		defer r.Close()
		record, err := historyRecord(ctx, r, ref)
		if err != nil {
			return nil, err
		}
		if record == nil || record.Revision.String() != versions[i].Revision.String() {
			return nil, historyMemoryNotFound()
		}
		return record, nil
	}
	return nil, nil
}

func sameMemoryLifetime(versions []driver.Version, current model.Revision, requested []model.Revision) bool {
	if len(versions) == 0 || len(requested) == 0 {
		return false
	}
	last := versions[len(versions)-1]
	if last.Deleted || last.Revision.String() != current.String() {
		return false
	}
	live := make(map[string]bool)
	for _, version := range versions {
		if version.Deleted {
			clear(live)
		} else {
			live[version.Revision.String()] = true
		}
	}
	for _, revision := range requested {
		if !live[revision.String()] {
			return false
		}
	}
	return true
}

func (access *historyMemoryAccess) observeRecord(record *driver.Record, snapshot driver.View) error {
	if snapshot.Incarnation != access.incarnation || record != nil && (record.Kind == "" || record.Kind != access.kind) {
		return historyMemoryNotFound()
	}
	if access.kind == "memory" {
		if record == nil {
			return historyMemoryNotFound()
		}
		access.revisions = append(access.revisions, record.Revision)
	}
	return nil
}

func (s *Service) HistoryGet(ctx context.Context, by model.PrincipalRef, req HistoryVersionRequest) (HistoryVersion, error) {
	if err := req.Ref.Validate(); err != nil {
		return HistoryVersion{}, failure("invalid_params", "invalid history reference")
	}
	if err := ValidateAsOf(req.AsOf); err != nil {
		return HistoryVersion{}, err
	}
	memoryAccess, err := s.beginHistoryMemoryAccess(ctx, by, req.Ref)
	if err != nil {
		return HistoryVersion{}, err
	}
	r, err := s.reader(ctx, by, &req.AsOf)
	if err != nil {
		return HistoryVersion{}, historyGone(err)
	}
	defer r.Close()

	versions, err := historyVersions(ctx, r, req.Ref)
	if err != nil {
		return HistoryVersion{}, err
	}
	if len(versions) == 0 {
		if memoryAccess.kind == "memory" {
			return HistoryVersion{}, historyMemoryNotFound()
		}
		return HistoryVersion{}, failure("not_found", "record has no retained version at the requested revision")
	}
	version := versions[len(versions)-1]
	record, err := historyRecord(ctx, r, req.Ref)
	if err != nil {
		return HistoryVersion{}, err
	}
	if version.Deleted {
		if memoryAccess.kind == "memory" {
			return HistoryVersion{}, historyMemoryNotFound()
		}
		if record != nil {
			return HistoryVersion{}, failure("internal_error", "persisted history body does not match its version")
		}
		record, err = s.lastLiveHistoryRecord(ctx, by, req.Ref, r.Snapshot(), versions)
		if err != nil {
			return HistoryVersion{}, err
		}
		if record == nil {
			return HistoryVersion{}, historyMemoryNotFound()
		}
	} else if record == nil || record.Revision.String() != version.Revision.String() {
		return HistoryVersion{}, failure("internal_error", "persisted history body does not match its version")
	}
	if err := memoryAccess.observeRecord(record, r.Snapshot()); err != nil {
		return HistoryVersion{}, err
	}
	if !version.Deleted && len(version.Body) == 0 {
		if version.Base || len(version.Patch) == 0 {
			return HistoryVersion{}, failure("internal_error", "live history version has no body or forward patch")
		}
		// Non-base versions retain their forward patch. Materialize the body
		// from this same historical snapshot without replacing that patch or
		// any of the selected version's metadata.
		version.Body = record.Body
	}
	result, err := decodeHistoryVersion(version)
	if err != nil {
		return HistoryVersion{}, err
	}
	if err := s.finishHistoryMemoryAccess(ctx, by, req.Ref, memoryAccess); err != nil {
		return HistoryVersion{}, err
	}
	return result, nil
}

func historyRefEqual(a, b model.Ref) bool {
	return a.Validate() == nil && b.Validate() == nil && a.IsID() == b.IsID() &&
		a.ID == b.ID && a.Key == b.Key && a.Branch.String() == b.Branch.String()
}

func decodeHistoryVersion(version driver.Version) (HistoryVersion, error) {
	result := HistoryVersion{
		Ref:       version.Ref,
		Revision:  version.Revision,
		Base:      version.Base,
		Patch:     []map[string]any{},
		CreatedAt: version.CreatedAt.UTC(),
		Principal: version.Principal,
		Deleted:   version.Deleted,
	}
	if len(version.Patch) != 0 {
		value, err := decodeJSONValue(version.Patch)
		if err != nil {
			return HistoryVersion{}, failure("internal_error", "persisted history patch is invalid")
		}
		patch, ok := value.([]any)
		if !ok {
			return HistoryVersion{}, failure("internal_error", "persisted history patch is not an array")
		}
		result.Patch = make([]map[string]any, 0, len(patch))
		for _, item := range patch {
			object, ok := item.(map[string]any)
			if !ok {
				return HistoryVersion{}, failure("internal_error", "persisted history patch operation is not an object")
			}
			result.Patch = append(result.Patch, object)
		}
	}
	if !version.Deleted {
		if len(version.Body) == 0 {
			return HistoryVersion{}, failure("internal_error", "live history version has no body")
		}
		body, err := decodeJSONObject(version.Body)
		if err != nil {
			return HistoryVersion{}, failure("internal_error", "persisted history body is invalid")
		}
		result.Body = body
	}
	return result, nil
}

func (s *Service) HistoryDiff(ctx context.Context, by model.PrincipalRef, req HistoryDiffRequest) (HistoryDiff, error) {
	if err := req.Ref.Validate(); err != nil {
		return HistoryDiff{}, failure("invalid_params", "invalid history reference")
	}
	if err := req.FromRevision.Validate(); err != nil {
		return HistoryDiff{}, failure("invalid_params", "invalid history revision")
	}
	if err := req.ToRevision.Validate(); err != nil {
		return HistoryDiff{}, failure("invalid_params", "invalid history revision")
	}
	if req.FromRevision.String() == req.ToRevision.String() {
		if _, err := s.HistoryGet(ctx, by, HistoryVersionRequest{Ref: req.Ref, AsOf: AsOf{Revision: req.ToRevision}}); err != nil {
			return HistoryDiff{}, err
		}
		return HistoryDiff{Patch: []JSONPatchOperation{}, ChangedPaths: []string{}}, nil
	}
	if compareNatural(req.FromRevision, req.ToRevision) > 0 {
		return HistoryDiff{}, failure("invalid_params", "fromRevision must not be after toRevision")
	}
	memoryAccess, err := s.beginHistoryMemoryAccess(ctx, by, req.Ref)
	if err != nil {
		return HistoryDiff{}, err
	}
	from, err := s.historyBody(ctx, by, req.Ref, AsOf{Revision: req.FromRevision}, &memoryAccess)
	if err != nil {
		return HistoryDiff{}, err
	}
	to, err := s.historyBody(ctx, by, req.Ref, AsOf{Revision: req.ToRevision}, &memoryAccess)
	if err != nil {
		return HistoryDiff{}, err
	}
	if from == nil && to == nil {
		return HistoryDiff{}, failure("not_found", "record has no retained version in either revision")
	}
	result := HistoryDiff{Patch: []JSONPatchOperation{}, ChangedPaths: []string{}}
	if err := appendDiff(&result, from, to, ""); err != nil {
		return HistoryDiff{}, err
	}
	if err := s.finishHistoryMemoryAccess(ctx, by, req.Ref, memoryAccess); err != nil {
		return HistoryDiff{}, err
	}
	return result, nil
}

func compareNatural(a, b model.Natural) int {
	value, err := coreevents.CompareNatural(a, b)
	if err != nil {
		return 0
	}
	return value
}

func (s *Service) historyBody(ctx context.Context, by model.PrincipalRef, ref model.Ref, asOf AsOf, access *historyMemoryAccess) (map[string]any, error) {
	r, err := s.reader(ctx, by, &asOf)
	if err != nil {
		return nil, historyGone(err)
	}
	defer r.Close()
	record, err := historyRecord(ctx, r, ref)
	if err != nil {
		return nil, err
	}
	if err := access.observeRecord(record, r.Snapshot()); err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	body, err := decodeJSONObject(record.Body)
	if err != nil {
		return nil, failure("internal_error", "persisted history body is invalid")
	}
	return body, nil
}

func decodeJSONValue(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON value")
		}
		return nil, err
	}
	return value, nil
}

func decodeJSONObject(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil || !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON value")
		}
		return nil, err
	}
	return value, nil
}

func appendDiff(result *HistoryDiff, from, to map[string]any, path string) error {
	if from == nil && to == nil {
		return nil
	}
	if from == nil {
		return addDiffOperation(result, JSONPatchOperation{Op: "add", Path: path, Value: to})
	}
	if to == nil {
		return addDiffOperation(result, JSONPatchOperation{Op: "remove", Path: path})
	}
	keys := make(map[string]struct{}, len(from)+len(to))
	for key := range from {
		keys[key] = struct{}{}
	}
	for key := range to {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		before, beforeOK := from[key]
		after, afterOK := to[key]
		child := path + "/" + escapeJSONPointer(key)
		switch {
		case !beforeOK:
			if err := addDiffOperation(result, JSONPatchOperation{Op: "add", Path: child, Value: after}); err != nil {
				return err
			}
		case !afterOK:
			if err := addDiffOperation(result, JSONPatchOperation{Op: "remove", Path: child}); err != nil {
				return err
			}
		default:
			beforeObject, beforeIsObject := before.(map[string]any)
			afterObject, afterIsObject := after.(map[string]any)
			if beforeIsObject && afterIsObject {
				if err := appendDiff(result, beforeObject, afterObject, child); err != nil {
					return err
				}
			} else if !reflect.DeepEqual(before, after) {
				if err := addDiffOperation(result, JSONPatchOperation{Op: "replace", Path: child, Value: after}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func addDiffOperation(result *HistoryDiff, operation JSONPatchOperation) error {
	if len(result.Patch) >= maxDiffOperations {
		return failure("scope_too_large", "history diff exceeds the bounded operation limit")
	}
	result.Patch = append(result.Patch, operation)
	result.ChangedPaths = append(result.ChangedPaths, operation.Path)
	return nil
}

func escapeJSONPointer(value string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(value)
}
