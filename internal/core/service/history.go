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

func (s *Service) HistoryGet(ctx context.Context, by model.PrincipalRef, req HistoryVersionRequest) (HistoryVersion, error) {
	if err := req.Ref.Validate(); err != nil {
		return HistoryVersion{}, failure("invalid_params", "invalid history reference")
	}
	if err := req.AsOf.Revision.Validate(); err != nil {
		return HistoryVersion{}, failure("invalid_params", "invalid history revision")
	}
	r, err := s.reader(ctx, by, &req.AsOf)
	if err != nil {
		return HistoryVersion{}, historyGone(err)
	}
	defer r.Close()

	versions := make([]driver.Version, 0, 32)
	var cursor *driver.Cursor
	for {
		page, pageErr := r.Versions(ctx, req.Ref, driver.PageReq{Cursor: cursor, Limit: 1000})
		if pageErr != nil {
			return HistoryVersion{}, historyGone(pageErr)
		}
		versions = append(versions, page.Items...)
		if len(versions) > maxHistoryVersionScan {
			return HistoryVersion{}, failure("scope_too_large", "history version scan exceeds the bounded read limit")
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	if len(versions) == 0 {
		return HistoryVersion{}, failure("not_found", "record has no retained version at the requested revision")
	}
	return decodeHistoryVersion(versions[len(versions)-1])
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
	from, err := s.historyBody(ctx, by, req.Ref, AsOf{Revision: req.FromRevision})
	if err != nil {
		return HistoryDiff{}, err
	}
	to, err := s.historyBody(ctx, by, req.Ref, AsOf{Revision: req.ToRevision})
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
	return result, nil
}

func compareNatural(a, b model.Natural) int {
	value, err := coreevents.CompareNatural(a, b)
	if err != nil {
		return 0
	}
	return value
}

func (s *Service) historyBody(ctx context.Context, by model.PrincipalRef, ref model.Ref, asOf AsOf) (map[string]any, error) {
	r, err := s.reader(ctx, by, &asOf)
	if err != nil {
		return nil, historyGone(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []driver.Ref{ref})
	if err != nil {
		return nil, historyGone(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	body, err := decodeJSONObject(rows[0].Body)
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
