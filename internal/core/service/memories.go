package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const scanLimit = 20000

var memoryNonSlugChars = regexp.MustCompile(`[^a-z0-9]+`)

// deriveMemoryKey is the compatibility key used when a memory caller omits
// its key. The memory card deliberately preserves Beads' byte-visible rules:
// lowercase, collapse each run of non-ASCII-alphanumeric characters to one
// hyphen, keep the first eight segments, then cap the result at 60 bytes.
func deriveMemoryKey(content string) string {
	slug := strings.Trim(memoryNonSlugChars.ReplaceAllString(strings.ToLower(content), "-"), "-")
	parts := strings.SplitN(slug, "-", 10)
	if len(parts) > 8 {
		parts = parts[:8]
	}
	slug = strings.Join(parts, "-")
	if len(slug) > 60 {
		slug = strings.TrimRight(slug[:60], "-")
	}
	return slug
}

func resolveMemoryKey(key, content string) (string, error) {
	if strings.TrimSpace(content) == "" {
		return "", failure("invalid_params", "memory content must be nonempty")
	}
	if key != "" {
		if strings.TrimSpace(key) == "" {
			return "", failure("invalid_params", "memory key must be nonempty")
		}
		return key, nil
	}
	derived := deriveMemoryKey(content)
	if derived == "" {
		return "", failure("invalid_params", "memory content cannot derive a key; supply an explicit key")
	}
	return derived, nil
}

func memoryRef(ref model.Ref) error {
	if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
		return failure("invalid_params", "memory methods require a nonempty ID on the main branch")
	}
	return nil
}

func (s *Service) memoryAuthorizationInstalled() bool {
	installed := s.memoryReadAuthorizer != nil
	if s.authorizationInstalled != nil {
		installed = s.authorizationInstalled()
	}
	return installed
}

func (s *Service) memoryAuthorizationState(by model.PrincipalRef) (bool, error) {
	if s == nil {
		return false, failure("internal_error", "service is not configured")
	}
	installed := s.memoryAuthorizationInstalled()
	if installed && s.memoryReadAuthorizer == nil {
		return false, failure("authz_unavailable", "current memory authorization is unavailable")
	}
	if !installed && (by.Kind == "agent" || by.Sponsor != nil || len(by.Caveats) != 0) {
		return false, failure("authz_not_installed", "scoped principals require an authorization plugin")
	}
	return installed, nil
}

func clonePrincipalValue(value model.PrincipalRef) (model.PrincipalRef, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return model.PrincipalRef{}, failure("invalid_params", "invalid principal")
	}
	var copy model.PrincipalRef
	if err := json.Unmarshal(raw, &copy); err != nil {
		return model.PrincipalRef{}, failure("invalid_params", "invalid principal")
	}
	return copy, nil
}

func cloneMemoryMetadata(value map[string]any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var copy map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&copy); err != nil {
		return nil, err
	}
	return copy, nil
}

func memoryValue(record driver.Record, document pipeline.MemoryDocument) (Memory, error) {
	if record.Kind != "memory" || document.ID != record.Ref.ID || document.Key == "" {
		return Memory{}, failure("internal_error", "invalid persisted memory")
	}
	metadata, err := cloneMemoryMetadata(document.Metadata)
	if err != nil {
		return Memory{}, failure("internal_error", "invalid persisted memory metadata")
	}
	return Memory{ID: document.ID, Key: document.Key, Content: document.Content, Metadata: metadata, Revision: record.Revision}, nil
}

func (s *Service) authorizeMemory(ctx context.Context, by model.PrincipalRef, memory Memory) (bool, error) {
	installed, err := s.memoryAuthorizationState(by)
	if err != nil {
		return false, err
	}
	if !installed {
		return true, nil
	}
	principal, err := clonePrincipalValue(by)
	if err != nil {
		return false, err
	}
	allowed, err := s.memoryReadAuthorizer.CanReadMemory(ctx, principal, memory)
	if ctx.Err() != nil {
		return false, failure("busy", "request was canceled")
	}
	if err != nil {
		return false, failure("authz_unavailable", "current memory authorization failed")
	}
	if s.authorizationInstalled != nil && installed != s.authorizationInstalled() {
		return false, failure("authz_unavailable", "authorization installation changed during read")
	}
	return allowed, nil
}

// History authorization uses the same persisted-envelope checks and visibility
// boundary as public memory reads before it can return any historical body.
func decodeMemoryRecord(record driver.Record) (Memory, error) {
	document, source, err := pipeline.DecodeMemoryRecord(record)
	if err != nil {
		return Memory{}, failure("internal_error", "invalid persisted memory")
	}
	if source != nil && !source.Visible || document.Overlay != nil && document.Overlay.Deleted {
		return Memory{}, historyMemoryNotFound()
	}
	return memoryValue(record, document)
}

// readPublicMemory projects a validated retained source envelope without
// changing its persisted schema. Writes use the same strict classifier.
func readPublicMemory(ctx context.Context, r driver.Reader, record driver.Record) (Memory, bool, error) {
	effective, document, source, err := pipeline.ResolveMemoryRecord(ctx, r, record)
	if err != nil {
		converted := mapped(err)
		var typed *model.Error
		if !errors.As(converted, &typed) {
			converted = failure("internal_error", "invalid persisted memory")
		}
		return Memory{}, false, converted
	}
	if source != nil && !source.Visible || document.Overlay != nil && document.Overlay.Deleted {
		return Memory{}, false, nil
	}
	memory, err := memoryValue(effective, document)
	if err != nil {
		return Memory{}, false, err
	}
	if source == nil {
		return memory, true, nil
	}
	if !source.Legacy && document.Overlay == nil {
		// Classification/encoding belong to the retained envelope. Public
		// metadata remains the established provenance projection until a user
		// writes actual overlay metadata.
		memory.Metadata = nil
	}
	if memory.Metadata == nil {
		memory.Metadata = map[string]any{}
	}
	// Request metadata cannot replace the authoritative original provenance.
	memory.Metadata["plug.restore.source_id"] = strings.TrimPrefix(source.Fields["key"].(string), "kv.memory.")
	memory.Metadata["plug.restore.source_table"] = "config"
	memory.Metadata["plug.restore.source"] = source.Fields
	if !source.Legacy {
		memory.Metadata["plug.restore.source_project"] = source.Project
	}
	memory.Metadata["plug.restore.archive_sha256"] = source.Archive
	return memory, true, nil
}

func (s *Service) MemoryGet(ctx context.Context, by model.PrincipalRef, req MemoryGetRequest) (Memory, error) {
	if err := memoryRef(req.ID); err != nil {
		return Memory{}, err
	}
	if _, err := s.memoryAuthorizationState(by); err != nil {
		return Memory{}, err
	}
	var access historyMemoryAccess
	if req.AsOf != nil {
		var err error
		access, err = s.beginHistoryMemoryAccess(ctx, by, req.ID)
		if err != nil {
			return Memory{}, err
		}
	}
	r, err := s.reader(ctx, by, req.AsOf)
	if err != nil {
		return Memory{}, err
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{req.ID})
	if err != nil {
		return Memory{}, mapped(err)
	}
	if len(rows) != 1 || rows[0].Kind != "memory" {
		return Memory{}, failure("not_found", "memory was not found")
	}
	memory, eligible, err := readPublicMemory(ctx, r, rows[0])
	if err != nil {
		return Memory{}, err
	}
	if !eligible {
		return Memory{}, failure("not_found", "memory was not found")
	}
	allowed, err := s.authorizeMemory(ctx, by, memory)
	if err != nil {
		return Memory{}, err
	}
	if !allowed {
		return Memory{}, failure("not_found", "memory was not found")
	}
	if req.AsOf != nil {
		if err := access.observeRecord(&rows[0], r.Snapshot()); err != nil {
			return Memory{}, err
		}
		if err := s.finishHistoryMemoryAccess(ctx, by, req.ID, access); err != nil {
			return Memory{}, err
		}
	}
	return memory, nil
}

func memoryPageResult(items []Memory, limit int, position *PagePosition, asOf AsOf) (MemoryPage, error) {
	if position != nil {
		if position.AsOf.Incarnation != asOf.Incarnation {
			return MemoryPage{}, mapped(driver.ErrIncarnationChanged{From: position.AsOf.Incarnation, To: asOf.Incarnation})
		}
		if position.Offset < 0 || position.Offset > scanLimit {
			return MemoryPage{}, failure("cursor_invalid", "invalid memory page position")
		}
	}
	offset := 0
	if position != nil {
		offset = position.Offset
	}
	end := min(offset+limit, len(items))
	// Memory lists continue at the signed offset in current data. A list that
	// shrinks below that cutoff ends with an empty page, as other live lists do.
	page := MemoryPage{Items: items[min(offset, len(items)):end], AsOf: asOf}
	if end < len(items) {
		page.Next = &PagePosition{Offset: end, AsOf: asOf}
	}
	return page, nil
}

func (s *Service) MemoryList(ctx context.Context, by model.PrincipalRef, req MemoryListRequest) (MemoryPage, error) {
	if req.Cursor != "" {
		return MemoryPage{}, failure("cursor_invalid", "RPC must validate the opaque cursor before passing a domain page position")
	}
	if _, err := s.memoryAuthorizationState(by); err != nil {
		return MemoryPage{}, err
	}
	if err := validatePosition(req.Position); err != nil {
		return MemoryPage{}, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return MemoryPage{}, failure("invalid_params", "limit must be between 1 and 1000")
	}
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return MemoryPage{}, err
	}
	defer r.Close()
	needle := strings.ToLower(req.Query)
	items := make([]Memory, 0)
	seen := map[string]bool{}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Filter: map[string]any{"kind": "memory"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			return MemoryPage{}, mapped(err)
		}
		for _, record := range page.Items {
			memory, eligible, err := readPublicMemory(ctx, r, record)
			if err != nil {
				return MemoryPage{}, err
			}
			if !eligible || seen[memory.ID] {
				continue
			}
			seen[memory.ID] = true
			if needle != "" && !strings.Contains(strings.ToLower(memory.Key), needle) && !strings.Contains(strings.ToLower(memory.Content), needle) {
				continue
			}
			allowed, err := s.authorizeMemory(ctx, by, memory)
			if err != nil {
				return MemoryPage{}, err
			}
			if allowed {
				items = append(items, memory)
			}
			if len(items) > scanLimit {
				return MemoryPage{}, failure("scope_too_large", "memory scan exceeds 20000 records")
			}
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			return MemoryPage{}, failure("internal_error", "memory scan did not advance")
		}
		cursor = page.NextCursor
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key != items[j].Key {
			return items[i].Key < items[j].Key
		}
		return items[i].ID < items[j].ID
	})
	view := r.Snapshot()
	return memoryPageResult(items, limit, req.Position, AsOf{Revision: *view.At, Incarnation: view.Incarnation})
}

func (s *Service) MemorySave(ctx context.Context, by model.PrincipalRef, req MemorySaveRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.memorySave(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) MemorySaveWithOutcome(ctx context.Context, by model.PrincipalRef, req MemorySaveRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.memorySave(ctx, by, req, meta, true)
}

func (s *Service) memorySave(ctx context.Context, by model.PrincipalRef, req MemorySaveRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	key, err := resolveMemoryKey(req.Key, req.Content)
	if err != nil {
		return port.WriteOutcome{}, err
	}
	metadata, err := cloneMemoryMetadata(req.Metadata)
	if err != nil {
		return port.WriteOutcome{}, failure("invalid_params", "memory metadata must be a JSON object")
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "memory", Fields: map[string]any{"key": key, "content": req.Content, "metadata": metadata}}}}, Expect: req.Expect}
	return s.applyMemory(ctx, by, "memory.save", cs, meta, outcomeRequired)
}

func (s *Service) MemoryDelete(ctx context.Context, by model.PrincipalRef, req MemoryDeleteRequest, meta model.WriteMeta) (model.Commit, error) {
	v, err := s.memoryDelete(ctx, by, req, meta, false)
	return v.Commit, err
}

func (s *Service) MemoryDeleteWithOutcome(ctx context.Context, by model.PrincipalRef, req MemoryDeleteRequest, meta model.WriteMeta) (port.WriteOutcome, error) {
	return s.memoryDelete(ctx, by, req, meta, true)
}

func (s *Service) memoryDelete(ctx context.Context, by model.PrincipalRef, req MemoryDeleteRequest, meta model.WriteMeta, outcomeRequired bool) (port.WriteOutcome, error) {
	if err := memoryRef(req.ID); err != nil {
		return port.WriteOutcome{}, err
	}
	cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: req.ID}}}, Expect: req.Expect}
	return s.applyMemory(ctx, by, "memory.delete", cs, meta, outcomeRequired)
}

func (s *Service) Brief(ctx context.Context, by model.PrincipalRef, req BriefRequest) (BriefResult, error) {
	if req.BudgetTokens < 0 {
		return BriefResult{}, failure("invalid_params", "budgetTokens must be nonnegative")
	}
	if err := s.admit(ctx, by); err != nil {
		return BriefResult{}, err
	}
	memoryPage, err := s.MemoryList(ctx, by, MemoryListRequest{Limit: 1000})
	if err != nil {
		return BriefResult{}, err
	}
	result := BriefResult{
		WhoAmI:          WhoAmIResult{Principal: by, Sponsor: by.Sponsor, Caveats: by.Caveats, DefaultProject: req.Project},
		Claims:          []Bead{},
		ReadyTop:        []Bead{},
		RecentChanges:   []map[string]any{},
		OntologySummary: map[string]any{},
		Conventions:     map[string]any{},
		Memories:        memoryPage.Items,
	}
	if by.Kind != "anonymous" {
		claims, err := s.claimed(ctx, by, req.Project)
		if err != nil {
			return BriefResult{}, err
		}
		result.Claims = claims
	}
	if req.Project != "" {
		ready, err := s.Ready(ctx, by, ReadyRequest{Project: req.Project, Limit: 20})
		if err != nil {
			return BriefResult{}, err
		}
		result.ReadyTop = ready.Items
	}
	return result, nil
}

func (s *Service) claimed(ctx context.Context, by model.PrincipalRef, project string) ([]Bead, error) {
	r, err := s.reader(ctx, by, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	graph, err := scanBeads(ctx, r)
	if err != nil {
		return nil, err
	}
	holder, err := driver.PrincipalKey(by)
	if err != nil {
		return nil, failure("invalid_params", "invalid principal")
	}
	ids := sortedIDs(graph)
	items := make([]Bead, 0)
	for _, id := range ids {
		row := graph[id]
		if row.doc.Lease == nil || project != "" && row.doc.Project != project {
			continue
		}
		leased, err := driver.PrincipalKey(row.doc.Lease.Principal)
		if err != nil || leased != holder {
			continue
		}
		bead, err := beadValue(row)
		if err != nil {
			return nil, err
		}
		items = append(items, bead)
	}
	return items, nil
}
