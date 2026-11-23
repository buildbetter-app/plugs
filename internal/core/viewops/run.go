package viewops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	coreservice "github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type queryTerm struct {
	key    string
	value  string
	labels *labelPredicate
}

// queryMatches applies the small, deliberately closed view grammar. Scalar
// identity/status terms retain exact equality; title is the text-search arm
// and therefore matches a case-sensitive substring, as the source behavior
// does. Keeping this distinction here prevents callers from mistaking a text
// predicate for an identifier predicate while preserving deterministic scans.
func queryMatches(row stored, term queryTerm) bool {
	if term.labels != nil {
		return term.labels.matches(row.doc.Labels)
	}
	if term.key == "title" {
		value, _ := row.doc.Fields["title"].(string)
		return strings.Contains(value, term.value)
	}
	return sortValue(row, term.key) == term.value
}

func parseQuery(query string) ([]queryTerm, error) {
	return parseLabelQuery(query)
}

// Scalar token validation retains the existing equality-only grammar.
func parseScalarQuery(query string) ([]queryTerm, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil
	}
	// The admitted slice is deliberately small and closed: equality clauses
	// joined by AND. No unknown operator is ignored or treated as a match-all.
	parts := strings.Fields(query)
	terms := make([]queryTerm, 0, len(parts))
	wantTerm := true
	for _, part := range parts {
		if !wantTerm {
			if !strings.EqualFold(part, "and") {
				return nil, failure("invalid_params", "unsupported view query; use equality clauses joined by AND")
			}
			wantTerm = true
			continue
		}
		if strings.EqualFold(part, "and") {
			return nil, failure("invalid_params", "unsupported view query; use equality clauses joined by AND")
		}
		key, value, ok := strings.Cut(part, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" || value == "" || strings.ContainsAny(value, " \t\r\n") || strings.Contains(value, "&&") {
			return nil, failure("invalid_params", "unsupported view query; use equality clauses joined by AND")
		}
		switch key {
		case "id", "type", "project", "status", "title":
		default:
			return nil, &model.Error{Data: model.ErrorData{Code: "unknown_field", Hint: "view query field is not supported: " + key}}
		}
		terms = append(terms, queryTerm{key: key, value: value})
		wantTerm = false
	}
	if len(terms) == 0 {
		return nil, failure("invalid_params", "view query contains no equality clause")
	}
	if wantTerm {
		return nil, failure("invalid_params", "unsupported view query; use equality clauses joined by AND")
	}
	return terms, nil
}

func matches(row stored, filter Filter, project string, terms []queryTerm) bool {
	if project != "" && row.doc.Project != project {
		return false
	}
	if len(filter.Resources) != 0 {
		found := false
		for _, id := range filter.Resources {
			if id == row.doc.ID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(filter.Types) != 0 {
		found := false
		for _, typ := range filter.Types {
			if typ == row.doc.Type {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for _, term := range terms {
		if !queryMatches(row, term) {
			return false
		}
	}
	return true
}

func filterReason(row stored, filter Filter, project string, terms []queryTerm) []string {
	reasons := make([]string, 0, 3)
	if project != "" && row.doc.Project != project {
		reasons = append(reasons, "project")
	}
	if len(filter.Resources) != 0 && !contains(filter.Resources, row.doc.ID) {
		reasons = append(reasons, "resource")
	}
	if len(filter.Types) != 0 && !contains(filter.Types, row.doc.Type) {
		reasons = append(reasons, "type")
	}
	for _, term := range terms {
		if !queryMatches(row, term) {
			reasons = append(reasons, term.key)
		}
	}
	sort.Strings(reasons)
	return reasons
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (s *Service) Run(ctx context.Context, by model.PrincipalRef, req RunRequest) (Result, error) {
	if err := s.admit(ctx, by); err != nil {
		return Result{}, err
	}
	definition, err := s.resolve(ctx, by, req.ViewID, req.Definition)
	if err != nil {
		return Result{}, err
	}
	if err := validateStale(req.Stale); err != nil {
		return Result{}, err
	}
	if req.Stale != nil && s.cfg.StaleOntologySource == nil && s.cfg.StaleDefaultOntology == nil {
		return Result{}, failure("invalid_params", "stale selection requires an ontology source")
	}
	if req.CountOnly && (req.Cursor != nil || len(definition.GroupBy) != 0) {
		return Result{}, failure("invalid_params", "countOnly does not accept a cursor or grouping")
	}
	if isEpicProgress(definition) {
		if req.Stale != nil {
			return Result{}, failure("invalid_params", "stale selection is unavailable for epic progress")
		}
		return s.runEpicProgress(ctx, by, req, definition)
	}
	filter := *definition.Filter
	terms, err := parseQuery(filter.Query)
	if err != nil {
		return Result{}, err
	}
	project := req.Project
	for _, term := range terms {
		if term.key == "project" {
			if project != "" && project != term.value {
				return Result{}, failure("invalid_params", "view project conflicts with _meta.project")
			}
			project = term.value
		}
	}
	definitionDigest, err := s.effectiveDigest(definition, project, req.ViewID, req.Stale)
	if err != nil {
		return Result{}, err
	}
	_, reader, err := s.reader(ctx, by, cursorAsOf(req.Cursor, req.AsOf))
	if err != nil {
		return Result{}, err
	}
	defer reader.Close()
	asOf := AsOf{Revision: *reader.Snapshot().At, Incarnation: reader.Snapshot().Incarnation}
	projection, err := s.projectionStatus(ctx)
	if err != nil {
		return Result{}, err
	}
	if err := s.validateCursor(req.Cursor, by, req.Agent, req.ViewID, definitionDigest, asOf.Incarnation, req.Stale); err != nil {
		return Result{}, err
	}
	if req.AsOf != nil && req.Cursor != nil && (!sameRevision(req.AsOf.Revision, req.Cursor.Revision) || (req.AsOf.Incarnation != "" && req.AsOf.Incarnation != req.Cursor.Incarnation)) {
		return Result{}, failure("cursor_invalid", "cursor does not match asOf")
	}
	cutoff, err := s.staleCutoff(req.Stale, req.Cursor)
	if err != nil {
		return Result{}, err
	}
	rows, err := scan(ctx, reader)
	if err != nil {
		return Result{}, err
	}
	// Legacy point authorizers, including the guarded service Get adapter,
	// receive one request-local candidate set so their host can load current
	// permission facts once and return independent per-resource decisions.
	if s.cfg.AuthorizeRead != nil {
		resources := make([]model.ResourceRef, 0, len(rows))
		for _, row := range rows {
			if matches(row, filter, project, terms) {
				resources = append(resources, model.ResourceRef{Kind: "bead", Ref: row.record.Ref})
			}
		}
		if len(resources) != 0 {
			ctx, _ = coreservice.WithReadBatchSession(ctx, resources)
		}
	}
	typeSummary := len(definition.GroupBy) == 1 && definition.GroupBy[0] == "type" && definition.Fields != nil && len(definition.Fields) == 0
	useBatchAuthorization := (req.CountOnly || typeSummary) && s.cfg.AuthorizeReadBatch != nil
	// Cache current authorization on every request. For stale reads, the
	// query/order stream is the paging coordinate: authorization is checked
	// before any age or protected-ontology read, then only authorized rows are
	// admitted to the stale predicate. This keeps denied rows opaque while
	// preventing a changed allowed set from shifting a signed cursor offset.
	authorized := make(map[string]bool, len(rows))
	var queryOrder []stored
	var candidates []stored
	if cutoff != nil {
		for _, row := range rows {
			if matches(row, filter, project, terms) {
				queryOrder = append(queryOrder, row)
			}
		}
		sortStored(queryOrder, definition.Sort)
		if useBatchAuthorization {
			refs := make([]model.ResourceRef, 0, len(queryOrder))
			for _, row := range queryOrder {
				refs = append(refs, model.ResourceRef{Kind: "bead", Ref: row.record.Ref})
			}
			if len(refs) != 0 {
				decisions, err := s.cfg.AuthorizeReadBatch(ctx, by, refs)
				if err != nil {
					return Result{}, mapped(err)
				}
				if len(decisions) != len(refs) {
					return Result{}, failure("authz_unavailable", "batch read authorization returned incomplete decisions")
				}
				for i, ref := range refs {
					authorized[ref.Ref.ID] = decisions[i]
				}
			}
		} else {
			for _, row := range queryOrder {
				ok, err := s.authorize(ctx, by, row)
				if err != nil {
					return Result{}, mapped(err)
				}
				authorized[row.doc.ID] = ok
			}
		}
		for _, row := range queryOrder {
			if !authorized[row.doc.ID] {
				continue
			}
			stale, err := s.staleCandidates(ctx, reader, []stored{row}, *req.Stale, *cutoff)
			if err != nil {
				return Result{}, err
			}
			candidates = append(candidates, stale...)
		}
	} else {
		if useBatchAuthorization {
			refs := make([]model.ResourceRef, 0, len(rows))
			for _, row := range rows {
				if matches(row, filter, project, terms) {
					refs = append(refs, model.ResourceRef{Kind: "bead", Ref: row.record.Ref})
				}
			}
			if len(refs) != 0 {
				decisions, err := s.cfg.AuthorizeReadBatch(ctx, by, refs)
				if err != nil {
					return Result{}, mapped(err)
				}
				if len(decisions) != len(refs) {
					return Result{}, failure("authz_unavailable", "batch read authorization returned incomplete decisions")
				}
				for i, ref := range refs {
					authorized[ref.Ref.ID] = decisions[i]
				}
			}
		} else {
			for _, row := range rows {
				if !matches(row, filter, project, terms) {
					continue
				}
				ok, err := s.authorize(ctx, by, row)
				if err != nil {
					return Result{}, mapped(err)
				}
				authorized[row.doc.ID] = ok
			}
		}
		for _, row := range rows {
			if matches(row, filter, project, terms) && authorized[row.doc.ID] {
				candidates = append(candidates, row)
			}
		}
	}
	if !req.CountOnly {
		// Only unreadable blocker targets contribute a hidden-blocker count. They
		// are authorized independently so a visible non-candidate never becomes a
		// false hidden trace.
		for _, row := range candidates {
			for _, link := range row.doc.Links {
				if link.Kind != "blocks" && link.Kind != "conditional-blocks" {
					continue
				}
				if link.TargetKind == model.LinkTargetExternal || !link.To.IsID() || !link.To.Branch.IsZero() {
					continue
				}
				target, exists := findStored(rows, link.To.ID)
				if !exists {
					continue
				}
				if _, checked := authorized[target.doc.ID]; checked {
					continue
				}
				ok, err := s.authorize(ctx, by, target)
				if err != nil {
					return Result{}, mapped(err)
				}
				authorized[target.doc.ID] = ok
			}
		}
		sortStored(candidates, definition.Sort)
	}
	cap := OrdinaryCandidateCap
	if typeSummary {
		cap = TypeSummaryCandidateCap
	}
	if len(candidates) > cap {
		return Result{}, &model.Error{Data: model.ErrorData{Code: "scope_too_large", Hint: "authorized view candidates exceed the bounded cap", Details: map[string]any{"cap": cap}}}
	}
	if req.CountOnly {
		// Membership is the same complete, currently authorized candidate set
		// as item mode. Output budgets cannot truncate an exact count, but
		// their supported range is still validated as it is for item mode.
		if _, err := s.outputBudget(definition); err != nil {
			return Result{}, err
		}
		if err := s.admit(ctx, by); err != nil {
			return Result{}, err
		}
		count := len(candidates)
		return Result{Items: []Bead{}, AsOf: asOf, Count: &count, Projection: projection}, nil
	}
	pending, err := s.pendingReadiness(ctx, reader, candidates)
	if err != nil {
		return Result{}, err
	}
	if typeSummary {
		result, err := s.typeSummaryResult(ctx, candidates, rows, authorized, pending, definition, asOf)
		if err != nil {
			return Result{}, err
		}
		// Summary rendering can finish after batch authorization returns. Fence
		// the complete result against cancellation or lease loss before publish.
		if err := s.admit(ctx, by); err != nil {
			return Result{}, err
		}
		result.Projection = projection
		return result, nil
	}
	budget, err := s.outputBudget(definition)
	if err != nil {
		return Result{}, err
	}
	// Every stale page, including grouped output, uses the same visible keyset
	// coordinate. Group membership is rendered after the page scan; it must not
	// fall back to an authorized-subset offset when an earlier row is revoked.
	stalePaging := cutoff != nil
	start, end := 0, 0
	pageRows := candidates
	remainingRows := candidates
	hasNext := false
	var staleResumeDigest string
	if stalePaging {
		start, err = staleResumeIndex(queryOrder, req.Cursor)
		if err != nil {
			return Result{}, err
		}
		pageRows, end, hasNext, staleResumeDigest, remainingRows = stalePage(queryOrder, candidates, authorized, start, budget)
	} else {
		start, err = cursorIndex(req.Cursor, len(candidates))
		if err != nil {
			return Result{}, err
		}
		end = start + budget
		if end < start || end > len(candidates) {
			end = len(candidates)
		}
		pageRows = candidates[start:end]
		remainingRows = candidates[end:]
		hasNext = end < len(candidates)
	}
	result := Result{Items: make([]Bead, 0, len(pageRows)), AsOf: asOf, Projection: projection}
	for _, row := range pageRows {
		item, err := s.item(ctx, row, definition.Fields, rows, authorized, pending)
		if err != nil {
			return Result{}, err
		}
		result.Items = append(result.Items, item)
	}
	if hasNext {
		index := uint64(end)
		resume := ""
		if stalePaging {
			index = 0
			resume = staleResumeDigest
		}
		result.NextCursor = s.nextCursor(by, req.Agent, req.ViewID, definitionDigest, asOf, index, cutoff, resume)
	}
	if len(definition.GroupBy) != 0 {
		result.Groups, err = s.grouped(ctx, candidates, pageRows, remainingRows, definition.Fields, rows, authorized, pending)
		if err != nil {
			return Result{}, err
		}
		result.Items = []Bead{}
		if result.NextCursor != nil && !stalePaging {
			for i := range result.Groups {
				if result.Groups[i].Elided != nil {
					result.Groups[i].Elided.ContinueIndex = result.NextCursor.Index
				}
			}
		}
	}
	if err := s.projectParents(ctx, by, reader, rows, definition.Fields, &result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Service) pendingReadiness(ctx context.Context, reader driver.Reader, candidates []stored) (map[string]ReadinessState, error) {
	if s.cfg.Readiness == nil || len(candidates) == 0 {
		return nil, nil
	}
	refs := make([]model.Ref, 0, len(candidates))
	for _, row := range candidates {
		refs = append(refs, row.record.Ref)
	}
	states, err := s.cfg.Readiness.Pending(ctx, reader, refs)
	if err != nil {
		return nil, mapped(err)
	}
	if states == nil {
		return nil, nil
	}
	// A readiness source may know about more rows than this page. Keep only
	// exact candidate IDs so a stale or broad projection cannot add a hidden
	// membership trace to a view result.
	wanted := make(map[string]struct{}, len(candidates))
	for _, row := range candidates {
		wanted[row.doc.ID] = struct{}{}
	}
	pending := make(map[string]ReadinessState, len(states))
	for id, state := range states {
		if _, ok := wanted[id]; ok {
			pending[id] = state
		}
	}
	return pending, nil
}

func cursorAsOf(cursor *Cursor, asOf *AsOf) *AsOf {
	if cursor != nil {
		return &AsOf{Revision: cursor.Revision, Incarnation: cursor.Incarnation}
	}
	return asOf
}

func cursorIndex(cursor *Cursor, length int) (int, error) {
	if cursor == nil {
		return 0, nil
	}
	if cursor.Index > uint64(length) || cursor.Index > uint64(^uint(0)>>1) {
		return 0, failure("cursor_invalid", "cursor position is outside the view snapshot")
	}
	return int(cursor.Index), nil
}

// The signed digest identifies an emitted row in the pinned query/order stream.
// Its sort position is recovered from that stream on every page, so a later
// authority change cannot shift an unread row or expose a denied-row index.
func staleResumeIndex(queryOrder []stored, cursor *Cursor) (int, error) {
	if cursor == nil {
		return 0, nil
	}
	position := -1
	for i, row := range queryOrder {
		if staleResumeDigest(row.doc.ID) == cursor.StaleResumeDigest {
			if position >= 0 {
				return 0, failure("cursor_invalid", "age-query cursor resume row is ambiguous")
			}
			position = i
		}
	}
	if position < 0 {
		return 0, failure("cursor_invalid", "age-query cursor resume row is outside the view snapshot")
	}
	return position + 1, nil
}

func staleResumeDigest(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// stalePage consumes the pinned stream once while emitting only rows that
// passed this request's fresh authorization and stale predicate.
func stalePage(queryOrder, candidates []stored, authorized map[string]bool, start, budget int) ([]stored, int, bool, string, []stored) {
	eligible := make(map[string]struct{}, len(candidates))
	for _, row := range candidates {
		eligible[row.doc.ID] = struct{}{}
	}
	page := make([]stored, 0, budget)
	position := start
	for position < len(queryOrder) && len(page) < budget {
		row := queryOrder[position]
		position++
		if !authorized[row.doc.ID] {
			continue
		}
		if _, ok := eligible[row.doc.ID]; !ok {
			continue
		}
		page = append(page, row)
	}
	remaining := make([]stored, 0)
	for i := position; i < len(queryOrder); i++ {
		row := queryOrder[i]
		if authorized[row.doc.ID] {
			if _, ok := eligible[row.doc.ID]; ok {
				remaining = append(remaining, row)
			}
		}
	}
	var resume string
	if len(page) != 0 {
		resume = staleResumeDigest(page[len(page)-1].doc.ID)
	}
	return page, position, len(remaining) != 0, resume, remaining
}

func (s *Service) nextCursor(by model.PrincipalRef, agent, viewID, definitionDigest string, asOf AsOf, index uint64, cutoff *time.Time, staleResumeDigest string) *Cursor {
	principal, _ := principalDigest(by, agent)
	return &Cursor{Version: 1, Method: "view.run", PrincipalDigest: principal, DefinitionDigest: definitionDigest, ViewID: viewID, OntologyRevision: s.cfg.OntologyRevision, Incarnation: asOf.Incarnation, Revision: asOf.Revision, Index: index, StaleResumeDigest: staleResumeDigest, StaleCutoff: cutoff}
}

func (s *Service) typeSummaryResult(ctx context.Context, candidates, rows []stored, authorized map[string]bool, pending map[string]ReadinessState, definition Definition, asOf AsOf) (Result, error) {
	groups := map[string][]stored{}
	for _, row := range candidates {
		driver.ReportReadWork(ctx, driver.ReadAggregateInputs, 1)
		groups[row.doc.Type] = append(groups[row.doc.Type], row)
	}
	if len(groups) > MaxGroups {
		return Result{}, &model.Error{Data: model.ErrorData{Code: "scope_too_large", Hint: "type summary exceeds the 100-group bound", Details: map[string]any{"groups": len(groups), "cap": MaxGroups}}}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := Result{Items: []Bead{}, Groups: make([]Group, 0, len(keys)), AsOf: asOf}
	for _, key := range keys {
		groupRows := groups[key]
		items := make([]Bead, 0)
		for _, row := range groupRows {
			item, err := s.item(ctx, row, definition.Fields, rows, authorized, pending)
			if err != nil {
				return Result{}, err
			}
			// fields=[] is a count summary: no payload rows are emitted.
			if len(definition.Fields) != 0 {
				items = append(items, item)
			}
		}
		driver.ReportReadWork(ctx, driver.ReadAggregateGroups, 1)
		result.Groups = append(result.Groups, Group{Key: key, Count: len(groupRows), Items: items})
	}
	return result, nil
}

func (s *Service) grouped(ctx context.Context, candidates, pageRows, remainingRows []stored, fields []string, rows []stored, authorized map[string]bool, pending map[string]ReadinessState) ([]Group, error) {
	groups := map[string][]stored{}
	for _, row := range candidates {
		driver.ReportReadWork(ctx, driver.ReadAggregateInputs, 1)
		groups[row.doc.Type] = append(groups[row.doc.Type], row)
	}
	pageGroups := map[string][]stored{}
	for _, row := range pageRows {
		pageGroups[row.doc.Type] = append(pageGroups[row.doc.Type], row)
	}
	remaining := map[string]int{}
	for _, row := range remainingRows {
		remaining[row.doc.Type]++
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Group, 0, len(keys))
	for _, key := range keys {
		all := groups[key]
		group := Group{Key: key, Count: len(all), Items: []Bead{}}
		for _, row := range pageGroups[key] {
			item, err := s.item(ctx, row, fields, rows, authorized, pending)
			if err != nil {
				return nil, err
			}
			group.Items = append(group.Items, item)
		}
		if count := remaining[key]; count != 0 {
			group.Elided = &Elision{Count: count, Reason: "output_budget"}
		}
		driver.ReportReadWork(ctx, driver.ReadAggregateGroups, 1)
		result = append(result, group)
	}
	return result, nil
}

func (s *Service) item(ctx context.Context, row stored, fields []string, rows []stored, authorized map[string]bool, pending map[string]ReadinessState) (Bead, error) {
	driver.ReportReadWork(ctx, driver.ReadItemProjections, 1)
	fieldsMap := cloneMap(row.doc.Fields)
	metadata := cloneMetadata(row.doc.Metadata)
	if fields != nil {
		fieldsMap, metadata = projectMaps(fields, fieldsMap, metadata)
	}
	status, _ := row.doc.Fields["status"].(string)
	item := Bead{ID: row.doc.ID, Type: row.doc.Type, Project: row.doc.Project, Revision: row.record.Revision, Status: status, Fields: fieldsMap, Metadata: metadata, Labels: append([]string(nil), row.doc.Labels...)}
	if state, ok := pending[row.doc.ID]; ok {
		item.ReadinessPending = state.Pending
	}
	if fields != nil {
		if !hasField(fields, "project") {
			item.Project = ""
		}
		if !hasField(fields, "status") {
			item.Status = ""
		}
		if !hasField(fields, "labels") {
			item.Labels = nil
		}
	}
	if row.doc.Parent != nil && (fields == nil || hasField(fields, "parent")) {
		if s.cfg.ParentProjection != nil || s.cfg.AuthorizeRead == nil {
			item.Parent = row.doc.Parent.ID
		} else {
			item.ParentHidden = true
		}
	}
	for _, target := range row.doc.Links {
		if target.Kind != "blocks" && target.Kind != "conditional-blocks" {
			continue
		}
		if target.TargetKind != model.LinkTargetExternal && target.To.IsID() && target.To.Branch.IsZero() {
			if _, exists := findStored(rows, target.To.ID); exists && !authorized[target.To.ID] {
				item.HiddenBlockers++
			}
		}
	}
	return item, nil
}

func hasField(fields []string, want string) bool {
	for _, field := range fields {
		if field == want {
			return true
		}
	}
	return false
}

func findStored(rows []stored, id string) (stored, bool) {
	for _, row := range rows {
		if row.doc.ID == id {
			return row, true
		}
	}
	return stored{}, false
}

func projectMaps(fields []string, allFields, allMetadata map[string]any) (map[string]any, map[string]any) {
	selectedFields, selectedMetadata := map[string]any{}, map[string]any{}
	for _, field := range fields {
		switch {
		case field == "fields":
			selectedFields = allFields
		case strings.HasPrefix(field, "fields."):
			key := strings.TrimPrefix(field, "fields.")
			if value, ok := allFields[key]; ok {
				selectedFields[key] = value
			}
		case field == "metadata":
			selectedMetadata = allMetadata
		}
	}
	if len(selectedFields) == 0 {
		selectedFields = nil
	}
	if len(selectedMetadata) == 0 {
		selectedMetadata = nil
	}
	return selectedFields, selectedMetadata
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var out map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&out) != nil {
		return nil
	}
	return out
}

func cloneMetadata(input map[string]json.RawMessage) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, raw := range input {
		var value any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&value) == nil {
			out[key] = value
		}
	}
	return out
}

func (s *Service) Explain(ctx context.Context, by model.PrincipalRef, req ExplainRequest) (ExplainResult, error) {
	if err := s.admit(ctx, by); err != nil {
		return ExplainResult{}, err
	}
	if strings.TrimSpace(req.ViewID) == "" || strings.TrimSpace(req.ItemID) == "" || req.ViewID != strings.TrimSpace(req.ViewID) || req.ItemID != strings.TrimSpace(req.ItemID) {
		return ExplainResult{}, failure("invalid_params", "viewId and itemId are required")
	}
	definition, err := s.resolve(ctx, by, req.ViewID, nil)
	if err != nil {
		return ExplainResult{}, err
	}
	terms, err := parseQuery(definition.Filter.Query)
	if err != nil {
		return ExplainResult{}, err
	}
	project := req.Project
	for _, term := range terms {
		if term.key == "project" {
			if project != "" && project != term.value {
				return ExplainResult{}, failure("invalid_params", "view project conflicts with _meta.project")
			}
			project = term.value
		}
	}
	_, reader, err := s.reader(ctx, by, nil)
	if err != nil {
		return ExplainResult{}, err
	}
	defer reader.Close()
	ref := model.RefByID(req.ItemID, model.Natural64(0))
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil {
		return ExplainResult{}, mapped(err)
	}
	if len(rows) != 1 || rows[0].Kind != "bead" {
		if _, err := s.authorizeRef(ctx, by, ref, project); err != nil {
			return ExplainResult{}, err
		}
		return ExplainResult{}, failure("not_found", "bead is not readable")
	}
	doc, err := decodeDocument(rows[0].Body)
	if err != nil || doc.ID != req.ItemID {
		if _, authErr := s.authorizeRef(ctx, by, ref, project); authErr != nil {
			return ExplainResult{}, authErr
		}
		return ExplainResult{}, failure("not_found", "bead is not readable")
	}
	row := stored{record: rows[0], doc: doc}
	ok, err := s.authorize(ctx, by, row)
	if err != nil || !ok {
		return ExplainResult{}, failure("not_found", "bead is not readable")
	}
	projection, err := s.projectionStatus(ctx)
	if err != nil {
		return ExplainResult{}, err
	}
	result := ExplainResult{ViewID: req.ViewID, ItemID: req.ItemID, Included: matches(row, *definition.Filter, project, terms), Reasons: []string{}, Projection: projection}
	if result.Included {
		result.Reasons = append(result.Reasons, "included")
		if s.cfg.DirectBlockers != nil {
			verdict, sourceErr := s.cfg.DirectBlockers(ctx, reader, ref, DirectBlockerExplainLimit)
			if sourceErr != nil {
				return ExplainResult{}, mapped(sourceErr)
			}
			result.DirectBlockers, err = s.projectDirectBlockers(ctx, by, reader, verdict)
			if err != nil {
				return ExplainResult{}, err
			}
			result.HiddenBlockers = result.DirectBlockers.HiddenCount
		} else {
			result.HiddenBlockers, err = s.hiddenBlockers(ctx, by, reader, row)
			if err != nil {
				return ExplainResult{}, err
			}
		}
		pending, pendingErr := s.pendingReadiness(ctx, reader, []stored{row})
		if pendingErr != nil {
			return ExplainResult{}, pendingErr
		}
		result.ReadinessPending = pending[req.ItemID].Pending
	} else {
		result.Reasons = append(result.Reasons, filterReason(row, *definition.Filter, project, terms)...)
	}
	if len(result.Reasons) == 0 {
		result.Reasons = []string{"excluded"}
	}
	return result, nil
}

func (s *Service) projectDirectBlockers(ctx context.Context, by model.PrincipalRef, reader driver.Reader, verdict DirectBlockerVerdict) (*DirectBlockerResult, error) {
	if len(verdict.Edges) > DirectBlockerExplainLimit {
		return nil, failure("closure_too_large", "direct blocker result exceeds the 100-edge bound")
	}
	result := &DirectBlockerResult{Edges: make([]DirectBlockerEdge, 0, len(verdict.Edges))}
	if len(verdict.Edges) == 0 {
		return result, nil
	}
	refs := make([]model.Ref, 0, len(verdict.Edges))
	seen := make(map[string]bool, len(verdict.Edges))
	for _, edge := range verdict.Edges {
		if edge.TargetKind == model.LinkTargetExternal || !edge.Blocker.IsID() || !edge.Blocker.Branch.IsZero() || seen[edge.Blocker.ID] {
			continue
		}
		seen[edge.Blocker.ID] = true
		refs = append(refs, edge.Blocker)
	}
	rows := []driver.Record{}
	var err error
	if len(refs) != 0 {
		rows, err = reader.Get(ctx, refs)
		if err != nil {
			return nil, mapped(err)
		}
	}
	byID := make(map[string]stored, len(rows))
	for _, record := range rows {
		if record.Kind != "bead" {
			continue
		}
		doc, decodeErr := decodeDocument(record.Body)
		if decodeErr != nil || doc.ID != record.Ref.ID {
			return nil, failure("internal_error", "invalid persisted blocker document")
		}
		byID[doc.ID] = stored{record: record, doc: doc}
	}
	for _, edge := range verdict.Edges {
		if edge.Kind != "blocks" && edge.Kind != "conditional-blocks" && edge.Kind != "waits-for" {
			return nil, failure("internal_error", "invalid direct blocker kind")
		}
		if edge.State != DirectBlockerSatisfied && edge.State != DirectBlockerUnsatisfied && edge.State != DirectBlockerUnknown {
			return nil, failure("internal_error", "invalid direct blocker state")
		}
		targetKind, kindErr := edge.TargetKind.Normalized()
		if kindErr != nil || model.ValidateLinkTarget(targetKind, edge.Blocker) != nil {
			return nil, failure("internal_error", "invalid direct blocker target kind")
		}
		edge.TargetKind = targetKind
		edge.State, edge.Reason = safeDirectBlockerVerdict(edge.State, edge.Reason)
		// An unresolved or missing reference is an unknown link on a readable
		// dependent. There is no target record/project to authorize.
		target, exists := byID[edge.Blocker.ID]
		if edge.TargetKind == model.LinkTargetExternal || !edge.Blocker.IsID() || !edge.Blocker.Branch.IsZero() || !exists {
			edge.State = DirectBlockerUnknown
			if edge.Reason == "" {
				edge.Reason = "blocker reference is unresolved or missing from the snapshot"
			}
			result.Edges = append(result.Edges, edge)
			continue
		}
		allowed, authErr := s.authorize(ctx, by, target)
		if authErr != nil {
			return nil, mapped(authErr)
		}
		if !allowed {
			result.HiddenCount++
			continue
		}
		result.Edges = append(result.Edges, edge)
	}
	return result, nil
}

func safeDirectBlockerVerdict(state DirectBlockerState, reason string) (DirectBlockerState, string) {
	switch state {
	case DirectBlockerSatisfied:
		return state, ""
	case DirectBlockerUnsatisfied:
		if reason == "blocker status does not satisfy blocks" || reason == "blocker status does not satisfy conditional-blocks" {
			return state, reason
		}
		return state, "blocker is unsatisfied"
	case DirectBlockerUnknown:
		switch reason {
		case "blocker reference is unresolved", "blocker is missing from the snapshot", "blocker ontology is unavailable in the snapshot", "blocker status is unknown to its project ontology":
			return state, reason
		default:
			return state, "blocker state is unknown"
		}
	default:
		return DirectBlockerUnknown, "blocker state is unknown"
	}
}

func (s *Service) hiddenBlockers(ctx context.Context, by model.PrincipalRef, reader driver.Reader, row stored) (int, error) {
	refs := make([]driver.Ref, 0, len(row.doc.Links))
	for _, link := range row.doc.Links {
		if link.TargetKind != model.LinkTargetExternal && (link.Kind == "blocks" || link.Kind == "conditional-blocks") && link.To.IsID() && link.To.Branch.IsZero() {
			refs = append(refs, link.To)
		}
	}
	if len(refs) == 0 {
		return 0, nil
	}
	records, err := reader.Get(ctx, refs)
	if err != nil {
		return 0, mapped(err)
	}
	byID := make(map[string]stored, len(records))
	for _, record := range records {
		if record.Kind != "bead" {
			continue
		}
		doc, err := decodeDocument(record.Body)
		if err != nil || doc.ID != record.Ref.ID {
			return 0, failure("internal_error", "invalid persisted blocker document")
		}
		byID[doc.ID] = stored{record: record, doc: doc}
	}
	hidden := 0
	for _, ref := range refs {
		target, exists := byID[ref.ID]
		if !exists {
			continue
		}
		ok, err := s.authorize(ctx, by, target)
		if err != nil {
			return 0, mapped(err)
		}
		if !ok {
			hidden++
		}
	}
	return hidden, nil
}
