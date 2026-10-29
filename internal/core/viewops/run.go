package viewops

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type queryTerm struct {
	key   string
	value string
}

func parseQuery(query string) ([]queryTerm, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil
	}
	// The admitted slice is deliberately small and closed: equality clauses
	// joined by AND. No unknown operator is ignored or treated as a match-all.
	query = strings.ReplaceAll(query, "&&", " and ")
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
		if !ok || key == "" || value == "" || strings.ContainsAny(value, " \t\r\n") {
			return nil, failure("invalid_params", "unsupported view query; use equality clauses joined by AND")
		}
		switch key {
		case "id", "type", "project", "status":
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
		if sortValue(row, term.key) != term.value {
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
		if sortValue(row, term.key) != term.value {
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
	definitionDigest, err := s.effectiveDigest(definition, project, req.ViewID)
	if err != nil {
		return Result{}, err
	}
	_, reader, err := s.reader(ctx, by, cursorAsOf(req.Cursor, req.AsOf))
	if err != nil {
		return Result{}, err
	}
	defer reader.Close()
	asOf := AsOf{Revision: *reader.Snapshot().At, Incarnation: reader.Snapshot().Incarnation}
	if err := s.validateCursor(req.Cursor, by, req.Agent, req.ViewID, definitionDigest, asOf.Incarnation); err != nil {
		return Result{}, err
	}
	if req.AsOf != nil && req.Cursor != nil && (!sameRevision(req.AsOf.Revision, req.Cursor.Revision) || (req.AsOf.Incarnation != "" && req.AsOf.Incarnation != req.Cursor.Incarnation)) {
		return Result{}, failure("cursor_invalid", "cursor does not match asOf")
	}
	rows, err := scan(ctx, reader)
	if err != nil {
		return Result{}, err
	}
	// Cache the current authorization result for candidates and blockers. This
	// still evaluates on every page request, so a revoked grant cannot reuse an
	// earlier page's decision.
	authorized := make(map[string]bool, len(rows))
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
	candidates := make([]stored, 0, len(rows))
	for _, row := range rows {
		if matches(row, filter, project, terms) && authorized[row.doc.ID] {
			candidates = append(candidates, row)
		}
	}
	// Only unreadable blocker targets contribute a hidden-blocker count. They
	// are authorized independently so a visible non-candidate never becomes a
	// false hidden trace.
	for _, row := range candidates {
		for _, link := range row.doc.Links {
			if link.Kind != "blocks" && link.Kind != "conditional-blocks" {
				continue
			}
			if !link.To.IsID() || !link.To.Branch.IsZero() {
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
	typeSummary := len(definition.GroupBy) == 1 && definition.GroupBy[0] == "type" && definition.Fields != nil && len(definition.Fields) == 0
	cap := OrdinaryCandidateCap
	if typeSummary {
		cap = TypeSummaryCandidateCap
	}
	if len(candidates) > cap {
		return Result{}, &model.Error{Data: model.ErrorData{Code: "scope_too_large", Hint: "authorized view candidates exceed the bounded cap", Details: map[string]any{"cap": cap}}}
	}
	if typeSummary {
		return s.typeSummaryResult(candidates, rows, authorized, definition, asOf)
	}
	budget, err := s.outputBudget(definition)
	if err != nil {
		return Result{}, err
	}
	start, err := cursorIndex(req.Cursor, len(candidates))
	if err != nil {
		return Result{}, err
	}
	end := start + budget
	if end < start || end > len(candidates) {
		end = len(candidates)
	}
	result := Result{Items: make([]Bead, 0, end-start), AsOf: asOf}
	for _, row := range candidates[start:end] {
		item, err := s.item(row, definition.Fields, rows, authorized)
		if err != nil {
			return Result{}, err
		}
		result.Items = append(result.Items, item)
	}
	if end < len(candidates) {
		result.NextCursor = s.nextCursor(by, req.Agent, req.ViewID, definitionDigest, asOf, uint64(end))
	}
	if len(definition.GroupBy) != 0 {
		result.Groups, err = s.grouped(candidates, start, end, definition.Fields, rows, authorized)
		if err != nil {
			return Result{}, err
		}
		result.Items = []Bead{}
		if result.NextCursor != nil {
			for i := range result.Groups {
				if result.Groups[i].Elided != nil {
					result.Groups[i].Elided.ContinueIndex = result.NextCursor.Index
				}
			}
		}
	}
	return result, nil
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

func (s *Service) nextCursor(by model.PrincipalRef, agent, viewID, definitionDigest string, asOf AsOf, index uint64) *Cursor {
	principal, _ := principalDigest(by, agent)
	return &Cursor{Version: 1, Method: "view.run", PrincipalDigest: principal, DefinitionDigest: definitionDigest, ViewID: viewID, OntologyRevision: s.cfg.OntologyRevision, Incarnation: asOf.Incarnation, Revision: asOf.Revision, Index: index}
}

func (s *Service) typeSummaryResult(candidates, rows []stored, authorized map[string]bool, definition Definition, asOf AsOf) (Result, error) {
	groups := map[string][]stored{}
	for _, row := range candidates {
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
			item, err := s.item(row, definition.Fields, rows, authorized)
			if err != nil {
				return Result{}, err
			}
			// fields=[] is a count summary: no payload rows are emitted.
			if len(definition.Fields) != 0 {
				items = append(items, item)
			}
		}
		result.Groups = append(result.Groups, Group{Key: key, Count: len(groupRows), Items: items})
	}
	return result, nil
}

func (s *Service) grouped(candidates []stored, start, end int, fields []string, rows []stored, authorized map[string]bool) ([]Group, error) {
	groups := map[string][]stored{}
	for _, row := range candidates {
		groups[row.doc.Type] = append(groups[row.doc.Type], row)
	}
	pageGroups := map[string][]stored{}
	for _, row := range candidates[start:end] {
		pageGroups[row.doc.Type] = append(pageGroups[row.doc.Type], row)
	}
	remaining := map[string]int{}
	for _, row := range candidates[end:] {
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
			item, err := s.item(row, fields, rows, authorized)
			if err != nil {
				return nil, err
			}
			group.Items = append(group.Items, item)
		}
		if count := remaining[key]; count != 0 {
			group.Elided = &Elision{Count: count, Reason: "output_budget"}
		}
		result = append(result, group)
	}
	return result, nil
}

func (s *Service) item(row stored, fields []string, rows []stored, authorized map[string]bool) (Bead, error) {
	fieldsMap := cloneMap(row.doc.Fields)
	metadata := cloneMetadata(row.doc.Metadata)
	if fields != nil {
		fieldsMap, metadata = projectMaps(fields, fieldsMap, metadata)
	}
	status, _ := row.doc.Fields["status"].(string)
	item := Bead{ID: row.doc.ID, Type: row.doc.Type, Project: row.doc.Project, Revision: row.record.Revision, Status: status, Fields: fieldsMap, Metadata: metadata}
	if fields != nil {
		if !hasField(fields, "project") {
			item.Project = ""
		}
		if !hasField(fields, "status") {
			item.Status = ""
		}
	}
	for _, target := range row.doc.Links {
		if target.Kind != "blocks" && target.Kind != "conditional-blocks" {
			continue
		}
		if target.To.IsID() && target.To.Branch.IsZero() {
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
		return ExplainResult{}, failure("not_found", "bead is not readable")
	}
	doc, err := decodeDocument(rows[0].Body)
	if err != nil || doc.ID != req.ItemID {
		return ExplainResult{}, failure("not_found", "bead is not readable")
	}
	row := stored{record: rows[0], doc: doc}
	ok, err := s.authorize(ctx, by, row)
	if err != nil || !ok {
		return ExplainResult{}, failure("not_found", "bead is not readable")
	}
	result := ExplainResult{ViewID: req.ViewID, ItemID: req.ItemID, Included: matches(row, *definition.Filter, project, terms), Reasons: []string{}}
	if result.Included {
		result.Reasons = append(result.Reasons, "included")
		result.HiddenBlockers, err = s.hiddenBlockers(ctx, by, reader, row)
		if err != nil {
			return ExplainResult{}, err
		}
	} else {
		result.Reasons = append(result.Reasons, filterReason(row, *definition.Filter, project, terms)...)
	}
	if len(result.Reasons) == 0 {
		result.Reasons = []string{"excluded"}
	}
	return result, nil
}

func (s *Service) hiddenBlockers(ctx context.Context, by model.PrincipalRef, reader driver.Reader, row stored) (int, error) {
	refs := make([]driver.Ref, 0, len(row.doc.Links))
	for _, link := range row.doc.Links {
		if (link.Kind == "blocks" || link.Kind == "conditional-blocks") && link.To.IsID() && link.To.Branch.IsZero() {
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
