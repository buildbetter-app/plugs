package ontology

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

// Decode rejects unknown configuration, including attempts to redefine v1's
// fixed kernel concepts. Compile also validates native Go documents.
func Decode(data []byte) (Document, error) {
	var doc Document
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	value, err := uniqueValue(dec)
	if err != nil {
		return Document{}, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return Document{}, fmt.Errorf("expected one ontology object")
	}
	if err := checkShape(value, reflect.TypeFor[Document](), "ontology"); err != nil {
		return Document{}, err
	}
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return Document{}, err
	}
	return doc, nil
}

// Compile takes ownership of no caller memory. Invalid base or overlay
// documents produce ordered problems and no usable snapshot.
func Compile(base Document, overlays map[string]Document, plugins PluginCatalog) (Snapshot, []Problem) {
	var problems []Problem
	add := func(path, message string) {
		problems = append(problems, Problem{Code: "invalid_params", Path: path, Message: message})
	}
	base, err := cloneDocument(base)
	if err != nil {
		add("base", err.Error())
		return Snapshot{}, problems
	}
	compiled := Snapshot{projects: map[string]Effective{}}
	compiled.base = compileEffective(base, plugins, "base", add)
	for _, project := range keys(overlays) {
		path := "projects." + project
		if project == "" {
			add(path, "project key must not be empty")
		}
		overlay, err := cloneDocument(overlays[project])
		if err != nil {
			add(path, err.Error())
			continue
		}
		merged, _ := cloneDocument(base)
		mergeDefinitions(merged.Statuses, overlay.Statuses, func(v Status) bool { return v.Archived }, func(v Status) Status { v.Archived = true; return v }, path+".statuses", add)
		mergeDefinitions(merged.Workflows, overlay.Workflows, func(v Workflow) bool { return v.Archived }, func(v Workflow) Workflow { v.Archived = true; return v }, path+".workflows", add)
		mergeDefinitions(merged.Types, overlay.Types, func(v Type) bool { return v.Archived }, func(v Type) Type { v.Archived = true; return v }, path+".types", add)
		mergeDefinitions(merged.GateKinds, overlay.GateKinds, func(v GateKind) bool { return v.Archived }, func(v GateKind) GateKind { v.Archived = true; return v }, path+".gate_kinds", add)
		compiled.projects[project] = compileEffective(merged, plugins, path, add)
	}
	if len(problems) != 0 {
		sort.Slice(problems, func(i, j int) bool {
			if problems[i].Path != problems[j].Path {
				return problems[i].Path < problems[j].Path
			}
			return problems[i].Message < problems[j].Message
		})
		return Snapshot{}, problems
	}
	// Hash normalized effective definitions, in JSON's canonical map-key order.
	// No caller-owned map participates after cloning. Include plugin availability
	// because it determines whether a type is degraded.
	projects := map[string]Document{}
	for project, effective := range compiled.projects {
		projects[project] = effective.document
	}
	if plugins == nil {
		plugins = PluginCatalog{}
	}
	data, err := json.Marshal(struct {
		Base     Document            `json:"base"`
		Projects map[string]Document `json:"projects"`
		Plugins  PluginCatalog       `json:"plugins"`
	}{base, projects, plugins})
	if err != nil {
		add("digest", err.Error())
		return Snapshot{}, problems
	}
	digest := sha256.Sum256(data)
	compiled.digest = hex.EncodeToString(digest[:])
	return compiled, nil
}

func cloneDocument(doc Document) (Document, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return Document{}, err
	}
	copy, err := Decode(data)
	if copy.Statuses == nil {
		copy.Statuses = map[string]Status{}
	}
	if copy.Workflows == nil {
		copy.Workflows = map[string]Workflow{}
	}
	if copy.Types == nil {
		copy.Types = map[string]Type{}
	}
	if copy.GateKinds == nil {
		copy.GateKinds = map[string]GateKind{}
	}
	return copy, err
}

func mergeDefinitions[T any](base, overlay map[string]T, archived func(T) bool, archive func(T) T, path string, add func(string, string)) {
	for _, key := range keys(overlay) {
		value := overlay[key]
		old, exists := base[key]
		if archived(value) {
			if !exists {
				add(path+"."+key, "overlays may archive only base elements")
				continue
			}
			base[key] = archive(old)
		} else if exists && archived(old) {
			add(path+"."+key, "archived keys cannot be reused")
		} else {
			base[key] = value
		}
	}
}

func compileEffective(doc Document, plugins PluginCatalog, prefix string, add func(string, string)) Effective {
	e := Effective{document: doc, degraded: map[string]bool{}}
	for _, key := range keys(doc.GateKinds) {
		gate := doc.GateKinds[key]
		path := prefix + ".gate_kinds." + key
		checkKey(key, path, add)
		if gate.Archived {
			continue
		}
		if gate.Resolver == "" {
			add(path+".resolver", "resolver is required")
		}
		if gate.Count != "" && gate.Count != GateCountHumans && gate.Count != GateCountPrincipals {
			add(path+".count", "count must be humans or principals")
		}
		if gate.OnTimeout != "" && gate.OnTimeout != GateTimeoutEscalate && gate.OnTimeout != GateTimeoutFail {
			add(path+".on_timeout", "on_timeout must be escalate or fail")
		}
		if gate.Timeout != "" {
			if duration, err := parseGateTimeout(gate.Timeout); err != nil || duration <= 0 {
				add(path+".timeout", "timeout must be a positive duration")
			}
		}
		// Resolver parameters are opaque to core. Only the built-in manual
		// resolver gives min_approvals a kernel meaning and therefore receives
		// this validation.
		if isManualResolver(gate.Resolver) {
			if _, err := gateMinApprovals(gate); err != nil {
				add(path+".params.min_approvals", err.Error())
			}
		}
	}
	if len(doc.Statuses) == 0 {
		add(prefix+".statuses", "at least one status is required")
	}
	if len(doc.Types) == 0 {
		add(prefix+".types", "at least one type is required")
	}
	for _, key := range keys(doc.Statuses) {
		s := doc.Statuses[key]
		path := prefix + ".statuses." + key
		checkKey(key, path, add)
		switch s.Category {
		case Triage, Backlog, Unstarted, Started, Completed, Canceled, Active, WIP, Done, Frozen:
		default:
			add(path+".category", "unknown category")
		}
		if s.Category.Completed() {
			if s.Outcome == "" {
				s.Outcome = Success
				doc.Statuses[key] = s
			}
			if s.Outcome != Success && s.Outcome != Failure {
				add(path+".outcome", "completed requires success or failure")
			}
			if s.Category == Done && s.Outcome != Success {
				add(path+".outcome", "native done category requires success")
			}
		} else if s.Outcome != "" {
			add(path+".outcome", "only completed statuses have outcomes")
		}
	}
	for _, key := range keys(doc.Workflows) {
		w := doc.Workflows[key]
		path := prefix + ".workflows." + key
		checkKey(key, path, add)
		if w.Archived {
			continue
		}
		seen := map[string]bool{}
		for _, status := range w.Statuses {
			_, ok := doc.Statuses[status]
			if !ok {
				add(path+".statuses", "unknown status "+status)
			}
			if seen[status] {
				add(path+".statuses", "duplicate status "+status)
			}
			seen[status] = true
		}
		if w.Initial == "" {
			w.Initial = w.Slots[Reopen]
			doc.Workflows[key] = w
		}
		if s, ok := doc.Statuses[w.Initial]; !ok || s.Archived || !seen[w.Initial] {
			add(path+".initial", "initial must name an active workflow status")
		}
		for _, slot := range []Slot{Close, Reopen} {
			if w.Slots[slot] == "" {
				add(path+".slots."+string(slot), "required slot missing")
			}
		}
		for _, slot := range keys(w.Slots) {
			target := w.Slots[slot]
			s, ok := doc.Statuses[target]
			if !ok || s.Archived || !seen[target] {
				add(path+".slots."+string(slot), "slot must target an active workflow status")
				continue
			}
			valid := false
			switch slot {
			case Claim:
				valid = s.Category.Started()
			case Close:
				valid = s.Category.Completed() && s.Outcome == Success
			case Fail:
				valid = s.Category == Completed && s.Outcome == Failure
			case Reopen:
				valid = s.Category.ReadyEligible()
			case Cancel:
				valid = s.Category == Canceled
			case Defer:
				valid = s.Category.Backlog()
			}
			if !valid {
				add(path+".slots."+string(slot), "unknown slot or incompatible category/outcome")
			}
		}
		for i, rule := range w.Transitions {
			rp := fmt.Sprintf("%s.transitions[%d]", path, i)
			if !seen[rule.To] {
				add(rp+".to", "target is outside workflow")
			}
			for _, from := range rule.From {
				if !seen[from] {
					add(rp+".from", "source is outside workflow")
				}
			}
			for j, guard := range rule.Guards {
				validateGuard(guard, fmt.Sprintf("%s.guards[%d]", rp, j), add)
			}
		}
	}
	for _, key := range keys(doc.Types) {
		t := doc.Types[key]
		path := prefix + ".types." + key
		checkKey(key, path, add)
		if t.Archived {
			continue
		}
		w, ok := doc.Workflows[t.Workflow]
		if !ok || w.Archived {
			add(path+".workflow", "unknown or archived workflow")
		}
		if t.ReadyEligible == nil || *t.ReadyEligible {
			for _, status := range w.Statuses {
				s := doc.Statuses[status]
				if !s.Archived && s.Category.ReadyEligible() && w.Slots[Claim] == "" {
					add(path+".workflow", "ready-eligible type requires claim slot")
					break
				}
			}
		}
		for _, name := range keys(t.Fields) {
			field := t.Fields[name]
			fp := path + ".fields." + name
			checkKey(name, fp, add)
			if contains([]string{"status", "type", "project", "template", "priority", "metadata", "id"}, name) {
				add(fp, "kernel field cannot be redefined as a typed field")
			}
			switch field.Type {
			case "string", "integer", "number", "boolean", "strings":
				if len(field.Values) != 0 {
					add(fp, "values are only valid for enum fields")
				}
			case "enum":
				if len(field.Values) == 0 {
					add(fp, "enum requires values")
				}
				seen := map[string]bool{}
				for _, value := range field.Values {
					if seen[value] {
						add(fp+".values", "duplicate enum value")
					}
					seen[value] = true
				}
			default:
				add(fp+".type", "unsupported field type")
			}
			if field.Default != nil && !validValue(field, field.Default) {
				add(fp+".default", "default does not match field type")
			}
			if field.Owner != "" && !plugins[field.Owner] {
				e.degraded[key] = true
			}
		}
		for _, rule := range w.Transitions {
			for _, guard := range rule.Guards {
				for _, name := range guard.Fields {
					if _, ok := t.Fields[name]; !ok {
						add(path+".fields."+name, "guard refers to unknown field")
					}
				}
			}
		}
	}
	return e
}

func validateGuard(g Guard, path string, add func(string, string)) {
	switch g.Kind {
	case FieldsRequired:
		if len(g.Fields) == 0 || len(g.Kinds) != 0 {
			add(path, "fields_required needs fields only")
		}
	case EvidenceRequired, GatesResolved:
		if len(g.Kinds) == 0 || len(g.Fields) != 0 {
			add(path, "guard needs kinds only")
		}
	case NoOpenChildren, NoOpenBlockers, NotTemplate, Permission:
		if len(g.Fields)+len(g.Kinds) != 0 {
			add(path, "guard takes no parameters")
		}
	default:
		add(path, "unknown guard")
	}
	for _, name := range append(append([]string{}, g.Fields...), g.Kinds...) {
		if name == "" {
			add(path, "guard parameters must not be empty")
		}
	}
}

func checkKey(key, path string, add func(string, string)) {
	if key == "" || strings.ContainsAny(key, ". /\\\t\r\n*") {
		add(path, "invalid element key")
	}
}

func keys[K ~string, V any](m map[K]V) []K {
	result := make([]K, 0, len(m))
	for key := range m {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
