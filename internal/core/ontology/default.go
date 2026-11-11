package ontology

// GateType identifies a gate installed by the host's ontology catalog.
// Await fields do not select or install a type.
const GateType = "gate"

// DefaultDocument is usable without a plugin host or authorization provider.
// Only this package knows the local slice's concrete workflow keys.
func DefaultDocument() Document {
	return Document{
		Statuses: map[string]Status{
			"todo":        {Name: "Todo", Category: Unstarted},
			"in_progress": {Name: "In progress", Category: Started},
			"done":        {Name: "Done", Category: Completed, Outcome: Success},
			"failed":      {Name: "Failed", Category: Completed, Outcome: Failure},
			"canceled":    {Name: "Canceled", Category: Canceled},
			"backlog":     {Name: "Backlog", Category: Backlog},
		},
		Workflows: map[string]Workflow{
			"issue": {
				Statuses: []string{"todo", "in_progress", "done", "failed", "canceled", "backlog"},
				Initial:  "todo",
				Slots:    map[Slot]string{Claim: "in_progress", Close: "done", Reopen: "todo", Fail: "failed", Cancel: "canceled", Defer: "backlog"},
				Transitions: []Rule{
					{From: []string{"todo"}, To: "in_progress", Guards: []Guard{{Kind: NotTemplate}, {Kind: NoOpenBlockers}, {Kind: Permission}}},
					{To: "done", Guards: []Guard{{Kind: NotTemplate}, {Kind: NoOpenChildren}, {Kind: NoOpenBlockers}, {Kind: Permission}}},
					{To: "failed", Guards: []Guard{{Kind: NoOpenChildren}, {Kind: Permission}}},
					{To: "canceled", Guards: []Guard{{Kind: Permission}}},
					{To: "todo", Guards: []Guard{{Kind: Permission}}},
					{To: "backlog", Guards: []Guard{{Kind: Permission}}},
				},
			},
		},
		Types: map[string]Type{
			"issue": {Name: "Issue", Workflow: "issue", Fields: map[string]Field{
				"title": {Type: "string"}, "description": {Type: "string"},
				"labels": {Type: "strings"}, "blocked": {Type: "boolean"},
			}},
		},
	}
}

func Default() Effective {
	// The no-source host carries the built-in gate node. Explicit source
	// documents remain complete catalogs: compiling one never adds a type
	// that its author did not install.
	document := DefaultDocument()
	document.Types[GateType] = Type{Name: "Gate", Workflow: "issue", Fields: map[string]Field{
		"title": {Type: "string"}, "description": {Type: "string"},
		"labels": {Type: "strings"}, "blocked": {Type: "boolean"},
	}}
	snapshot, problems := Compile(document, nil, nil)
	if len(problems) != 0 {
		panic("invalid built-in ontology")
	}
	return snapshot.Effective("")
}

// InitialStatus gives the create default for a type.
func (e Effective) InitialStatus(typ string) (string, bool) {
	t, ok := e.document.Types[typ]
	if !ok || t.Archived || e.degraded[typ] {
		return "", false
	}
	w, ok := e.document.Workflows[t.Workflow]
	return w.Initial, ok && !w.Archived
}
