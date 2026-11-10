package ontology

import (
	"encoding/json"
	"testing"
)

func TestAwaitMemberSchemaAndKernelReservation(t *testing.T) {
	schema, violations := Default().Schema("issue", "")
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	fields := schema["properties"].(map[string]any)["fields"].(map[string]any)["properties"].(map[string]any)
	for _, test := range []struct{ name, property, number string }{
		{"await_type", "maxLength", "32"}, {"await_id", "maxLength", "255"},
		{"timeout", "minimum", "0"}, {"timeout", "maximum", "9223372036854775807"},
		{"waiters", "x-plug-max-json-bytes", "65535"},
	} {
		if got := fields[test.name].(map[string]any)[test.property]; got != json.Number(test.number) {
			t.Fatalf("%s.%s=%v want %s", test.name, test.property, got, test.number)
		}
	}
	for _, name := range []string{"await_type", "await_id", "timeout", "waiters"} {
		t.Run(name, func(t *testing.T) {
			document := DefaultDocument()
			document.Types["issue"].Fields[name] = Field{Type: "string"}
			_, problems := Compile(document, nil, nil)
			for _, problem := range problems {
				if problem.Path == "base.types.issue.fields."+name && problem.Message == "kernel field cannot be redefined as a typed field" {
					return
				}
			}
			t.Fatal("custom schema replaced a fixed await member", problems)
		})
	}
}
