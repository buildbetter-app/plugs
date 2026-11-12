package cardb20

import (
	"path/filepath"
	"reflect"
	"strings"
)

func b06MixedRelaxParallel(r *b06NativeRecorder, w b06NativeWorkspace, binary, target string) {
	schema := r.rows(w, binary, "mixed-parallel-original-schema", "SHOW CREATE TABLE dependencies")
	rows := r.rows(w, binary, "mixed-parallel-original-indexes", "SELECT index_name AS name, column_name AS field FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'dependencies' AND non_unique = 0 ORDER BY index_name, seq_in_index")
	columns := map[string][]string{}
	for _, row := range rows {
		name, okName := row["name"].(string)
		field, okField := row["field"].(string)
		if !okName || !okField || strings.ContainsAny(name, "`\x00") {
			r.t.Fatalf("invalid actual native index metadata %+v", row)
		}
		columns[name] = append(columns[name], field)
	}
	var selected string
	for name, fields := range columns {
		if reflect.DeepEqual(fields, []string{"issue_id", target}) {
			if selected != "" {
				r.t.Fatal("ambiguous native typed endpoint uniqueness")
			}
			selected = name
		}
	}
	if selected == "" {
		r.t.Fatal("actual native mixed endpoint unique index missing")
	}
	r.writeJSON(filepath.Join(r.root, "mixed-parallel-diagnostic-origin.json"), map[string]any{"original_schema": schema, "original_indexes": rows, "selected_index": selected, "target_column": target, "ordinary_admission": false})
	r.command(w, "mixed-parallel-drop-index", binary, []string{"sql", "ALTER TABLE dependencies DROP INDEX `" + selected + "`", "--json"}, false)
}
