package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A publication's impacted identities also contain derived and sentinel keys.
// Counting returned physical rows alone hides repeated array preprocessing in
// a single-key header lookup. Observe the actual executor with that full key
// domain, rather than the much smaller list of record owners alone.
func TestSelectionAffectedHeaderProbeBound(t *testing.T) {
	s, lease := openCommitQueryTest(t, selectionPublicationQueryTracer{})
	for offset := 0; offset < 2000; offset += 1000 {
		publishSelectionCensusBatch(t, s, lease, offset, true)
		t.Run(fmt.Sprintf("population_%d", offset+1000), func(t *testing.T) {
			r := testRead(t, s, nil).(*reader)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			clean, generation, err := r.selectionCertificate(ctx)
			if err != nil || !clean {
				t.Fatalf("complete prior certificate=%t: %v", clean, err)
			}
			rows, err := r.sql.QueryContext(ctx, `SELECT key FROM plug_postgres.selection_sources WHERE incarnation=$1 AND start_revision=$2 ORDER BY key`, r.meta.Incarnation, r.meta.Head.String())
			if err != nil {
				t.Fatal(err)
			}
			sources := [][]byte{}
			for rows.Next() {
				var key []byte
				if err := rows.Scan(&key); err != nil {
					_ = rows.Close()
					t.Fatal(err)
				}
				sources = append(sources, key)
			}
			err = rows.Err()
			_ = rows.Close()
			if err != nil || len(sources) <= 1000 {
				t.Fatalf("actual publication source/header domain=%d: %v", len(sources), err)
			}
			query, args, err := r.compileSelectionValidityForSources(ctx, r.at, false, sources, generation)
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if err := r.sql.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			t.Logf("W6_HEADER_PROBE_PLAN population=%d affected_keys=%d plan=%s", offset+1000, len(sources), raw)
			var plan any
			if err := json.Unmarshal(raw, &plan); err != nil {
				t.Fatal(err)
			}
			repeated := 0.0
			var walk func(any)
			walk = func(value any) {
				switch node := value.(type) {
				case map[string]any:
					relation, _ := node["Relation Name"].(string)
					loops, _ := node["Actual Loops"].(float64)
					condition, _ := node["Index Cond"].(string)
					if (relation == "selection_sources" || relation == "records") && loops > 1 && strings.Contains(condition, "ANY (") {
						repeated += loops
					}
					for _, child := range node {
						walk(child)
					}
				case []any:
					for _, child := range node {
						walk(child)
					}
				}
			}
			walk(plan)
			valid, err := r.readSelectionValidity(ctx, query, args, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, family := range selectionFamilies() {
				if !valid[family] {
					t.Errorf("complete publication family=%v validity=%v", family, valid)
				}
			}
			t.Logf("W6_HEADER_PROBE_WORK population=%d affected_keys=%d repeated_array_probes=%v", offset+1000, len(sources), repeated)
			if repeated != 0 {
				t.Errorf("single-key freshness probes repeatedly preprocess the full publication domain: %v executor loops", repeated)
			}
		})
	}
}
