package driver

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ReadSelectionSQL receives only FreezeReadSelection output. All values are
// parameters; only validated node names and the closed dialect choose text.
func ReadSelectionSQL(s ReadSelection, dialect string) (string, []any, error) {
	if dialect != "sqlite" && dialect != "postgres" {
		return "", nil, fmt.Errorf("%w: selection dialect", ErrUnsupported)
	}
	var params []any
	bind := func(value any) string {
		params = append(params, value)
		if dialect == "postgres" {
			return fmt.Sprintf("$%d", len(params))
		}
		return "?"
	}
	candidates := s.Candidates
	if candidates == nil {
		candidates = []ReadCandidate{}
	}
	encoded, err := json.Marshal(candidates)
	if err != nil {
		return "", nil, err
	}
	input := bind(string(encoded))
	relation := "SELECT CAST(json_extract(value,'$.ordinal') AS INTEGER) AS ordinal, json_extract(value,'$.ref.id') AS id, json_extract(value,'$.prepared') = 1 AS prepared FROM json_each(" + input + ")"
	if dialect == "postgres" {
		relation = "SELECT ordinal,ref->>'id' AS id,prepared FROM jsonb_to_recordset(" + input + "::jsonb) AS x(ordinal bigint,ref jsonb,prepared boolean)"
	}
	var lower func(SelectionExpr) (string, error)
	lower = func(f SelectionExpr) (string, error) {
		switch f.Op {
		case SelectionTrue:
			return "TRUE", nil
		case SelectionFalse:
			return "FALSE", nil
		case SelectionNot:
			x, err := lower(f.Args[0])
			return "(NOT (" + x + "))", err
		case SelectionAll, SelectionAny:
			var parts []string
			for _, a := range f.Args {
				x, err := lower(a)
				if err != nil {
					return "", err
				}
				parts = append(parts, "("+x+")")
			}
			if len(parts) == 0 {
				if f.Op == SelectionAll {
					return "TRUE", nil
				}
				return "FALSE", nil
			}
			join := " AND "
			if f.Op == SelectionAny {
				join = " OR "
			}
			return "(" + strings.Join(parts, join) + ")", nil
		case SelectionPrincipalKind, SelectionSponsorKind:
			actual := s.PrincipalKind
			if f.Op == SelectionSponsorKind {
				actual = s.SponsorKind
			}
			left, right := bind(actual), bind(f.Kind)
			if dialect == "sqlite" {
				return "(" + left + " COLLATE BINARY = " + right + " COLLATE BINARY)", nil
			}
			return "(" + left + "::text COLLATE \"C\" = " + right + "::text COLLATE \"C\")", nil
		case SelectionIDs:
			ids := f.IDs
			if ids == nil {
				ids = []string{}
			}
			raw, err := json.Marshal(ids)
			if err != nil {
				return "", err
			}
			parameter := bind(string(raw))
			if dialect == "sqlite" {
				return "EXISTS (SELECT 1 FROM json_each(" + parameter + ") AS allowed WHERE allowed.value COLLATE BINARY = candidates.id COLLATE BINARY)", nil
			}
			return "EXISTS (SELECT 1 FROM jsonb_array_elements_text(" + parameter + "::jsonb) AS allowed(id) WHERE allowed.id COLLATE \"C\" = candidates.id COLLATE \"C\")", nil
		}
		return "", fmt.Errorf("%w: read selection node %q", ErrUnsupported, f.Op)
	}
	predicate, err := lower(s.Predicate)
	if err != nil {
		return "", nil, err
	}
	limit, offset := bind(s.Limit), bind(s.Offset)
	statement := "WITH candidates AS (" + relation + "), authorized AS (SELECT ordinal FROM candidates WHERE prepared AND (" + predicate + ")), total AS (SELECT COUNT(*) AS n FROM authorized), page AS (SELECT ordinal FROM authorized ORDER BY ordinal LIMIT " + limit + " OFFSET " + offset + ") SELECT total.n,page.ordinal FROM total LEFT JOIN page ON TRUE ORDER BY page.ordinal"
	return statement, params, nil
}
