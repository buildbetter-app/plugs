package driver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type SelectionOp string

const (
	SelectionFalse         SelectionOp = "false"
	SelectionTrue          SelectionOp = "true"
	SelectionAll           SelectionOp = "all"
	SelectionAny           SelectionOp = "any"
	SelectionNot           SelectionOp = "not"
	SelectionPrincipalKind SelectionOp = "principal_kind"
	SelectionSponsorKind   SelectionOp = "sponsor_kind"
	SelectionIDs           SelectionOp = "ids"
)

// A closed internal storage expression. This is not a public policy grammar.
type SelectionExpr struct {
	Op   SelectionOp
	Args []SelectionExpr
	Kind string
	IDs  []string
}
type ReadCandidate struct {
	Ordinal  int  `json:"ordinal"`
	Ref      Ref  `json:"ref"`
	Prepared bool `json:"prepared"`
}
type ReadSelection struct {
	View                       View
	Candidates                 []ReadCandidate
	Predicate                  SelectionExpr
	PrincipalKind, SponsorKind string
	Offset, Limit              int
}
type ReadSelectionPage struct {
	Total    int
	Ordinals []int
}

var ErrReadSelectionOffset = errors.New("read selection offset exceeds authorized total")

func selectionKind(k string) bool {
	switch k {
	case "anonymous", "user", "agent", "service", "plugin", "system":
		return true
	}
	return false
}
func selectionID(id string) bool {
	return id != "" && utf8.ValidString(id) && strings.TrimSpace(id) == id && !strings.ContainsRune(id, 0)
}

func freezeSelectionExpr(f SelectionExpr) (SelectionExpr, error) {
	out := SelectionExpr{Op: f.Op, Kind: f.Kind}
	switch f.Op {
	case SelectionAll, SelectionAny, SelectionNot:
		if f.Kind != "" || len(f.IDs) != 0 || f.Op == SelectionNot && len(f.Args) != 1 {
			return out, fmt.Errorf("invalid selection boolean fields")
		}
		out.Args = make([]SelectionExpr, len(f.Args))
		for i, a := range f.Args {
			var err error
			out.Args[i], err = freezeSelectionExpr(a)
			if err != nil {
				return out, err
			}
		}
	case SelectionTrue, SelectionFalse:
		if len(f.Args) != 0 || len(f.IDs) != 0 || f.Kind != "" {
			return out, fmt.Errorf("invalid selection constant")
		}
	case SelectionPrincipalKind, SelectionSponsorKind:
		if len(f.Args) != 0 || len(f.IDs) != 0 || !selectionKind(f.Kind) {
			return out, fmt.Errorf("invalid selection kind")
		}
	case SelectionIDs:
		if len(f.Args) != 0 || f.Kind != "" {
			return out, fmt.Errorf("invalid selection ids")
		}
		out.IDs = append([]string(nil), f.IDs...)
		for _, id := range out.IDs {
			if !selectionID(id) {
				return out, fmt.Errorf("invalid selection id")
			}
		}
	default:
		return out, fmt.Errorf("%w: read selection expression %q", ErrUnsupported, f.Op)
	}
	return out, nil
}

// FreezeReadSelection is used after the ordinary reader/context/AsOf checks.
// A nonnil descriptor is exclusive; no malformed mode falls through to Query.
func FreezeReadSelection(q Query, snapshot View, head bool) (ReadSelection, error) {
	if q.ReadSelection == nil || q.Filter != nil || q.Cursor != nil || q.Limit != 0 {
		return ReadSelection{}, fmt.Errorf("invalid mixed read selection query")
	}
	s := *q.ReadSelection
	if q.AsOf != nil && (q.AsOf.Validate() != nil || snapshot.At == nil || q.AsOf.String() != snapshot.At.String()) {
		return ReadSelection{}, fmt.Errorf("read selection as-of differs from snapshot")
	}
	if !head || snapshot.At == nil || s.View.At == nil || !snapshot.Branch.IsZero() || !s.View.Branch.IsZero() || s.View.Incarnation != snapshot.Incarnation || s.View.At.Validate() != nil || s.View.At.String() != snapshot.At.String() {
		return ReadSelection{}, fmt.Errorf("%w: read selection requires its nonwriter pinned authority head", ErrUnsupported)
	}
	if s.Offset < 0 || s.Limit < 1 || s.Limit > 1000 {
		return ReadSelection{}, fmt.Errorf("invalid read selection bounds")
	}
	// Invalid callers can be denied by prepare without becoming request errors.
	// Their kinds are kept as literal bound values; only expression kinds are
	// constrained to the closed logical vocabulary.
	if !utf8.ValidString(s.PrincipalKind) || !utf8.ValidString(s.SponsorKind) {
		return ReadSelection{}, fmt.Errorf("invalid read selection principal bytes")
	}
	at := *s.View.At
	s.View.At = &at
	s.Candidates = append([]ReadCandidate(nil), s.Candidates...)
	seen := make(map[string]bool, len(s.Candidates))
	for i, row := range s.Candidates {
		if row.Ordinal != i || row.Ref.Validate() != nil || !row.Ref.IsID() || !row.Ref.Branch.IsZero() || !selectionID(row.Ref.ID) || strings.Contains(row.Ref.ID, "/") || seen[row.Ref.ID] {
			return ReadSelection{}, fmt.Errorf("invalid or duplicate read selection candidate")
		}
		seen[row.Ref.ID] = true
	}
	var err error
	s.Predicate, err = freezeSelectionExpr(s.Predicate)
	return s, err
}

func selectionMatch(f SelectionExpr, s ReadSelection, id string) bool {
	switch f.Op {
	case SelectionTrue:
		return true
	case SelectionFalse:
		return false
	case SelectionPrincipalKind:
		return s.PrincipalKind == f.Kind
	case SelectionSponsorKind:
		return s.SponsorKind == f.Kind
	case SelectionIDs:
		for _, allowed := range f.IDs {
			if allowed == id {
				return true
			}
		}
		return false
	case SelectionNot:
		return !selectionMatch(f.Args[0], s, id)
	case SelectionAll, SelectionAny:
		for _, a := range f.Args {
			v := selectionMatch(a, s, id)
			if f.Op == SelectionAll && !v {
				return false
			}
			if f.Op == SelectionAny && v {
				return true
			}
		}
		return f.Op == SelectionAll
	}
	panic("unvalidated selection expression")
}

// SelectReadMemory receives only an already frozen and validated descriptor.
func SelectReadMemory(ctx context.Context, s ReadSelection) (ReadSelectionPage, error) {
	var out ReadSelectionPage
	for _, row := range s.Candidates {
		if err := ctx.Err(); err != nil {
			return ReadSelectionPage{}, err
		}
		if row.Prepared && selectionMatch(s.Predicate, s, row.Ref.ID) {
			if out.Total >= s.Offset && len(out.Ordinals) < s.Limit {
				out.Ordinals = append(out.Ordinals, row.Ordinal)
			}
			out.Total++
		}
	}
	if s.Offset > out.Total {
		return ReadSelectionPage{}, ErrReadSelectionOffset
	}
	return out, ctx.Err()
}

// ReadSelectionRows consumes the complete result and refuses partial success.
func ReadSelectionRows(ctx context.Context, rows *sql.Rows, s ReadSelection) (ReadSelectionPage, error) {
	defer rows.Close()
	var out ReadSelectionPage
	total := -1
	nullOrdinal := false
	for rows.Next() {
		var n int64
		var ordinal sql.NullInt64
		if err := rows.Scan(&n, &ordinal); err != nil {
			return ReadSelectionPage{}, err
		}
		if n < 0 || n > int64(len(s.Candidates)) || total >= 0 && int64(total) != n {
			return ReadSelectionPage{}, fmt.Errorf("invalid selection total")
		}
		total = int(n)
		if !ordinal.Valid {
			if nullOrdinal || len(out.Ordinals) != 0 {
				return ReadSelectionPage{}, fmt.Errorf("invalid selection empty-page sentinel")
			}
			nullOrdinal = true
		}
		if ordinal.Valid {
			if nullOrdinal {
				return ReadSelectionPage{}, fmt.Errorf("mixed selection empty-page sentinel")
			}
			if ordinal.Int64 < 0 || ordinal.Int64 >= int64(len(s.Candidates)) || len(out.Ordinals) > 0 && ordinal.Int64 <= int64(out.Ordinals[len(out.Ordinals)-1]) || len(out.Ordinals) >= s.Limit {
				return ReadSelectionPage{}, fmt.Errorf("invalid selection ordinal")
			}
			out.Ordinals = append(out.Ordinals, int(ordinal.Int64))
		}
	}
	if err := rows.Err(); err != nil {
		return ReadSelectionPage{}, err
	}
	if err := rows.Close(); err != nil {
		return ReadSelectionPage{}, err
	}
	if err := ctx.Err(); err != nil {
		return ReadSelectionPage{}, err
	}
	if total < 0 {
		return ReadSelectionPage{}, fmt.Errorf("selection omitted total")
	}
	if s.Offset > total {
		return ReadSelectionPage{}, ErrReadSelectionOffset
	}
	if len(out.Ordinals) != min(s.Limit, total-s.Offset) {
		return ReadSelectionPage{}, fmt.Errorf("selection returned a partial page")
	}
	out.Total = total
	return out, nil
}

// Use the same branch-zero representation as normal current-head queries.
func ReadSelectionView(at uint64, incarnation string) View {
	revision := model.Natural64(at)
	return View{At: &revision, Incarnation: incarnation}
}
