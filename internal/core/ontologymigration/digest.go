package ontologymigration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func detached[T any](value T) (T, error) {
	var copy T
	data, err := json.Marshal(value)
	if err != nil {
		return copy, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err = decoder.Decode(&copy)
	return copy, err
}

func digest(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic("ontologymigration: non-JSON value escaped input boundary: " + err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func equal(left, right any) bool { return digest(left) == digest(right) }

func rowKey(row Row) string { return row.State.Project + "/" + row.State.ID }

func sortRows(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].State.Project != rows[j].State.Project {
			return rows[i].State.Project < rows[j].State.Project
		}
		if rows[i].State.ID != rows[j].State.ID {
			return rows[i].State.ID < rows[j].State.ID
		}
		return digest(rows[i]) < digest(rows[j])
	})
}

// RowsDigest is order independent and lossless for JSON numbers. It rejects
// incomplete row views and duplicate bead IDs (IDs are globally immutable).
// Producers compute it from their complete authoritative snapshot, separately
// from Scan. The planner recomputes it from the actual supplied images.
func RowsDigest(rows []Row) (string, error) {
	copy, err := detached(rows)
	if err != nil {
		return "", err
	}
	if copy == nil {
		copy = []Row{}
	}
	seen := map[string]bool{}
	for i := range copy {
		row := &copy[i]
		if row.State.ID == "" || row.State.Project == "" || row.State.Type == "" || row.State.Status == "" || row.State.Fields == nil || row.Revision == "" {
			return "", fmt.Errorf("row requires identity, project, type, status, fields and explicit revision")
		}
		if seen[row.State.ID] {
			return "", fmt.Errorf("duplicate row identity %q", row.State.ID)
		}
		seen[row.State.ID] = true
		canonical, err := row.Revision.Canonical()
		if err != nil {
			return "", err
		}
		row.Revision = model.Revision(canonical)
	}
	sortRows(copy)
	return digest(struct {
		Format string `json:"format"`
		Rows   []Row  `json:"rows"`
	}{"ontology-migration-rows/1", copy}), nil
}

func RowDigest(row Row) (string, error) { return RowsDigest([]Row{row}) }

// BindingsDigest commits to the complete, sorted validating-pre manifest and
// candidate revision/digest. An explicit empty list is a valid observed manifest.
func BindingsDigest(candidate Reference, bindings []Binding) (string, error) {
	if bindings == nil || !validDigest(candidate.Digest) || candidate.Revision == "" {
		return "", fmt.Errorf("candidate and explicit binding manifest required")
	}
	if err := candidate.Revision.Validate(); err != nil {
		return "", err
	}
	copy := append([]Binding{}, bindings...)
	sort.Slice(copy, func(i, j int) bool { return copy[i].ID < copy[j].ID })
	for i, binding := range copy {
		if binding.ID == "" || binding.Type == "" || !validDigest(binding.Digest) || (i > 0 && copy[i-1].ID == binding.ID) {
			return "", fmt.Errorf("invalid or duplicate validating-pre binding")
		}
	}
	return digest(struct {
		Format    string    `json:"format"`
		Candidate Reference `json:"candidate"`
		Bindings  []Binding `json:"bindings"`
	}{"ontology-migration-pre/1", candidate, copy}), nil
}

// Successor comparison never expands a symbolic large exponent. If current
// ends in exponent-implied zeros, its successor must be supplied literally;
// compare that already allocated input against coefficient/zero suffix instead.
func successor(current, candidate model.Revision) bool {
	a, err := current.Canonical()
	if err != nil {
		return false
	}
	b, err := candidate.Canonical()
	if err != nil {
		return false
	}
	if at := strings.IndexByte(a, 'e'); at >= 0 {
		if strings.Contains(b, "e") {
			return false
		}
		coefficient := a[:at]
		exponent, _ := new(big.Int).SetString(a[at+1:], 10)
		suffix := len(b) - len(coefficient)
		return suffix > 0 && exponent.Cmp(big.NewInt(int64(suffix))) == 0 && strings.HasPrefix(b, coefficient) && b[len(b)-1] == '1' && strings.Trim(b[len(coefficient):len(b)-1], "0") == ""
	}
	digits := []byte(a)
	carry := true
	for i := len(digits) - 1; i >= 0 && carry; i-- {
		if digits[i] == '9' {
			digits[i] = '0'
		} else {
			digits[i]++
			carry = false
		}
	}
	if carry {
		digits = append([]byte{'1'}, digits...)
	}
	next, _ := model.Revision(digits).Canonical()
	return next == b
}

func finish(report Report) Plan {
	sort.Slice(report.Refusals, func(i, j int) bool {
		return digest(report.Refusals[i]) < digest(report.Refusals[j])
	})
	report.PlanDigest = ""
	report.PlanDigest = digest(report)
	return Plan{report: report}
}
