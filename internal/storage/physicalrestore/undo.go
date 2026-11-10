package physicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	embedded "github.com/dolthub/driver/v2"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

const UndoFormat = "beads-physical-undo/1"

// UndoRequest replaces all user tables on a disposable preserved-store copy.
// HistoryMode must explicitly request one switch-back commit; replay is refused.
// ReverseReport is the existing converter Report, retained without reclassifying
// losses or mistaking explanation strings for measured Plug history counts.
type UndoRequest struct {
	Format        string          `json:"format"`
	HistoryMode   string          `json:"history_mode"`
	Source        SourceBinding   `json:"source"`
	Current       Manifest        `json:"current"`
	ReverseReport json.RawMessage `json:"reverse_report"`
	// Optional original file bytes retain leading/trailing whitespace through a
	// JSON transport. The object and original bytes must agree; no classification
	// or numeric coercion is performed. Byte encoding is only for this loss file.
	ReverseReportBytes []byte `json:"reverse_report_bytes,omitempty"`
}

type UndoReceipt struct {
	Format                   string          `json:"format"`
	SourcePort               string          `json:"source_port"`
	SourceSHA256             string          `json:"source_sha256"`
	OriginalCommit           string          `json:"original_commit"`
	SwitchBackCommit         string          `json:"switch_back_commit"`
	OriginalGraphSHA256      string          `json:"original_graph_sha256"`
	OriginalCommits          int             `json:"original_commits"`
	CurrentCommits           int             `json:"current_commits"`
	DeletedRows              int             `json:"deleted_rows"`
	WrittenRows              int             `json:"written_rows"`
	PhysicalReadbackVerified bool            `json:"physical_readback_verified"`
	Reopened                 bool            `json:"reopened"`
	ReverseReport            json.RawMessage `json:"reverse_report"`
	ReverseReportSHA256      string          `json:"reverse_report_sha256"`
	HistoryDisposition       string          `json:"history_disposition"`
}

func ReadUndo(reader io.Reader) (UndoRequest, error) {
	var request UndoRequest
	if err := readEnvelope(reader, &request); err != nil {
		return request, err
	}
	return request, validateUndo(request)
}
func validateUndo(request UndoRequest) error {
	if request.Format != UndoFormat {
		return &Error{Kind: "format", Row: -1, Cause: errors.New("unsupported undo format")}
	}
	if request.HistoryMode != "switch-back" {
		return &Error{Kind: "history-replay-refused", Row: -1, Cause: errors.New("undo preserves original history plus one switch-back commit; strict history replay is outside this API")}
	}
	if err := validateBinding(request.Source); err != nil {
		return err
	}
	if request.Source.Working == nil {
		return &Error{Kind: "source-completeness", Row: -1, Cause: errors.New("undo requires the distinct complete same-source typed working-root capture, including unversioned tables")}
	}
	if _, err := decode(request.Current); err != nil {
		return err
	}
	// Keep the loss-accounting owner’s complete JSON object. Unknown additive
	// report fields are preserved. This route does not grant converter admission.
	report, err := undoReportBytes(request)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(report))
	decoder.UseNumber()
	if err := checkValue(decoder); err != nil {
		return &Error{Kind: "reverse-report", Row: -1, Cause: err}
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(report, &object); err != nil || object == nil {
		return &Error{Kind: "reverse-report", Row: -1, Cause: errors.New("existing reverse converter report object required")}
	}
	if _, err := decoder.Token(); err != io.EOF {
		return &Error{Kind: "reverse-report", Row: -1, Cause: errors.New("trailing reverse report")}
	}
	return nil
}

// Undo owns the atomic storage operation, then authenticates the new immutable
// root using the unchanged typed capture port. The caller owns the disposable
// store lifetime. VerifyUndo must also run after closing and reopening that
// store; the command does so before publishing its receipt/loss report.
func Undo(ctx context.Context, conn *sql.Conn, request UndoRequest) (UndoReceipt, error) {
	if err := validateUndo(request); err != nil {
		return UndoReceipt{}, err
	}
	if conn == nil {
		return UndoReceipt{}, errors.New("dedicated undo connection required")
	}
	// Freeze detached original report bytes before any storage mutation.
	report, err := undoReportBytes(request)
	if err != nil {
		return UndoReceipt{}, err
	}
	originalImage, err := physicalcapture.ReadCommit(ctx, conn, request.Source.CommitID)
	if err != nil {
		return UndoReceipt{}, err
	}
	originalManifest, err := FromImage(originalImage)
	if err != nil {
		return UndoReceipt{}, err
	}
	if err := compareManifests(request.Source.Image, originalManifest); err != nil {
		return UndoReceipt{}, &Error{Kind: "source-immutable-binding", Row: -1, Cause: err}
	}
	original, err := decode(request.Source.Working.Image)
	if err != nil {
		return UndoReceipt{}, err
	}
	current, err := decode(request.Current)
	if err != nil {
		return UndoReceipt{}, err
	}
	result, err := embedded.RestorePhysicalUndo(ctx, conn, embedded.PhysicalUndoPlan{BaseCommit: request.Source.CommitID, Database: request.Source.Working.Database, WorkingRootHash: request.Source.Working.WorkingRootHash, Original: original, Tables: current})
	if err != nil {
		return UndoReceipt{}, translateRestoreError(err)
	}
	sum := sha256.Sum256(report)
	receipt := UndoReceipt{Format: UndoFormat, SourcePort: request.Source.Port, SourceSHA256: request.Source.SHA256, OriginalCommit: result.BaseCommit, SwitchBackCommit: result.CommitID, OriginalGraphSHA256: result.OriginalGraphSHA256, OriginalCommits: result.PriorCommits, CurrentCommits: result.CurrentCommits, DeletedRows: result.DeletedRows, WrittenRows: result.WrittenRows, ReverseReport: append(json.RawMessage{}, report...), ReverseReportSHA256: hex.EncodeToString(sum[:]), HistoryDisposition: "original native history retained; exactly one switch-back commit; Plug-created history not replayed; converter loss classifications copied without inventing history counts"}
	if _, err := VerifyUndo(ctx, conn, request, receipt); err != nil {
		return UndoReceipt{}, &Error{Kind: "post-commit-indeterminate", Row: -1, Cause: err}
	}
	receipt.PhysicalReadbackVerified = true
	return receipt, nil
}

// VerifyUndo is read-only. It never creates an extra commit. A successful call
// after a real close/reopen proves immutable-root and original-parent bindings.
func VerifyUndo(ctx context.Context, conn *sql.Conn, request UndoRequest, receipt UndoReceipt) (Manifest, error) {
	if err := validateUndo(request); err != nil {
		return Manifest{}, err
	}
	if receipt.Format != UndoFormat || receipt.SourcePort != request.Source.Port || receipt.SourceSHA256 != request.Source.SHA256 || receipt.OriginalCommit != request.Source.CommitID || receipt.CurrentCommits != receipt.OriginalCommits+1 {
		return Manifest{}, &Error{Kind: "undo-receipt", Row: -1, Cause: errors.New("undo identity/one-commit receipt mismatch")}
	}
	if err := embedded.VerifyPhysicalUndo(ctx, conn, embedded.PhysicalUndoReceipt{BaseCommit: receipt.OriginalCommit, CommitID: receipt.SwitchBackCommit, OriginalGraphSHA256: receipt.OriginalGraphSHA256, PriorCommits: receipt.OriginalCommits, CurrentCommits: receipt.CurrentCommits}); err != nil {
		return Manifest{}, translateRestoreError(err)
	}
	var head, parent string
	if err := conn.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD'), DOLT_HASHOF('HEAD~1')").Scan(&head, &parent); err != nil {
		return Manifest{}, err
	}
	if head != receipt.SwitchBackCommit || parent != receipt.OriginalCommit {
		return Manifest{}, &Error{Kind: "undo-graph", Row: -1, Cause: errors.New("reopened graph differs from switch-back receipt")}
	}
	image, err := physicalcapture.ReadCommit(ctx, conn, receipt.SwitchBackCommit)
	if err != nil {
		return Manifest{}, err
	}
	actual, err := FromImage(image)
	if err != nil {
		return Manifest{}, err
	}
	// Immutable capture remains immutable: only its original versioned tables
	// can be expected in the new commit. Current-only rows are verified separately
	// from the complete working-root port, never grafted onto immutable history.
	expectedImmutable := request.Source.Image
	expectedImmutable.Tables = append([]Table{}, expectedImmutable.Tables...)
	replacements := map[string]Table{}
	for _, table := range request.Current.Tables {
		replacements[table.Name] = table
	}
	for i, table := range expectedImmutable.Tables {
		if replacement, ok := replacements[table.Name]; ok {
			expectedImmutable.Tables[i] = replacement
		}
	}
	if err := compareManifests(expectedImmutable, actual); err != nil {
		return Manifest{}, err
	}
	working, err := physicalcapture.ReadWorkingRoot(ctx, conn, receipt.SwitchBackCommit)
	if err != nil {
		return Manifest{}, err
	}
	if working.Database != request.Source.Working.Database {
		return Manifest{}, &Error{Kind: "source-database", Row: -1, Cause: errors.New("reopened actual database differs from typed source")}
	}
	complete, err := FromWorkingImage(working)
	if err != nil {
		return Manifest{}, err
	}
	expected := request.Source.Working.Image
	expected.Tables = append([]Table{}, expected.Tables...)
	for i, table := range expected.Tables {
		if replacement, ok := replacements[table.Name]; ok {
			expected.Tables[i] = replacement
			delete(replacements, table.Name)
		}
	}
	if len(replacements) != 0 {
		return Manifest{}, &Error{Kind: "source-completeness", Row: -1, Cause: errors.New("current user table lacks original complete typed binding")}
	}
	if err := compareManifests(expected, complete); err != nil {
		return Manifest{}, err
	}
	return complete, nil
}

func compareManifests(want, got Manifest) error {
	actual := map[string]Table{}
	for _, table := range got.Tables {
		if _, exists := actual[table.Name]; exists {
			return &Error{Kind: "physical-readback", Table: table.Name, Column: "*", Row: -1, Cause: errors.New("duplicate physical table")}
		}
		actual[table.Name] = table
	}
	for _, table := range want.Tables {
		observed, ok := actual[table.Name]
		if !ok {
			return &Error{Kind: "physical-readback", Table: table.Name, Column: "*", Row: 0, Cause: errors.New("physical table absent")}
		}
		if !reflect.DeepEqual(table.Columns, observed.Columns) {
			column := "*"
			for ci, col := range table.Columns {
				if ci >= len(observed.Columns) || col != observed.Columns[ci] {
					column = col.Name
					break
				}
			}
			return &Error{Kind: "physical-readback", Table: table.Name, Column: column, Row: -1, Cause: errors.New("physical columns/SQL types differ")}
		}
		normalize := func(rows [][]Cell) ([][]Cell, error) {
			out := make([][]Cell, len(rows))
			for ri, row := range rows {
				if len(row) != len(table.Columns) {
					return nil, &Error{Kind: "physical-readback", Table: table.Name, Column: "*", Row: ri, Cause: errors.New("physical row width differs")}
				}
				out[ri] = append([]Cell{}, row...)
				for ci, cell := range out[ri] {
					if strings.EqualFold(table.Columns[ci].SQLType, "json") && cell.Type == "string" {
						decoder := json.NewDecoder(strings.NewReader(cell.Data))
						decoder.UseNumber()
						var value any
						if err := decoder.Decode(&value); err != nil {
							return nil, err
						}
						data, err := json.Marshal(value)
						if err != nil {
							return nil, err
						}
						out[ri][ci].Data = string(data)
					}
				}
			}
			return out, nil
		}
		left, err := normalize(table.Rows)
		if err != nil {
			return err
		}
		right, err := normalize(observed.Rows)
		if err != nil {
			return err
		}
		used := make([]bool, len(right))
		for ri, row := range left {
			exact := -1
			for gi, cells := range right {
				if !used[gi] && reflect.DeepEqual(row, cells) {
					exact = gi
					break
				}
			}
			if exact >= 0 {
				used[exact] = true
				continue
			}
			nearest, best := -1, -1
			for gi, cells := range right {
				if used[gi] {
					continue
				}
				score := 0
				for ci, cell := range row {
					if cell == cells[ci] {
						score++
					}
				}
				if score > best {
					nearest, best = gi, score
				}
			}
			column := "*"
			if nearest >= 0 {
				for ci, cell := range row {
					if cell != right[nearest][ci] {
						column = table.Columns[ci].Name
						break
					}
				}
			}
			return &Error{Kind: "physical-readback", Table: table.Name, Column: column, Row: ri, Cause: errors.New("immutable typed cell or multiplicity differs")}
		}
		if len(left) != len(right) {
			return &Error{Kind: "physical-readback", Table: table.Name, Column: "*", Row: len(left), Cause: errors.New("extra physical rows")}
		}
		delete(actual, table.Name)
	}
	for name := range actual {
		return &Error{Kind: "physical-readback", Table: name, Column: "*", Row: -1, Cause: errors.New("extra physical table")}
	}
	return nil
}

// UndoReportBytes returns owned, exact original converter Report bytes when
// supplied, preserving whitespace and number spellings in the standalone file.
// The JSON object is retained for compatibility and must match those bytes.
func UndoReportBytes(request UndoRequest) ([]byte, error) { return undoReportBytes(request) }
func undoReportBytes(request UndoRequest) ([]byte, error) {
	report := []byte(request.ReverseReport)
	if len(request.ReverseReportBytes) != 0 {
		report = request.ReverseReportBytes
		if len(request.ReverseReport) != 0 {
			var a, b bytes.Buffer
			if err := json.Compact(&a, report); err != nil {
				return nil, &Error{Kind: "reverse-report", Row: -1, Cause: errors.New("invalid original converter Report bytes")}
			}
			if err := json.Compact(&b, request.ReverseReport); err != nil || !bytes.Equal(a.Bytes(), b.Bytes()) {
				return nil, &Error{Kind: "reverse-report", Row: -1, Cause: errors.New("original Report bytes disagree with envelope object")}
			}
		}
	}
	return append([]byte{}, report...), nil
}
