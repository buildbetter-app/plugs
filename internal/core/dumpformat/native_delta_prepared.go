package dumpformat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeDeltaVisitor observes candidates from one actual source traversal.
// Nothing observed is usable until VisitNativeDelta returns nil after EOF,
// source close and the final original-file recheck. Callback values are owned
// copies; changing them cannot change authenticated parser input.
type NativeDeltaVisitor struct {
	Commit func(NativeDeltaRowCommit) error
	// Checkpoint receives complete non-commit checkpoint tables. It mirrors
	// the row-change visitor callback and is kept separate from TableState so
	// prepared readback cannot silently drop checkpoint evidence.
	Checkpoint func(string, NativeDeltaTableSchema, []NativeDeltaRow, map[string]string) error
	TableState func(NativeDeltaTableState) error
	File       func(string, []byte) error
	// CheckpointsOnly omits historical table-state materializations. It never
	// omits a commit delta or any source authentication.
	CheckpointsOnly bool
}

type nativeDeltaPreparation struct {
	owner *NativeDeltaBudget
	view  *NativeDeltaPrepared
}

// NativeDeltaPrepared is an archive-issued immutable staging result. There is
// no constructor from caller records, proofs, digests or a detached snapshot.
// Only a complete authenticated visitor can issue a usable value.
type NativeDeltaPrepared struct {
	budget      *NativeDeltaBudget
	commits     []NativeDeltaRowCommit
	checkpoints []NativeDeltaTableState
	files       map[string][]byte
	ready       bool
}

// NativeCaptureFileLedger returns the ordered file commitments retained by a
// complete authenticated preparation. The returned values contain metadata
// only; raw roots, heads and parts remain owned by this preparation and are
// released by Close. The detached ledger remains charged to owner until the
// caller discards it and releases that owner. The owner must be outside this
// preparation, be a releasable child scope, and share its session budget. The canonical order is
// capture-set.json, head.tar, then each numeric part in ordinal order.
func (p *NativeDeltaPrepared) NativeCaptureFileLedger(owner *NativeDeltaBudget) ([]NativeCaptureFile, error) {
	if p == nil || !p.ready {
		return nil, fmt.Errorf("unqualified native staging view")
	}
	if owner == nil || owner.parent == nil || owner == p.budget || owner.root() != p.budget.root() {
		return nil, fmt.Errorf("native capture ledger requires shared owner")
	}
	for scope := owner.parent; scope != nil; scope = scope.parent {
		if scope == p.budget {
			return nil, fmt.Errorf("native capture ledger requires external owner")
		}
	}
	if len(p.files) < 3 {
		return nil, fmt.Errorf("native capture file ledger is incomplete")
	}
	if _, ok := p.files["capture-set.json"]; !ok {
		return nil, fmt.Errorf("native capture file ledger is incomplete")
	}
	if _, ok := p.files["head.tar"]; !ok {
		return nil, fmt.Errorf("native capture file ledger is incomplete")
	}
	partCount := len(p.files) - 2
	for name := range p.files {
		if name == "capture-set.json" || name == "head.tar" {
			continue
		}
		if len(name) != len("parts/")+20+len(".part") || !strings.HasPrefix(name, "parts/") || !strings.HasSuffix(name, ".part") {
			return nil, fmt.Errorf("native capture file ledger order differs")
		}
		for _, digit := range name[len("parts/") : len("parts/")+20] {
			if digit < '0' || digit > '9' {
				return nil, fmt.Errorf("native capture file ledger order differs")
			}
		}
		ordinal, err := strconv.ParseUint(name[len("parts/"):len("parts/")+20], 10, 64)
		if err != nil || ordinal == 0 || ordinal > uint64(partCount) {
			return nil, fmt.Errorf("native capture file ledger order differs")
		}
	}

	// Admit the complete metadata payload before allocating the result slice
	// or any hashes. The size helper walks one entry at a time and retains no
	// aggregate representation, so a limit refusal cannot leave a ledger live.
	ledgerBytes, err := nativeCaptureFileLedgerJSONSize(p.files)
	if err != nil {
		return nil, err
	}
	if err := owner.ChargeBytes("native_capture_file_ledger", ledgerBytes); err != nil {
		return nil, err
	}
	ledger := make([]NativeCaptureFile, 0, len(p.files))
	appendFile := func(name string) {
		raw := p.files[name]
		ledger = append(ledger, NativeCaptureFile{Name: name, Bytes: uint64(len(raw)), SHA256: digest(raw)})
	}
	appendFile("capture-set.json")
	appendFile("head.tar")
	for i := 1; i <= partCount; i++ {
		appendFile(fmt.Sprintf("parts/%020d.part", i))
	}
	return ledger, nil
}

// nativeCaptureFileLedgerJSONSize computes the exact compact JSON size of the
// ledger without allocating the ledger itself. Each entry is counted and
// discarded before the aggregate result is admitted by the shared budget.
func nativeCaptureFileLedgerJSONSize(files map[string][]byte) (uint64, error) {
	var total uint64
	// SHA-256 is always the fixed 64-byte lowercase hexadecimal form. Count
	// with a placeholder of the same length so no source digest or ledger
	// metadata is materialized before admission.
	const placeholderSHA = "0000000000000000000000000000000000000000000000000000000000000000"
	for name, raw := range files {
		n, err := NativeDeltaJSONSize(NativeCaptureFile{Name: name, Bytes: uint64(len(raw)), SHA256: placeholderSHA})
		if err != nil {
			return 0, err
		}
		if err := deltaAddCount(&total, n); err != nil {
			return 0, err
		}
	}
	// Each entry count includes its own terminal newline. The array has one
	// final newline and one comma between adjacent entries, so the net framing
	// adjustment is two bytes for every non-empty array.
	if err := deltaAddCount(&total, 2); err != nil {
		return 0, err
	}
	return total, nil
}

func newNativeDeltaPrepared(owner *NativeDeltaBudget) *NativeDeltaPrepared {
	return &NativeDeltaPrepared{budget: owner.Scope(), files: map[string][]byte{}}
}

func cloneNativeDeltaRecord(record *driver.Record) *driver.Record {
	if record == nil {
		return nil
	}
	copy := *record
	copy.Body = bytes.Clone(record.Body)
	return &copy
}

func cloneNativeDeltaCommit(commit NativeDeltaRowCommit) NativeDeltaRowCommit {
	copy := commit
	if commit.Changes != nil {
		copy.Changes = make([]driver.SourceHistoryChange, len(commit.Changes))
	}
	for i, change := range commit.Changes {
		copy.Changes[i] = driver.SourceHistoryChange{Before: cloneNativeDeltaRecord(change.Before), After: cloneNativeDeltaRecord(change.After)}
	}
	return copy
}

func cloneNativeDeltaState(state NativeDeltaTableState) NativeDeltaTableState {
	copy := state
	if state.Schema.Columns != nil {
		copy.Schema.Columns = append(make([]NativeDeltaColumn, 0, len(state.Schema.Columns)), state.Schema.Columns...)
	}
	for i := range copy.Schema.Columns {
		copy.Schema.Columns[i].PrimaryKeyOrdinal = cloneNativeDeltaCursor(copy.Schema.Columns[i].PrimaryKeyOrdinal)
	}
	if state.Rows != nil {
		copy.Rows = append(make([]NativeDeltaRow, 0, len(state.Rows)), state.Rows...)
	}
	for i := range copy.Rows {
		if state.Rows[i].Cells != nil {
			copy.Rows[i].Cells = append(make([]NativeDeltaCell, 0, len(state.Rows[i].Cells)), state.Rows[i].Cells...)
		}
	}
	copy.Normalized = cloneNativeDeltaTable(state.Normalized)
	if state.Normalized.Columns == nil {
		copy.Normalized.Columns = nil
	}
	if state.Normalized.Rows == nil {
		copy.Normalized.Rows = nil
	}
	if state.ConfigPolicy != nil {
		copy.ConfigPolicy = make(map[string]string, len(state.ConfigPolicy))
		for key, value := range state.ConfigPolicy {
			copy.ConfigPolicy[key] = value
		}
	}
	return copy
}

func (p *NativeDeltaPrepared) callbacks() *nativeDeltaRowCallbacks {
	return &nativeDeltaRowCallbacks{
		checkpointsOnly: true,
		commit: func(commit NativeDeltaRowCommit) error {
			if err := p.budget.ChargeJSON("receiving_staged_changes", commit); err != nil {
				return err
			}
			p.commits = append(p.commits, cloneNativeDeltaCommit(commit))
			return nil
		},
		tableState: func(state NativeDeltaTableState) error {
			// Full historical tables are deliberately not retained. The genuine
			// row-delta callback above already owns exactly the changed bodies.
			if state.Kind == "commit" {
				return nil
			}
			if err := p.budget.ChargeJSON("receiving_staged_checkpoints", state); err != nil {
				return err
			}
			p.checkpoints = append(p.checkpoints, cloneNativeDeltaState(state))
			return nil
		},
	}
}

func (p *NativeDeltaPrepared) retainFile(name string, raw []byte) error {
	if _, exists := p.files[name]; exists {
		return fmt.Errorf("native staging repeats original")
	}
	// Wire parts are normally outside decoded accounting. Retaining them here
	// is new live memory, so it participates in the same unchanged ceiling.
	if err := p.budget.ChargeBytes("receiving_staged_originals", uint64(len(name)+len(raw))); err != nil {
		return err
	}
	p.files[name] = bytes.Clone(raw)
	return nil
}

func (p *NativeDeltaPrepared) Close() {
	if p == nil {
		return
	}
	p.ready = false
	p.commits, p.checkpoints, p.files = nil, nil, nil
	if p.budget != nil {
		p.budget.Release()
	}
}

// PrepareNativeDelta shares only a private complete result owned by this
// archive. A failed/cancelled traversal never installs a cache entry. All
// accounting scopes of this archive share the same sequential preparation.
func (a *Archive) PrepareNativeDelta(ctx context.Context) (*NativeDeltaPrepared, error) {
	if !a.HasNativeDelta() || a.delta.prepared == nil {
		return nil, fmt.Errorf("native staging unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p := a.delta.prepared.view; p != nil && p.ready {
		return p, nil
	}
	p := newNativeDeltaPrepared(a.delta.prepared.owner)
	callbacks := p.callbacks()
	err := a.VisitNativeDelta(ctx, NativeDeltaVisitor{Commit: callbacks.commit, TableState: callbacks.tableState, File: p.retainFile, CheckpointsOnly: true})
	if err != nil {
		p.Close()
		return nil, err
	}
	p.ready = true
	a.delta.prepared.view = p
	return p, nil
}

// UseNativeDeltaPreparedReadback switches subsequent native-delta readback
// traversals to the authenticated retained preparation. Recovery calls this
// after the original capture reader may have been closed; the prepared view
// is already authenticated by ReadNativeCaptureSet and does not reopen it.
// If no preparation is retained, traversals continue through the normal
// authenticated source path and report its ordinary errors.
func (a *Archive) UseNativeDeltaPreparedReadback() {
	if a == nil || a.delta == nil {
		return
	}
	a.delta.usePreparedReadback = true
}

func (a *Archive) ClearNativeDeltaPreparation() {
	if a != nil && a.delta != nil && a.delta.prepared != nil && a.delta.prepared.view != nil {
		a.delta.prepared.view.Close()
		a.delta.prepared.view = nil
	}
}

// Visit delivers detached staging values without reopening source files.
// It cannot authenticate a new stream. Independent verification uses the
// archive's fresh VisitNativeDelta method, never this retained view.
func (p *NativeDeltaPrepared) Visit(ctx context.Context, visitor NativeDeltaVisitor) error {
	if p == nil || !p.ready {
		return fmt.Errorf("unqualified native staging view")
	}
	for _, commit := range p.commits {
		if err := ctx.Err(); err != nil {
			return err
		}
		if visitor.Commit == nil {
			break
		}
		work := p.budget.Scope()
		if err := work.ChargeJSON("receiving_detached_changes", commit); err != nil {
			work.Release()
			return err
		}
		err := visitor.Commit(cloneNativeDeltaCommit(commit))
		work.Release()
		if err != nil {
			return err
		}
	}
	for _, state := range p.checkpoints {
		if err := ctx.Err(); err != nil {
			return err
		}
		if visitor.TableState == nil && visitor.Checkpoint == nil {
			break
		}
		work := p.budget.Scope()
		if err := work.ChargeJSON("receiving_detached_checkpoint", state); err != nil {
			work.Release()
			return err
		}
		cloned := cloneNativeDeltaState(state)
		var err error
		if visitor.Checkpoint != nil && !cloned.Complete {
			err = visitor.Checkpoint(cloned.Kind, cloned.Schema, cloned.Rows, cloned.ConfigPolicy)
		}
		if err == nil && visitor.TableState != nil {
			err = visitor.TableState(cloned)
		}
		work.Release()
		if err != nil {
			return err
		}
	}
	if visitor.File != nil {
		names := make([]string, 0, len(p.files))
		for name := range p.files {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			work := p.budget.Scope()
			if err := work.ChargeBytes("receiving_detached_original", uint64(len(p.files[name]))); err != nil {
				work.Release()
				return err
			}
			err := visitor.File(name, bytes.Clone(p.files[name]))
			work.Release()
			if err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

type nativeDeltaPartBuffer struct {
	*bytes.Reader
	release func()
}

func (r *nativeDeltaPartBuffer) Close() error { r.release(); return nil }

// File callbacks see a separate candidate before parser consumption. The
// parser still authenticates exact framing, SHA, byte counts and terminal EOF.
func nativeDeltaFileOpener(open NativeDeltaPartOpener, owner *NativeDeltaBudget, visit func(string, []byte) error) NativeDeltaPartOpener {
	return func(ctx context.Context, ordinal uint64) (io.ReadCloser, error) {
		work := owner.Scope()
		f, err := open(ctx, ordinal)
		if err != nil {
			work.Release()
			return nil, err
		}
		raw, readErr := nativeDeltaReadBounded(f, NativeDeltaPartBytes+NativeDeltaPartHeaderBytes, work, "combined_part_buffer")
		closeErr := f.Close()
		if readErr != nil {
			work.Release()
			return nil, readErr
		}
		if closeErr != nil {
			work.Release()
			return nil, closeErr
		}
		copyOwner := work.Scope()
		if err := copyOwner.ChargeBytes("combined_part_callback", uint64(len(raw))); err != nil {
			work.Release()
			return nil, err
		}
		err = visit(fmt.Sprintf("parts/%020d.part", ordinal), bytes.Clone(raw))
		copyOwner.Release()
		if err != nil {
			work.Release()
			return nil, err
		}
		return &nativeDeltaPartBuffer{bytes.NewReader(raw), work.Release}, nil
	}
}

// VisitNativeDelta always reopens the real source. This is the shared actual
// path for staging and independent verifier transcript construction.
func (a *Archive) VisitNativeDelta(ctx context.Context, visitor NativeDeltaVisitor) error {
	if !a.HasNativeDelta() {
		return fmt.Errorf("native delta stream unavailable")
	}
	work := a.delta.budget.Scope()
	defer work.Release()
	open := a.delta.open
	if visitor.File != nil {
		for _, file := range []struct {
			name string
			raw  []byte
		}{{"capture-set.json", a.delta.rootBytes}, {"head.tar", a.tarBytes}} {
			copyOwner := work.Scope()
			if err := copyOwner.ChargeBytes("combined_file_callback", uint64(len(file.raw))); err != nil {
				copyOwner.Release()
				return err
			}
			err := visitor.File(file.name, bytes.Clone(file.raw))
			copyOwner.Release()
			if err != nil {
				return err
			}
		}
		open = nativeDeltaFileOpener(open, work, visitor.File)
	}
	callbacks := &nativeDeltaRowCallbacks{checkpointsOnly: visitor.CheckpointsOnly}
	if visitor.Commit != nil {
		callbacks.commit = func(commit NativeDeltaRowCommit) error {
			copyOwner := work.Scope()
			defer copyOwner.Release()
			if err := copyOwner.ChargeJSON("combined_commit_callback", commit); err != nil {
				return err
			}
			return visitor.Commit(cloneNativeDeltaCommit(commit))
		}
	}
	if visitor.TableState != nil {
		callbacks.tableState = func(state NativeDeltaTableState) error {
			copyOwner := work.Scope()
			defer copyOwner.Release()
			if err := copyOwner.ChargeJSON("combined_table_callback", state); err != nil {
				return err
			}
			return visitor.TableState(cloneNativeDeltaState(state))
		}
	}
	return a.visitNativeDeltaOpen(ctx, nil, callbacks, open)
}
