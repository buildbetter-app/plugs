package dumpformat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

// This deliberately synthetic branch/merge fixture exercises the real framed
// reader and reconstructor. It grants no physical producer qualification.
func preparedDeltaFixture(t *testing.T) (*Archive, *int) {
	t.Helper()
	f := deltaReconstructionFixture(t)
	parts, last := deltaTestParts(t, f.stream.Bytes(), NativeDeltaPartPayloadLimit)
	f.root.PartCount = uint64(len(parts))
	f.root.FinalPartSHA256 = last
	f.root.StreamSHA256 = digest(f.stream.Bytes())
	f.root.StreamBytes = uint64(f.stream.Len())
	owner := NewNativeDeltaBudget(0).Scope()
	h, proof, err := reconstructNativeDeltaCallbacks(context.Background(), bytes.NewReader(f.stream.Bytes()), f.root, owner, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	opens := new(int)
	a := &Archive{sha256: digest([]byte("synthetic root")), nativeBudget: owner, nativeHistories: []NativeHistory{h}}
	a.delta = &nativeDeltaAttachment{root: f.root, rootBytes: []byte("synthetic root"), budget: owner, proof: proof, prepared: &nativeDeltaPreparation{owner: owner}, open: func(_ context.Context, n uint64) (io.ReadCloser, error) {
		*opens++
		return io.NopCloser(bytes.NewReader(parts[n-1])), nil
	}}
	t.Cleanup(a.ReleaseNativeDecoded)
	return a, opens
}

func TestNativeDeltaPreparedActualReplayIsolationOwnershipAndFreshReadback(t *testing.T) {
	a, opens := preparedDeltaFixture(t)
	baseline, _ := a.NativeDecodedUsage()
	work, release := a.NativeDeltaAccountingScope()
	p, err := work.PrepareNativeDelta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release() // The archive, not this transient conversion scope, owns staging.
	if *opens != 1 {
		t.Fatal("preparation did not traverse actual source exactly once", *opens)
	}
	staged, _ := a.NativeDecodedUsage()
	for _, phase := range []string{"receiving_staged_changes", "receiving_staged_checkpoints", "receiving_staged_originals"} {
		if staged.CurrentCategories[phase] == 0 {
			t.Fatal("retained payload unaccounted", phase)
		}
	}
	var changes, checkpoints, files int
	var originalBody []byte
	err = p.Visit(context.Background(), NativeDeltaVisitor{
		Commit: func(commit NativeDeltaRowCommit) error {
			changes++
			if len(commit.Changes) > 0 && commit.Changes[0].After != nil {
				if originalBody == nil {
					originalBody = bytes.Clone(commit.Changes[0].After.Body)
				}
				commit.Changes[0].After.Body[0] ^= 1
			}
			return nil
		},
		TableState: func(state NativeDeltaTableState) error {
			if state.Kind == "commit" {
				t.Fatal("full historical table retained")
			}
			checkpoints++
			if len(state.Rows) > 0 {
				state.Rows[0].Cells[0].Value = "caller mutation"
			}
			if len(state.Schema.Columns) > 0 && state.Schema.Columns[0].PrimaryKeyOrdinal != nil {
				*state.Schema.Columns[0].PrimaryKeyOrdinal = 99
			}
			if len(state.Normalized.Rows) > 0 {
				for key := range state.Normalized.Rows[0] {
					state.Normalized.Rows[0][key] = "caller mutation"
				}
			}
			return nil
		},
		File: func(_ string, raw []byte) error {
			files++
			if len(raw) > 0 {
				raw[0] ^= 1
			}
			return nil
		},
	})
	if err != nil || changes != 4 || checkpoints != 6 || files != 3 {
		t.Fatal("actual staged census differs", changes, checkpoints, files, err)
	}
	var first NativeDeltaRowCommit
	var snapshots []NativeDeltaTableState
	var originals = map[string][]byte{}
	err = p.Visit(context.Background(), NativeDeltaVisitor{Commit: func(c NativeDeltaRowCommit) error {
		if first.Sequence == 0 {
			first = c
		}
		return nil
	}, TableState: func(s NativeDeltaTableState) error { snapshots = append(snapshots, s); return nil }, File: func(n string, b []byte) error { originals[n] = b; return nil }})
	if err != nil || len(first.Changes) == 0 || !bytes.Equal(first.Changes[0].After.Body, originalBody) {
		t.Fatal("detached commit mutation escaped", err)
	}
	if !reflect.DeepEqual(snapshots, p.checkpoints) || !reflect.DeepEqual(originals, p.files) {
		t.Fatal("detached checkpoint/raw mutation escaped")
	}
	if *opens != 1 {
		t.Fatal("prepared consumers reread physical source")
	}
	fresh := 0
	if err := a.VisitNativeDeltaTableStates(context.Background(), func(s NativeDeltaTableState) error {
		if s.Kind == "commit" && s.Complete {
			fresh++
		}
		return nil
	}); err != nil || fresh != 4 || *opens != 2 {
		t.Fatal("independent readback did not authenticate fresh source", fresh, *opens, err)
	}
	a.ClearNativeDeltaPreparation()
	if err := p.Visit(context.Background(), NativeDeltaVisitor{}); err == nil {
		t.Fatal("released view remained usable")
	}
	held, _ := a.NativeDecodedUsage()
	if held.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(held.CurrentCategories, baseline.CurrentCategories) || !reflect.DeepEqual(held.CurrentCharges, baseline.CurrentCharges) {
		t.Fatal("preparation ownership escaped release", baseline, held)
	}
}

func TestNativeDeltaPreparedTerminalRefusalCancellationAndLimitUnwind(t *testing.T) {
	for _, fault := range []string{"final-original-recheck", "late-terminal-read", "source-close", "cancel-after-callback", "decoded-limit"} {
		t.Run(fault, func(t *testing.T) {
			a, _ := preparedDeltaFixture(t)
			baseline, _ := a.NativeDecodedUsage()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			baseOpen := a.delta.open
			checks, candidates := 0, 0
			a.delta.budget.Observe(func(e NativeDeltaAllocationEvent) {
				if e.Action == "reserve" && e.Phase == "receiving_staged_changes" {
					candidates++
					if fault == "cancel-after-callback" {
						cancel()
					}
				}
			})
			switch fault {
			case "final-original-recheck":
				a.delta.recheck = func(context.Context) error {
					checks++
					if checks == 2 {
						return fmt.Errorf("original changed after callbacks")
					}
					return nil
				}
			case "late-terminal-read", "source-close":
				a.delta.open = func(ctx context.Context, n uint64) (io.ReadCloser, error) {
					f, err := baseOpen(ctx, n)
					if err != nil {
						return nil, err
					}
					return &preparedFaultReader{ReadCloser: f, readFailure: fault == "late-terminal-read", closeFailure: fault == "source-close"}, nil
				}
			case "decoded-limit":
				a.delta.budget.root().limit = baseline.CurrentAccountedDecodedBytes + 1
			}
			p, err := a.PrepareNativeDelta(ctx)
			if err == nil || p != nil || a.delta.prepared.view != nil {
				t.Fatal("unqualified candidate escaped", fault, err)
			}
			if fault == "final-original-recheck" && (checks != 2 || candidates != 4) {
				t.Fatal("terminal check did not follow genuine callbacks", checks, candidates)
			}
			if fault == "cancel-after-callback" && (candidates == 0 || !errors.Is(err, context.Canceled)) {
				t.Fatal("late cancellation not reached", candidates, err)
			}
			if fault == "decoded-limit" {
				var limit *NativeDeltaDecodedLimitError
				if !errors.As(err, &limit) || limit.AdmittedBytes > limit.LimitBytes {
					t.Fatal("limit refused after retaining", err)
				}
			}
			held, _ := a.NativeDecodedUsage()
			if held.CurrentAccountedDecodedBytes != baseline.CurrentAccountedDecodedBytes || !reflect.DeepEqual(held.CurrentCategories, baseline.CurrentCategories) || !reflect.DeepEqual(held.CurrentCharges, baseline.CurrentCharges) {
				t.Fatal("failed preparation leaked", fault, baseline, held)
			}
		})
	}
}

type preparedFaultReader struct {
	io.ReadCloser
	readFailure, closeFailure bool
}

func (r *preparedFaultReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err == io.EOF && r.readFailure {
		err = fmt.Errorf("late terminal read failure")
	}
	return n, err
}
func (r *preparedFaultReader) Close() error {
	err := r.ReadCloser.Close()
	if r.closeFailure {
		return fmt.Errorf("source close failure")
	}
	return err
}
