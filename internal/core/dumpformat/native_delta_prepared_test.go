package dumpformat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// This deliberately synthetic branch/merge fixture exercises the real framed
// reader and reconstructor. It grants no physical producer qualification.
func preparedDeltaFixture(t *testing.T) (*Archive, *int) {
	t.Helper()
	// Use the inherited branch/merge states, but supply the physical pinned
	// migration prefix required by the real archive visitor. The lower-level
	// reconstruction fixture intentionally omits that admission evidence.
	states := deltaReconstructionFixture(t)
	bag := NativeDeltaTableSchema{Name: "bag", Columns: []NativeDeltaColumn{{Name: "value", SQLType: "text"}}}
	items := NativeDeltaTableSchema{Name: "items", Columns: []NativeDeltaColumn{{Name: "id", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	migrations := NativeDeltaTableSchema{Name: "schema_migrations", Columns: []NativeDeltaColumn{{Name: "version", SQLType: "bigint", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "content_hash", SQLType: "text"}}}
	migrationRows := []NativeDeltaRow{}
	migrationTable := NativeTable{Columns: []string{"content_hash", "version"}, Rows: []Row{}}
	for version := 1; version <= 66; version++ {
		text := fmt.Sprint(version)
		fields := Row{"version": json.Number(text), "content_hash": migrationHashes["main"][version]}
		raw, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		cells := []NativeDeltaCell{{Kind: "int64", Value: text}, {Kind: "string", Value: migrationHashes["main"][version]}}
		key, err := NativeDeltaPrimaryKey(migrations, cells)
		if err != nil {
			t.Fatal(err)
		}
		migrationRows = append(migrationRows, NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(append(raw, '\n'))})
		migrationTable.Rows = append(migrationTable.Rows, fields)
	}
	tables := map[string]NativeTable{"bag": states.expected[0].Tables["bag"], "items": states.expected[0].Tables["items"], "schema_migrations": migrationTable}
	schema := NativeDeltaSchema{Type: "schema", Profile: NativeCommitSchemaProfile{Format: NativeCommitSchemaFormat, Scope: NativeCommitSchemaScope, MainVersion: deltaTestUint(66), Absences: nativeCommitAbsences(tables, nativeCommitSchemaSurfaces())}, Tables: []NativeDeltaTableSchema{bag, items, migrations}}
	schema.SchemaID = NativeDeltaSchemaID(schema)
	f := &deltaTestFixture{t: t, root: states.root}
	f.write(NativeDeltaHeader{Type: "header", Format: NativeDeltaFormat, StoreID: f.root.StoreID, Projects: f.root.Projects, Head: f.root.Head, CommitCount: 4, Ignore: NativeTable{Columns: []string{"ignored", "pattern"}, Rows: []Row{}}, ConfigPolicy: map[string]string{}})
	f.schema(schema)
	parents := [][]string{{}, {strings.Repeat("a", 32)}, {strings.Repeat("a", 32)}, {strings.Repeat("c", 32), strings.Repeat("b", 32)}}
	retained := map[string]map[string][]NativeDeltaRow{}
	var head []deltaTestTransaction
	for i, image := range states.expected {
		transactions := []deltaTestTransaction{}
		current := map[string][]NativeDeltaRow{}
		for _, table := range []NativeDeltaTableSchema{bag, items, migrations} {
			rows := migrationRows
			if table.Name != "schema_migrations" {
				rows = []NativeDeltaRow{}
				for occurrence, row := range image.Tables[table.Name].Rows {
					id := ""
					if table.Name == "items" {
						id = string(row["id"].(json.Number))
					}
					rows = append(rows, deltaTestRow(t, table, id, row["value"].(string), uint64(occurrence)))
				}
			}
			var before []NativeDeltaRow
			tableMode := "replace"
			if len(parents[i]) > 0 {
				tableMode = "delta"
				before = retained[parents[i][0]][table.Name]
			}
			transactions = append(transactions, deltaTestTransaction{table, tableMode, rows, preparedDeltaOperations(t, before, rows)})
			current[table.Name] = rows
		}
		mode := "snapshot"
		if len(parents[i]) > 0 {
			mode = "delta"
		}
		f.transaction("commit", uint64(i+1), image.CommitID, schema, parents[i], mode, transactions)
		retained[image.CommitID] = current
		head = transactions
	}
	for i := range head {
		head[i].mode = "replace"
		head[i].ops = preparedDeltaOperations(t, nil, head[i].rows)
	}
	for _, kind := range []string{"head-checkpoint", "current"} {
		f.transaction(kind, 0, f.root.Head, schema, nil, "", head)
	}
	f.write(NativeDeltaEnd{Type: "end", Totals: f.totals, HeadRootSHA256: f.lastRoot, CurrentRootSHA256: f.lastRoot})
	f.root.Totals = f.totals
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

// The fixture's operations are relative to its actual first parent, including
// the divergent branch. Checkpoints independently contain full replacements.
func preparedDeltaOperations(t *testing.T, before, after []NativeDeltaRow) []any {
	t.Helper()
	index := func(rows []NativeDeltaRow) map[string]NativeDeltaRow {
		result := map[string]NativeDeltaRow{}
		for _, row := range rows {
			key, err := base64.StdEncoding.DecodeString(row.Key)
			if err != nil {
				t.Fatal(err)
			}
			result[string(key)] = row
		}
		return result
	}
	previous, current := index(before), index(after)
	operations := map[string]any{}
	for key, row := range previous {
		if _, exists := current[key]; !exists {
			operations[key] = NativeDeltaDelete{Type: "row", Op: "delete", Key: row.Key}
		}
	}
	for key, row := range current {
		old, exists := previous[key]
		if exists && reflect.DeepEqual(old.Cells, row.Cells) && old.NormalizedJSON == row.NormalizedJSON {
			continue
		}
		if exists {
			row.Op = "update"
		}
		operations[key] = row
	}
	keys := make([]string, 0, len(operations))
	for key := range operations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]any, 0, len(keys))
	for _, key := range keys {
		result = append(result, operations[key])
	}
	return result
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
	if err != nil || changes != 4 || checkpoints != 8 || files != 3 {
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

func TestNativeDeltaPreparedReadbackAfterSourceReaderClosed(t *testing.T) {
	a, opens := preparedDeltaFixture(t)
	defer a.ReleaseNativeDecoded()
	if _, err := a.PrepareNativeDelta(context.Background()); err != nil {
		t.Fatal("prepare authenticated native delta", err)
	}
	a.UseNativeDeltaPreparedReadback()
	// Complete table-state readback retains its original contract, including
	// every authenticated historical commit state, while the source reader is
	// still available. Prepared staging separately retains eight checkpoints.
	states := 0
	var before []NativeDeltaTableState
	if err := a.VisitNativeDeltaTableStates(context.Background(), func(state NativeDeltaTableState) error {
		states++
		before = append(before, cloneNativeDeltaState(state))
		return nil
	}); err != nil || states != 24 {
		t.Fatalf("complete table readback failed before source close: states=%d err=%v", states, err)
	}
	opened := *opens
	// Recovery can outlive the original native capture reader. A row readback
	// that reopens it would report the closed-reader error even though the
	// complete authenticated preparation is still retained.
	closedSourceOpens := 0
	a.delta.open = func(context.Context, uint64) (io.ReadCloser, error) {
		closedSourceOpens++
		return nil, errors.New("native capture reader closed")
	}
	commits, checkpoints := 0, 0
	if err := a.VisitNativeDeltaRowChanges(context.Background(), func(commit NativeDeltaRowCommit) error {
		commits++
		return nil
	}, func(_ string, _ NativeDeltaTableSchema, _ []NativeDeltaRow, _ map[string]string) error {
		checkpoints++
		return nil
	}); err != nil {
		t.Fatalf("prepared row readback reopened closed source: %v", err)
	}
	if commits != 4 || checkpoints != 6 || *opens != opened {
		t.Fatalf("prepared readback did not use retained source: commits=%d checkpoints=%d opens=%d want=%d", commits, checkpoints, *opens, opened)
	}
	// Complete table-state readback replays the retained original parts. It
	// must include every historical commit state, not just eight checkpoints.
	var after []NativeDeltaTableState
	if err := a.VisitNativeDeltaTableStates(context.Background(), func(state NativeDeltaTableState) error {
		after = append(after, cloneNativeDeltaState(state))
		return nil
	}); err != nil || !reflect.DeepEqual(before, after) || *opens != opened || closedSourceOpens != 0 {
		t.Fatalf("complete retained table readback: states=%d opens=%d closed_opens=%d err=%v", len(after), *opens, closedSourceOpens, err)
	}
	a.ReleaseNativeDecoded()
	usage, _ := a.NativeDecodedUsage()
	if usage.CurrentAccountedDecodedBytes != 0 || len(usage.CurrentCategories) != 0 || len(usage.CurrentCharges) != 0 {
		t.Fatal("released preparation retained decoded ownership", usage)
	}
	if err := a.VisitNativeDeltaTableStates(context.Background(), func(NativeDeltaTableState) error { return nil }); err == nil || closedSourceOpens != 0 {
		t.Fatal("released archive used retained parts or reopened original source", err, closedSourceOpens)
	}
}

func TestNativeDeltaRetainedOriginalReplayRejectsCorruption(t *testing.T) {
	a, opens := preparedDeltaFixture(t)
	prepared, err := a.PrepareNativeDelta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a.UseNativeDeltaPreparedReadback()
	opened := *opens
	// The private retained bytes must still pass the real part/stream verifier;
	// possession of an authenticated preparation never bypasses commitments.
	part := prepared.files["parts/00000000000000000001.part"]
	part[len(part)-1] ^= 1
	if err := a.VisitNativeDeltaTableStates(context.Background(), func(NativeDeltaTableState) error { return nil }); err == nil || *opens != opened {
		t.Fatal("corrupt retained original accepted or original source reopened", err)
	}
}
