package dumpformat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

type NativeDeltaCommitProof struct {
	Sequence   uint64                            `json:"sequence"`
	CommitID   string                            `json:"commit_id"`
	SchemaID   string                            `json:"schema_id"`
	RootSHA256 string                            `json:"root_sha256"`
	Tables     map[string]NativeDeltaTableDigest `json:"tables"`
}

type NativeDeltaVerification struct {
	Contract          string                   `json:"contract"`
	Totals            NativeDeltaTotals        `json:"totals"`
	HeadRootSHA256    string                   `json:"head_root_sha256"`
	CurrentRootSHA256 string                   `json:"current_root_sha256"`
	Commits           []NativeDeltaCommitProof `json:"commits"`
	Usage             NativeDeltaDecodedUsage  `json:"decoded_usage"`
}

type deltaStagedTable struct {
	schema NativeDeltaTableSchema
	path   string
	digest NativeDeltaTableDigest
}
type deltaStagedState struct {
	schemaID   string
	tables     map[string]deltaStagedTable
	image      string
	imageBytes uint64
	root       string
}
type nativeDeltaReconstructor struct {
	ctx         context.Context
	input       *nativeDeltaRecordReader
	root        NativeCaptureSet
	header      NativeDeltaHeader
	budget      *NativeDeltaBudget
	dir         string
	schemas     map[string]NativeDeltaSchema
	usedSchemas map[string]bool
	states      map[string]deltaStagedState
	commits     []SourceCommit
	proof       NativeDeltaVerification
	totals      NativeDeltaTotals
	nextFile    uint64
}

func deltaAddCount(target *uint64, n uint64) error {
	if n > math.MaxUint64-*target {
		return fmt.Errorf("native delta counter overflow")
	}
	*target += n
	return nil
}

func (r *nativeDeltaReconstructor) next() ([]byte, string, error) {
	line, err := r.input.next()
	if err != nil {
		return nil, "", err
	}
	var shape map[string]any
	if err := decodeJSON(line, &shape); err != nil {
		return nil, "", err
	}
	kind, ok := shape["type"].(string)
	if !ok {
		return nil, "", fmt.Errorf("native delta record type missing")
	}
	return line, kind, nil
}

// reconstructNativeDelta stages independently verified first-parent states in
// an owned temporary directory. No history is exposed until terminal framing,
// all reconstruction commitments and both independent snapshots have passed.
func reconstructNativeDelta(ctx context.Context, input io.Reader, root NativeCaptureSet, budget *NativeDeltaBudget) (NativeHistory, NativeDeltaVerification, error) {
	dir, err := os.MkdirTemp("", "native-delta-reconstruct-")
	if err != nil {
		return NativeHistory{}, NativeDeltaVerification{}, err
	}
	defer os.RemoveAll(dir)
	r := nativeDeltaReconstructor{ctx: ctx, input: newNativeDeltaRecordReader(ctx, input), root: root, budget: budget, dir: dir, schemas: map[string]NativeDeltaSchema{}, usedSchemas: map[string]bool{}, states: map[string]deltaStagedState{}, proof: NativeDeltaVerification{Contract: NativeDeltaFormat, Commits: []NativeDeltaCommitProof{}}}
	h, err := r.run()
	r.proof.Usage = budget.Usage()
	return h, r.proof, err
}

func (r *nativeDeltaReconstructor) schema(raw []byte) error {
	var s NativeDeltaSchema
	if err := decodeNativeDeltaJSON(raw, &s); err != nil {
		return err
	}
	if s.Type != "schema" || s.SchemaID != NativeDeltaSchemaID(s) || s.Profile.Format != NativeCommitSchemaFormat || s.Profile.Scope != NativeCommitSchemaScope {
		return fmt.Errorf("native delta schema identity/profile differs")
	}
	if _, ok := r.schemas[s.SchemaID]; ok {
		return fmt.Errorf("native delta schema repeated")
	}
	previous := ""
	for _, t := range s.Tables {
		if !nativeSQLName.MatchString(t.Name) || t.Name <= previous || len(t.Columns) == 0 {
			return fmt.Errorf("native delta schema table order/shape differs")
		}
		previous = t.Name
		names, pk := map[string]bool{}, map[uint64]bool{}
		for _, c := range t.Columns {
			if !nativeSQLName.MatchString(c.Name) || names[c.Name] || c.SQLType == "" {
				return fmt.Errorf("native delta column shape differs")
			}
			names[c.Name] = true
			if c.PrimaryKeyOrdinal != nil {
				if pk[*c.PrimaryKeyOrdinal] {
					return fmt.Errorf("native delta duplicate PK ordinal")
				}
				pk[*c.PrimaryKeyOrdinal] = true
			}
		}
		for i := uint64(0); i < uint64(len(pk)); i++ {
			if !pk[i] {
				return fmt.Errorf("native delta noncontiguous PK ordinals")
			}
		}
	}
	previous = ""
	for _, a := range s.Profile.Absences {
		k := a.Family + "\x00" + a.Column + "\x00" + a.Reason
		if k <= previous {
			return fmt.Errorf("native delta absence order differs")
		}
		previous = k
	}
	if err := r.budget.ChargeJSON("schemas", s); err != nil {
		return err
	}
	r.schemas[s.SchemaID] = s
	return deltaAddCount(&r.totals.Schemas, 1)
}

func (r *nativeDeltaReconstructor) readRows(table deltaStagedTable) (map[string]NativeDeltaRow, error) {
	rows := map[string]NativeDeltaRow{}
	if table.path == "" {
		return rows, nil
	}
	f, err := os.Open(table.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := newNativeDeltaRecordReader(r.ctx, f)
	for {
		raw, err := reader.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var row NativeDeltaRow
		if err := decodeNativeDeltaJSON(raw, &row); err != nil {
			return nil, err
		}
		key, err := nativeDeltaBase64(row.Key)
		if err != nil {
			return nil, err
		}
		if err := r.budget.ChargeJSON("decoded_baseline_rows", row); err != nil {
			return nil, err
		}
		rows[string(key)] = row
	}
	return rows, nil
}

func (r *nativeDeltaReconstructor) table(open NativeDeltaTable, schema NativeDeltaTableSchema, base deltaStagedTable, replace bool) (deltaStagedTable, NativeTable, error) {
	var zero deltaStagedTable
	var normalized NativeTable
	mode := "delta"
	if replace {
		mode = "replace"
	}
	if open.Type != "table" || open.Name != schema.Name || open.TableSchemaID != NativeDeltaTableSchemaID(schema) || open.Mode != mode || !validDeltaSHA256(open.ExpectedSHA256) {
		return zero, normalized, fmt.Errorf("native delta table binding/mode differs")
	}
	rows := map[string]NativeDeltaRow{}
	if !replace {
		var err error
		rows, err = r.readRows(base)
		if err != nil {
			return zero, normalized, err
		}
	}
	var operations uint64
	var previous []byte
	for {
		raw, kind, err := r.next()
		if err != nil {
			return zero, normalized, err
		}
		if kind == "end_table" {
			var end NativeDeltaEndTable
			if err := decodeNativeDeltaJSON(raw, &end); err != nil {
				return zero, normalized, err
			}
			if end.RowOperations != operations || end.Rows != uint64(len(rows)) || end.Rows != open.ExpectedRows || end.SHA256 != open.ExpectedSHA256 {
				return zero, normalized, fmt.Errorf("native delta table terminal counts differ")
			}
			break
		}
		if kind != "row" {
			return zero, normalized, fmt.Errorf("native delta table row expected")
		}
		var fields map[string]any
		if err := decodeJSON(raw, &fields); err != nil {
			return zero, normalized, err
		}
		op, _ := fields["op"].(string)
		var row NativeDeltaRow
		var key []byte
		if op == "delete" {
			var d NativeDeltaDelete
			if err := decodeNativeDeltaJSON(raw, &d); err != nil {
				return zero, normalized, err
			}
			key, err = nativeDeltaBase64(d.Key)
		} else {
			if err := decodeNativeDeltaJSON(raw, &row); err != nil {
				return zero, normalized, err
			}
			_, key, err = validateNativeDeltaRow(schema, row, r.header.ConfigPolicy)
		}
		if err != nil || len(key) == 0 {
			return zero, normalized, fmt.Errorf("native delta invalid row/key: %w", err)
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return zero, normalized, fmt.Errorf("native delta operation keys unordered")
		}
		previous = key
		old, present := rows[string(key)]
		switch op {
		case "insert":
			if present {
				return zero, normalized, fmt.Errorf("native delta insert key exists")
			}
		case "update":
			if replace || !present || old.NormalizedJSON == row.NormalizedJSON && equalJSON(old.Cells, row.Cells) {
				return zero, normalized, fmt.Errorf("native delta update precondition differs")
			}
		case "delete":
			if replace || !present {
				return zero, normalized, fmt.Errorf("native delta delete key absent")
			}
		default:
			return zero, normalized, fmt.Errorf("native delta operation unsupported")
		}
		if op == "delete" {
			if err := r.budget.ChargeBytes("decoded_operations", uint64(len(raw))); err != nil {
				return zero, normalized, err
			}
			delete(rows, string(key))
		} else {
			// The bounded input row is charged before it becomes retained state.
			if err := r.budget.ChargeJSON("decoded_operations", row); err != nil {
				return zero, normalized, err
			}
			rows[string(key)] = row
		}
		if err := deltaAddCount(&operations, 1); err != nil {
			return zero, normalized, err
		}
	}
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([]NativeDeltaRow, 0, len(keys))
	occurrences := map[string]uint64{}
	bagPayloads := map[string][]byte{}
	normalized = NativeTable{Columns: []string{}, Rows: []Row{}}
	for _, c := range schema.Columns {
		normalized.Columns = append(normalized.Columns, c.Name)
	}
	sort.Strings(normalized.Columns)
	if err := r.budget.ChargeJSON("reconstructed_table_metadata", normalized); err != nil {
		return zero, normalized, err
	}
	for _, key := range keys {
		row := rows[key]
		fields, _, err := validateNativeDeltaRow(schema, row, r.header.ConfigPolicy)
		if err != nil {
			return zero, normalized, err
		}
		pk, err := NativeDeltaPrimaryKey(schema, row.Cells)
		if err != nil {
			return zero, normalized, err
		}
		if pk == nil {
			payload, err := nativeDeltaBase64(row.NormalizedJSON)
			if err != nil {
				return zero, normalized, err
			}
			payload = NativeDeltaRowPayload(row.Cells, payload)
			prefix := string(NativeDeltaBagKey(payload, 0)[:36])
			if previous, ok := bagPayloads[prefix]; ok && !bytes.Equal(previous, payload) {
				return zero, normalized, fmt.Errorf("native delta keyless payload digest collision")
			}
			bagPayloads[prefix] = payload
			occ := occurrences[prefix]
			if !bytes.Equal([]byte(key), NativeDeltaBagKey(payload, occ)) {
				return zero, normalized, fmt.Errorf("native delta keyless multiplicity differs")
			}
			occurrences[prefix] = occ + 1
		}
		if err := r.budget.ChargeJSON("reconstructed_rows", fields); err != nil {
			return zero, normalized, err
		}
		normalized.Rows = append(normalized.Rows, fields)
		row.Op = "insert"
		ordered = append(ordered, row)
	}
	hash, err := NativeDeltaTableSHA256(schema, ordered)
	if err != nil || hash != open.ExpectedSHA256 {
		return zero, normalized, fmt.Errorf("native delta reconstructed table hash differs")
	}
	// Legacy normalized tables order rows by their exact canonical JSON bytes.
	sort.SliceStable(normalized.Rows, func(i, j int) bool {
		a, _ := json.Marshal(normalized.Rows[i])
		b, _ := json.Marshal(normalized.Rows[j])
		return bytes.Compare(a, b) < 0
	})
	r.nextFile++
	path := filepath.Join(r.dir, fmt.Sprintf("table-%020d.ndjson", r.nextFile))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return zero, normalized, err
	}
	enc := json.NewEncoder(f)
	for _, row := range ordered {
		if err = enc.Encode(row); err != nil {
			break
		}
	}
	closeErr := f.Close()
	if err != nil {
		return zero, normalized, err
	}
	if closeErr != nil {
		return zero, normalized, closeErr
	}
	if err := deltaAddCount(&r.totals.TableTransactions, 1); err != nil {
		return zero, normalized, err
	}
	if err := deltaAddCount(&r.totals.RowOperations, operations); err != nil {
		return zero, normalized, err
	}
	if err := deltaAddCount(&r.totals.ReconstructedRows, uint64(len(rows))); err != nil {
		return zero, normalized, err
	}
	if uint64(len(rows)) > math.MaxUint64/uint64(len(schema.Columns)) {
		return zero, normalized, fmt.Errorf("native delta cell count overflow")
	}
	if err := deltaAddCount(&r.totals.ReconstructedCells, uint64(len(rows))*uint64(len(schema.Columns))); err != nil {
		return zero, normalized, err
	}
	return deltaStagedTable{schema, path, NativeDeltaTableDigest{uint64(len(rows)), hash}}, normalized, nil
}

func (r *nativeDeltaReconstructor) transaction(schemaID string, base deltaStagedState, snapshot bool, sequence uint64, commitID, expectedRoot, endKind string) (deltaStagedState, NativeHistoryImage, error) {
	var zero deltaStagedState
	schema, ok := r.schemas[schemaID]
	if !ok {
		return zero, NativeHistoryImage{}, fmt.Errorf("native delta schema unavailable")
	}
	r.usedSchemas[schemaID] = true
	image := NativeHistoryImage{Sequence: sequence, CommitID: commitID, Tables: map[string]NativeTable{}, SchemaProfile: &schema.Profile}
	if err := r.budget.ChargeJSON("image_metadata", image); err != nil {
		return zero, image, err
	}
	want := map[string]NativeDeltaTableSchema{}
	names := []string{}
	for _, t := range schema.Tables {
		want[t.Name] = t
		names = append(names, t.Name)
	}
	for name := range base.tables {
		if _, ok := want[name]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	state := deltaStagedState{schemaID: schemaID, tables: map[string]deltaStagedTable{}}
	digests := map[string]NativeDeltaTableDigest{}
	for _, name := range names {
		raw, kind, err := r.next()
		if err != nil {
			return zero, image, err
		}
		t, present := want[name]
		old, existed := base.tables[name]
		if !present {
			var drop NativeDeltaDropTable
			if kind != "drop_table" {
				return zero, image, fmt.Errorf("native delta table drop missing")
			}
			if err := decodeNativeDeltaJSON(raw, &drop); err != nil {
				return zero, image, err
			}
			if snapshot || drop.Name != name || drop.PreviousTableSchemaID != NativeDeltaTableSchemaID(old.schema) {
				return zero, image, fmt.Errorf("native delta drop binding differs")
			}
			continue
		}
		if kind != "table" {
			return zero, image, fmt.Errorf("native delta table census differs")
		}
		var open NativeDeltaTable
		if err := decodeNativeDeltaJSON(raw, &open); err != nil {
			return zero, image, err
		}
		replace := snapshot || !existed || NativeDeltaTableSchemaID(t) != NativeDeltaTableSchemaID(old.schema)
		staged, table, err := r.table(open, t, old, replace)
		if err != nil {
			return zero, image, err
		}
		state.tables[name] = staged
		image.Tables[name] = table
		digests[name] = staged.digest
	}
	root, err := NativeDeltaRootSHA256(digests)
	if err != nil || root != expectedRoot {
		return zero, image, fmt.Errorf("native delta reconstructed state root differs")
	}
	raw, kind, err := r.next()
	if err != nil || kind != endKind {
		return zero, image, fmt.Errorf("native delta transaction terminal missing: %w", err)
	}
	if endKind == "end_commit" {
		var end NativeDeltaEndCommit
		if err := decodeNativeDeltaJSON(raw, &end); err != nil {
			return zero, image, err
		}
		if end.Sequence != sequence || end.CommitID != commitID || end.TableCount != uint64(len(want)) || end.RootSHA256 != root {
			return zero, image, fmt.Errorf("native delta commit terminal differs")
		}
	} else {
		var end NativeDeltaEndCheckpoint
		if err := decodeNativeDeltaJSON(raw, &end); err != nil {
			return zero, image, err
		}
		if end.TableCount != uint64(len(want)) || end.RootSHA256 != root {
			return zero, image, fmt.Errorf("native delta checkpoint terminal differs")
		}
	}
	state.root = root
	if endKind == "end_commit" {
		if err := r.budget.ChargeJSON("reconstructed_images", image); err != nil {
			return zero, image, err
		}
		size, err := NativeDeltaJSONSize(image)
		if err != nil {
			return zero, image, err
		}
		if err := deltaAddCount(&r.totals.FullImageBytes, size); err != nil {
			return zero, image, err
		}
		state.image = filepath.Join(r.dir, fmt.Sprintf("image-%020d.json", sequence))
		state.imageBytes = size
		f, err := os.OpenFile(state.image, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return zero, image, err
		}
		err = json.NewEncoder(f).Encode(image)
		closeErr := f.Close()
		if err != nil {
			return zero, image, err
		}
		if closeErr != nil {
			return zero, image, closeErr
		}
		proof := NativeDeltaCommitProof{sequence, commitID, schemaID, root, digests}
		if err := r.budget.ChargeJSON("commit_proofs", proof); err != nil {
			return zero, image, err
		}
		r.proof.Commits = append(r.proof.Commits, proof)
	} else {
		if err := r.budget.ChargeJSON("checkpoints", image); err != nil {
			return zero, image, err
		}
	}
	return state, image, nil
}

func (r *nativeDeltaReconstructor) run() (NativeHistory, error) {
	var result NativeHistory
	raw, kind, err := r.next()
	if err != nil || kind != "header" {
		return result, fmt.Errorf("native delta header missing: %w", err)
	}
	if err := decodeNativeDeltaJSON(raw, &r.header); err != nil {
		return result, err
	}
	h := r.header
	if h.Format != NativeDeltaFormat || h.StoreID != r.root.StoreID || !equalJSON(h.Projects, r.root.Projects) || h.Head != r.root.Head || h.CommitCount != r.root.Totals.Commits || h.CommitCount == 0 {
		return result, fmt.Errorf("native delta header/root binding differs")
	}
	if err := validateNativeTable("dolt_ignore", h.Ignore); err != nil {
		return result, err
	}
	for key, value := range h.ConfigPolicy {
		if value != "public" && value != "secret" {
			return result, fmt.Errorf("native delta config policy value differs")
		}
		if len(key) < 8 || (!bytes.HasPrefix([]byte(key), []byte("config/")) && !bytes.HasPrefix([]byte(key), []byte("metadata/"))) {
			return result, fmt.Errorf("native delta config policy scope differs")
		}
	}
	if err := r.budget.ChargeJSON("header", h); err != nil {
		return result, err
	}
	boundaries := map[string]bool{}
	if h.HistoryCoverage != nil {
		for _, b := range h.HistoryCoverage.Boundaries {
			boundaries[b.CommitID] = true
		}
	}
	for uint64(len(r.commits)) < h.CommitCount {
		raw, kind, err = r.next()
		if err != nil {
			return result, err
		}
		if kind == "schema" {
			if err := r.schema(raw); err != nil {
				return result, err
			}
			continue
		}
		if kind != "commit" {
			return result, fmt.Errorf("native delta immutable commit expected")
		}
		var c NativeDeltaCommit
		if err := decodeNativeDeltaJSON(raw, &c); err != nil {
			return result, err
		}
		if c.Sequence != uint64(len(r.commits))+1 || !validCommitID(c.CommitID) || c.Metadata == nil {
			return result, fmt.Errorf("native delta commit identity/order differs")
		}
		if _, exists := r.states[c.CommitID]; exists || boundaries[c.CommitID] {
			return result, fmt.Errorf("native delta duplicate/boundary commit")
		}
		parents := map[string]bool{}
		for _, p := range c.Parents {
			if !validCommitID(p) || parents[p] {
				return result, fmt.Errorf("native delta parent identity differs")
			}
			parents[p] = true
			if _, ok := r.states[p]; !ok && !boundaries[p] {
				return result, fmt.Errorf("native delta parent unavailable/out of order")
			}
		}
		base := deltaStagedState{tables: map[string]deltaStagedTable{}}
		snapshot := true
		if len(c.Parents) == 0 {
			if c.FirstParent != nil {
				return result, fmt.Errorf("native delta root first parent differs")
			}
		} else {
			if c.FirstParent == nil || *c.FirstParent != c.Parents[0] {
				return result, fmt.Errorf("native delta first parent differs")
			}
			if b, ok := r.states[c.Parents[0]]; ok {
				base = b
				snapshot = false
			}
		}
		if snapshot {
			if c.Mode != "snapshot" || c.BaseCommit != nil {
				return result, fmt.Errorf("native delta root/frontier must snapshot")
			}
		} else {
			if c.Mode != "delta" || c.BaseCommit == nil || *c.BaseCommit != c.Parents[0] {
				return result, fmt.Errorf("native delta retained first parent must be delta base")
			}
		}
		if err := validateSecretReferences(map[string]any(c.Metadata)); err != nil {
			return result, err
		}
		if err := r.budget.ChargeJSON("commit_metadata", c); err != nil {
			return result, err
		}
		state, _, err := r.transaction(c.SchemaID, base, snapshot, c.Sequence, c.CommitID, c.ExpectedRootSHA256, "end_commit")
		if err != nil {
			return result, err
		}
		r.states[c.CommitID] = state
		r.commits = append(r.commits, SourceCommit{c.CommitID, c.Parents, c.Metadata})
		if err := deltaAddCount(&r.totals.Commits, 1); err != nil {
			return result, err
		}
	}
	head, ok := r.states[h.Head]
	if !ok {
		return result, fmt.Errorf("native delta HEAD missing")
	}
	var current NativeHistoryImage
	for _, checkpoint := range []string{"head-checkpoint", "current"} {
		for {
			raw, kind, err = r.next()
			if err != nil {
				return result, err
			}
			if kind != "schema" {
				break
			}
			if err := r.schema(raw); err != nil {
				return result, err
			}
		}
		if kind != checkpoint {
			return result, fmt.Errorf("native delta independent checkpoint order differs")
		}
		var c NativeDeltaCheckpoint
		if err := decodeNativeDeltaJSON(raw, &c); err != nil {
			return result, err
		}
		if c.CommitID != h.Head {
			return result, fmt.Errorf("native delta checkpoint HEAD differs")
		}
		state, image, err := r.transaction(c.SchemaID, deltaStagedState{tables: map[string]deltaStagedTable{}}, true, 0, h.Head, c.ExpectedRootSHA256, "end_"+checkpoint)
		if err != nil {
			return result, err
		}
		if checkpoint == "head-checkpoint" {
			if state.schemaID != head.schemaID || state.root != head.root {
				return result, fmt.Errorf("native delta independent HEAD checkpoint differs")
			}
			for name, t := range state.tables {
				if t.digest != head.tables[name].digest {
					return result, fmt.Errorf("native delta independent HEAD table differs")
				}
			}
			r.proof.HeadRootSHA256 = state.root
		} else {
			current = image
			r.proof.CurrentRootSHA256 = state.root
		}
	}
	raw, kind, err = r.next()
	if err != nil || kind != "end" {
		return result, fmt.Errorf("native delta end missing: %w", err)
	}
	var end NativeDeltaEnd
	if err := decodeNativeDeltaJSON(raw, &end); err != nil {
		return result, err
	}
	if end.Totals != r.totals || r.root.Totals != r.totals || end.HeadRootSHA256 != r.proof.HeadRootSHA256 || end.CurrentRootSHA256 != r.proof.CurrentRootSHA256 {
		return result, fmt.Errorf("native delta final counters/roots differ")
	}
	if _, _, err := r.next(); err != io.EOF {
		return result, fmt.Errorf("native delta terminal stream not EOF: %w", err)
	}
	if len(r.usedSchemas) != len(r.schemas) {
		return result, fmt.Errorf("native delta unused schema")
	}
	seenPolicy := map[string]bool{}
	for _, family := range []string{"config", "metadata"} {
		for _, row := range current.Tables[family].Rows {
			key, _ := row["key"].(string)
			name := family + "/" + key
			policy, ok := h.ConfigPolicy[name]
			if key == "" || !ok || secretKey(key) && policy != "secret" {
				return result, fmt.Errorf("native delta current config policy missing")
			}
			if policy == "secret" {
				ref, ok := row["value"].(map[string]any)
				if !ok || len(ref) != 1 || ref["$secret"] != name {
					return result, fmt.Errorf("native delta current secret policy differs")
				}
			}
			seenPolicy[name] = true
		}
	}
	if len(seenPolicy) != len(h.ConfigPolicy) {
		return result, fmt.Errorf("native delta unused config policy")
	}
	// Reachability and boundary edge/ordinal semantics are verified separately
	// from physical first-parent ordering; no arbitrary DAG count equality.
	result = NativeHistory{Format: NativeHistoryFormat, StoreID: h.StoreID, Projects: h.Projects, Head: h.Head, Commits: r.commits, Ignore: h.Ignore, Current: current.Tables, Images: []NativeHistoryImage{}, Complete: h.HistoryCoverage == nil, Gaps: []NativeHistoryGap{}, HistoryCoverage: h.HistoryCoverage}
	for _, c := range r.commits {
		state := r.states[c.ID]
		if err := r.budget.ChargeBytes("aggregate_images", state.imageBytes); err != nil {
			return NativeHistory{}, err
		}
		f, err := os.Open(state.image)
		if err != nil {
			return NativeHistory{}, err
		}
		var image NativeHistoryImage
		decoder := json.NewDecoder(bufio.NewReader(f))
		decoder.UseNumber()
		err = decoder.Decode(&image)
		closeErr := f.Close()
		if err != nil {
			return NativeHistory{}, err
		}
		if closeErr != nil {
			return NativeHistory{}, closeErr
		}
		result.Images = append(result.Images, image)
	}
	r.proof.Totals = r.totals
	return result, nil
}
