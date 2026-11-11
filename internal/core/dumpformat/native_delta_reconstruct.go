package dumpformat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
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
	schemaID string
	tables   map[string]deltaStagedTable
	root     string
}
type nativeDeltaReconstructor struct {
	ctx            context.Context
	input          *nativeDeltaRecordReader
	root           NativeCaptureSet
	header         NativeDeltaHeader
	budget         *NativeDeltaBudget
	retainedBudget *NativeDeltaBudget
	visit          func(NativeHistoryImage, []string) error
	rowCallbacks   *nativeDeltaRowCallbacks
	rowMetadata    map[string]driver.Record
	validateImages bool
	previousTables map[string]deltaStagedTable
	dir            string
	schemas        map[string]NativeDeltaSchema
	usedSchemas    map[string]bool
	states         map[string]deltaStagedState
	commits        []SourceCommit
	proof          NativeDeltaVerification
	totals         NativeDeltaTotals
	nextFile       uint64
	knownGraph     []SourceCommit
	lastUse        map[string]uint64
	stagedBytes    map[string]uint64
	seenCommits    map[string]bool
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
	var shape struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &shape); err != nil {
		return nil, "", err
	}
	kind := shape.Type
	if kind == "" {
		return nil, "", fmt.Errorf("native delta record type missing")
	}
	return line, kind, nil
}

// reconstructNativeDelta stages independently verified first-parent states in
// an owned temporary directory. No history is exposed until terminal framing,
// all reconstruction commitments and both independent snapshots have passed.
func reconstructNativeDelta(ctx context.Context, input io.Reader, root NativeCaptureSet, budget *NativeDeltaBudget) (NativeHistory, NativeDeltaVerification, error) {
	return reconstructNativeDeltaVisit(ctx, input, root, budget, nil, false)
}

// visit receives a candidate commit only after its row/table/root checks. Its
// effects must remain private until this call returns with verified EOF. It
// must not retain the ephemeral image or mutate its tables.
func reconstructNativeDeltaVisit(ctx context.Context, input io.Reader, root NativeCaptureSet, budget *NativeDeltaBudget, visit func(NativeHistoryImage, []string) error, validateImages bool) (NativeHistory, NativeDeltaVerification, error) {
	return reconstructNativeDeltaCallbacks(ctx, input, root, budget, visit, nil, validateImages)
}

func reconstructNativeDeltaCallbacks(ctx context.Context, input io.Reader, root NativeCaptureSet, budget *NativeDeltaBudget, visit func(NativeHistoryImage, []string) error, rows *nativeDeltaRowCallbacks, validateImages bool, known ...[]SourceCommit) (NativeHistory, NativeDeltaVerification, error) {
	work := budget.Scope()
	defer work.Release()
	dir, err := os.MkdirTemp("", "native-delta-reconstruct-")
	if err != nil {
		return NativeHistory{}, NativeDeltaVerification{}, err
	}
	defer os.RemoveAll(dir)
	r := nativeDeltaReconstructor{ctx: ctx, input: newNativeDeltaRecordReader(ctx, input, work), root: root, budget: work, retainedBudget: work, visit: visit, rowCallbacks: rows, rowMetadata: map[string]driver.Record{}, validateImages: validateImages, previousTables: map[string]deltaStagedTable{}, dir: dir, schemas: map[string]NativeDeltaSchema{}, usedSchemas: map[string]bool{}, states: map[string]deltaStagedState{}, proof: NativeDeltaVerification{Contract: NativeDeltaFormat, Commits: []NativeDeltaCommitProof{}}}
	// Replays already have an authenticated complete ordered graph. Initial
	// authentication has no future-use oracle, so every possible parent stays
	// staged until EOF. Neither path retains decoded per-commit images.
	r.seenCommits = map[string]bool{}
	r.stagedBytes = map[string]uint64{}
	if len(known) != 0 && len(known[0]) != 0 {
		r.knownGraph = known[0]
		r.lastUse = map[string]uint64{}
		for i, commit := range r.knownGraph {
			if err := work.ChargeBytes("graph_frontier_index", uint64(len(commit.ID))+8); err != nil {
				return NativeHistory{}, NativeDeltaVerification{}, err
			}
			r.lastUse[commit.ID] = uint64(i + 2)
		}
		for i, commit := range r.knownGraph {
			if len(commit.Parents) != 0 {
				if _, retained := r.lastUse[commit.Parents[0]]; retained {
					r.lastUse[commit.Parents[0]] = uint64(i + 1)
				}
			}
		}
		r.lastUse[root.Head] = uint64(len(r.knownGraph)) + 1
	}
	defer r.input.Close()
	h, err := r.run()
	r.input.Close()
	if err == nil {
		// Reclassify the surviving payload at the ownership boundary. No
		// aggregate is encoded or copied here. Scratch can be dropped only
		// after the complete result has passed the same admission ceiling.
		historyBytes, countErr := NativeDeltaJSONSize(h)
		if countErr != nil {
			return NativeHistory{}, r.proof, countErr
		}
		proofBytes, countErr := NativeDeltaJSONSize(r.proof.Commits)
		if countErr != nil {
			return NativeHistory{}, r.proof, countErr
		}
		total := historyBytes
		if countErr := deltaAddCount(&total, proofBytes); countErr != nil {
			return NativeHistory{}, r.proof, countErr
		}
		if total > work.used {
			if countErr := work.ChargeBytes("returned_result_admission", total-work.used); countErr != nil {
				return NativeHistory{}, r.proof, countErr
			}
		}
		work.Release()
		if countErr := budget.ChargeBytes("retained_history", historyBytes); countErr != nil {
			return NativeHistory{}, r.proof, countErr
		}
		if countErr := budget.ChargeBytes("retained_commit_proofs", proofBytes); countErr != nil {
			return NativeHistory{}, r.proof, countErr
		}
	}
	r.proof.Usage = budget.Usage()
	return h, r.proof, err
}

func (r *nativeDeltaReconstructor) schema(raw []byte) error {
	var s NativeDeltaSchema
	if err := decodeNativeDeltaJSONWithBudget(raw, &s, r.budget, "schemas"); err != nil {
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
	r.schemas[s.SchemaID] = s
	return deltaAddCount(&r.totals.Schemas, 1)
}

func (r *nativeDeltaReconstructor) readRows(table deltaStagedTable, budget *NativeDeltaBudget) (map[string]NativeDeltaRow, error) {
	rows := map[string]NativeDeltaRow{}
	if table.path == "" {
		return rows, nil
	}
	f, err := os.Open(table.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := newNativeDeltaRecordReader(r.ctx, f, budget)
	defer reader.Close()
	for {
		raw, err := reader.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var row NativeDeltaRow
		if err := decodeNativeDeltaJSONWithBudget(raw, &row, budget, "decoded_baseline_rows"); err != nil {
			return nil, err
		}
		keyOwner := budget.Scope()
		key, err := nativeDeltaKeyAccounted(row.Key, keyOwner)
		if err != nil {
			keyOwner.Release()
			return nil, err
		}
		mapKey, err := nativeDeltaMapKeyAccounted(key, budget)
		if err != nil {
			keyOwner.Release()
			return nil, err
		}
		rows[mapKey] = row
		keyOwner.Release()
	}
	return rows, nil
}

func (r *nativeDeltaReconstructor) table(open NativeDeltaTable, schema NativeDeltaTableSchema, base deltaStagedTable, replace bool) (deltaStagedTable, NativeTable, error) {
	work := r.budget.Scope()
	defer work.Release()
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
		rows, err = r.readRows(base, work)
		if err != nil {
			return zero, normalized, err
		}
	}
	var operations uint64
	var previous []byte
	var previousOwner *NativeDeltaBudget
	for {
		raw, kind, err := r.next()
		if err != nil {
			return zero, normalized, err
		}
		if kind == "end_table" {
			var end NativeDeltaEndTable
			if err := decodeNativeDeltaJSONWithBudget(raw, &end, work); err != nil {
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
		var fields struct {
			Op string `json:"op"`
		}
		if err := json.Unmarshal(raw, &fields); err != nil {
			return zero, normalized, err
		}
		op := fields.Op
		var row NativeDeltaRow
		var key []byte
		candidate := work.Scope()
		if op == "delete" {
			var d NativeDeltaDelete
			if err := decodeNativeDeltaJSONWithBudget(raw, &d, work, "decoded_operations"); err != nil {
				return zero, normalized, err
			}
			key, err = nativeDeltaKeyAccounted(d.Key, candidate)
		} else {
			if err := decodeNativeDeltaJSONWithBudget(raw, &row, work, "decoded_operations"); err != nil {
				return zero, normalized, err
			}
			_, key, err = validateNativeDeltaRowAccounted(schema, row, r.header.ConfigPolicy, candidate)
			if err == nil {
				// This pass retains only the ordering key, not normalized fields.
				candidate.ReleaseBytes("normalized_fields", candidate.ReservedBytes("normalized_fields"))
			}
		}
		if err != nil || len(key) == 0 {
			return zero, normalized, fmt.Errorf("native delta invalid row/key: %w", err)
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return zero, normalized, fmt.Errorf("native delta operation keys unordered")
		}
		previous = key
		if previousOwner != nil {
			previousOwner.Release()
		}
		previousOwner = candidate
		mapKeyOwner := work.Scope()
		mapKey, err := nativeDeltaMapKeyAccounted(key, mapKeyOwner)
		if err != nil {
			return zero, normalized, err
		}
		old, present := rows[mapKey]
		switch op {
		case "insert":
			if present {
				return zero, normalized, fmt.Errorf("native delta insert key exists")
			}
		case "update":
			if replace || !present || nativeDeltaSameRow(old, row) {
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
			delete(rows, mapKey)
			work.ReleaseBytes("decoded_row_map_keys", uint64(len(key)))
			mapKeyOwner.Release()
		} else {
			// The typed decoder admitted this row before allocating its target.
			rows[mapKey] = row
			if present {
				work.ReleaseBytes("decoded_row_map_keys", uint64(len(key)))
			}
			mapKeyOwner.Retain()
		}
		if err := deltaAddCount(&operations, 1); err != nil {
			return zero, normalized, err
		}
	}
	previous = nil
	if previousOwner != nil {
		previousOwner.Release()
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
		candidate := r.budget.Scope()
		fields, _, err := validateNativeDeltaRowAccounted(schema, row, r.header.ConfigPolicy, candidate)
		if err != nil {
			candidate.Release()
			return zero, normalized, err
		}
		candidate.ReleaseBytes("physical_key_decode", candidate.ReservedBytes("physical_key_decode"))
		candidate.Retain()
		pk, err := NativeDeltaPrimaryKey(schema, row.Cells)
		if err != nil {
			return zero, normalized, err
		}
		if pk == nil {
			payloadSize := uint64(16 + len(row.NormalizedJSON))
			for _, cell := range row.Cells {
				if err := deltaAddCount(&payloadSize, 16+uint64(len(cell.Kind))+uint64(len(cell.Value))); err != nil {
					return zero, normalized, err
				}
			}
			if err := work.ChargeBytes("keyless_payload_preimages", payloadSize); err != nil {
				return zero, normalized, err
			}
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
		normalized.Rows = append(normalized.Rows, fields)
		row.Op = "insert"
		ordered = append(ordered, row)
	}
	if err := reserveNativeDeltaTableHash(ordered, work); err != nil {
		return zero, normalized, err
	}
	hash, err := NativeDeltaTableSHA256(schema, ordered)
	if err != nil || hash != open.ExpectedSHA256 {
		return zero, normalized, fmt.Errorf("native delta reconstructed table hash differs")
	}
	// Sorting retains one exact canonical row byte key per row, with an
	// explicit work scope, rather than allocating on every comparison.
	if err := sortNativeDeltaNormalizedRows(normalized.Rows, work); err != nil {
		return zero, normalized, err
	}
	path := base.path
	if replace || base.digest != (NativeDeltaTableDigest{Rows: uint64(len(rows)), SHA256: hash}) {
		r.nextFile++
		path = filepath.Join(r.dir, fmt.Sprintf("table-%020d.ndjson", r.nextFile))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return zero, normalized, err
		}
		enc := json.NewEncoder(f)
		for _, row := range ordered {
			output := work.Scope()
			if err = output.ChargeJSON("staged_row_encoder", row); err == nil {
				err = enc.Encode(row)
			}
			output.Release()
			if err != nil {
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
			if err := decodeNativeDeltaJSONWithBudget(raw, &drop, r.budget); err != nil {
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
		if err := decodeNativeDeltaJSONWithBudget(raw, &open, r.budget); err != nil {
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
		if err := decodeNativeDeltaJSONWithBudget(raw, &end, r.budget); err != nil {
			return zero, image, err
		}
		if end.Sequence != sequence || end.CommitID != commitID || end.TableCount != uint64(len(want)) || end.RootSHA256 != root {
			return zero, image, fmt.Errorf("native delta commit terminal differs")
		}
	} else {
		var end NativeDeltaEndCheckpoint
		if err := decodeNativeDeltaJSONWithBudget(raw, &end, r.budget); err != nil {
			return zero, image, err
		}
		if end.TableCount != uint64(len(want)) || end.RootSHA256 != root {
			return zero, image, fmt.Errorf("native delta checkpoint terminal differs")
		}
	}
	state.root = root
	if endKind == "end_commit" {
		size, err := NativeDeltaJSONSize(image)
		if err != nil {
			return zero, image, err
		}
		if err := deltaAddCount(&r.totals.FullImageBytes, size); err != nil {
			return zero, image, err
		}
		proof := NativeDeltaCommitProof{sequence, commitID, schemaID, root, digests}
		if err := r.retainedBudget.ChargeJSON("commit_proofs", proof); err != nil {
			return zero, image, err
		}
		r.proof.Commits = append(r.proof.Commits, proof)
	}
	if r.validateImages {
		if err := validateNativeTablesPolicy(image.Tables, true); err != nil {
			return zero, image, err
		}
		if endKind == "end_commit" {
			if err := validateNativeCommitSchema(image, uint64(r.root.Totals.Commits)); err != nil {
				return zero, image, err
			}
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
	if err := decodeNativeDeltaJSONWithBudget(raw, &r.header, r.budget, "header"); err != nil {
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
		if err := decodeNativeDeltaJSONWithBudget(raw, &c, r.budget, "commit_metadata"); err != nil {
			return result, err
		}
		if c.Sequence != uint64(len(r.commits))+1 || !validCommitID(c.CommitID) || c.Metadata == nil {
			return result, fmt.Errorf("native delta commit identity/order differs")
		}
		if r.seenCommits[c.CommitID] || boundaries[c.CommitID] {
			return result, fmt.Errorf("native delta duplicate/boundary commit")
		}
		if len(r.knownGraph) != 0 {
			if c.Sequence > uint64(len(r.knownGraph)) {
				return result, fmt.Errorf("native delta replay graph length differs")
			}
			expected := r.knownGraph[c.Sequence-1]
			if expected.ID != c.CommitID || !equalJSON(expected.Parents, c.Parents) {
				return result, fmt.Errorf("native delta replay ordered graph differs")
			}
		}
		parents := map[string]bool{}
		for _, p := range c.Parents {
			if !validCommitID(p) || parents[p] {
				return result, fmt.Errorf("native delta parent identity differs")
			}
			parents[p] = true
			if !r.seenCommits[p] && !boundaries[p] {
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
		work := r.budget.Scope()
		retained := r.budget
		r.budget = work
		state, image, err := r.transaction(c.SchemaID, base, snapshot, c.Sequence, c.CommitID, c.ExpectedRootSHA256, "end_commit")
		r.budget = retained
		if err != nil {
			work.Release()
			return result, err
		}
		touched := []string{}
		for name, table := range state.tables {
			old, exists := r.previousTables[name]
			if !exists || old.digest != table.digest || NativeDeltaTableSchemaID(old.schema) != NativeDeltaTableSchemaID(table.schema) {
				touched = append(touched, name)
			}
		}
		for name := range r.previousTables {
			if _, present := state.tables[name]; !present {
				touched = append(touched, name)
			}
		}
		sort.Strings(touched)
		if err := r.emitRowChanges(state, image, touched); err != nil {
			work.Release()
			return result, err
		}
		if r.rowCallbacks != nil && r.rowCallbacks.tableState != nil {
			if err := r.emitRowCheckpoint("commit", state, image); err != nil {
				work.Release()
				return result, err
			}
		}
		if r.visit != nil {
			if err := r.visit(image, touched); err != nil {
				work.Release()
				return result, err
			}
		}
		r.previousTables = state.tables
		work.Release()
		// Only first-parent table-file metadata survives a commit. No decoded
		// per-commit image or serialized image file is retained.
		metadata := map[string]any{}
		for name, table := range state.tables {
			metadata[name] = struct {
				Path   string
				Digest NativeDeltaTableDigest
			}{table.path, table.digest}
		}
		metadataBytes, err := NativeDeltaJSONSize(metadata)
		if err != nil {
			return result, err
		}
		if err := r.budget.ChargeBytes("staged_state_metadata", metadataBytes); err != nil {
			return result, err
		}
		r.stagedBytes[c.CommitID] = metadataBytes
		r.states[c.CommitID] = state
		r.seenCommits[c.CommitID] = true
		r.commits = append(r.commits, SourceCommit{c.CommitID, c.Parents, c.Metadata})
		if err := deltaAddCount(&r.totals.Commits, 1); err != nil {
			return result, err
		}
		if err := r.releaseGraphFrontier(c.Sequence); err != nil {
			return result, err
		}
	}
	head, ok := r.states[h.Head]
	if !ok {
		return result, fmt.Errorf("native delta HEAD missing")
	}
	var current, headImage NativeHistoryImage
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
		if err := decodeNativeDeltaJSONWithBudget(raw, &c, r.budget); err != nil {
			return result, err
		}
		if c.CommitID != h.Head {
			return result, fmt.Errorf("native delta checkpoint HEAD differs")
		}

		work := r.budget.Scope()
		defer work.Release()
		retained := r.budget
		r.budget = work
		state, image, err := r.transaction(c.SchemaID, deltaStagedState{tables: map[string]deltaStagedTable{}}, true, 0, h.Head, c.ExpectedRootSHA256, "end_"+checkpoint)
		r.budget = retained
		if err != nil {
			return result, err
		}
		if err := r.emitRowCheckpoint(checkpoint, state, image); err != nil {
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

			headImage = image
			for i, commit := range r.commits {
				if commit.ID == h.Head {
					headImage.Sequence = uint64(i + 1)
					break
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
	if err := decodeNativeDeltaJSONWithBudget(raw, &end, r.budget); err != nil {
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
	result.Images = append(result.Images, headImage)
	result.deltaImageCount = uint64(len(r.commits))
	r.proof.Totals = r.totals
	return result, nil
}

// releaseGraphFrontier drops a state after its final authenticated first-parent
// use. The immediately previous emitted state remains a row-diff oracle until
// the next emission. Table files can be shared by several retained states.
func (r *nativeDeltaReconstructor) releaseGraphFrontier(sequence uint64) error {
	if r.lastUse == nil {
		return nil
	}
	for id := range r.states {
		if r.lastUse[id] <= sequence {
			delete(r.states, id)
			r.budget.ReleaseBytes("staged_state_metadata", r.stagedBytes[id])
			delete(r.stagedBytes, id)
		}
	}
	live := map[string]bool{}
	for _, state := range r.states {
		for _, table := range state.tables {
			if table.path != "" {
				live[table.path] = true
			}
		}
	}
	for _, table := range r.previousTables {
		if table.path != "" {
			live[table.path] = true
		}
	}
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(r.dir, entry.Name())
		if !live[path] {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}
