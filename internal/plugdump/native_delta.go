package plugdump

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
)

type NativeDeltaSource interface {
	Source
	ReadSourceCommits(context.Context) (SourceCommitLog, error)
	ReadSourceIgnore(context.Context) (Table, error)
	ReadNativeDeltaImage(context.Context, string) (physicalcapture.Image, error)
}

// PreparedNativeDelta has no published root. The caller closes all source
// resources, writes the bounded head/evidence, then publishes SetBytes LAST.
// Abort removes only paths exclusively created by this preparation.
type PreparedNativeDelta struct {
	Set                               NativeCaptureSet
	SetBytes                          []byte
	HeadArchive                       []byte
	Manifest                          Manifest
	HeadRootSHA256, CurrentRootSHA256 string
	HistoryCoverage                   *NativeHistoryCoverage
	ignore                            NativeTable
	partsPath                         string
}

func (p *PreparedNativeDelta) Abort() error { return os.RemoveAll(p.partsPath) }

func (p *PreparedNativeDelta) VerifyIgnore(ctx context.Context, src NativeDeltaSource) error {
	observed, err := src.ReadSourceIgnore(ctx)
	if err != nil {
		return err
	}
	value, err := nativeTable(observed, "dolt_ignore", nil)
	if err != nil {
		return err
	}
	if !equalJSON(p.ignore, value) {
		return ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
	}
	return nil
}

type nativeDeltaEmitter struct {
	parts   *nativeDeltaParts
	schemas map[string]bool
	totals  NativeDeltaTotals
}

func nativeDeltaAdd(value *uint64, next uint64) error {
	if ^uint64(0)-*value < next {
		return ErrSourceHistoryUnavailable{Reason: "image-census-limit"}
	}
	*value += next
	return nil
}

func (e *nativeDeltaEmitter) schema(state *nativeDeltaState) error {
	if e.schemas[state.Schema.SchemaID] {
		return nil
	}
	if err := e.parts.record(state.Schema); err != nil {
		return err
	}
	e.schemas[state.Schema.SchemaID] = true
	return nativeDeltaAdd(&e.totals.Schemas, 1)
}

func (e *nativeDeltaEmitter) tables(state, base *nativeDeltaState) error {
	definitions := map[string]NativeDeltaTableSchema{}
	for _, t := range state.Schema.Tables {
		definitions[t.Name] = t
	}
	previous := map[string]NativeDeltaTableSchema{}
	if base != nil {
		for _, t := range base.Schema.Tables {
			previous[t.Name] = t
		}
	}
	names := map[string]bool{}
	for name := range definitions {
		names[name] = true
	}
	for name := range previous {
		names[name] = true
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		definition, present := definitions[name]
		if !present {
			if err := e.parts.record(NativeDeltaDropTable{Type: "drop_table", Name: name, PreviousTableSchemaID: NativeDeltaTableSchemaID(previous[name])}); err != nil {
				return err
			}
			continue
		}
		mode := "replace"
		old := []NativeDeltaRow{}
		if previousDefinition, exists := previous[name]; exists && NativeDeltaTableSchemaID(previousDefinition) == NativeDeltaTableSchemaID(definition) {
			mode = "delta"
			old = base.Tables[name]
		}
		digest := state.Digests[name]
		if err := e.parts.record(NativeDeltaTable{Type: "table", Name: name, TableSchemaID: NativeDeltaTableSchemaID(definition), Mode: mode, ExpectedRows: digest.Rows, ExpectedSHA256: digest.SHA256}); err != nil {
			return err
		}
		if err := nativeDeltaAdd(&e.totals.TableTransactions, 1); err != nil {
			return err
		}
		operations, err := e.rows(old, state.Tables[name])
		if err != nil {
			return err
		}
		if err := nativeDeltaAdd(&e.totals.RowOperations, operations); err != nil {
			return err
		}
		if err := nativeDeltaAdd(&e.totals.ReconstructedRows, digest.Rows); err != nil {
			return err
		}
		if digest.Rows > 0 && uint64(len(definition.Columns)) > ^uint64(0)/digest.Rows {
			return ErrSourceHistoryUnavailable{Reason: "image-census-limit"}
		}
		if err := nativeDeltaAdd(&e.totals.ReconstructedCells, digest.Rows*uint64(len(definition.Columns))); err != nil {
			return err
		}
		if err := e.parts.record(NativeDeltaEndTable{Type: "end_table", RowOperations: operations, Rows: digest.Rows, SHA256: digest.SHA256}); err != nil {
			return err
		}
	}
	return nil
}

func (e *nativeDeltaEmitter) rows(old, current []NativeDeltaRow) (uint64, error) {
	var operations uint64
	i, j := 0, 0
	for i < len(old) || j < len(current) {
		comparison := 0
		if i == len(old) {
			comparison = 1
		} else if j == len(current) {
			comparison = -1
		} else {
			a, _ := base64.StdEncoding.DecodeString(old[i].Key)
			b, _ := base64.StdEncoding.DecodeString(current[j].Key)
			comparison = bytes.Compare(a, b)
		}
		var record any
		switch {
		case comparison < 0:
			record = NativeDeltaDelete{Type: "row", Op: "delete", Key: old[i].Key}
			i++
		case comparison > 0:
			row := current[j]
			row.Op = "insert"
			record = row
			j++
		default:
			if old[i].NormalizedJSON != current[j].NormalizedJSON || !equalJSON(old[i].Cells, current[j].Cells) {
				row := current[j]
				row.Op = "update"
				record = row
			}
			i++
			j++
		}
		if record != nil {
			if err := e.parts.record(record); err != nil {
				return 0, err
			}
			operations++
		}
	}
	return operations, nil
}

func saveNativeDeltaState(directory, id string, state *nativeDeltaState) (resultErr error) {
	if !validCommitID(id) {
		return fmt.Errorf("invalid native cache identity")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	file, err := root.OpenFile(id+".json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	return json.NewEncoder(file).Encode(state)
}

func loadNativeDeltaState(directory, id string) (state *nativeDeltaState, resultErr error) {
	if !validCommitID(id) {
		return nil, fmt.Errorf("invalid native cache identity")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	file, err := root.Open(id + ".json")
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var result nativeDeltaState
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// PrepareNativeDelta captures a DAG with only one current/first-parent image
// in memory and a caller-owned on-disk cache. It never commits/stages the source.
func PrepareNativeDelta(ctx context.Context, src NativeDeltaSource, inventory Inventory, working physicalcapture.WorkingImage, log SourceCommitLog, output string) (result *PreparedNativeDelta, resultErr error) {
	var atOrdinal uint64
	var atSchema *uint64
	defer func() {
		if resultErr != nil {
			resultErr = historyFailureCoordinates(resultErr, atOrdinal, atSchema)
		}
	}()
	if !working.ReadOnlyScope || !working.Complete || working.HeadCommitID != log.Head {
		return nil, ErrSourceHistoryUnavailable{Reason: "unauthenticated-history"}
	}
	if !log.Clean {
		return nil, ErrSourceHistoryUnavailable{Reason: "working-set-not-clean"}
	}
	commits, err := historyCommitOrder(log)
	if err != nil {
		return nil, err
	}
	if len(log.OrderedParents) < len(commits) {
		return nil, ErrSourceHistoryUnavailable{Reason: "contradictory-physical-ancestry"}
	}
	scratch, err := os.MkdirTemp(output, ".native-delta-work-*")
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(scratch)) }()
	partsPath := filepath.Join(output, "parts")
	parts, err := newNativeDeltaParts(partsPath, scratch, NativeDeltaPartBytes)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, parts.Close())
		if resultErr != nil {
			result = nil
			resultErr = errors.Join(resultErr, os.RemoveAll(partsPath))
		}
	}()
	current, err := nativeDeltaPhysicalState(working.Tables, inventory.ConfigPolicy)
	if err != nil {
		return nil, err
	}
	atOrdinal = 1
	headImage, err := src.ReadNativeDeltaImage(ctx, log.Head)
	if err != nil {
		return nil, locatedCommitFailure(err, 1)
	}
	if !headImage.ReadOnlyScope || headImage.CommitID != log.Head {
		return nil, locatedCommitFailure(ErrSourceHistoryUnavailable{Reason: "unauthenticated-history"}, 1)
	}
	head, err := nativeDeltaPhysicalState(headImage.Tables, inventory.ConfigPolicy)
	if err != nil {
		return nil, locatedCommitFailure(err, 1)
	}
	atSchema = head.Schema.Profile.MainVersion
	ignoreTable, err := src.ReadSourceIgnore(ctx)
	if err != nil {
		return nil, err
	}
	ignore, err := nativeTable(ignoreTable, "dolt_ignore", nil)
	if err != nil {
		return nil, err
	}
	if physicalIgnore, present := current.Native["dolt_ignore"]; present && !equalJSON(physicalIgnore, ignore) {
		return nil, ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
	}
	for name, value := range current.Native {
		if _, declared := tableColumns[name]; !declared {
			continue
		}
		classification, err := nativeFamilyHistory(name, head.Native, ignore)
		if err != nil {
			return nil, err
		}
		if classification == FamilyVersioned && !equalJSON(value, head.Native[name]) {
			return nil, ErrSourceHistoryUnavailable{Reason: "head-image-differs-versioned-family:" + name, OrdinalFromHead: 1, SchemaVersion: head.Schema.Profile.MainVersion}
		}
	}
	archive, manifest, coverage, err := buildNativeDeltaHead(ctx, src, inventory, log, current.Native, head.Native, ignore)
	if err != nil {
		return nil, err
	}
	projects := []string{}
	for _, project := range inventory.Projects {
		projects = append(projects, project.Prefix)
	}
	sort.Strings(projects)
	if err := parts.record(NativeDeltaHeader{Type: "header", Format: NativeDeltaFormat, StoreID: inventory.StoreID, Projects: projects, Head: log.Head, CommitCount: uint64(len(commits)), HistoryCoverage: coverage, Ignore: ignore, ConfigPolicy: inventory.ConfigPolicy}); err != nil {
		return nil, err
	}
	emitter := nativeDeltaEmitter{parts: parts, schemas: map[string]bool{}}
	retained := map[string]bool{}
	for i, c := range commits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordinal := historyCommitOrdinal(log, c.ID)
		atOrdinal, atSchema = ordinal, nil
		var state *nativeDeltaState
		if c.ID == log.Head {
			state = head
		} else {
			image, err := src.ReadNativeDeltaImage(ctx, c.ID)
			if err != nil {
				return nil, locatedCommitFailure(err, ordinal)
			}
			if !image.ReadOnlyScope || image.CommitID != c.ID {
				return nil, locatedCommitFailure(ErrSourceHistoryUnavailable{Reason: "unauthenticated-history"}, ordinal)
			}
			state, err = nativeDeltaPhysicalState(image.Tables, inventory.ConfigPolicy)
			if err != nil {
				return nil, locatedCommitFailure(err, ordinal)
			}
		}
		atSchema = state.Schema.Profile.MainVersion
		if err := emitter.schema(state); err != nil {
			return nil, &NativeCommitFailure{SchemaVersion: state.Schema.Profile.MainVersion, OrdinalFromHead: ordinal, Cause: err}
		}
		parents, exists := log.OrderedParents[c.ID]
		if !exists {
			return nil, locatedCommitFailure(ErrSourceHistoryUnavailable{Reason: "contradictory-physical-ancestry"}, ordinal)
		}
		orderedSet := append([]string{}, parents...)
		sort.Strings(orderedSet)
		legacySet := append([]string{}, c.Parents...)
		sort.Strings(legacySet)
		if !equalJSON(orderedSet, legacySet) {
			return nil, locatedCommitFailure(ErrSourceHistoryUnavailable{Reason: "contradictory-physical-ancestry"}, ordinal)
		}
		var first, baseID *string
		var base *nativeDeltaState
		mode := "snapshot"
		if len(parents) > 0 {
			value := parents[0]
			first = &value
			if retained[value] {
				baseID = &value
				mode = "delta"
				base, err = loadNativeDeltaState(scratch, value)
				if err != nil {
					return nil, err
				}
			}
		}
		record := NativeDeltaCommit{Type: "commit", Sequence: uint64(i + 1), CommitID: c.ID, Parents: parents, FirstParent: first, Metadata: c.Metadata, SchemaID: state.Schema.SchemaID, Mode: mode, BaseCommit: baseID, ExpectedRootSHA256: state.Root}
		if err := parts.record(record); err != nil {
			return nil, &NativeCommitFailure{SchemaVersion: state.Schema.Profile.MainVersion, OrdinalFromHead: ordinal, Cause: err}
		}
		if err := emitter.tables(state, base); err != nil {
			return nil, &NativeCommitFailure{SchemaVersion: state.Schema.Profile.MainVersion, OrdinalFromHead: ordinal, Cause: err}
		}
		if err := parts.record(NativeDeltaEndCommit{Type: "end_commit", Sequence: uint64(i + 1), CommitID: c.ID, TableCount: uint64(len(state.Schema.Tables)), RootSHA256: state.Root}); err != nil {
			return nil, err
		}
		frame, err := marshal(NativeHistoryImage{SchemaProfile: &state.Schema.Profile, Sequence: uint64(i + 1), CommitID: c.ID, Tables: state.Native})
		if err != nil {
			return nil, err
		}
		if err := nativeDeltaAdd(&emitter.totals.FullImageBytes, uint64(len(frame))); err != nil {
			return nil, &NativeCommitFailure{SchemaVersion: state.Schema.Profile.MainVersion, OrdinalFromHead: ordinal, Cause: err}
		}
		if err := nativeDeltaAdd(&emitter.totals.Commits, 1); err != nil {
			return nil, err
		}
		if err := saveNativeDeltaState(scratch, c.ID, state); err != nil {
			return nil, err
		}
		retained[c.ID] = true
	}
	// A fresh independent physical HEAD read checks the delta-reconstructed HEAD.
	atOrdinal, atSchema = 1, head.Schema.Profile.MainVersion
	checkpointImage, err := src.ReadNativeDeltaImage(ctx, log.Head)
	if err != nil {
		return nil, locatedCommitFailure(err, 1)
	}
	checkpoint, err := nativeDeltaPhysicalState(checkpointImage.Tables, inventory.ConfigPolicy)
	if err != nil {
		return nil, locatedCommitFailure(err, 1)
	}
	if !checkpointImage.ReadOnlyScope || checkpointImage.CommitID != log.Head || checkpoint.Root != head.Root || checkpoint.Schema.SchemaID != head.Schema.SchemaID || !equalJSON(checkpoint.Tables, head.Tables) {
		return nil, locatedCommitFailure(ErrSourceHistoryUnavailable{Reason: "source-census-changed"}, 1)
	}
	for _, entry := range []struct {
		kind  string
		state *nativeDeltaState
	}{{"head-checkpoint", checkpoint}, {"current", current}} {
		atSchema = entry.state.Schema.Profile.MainVersion
		if entry.kind == "current" {
			atOrdinal = 0
		}
		if err := emitter.schema(entry.state); err != nil {
			return nil, err
		}
		if err := parts.record(NativeDeltaCheckpoint{Type: entry.kind, CommitID: log.Head, SchemaID: entry.state.Schema.SchemaID, ExpectedRootSHA256: entry.state.Root}); err != nil {
			return nil, err
		}
		if err := emitter.tables(entry.state, nil); err != nil {
			return nil, err
		}
		if err := parts.record(NativeDeltaEndCheckpoint{Type: "end_" + entry.kind, TableCount: uint64(len(entry.state.Schema.Tables)), RootSHA256: entry.state.Root}); err != nil {
			return nil, err
		}
	}
	if err := parts.record(NativeDeltaEnd{Type: "end", Totals: emitter.totals, HeadRootSHA256: head.Root, CurrentRootSHA256: current.Root}); err != nil {
		return nil, err
	}
	count, finalHash, streamHash, streamBytes, err := parts.finish()
	if err != nil {
		return nil, err
	}
	set := NativeCaptureSet{Format: NativeCaptureSetFormat, Contract: NativeDeltaFormat, StoreID: inventory.StoreID, Projects: projects, Head: log.Head, HeadArchive: File{SHA256: digest(archive), Size: int64(len(archive))}, PartCount: count, FinalPartSHA256: finalHash, StreamSHA256: streamHash, StreamBytes: streamBytes, Totals: emitter.totals}
	setBytes, err := marshal(set)
	if err != nil {
		return nil, err
	}
	if len(setBytes) > NativeDeltaRootBytes {
		return nil, fmt.Errorf("native set root limit")
	}
	return &PreparedNativeDelta{Set: set, SetBytes: setBytes, HeadArchive: archive, Manifest: manifest, HeadRootSHA256: head.Root, CurrentRootSHA256: current.Root, HistoryCoverage: coverage, ignore: ignore, partsPath: partsPath}, nil
}
