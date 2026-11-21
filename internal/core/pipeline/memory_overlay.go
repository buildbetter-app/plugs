package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/recordimage"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// MemoryOverlay belongs to a separate core memory row. Source identity and
// bytes are pinned by the Writer, never supplied as request metadata. A logical
// generation changes only on re-remember after forget, delimiting history even
// though the durable tombstone must remain to suppress the immutable source.
type MemoryOverlay = recordimage.MemoryOverlay

func MemoryOverlayRef(source model.Ref) model.Ref {
	digest := sha256.Sum256([]byte(source.ID))
	return model.RefByID("memory-overlay/"+hex.EncodeToString(digest[:]), model.Natural64(0))
}

func memorySourceHash(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func validateMemoryOverlay(record driver.Record, raw map[string]any, document MemoryDocument) error {
	_, present := raw["sourceOverlay"]
	if !present {
		if strings.HasPrefix(record.Ref.ID, "memory-overlay/") {
			return fmt.Errorf("missing memory overlay binding")
		}
		return nil
	}
	overlay := document.Overlay
	fields, ok := raw["sourceOverlay"].(map[string]any)
	content, contentOK := raw["content"].(string)
	if !ok || !contentOK || content != document.Content || overlay == nil || len(fields) != 7 || overlay.Source.Validate() != nil || !overlay.Source.IsID() ||
		!overlay.Source.Branch.IsZero() || overlay.SourceRevision.Validate() != nil || overlay.SourceRevision.IsZero() ||
		overlay.Generation == 0 || overlay.Time.IsZero() || overlay.Principal.Validate() != nil || MemoryOverlayRef(overlay.Source).ID != record.Ref.ID ||
		len(overlay.SourceSHA256) != 64 || strings.HasPrefix(overlay.Source.ID, "memory-overlay/") ||
		!record.Ref.Branch.IsZero() || overlay.Deleted && (document.Content != "" || len(document.Metadata) != 0) || !overlay.Deleted && strings.TrimSpace(document.Content) == "" {
		return fmt.Errorf("invalid memory overlay")
	}
	for _, name := range []string{"source", "sourceRevision", "sourceSha256", "principal", "time", "generation", "deleted"} {
		if _, present := fields[name]; !present {
			return fmt.Errorf("missing memory overlay field")
		}
	}
	if _, err := hex.DecodeString(overlay.SourceSHA256); err != nil {
		return fmt.Errorf("invalid memory overlay digest")
	}
	for key := range raw {
		if key != "id" && key != "key" && key != "content" && key != "metadata" && key != "sourceOverlay" {
			return fmt.Errorf("invalid memory overlay field")
		}
	}
	return nil
}

// Legacy public mirrors have a canonical body but an exact reserved source
// identity. Arbitrary request metadata on an ordinary memory grants no source
// authority. The same-snapshot import binding is checked below for both forms.
func legacyMemorySource(record driver.Record, document MemoryDocument) (*MemorySource, bool, error) {
	if !strings.HasPrefix(record.Ref.ID, "source/memory/") {
		return nil, false, nil
	}
	id, idOK := document.Metadata["plug.restore.source_id"].(string)
	table, tableOK := document.Metadata["plug.restore.source_table"].(string)
	fields, fieldsOK := document.Metadata["plug.restore.source"].(map[string]any)
	archive, archiveOK := document.Metadata["plug.restore.archive_sha256"].(string)
	if document.Overlay != nil || !idOK || id == "" || !tableOK || table != "config" || !fieldsOK || !archiveOK ||
		len(archive) != 64 || len(document.Metadata) != 4 || len(fields) != 2 || !record.Ref.Branch.IsZero() ||
		record.Ref.ID != "source/memory/"+id || document.Key != strings.TrimSpace(id) ||
		fields["key"] != "kv.memory."+id || fields["value"] != document.Content {
		return nil, true, fmt.Errorf("invalid legacy source memory")
	}
	return &MemorySource{Legacy: true, Visible: true, Fields: fields, Record: record, Archive: archive}, true, nil
}

// Both ordinary Readers and the Writer's tracked snapshot implement this
// narrow view. Source, import-binding and overlay reads keep the commit guards.
type memoryRecordReader interface {
	Snapshot() driver.View
	Get(context.Context, []model.Ref) ([]driver.Record, error)
}

func bindMemorySource(ctx context.Context, reader memoryRecordReader, record driver.Record, source *MemorySource) error {
	if source == nil {
		return fmt.Errorf("memory overlay requires an imported source")
	}
	rows, err := reader.Get(ctx, []model.Ref{model.RefByID("instance_config/restore-import/binding", model.Natural64(0)), model.RefByID(driver.SourcePlaneCommitRecordID, model.Natural64(0))})
	if err != nil {
		return err
	}
	var binding struct {
		Profile     driver.RestoreImportProfile `json:"profile"`
		RequestHash string                      `json:"request_hash"`
	}
	var bindingRows, commitRows []driver.Record
	for _, row := range rows {
		switch row.Ref.ID {
		case "instance_config/restore-import/binding":
			bindingRows = append(bindingRows, row)
		case driver.SourcePlaneCommitRecordID:
			commitRows = append(commitRows, row)
		}
	}
	if len(bindingRows) != 1 || bindingRows[0].Kind != "instance_config" || json.Unmarshal(bindingRows[0].Body, &binding) != nil {
		return fmt.Errorf("invalid source memory import binding")
	}
	request, requestErr := binding.Profile.RequestHash()
	if binding.Profile.SourcePlanes != nil {
		if len(commitRows) != 1 || commitRows[0].Kind != "instance_config" {
			return fmt.Errorf("invalid source memory import binding")
		}
		var commitBinding driver.SourcePlaneCommitBindingV1
		if json.Unmarshal(commitRows[0].Body, &commitBinding) != nil ||
			commitBinding.Version != driver.SourcePlaneCommitVersionV1 ||
			commitBinding.JointPlanDigest != binding.Profile.SourcePlanes.JointPlanDigest ||
			commitBinding.RequestHash != request || commitBinding.StoreID != binding.Profile.SourceStoreID ||
			commitBinding.Revision.IsZero() ||
			commitBinding.Incarnation != reader.Snapshot().Incarnation {
			return fmt.Errorf("historical source memory binding")
		}
	}
	if requestErr != nil || request != binding.RequestHash || source.Archive != "" && source.Archive != binding.Profile.ArchiveDigest ||
		source.Legacy != (binding.Profile.SourcePlanes == nil) {
		return fmt.Errorf("invalid source memory import binding")
	}
	source.Record, source.Archive = record, binding.Profile.ArchiveDigest
	return nil
}

// ResolveMemoryRecord returns the effective source/overlay image on one reader.
// Canonical memories pass through. Hidden source categories stay hidden and
// cannot be reclassified by an overlay, including a forged persisted binding.
func ResolveMemoryRecord(ctx context.Context, reader memoryRecordReader, record driver.Record) (driver.Record, MemoryDocument, *MemorySource, error) {
	document, source, err := DecodeMemoryRecord(record)
	if err != nil {
		return driver.Record{}, MemoryDocument{}, nil, err
	}
	if document.Overlay != nil {
		rows, err := reader.Get(ctx, []model.Ref{document.Overlay.Source})
		if err != nil {
			return driver.Record{}, MemoryDocument{}, nil, err
		}
		if len(rows) != 1 {
			return driver.Record{}, MemoryDocument{}, nil, fmt.Errorf("missing overlay source")
		}
		sourceDocument, original, err := DecodeMemoryRecord(rows[0])
		if err != nil || original == nil || !original.Visible || sourceDocument.Key != document.Key ||
			rows[0].Revision.String() != document.Overlay.SourceRevision.String() || memorySourceHash(rows[0].Body) != document.Overlay.SourceSHA256 {
			return driver.Record{}, MemoryDocument{}, nil, fmt.Errorf("invalid overlay source binding")
		}
		if err := bindMemorySource(ctx, reader, rows[0], original); err != nil {
			return driver.Record{}, MemoryDocument{}, nil, err
		}
		return record, document, original, nil
	}
	if source == nil || !source.Visible {
		return record, document, source, nil
	}
	if err := bindMemorySource(ctx, reader, record, source); err != nil {
		return driver.Record{}, MemoryDocument{}, nil, err
	}
	rows, err := reader.Get(ctx, []model.Ref{MemoryOverlayRef(record.Ref)})
	if err != nil {
		return driver.Record{}, MemoryDocument{}, nil, err
	}
	if len(rows) == 0 {
		return record, document, source, nil
	}
	if len(rows) != 1 {
		return driver.Record{}, MemoryDocument{}, nil, fmt.Errorf("ambiguous memory overlay")
	}
	effective, replacement, bound, err := ResolveMemoryRecord(ctx, reader, rows[0])
	if err != nil || bound == nil || bound.Record.Ref.ID != record.Ref.ID || !bytes.Equal(bound.Record.Body, record.Body) {
		return driver.Record{}, MemoryDocument{}, nil, fmt.Errorf("invalid memory overlay binding")
	}
	return effective, replacement, bound, nil
}

// A replay rechecks authority over both the immutable slot and its current
// overlay, including tombstones. Stored result bodies alone are not authority
// for the source, and a delete result deliberately contains no body.
func memoryReplayGuards(ctx context.Context, reader memoryRecordReader, ref model.Ref, method string) ([]port.Change, error) {
	rows, err := reader.Get(ctx, []model.Ref{ref})
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, failure("internal_error", "ambiguous persisted memory")
	}
	effective, _, source, err := ResolveMemoryRecord(ctx, reader, rows[0])
	if err != nil {
		return nil, failure("internal_error", "invalid persisted memory")
	}
	if source == nil {
		return nil, nil
	}
	var guards []port.Change
	for _, row := range []driver.Record{source.Record, effective} {
		if row.Ref.ID == "" || row.Ref.ID == ref.ID {
			continue
		}
		op := model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: row.Ref, Fields: map[string]any{}}}
		if method == "memory.delete" {
			op = model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: row.Ref}}
		}
		guards = append(guards, port.Change{Resource: model.ResourceRef{Kind: "memory", Ref: row.Ref}, Op: op, Before: row.Body, After: row.Body})
	}
	return guards, nil
}

func (w *Writer) prepareMemoryOverlay(ctx context.Context, p *prepared, by model.PrincipalRef, source *MemorySource, effective driver.Record, document MemoryDocument, content string, metadata map[string]any, deleted bool) (*pending, model.Op, error) {
	if !source.Visible {
		return nil, model.Op{}, failure("invalid_params", "hidden source memory cannot be replaced")
	}
	for key := range metadata {
		if strings.HasPrefix(key, "plug.restore.") {
			return nil, model.Op{}, failure("invalid_params", "source provenance metadata is immutable")
		}
	}
	if len(metadata) == 0 {
		metadata = nil
	}
	if len(document.Metadata) == 0 {
		document.Metadata = nil
	}
	ref := MemoryOverlayRef(source.Record.Ref)
	rows, err := p.snapshot.Get(ctx, []model.Ref{ref})
	if err != nil {
		return nil, model.Op{}, err
	}
	var before json.RawMessage
	var revision model.Revision
	if len(rows) == 1 {
		if document.Overlay == nil || effective.Ref.ID != ref.ID {
			return nil, model.Op{}, failure("internal_error", "invalid memory overlay")
		}
		before, revision = append(json.RawMessage(nil), effective.Body...), effective.Revision
	} else if len(rows) != 0 {
		return nil, model.Op{}, failure("internal_error", "ambiguous memory overlay")
	}
	// Saving exactly the source value without user metadata is a genuine no-op.
	// Do not create a timestamped overlay or rewrite the retained envelope.
	if document.Overlay == nil && !deleted && document.Content == content && len(metadata) == 0 {
		sourceGuard := &pending{ref: source.Record.Ref, kind: "memory", memory: &document, rawMemory: append(json.RawMessage(nil), source.Record.Body...), before: append(json.RawMessage(nil), source.Record.Body...), revision: source.Record.Revision, setBy: map[string]string{}}
		return sourceGuard, model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: sourceGuard.ref, Fields: map[string]any{"content": content}}}, nil
	}
	if document.Overlay == nil {
		document = MemoryDocument{ID: ref.ID, Key: document.Key, Overlay: &MemoryOverlay{Source: source.Record.Ref, SourceRevision: source.Record.Revision, SourceSHA256: memorySourceHash(source.Record.Body), Generation: 1}}
	} else {
		copy := *document.Overlay
		document.Overlay = &copy
		if copy.Deleted && !deleted {
			if copy.Generation == math.MaxUint64 {
				return nil, model.Op{}, failure("write_conflict", "memory generation is exhausted")
			}
			document.Overlay.Generation++
		}
	}
	oldComparable, _ := json.Marshal(struct {
		Content  string
		Metadata map[string]any
		Deleted  bool
	}{document.Content, document.Metadata, document.Overlay.Deleted})
	newComparable, err := json.Marshal(struct {
		Content  string
		Metadata map[string]any
		Deleted  bool
	}{content, metadata, deleted})
	if err != nil {
		return nil, model.Op{}, err
	}
	if len(before) == 0 || !bytes.Equal(oldComparable, newComparable) {
		document.Overlay.Principal, document.Overlay.Time = by, w.cfg.now().UTC()
	}
	document.Content, document.Metadata, document.Overlay.Deleted = content, cloneAnyMap(metadata), deleted
	result := &pending{ref: ref, kind: "memory", memory: &document, before: before, revision: revision, setBy: map[string]string{"content": "request", "metadata": "request", "sourceOverlay": "core"}}
	// Require the ordinary AuthorizeBefore chain to cover the original slot as
	// well as the overlay. This guard has no changed body, result or version.
	sourceGuard := &pending{ref: source.Record.Ref, kind: "memory", rawMemory: append(json.RawMessage(nil), source.Record.Body...), before: append(json.RawMessage(nil), source.Record.Body...), revision: source.Record.Revision, setBy: map[string]string{}}
	p.writes[sourceGuard.ref.ID] = sourceGuard
	gate := model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: sourceGuard.ref, Fields: map[string]any{"content": content}}}
	if deleted {
		gate = model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: sourceGuard.ref}}
	}
	p.proposal.Changes = append(p.proposal.Changes, port.Change{Resource: model.ResourceRef{Kind: "memory", Ref: sourceGuard.ref}, Op: gate, Before: sourceGuard.before})
	operation := model.Op{Kind: model.OpPatch, Patch: &model.PatchOp{Kind: model.OpPatch, Target: ref, Fields: map[string]any{"content": content, "metadata": metadata}}}
	if len(before) == 0 {
		operation = model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "memory", Fields: map[string]any{"key": document.Key, "content": content, "metadata": metadata}}}
	}
	if deleted {
		operation = model.Op{Kind: model.OpDelete, Delete: &model.DeleteOp{Kind: model.OpDelete, Target: ref}}
	}
	return result, operation, nil
}
