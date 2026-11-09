package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReverseRefusal identifies a live image outside the bounded imported-source
// profile. A refusal never contains source values or a partial JSONL result.
type ReverseRefusal struct{ Reason string }

func (e ReverseRefusal) Error() string { return "live reverse refused: " + e.Reason }

type LiveReverse struct {
	Snapshot driver.View
	Result   reverseconverter.Result
}

// ReverseRestored reads one immutable store snapshot, including its retained
// source evidence. It exports the live title, description, priority, status,
// labels, metadata and opaque fields of imported beads. Unknown/new/deleted
// records, active claims, attachments and changed source relationships or
// comments refuse rather than silently exporting the old archive instead.
// This is an offline trusted-host seam, not an authorization or RPC surface.
func ReverseRestored(ctx context.Context, store driver.Store) (LiveReverse, error) {
	return ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{})
}

// ReverseRestoredWithOptions selects the pinned default export or explicit
// all-planes export. Imported joint images use one core/namespace snapshot.
func ReverseRestoredWithOptions(ctx context.Context, store driver.Store, options reverseconverter.Options) (LiveReverse, error) {
	if store == nil {
		return LiveReverse{}, ReverseRefusal{"missing_store"}
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		return LiveReverse{}, err
	}
	capability, ok := store.(driver.CheckpointInventoryStore)
	if !ok {
		return LiveReverse{}, ReverseRefusal{"checkpoint_inventory_unavailable"}
	}
	r, err := capability.ReadCheckpointInventory(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return LiveReverse{}, err
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(importBindingRecordID, model.Natural64(0))})
	if err != nil {
		return LiveReverse{}, err
	}
	if len(rows) != 0 {
		var binding importBindingEvidence
		if len(rows) != 1 || rows[0].Kind != "instance_config" || json.Unmarshal(rows[0].Body, &binding) != nil {
			return LiveReverse{}, ReverseRefusal{"invalid_import_binding"}
		}
		if binding.Profile.SourcePlanes != nil {
			return reverseSourceRestored(ctx, r, meta, binding.Profile, options)
		}
	}
	dump, err := retainedReverseDump(ctx, r)
	if err != nil {
		return LiveReverse{}, err
	}
	live, err := reverseLiveCensus(ctx, r)
	if err != nil {
		return LiveReverse{}, err
	}
	coreLosses, namespaceLosses, err := reverseLossCensus(r, live, nil, options)
	if err != nil {
		return LiveReverse{}, err
	}
	if err := projectLiveDump(ctx, r, &dump, live); err != nil {
		return LiveReverse{}, err
	}
	converted, err := reverseconverter.ConvertWithOptions(dump, options)
	attachReverseLossCensus(&converted, err, coreLosses, namespaceLosses)
	if err != nil {
		return LiveReverse{Snapshot: r.Snapshot(), Result: converted}, err
	}
	return LiveReverse{Snapshot: r.Snapshot(), Result: converted}, nil
}

func reverseLiveCensus(ctx context.Context, r driver.Reader) (map[string]driver.Record, error) {
	live := map[string]driver.Record{}
	var cursor *driver.Cursor
	totalBytes := 0
	for {
		page, err := r.Query(ctx, driver.Query{Cursor: cursor, Limit: 1000})
		if err != nil {
			return nil, err
		}
		for _, row := range page.Items {
			totalBytes += len(row.Body)
			if !row.Ref.IsID() || !row.Ref.Branch.IsZero() || row.Ref.ID == "" || live[row.Ref.ID].Kind != "" || totalBytes > dumpformat.MaxArchiveBytes {
				return nil, ReverseRefusal{"unsupported_live_census"}
			}
			live[row.Ref.ID] = row
		}
		if page.NextCursor == nil {
			break
		}
		if reflect.DeepEqual(cursor, page.NextCursor) {
			return nil, ReverseRefusal{"invalid_live_cursor"}
		}
		cursor = page.NextCursor
	}
	return live, nil
}

func retainedReverseDump(ctx context.Context, r driver.Reader) (reverseconverter.Dump, error) {
	rows, err := r.Get(ctx, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
	if err != nil {
		return reverseconverter.Dump{}, err
	}
	var evidence archiveEvidence
	if len(rows) != 1 || rows[0].Kind != "restore-archive" || json.Unmarshal(rows[0].Body, &evidence) != nil ||
		evidence.Profile != "builtin-head/1" || !validDigest(evidence.ArchiveSHA256) {
		return reverseconverter.Dump{}, ReverseRefusal{"missing_source_evidence"}
	}
	if _, ok := evidence.Members["manifest.json"]; !ok {
		return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
	}
	if envelope, err := r.Blob(ctx, driver.Hash(evidence.ArchiveSHA256)); err == nil {
		if hashBytes(envelope) != evidence.ArchiveSHA256 {
			return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
		}
		dump, err := reverseconverter.ReadBytes(envelope)
		if err != nil || len(dump.Members) != len(evidence.Members) {
			return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
		}
		for path, file := range evidence.Members {
			member, ok := dump.Members[path]
			if !ok || int64(len(member)) != file.Size || hashBytes(member) != file.SHA256 {
				return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
			}
		}
		return dump, nil
	} else if !errors.Is(err, driver.ErrNotFound) {
		return reverseconverter.Dump{}, err
	}
	// Older restores retained only members. Their exact source hash can still
	// be checked when the envelope used the canonical Go writer.
	paths := []string{}
	for path := range evidence.Members {
		if path != "manifest.json" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	for _, path := range append([]string{"manifest.json"}, paths...) {
		file := evidence.Members[path]
		if !validDigest(file.SHA256) || file.Size < 0 || file.Size > dumpformat.MaxArchiveBytes || archive.Len()+int(file.Size) > dumpformat.MaxArchiveBytes {
			return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
		}
		data, err := r.Blob(ctx, driver.Hash(file.SHA256))
		if err != nil || int64(len(data)) != file.Size || hashBytes(data) != file.SHA256 {
			return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
		}
		if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: file.Size, Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}); err != nil {
			return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
		}
		if _, err := tw.Write(data); err != nil {
			return reverseconverter.Dump{}, err
		}
	}
	if err := tw.Close(); err != nil {
		return reverseconverter.Dump{}, err
	}
	if hashBytes(archive.Bytes()) != evidence.ArchiveSHA256 {
		return reverseconverter.Dump{}, ReverseRefusal{"invalid_source_evidence"}
	}
	return reverseconverter.ReadBytes(archive.Bytes())
}

func projectLiveDump(ctx context.Context, reader driver.Reader, dump *reverseconverter.Dump, live map[string]driver.Record) error {
	var archiveBinding archiveEvidence
	if json.Unmarshal(live["restore/archive"].Body, &archiveBinding) != nil {
		return ReverseRefusal{"invalid_source_evidence"}
	}
	delete(live, "restore/archive")
	imported := false
	if binding, ok := live[importBindingRecordID]; ok {
		var evidence importBindingEvidence
		if binding.Kind != "instance_config" || json.Unmarshal(binding.Body, &evidence) != nil ||
			evidence.Profile.ArchiveDigest != archiveBinding.ArchiveSHA256 || string(evidence.Profile.SourceStoreID) != dump.Manifest.StoreID {
			return ReverseRefusal{"invalid_import_binding"}
		}
		imported = true
		if evidence.Profile.HistoryDigest != "" {
			marker, ok := live[driver.OfflineHistoryEvidenceID]
			if !ok || marker.Kind != "beads-archive" || validateReverseHistory(ctx, reader, evidence, archiveBinding, marker) != nil {
				return ReverseRefusal{"invalid_history_binding"}
			}
			delete(live, driver.OfflineHistoryEvidenceID)
		}
		delete(live, importBindingRecordID)
		for _, project := range dump.Manifest.Projects {
			id := importProjectRecordID(project.Prefix)
			row, ok := live[id]
			var metadata importProjectMetadata
			if !ok || row.Kind != "instance_config" || json.Unmarshal(row.Body, &metadata) != nil ||
				metadata.Project != project.Prefix || !reflect.DeepEqual(metadata.ImportProfile, evidence.Profile) {
				return ReverseRefusal{"invalid_import_binding"}
			}
			delete(live, id)
		}
	}
	for kind, records := range dump.Records {
		for i, source := range records {
			id := source.ID
			if kind != "bead" {
				id = "source/" + kind + "/" + source.ID
			}
			row, ok := live[id]
			if !ok || row.Kind != kind {
				return ReverseRefusal{"missing_source_record"}
			}
			delete(live, id)
			if kind == "bead" {
				current, err := projectLiveBead(source, row, *dump)
				if err != nil {
					return err
				}
				dump.Records[kind][i] = current
				continue
			}
			want := cloneRow(source.Body)
			want["source_id"], want["source_table"], want["source"] = source.ID, source.Source.Table, source.Source.Fields
			// The public imported-memory view is canonical, while the retained
			// source row remains authoritative for lossless reverse conversion.
			if imported && kind == "memory" && source.Body["classification"] == "public" {
				if content, ok := source.Body["value"].(string); ok {
					want = map[string]any{"id": id, "key": strings.TrimSpace(stringValue(source.Body["key"])), "content": content, "metadata": map[string]any{
						"plug.restore.source_id": source.ID, "plug.restore.source_table": source.Source.Table, "plug.restore.source": source.Source.Fields,
						"plug.restore.archive_sha256": archiveBinding.ArchiveSHA256,
					}}
				}
			}
			var got map[string]any
			if decodeReverseJSON(row.Body, &got) != nil || !sameReverseJSON(want, got) {
				return ReverseRefusal{"changed_source_mirror"}
			}
		}
	}
	if len(live) != 0 {
		return ReverseRefusal{"unmapped_live_record"}
	}
	return nil
}

// A source replay adds a core-owned marker beyond the source head records.
// Accept it only when the same snapshot's original final receipt and retained
// source history agree. Later ordinary writes do not change that receipt.
func validateReverseHistory(ctx context.Context, reader driver.Reader, binding importBindingEvidence, archive archiveEvidence, marker driver.Record) error {
	p, err := binding.Profile.Detached()
	if err != nil {
		return err
	}
	request, err := p.RequestHash()
	if err != nil || request != binding.RequestHash {
		return driver.ErrConflict
	}
	principal, err := driver.PrincipalKey(p.Admission.Actor)
	if err != nil {
		return err
	}
	receipt, err := reader.IdempotencyByKey(ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || receipt == nil || receipt.RequestHash != request || receipt.ResponsePending || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Incarnation != reader.Snapshot().Incarnation {
		return driver.ErrConflict
	}
	rows, err := reader.Get(ctx, []model.Ref{marker.Ref})
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, marker.Body) {
		return driver.ErrConflict
	}
	return validateHistoryRecovery(ctx, reader, p, archive, receipt)
}

func projectLiveBead(source reverseconverter.Record, row driver.Record, dump reverseconverter.Dump) (reverseconverter.Record, error) {
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil || doc.ID != source.ID || doc.Project != source.Project || doc.Type != stringValue(source.Body["type"]) {
		return reverseconverter.Record{}, ReverseRefusal{"changed_source_identity"}
	}
	if doc.Lease != nil || len(doc.Attachments) != 0 {
		return reverseconverter.Record{}, ReverseRefusal{"live_claim_or_attachment"}
	}
	for field := range doc.Fields {
		if field != "status" && field != "title" && field != "description" && field != "priority" && field != "labels" {
			return reverseconverter.Record{}, ReverseRefusal{"unsupported_live_field"}
		}
	}
	if raw, present := doc.Fields["labels"]; present {
		labels, ok := exactLabelSet(raw)
		if !ok || !slices.Equal(labels, doc.Labels) {
			return reverseconverter.Record{}, ReverseRefusal{"inconsistent_live_labels"}
		}
	}
	if err := checkLiveRelationsAndComments(source.ID, doc, dump); err != nil {
		return reverseconverter.Record{}, err
	}
	source.Body, source.Source.Fields = cloneRow(source.Body), cloneRow(source.Source.Fields)
	reserved := map[string]any{"beads.source_fields": source.Source.Fields, "beads.source.status": source.Source.Fields["status"]}
	for key, value := range source.Body {
		switch key {
		case "title", "description", "priority", "metadata", "labels":
			continue
		}
		reserved["beads.source."+key] = value
	}
	for key, want := range reserved {
		var got any
		if decodeReverseJSON(doc.Metadata[key], &got) != nil || !sameReverseJSON(want, got) {
			return reverseconverter.Record{}, ReverseRefusal{"changed_source_provenance"}
		}
	}
	opaque := map[string]bool{}
	for _, field := range dumpformat.OpaqueIssueFields() {
		key := "beads." + field
		opaque[key] = true
		var value any
		if decodeReverseJSON(doc.Metadata[key], &value) != nil {
			return reverseconverter.Record{}, ReverseRefusal{"missing_opaque_metadata"}
		}
		source.Source.Fields[field] = value
	}
	metadata := map[string]any{}
	for key, raw := range doc.Metadata {
		if _, ok := reserved[key]; ok || opaque[key] {
			continue
		}
		if key == "plug.restore.allocator" {
			continue // Legacy restore's allocator mirror is not source metadata.
		}
		if strings.HasPrefix(key, "beads.") || strings.HasPrefix(key, "plug.restore.") {
			return reverseconverter.Record{}, ReverseRefusal{"unknown_reserved_metadata"}
		}
		var value any
		if err := decodeReverseJSON(raw, &value); err != nil {
			return reverseconverter.Record{}, ReverseRefusal{"invalid_live_metadata"}
		}
		metadata[key] = value
	}
	for _, field := range []string{"title", "description", "priority"} {
		source.Body[field], source.Source.Fields[field] = doc.Fields[field], doc.Fields[field]
	}
	originalMetadata, err := dumpformat.SourceMetadataObject(source.Body["metadata"])
	if err != nil {
		return reverseconverter.Record{}, ReverseRefusal{"invalid_source_metadata"}
	}
	// The live document projects metadata keys, not the SQL cell's spelling.
	// Keep an unchanged source cell verbatim, including NULL and JSON text.
	if len(originalMetadata) != len(metadata) || len(metadata) != 0 && !sameReverseJSON(originalMetadata, metadata) {
		source.Body["metadata"], source.Source.Fields["metadata"] = metadata, metadata
	}
	labels := []any{}
	for _, label := range doc.Labels {
		labels = append(labels, label)
	}
	source.Body["labels"] = labels
	status := stringValue(source.Source.Fields["status"])
	if original, ok := targetStatus(status); !ok || original != stringValue(doc.Fields["status"]) {
		switch stringValue(doc.Fields["status"]) {
		case "todo":
			status = "open"
		case "in_progress":
			status = "in_progress"
		case "backlog":
			status = "blocked"
		case "done":
			status = "closed"
		default:
			return reverseconverter.Record{}, ReverseRefusal{"unsupported_live_status"}
		}
	}
	source.Source.Fields["status"] = status
	originalOutcome := stringValue(source.Body["result"])
	originalCompleted := source.Body["status_category"] == "completed"
	delete(source.Body, "result")
	switch stringValue(doc.Fields["status"]) {
	case "todo":
		source.Body["status_category"] = "unstarted"
	case "in_progress":
		source.Body["status_category"] = "started"
	case "done":
		source.Body["status_category"], source.Body["result"] = "completed", "success"
		if originalCompleted && (originalOutcome == "success" || originalOutcome == "failure") {
			source.Body["result"] = originalOutcome
		}
	default:
		source.Body["status_category"] = "backlog"
	}
	return source, nil
}

func checkLiveRelationsAndComments(id string, doc pipeline.Document, dump reverseconverter.Dump) error {
	links := []pipeline.Link{}
	var parent *model.Ref
	edgeVersion := 1
	for _, kind := range dump.Manifest.Kinds {
		if kind.Name == "edge" {
			edgeVersion = kind.Version
		}
	}
	for _, edge := range dump.Records["edge"] {
		if edge.Body["from"] != id {
			continue
		}
		kind := stringValue(edge.Body["type"])
		var targetKind model.LinkTargetKind
		switch stringValue(edge.Body["target_kind"]) {
		case "depends_on_issue_id":
		case "depends_on_external":
			targetKind = model.LinkTargetExternal
		default:
			return ReverseRefusal{"changed_source_relationships"}
		}
		if kind == "parent-child" && targetKind != model.LinkTargetExternal {
			if parent != nil {
				return ReverseRefusal{"changed_source_relationships"}
			}
			ref := model.RefByID(stringValue(edge.Body["to"]), model.Natural64(0))
			parent = &ref
			continue
		}
		if kind == "related" {
			kind = "relates-to"
		}
		link, err := importedVersionedSourceLink(dumpformat.Record{ID: edge.ID, Body: dumpformat.Row(edge.Body)}, kind, targetKind, edgeVersion)
		if err != nil {
			return ReverseRefusal{"changed_source_relationships"}
		}
		links = append(links, link)
	}
	if !reflect.DeepEqual(parent, doc.Parent) {
		return ReverseRefusal{"changed_source_relationships"}
	}
	key := func(link pipeline.Link) string {
		return fmt.Sprintf("%s/%s/%s/%s", link.Kind, link.TargetKind, link.To.Branch.String(), link.To.ID)
	}
	sort.Slice(links, func(i, j int) bool { return key(links[i]) < key(links[j]) })
	current := append([]pipeline.Link{}, doc.Links...)
	sort.Slice(current, func(i, j int) bool { return key(current[i]) < key(current[j]) })
	if !reflect.DeepEqual(links, current) {
		return ReverseRefusal{"changed_source_relationships"}
	}
	comments := []reverseconverter.Record{}
	for _, comment := range dump.Records["comment"] {
		if comment.Body["issue_id"] == id {
			comments = append(comments, comment)
		}
	}
	if len(comments) != len(doc.Comments) {
		return ReverseRefusal{"changed_source_comments"}
	}
	for i, source := range comments {
		current := doc.Comments[i]
		author, err := pipeline.NormalizeSourceAuthor(stringValue(source.Body["author"]))
		timestamp, timeErr := time.Parse(time.RFC3339Nano, stringValue(source.Body["created_at"]))
		if err != nil || timeErr != nil || current.Origin != "import" || current.Body != source.Body["text"] ||
			current.SourceAuthor != author || current.SourceTime == nil || !current.SourceTime.Equal(timestamp) {
			return ReverseRefusal{"changed_source_comments"}
		}
	}
	return nil
}

func decodeReverseJSON(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return ReverseRefusal{"invalid_live_json"}
	}
	return nil
}

func sameReverseJSON(a, b any) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}
