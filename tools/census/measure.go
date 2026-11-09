package census

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func bytesDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func jsonDigest(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return bytesDigest(data)
}

func sameJSON(a, b any) bool { return jsonDigest(a) != "" && jsonDigest(a) == jsonDigest(b) }

// Capture owns a validated, detached source archive. It has no JSON encoder
// and no raw-value accessor. Only Summary can cross the sanitized boundary.
type Capture struct{ archive *dumpformat.Archive }

// MeasureArchive uses the supported canonical reader, including both complete
// migration series and their content hashes. A cursor maximum alone cannot
// produce a measurement. Parser error text is deliberately discarded.
func MeasureArchive(input io.Reader) (*Capture, error) {
	if input == nil {
		return nil, refused(ArchiveInvalid)
	}
	a, err := dumpformat.Read(input)
	if err != nil || a.Native() || !validWorkflowDispositions(a.Manifest().Sources) {
		return nil, refused(ArchiveInvalid)
	}
	return &Capture{archive: a}, nil
}

func validWorkflowDispositions(sources []dumpformat.Disposition) bool {
	byFamily := make(map[string]dumpformat.Disposition, len(sources))
	for _, source := range sources {
		byFamily[source.Family] = source
	}
	for _, family := range workflowDispositionFamilies {
		source, ok := byFamily[family]
		if !ok || source.Rows < 0 || source.Action != workflowDispositionAction || source.Reason != workflowDispositionReason {
			return false
		}
	}
	return true
}

func allSourceRecords(a *dumpformat.Archive) []dumpformat.Record {
	rows := []dumpformat.Record{}
	for _, kind := range a.Manifest().Kinds {
		rows = append(rows, a.Records(kind.Name)...)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Kind != rows[j].Kind {
			return rows[i].Kind < rows[j].Kind
		}
		return rows[i].ID < rows[j].ID
	})
	return rows
}

func (c *Capture) Summary() Snapshot {
	if c == nil || c.archive == nil {
		return Snapshot{}
	}
	return summarize(c.archive, allSourceRecords(c.archive))
}

func summarize(a *dumpformat.Archive, rows []dumpformat.Record) Snapshot {
	m := a.Manifest()
	s := Snapshot{Scope: PreservationScope, SourceSHA256: a.SHA256(), WorkflowDispositions: summarizeWorkflowDispositions(m.Sources)}
	s.Schema.Main, s.Schema.Ignored = m.MainVersion, m.IgnoredVersion
	type identity struct{ Kind, ID, Project string }
	type migration struct{ Version, Hash any }
	ids := []identity{}
	edges, versions, archives := []dumpformat.Record{}, []dumpformat.Record{}, []dumpformat.Record{}
	main, ignored := []migration{}, []migration{}
	labels := 0
	for _, r := range rows {
		ids = append(ids, identity{r.Kind, r.ID, r.Project})
		if r.Kind == "bead" {
			for _, label := range r.Body["labels"].([]any) {
				ids = append(ids, identity{"label", r.ID, jsonDigest(label)})
				labels++
			}
		}
		if r.Kind == "edge" {
			edges = append(edges, r)
		}
		if r.Kind == "beads-archive" {
			archives = append(archives, r)
		}
		switch r.Source.Table {
		case "schema_migrations":
			main = append(main, migration{r.Source.Fields["version"], r.Source.Fields["content_hash"]})
		case "ignored_schema_migrations":
			ignored = append(ignored, migration{r.Source.Fields["version"], r.Source.Fields["content_hash"]})
		case "issue_versions", "events", "issue_snapshots", "compaction_snapshots", "provenance_events", "\x62d_events_journal":
			versions = append(versions, r)
		}
	}
	// The source record order is canonical by kind/id. Migration archive IDs
	// are hashes, so sort the fingerprint tuples independently of those IDs.
	sort.Slice(main, func(i, j int) bool { return jsonDigest(main[i]) < jsonDigest(main[j]) })
	sort.Slice(ignored, func(i, j int) bool { return jsonDigest(ignored[i]) < jsonDigest(ignored[j]) })
	s.Schema.MainSHA256, s.Schema.IgnoredSHA256 = jsonDigest(main), jsonDigest(ignored)
	projects := append([]dumpformat.Project(nil), m.Projects...)
	sort.Slice(projects, func(i, j int) bool { return projects[i].Prefix < projects[j].Prefix })
	type prefix struct {
		Prefix, Mode string
		HashLength   int
	}
	type counter struct {
		Prefix   string
		Last     uint64
		Children map[string]uint64
	}
	prefixes, counters := []prefix{}, []counter{}
	for _, p := range projects {
		prefixes = append(prefixes, prefix{p.Prefix, p.IDMode, p.HashLength})
		counters = append(counters, counter{p.Prefix, p.LastID, p.ChildCounters})
	}
	sources := append([]dumpformat.Disposition(nil), m.Sources...)
	sort.Slice(sources, func(i, j int) bool { return sources[i].Family < sources[j].Family })
	add := func(name string, count int, value any, block BlockCode) {
		s.Metrics = append(s.Metrics, Metric{Dimension: name, Count: count, SHA256: jsonDigest(value), Block: block})
	}
	add("counts", len(rows)+labels, sources, "")
	add("ids", len(ids), ids, "")
	add("prefixes", len(prefixes), prefixes, "")
	add("counters", len(counters), counters, "")
	add("dependencies", len(edges), edges, "")
	// Inventory of available source history is useful, but archive retention
	// alone cannot prove an ordered, complete and replayable history range.
	add("versions", len(versions), versions, HistoryUnavailable)
	add("archives", len(archives), archives, "")
	add("digests", len(rows), struct {
		Rows  []dumpformat.Record
		Files map[string]dumpformat.File
	}{rows, m.Files}, "")
	return s
}

func summarizeWorkflowDispositions(sources []dumpformat.Disposition) []WorkflowDisposition {
	byFamily := make(map[string]dumpformat.Disposition, len(sources))
	for _, source := range sources {
		byFamily[source.Family] = source
	}
	result := make([]WorkflowDisposition, 0, len(workflowDispositionFamilies))
	for _, family := range workflowDispositionFamilies {
		source := byFamily[family]
		result = append(result, WorkflowDisposition{Family: family, Rows: source.Rows, Action: workflowDispositionAction, Reason: workflowDispositionReason})
	}
	return result
}

func decodeRow(data []byte, target any) bool {
	if rejectDuplicateMembers(data) != nil {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if d.Decode(target) != nil {
		return false
	}
	var extra any
	return d.Decode(&extra) == io.EOF
}

// MeasureStore reads one pinned immutable snapshot through the ordinary
// driver API. It never acquires a lease, opens a writer, migrates a schema,
// constructs a restore plan or borrows a raw database handle. The caller owns
// the store; this function owns and closes exactly its reader.
//
// It compares current native fields as well as retained source provenance and
// every archived member blob. Provenance-only copies cannot hide a damaged
// live title, labels, dependency endpoints, comments or configuration body.
func (c *Capture) MeasureStore(ctx context.Context, store driver.Store, pageSize int) (result Snapshot, err error) {
	if c == nil || c.archive == nil || store == nil || pageSize < 1 || pageSize > 1000 {
		return Snapshot{}, refused(MeasurementInvalid)
	}
	meta, e := store.Meta(ctx)
	if e != nil {
		return Snapshot{}, refused(CapabilityUnavailable)
	}
	head := meta.Head
	r, e := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &head})
	if e != nil {
		return Snapshot{}, refused(CapabilityUnavailable)
	}
	defer func() {
		if r.Close() != nil && err == nil {
			result = Snapshot{}
			err = refused(MeasurementInvalid)
		}
	}()
	view := r.Snapshot()
	if view.At == nil || view.At.String() != head.String() || view.Incarnation != meta.Incarnation || !view.Branch.IsZero() {
		return Snapshot{}, refused(MeasurementInvalid)
	}
	rows := map[string]driver.Record{}
	var cursor *driver.Cursor
	seenCursors := map[string]bool{}
	for {
		page, e := r.Query(ctx, driver.Query{Limit: pageSize, Cursor: cursor})
		if e != nil || page.AsOf.String() != head.String() {
			return Snapshot{}, refused(MeasurementInvalid)
		}
		for _, row := range page.Items {
			if !row.Ref.IsID() || !row.Ref.Branch.IsZero() || row.Ref.Validate() != nil {
				return Snapshot{}, refused(TargetDiffers)
			}
			if _, exists := rows[row.Ref.ID]; exists {
				return Snapshot{}, refused(TargetDiffers)
			}
			rows[row.Ref.ID] = row
		}
		if page.NextCursor == nil {
			break
		}
		key := jsonDigest(page.NextCursor)
		if len(page.Items) == 0 || seenCursors[key] {
			return Snapshot{}, refused(MeasurementInvalid)
		}
		seenCursors[key] = true
		cursor = page.NextCursor
	}
	actual, err := c.checkRecords(ctx, r, rows)
	if err != nil {
		return Snapshot{}, err
	}
	result = summarize(c.archive, actual)
	// The source allocator mirror is not proof of native next-ID continuity.
	// Keep that boundary blocked even when all retained source counters match.
	for i := range result.Metrics {
		if result.Metrics[i].Dimension == "counters" {
			result.Metrics[i].Block = AllocatorUnproven
		}
	}
	history, e := measureNativeHistory(ctx, r, meta, rows, pageSize)
	if e != nil {
		return Snapshot{}, e
	}
	result.NativeHistory = &history
	return result, nil
}

func (c *Capture) checkRecords(ctx context.Context, r driver.Reader, target map[string]driver.Record) ([]dumpformat.Record, error) {
	expected := allSourceRecords(c.archive)
	actual := make([]dumpformat.Record, 0, len(expected))
	used := map[string]bool{}
	for _, source := range expected {
		id := source.ID
		if source.Kind != "bead" {
			id = "source/" + source.Kind + "/" + id
		}
		row, ok := target[id]
		if !ok || row.Kind != source.Kind {
			return nil, refused(TargetDiffers)
		}
		used[id] = true
		measured := source
		if source.Kind == "bead" {
			var doc pipeline.Document
			if !decodeRow(row.Body, &doc) || !c.checkBead(source, doc) {
				return nil, refused(TargetDiffers)
			}
			var fields dumpformat.Row
			if !decodeRow(doc.Metadata["beads.source_fields"], &fields) {
				return nil, refused(TargetDiffers)
			}
			measured.Source.Fields = fields
		} else {
			var body dumpformat.Row
			if !decodeRow(row.Body, &body) || body["source_id"] != source.ID || body["source_table"] != source.Source.Table {
				return nil, refused(TargetDiffers)
			}
			fields, ok := body["source"].(map[string]any)
			if !ok {
				return nil, refused(TargetDiffers)
			}
			measured.Source.Fields = fields
			delete(body, "source")
			delete(body, "source_id")
			delete(body, "source_table")
			measured.Body = body
		}
		if !sameJSON(source, measured) {
			return nil, refused(TargetDiffers)
		}
		actual = append(actual, measured)
	}
	marker, ok := target["restore/archive"]
	if !ok || marker.Kind != "restore-archive" {
		return nil, refused(TargetDiffers)
	}
	var evidence struct {
		ArchiveSHA256 string                     `json:"archive_sha256"`
		DerivedSHA256 string                     `json:"derived_sha256"`
		Profile       string                     `json:"profile"`
		SourceStore   string                     `json:"source_store"`
		Members       map[string]dumpformat.File `json:"members"`
	}
	if !decodeRow(marker.Body, &evidence) || evidence.ArchiveSHA256 != c.archive.SHA256() || evidence.SourceStore != c.archive.Manifest().StoreID || evidence.Profile != "builtin-head/1" || !sha256Pattern.MatchString(evidence.DerivedSHA256) {
		return nil, refused(TargetDiffers)
	}
	files := c.archive.Manifest().Files
	manifest, _ := c.archive.Member("manifest.json")
	files["manifest.json"] = dumpformat.File{SHA256: bytesDigest(manifest), Size: int64(len(manifest))}
	if !sameJSON(files, evidence.Members) {
		return nil, refused(TargetDiffers)
	}
	for name, file := range files {
		data, e := r.Blob(ctx, driver.Hash(file.SHA256))
		source, _ := c.archive.Member(name)
		if e != nil || !bytes.Equal(data, source) {
			return nil, refused(TargetDiffers)
		}
	}
	used[marker.Ref.ID] = true
	// Import-only protected metadata may coexist with the bounded head. Bind
	// its source allocator and archive to this capture before accounting for it.
	for _, p := range c.archive.Manifest().Projects {
		id := "instance_config/restore-import/project/" + p.Prefix
		if row, exists := target[id]; exists {
			var doc struct {
				Project         string                     `json:"project"`
				Metadata        map[string]json.RawMessage `json:"metadata"`
				TargetAllocator driver.OfflineAllocator    `json:"targetAllocator"`
			}
			if row.Kind != "instance_config" || !decodeRow(row.Body, &doc) || doc.Project != p.Prefix || !sameJSON(doc.TargetAllocator, driver.NewRandomAllocator(p.Prefix)) {
				return nil, refused(TargetDiffers)
			}
			var allocator dumpformat.Project
			if !decodeRow(doc.Metadata["plug.restore.sourceAllocator"], &allocator) || !sameJSON(allocator, p) {
				return nil, refused(TargetDiffers)
			}
			used[id] = true
		}
	}
	const bindingID = "instance_config/restore-import/binding"
	if row, exists := target[bindingID]; exists {
		var binding struct {
			Profile struct {
				ArchiveDigest string `json:"archiveDigest"`
			} `json:"profile"`
		}
		if row.Kind != "instance_config" || !decodeRow(row.Body, &binding) || binding.Profile.ArchiveDigest != c.archive.SHA256() {
			return nil, refused(TargetDiffers)
		}
		used[bindingID] = true
	}
	if len(used) != len(target) {
		return nil, refused(TargetDiffers)
	}
	return actual, nil
}

func (c *Capture) checkBead(source dumpformat.Record, doc pipeline.Document) bool {
	if doc.ID != source.ID || doc.Type != source.Body["type"] || doc.Project != source.Project || doc.Lease != nil || len(doc.Attachments) != 0 {
		return false
	}
	status := map[string]string{"open": "todo", "in_progress": "in_progress", "\x68ooked": "in_progress", "blocked": "backlog", "deferred": "backlog", "pinned": "backlog", "frozen": "backlog", "closed": "done"}[textValue(source.Source.Fields["status"])]
	fields := map[string]any{"status": status}
	for _, name := range []string{"title", "description", "priority"} {
		if v := source.Body[name]; v != nil {
			fields[name] = v
		}
	}
	if labels := source.Body["labels"].([]any); len(labels) != 0 {
		fields["labels"] = append([]any(nil), labels...)
	}
	if status == "" || !sameJSON(fields, doc.Fields) || !sameJSON(source.Body["labels"], append([]string{}, doc.Labels...)) {
		return false
	}
	metadata := map[string]any{}
	if original, ok := source.Body["metadata"].(map[string]any); ok {
		for k, v := range original {
			metadata[k] = v
		}
	}
	for _, name := range dumpformat.OpaqueIssueFields() {
		if v, ok := source.Source.Fields[name]; ok {
			metadata["beads."+name] = v
		}
	}
	for k, v := range source.Body {
		switch k {
		case "title", "description", "priority", "metadata", "labels":
			continue
		}
		metadata["beads.source."+k] = v
	}
	metadata["beads.source.status"] = source.Source.Fields["status"]
	metadata["beads.source_fields"] = source.Source.Fields
	if _, legacy := doc.Metadata["plug.restore.allocator"]; legacy {
		for _, p := range c.archive.Manifest().Projects {
			if p.Prefix == source.Project {
				metadata["plug.restore.allocator"] = p
			}
		}
	}
	if !sameJSON(metadata, doc.Metadata) {
		return false
	}
	links := []pipeline.Link{}
	for _, edge := range c.archive.Records("edge") {
		if edge.Body["from"] != source.ID {
			continue
		}
		kind := textValue(edge.Body["type"])
		if kind == "related" {
			kind = "relates-to"
		}
		links = append(links, pipeline.Link{Kind: kind, To: model.RefByID(textValue(edge.Body["to"]), model.Natural64(0))})
	}
	key := func(link pipeline.Link) string { return link.Kind + "/" + link.To.ID }
	sort.Slice(links, func(i, j int) bool { return key(links[i]) < key(links[j]) })
	native := append([]pipeline.Link{}, doc.Links...)
	sort.Slice(native, func(i, j int) bool { return key(native[i]) < key(native[j]) })
	if !sameJSON(links, native) {
		return false
	}
	comments := []dumpformat.Record{}
	for _, comment := range c.archive.Records("comment") {
		if comment.Body["issue_id"] == source.ID {
			comments = append(comments, comment)
		}
	}
	if len(comments) != len(doc.Comments) {
		return false
	}
	for i, comment := range comments {
		actual := doc.Comments[i]
		timestamp, e := time.Parse(time.RFC3339Nano, textValue(comment.Body["created_at"]))
		if e != nil || actual.Origin != "import" || actual.Body != comment.Body["text"] || actual.SourceAuthor != comment.Body["author"] || actual.SourceTime == nil || !actual.SourceTime.Equal(timestamp) || actual.Validate() != nil {
			return false
		}
	}
	return true
}

func textValue(v any) string { s, _ := v.(string); return s }

// Native counts describe only versions of current records and changesets
// strictly above the reported floor. They are diagnostic inventory, never a
// certificate of source history or of tombstones below that floor.
func measureNativeHistory(ctx context.Context, r driver.Reader, meta driver.Meta, rows map[string]driver.Record, pageSize int) (NativeHistory, error) {
	h := NativeHistory{Head: meta.Head.String(), Floor: meta.HistoryFloor.String()}
	versionDigests, changeDigests := []string{}, []string{}
	for _, row := range rows {
		var cursor *driver.Cursor
		seen := map[string]bool{}
		for {
			page, e := r.Versions(ctx, row.Ref, driver.PageReq{Limit: pageSize, Cursor: cursor})
			if e != nil || page.AsOf.String() != meta.Head.String() {
				return NativeHistory{}, refused(MeasurementInvalid)
			}
			for _, v := range page.Items {
				versionDigests = append(versionDigests, jsonDigest(v))
				h.Versions++
			}
			if page.NextCursor == nil {
				break
			}
			key := jsonDigest(page.NextCursor)
			if len(page.Items) == 0 || seen[key] {
				return NativeHistory{}, refused(MeasurementInvalid)
			}
			seen[key] = true
			cursor = page.NextCursor
		}
	}
	cursor := &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor, Method: "changesets"}
	for {
		entries, e := r.Changesets(ctx, cursor, pageSize)
		if e != nil {
			return NativeHistory{}, refused(MeasurementInvalid)
		}
		if len(entries) == 0 {
			break
		}
		for _, v := range entries {
			previous, _ := cursor.Revision.Uint64()
			next, e := v.Revision.Uint64()
			head, _ := meta.Head.Uint64()
			if e != nil || next <= previous || next > head {
				return NativeHistory{}, refused(MeasurementInvalid)
			}
			changeDigests = append(changeDigests, jsonDigest(v))
			h.Changesets++
			cursor.Revision = v.Revision
		}
	}
	sort.Strings(versionDigests)
	h.SHA256 = jsonDigest(struct{ Versions, Changesets []string }{versionDigests, changeDigests})
	return h, nil
}
