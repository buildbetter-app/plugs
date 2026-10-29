// Package converterroundtrip contains the bounded, offline Beads-to-Plug
// restore seam. The archive validator owns the source trust boundary; this
// package only lowers its already validated records into an empty Plug store.
package converterroundtrip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var (
	// ErrRestoreUnavailable is retained for source compatibility with the
	// parser-only checkpoint. Valid archives now use the atomic restore seam.
	ErrRestoreUnavailable = errors.New("trusted offline restore seam unavailable")
	ErrTargetNotEmpty     = errors.New("restore target is not empty")
)

// Report retains the complete parsed source evidence and the target metadata
// observed before the restore transaction. Target is intentionally the
// pre-restore snapshot so refusal reports cannot be mistaken for a commit.
type Report struct {
	Archive *dumpformat.Archive
	Target  driver.Meta
}

// Restore validates the complete archive and restores it into an empty store.
// The wrapper obtains the store lease itself; callers that already own the
// lease (including integration tests) can use RestoreWithLease.
func Restore(ctx context.Context, input io.Reader, target driver.Store) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.Read(input)
	if err != nil {
		return Report{}, err
	}
	return restoreArchive(ctx, archive, target, nil)
}

// RestoreWithLease is the same restore operation for a caller that already
// holds the target's active lease. It is deliberately not a second restore
// implementation: both entry points run the same bounded transaction.
func RestoreWithLease(ctx context.Context, input io.Reader, target driver.Store, lease driver.ActiveLease) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.Read(input)
	if err != nil {
		return Report{}, err
	}
	return restoreArchive(ctx, archive, target, lease)
}

func restoreArchive(ctx context.Context, archive *dumpformat.Archive, target driver.Store, supplied driver.ActiveLease) (Report, error) {
	if archive == nil || target == nil {
		return Report{Archive: archive}, fmt.Errorf("restore requires an archive and target store")
	}
	meta, err := target.Meta(ctx)
	report := Report{Archive: archive, Target: meta}
	if err != nil {
		return report, err
	}
	if !meta.Head.IsZero() {
		return report, ErrTargetNotEmpty
	}

	plan, err := buildPlan(archive)
	if err != nil {
		return report, err
	}

	lease := supplied
	owned := false
	if lease == nil {
		lease, err = target.Lease(ctx, "plug-converter-restore", driver.LeaseTTL)
		if err != nil {
			return report, err
		}
		owned = true
	}
	if owned {
		defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	}

	// A lease serializes all writers. Re-read metadata after acquiring it so a
	// restore cannot use an empty pre-lease observation after another writer
	// committed before the lease boundary.
	meta, err = target.Meta(ctx)
	if err != nil {
		return report, err
	}
	if !meta.Head.IsZero() {
		return report, ErrTargetNotEmpty
	}

	reader, err := target.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return report, err
	}
	existing, err := allRecords(ctx, reader)
	_ = reader.Close()
	if err != nil {
		return report, err
	}

	requests := lockRequests(existing, plan)
	tx, err := target.Write(ctx, lease)
	if err != nil {
		return report, err
	}
	defer tx.Close()
	if _, err := tx.Lock(ctx, requests); err != nil {
		return report, err
	}

	// The target must be empty while the complete lock set is held. The lease
	// is the store-wide writer fence; this second read makes the invariant
	// explicit and protects drivers with a wider read surface.
	recheck, err := allRecords(ctx, tx)
	if err != nil {
		return report, err
	}
	if len(recheck) != 0 {
		return report, ErrTargetNotEmpty
	}

	if _, err := tx.Head(ctx); err != nil {
		return report, err
	}
	previous := map[string]model.Revision{}
	states, err := tx.Observe(ctx, lockKeys(requests))
	if err != nil {
		return report, err
	}
	for _, state := range states {
		if state.Key.Kind != driver.LockRecord || state.Key.Ref == nil {
			continue
		}
		key, err := driver.EncodeLockKey(state.Key)
		if err != nil {
			return report, err
		}
		previous[key] = state.Revision
	}

	changes := make([]model.ChangeRecord, 0, len(plan.records))
	results := make([]model.OpResult, 0, len(plan.records))
	for _, record := range plan.records {
		if err := tx.Put(ctx, driver.Record{Ref: record.ref, Kind: record.kind, Body: record.body}); err != nil {
			return report, err
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.ref})
		if err != nil {
			return report, err
		}
		changes = append(changes, model.ChangeRecord{Ref: record.ref, Op: model.OpCreate, FromRevision: previous[key], ChangedPaths: []string{"/"}})
		results = append(results, model.OpResult{Ref: record.ref, Revision: model.Natural64(1), Record: append([]byte(nil), record.body...)})
	}
	if err := tx.BumpSentinels(ctx, plan.sentinels); err != nil {
		return report, err
	}

	head, err := meta.Head.Uint64()
	if err != nil || head == ^uint64(0) {
		return report, fmt.Errorf("restore revision space is exhausted")
	}
	commit, err := tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: meta.Incarnation,
		Kind:        model.ChangesetRestore,
		Time:        time.Now().UTC(),
		Principal:   model.PrincipalRef{Kind: "system", ID: "plug-converter"},
		Chain:       []string{},
		Method:      "admin.restore",
		Response:    model.Commit{Revision: model.Natural64(head + 1), Results: results, Warnings: []model.Warning{}},
		Changes:     changes,
		LockKeys:    lockKeys(requests),
	})
	if err != nil {
		return report, err
	}
	if commit.Kind != model.ChangesetRestore || len(commit.Changes) != len(plan.records) {
		return report, fmt.Errorf("restore commit did not retain its complete changeset")
	}
	return report, nil
}

type restoreRecord struct {
	ref  model.Ref
	kind string
	body []byte
}

type restorePlan struct {
	records   []restoreRecord
	sentinels []driver.Sentinel
}

func buildPlan(archive *dumpformat.Archive) (restorePlan, error) {
	manifest := archive.Manifest()
	projects := map[string]dumpformat.Project{}
	for _, project := range manifest.Projects {
		projects[project.Prefix] = project
	}

	beads := archive.Records("bead")
	docs := map[string]pipeline.Document{}
	for _, source := range beads {
		status, ok := targetStatus(stringValue(source.Source.Fields["status"]))
		if !ok {
			return restorePlan{}, fmt.Errorf("unsupported source status for %s", source.ID)
		}
		// The current built-in ontology has only title/description plus the
		// kernel priority field. Preserve every other source field in metadata
		// so restored beads remain writable by the ordinary pipeline instead of
		// presenting unsupported source columns as typed Plug fields.
		fields := map[string]any{"status": status}
		metadata := map[string]json.RawMessage{}
		for _, key := range []string{"title", "description", "priority"} {
			if value := source.Body[key]; value != nil {
				fields[key] = value
			}
		}
		if raw, ok := source.Body["metadata"].(map[string]any); ok {
			for key, value := range raw {
				encoded, err := json.Marshal(value)
				if err != nil {
					return restorePlan{}, fmt.Errorf("bead %s metadata: %w", source.ID, err)
				}
				metadata[key] = encoded
			}
		}
		for key, value := range source.Body {
			switch key {
			case "title", "description", "priority", "metadata":
				continue
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				return restorePlan{}, fmt.Errorf("bead %s source field %s: %w", source.ID, key, err)
			}
			metadata["beads.source."+key] = encoded
		}
		sourceStatus, err := json.Marshal(stringValue(source.Source.Fields["status"]))
		if err != nil {
			return restorePlan{}, fmt.Errorf("bead %s source status: %w", source.ID, err)
		}
		metadata["beads.source.status"] = sourceStatus
		sourceFields, err := json.Marshal(source.Source.Fields)
		if err != nil {
			return restorePlan{}, fmt.Errorf("bead %s source fields: %w", source.ID, err)
		}
		metadata["beads.source_fields"] = sourceFields
		if project, ok := projects[source.Project]; ok {
			encoded, err := json.Marshal(project)
			if err != nil {
				return restorePlan{}, err
			}
			metadata["plug.restore.allocator"] = encoded
		}
		docs[source.ID] = pipeline.Document{ID: source.ID, Type: stringValue(source.Body["type"]), Project: source.Project, Fields: fields, Metadata: metadata}
	}

	// Relations that the current bounded service understands become live links;
	// parent-child and other source-only relations remain evidence records.
	for _, source := range archive.Records("edge") {
		from, to := stringValue(source.Body["from"]), stringValue(source.Body["to"])
		doc, ok := docs[from]
		if !ok {
			return restorePlan{}, fmt.Errorf("edge %s has no restored owner", source.ID)
		}
		kind := stringValue(source.Body["type"])
		linkKind := ""
		switch kind {
		case "blocks":
			linkKind = "blocks"
		case "related", "discovered-from":
			linkKind = "relates-to"
		}
		if linkKind != "" {
			doc.Links = append(doc.Links, pipeline.Link{To: model.RefByID(to, model.Natural64(0)), Kind: linkKind})
			docs[from] = doc
		}
	}

	for _, source := range archive.Records("comment") {
		owner := stringValue(source.Source.Fields["issue_id"])
		doc, ok := docs[owner]
		if !ok {
			return restorePlan{}, fmt.Errorf("comment %s has no restored owner", source.ID)
		}
		stamp, err := sourceTime(source.Source.Fields["created_at"])
		if err != nil {
			return restorePlan{}, fmt.Errorf("comment %s: %w", source.ID, err)
		}
		doc.Comments = append(doc.Comments, pipeline.Comment{
			Body:      stringValue(source.Body["text"]),
			Principal: model.PrincipalRef{Kind: "system", ID: "beads/" + stringValue(source.Body["author"])},
			Time:      stamp,
		})
		docs[owner] = doc
	}

	plan := restorePlan{}
	seen := map[string]bool{}
	add := func(ref model.Ref, kind string, body any) error {
		if err := ref.Validate(); err != nil || !ref.IsID() || ref.ID == "" || !ref.Branch.IsZero() {
			return fmt.Errorf("invalid restore record identity %q", ref.ID)
		}
		key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if err != nil {
			return err
		}
		if seen[key] {
			return fmt.Errorf("duplicate restore record identity %q", ref.ID)
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		seen[key] = true
		plan.records = append(plan.records, restoreRecord{ref: ref, kind: kind, body: encoded})
		return nil
	}

	for _, source := range beads {
		doc := docs[source.ID]
		if err := add(model.RefByID(source.ID, model.Natural64(0)), "bead", doc); err != nil {
			return restorePlan{}, err
		}
	}
	for _, kind := range []string{"edge", "comment", "memory", "config", "beads-archive"} {
		for _, source := range archive.Records(kind) {
			body := cloneRow(source.Body)
			body["source_id"] = source.ID
			body["source_table"] = source.Source.Table
			body["source"] = cloneRow(source.Source.Fields)
			if kind == "beads-archive" {
				body["read_only"] = true
				body["redactable"] = true
			}
			if err := add(model.RefByID("source/"+kind+"/"+source.ID, model.Natural64(0)), kind, body); err != nil {
				return restorePlan{}, err
			}
		}
	}

	// Every bead write needs its exact predicate bump. The graph sentinel is
	// always included so the restore transaction is serialized with blocking
	// relation maintenance, even when this particular archive has no block edge.
	plan.sentinels = append(plan.sentinels, driver.SentinelGraphBlocking)
	for _, source := range beads {
		sentinel, err := driver.NewBeadSentinel(source.ID)
		if err != nil {
			return restorePlan{}, err
		}
		plan.sentinels = append(plan.sentinels, sentinel)
	}
	sort.Slice(plan.records, func(i, j int) bool {
		return plan.records[i].ref.ID < plan.records[j].ref.ID
	})
	sort.Slice(plan.sentinels, func(i, j int) bool { return string(plan.sentinels[i]) < string(plan.sentinels[j]) })
	return plan, nil
}

func lockRequests(existing []driver.Record, plan restorePlan) []driver.LockRequest {
	keys := map[string]driver.LockKey{}
	for _, record := range existing {
		ref := record.Ref
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	for _, record := range plan.records {
		ref := record.ref
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	for _, sentinel := range plan.sentinels {
		key := driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	ordered := make([]string, 0, len(keys))
	for encoded := range keys {
		ordered = append(ordered, encoded)
	}
	sort.Strings(ordered)
	requests := make([]driver.LockRequest, 0, len(ordered))
	for _, encoded := range ordered {
		requests = append(requests, driver.LockRequest{Key: keys[encoded], Mode: driver.LockUpdate})
	}
	return requests
}

func lockKeys(requests []driver.LockRequest) []driver.LockKey {
	keys := make([]driver.LockKey, len(requests))
	for i, request := range requests {
		keys[i] = request.Key
	}
	return keys
}

func allRecords(ctx context.Context, reader driver.Reader) ([]driver.Record, error) {
	var records []driver.Record
	var cursor *driver.Cursor
	for {
		page, err := reader.Query(ctx, driver.Query{Cursor: cursor, Limit: 1000})
		if err != nil {
			return nil, err
		}
		records = append(records, page.Items...)
		if page.NextCursor == nil {
			return records, nil
		}
		if len(page.Items) == 0 {
			return nil, fmt.Errorf("target query did not advance")
		}
		cursor = page.NextCursor
	}
}

func targetStatus(source string) (string, bool) {
	switch source {
	case "open":
		return "todo", true
	case "in_progress", "hooked":
		return "in_progress", true
	case "blocked", "deferred", "pinned", "frozen":
		return "backlog", true
	case "closed":
		return "done", true
	default:
		return "", false
	}
}

func sourceTime(value any) (time.Time, error) {
	text, _ := value.(string)
	if text == "" {
		return time.Time{}, nil
	}
	stamp, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid source timestamp %q", text)
	}
	return stamp.UTC(), nil
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func cloneRow(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = cloneValue(value)
	}
	return output
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneRow(value)
	case []any:
		output := make([]any, len(value))
		for i, item := range value {
			output[i] = cloneValue(item)
		}
		return output
	default:
		return value
	}
}
