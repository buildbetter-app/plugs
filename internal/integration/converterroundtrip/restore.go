// Package converterroundtrip contains the bounded, offline Beads-to-Plug
// restore seam. The archive validator owns the source trust boundary; this
// package only lowers its already validated records into an empty Plug store.
package converterroundtrip

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var (
	// ErrRestoreUnavailable is retained for source compatibility with the
	// parser-only checkpoint. Unsupported semantics and absent verification
	// refuse before opening a write transaction.
	ErrRestoreUnavailable = errors.New("trusted offline restore seam unavailable")
	ErrTargetNotEmpty     = driver.ErrRestoreTargetNotEmpty
)

// Report retains the complete parsed source evidence and the target metadata
// observed before the restore transaction. Target is intentionally the
// pre-restore snapshot so refusal reports cannot be mistaken for a commit.
type Report struct {
	Archive        *dumpformat.Archive
	Target         driver.Meta
	Restored       driver.Meta
	IdempotencyKey string
	RequestHash    string
	Recovered      bool
}

// Verification is a trusted offline comparison, bound to the exact tar bytes.
// The Beads exporter has no derived certificate. Callers must independently
// verify this bounded profile's derived digest; omission fails before writes.
// This does not assert source history, A3, authorization or migration parity.
type Verification struct {
	ArchiveSHA256 string
	DerivedSHA256 string
}

// Restore validates the complete archive and restores it into an empty store.
// The wrapper obtains the store lease itself; callers that already own the
// lease (including integration tests) can use RestoreWithLease.
func Restore(ctx context.Context, input io.Reader, target driver.Store, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.Read(input)
	if err != nil {
		return Report{}, err
	}
	return restoreArchive(ctx, archive, target, nil, verification)
}

// RestoreWithLease is the same restore operation for a caller that already
// holds the target's active lease. It is deliberately not a second restore
// implementation: both entry points run the same bounded transaction.
func RestoreWithLease(ctx context.Context, input io.Reader, target driver.Store, lease driver.ActiveLease, verification ...Verification) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	archive, err := dumpformat.Read(input)
	if err != nil {
		return Report{}, err
	}
	return restoreArchive(ctx, archive, target, lease, verification)
}

func restoreArchive(ctx context.Context, archive *dumpformat.Archive, target driver.Store, supplied driver.ActiveLease, checks []Verification) (Report, error) {
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

	// Complete semantic validation and digest comparison precede lease/write
	// acquisition. A source archive alone is never proof of usable live state.
	plan, err := buildPlan(archive)
	if err != nil {
		return report, err
	}
	if len(checks) != 1 || checks[0].ArchiveSHA256 != archive.SHA256() || checks[0].DerivedSHA256 != plan.derivedDigest {
		return report, fmt.Errorf("%w: exact archive and rebuilt derived digest verification required", ErrRestoreUnavailable)
	}
	report.IdempotencyKey, report.RequestHash = recoveryScope(checks[0])

	lease := supplied
	if lease == nil {
		lease, err = target.Lease(ctx, "plug-converter-restore", driver.LeaseTTL)
		if err != nil {
			return report, err
		}
		defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	}
	requests := lockRequests(plan)
	tx, err := target.Write(ctx, lease)
	if err != nil {
		return report, err
	}
	defer tx.Close()
	capability, ok := tx.(driver.OfflineRestoreTx)
	if !ok {
		return report, driver.ErrUnsupported
	}
	if _, err = tx.Lock(ctx, requests); err != nil {
		return report, err
	}
	// Head is the global writer fence, acquired last. Empty-target validation
	// happens AFTER it; same-lease writers cannot slip a write between the check
	// and publication. The driver repeats the full check at atomic commit.
	if _, err = tx.Head(ctx); err != nil {
		return report, err
	}
	restored, err := capability.PrepareOfflineRestore(ctx, driver.StoreID(archive.Manifest().StoreID))
	if err != nil {
		return report, err
	}

	changes := make([]model.ChangeRecord, 0, len(plan.records))
	results := make([]model.OpResult, 0, len(plan.records))
	for _, record := range plan.records {
		if err = tx.Put(ctx, driver.Record{Ref: record.ref, Kind: record.kind, Body: record.body}); err != nil {
			return report, err
		}
		changes = append(changes, model.ChangeRecord{Ref: record.ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}})
		results = append(results, model.OpResult{Ref: record.ref, Revision: model.Natural64(1), Record: append([]byte(nil), record.body...)})
	}
	for _, item := range plan.derived {
		if err = tx.PutDerived(ctx, item.Table, nil, []driver.DerivedRow{{Key: item.Key, Value: item.Value}}); err != nil {
			return report, err
		}
	}
	for hash, data := range plan.blobs {
		actual, putErr := tx.PutBlob(ctx, data)
		if putErr != nil {
			return report, putErr
		}
		if string(actual) != hash {
			return report, fmt.Errorf("restore blob digest mismatch")
		}
	}
	if err = tx.BumpSentinels(ctx, plan.sentinels); err != nil {
		return report, err
	}
	commit, err := tx.Commit(ctx, driver.ChangesetHeader{
		Incarnation: restored.Incarnation, Kind: model.ChangesetRestore,
		Time: time.Now().UTC(), Principal: restorePrincipal, Chain: []string{},
		Method: "offline.restore", IdempotencyKey: report.IdempotencyKey, RequestHash: report.RequestHash,
		Response: model.Commit{Revision: model.Natural64(1), Results: results, Warnings: []model.Warning{}},
		Changes:  changes, LockKeys: lockKeys(requests),
	})
	if err != nil {
		var uncertain driver.ErrIndeterminate
		if !errors.As(err, &uncertain) || uncertain.IdempotencyKey != report.IdempotencyKey {
			return report, err
		}
		// Resolve only a matching committed receipt, never replay the restore.
		recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		recovered, lookupErr := Recover(recoveryCtx, target, checks[0])
		cancel()
		if lookupErr != nil {
			return report, err
		}
		report.Restored, report.Recovered = recovered.Restored, true
		return report, nil
	}
	if commit.Kind != model.ChangesetRestore || commit.Incarnation != restored.Incarnation || len(commit.Changes) != len(plan.records) {
		return report, fmt.Errorf("restore commit did not retain its complete changeset")
	}
	report.Restored = restored
	return report, nil
}

var restorePrincipal = model.PrincipalRef{Kind: "system", ID: "plug-converter"}

func recoveryScope(check Verification) (string, string) {
	return "restore/" + check.ArchiveSHA256, hashBytes([]byte("beads-head/1\x00" + check.ArchiveSHA256 + "\x00" + check.DerivedSHA256))
}

// Recover is read-only and can run after restart or lease takeover. A normal
// Restore retry still refuses a populated target, including a prior restore.
func Recover(ctx context.Context, target driver.Store, check Verification) (Report, error) {
	if !validDigest(check.ArchiveSHA256) || !validDigest(check.DerivedSHA256) {
		return Report{}, driver.ErrConflict
	}
	key, request := recoveryScope(check)
	meta, err := target.Meta(ctx)
	if err != nil {
		return Report{}, err
	}
	r, err := target.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		return Report{}, err
	}
	defer r.Close()
	principal, _ := driver.PrincipalKey(restorePrincipal)
	receipt, err := r.IdempotencyByKey(ctx, principal, "", "offline.restore", key)
	if err != nil {
		return Report{}, err
	}
	if receipt == nil || receipt.RequestHash != request || receipt.ResponsePending || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Incarnation != meta.Incarnation || receipt.Changeset.Revision.String() != "1" || receipt.Commit.Revision.String() != "1" || receipt.Changeset.IdempotencyKey != key {
		return Report{}, driver.ErrConflict
	}
	// Bind recovery to the durable archive marker as well as the receipt.
	rows, err := r.Get(ctx, []model.Ref{model.RefByID("restore/archive", model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		return Report{}, driver.ErrConflict
	}
	var evidence archiveEvidence
	if err = json.Unmarshal(rows[0].Body, &evidence); err != nil || evidence.ArchiveSHA256 != check.ArchiveSHA256 || evidence.DerivedSHA256 != check.DerivedSHA256 {
		return Report{}, driver.ErrConflict
	}
	return Report{Restored: meta, IdempotencyKey: key, RequestHash: request, Recovered: true}, nil
}

type restoreRecord struct {
	ref  model.Ref
	kind string
	body []byte
}

type restorePlan struct {
	records       []restoreRecord
	sentinels     []driver.Sentinel
	derived       []derivedImage
	derivedDigest string
	blobs         map[string][]byte
}

func buildPlan(archive *dumpformat.Archive) (restorePlan, error) {
	manifest := archive.Manifest()
	if manifest.History != "unavailable" {
		return restorePlan{}, fmt.Errorf("%w: unverified source history cannot be restored", ErrRestoreUnavailable)
	}
	if len(archive.Records("comment")) != 0 {
		return restorePlan{}, fmt.Errorf("%w: unverified comment attribution", ErrRestoreUnavailable)
	}
	projects := map[string]dumpformat.Project{}
	for _, project := range manifest.Projects {
		if project.IDMode != "hash" || project.Prefix != "pl" || project.HashLength != 32 || project.LastID != 0 || len(project.ChildCounters) != 0 {
			return restorePlan{}, fmt.Errorf("%w: allocator continuation is unsupported for %s", ErrRestoreUnavailable, project.Prefix)
		}
		projects[project.Prefix] = project
	}

	beads := archive.Records("bead")
	docs := map[string]pipeline.Document{}
	if len(beads) > 20000 {
		return restorePlan{}, fmt.Errorf("restore graph exceeds local service limit")
	}
	for _, source := range beads {
		if strings.Contains(source.ID, ".") {
			return restorePlan{}, fmt.Errorf("%w: child allocator", ErrRestoreUnavailable)
		}
		suffix := strings.TrimPrefix(source.ID, "pl-")
		identity, err := hex.DecodeString(suffix)
		if err != nil || len(identity) != 16 || hex.EncodeToString(identity) != suffix {
			return restorePlan{}, fmt.Errorf("%w: source ID does not match the live hash allocator", ErrRestoreUnavailable)
		}
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
				if strings.HasPrefix(key, "beads.") || strings.HasPrefix(key, "plug.restore.") {
					return restorePlan{}, fmt.Errorf("reserved restore metadata key %q", key)
				}
				encoded, err := json.Marshal(value)
				if err != nil {
					return restorePlan{}, fmt.Errorf("bead %s metadata: %w", source.ID, err)
				}
				metadata[key] = encoded
			}
		}
		for key, value := range source.Body {
			switch key {
			case "title", "description", "priority", "metadata", "labels":
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
		labels := []string{}
		for _, value := range source.Body["labels"].([]any) {
			labels = append(labels, value.(string))
		}
		docs[source.ID] = pipeline.Document{Labels: labels, ID: source.ID, Type: stringValue(source.Body["type"]), Project: source.Project, Fields: fields, Metadata: metadata}
	}

	for _, source := range archive.Records("edge") {
		if stringValue(source.Body["target_kind"]) != "depends_on_issue_id" {
			return restorePlan{}, fmt.Errorf("%w: external relation %s", ErrRestoreUnavailable, source.ID)
		}
		from, to := stringValue(source.Body["from"]), stringValue(source.Body["to"])
		doc, ok := docs[from]
		if _, targetExists := docs[to]; !ok || !targetExists {
			return restorePlan{}, fmt.Errorf("edge %s has no restored endpoint", source.ID)
		}
		kind := stringValue(source.Body["type"])
		switch kind {
		case "blocks":
		case "related":
			kind = "relates-to"
		default:
			return restorePlan{}, fmt.Errorf("%w: relation type %s", ErrRestoreUnavailable, kind)
		}
		doc.Links = append(doc.Links, pipeline.Link{To: model.RefByID(to, model.Natural64(0)), Kind: kind})
		docs[from] = doc
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

	if err := rebuildDerived(&plan, docs); err != nil {
		return restorePlan{}, err
	}
	if err := retainArchive(&plan, archive, add); err != nil {
		return restorePlan{}, err
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

func lockRequests(plan restorePlan) []driver.LockRequest {
	keys := map[string]driver.LockKey{}
	for _, record := range plan.records {
		ref := record.ref
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		encoded, _ := driver.EncodeLockKey(key)
		keys[encoded] = key
	}
	for _, item := range plan.derived {
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: item.Table, Key: item.Key}}
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
