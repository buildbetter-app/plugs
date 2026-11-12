package nativeprojection

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/recordimage"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type DerivedImage struct {
	Table driver.DerivedTable `json:"table"`
	Key   string              `json:"key"`
	Value json.RawMessage     `json:"value"`
}

// ImportedOntologySourceID is the protected, branch-zero ontology source
// installed only by an explicitly declared native status-category import.
// A composed host can select it using service.StaticOntologySource.
const ImportedOntologySourceID = "ontology/beads-import"

// These profiles admit the built-in ontology or the validated strict-union
// import document, with no authorization/plugin provider, cuts, templates or leases.
// Every table participates in the digest, including empty tables. Its rows
// are rebuilt from live documents, never copied from a source's cached flags.
func rebuildDerived(plan *restorePlan, docs map[string]recordimage.Document, archives ...*dumpformat.Archive) error {
	var archive *dumpformat.Archive
	if len(archives) != 0 {
		archive = archives[0]
	}
	scratch, release := archive.NativeDeltaAccountingScope()
	defer release()
	ids := make([]string, 0, len(docs))
	for id := range docs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	visiting, done := map[string]bool{}, map[string]bool{}
	var walk func(string) error
	walk = func(id string) error {
		if visiting[id] {
			return fmt.Errorf("restore blocking graph contains a cycle")
		}
		if done[id] {
			return nil
		}
		visiting[id] = true
		if parent := docs[id].Parent; parent != nil {
			if _, exists := docs[parent.ID]; !exists {
				return fmt.Errorf("restore parent has no restored endpoint")
			}
			if err := walk(parent.ID); err != nil {
				return err
			}
		}
		for _, link := range docs[id].Links {
			if link.TargetKind != model.LinkTargetExternal && (link.Kind == "blocks" || link.Kind == "conditional-blocks") {
				if err := walk(link.To.ID); err != nil {
					return err
				}
			}
		}
		delete(visiting, id)
		done[id] = true
		return nil
	}
	for _, id := range ids {
		if err := walk(id); err != nil {
			return err
		}
	}
	// Use the live Writer's injective row codec. Aggregate arrays keyed by
	// descendant cannot be read or guarded by ordinary hierarchy operations.
	ancestors := make(map[string]map[string]uint64, len(docs))
	for _, id := range ids {
		depths := map[string]uint64{}
		for current, depth := id, uint64(0); current != ""; depth++ {
			depths[current] = depth
			// Inventory the live codec's input, encoded buffer/string, prefix
			// and decimal candidates separately from its returned key/value.
			descendantBytes := uint64(base64.RawURLEncoding.EncodedLen(len(id)))
			ancestorBytes := uint64(base64.RawURLEncoding.EncodedLen(len(current)))
			depthBytes := uint64(1)
			for n := depth; n >= 10; n /= 10 {
				depthBytes++
			}
			components := []struct {
				phase string
				size  uint64
			}{
				{"ancestor_descendant_input", uint64(len(id))},
				{"ancestor_descendant_base64_buffer", descendantBytes},
				{"ancestor_descendant_base64_text", descendantBytes},
				{"ancestor_prefix", descendantBytes + 1},
				{"ancestor_ancestor_input", uint64(len(current))},
				{"ancestor_ancestor_base64_buffer", ancestorBytes},
				{"ancestor_ancestor_base64_text", ancestorBytes},
				{"ancestor_depth_text", depthBytes},
			}
			for _, component := range components {
				if err := scratch.ChargeNativeDeltaBytes(component.phase, component.size); err != nil {
					return err
				}
			}
			if err := archive.ChargeNativeDeltaBytes("derived_ancestor_keys", descendantBytes+1+ancestorBytes); err != nil {
				return err
			}
			if err := archive.ChargeNativeDeltaBytes("derived_ancestor_values", depthBytes); err != nil {
				return err
			}
			row, err := recordimage.AncestorRow(id, current, depth)
			if err != nil {
				return err
			}
			plan.derived = append(plan.derived, DerivedImage{driver.DerivedAncestorClosure, row.Key, row.Value})
			for _, component := range components {
				scratch.ReleaseNativeDeltaBytes(component.phase, component.size)
			}
			parent := docs[current].Parent
			if parent == nil {
				break
			}
			current = parent.ID
		}
		ancestors[id] = depths
	}
	for id, doc := range docs {
		for _, link := range doc.Links {
			if link.TargetKind != model.LinkTargetExternal && (link.Kind == "blocks" || link.Kind == "conditional-blocks") && (ancestors[id][link.To.ID] > 0 || ancestors[link.To.ID][id] > 0) {
				return fmt.Errorf("restore blocking link cannot join an ancestor and descendant")
			}
		}
	}
	add := func(table driver.DerivedTable, key string, value any) error {
		data, err := MarshalNativePayload(archive, "derived_row_bodies", value)
		if err != nil {
			return err
		}
		plan.derived = append(plan.derived, DerivedImage{table, key, data})
		return nil
	}
	ontologyDocument := ontology.DefaultDocument()
	if plan.migration != nil {
		ontologyDocument = plan.migration.Document()
	}
	var overlays map[string]ontology.Document
	var ontologyImage any = ontologyDocument
	if plan.nativeOntology != nil {
		ontologyDocument, overlays = plan.nativeOntology.Base, plan.nativeOntology.Overlays
		ontologyImage = plan.nativeOntology
	}
	snapshot, problems := ontology.Compile(ontologyDocument, overlays, nil)
	if len(problems) != 0 {
		return fmt.Errorf("invalid admitted restore ontology: %v", problems)
	}
	for _, id := range ids {
		doc := docs[id]
		effective := snapshot.Effective(doc.Project)
		if _, known := effective.Status(StringValue(doc.Fields["status"])); !known {
			return fmt.Errorf("restore bead %s has an undeclared status in project %s", id, doc.Project)
		}
		blocked, unresolvedScheduling := false, false
		for _, link := range doc.Links {
			if link.TargetKind == model.LinkTargetExternal {
				blocked = blocked || link.Kind == "blocks" || link.Kind == "conditional-blocks" || link.Kind == "waits-for"
				unresolvedScheduling = unresolvedScheduling || link.Kind == "parent-child"
				continue
			}
			blocker := docs[link.To.ID]
			blockerOntology := snapshot.Effective(blocker.Project)
			switch link.Kind {
			case "blocks":
				blocked = blocked || !blockerOntology.SatisfiesBlocks(StringValue(blocker.Fields["status"]))
			case "conditional-blocks":
				blocked = blocked || !blockerOntology.SatisfiesConditionalBlocks(StringValue(blocker.Fields["status"]))
			case "waits-for":
				blocked = blocked || !importedWaitsForSatisfied(link, blocker, docs, snapshot)
			}
		}
		priority, violations := ontology.Priority(ontology.BeadState{Fields: doc.Fields})
		if len(violations) != 0 {
			return fmt.Errorf("invalid restore priority for %s", id)
		}
		links := append([]recordimage.Link{}, doc.Links...)
		sort.Slice(links, func(i, j int) bool {
			if links[i].Kind != links[j].Kind {
				return links[i].Kind < links[j].Kind
			}
			if links[i].TargetKind != links[j].TargetKind {
				return links[i].TargetKind < links[j].TargetKind
			}
			return links[i].To.ID < links[j].To.ID
		})
		for _, row := range []struct {
			table driver.DerivedTable
			value any
		}{
			{driver.DerivedReadyState, map[string]bool{"blocked": blocked, "ready": effective.ReadyEligible(doc.Type, StringValue(doc.Fields["status"])) && !blocked && !unresolvedScheduling}},
			{driver.DerivedCutClosure, []string{}},
			{driver.DerivedAttribute, map[string]any{}},
			{driver.DerivedSortKey, map[string]any{"priority": priority, "id": id}},
			{driver.DerivedReferenceIndex, links},
		} {
			if err := add(row.table, id, row.value); err != nil {
				return err
			}
		}
		ready, err := driver.NewReadySentinel(doc.Project, doc.Type)
		if err != nil {
			return err
		}
		closure, err := driver.NewClosureSentinel(id)
		if err != nil {
			return err
		}
		plan.sentinels = append(plan.sentinels, ready, closure)
	}
	if err := add(driver.DerivedOntology, "builtin", ontologyImage); err != nil {
		return err
	}
	if err := add(driver.DerivedPlugins, "builtin", []string{}); err != nil {
		return err
	}
	plan.sentinels = append(plan.sentinels, driver.SentinelOntology, driver.SentinelPlugins)
	sort.Slice(plan.derived, func(i, j int) bool {
		a, b := plan.derived[i], plan.derived[j]
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		return a.Key < b.Key
	})
	data, err := MarshalNativePayload(scratch, "derived_digest_preimage", struct {
		Profile string                `json:"profile"`
		Tables  []driver.DerivedTable `json:"tables"`
		Rows    []DerivedImage        `json:"rows"`
	}{derivedProfile(plan), []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey}, plan.derived})
	if err != nil {
		return err
	}
	plan.derivedDigest = HashBytes(data)
	return nil
}

// Use the same waits predicate as the live ontology: absent metadata keeps
// the terminal-spawner fallback; explicit metadata gates on direct children.
// No other dependency kind, including tracks and local opaque kinds, enters it.
func importedWaitsForSatisfied(link recordimage.Link, spawner recordimage.Document, docs map[string]recordimage.Document, snapshot ontology.Snapshot) bool {
	terminal := func(doc recordimage.Document) bool {
		return snapshot.Effective(doc.Project).SatisfiesBlocks(StringValue(doc.Fields["status"]))
	}
	if !link.Metadata.HasPredicate() && terminal(spawner) {
		return true
	}
	if link.Metadata != nil && link.Metadata.AlsoBlocks && !terminal(spawner) {
		return false
	}
	openChild, terminalChild := false, false
	for _, child := range docs {
		if child.Parent == nil || child.Parent.ID != spawner.ID {
			continue
		}
		if terminal(child) {
			terminalChild = true
		} else {
			openChild = true
		}
	}
	return !openChild || link.Metadata != nil && link.Metadata.Gate == "any-children" && terminalChild
}

// The strict profile binds the admitted document in the same canonical image
// encoding. Existing built-in digests and native restore profiles are unchanged.
// The reverse adapter's existing profile guard remains closed to this profile.
func derivedProfile(plan *restorePlan) string {
	if plan.nativeOntology != nil {
		return "beads-native-status-categories/1"
	}
	if plan.migration != nil {
		return "beads-compat-union-head/1"
	}
	return "builtin-head/1"
}

type ArchiveEvidence struct {
	ArchiveSHA256           string                     `json:"archive_sha256"`
	DerivedSHA256           string                     `json:"derived_sha256"`
	Profile                 string                     `json:"profile"`
	SourceStore             string                     `json:"source_store"`
	Members                 map[string]dumpformat.File `json:"members"`
	NativeCaptureFiles      map[string]dumpformat.File `json:"native_capture_files,omitempty"`
	NativeDecodedLimitBytes uint64                     `json:"native_decoded_limit_bytes,omitempty"`
}

func retainArchive(plan *restorePlan, archive *dumpformat.Archive, add func(model.Ref, string, any) error) error {
	if archive.HasNativeCaptureCollection() {
		files, err := archive.NativeCollectionFiles()
		if err != nil {
			return err
		}
		usage, _ := archive.NativeDecodedUsage()
		manifest := archive.Manifest()
		evidence := ArchiveEvidence{ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest, Profile: derivedProfile(plan), SourceStore: manifest.StoreID, Members: manifest.Files, NativeCaptureFiles: files, NativeDecodedLimitBytes: usage.LimitBytes}
		plan.blobs = map[string][]byte{}
		commitment, err := archive.NativeCollectionCommitment()
		if err != nil {
			return err
		}
		plan.blobs[archive.SHA256()] = commitment
		for name, expected := range manifest.Files {
			raw, ok, err := archive.MemberChecked(name)
			if err != nil {
				return err
			}
			if !ok || HashBytes(raw) != expected.SHA256 || int64(len(raw)) != expected.Size {
				return fmt.Errorf("collection current member differs")
			}
			plan.blobs[expected.SHA256] = raw
		}
		plan.nativeCapture = archive
		return add(model.RefByID("restore/archive", model.Natural64(0)), "restore-archive", evidence)
	}
	if archive.HasNativeDelta() {
		return retainNativeDeltaArchive(plan, archive, add)
	}
	manifest := archive.Manifest()
	evidence := ArchiveEvidence{ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest, Profile: derivedProfile(plan), SourceStore: manifest.StoreID, Members: manifest.Files}
	raw, ok, err := archive.MemberChecked("manifest.json")
	if err != nil {
		return err
	}
	if !ok || !ValidDigest(archive.SHA256()) {
		return fmt.Errorf("archive was not validated")
	}
	evidence.Members["manifest.json"] = dumpformat.File{SHA256: HashBytes(raw), Size: int64(len(raw))}
	plan.blobs = map[string][]byte{}
	envelope, err := archive.TarBytesChecked()
	if err != nil {
		return err
	}
	if len(envelope) != 0 {
		if HashBytes(envelope) != archive.SHA256() {
			return fmt.Errorf("archive envelope changed")
		}
		plan.blobs[archive.SHA256()] = envelope
	}
	for name, expected := range evidence.Members {
		data, ok, err := archive.MemberChecked(name)
		if err != nil {
			return err
		}
		if !ok || HashBytes(data) != expected.SHA256 || int64(len(data)) != expected.Size {
			return fmt.Errorf("archive member changed: %s", name)
		}
		plan.blobs[expected.SHA256] = data
	}
	return add(model.RefByID("restore/archive", model.Natural64(0)), "restore-archive", evidence)
}

func HashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func ValidDigest(value string) bool {
	data, err := hex.DecodeString(value)
	return err == nil && len(data) == 32 && hex.EncodeToString(data) == value
}
