package converterroundtrip

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type derivedImage struct {
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
func rebuildDerived(plan *restorePlan, docs map[string]pipeline.Document) error {
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
			if link.TargetKind != model.LinkTargetExternal && restoreSchedulingLink(link.Kind) {
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
			row, err := pipeline.AncestorRow(id, current, depth)
			if err != nil {
				return err
			}
			plan.derived = append(plan.derived, derivedImage{driver.DerivedAncestorClosure, row.Key, row.Value})
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
			if link.TargetKind != model.LinkTargetExternal && restoreBlockingLink(link.Kind) && (ancestors[id][link.To.ID] > 0 || ancestors[link.To.ID][id] > 0) {
				return fmt.Errorf("restore blocking link cannot join an ancestor and descendant")
			}
		}
	}
	add := func(table driver.DerivedTable, key string, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		plan.derived = append(plan.derived, derivedImage{table, key, data})
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
		if _, known := effective.Status(stringValue(doc.Fields["status"])); !known {
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
				blocked = blocked || !blockerOntology.SatisfiesBlocks(stringValue(blocker.Fields["status"]))
			case "conditional-blocks":
				blocked = blocked || !blockerOntology.SatisfiesConditionalBlocks(stringValue(blocker.Fields["status"]))
			case "waits-for":
				blocked = blocked || !importedWaitsForSatisfied(link, blocker, docs, snapshot)
			}
		}
		priority, violations := ontology.Priority(ontology.BeadState{Fields: doc.Fields})
		if len(violations) != 0 {
			return fmt.Errorf("invalid restore priority for %s", id)
		}
		ancestorsReady, err := restoreAncestorsReady(snapshot, docs, doc)
		if err != nil {
			return err
		}
		links := append([]pipeline.Link{}, doc.Links...)
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
			{driver.DerivedReadyState, map[string]bool{"blocked": blocked, "ready": effective.ReadyEligible(doc.Type, stringValue(doc.Fields["status"])) && !blocked && !unresolvedScheduling && ancestorsReady}},
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
	data, err := json.Marshal(struct {
		Profile string                `json:"profile"`
		Tables  []driver.DerivedTable `json:"tables"`
		Rows    []derivedImage        `json:"rows"`
	}{derivedProfile(plan), []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey}, plan.derived})
	if err != nil {
		return err
	}
	plan.derivedDigest = hashBytes(data)
	return nil
}

// Use the same waits predicate as the live ontology: absent metadata keeps
// the terminal-spawner fallback; explicit metadata gates on direct children.
// No other dependency kind, including tracks and local opaque kinds, enters it.
func importedWaitsForSatisfied(link pipeline.Link, spawner pipeline.Document, docs map[string]pipeline.Document, snapshot ontology.Snapshot) bool {
	terminal := func(doc pipeline.Document) bool {
		return snapshot.Effective(doc.Project).SatisfiesBlocks(stringValue(doc.Fields["status"]))
	}
	if link.Metadata == nil && terminal(spawner) {
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

// Match the canonical pipeline graph topology: waits-for expands readiness
// through the spawner's children, but does not add an ancestor/cycle edge.
func restoreSchedulingLink(kind string) bool {
	return kind == "blocks" || kind == "conditional-blocks" || kind == "parent-child"
}

func restoreBlockingLink(kind string) bool {
	return kind == "blocks" || kind == "conditional-blocks" || kind == "waits-for"
}

// Ancestors suppress scheduling without becoming the child's direct blockers.
// This is the same distinction used by the live ready/claim guard: backlog or
// an unsatisfied ancestor dependency suppresses every descendant, while an
// ordinary open parent does not. Each ancestor uses its own project ontology.
func restoreAncestorsReady(snapshot ontology.Snapshot, docs map[string]pipeline.Document, doc pipeline.Document) (bool, error) {
	seen := map[string]bool{doc.ID: true}
	for {
		parent, err := pipeline.HierarchyParent(doc)
		if err != nil {
			return false, err
		}
		if parent == nil {
			return true, nil
		}
		ancestor, exists := docs[parent.ID]
		if !exists || seen[parent.ID] {
			return false, nil
		}
		category, known := snapshot.Effective(ancestor.Project).Category(stringValue(ancestor.Fields["status"]))
		if !known || category == ontology.Backlog {
			return false, nil
		}
		for _, link := range ancestor.Links {
			if restoreBlockingLink(link.Kind) && !restoreLinkSatisfied(snapshot, docs, link) {
				return false, nil
			}
		}
		seen[parent.ID] = true
		doc = ancestor
	}
}

// The complete admitted local image is the independent source for every child
// predicate. Use each endpoint's ontology and the same nil/explicit metadata
// distinction as the ordinary service's canonical waits-for evaluator.
func restoreLinkSatisfied(snapshot ontology.Snapshot, docs map[string]pipeline.Document, link pipeline.Link) bool {
	if link.TargetKind == model.LinkTargetExternal {
		return false
	}
	blocker, exists := docs[link.To.ID]
	if !exists || !link.To.IsID() || !link.To.Branch.IsZero() {
		return false
	}
	terminal := func(doc pipeline.Document) bool {
		return snapshot.Effective(doc.Project).SatisfiesBlocks(stringValue(doc.Fields["status"]))
	}
	switch link.Kind {
	case "blocks":
		return terminal(blocker)
	case "conditional-blocks":
		return snapshot.Effective(blocker.Project).SatisfiesConditionalBlocks(stringValue(blocker.Fields["status"]))
	case "waits-for":
		if link.Metadata == nil && terminal(blocker) {
			return true
		}
		if link.Metadata != nil && link.Metadata.AlsoBlocks && !terminal(blocker) {
			return false
		}
		openChild, terminalChild := false, false
		for _, child := range docs {
			member := child.Parent != nil && child.Parent.ID == blocker.ID
			for _, edge := range child.Links {
				member = member || edge.TargetKind != model.LinkTargetExternal && edge.Kind == "parent-child" && edge.To.ID == blocker.ID
			}
			if member {
				if terminal(child) {
					terminalChild = true
				} else {
					openChild = true
				}
			}
		}
		return !openChild || link.Metadata != nil && link.Metadata.Gate == "any-children" && terminalChild
	}
	return true
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

// ImportedOntologySourceRef identifies the protected source produced only by
// an admitted strict-union configuration migration with actual declarations.
// It is a locator, not a promise that a legacy/default-only import has a source.
func ImportedOntologySourceRef() model.Ref {
	return model.RefByID("ontology/imported", model.Natural64(0))
}

// retainImportedOntology publishes the already validated native configuration
// document in the SAME import transaction as its original archive and receipt.
// A derived builtin or absent custom configuration is not source authority.
// The driver assigns the source's actual committed revision; original source
// declarations and member hashes remain independently available as lineage.
func retainImportedOntology(plan *restorePlan, archive *dumpformat.Archive, add func(model.Ref, string, any) error) error {
	if plan.migration == nil || len(plan.migration.Declarations()) == 0 {
		return nil
	}
	source := struct {
		Base ontology.Document `json:"base"`
	}{Base: plan.migration.Document()}
	compiled, problems := ontology.Compile(source.Base, nil, nil)
	if len(problems) != 0 {
		return fmt.Errorf("admitted imported ontology no longer compiles: %v", problems)
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	ref := ImportedOntologySourceRef()
	if err := add(ref, "ontology", source); err != nil {
		return err
	}
	manifest := archive.Manifest()
	evidence := struct {
		Source         model.Ref                      `json:"source"`
		BodySHA256     string                         `json:"body_sha256"`
		CompiledDigest string                         `json:"compiled_digest"`
		SourceStore    string                         `json:"source_store"`
		ArchiveSHA256  string                         `json:"archive_sha256"`
		DerivedSHA256  string                         `json:"derived_sha256"`
		Profile        string                         `json:"profile"`
		Declarations   []dumpformat.ConfigDeclaration `json:"declarations"`
		Members        map[string]dumpformat.File     `json:"members"`
	}{ref, hashBytes(raw), compiled.Digest(), manifest.StoreID, archive.SHA256(), plan.derivedDigest, derivedProfile(plan), plan.migration.Declarations(), manifest.Files}
	return add(model.RefByID("restore/ontology-source", model.Natural64(0)), "instance_config", evidence)
}

type archiveEvidence struct {
	ArchiveSHA256 string                     `json:"archive_sha256"`
	DerivedSHA256 string                     `json:"derived_sha256"`
	Profile       string                     `json:"profile"`
	SourceStore   string                     `json:"source_store"`
	Members       map[string]dumpformat.File `json:"members"`
}

func retainArchive(plan *restorePlan, archive *dumpformat.Archive, add func(model.Ref, string, any) error) error {
	manifest := archive.Manifest()
	evidence := archiveEvidence{ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest, Profile: derivedProfile(plan), SourceStore: manifest.StoreID, Members: manifest.Files}
	raw, ok := archive.Member("manifest.json")
	if !ok || !validDigest(archive.SHA256()) {
		return fmt.Errorf("archive was not validated")
	}
	evidence.Members["manifest.json"] = dumpformat.File{SHA256: hashBytes(raw), Size: int64(len(raw))}
	plan.blobs = map[string][]byte{}
	if envelope := archive.TarBytes(); len(envelope) != 0 {
		if hashBytes(envelope) != archive.SHA256() {
			return fmt.Errorf("archive envelope changed")
		}
		plan.blobs[archive.SHA256()] = envelope
	}
	for name, expected := range evidence.Members {
		data, ok := archive.Member(name)
		if !ok || hashBytes(data) != expected.SHA256 || int64(len(data)) != expected.Size {
			return fmt.Errorf("archive member changed: %s", name)
		}
		plan.blobs[expected.SHA256] = data
	}
	return add(model.RefByID("restore/archive", model.Natural64(0)), "restore-archive", evidence)
}

func hashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validDigest(value string) bool {
	data, err := hex.DecodeString(value)
	return err == nil && len(data) == 32 && hex.EncodeToString(data) == value
}
