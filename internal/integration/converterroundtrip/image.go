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

// The derived row key is the descendant ID; each entry identifies an ancestor
// and its distance. The parent-free restore profile still requires self.
type ancestorMembership struct {
	Ancestor string `json:"ancestor"`
	Depth    uint64 `json:"depth"`
}

// This profile is deliberately restricted to the built-in ontology, no
// authorization/plugin provider, no parents/cuts, no templates and no leases.
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
		for _, link := range docs[id].Links {
			if link.Kind == "blocks" {
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
	add := func(table driver.DerivedTable, key string, value any) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		plan.derived = append(plan.derived, derivedImage{table, key, data})
		return nil
	}
	effective := ontology.Default()
	for _, id := range ids {
		doc := docs[id]
		blocked := false
		for _, link := range doc.Links {
			if link.Kind == "blocks" && !effective.SatisfiesBlocks(stringValue(docs[link.To.ID].Fields["status"])) {
				blocked = true
			}
		}
		priority, violations := ontology.Priority(ontology.BeadState{Fields: doc.Fields})
		if len(violations) != 0 {
			return fmt.Errorf("invalid restore priority for %s", id)
		}
		links := append([]pipeline.Link{}, doc.Links...)
		sort.Slice(links, func(i, j int) bool {
			if links[i].Kind != links[j].Kind {
				return links[i].Kind < links[j].Kind
			}
			return links[i].To.ID < links[j].To.ID
		})
		for _, row := range []struct {
			table driver.DerivedTable
			value any
		}{
			{driver.DerivedReadyState, map[string]bool{"blocked": blocked, "ready": effective.ReadyEligible(doc.Type, stringValue(doc.Fields["status"])) && !blocked}},
			{driver.DerivedAncestorClosure, []ancestorMembership{{Ancestor: id, Depth: 0}}},
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
	if err := add(driver.DerivedOntology, "builtin", ontology.DefaultDocument()); err != nil {
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
	}{"builtin-head/1", []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey}, plan.derived})
	if err != nil {
		return err
	}
	plan.derivedDigest = hashBytes(data)
	return nil
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
	evidence := archiveEvidence{ArchiveSHA256: archive.SHA256(), DerivedSHA256: plan.derivedDigest, Profile: "builtin-head/1", SourceStore: manifest.StoreID, Members: manifest.Files}
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
