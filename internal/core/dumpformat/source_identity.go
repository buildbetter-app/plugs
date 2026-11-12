package dumpformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const SourceIdentityContract = "beads-source-identity/1"

// SourceIdentity names the original source, never the aggregate merge store.
// Project is the exact source prefix used by the allocator and history scope.
type SourceIdentity struct {
	StoreID string `json:"store_id"`
	Project string `json:"project"`
}

func (s *SourceIdentity) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := decodeJSON(data, &fields); err != nil {
		return err
	}
	if len(fields) != 2 || fields["store_id"] == nil || fields["project"] == nil {
		return fmt.Errorf("source identity requires exact store_id and project")
	}
	type plain SourceIdentity
	var value plain
	if err := decodeJSON(data, &value); err != nil {
		return err
	}
	if !validSourceIdentity(SourceIdentity(value)) {
		return fmt.Errorf("source identity requires nonempty unambiguous store/project")
	}
	*s = SourceIdentity(value)
	return nil
}

func validSourceIdentity(s SourceIdentity) bool {
	return s.StoreID != "" && s.Project != "" && utf8.ValidString(s.StoreID+s.Project) &&
		strings.TrimSpace(s.StoreID) == s.StoreID && strings.TrimSpace(s.Project) == s.Project && !strings.ContainsRune(s.StoreID+s.Project, '\x00')
}

// SourceRecordKey is the lossless key for provenance and legacy-ID maps. Its
// tuple fields are independent: even opaque physical IDs containing NUL cannot
// alias a different source, table or kind. Content IDs are never rewritten.
type SourceRecordKey struct {
	StoreID string `json:"store_id"`
	Project string `json:"project"`
	Kind    string `json:"kind"`
	Table   string `json:"table"`
	ID      string `json:"id"`
}

// SourceKey returns the original identity on validated head or historical
// records. False means a legacy record has no explicit source namespace.
func (r Record) SourceKey() (SourceRecordKey, bool) {
	if r.Source.Identity == nil || !validSourceIdentity(*r.Source.Identity) {
		return SourceRecordKey{}, false
	}
	return SourceRecordKey{StoreID: r.Source.Identity.StoreID, Project: r.Source.Identity.Project, Kind: r.Kind, Table: r.Source.Table, ID: r.ID}, true
}

// IncompleteHistoryError names the actual retained gap. No snapshot or
// fabricated history is produced when a history-bearing merge returns it.
type IncompleteHistoryError struct {
	StoreID  string
	Project  string
	First    uint64
	Last     uint64
	Reason   string
	CommitID string
	Family   string
}

func (e IncompleteHistoryError) Error() string {
	message := fmt.Sprintf("incomplete source history: store=%s project=%s gap=%d..%d: %s", e.StoreID, e.Project, e.First, e.Last, e.Reason)
	if e.CommitID != "" {
		message += " commit=" + e.CommitID
	}
	if e.Family != "" {
		message += " family=" + e.Family
	}
	return message
}

func requireCompleteSourceHistory(identity SourceIdentity, r SourceHistoryRange) error {
	if r.Status == "verified" && r.Complete && r.First == 1 && len(r.Gaps) == 0 && len(r.Baseline) == 0 {
		return nil
	}
	gap := IncompleteHistoryError{StoreID: identity.StoreID, Project: identity.Project, Reason: r.Reason}
	if len(r.Gaps) != 0 {
		gap.First, gap.Last, gap.Reason = r.Gaps[0].First, r.Gaps[0].Last, r.Gaps[0].Reason
	} else if r.First > 1 {
		gap.First, gap.Last, gap.Reason = 1, r.First-1, "history starts after source root"
	} else if gap.Reason == "" {
		gap.Reason = "complete source history unavailable"
	}
	return gap
}

func namespaceRecord(r Record, identity SourceIdentity) Record {
	r.Source.Identity = &identity
	return r
}

func namespaceHistory(h ProjectSourceHistory, identity SourceIdentity) ProjectSourceHistory {
	h.SourceStoreID = identity.StoreID
	for i := range h.Range.Baseline {
		h.Range.Baseline[i] = namespaceRecord(h.Range.Baseline[i], identity)
	}
	for i := range h.Range.Frames {
		for j := range h.Range.Frames[i].Records {
			h.Range.Frames[i].Records[j] = namespaceRecord(h.Range.Frames[i].Records[j], identity)
		}
	}
	return h
}

func mergeInputIdentity(input mergeInput, project string) (SourceIdentity, error) {
	for _, h := range input.archive.nativeHistories {
		if len(h.Projects) == 1 && (project == h.Projects[0] || project == "" && len(input.manifest.Projects) == 1) {
			return SourceIdentity{StoreID: h.StoreID, Project: h.Projects[0]}, nil
		}
	}
	for _, h := range input.histories {
		if h.Project != project && project != "" {
			continue
		}
		store := h.SourceStoreID
		if store == "" && len(input.manifest.Projects) == 1 {
			store = input.manifest.StoreID
		}
		identity := SourceIdentity{StoreID: store, Project: h.Project}
		if validSourceIdentity(identity) && (project != "" || len(input.histories) == 1) {
			return identity, nil
		}
	}
	return SourceIdentity{}, HistoryMergeRefusal{Kind: "source identity", Project: project, Reason: "source store/project is absent or ambiguous"}
}

func externalEntryKey(e externalEntry) string {
	key, _ := json.Marshal(struct {
		Identity *SourceIdentity
		Family   string
		Path     string
	}{e.Identity, e.Input.Family, e.Input.Path})
	return string(key)
}

func compareExternalEntries(a, b externalEntry) int {
	if a.Input.Precedence < b.Input.Precedence {
		return -1
	}
	if a.Input.Precedence > b.Input.Precedence {
		return 1
	}
	if c := strings.Compare(a.Input.Family, b.Input.Family); c != 0 {
		return c
	}
	if c := strings.Compare(a.Input.Path, b.Input.Path); c != 0 {
		return c
	}
	return strings.Compare(externalEntryKey(a), externalEntryKey(b))
}

func validateSourceIdentityMarker(fields map[string]json.RawMessage, manifest Manifest) error {
	for key := range fields {
		if key != "source_identity_contract" && strings.EqualFold(key, "source_identity_contract") {
			return fmt.Errorf("noncanonical source_identity_contract key")
		}
	}
	if _, present := fields["source_identity_contract"]; present && manifest.SourceIdentityContract != SourceIdentityContract {
		return fmt.Errorf("unsupported source identity contract")
	}
	if manifest.SourceIdentityContract != "" && (manifest.Scope != Scope || manifest.History != "verified" && manifest.SourceFamilyContract != SourceFamilyContract) {
		return fmt.Errorf("source identity contract requires complete offline source history")
	}
	return nil
}

// validateScopedSourceRecords reuses the admitted source validators within
// each declared source project, then checks the aggregate census. It does not
// create a shared catalog, global physical ID or cross-project deduplication.
func validateScopedSourceRecords(m Manifest, rows map[string][]Record, config []byte) error {
	bySource := map[SourceIdentity]map[string][]Record{}
	owners := map[string]SourceIdentity{}
	for kind, records := range rows {
		for _, r := range records {
			identity := r.Source.Identity
			if identity == nil || !validSourceIdentity(*identity) || !projectInManifest(identity.Project, m.Projects) || r.Project != "" && r.Project != identity.Project {
				return HistoryMergeRefusal{Kind: "source identity", Project: r.Project, ID: r.ID, Reason: "record source store/project is absent or inconsistent"}
			}
			if prior, ok := owners[identity.Project]; ok && prior != *identity {
				return HistoryMergeRefusal{Kind: "source identity", Project: identity.Project, Reason: "project has ambiguous source stores"}
			}
			owners[identity.Project] = *identity
			if bySource[*identity] == nil {
				bySource[*identity] = map[string][]Record{}
			}
			bySource[*identity][kind] = append(bySource[*identity][kind], r)
		}
	}
	if len(owners) != len(m.Projects) {
		return ErrSourceHistory{Reason: "missing source project records"}
	}
	var entries []externalEntry
	if err := decodeJSON(config, &entries); err != nil {
		return err
	}
	configs := map[SourceIdentity][]externalEntry{}
	for _, entry := range entries {
		if entry.Identity == nil || owners[entry.Identity.Project] != *entry.Identity || bySource[*entry.Identity] == nil {
			return HistoryMergeRefusal{Kind: "configuration source identity", Reason: "configuration provenance has no declared source"}
		}
		configs[*entry.Identity] = append(configs[*entry.Identity], entry)
	}
	counts := map[string]int{}
	planes := m.SourcePlaneAddressContract == SourcePlaneAddressContract
	native := m.SourceFamilyContract == SourceFamilyContract
	if native {
		for _, s := range m.Sources {
			if s.Identity == nil || owners[s.Identity.Project] != *s.Identity {
				return fmt.Errorf("source disposition lacks declared original identity")
			}
		}
	}
	for _, project := range m.Projects {
		identity := owners[project.Prefix]
		localCounts := counts
		if native {
			localCounts = map[string]int{}
		}
		scopedRows := bySource[identity]
		scoped := m
		scoped.Projects = []Project{project}
		scoped.Ontology = nil
		for _, d := range m.Ontology {
			if d.Target.Project == project.Prefix {
				scoped.Ontology = append(scoped.Ontology, d)
			}
		}
		localEntries := append([]externalEntry{}, configs[identity]...)
		for i := range localEntries {
			localEntries[i].Identity = nil
		}
		localConfig, err := json.Marshal(localEntries)
		if err != nil {
			return err
		}
		if err = validateConfigProvenance(localConfig); err != nil {
			return err
		}
		var statuses map[string]bool
		if planes {
			categories, err := sourcePlaneCatalogCategoriesFor(scopedRows, localConfig, m.StatusCategoryContract == StatusCategoryContract)
			if err != nil {
				return err
			}
			statuses = map[string]bool{}
			for name := range categories {
				statuses[name] = true
			}
		}
		if m.StatusCategoryContract == StatusCategoryContract {
			if err := validateNativeStatusCatalogConservation(scoped, scopedRows, localConfig); err != nil {
				return err
			}
		}
		if !planes || m.Verification["ontology"] == StrictUnionOntologyVerification {
			if _, err := compileImportMigration(scoped, scopedRows, localConfig, true); err != nil {
				return err
			}
		}
		for _, kind := range sortedRecordKinds(scopedRows) {
			for _, r := range scopedRows[kind] {
				var err error
				if native && m.NativeHistoryContract == NativeDeltaFormat && nativeDeltaOpaqueArchiveFamily(r.Source.Table) || planes && (r.Kind == "edge" || sourcePlaneArchiveTable(r.Source.Table) || nativeOperationalTable(r.Source.Table, m.SourceFamilyContract)) {
					err = validateSourcePlaneRecord(r, scoped, localCounts, statuses)
				} else {
					err = validateVersionedRecord(r, scoped.Projects, localCounts, nil, m.EdgeVersion(), m.MainVersion)
				}
				if err != nil {
					return err
				}
			}
		}
		if err := validateSourcePlaneRelations(scoped, scopedRows); err != nil {
			return err
		}
		if err := validateIssueOntology(scoped.Ontology, scoped.Projects, scopedRows["bead"], true); err != nil {
			return err
		}
		if native {
			localSources := []Disposition{}
			for _, s := range m.Sources {
				if *s.Identity == identity {
					s.Identity = nil
					localSources = append(localSources, s)
				}
			}
			if err := validateCaptureDispositionsForVersion(localSources, localCounts, localConfig, nil, planes, m.MainVersion, m.NativeHistoryContract == NativeDeltaFormat, m.SourceFamilyContract); err != nil {
				return err
			}
		}
	}
	if native {
		return nil
	}
	return validateBoundDispositions(m.Sources, counts, config, nil, planes)
}

func scopedHistoryManifest(manifest Manifest, project string) Manifest {
	manifest.Projects = []Project{findProject(project, manifest.Projects)}
	var directives []OntologyDirective
	for _, d := range manifest.Ontology {
		if d.Target.Project == project {
			directives = append(directives, d)
		}
	}
	manifest.Ontology = directives
	return manifest
}

func sourceConfigForIdentity(data []byte, identity SourceIdentity) ([]byte, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	result := []byte{'['}
	first := true
	for _, raw := range entries {
		var entry externalEntry
		if err := decodeJSON(raw, &entry); err != nil {
			return nil, err
		}
		if entry.Identity == nil || *entry.Identity != identity {
			continue
		}
		if !first {
			result = append(result, ',')
		}
		result = append(result, raw...)
		first = false
	}
	return append(result, ']', '\n'), nil
}

func identityRecordKey(r Record) string {
	key, _ := r.SourceKey()
	encoded, _ := json.Marshal(key)
	return string(encoded)
}

// sourceIdentities returns deterministic source namespaces for caller maps.
func (a *Archive) SourceIdentities() []SourceIdentity {
	seen := map[SourceIdentity]bool{}
	if a != nil {
		for _, records := range a.byKind {
			for _, r := range records {
				if r.Source.Identity != nil {
					seen[*r.Source.Identity] = true
				}
			}
		}
	}
	identities := make([]SourceIdentity, 0, len(seen))
	for identity := range seen {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].StoreID != identities[j].StoreID {
			return identities[i].StoreID < identities[j].StoreID
		}
		return identities[i].Project < identities[j].Project
	})
	return identities
}
