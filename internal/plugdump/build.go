package plugdump

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var prefixPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,31}$`)
var hashPattern = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// Build gathers and validates the whole read set before serializing any output.
// No partial success is possible after a source read or mapping failure.
func Build(ctx context.Context, src Source, inventory Inventory) ([]byte, error) {
	if inventory.NativeHistoryContract != "" {
		return nil, fmt.Errorf("native delta requires explicit capture-set builder")
	}
	return build(ctx, src, inventory, nil)
}

type nativeDeltaHead struct {
	Log           SourceCommitLog
	Current, Head map[string]NativeTable
	Ignore        NativeTable
}

func build(ctx context.Context, src Source, inventory Inventory, delta *nativeDeltaHead) ([]byte, error) {
	if (delta != nil) != (inventory.NativeHistoryContract == NativeDeltaFormat) || inventory.NativeHistoryContract != "" && inventory.NativeHistoryContract != NativeDeltaFormat {
		return nil, fmt.Errorf("unsupported native delta attachment")
	}
	if inventory.SourceFamilyContract != "" && (inventory.SourceFamilyContract != SourceFamilyContract || inventory.SourcePlaneAddressContract != SourcePlaneAddressContract) {
		return nil, fmt.Errorf("unsupported source family contract")
	}
	if err := validateSourcePlaneContract(inventory.SourcePlaneAddressContract); err != nil {
		return nil, err
	}
	if inventory.Version != "beads-inventory/1" || inventory.StoreID == "" || inventory.Machine == "" {
		return nil, fmt.Errorf("unsupported inventory version or missing source identity")
	}
	if !inventory.Quiesced || !inventory.RemotesMerged || !inventory.WorkingSetClean {
		return nil, fmt.Errorf("source must be quiesced, clean and have no unmerged remotes")
	}
	if inventory.History != "unavailable" && inventory.History != "available-unverified" {
		return nil, fmt.Errorf("history must explicitly report unavailable or available-unverified")
	}
	if err := validateStatusCategoryContract(inventory.StatusCategoryContract, inventory.Projects); err != nil {
		return nil, err
	}
	projects, err := copyProjects(inventory.Projects)
	if err != nil {
		return nil, err
	}
	ontology, err := copyOntology(inventory.Ontology, projects)
	if err != nil {
		return nil, err
	}
	config, dispositions, err := readExternal(inventory.External)
	if err != nil {
		return nil, err
	}
	yamlValues, err := nativeYAMLCatalogValues(config)
	if err != nil {
		return nil, err
	}
	tables, err := readTables(ctx, src)
	if err != nil {
		return nil, err
	}
	profile, err := sourceSchemaProfile(tables)
	if err != nil {
		return nil, err
	}
	if profile.MainVersion == 66 && inventory.SourceFamilyContract != SourceFamilyContract {
		return nil, fmt.Errorf("own-version capture requires native source family contract")
	}
	hasHistory := len(tables["issue_versions"].Rows)+len(tables["issue_snapshots"].Rows)+len(tables["compaction_snapshots"].Rows) != 0
	if hasHistory != (inventory.History == "available-unverified") {
		return nil, fmt.Errorf("history declaration disagrees with available version/snapshot rows")
	}
	for name, table := range tables {
		action, _ := familyAction(name, inventory.SourceFamilyContract)
		if len(table.Rows) != 0 && (action == "refuse-if-nonempty" || name == "leases" && inventory.SourceFamilyContract == "") {
			return nil, fmt.Errorf("unsupported nonempty family %s (live claims must be resolved)", name)
		}
	}
	storedConfig := map[string]any{}
	for _, row := range tables["config"].Rows {
		key := text(row, "key")
		if key == "status.custom" || key == "types.custom" {
			storedConfig[key] = row["value"]
		}
	}
	if inventory.StatusCategoryContract == StatusCategoryContract {
		if err := validateProjectStatusCatalogs(projects, tables["custom_statuses"].Rows, tables["custom_types"].Rows, storedConfig, yamlValues); err != nil {
			return nil, err
		}
	}
	mapper := mapping{
		statusCategories: inventory.StatusCategoryContract == StatusCategoryContract,
		sourcePlanes:     inventory.SourcePlaneAddressContract == SourcePlaneAddressContract,
		customStatuses:   map[string]bool{},
		projects:         projects,
		beads:            map[string]string{},
		wisps:            map[string]string{},
		records:          map[string][]Record{},
		policy:           inventory.ConfigPolicy,
		usedPolicy:       map[string]bool{},
		ontology:         ontology,
		usedOntology:     map[string]bool{},
	}
	if mapper.sourcePlanes {
		if err := readCatalogUnion(tables["custom_statuses"].Rows, tables["custom_types"].Rows, storedConfig, yamlValues, mapper.customStatuses); err != nil {
			return nil, err
		}
	}
	if err := mapper.wispRows(tables["wisps"].Rows); err != nil {
		return nil, err
	}
	if err := mapper.issues(tables["issues"].Rows); err != nil {
		return nil, err
	}
	if !mapper.sourcePlanes {
		if err := validateCatalogUnion(tables["custom_statuses"].Rows, tables["custom_types"].Rows, storedConfig, yamlValues); err != nil {
			return nil, err
		}
	}
	for _, family := range []string{"wisp_labels", "wisp_comments", "wisp_events", "wisp_child_counters"} {
		if err := mapper.wispAuxiliary(family, tables[family].Rows); err != nil {
			return nil, err
		}
	}
	if len(mapper.usedOntology) != len(ontology) {
		return nil, fmt.Errorf("unaccounted ontology directive")
	}
	if err := mapper.allocator(tables); err != nil {
		return nil, err
	}
	for _, name := range sortedNames(tables) {
		table := tables[name]
		action, reason := familyAction(name, inventory.SourceFamilyContract)
		dispositions = append(dispositions, Disposition{Family: name, Rows: len(table.Rows), Action: action, Reason: reason})
		if name == "issues" || name == "labels" || (name == "repo_mtimes" || name == "leases") && inventory.SourceFamilyContract == "" {
			continue
		}
		for _, row := range table.Rows {
			if err := mapper.row(name, row); err != nil {
				return nil, err
			}
		}
	}
	for key := range inventory.ConfigPolicy {
		if !mapper.usedPolicy[key] {
			return nil, fmt.Errorf("unaccounted config policy key; source inventory does not match read set")
		}
	}
	if err := mapper.labels(tables["labels"].Rows); err != nil {
		return nil, err
	}
	dispositions = append(dispositions,
		Disposition{Family: "issues.row_lock", Rows: len(tables["issues"].Rows), Action: "manifest/rebuild", Reason: "transient locks excluded"},
		Disposition{Family: "issues.is_blocked", Rows: len(tables["issues"].Rows), Action: "manifest/rebuild", Reason: "derived blocking excluded; Plug must recompute"})
	manifest := Manifest{
		NativeHistoryContract:      inventory.NativeHistoryContract,
		SourceFamilyContract:       inventory.SourceFamilyContract,
		StatusCategoryContract:     inventory.StatusCategoryContract,
		SourcePlaneAddressContract: inventory.SourcePlaneAddressContract,
		Format:                     Format, Schema: 1, Reference: BeadsReference, StoreID: inventory.StoreID, Machine: inventory.Machine,
		MainVersion: profile.MainVersion, IgnoredVersion: profile.IgnoredVersion, Scope: "offline-head-and-source-archives", History: inventory.History,
		SourceFieldsContract: IssueSourceFieldsContract,
		Projects:             mapper.projects, Ontology: ontology, Sources: dispositions,
		Warnings:     []string{"not a complete store migration or Plug restore certificate", "source history and attribution are unverified; no Plug versions, changesets or live leases invented"},
		Verification: verificationLimitsForStatus(inventory.StatusCategoryContract),
	}
	files := map[string][]byte{"config/sources.json": config}
	var history *SourceHistory
	if delta != nil {
		observed, err := nativeTables(tables, inventory.ConfigPolicy)
		if err != nil {
			return nil, err
		}
		for name, table := range observed {
			if !equalJSON(table, delta.Current[name]) {
				return nil, ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
			}
		}
		for name := range delta.Current {
			if _, known := tableColumns[name]; known {
				if _, present := observed[name]; !present {
					return nil, ErrSourceHistoryUnavailable{Reason: "source-census-changed"}
				}
			} else if name != "dolt_ignore" && name != "dolt_nonlocal_tables" && name != "dolt_schemas" {
				return nil, fmt.Errorf("unmapped physical current family %q", name)
			}
		}
		// Dolt's stored schema fragments are absent from the portable SQL
		// BASE TABLE census. Their complete physical state stays in the delta
		// stream, alongside the other named Dolt backing tables above.
		commits, err := historyCommitOrder(delta.Log)
		if err != nil {
			return nil, err
		}
		history = &SourceHistory{Format: SourceHistoryFormat, Commits: commits, Events: canonicalSourceEvents(tables["events"].Rows), Range: SourceHistoryRange{Status: "unverified", Reason: "native-delta", Baseline: []Record{}, BaselineEvents: []Row{}, Gaps: []HistoryGap{}, Frames: []SourceHistoryFrame{}}}
	} else {
		history, err = captureSourceHistory(ctx, src, inventory, mapper.records, tables)
		if err != nil {
			return nil, err
		}
	}
	if history != nil {
		manifest.History = "available-unverified"
		if history.Range.Status == "verified" && history.Range.Complete {
			manifest.History = "verified"
		}
		files[SourceHistoryMember], err = marshal(history)
		if err != nil {
			return nil, err
		}
	}
	if inventory.SourceFamilyContract == SourceFamilyContract {
		if delta != nil {
			native := NativeHistory{Head: delta.Log.Head, Ignore: delta.Ignore, Images: []NativeHistoryImage{{CommitID: delta.Log.Head, Tables: delta.Head}}}
			if err := classifyNativeSources(manifest.Sources, native); err != nil {
				return nil, err
			}
			return encode(manifest, mapper.records, files)
		}
		native, err := captureNativeHistory(ctx, src, inventory, tables)
		if err != nil {
			return nil, err
		}
		if err := classifyNativeSources(manifest.Sources, *native); err != nil {
			return nil, err
		}
		if native.HistoryCoverage != nil {
			if history == nil {
				return nil, fmt.Errorf("boundary history requires events attachment")
			}
			native.HistoryCoverage.EventsAttachment = NativeEventsAttachment{Member: SourceHistoryMember, SHA256: fmt.Sprintf("%x", sha256.Sum256(files[SourceHistoryMember])), Rows: uint64(len(history.Events))}
			if err := validateHistoryCoverage(*native, files[SourceHistoryMember], *history); err != nil {
				return nil, err
			}
		}
		files[NativeHistoryMember], err = marshal(native)
		if err != nil {
			return nil, err
		}
	}
	return encode(manifest, mapper.records, files)
}

func verificationLimits() map[string]string {
	return map[string]string{
		"capture": "operator declarations; freeze/remotes/working-set not independently verified",
		"schema":  "paired migration hashes and exact column names; SQL types, indexes and constraints not independently verified",
		"history": "available source rows archived only; no A6 replay or completeness claim",
		"a3":      "not-run", "authz.verify": "not-run", "derived": "not-run", "plugin_lock": "not-produced", "ontology": "built-in status mapping; explicit map_type directives recorded in manifest",
	}
}

func readTables(ctx context.Context, src Source) (map[string]Table, error) {
	names, err := src.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("read source inventory: %w", err)
	}
	tables := map[string]Table{}
	for _, name := range names {
		if _, ok := tableColumns[name]; !ok {
			return nil, fmt.Errorf("unknown source table %s", name)
		}
		if _, ok := tables[name]; ok {
			return nil, fmt.Errorf("duplicate source table %s", name)
		}
		table, err := src.ReadTable(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("read complete family %s: %w", name, err)
		}
		for _, row := range table.Rows {
			values := map[string]any(row)
			if name == "local_metadata" {
				// "key" is a SQL column name, not the configuration key.
				values = map[string]any{text(row, "key"): row["value"]}
			}
			if err := validateSQLSecretReferences(name, values); err != nil {
				// Config/metadata credentials are replaced using explicit policy.
				if name != "config" && name != "metadata" {
					return nil, fmt.Errorf("family %s: %w", name, err)
				}
			}
		}
		tables[name] = table
	}
	profile, err := sourceSchemaProfile(tables)
	if err != nil {
		return nil, err
	}
	if err := validateCurrentSchema(tables, profile); err != nil {
		return nil, err
	}
	return tables, nil
}

func checkMigrations(table Table, lane string) error {
	return checkMigrationPrefix(table, lane, len(migrationHashes[lane]))
}

func checkMigrationPrefix(table Table, lane string, limit int) error {
	known := migrationHashes[lane]
	seen := map[uint64]bool{}
	for _, row := range table.Rows {
		version, err := natural(row["version"])
		if err != nil || version > uint64(limit) || version == 0 || seen[version] || text(row, "content_hash") != known[int(version)] {
			return fmt.Errorf("unknown, duplicate, missing-hash or altered %s schema migration", lane)
		}
		seen[version] = true
	}
	if len(seen) != limit {
		return fmt.Errorf("incomplete %s migration series", lane)
	}
	return nil
}

func copyProjects(input []Project) ([]Project, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("project allocator inventory required")
	}
	projects := append([]Project(nil), input...)
	sort.Slice(projects, func(i, j int) bool { return projects[i].Prefix < projects[j].Prefix })
	for i := range projects {
		p := &projects[i]
		if !prefixPattern.MatchString(p.Prefix) || (p.IDMode != "hash" && p.IDMode != "sequential") || p.HashLength < 0 || p.HashLength > 64 || (p.IDMode == "hash" && p.HashLength == 0) {
			return nil, fmt.Errorf("invalid project prefix or unsupported allocator mode")
		}
		if i > 0 && (p.Prefix == projects[i-1].Prefix || strings.HasPrefix(p.Prefix, projects[i-1].Prefix+"-")) {
			return nil, fmt.Errorf("duplicate or ambiguous project prefix")
		}
		for j, namespace := range p.HashNamespaces {
			if p.IDMode != "hash" || !validHashNamespace(namespace) || j > 0 && namespace <= p.HashNamespaces[j-1] {
				return nil, fmt.Errorf("hash_namespaces requires sorted unique native namespaces in hash mode")
			}
		}
		p.Statuses = append([]ProjectStatus(nil), p.Statuses...)
		p.HashNamespaces = append([]string(nil), p.HashNamespaces...)
		counters := map[string]uint64{}
		for parent, count := range p.ChildCounters {
			counters[parent] = count
		}
		p.ChildCounters = counters
	}
	return projects, nil
}

func sortedNames(tables map[string]Table) []string {
	var names []string
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func text(row Row, key string) string {
	value, _ := row[key].(string)
	return value
}

func natural(value any) (uint64, error) {
	var s string
	switch value := value.(type) {
	case int:
		s = strconv.FormatInt(int64(value), 10)
	case int8:
		s = strconv.FormatInt(int64(value), 10)
	case int16:
		s = strconv.FormatInt(int64(value), 10)
	case int32:
		s = strconv.FormatInt(int64(value), 10)
	case int64:
		s = strconv.FormatInt(value, 10)
	case uint:
		s = strconv.FormatUint(uint64(value), 10)
	case uint8:
		s = strconv.FormatUint(uint64(value), 10)
	case uint16:
		s = strconv.FormatUint(uint64(value), 10)
	case uint32:
		s = strconv.FormatUint(uint64(value), 10)
	case uint64:
		s = strconv.FormatUint(value, 10)
	case json.Number:
		s = string(value)
	case string:
		s = value
	default:
		return 0, fmt.Errorf("expected a lossless nonnegative integer")
	}
	return strconv.ParseUint(s, 10, 64)
}

func cloneRow(row Row) Row {
	copy := Row{}
	for key, value := range row {
		copy[key] = value
	}
	return copy
}
