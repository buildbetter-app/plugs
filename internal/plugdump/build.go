package plugdump

import (
	"context"
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
	if err := checkMigrations(tables["schema_migrations"], "main"); err != nil {
		return nil, err
	}
	if err := checkMigrations(tables["ignored_schema_migrations"], "ignored"); err != nil {
		return nil, err
	}
	hasHistory := len(tables["issue_versions"].Rows)+len(tables["issue_snapshots"].Rows)+len(tables["compaction_snapshots"].Rows) != 0
	if hasHistory != (inventory.History == "available-unverified") {
		return nil, fmt.Errorf("history declaration disagrees with available version/snapshot rows")
	}
	for name, table := range tables {
		action, _ := actionFor(name)
		if len(table.Rows) != 0 && (action == "refuse-if-nonempty" || name == "leases") {
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
		action, reason := actionFor(name)
		dispositions = append(dispositions, Disposition{name, len(table.Rows), action, reason})
		if name == "issues" || name == "labels" || name == "repo_mtimes" || name == "leases" {
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
		Disposition{"issues.row_lock", len(tables["issues"].Rows), "manifest/rebuild", "transient locks excluded"},
		Disposition{"issues.is_blocked", len(tables["issues"].Rows), "manifest/rebuild", "derived blocking excluded; Plug must recompute"})
	manifest := Manifest{
		StatusCategoryContract:     inventory.StatusCategoryContract,
		SourcePlaneAddressContract: inventory.SourcePlaneAddressContract,
		Format:                     Format, Schema: 1, Reference: BeadsReference, StoreID: inventory.StoreID, Machine: inventory.Machine,
		MainVersion: 72, IgnoredVersion: 28, Scope: "offline-head-and-source-archives", History: inventory.History,
		SourceFieldsContract: IssueSourceFieldsContract,
		Projects:             mapper.projects, Ontology: ontology, Sources: dispositions,
		Warnings:     []string{"not a complete store migration or Plug restore certificate", "source history and attribution are unverified; no Plug versions, changesets or live leases invented"},
		Verification: verificationLimitsForStatus(inventory.StatusCategoryContract),
	}
	files := map[string][]byte{"config/sources.json": config}
	history, err := captureSourceHistory(ctx, src, inventory, mapper.records, tables)
	if err != nil {
		return nil, err
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
		expected := columnsFor(name)
		columns := append([]string(nil), table.Columns...)
		sort.Strings(expected)
		sort.Strings(columns)
		if strings.Join(expected, "\x00") != strings.Join(columns, "\x00") {
			return nil, fmt.Errorf("schema drift in %s", name)
		}
		for _, row := range table.Rows {
			if len(row) != len(expected) {
				return nil, fmt.Errorf("missing or unknown source fields in %s", name)
			}
			for _, column := range expected {
				if _, ok := row[column]; !ok {
					return nil, fmt.Errorf("missing source field in %s", name)
				}
			}
			values := map[string]any(row)
			if name == "local_metadata" {
				// "key" is a SQL column name, not the configuration key.
				values = map[string]any{text(row, "key"): row["value"]}
			}
			if err := validateSecretReferences(values); err != nil {
				// Config/metadata credentials are replaced using explicit policy.
				if name != "config" && name != "metadata" {
					return nil, fmt.Errorf("family %s: %w", name, err)
				}
			}
		}
		tables[name] = table
	}
	for name := range tableColumns {
		if _, ok := tables[name]; !ok {
			return nil, fmt.Errorf("unaccounted table family %s", name)
		}
	}
	return tables, nil
}

func checkMigrations(table Table, lane string) error {
	known := migrationHashes[lane]
	seen := map[uint64]bool{}
	for _, row := range table.Rows {
		version, err := natural(row["version"])
		if err != nil || version > uint64(len(known)) || version == 0 || seen[version] || text(row, "content_hash") != known[int(version)] {
			return fmt.Errorf("unknown, duplicate, missing-hash or altered %s schema migration", lane)
		}
		seen[version] = true
	}
	if len(seen) != len(known) {
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
