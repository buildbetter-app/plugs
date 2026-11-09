package plugdump

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/steveyegge/beads/internal/captureclock"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"gopkg.in/yaml.v3"
)

// NativeCapture describes the resolved sources of this process. Public is an
// explicit operator declaration covering values, quiescence and disposal. SQL
// and filesystem reads are observations, not a global filesystem snapshot.
type NativeCapture struct {
	SourcePlaneAddressContract              string
	Public                                  bool
	StoreDir, BeadsDir, Machine, SidecarDir string
	Paths                                   map[string][]string
	Environment, Flags                      map[string]any
}

// NativeInventory reads through the same scoped SQL port as the dump exporter.
// It returns data only; the command publishes sidecars before the inventory.
func (s SQLSource) NativeInventory(ctx context.Context, capture NativeCapture) (Inventory, map[string][]byte, error) {
	if err := validateSourcePlaneContract(capture.SourcePlaneAddressContract); err != nil {
		return Inventory{}, nil, err
	}
	if !capture.Public {
		return Inventory{}, nil, fmt.Errorf("native inventory requires explicit --plug-capture-public")
	}
	clockInput, err := captureclock.Read()
	if err != nil {
		return Inventory{}, nil, err
	}
	for _, path := range []string{capture.StoreDir, capture.BeadsDir, capture.SidecarDir} {
		if !filepath.IsAbs(path) {
			return Inventory{}, nil, fmt.Errorf("native capture requires resolved absolute paths")
		}
	}
	tables, err := readTables(ctx, s)
	if err != nil {
		return Inventory{}, nil, err
	}
	if err := checkMigrations(tables["schema_migrations"], "main"); err != nil {
		return Inventory{}, nil, err
	}
	if err := checkMigrations(tables["ignored_schema_migrations"], "ignored"); err != nil {
		return Inventory{}, nil, err
	}
	log, err := s.ReadSourceCommits(ctx)
	if err != nil {
		return Inventory{}, nil, err
	}
	if !log.Clean {
		return Inventory{}, nil, fmt.Errorf("native capture requires a clean source working set")
	}
	inventory := Inventory{Version: "beads-inventory/1", StoreID: "native/" + digest([]byte(capture.StoreDir)), Machine: capture.Machine, Quiesced: true, RemotesMerged: true, WorkingSetClean: true, History: "unavailable", Projects: []Project{}, Ontology: []OntologyDirective{}, External: []ExternalInput{}, ConfigPolicy: map[string]string{}}
	inventory.SourcePlaneAddressContract = capture.SourcePlaneAddressContract
	if len(tables["issue_versions"].Rows)+len(tables["issue_snapshots"].Rows)+len(tables["compaction_snapshots"].Rows) > 0 {
		inventory.History = "available-unverified"
	}
	cfg := map[string]string{}
	for _, name := range []string{"config", "metadata"} {
		for _, row := range tables[name].Rows {
			key := text(row, "key")
			if key == "" {
				return Inventory{}, nil, fmt.Errorf("empty native policy key")
			}
			policy := "public" // Explicit public capture declaration, never a key-name inference.
			if secretKey(key) {
				policy = "secret"
			}
			full := name + "/" + key
			if _, ok := inventory.ConfigPolicy[full]; ok {
				return Inventory{}, nil, fmt.Errorf("duplicate native policy key")
			}
			inventory.ConfigPolicy[full] = policy
			if name == "config" {
				cfg[key] = text(row, "value")
			}
		}
	}
	prefix := cfg["issue_prefix"]
	if !prefixPattern.MatchString(prefix) || cfg["allowed_prefixes"] != "" {
		return Inventory{}, nil, fmt.Errorf("native inventory supports one explicit source issue_prefix")
	}
	hashLength := issueops.DefaultAdaptiveConfig().MinLength
	if value := cfg["min_hash_length"]; value != "" {
		hashLength, err = strconv.Atoi(value)
		if err != nil || hashLength < 1 || hashLength > 64 {
			return Inventory{}, nil, fmt.Errorf("unsupported native min_hash_length")
		}
	}
	// Import preserves older valid source IDs even when they are shorter than
	// the native generator minimum. This inventory floor describes observed IDs;
	// the actual generator settings remain unchanged in the archived config.
	namespaces := map[string]bool{}
	for _, row := range tables["issues"].Rows {
		id := text(row, "id")
		if !strings.HasPrefix(id, prefix+"-") {
			return Inventory{}, nil, fmt.Errorf("native issue outside source prefix")
		}
		suffix, _, _ := strings.Cut(strings.TrimPrefix(id, prefix+"-"), ".")
		namespace, hash, ok := splitHashRoot(suffix)
		if !ok {
			return Inventory{}, nil, fmt.Errorf("unsupported native issue identity")
		}
		if namespace != "" {
			namespaces[namespace] = true
		}
		hashLength = min(hashLength, len(hash))
	}
	project := Project{Prefix: prefix, IDMode: "hash", HashLength: hashLength, ChildCounters: map[string]uint64{}}
	for namespace := range namespaces {
		project.HashNamespaces = append(project.HashNamespaces, namespace)
	}
	sort.Strings(project.HashNamespaces)
	for _, row := range tables["issues"].Rows {
		if _, _, _, err := issueIDParts(text(row, "id"), project); err != nil {
			return Inventory{}, nil, fmt.Errorf("unsupported native issue identity")
		}
	}
	inventory.Projects = []Project{project}
	inventory.Ontology, err = nativeTypeDirectives(tables["issues"].Rows, project)
	if err != nil {
		return Inventory{}, nil, err
	}
	// The existing closed-world mapping remains authoritative for allocator,
	// ontology, unsupported families and every stored business value.
	observations := map[string]map[string]any{}
	for family, paths := range capture.Paths {
		if !nativeFileFamily(family) {
			return Inventory{}, nil, fmt.Errorf("unsupported native filesystem family %s", family)
		}
		rows := []any{}
		for _, path := range uniqueNativePaths(paths) {
			observed, err := observeNativePath(path, 0, &nativeCaptureBudget{})
			if err != nil {
				return Inventory{}, nil, fmt.Errorf("native %s: %w", family, err)
			}
			rows = append(rows, observed)
		}
		observations[family] = map[string]any{"observations": rows}
	}
	observations["environment-and-flags"] = map[string]any{"environment": nativePublicValues(capture.Environment), "flags": nativePublicValues(capture.Flags), "public_quiesced_disposable_declaration": true}
	clockEvidence, err := s.observeNativeClock(ctx, clockInput)
	if err != nil {
		return Inventory{}, nil, err
	}
	if clockEvidence != nil {
		observations["environment-and-flags"]["capture_clock"] = clockEvidence
	}
	queries := map[string]string{
		"dolt-ignore":          "SELECT * FROM dolt_ignore",
		"dolt-nonlocal-tables": "SELECT * FROM dolt_nonlocal_tables",
		"dolt-working-set":     "SELECT table_name, staged, status FROM dolt_status",
		"dolt-remotes":         "SELECT * FROM dolt_remotes",
		"dolt-conflicts":       "SELECT * FROM dolt_conflicts",
	}
	for family, query := range queries {
		table, err := s.historyTable(ctx, query)
		if err != nil {
			return Inventory{}, nil, fmt.Errorf("native %s census: %w", family, err)
		}
		if (family == "dolt-remotes" || family == "dolt-conflicts" || family == "dolt-working-set") && len(table.Rows) > 0 {
			return Inventory{}, nil, fmt.Errorf("native %s must be empty", family)
		}
		sort.Slice(table.Rows, func(i, j int) bool {
			a, _ := marshal(table.Rows[i])
			b, _ := marshal(table.Rows[j])
			return string(a) < string(b)
		})
		observations[family] = map[string]any{"query": query, "table": table}
	}
	observations["dolt-commit-graph"] = map[string]any{"head": log.Head, "clean": log.Clean, "commits": log.Commits}
	observations["dolt-history-maintenance"] = map[string]any{"observed_reachable_head": log.Head, "retention_before_reachable_root": "unverified", "engine_contents": "not-read"}
	dirs := []any{}
	for _, path := range []string{capture.BeadsDir, capture.StoreDir} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() {
			return Inventory{}, nil, fmt.Errorf("native storage path is not a real directory")
		}
		dirs = append(dirs, map[string]any{"path": path, "state": "directory", "mode": uint32(info.Mode().Perm()), "contents": "not-read"})
	}
	observations["store-files-and-quiescence"] = map[string]any{"observations": dirs, "operator_quiesced": true, "sql_clean": log.Clean, "filesystem_atomicity": "not-verified"}
	sidecars := map[string][]byte{}
	for _, family := range externalFamilies {
		values, ok := observations[family]
		if !ok {
			return Inventory{}, nil, fmt.Errorf("unaccounted native external family %s", family)
		}
		if err := validateSecretReferences(values); err != nil {
			return Inventory{}, nil, fmt.Errorf("native %s: %w", family, err)
		}
		data, err := marshal(values)
		if err != nil || len(data) > 4<<20 {
			return Inventory{}, nil, fmt.Errorf("native source capture exceeds supported JSON envelope")
		}
		name := family + ".json"
		sidecars[name] = data
		precedence := len(inventory.External)
		// Native user config is merged before the documented ~/.config/bd
		// location, even on systems where those paths differ.
		if family == "user-native-config" {
			precedence = 1
		} else if family == "user-xdg-config" {
			precedence = 2
		}
		inventory.External = append(inventory.External, ExternalInput{Family: family, Path: filepath.Join(capture.SidecarDir, name), State: "json-config", Precedence: precedence})
	}
	// Build the category catalog from the captured native files and SQL rows,
	// never by guessing semantics from a status spelling.
	yamlValues := []map[string]any{}
	for family, paths := range capture.Paths {
		if !nativeFileFamily(family) || len(paths) == 0 {
			continue
		}
		if err := appendYAMLCatalogValues(observations[family]["observations"], &yamlValues); err != nil {
			return Inventory{}, nil, err
		}
	}
	storedConfig := map[string]any{}
	for _, row := range tables["config"].Rows {
		if key := text(row, "key"); key == "status.custom" || key == "types.custom" {
			storedConfig[key] = row["value"]
		}
	}
	statuses, err := declaredStatusCatalog(tables["custom_statuses"].Rows, tables["custom_types"].Rows, storedConfig, yamlValues)
	if err != nil {
		return Inventory{}, nil, err
	}
	inventory.StatusCategoryContract = StatusCategoryContract
	inventory.Projects[0].Statuses = statuses
	finalLog, err := s.ReadSourceCommits(ctx)
	if err != nil {
		return Inventory{}, nil, err
	}
	if !equalJSON(log, finalLog) {
		return Inventory{}, nil, fmt.Errorf("native source census changed")
	}
	if err := unchangedNativeClock(clockInput); err != nil {
		return Inventory{}, nil, err
	}
	return inventory, sidecars, nil
}

// Public native capture explicitly records a map_type directive for every
// observed non-built-in type. It does not rename the source type, interpret
// its workflow, or relax the offline exporter's required-directive check.
func nativeTypeDirectives(rows []Row, project Project) ([]OntologyDirective, error) {
	seen := map[string]bool{}
	directives := []OntologyDirective{}
	for _, row := range rows {
		issueType, err := requiredText(row, "issue_type")
		if err != nil || !validCatalogTypeName(issueType) {
			return nil, fmt.Errorf("unsupported native issue type")
		}
		if builtInIssueTypes[issueType] || seen[issueType] {
			continue
		}
		seen[issueType] = true
		// A digest avoids delimiter collisions and preserves arbitrary valid
		// custom type spelling only in its explicit source/target fields.
		identity, err := marshal([]string{project.Prefix, issueType})
		if err != nil {
			return nil, err
		}
		directives = append(directives, OntologyDirective{
			ID: "native-map-type/" + digest(identity), Kind: "map_type",
			Target: OntologyTarget{Project: project.Prefix, Type: issueType},
			From:   issueType, To: "issue",
		})
	}
	if len(directives) == 0 {
		return directives, nil
	}
	return copyOntology(directives, []Project{project})
}

// uniqueNativePaths preserves the caller's first-seen order while preventing
// one filesystem path from being observed twice under the same native family.
// The producer enumerates some families from two equivalent workspace roots;
// when those roots resolve to the same absolute path, the strict consumer
// must receive one observation with the path's actual state.
func uniqueNativePaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	unique := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		unique = append(unique, path)
	}
	return unique
}

func nativeFileFamily(family string) bool {
	switch family {
	case "user-legacy-config", "user-xdg-config", "user-native-config",
		"project-config", "beads-dir-config", "project-local-config",
		"metadata-json", "legacy-config-json", "beads-hooks", "legacy-hooks",
		"git-hooks", "external-hook-managers", "routes-jsonl", "formulas",
		"formula-search-paths", "issues-jsonl", "interactions-jsonl":
		return true
	}
	return false
}

func nativePublicValues(input map[string]any) map[string]any {
	values := map[string]any{}
	for key, value := range input {
		if secretKey(key) {
			values[key] = map[string]any{"$secret": key}
		} else {
			values[key] = value
		}
	}
	return values
}

// Symlinks, special files and oversized trees refuse. Engine directories never
// pass through this reader; their location is recorded separately without reads.
type nativeCaptureBudget struct{ files, bytes int }

func observeNativePath(path string, depth int, budget *nativeCaptureBudget) (map[string]any, error) {
	if !filepath.IsAbs(path) || depth > 12 {
		return nil, fmt.Errorf("unresolved path or excessive directory depth")
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return map[string]any{"path": path, "state": "absent"}, nil
	}
	if err != nil {
		return nil, err
	}
	result := map[string]any{"path": path, "mode": uint32(info.Mode().Perm())}
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		children := []any{}
		for _, entry := range entries {
			if entry.Name() == ".dolt" || entry.Name() == "embeddeddolt" || entry.Name() == "dolt" {
				return nil, fmt.Errorf("engine contents are outside native filesystem capture")
			}
			budget.files++
			if budget.files > 1024 {
				return nil, fmt.Errorf("native capture file limit")
			}
			child, err := observeNativePath(filepath.Join(path, entry.Name()), depth+1, budget)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		result["state"], result["entries"] = "directory", children
		return result, nil
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return nil, fmt.Errorf("native source must be a bounded regular file")
	}
	file, err := os.OpenInRoot(filepath.Dir(path), filepath.Base(path))
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil || len(data) > 1<<20 {
		return nil, fmt.Errorf("native file read failed or exceeded bound")
	}
	budget.bytes += len(data)
	if budget.bytes > 3<<20 {
		return nil, fmt.Errorf("native capture byte limit")
	}
	// Decode structured sources to enforce the existing credential reference
	// rules before recording any raw bytes or digest of their contents.
	var values map[string]any
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		var err error
		values, err = decodeNativeYAMLValues(data)
		if err != nil {
			return nil, fmt.Errorf("invalid native YAML")
		}
	case ".json":
		if err := decodeJSON(data, &values); err != nil {
			return nil, fmt.Errorf("invalid native JSON object")
		}
	}
	if err := validateSecretReferences(values); err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	result["state"], result["sha256"], result["size"], result["bytes"] = "file", fmt.Sprintf("%x", hash), len(data), map[string]string{"$bytes": base64.StdEncoding.EncodeToString(data)}
	if values != nil {
		result["values"] = values
	}
	return result, nil
}

// decodeNativeYAMLValues keeps the distinction between an empty YAML document
// and an explicitly null document. yaml.Unmarshal maps both to a nil map, but
// an empty/comment-only config is a real, successfully parsed object for the
// native inventory. Explicit null (including an explicitly tagged empty
// scalar) remains nil so the receiver can refuse it under its object contract.
func decodeNativeYAMLValues(data []byte) (map[string]any, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	if emptyYAMLDocument(document) {
		return map[string]any{}, nil
	}

	var values map[string]any
	if err := yaml.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	return values, nil
}

func emptyYAMLDocument(document yaml.Node) bool {
	if document.Kind == 0 {
		return true
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) == 0 {
		return false
	}
	root := document.Content[0]
	if root.Kind == 0 {
		return true
	}
	// A parser-created empty document has an implicit null scalar with no
	// value or explicit tag. Tagged/quoted nulls are explicit input and must
	// retain the old nil/refusal behavior.
	return root.Kind == yaml.ScalarNode && root.Tag == "!!null" && root.Value == "" && root.Style&yaml.TaggedStyle == 0
}

// MarshalNativeInventory preserves the existing inventory representation.
func MarshalNativeInventory(inventory Inventory) ([]byte, error) {
	return json.MarshalIndent(inventory, "", "  ")
}
