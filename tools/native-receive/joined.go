package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const rehearsalPolicySHA256 = "3afb19b934e69e90b9f8e966d5e4e4d1fab27a47686eacf38543e23d1c7542d9"

type archivePaths []string

func (p *archivePaths) String() string { return fmt.Sprint([]string(*p)) }
func (p *archivePaths) Set(value string) error {
	if value == "" {
		return errors.New("empty original archive path")
	}
	*p = append(*p, value)
	return nil
}

// Every component is an external caller binding. Database and rehearsal labels
// are authenticated host assertions, never guessed from IDs or merge metadata.
type originalInput struct {
	RehearsalProject string                         `json:"rehearsal_project"`
	Database         string                         `json:"database"`
	SourceStoreID    string                         `json:"source_store_id"`
	SourceProject    string                         `json:"source_project"`
	ArchiveSHA256    string                         `json:"archive_sha256"`
	Files            []dumpformat.NativeCaptureFile `json:"files,omitempty"`
}

type joinedBinding struct {
	Format        string          `json:"format"`
	PolicySHA256  string          `json:"policy_sha256"`
	ProfileSHA256 string          `json:"profile_sha256"`
	Sources       []originalInput `json:"sources"`
}

type policyRefusal struct {
	Code, Family, Field string
	Count               int
}

func (e policyRefusal) Error() string {
	return fmt.Sprintf("%s: family=%s field=%s count=%d", e.Code, e.Family, e.Field, e.Count)
}
func (e policyRefusal) Unwrap() error { return driver.ErrUnsupported }

// Go's encoding/json marshals this struct in declaration order, without
// whitespace or a trailing newline. All digests are lower-case hex SHA-256.
type idempotencyCommitment struct {
	Format        string          `json:"format"`
	ArchiveSHA256 []string        `json:"archive_sha256"`
	Bindings      []originalInput `json:"bindings"`
	ProfileSHA256 string          `json:"profile_sha256"`
	PolicySHA256  string          `json:"policy_sha256"`
}

func loadOriginals(paths archivePaths) ([][]byte, error) {
	if len(paths) == 0 || len(paths) > 2 {
		return nil, errors.New("one original or exactly two ordered originals required")
	}
	result := make([][]byte, len(paths))
	for i, path := range paths {
		data, err := readBounded(path, dumpformat.MaxArchiveBytes)
		if err != nil {
			return nil, err
		}
		result[i] = data
	}
	return result, nil
}

func joinedArchive(ctx context.Context, originals [][]byte, binding joinedBinding, profileBytes []byte) ([]byte, string, error) {
	if binding.Format != "plug-native-joined-inputs/1" || binding.PolicySHA256 != rehearsalPolicySHA256 || binding.ProfileSHA256 != digest(profileBytes) || len(binding.Sources) != 2 || len(originals) != 2 {
		return nil, "", fmt.Errorf("%w: explicit ordered original/policy/profile bindings required", driver.ErrUnsupported)
	}
	var profile dumpformat.NativeSchemaProfile
	if err := decode(profileBytes, &profile); err != nil {
		return nil, "", err
	}
	want, err := dumpformat.NativeSchemaForVersion(66, 26)
	if err != nil || !reflect.DeepEqual(profile, want) {
		return nil, "", fmt.Errorf("%w: rehearsal requires the exact mechanically derived66/26profile", driver.ErrUnsupported)
	}
	canonical, err := json.Marshal(want)
	if err != nil {
		return nil, "", err
	}
	if !bytes.Equal(profileBytes, append(canonical, '\n')) {
		return nil, "", policyRefusal{"unsupported_profile_encoding", "schema_profile", "canonical-json-newline", 1}
	}
	archives := make([]*dumpformat.Archive, 2)
	hashes := make([]string, 2)
	owners, collisions := map[string]string{}, map[string]bool{}
	for i, raw := range originals {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		source := binding.Sources[i]
		if source.RehearsalProject != []string{"cody-ops", "signals-v2"}[i] || source.Database != []string{"ops", "sv2"}[i] || source.SourceStoreID == "" || source.SourceProject != source.Database || source.ArchiveSHA256 != digest(raw) {
			return nil, "", fmt.Errorf("%w: ordered archive/source/project/database binding differs", driver.ErrUnsupported)
		}
		a, err := dumpformat.Read(bytes.NewReader(raw))
		if err != nil {
			return nil, "", err
		}
		if err := strictNativeHistory(a); err != nil {
			return nil, "", err
		}
		scopes, manifest := a.NativeSourceHistories(), a.Manifest()
		if manifest.StoreID != source.SourceStoreID || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != source.SourceProject || len(scopes) != 1 || scopes[0].StoreID != source.SourceStoreID || !reflect.DeepEqual(scopes[0].Projects, []string{source.SourceProject}) || scopes[0].SchemaProfile == nil || !reflect.DeepEqual(*scopes[0].SchemaProfile, profile) {
			return nil, "", fmt.Errorf("%w: original native source/project/profile differs", driver.ErrUnsupported)
		}
		archives[i], hashes[i] = a, source.ArchiveSHA256
		tables := []map[string]dumpformat.NativeTable{scopes[0].Current}
		for _, image := range scopes[0].Images {
			tables = append(tables, image.Tables)
		}
		for _, image := range tables {
			for _, family := range []string{dumpformat.DurableEntityTable, dumpformat.EphemeralEntityTable} {
				for _, row := range image[family].Rows {
					id, ok := row["id"].(string)
					if !ok || id == "" {
						return nil, "", policyRefusal{"unsupported_source_cell", family, "id", 1}
					}
					if prior, present := owners[id]; present && prior != source.SourceProject {
						collisions[id] = true
					}
					owners[id] = source.SourceProject
				}
			}
		}
	}
	if len(collisions) != 0 {
		return nil, "", policyRefusal{"cross_project_id_collision", "issues/wisps", "id", len(collisions)}
	}
	joined, err := dumpformat.MergeArchives(archives...)
	if err != nil {
		return nil, "", err
	}
	a, err := dumpformat.Read(bytes.NewReader(joined))
	if err != nil {
		return nil, "", err
	}
	// MergeArchives copies the original native member verbatim to its existing
	// source-scoped path. Check that property rather than normalizing its JSON.
	for i, original := range archives {
		raw, ok := original.Member(dumpformat.NativeHistoryMember)
		kept, found := a.Member("history/native-tables/" + binding.Sources[i].SourceProject + ".json")
		if !ok || !found || !bytes.Equal(raw, kept) {
			return nil, "", errors.New("assembly did not conserve the original native member")
		}
	}
	preimage, err := json.Marshal(idempotencyCommitment{Format: "plug-native-rehearsal-idempotency/1", ArchiveSHA256: hashes, Bindings: binding.Sources, ProfileSHA256: binding.ProfileSHA256, PolicySHA256: binding.PolicySHA256})
	if err != nil {
		return nil, "", err
	}
	return joined, digest(preimage), nil
}

func constructForInputs(ctx context.Context, originals [][]byte, policyBytes, bindingBytes, profileBytes []byte, lifetime driver.CheckpointLifetime) ([]byte, request, error) {
	var zero request
	if digest(policyBytes) != rehearsalPolicySHA256 {
		if len(originals) != 1 || len(bindingBytes) != 0 || len(profileBytes) != 0 {
			return nil, zero, fmt.Errorf("%w: joined input requires the exact authorized rehearsal policy", driver.ErrUnsupported)
		}
		r, err := constructRequest(ctx, originals[0], policyBytes, lifetime)
		return originals[0], r, err
	}
	var binding joinedBinding
	if err := decode(bindingBytes, &binding); err != nil {
		return nil, zero, err
	}
	joined, key, err := joinedArchive(ctx, originals, binding, profileBytes)
	if err != nil {
		return nil, zero, err
	}
	a, err := dumpformat.Read(bytes.NewReader(joined))
	if err != nil {
		return nil, zero, err
	}
	projects, sources := []string{}, []dumpformat.SourceIdentity{}
	for _, project := range a.Manifest().Projects {
		projects = append(projects, project.Prefix)
	}
	for _, scope := range a.NativeSourceHistories() {
		sources = append(sources, dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]})
	}
	encoded, err := rehearsalConstructionPolicy(a.SHA256(), a.Manifest().StoreID, projects, sources, key, lifetime)
	if err != nil {
		return nil, zero, err
	}
	r, err := constructRequest(ctx, joined, encoded, lifetime)
	if err != nil {
		return nil, zero, err
	}
	r.PolicySHA256, r.OriginalInputs = rehearsalPolicySHA256, &binding
	return joined, r, nil
}

func rehearsalConstructionPolicy(archiveSHA256, sourceStoreID string, projects []string, sources []dumpformat.SourceIdentity, key string, lifetime driver.CheckpointLifetime) ([]byte, error) {
	principal := model.PrincipalRef{Kind: "user", ID: "plug-restore-rehearsal"}
	principalJSON, err := json.Marshal(principal)
	if err != nil {
		return nil, err
	}
	policy := constructionPolicy{Format: "plug-native-receiving-policy/1", ArchiveSHA256: archiveSHA256, SourceStoreID: sourceStoreID, Projects: projects, Sources: sources,
		Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: principal, Authority: "restore-admin", AdmissionDigest: key}, IdempotencyKey: key,
		Principal:    driver.SourcePlanePrincipalV1{Key: "plug-restore-rehearsal", Source: rehearsalPolicySHA256, Authority: "restore-only-local/1", PrincipalJSON: principalJSON},
		Provider:     driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, OriginAdapter: "beads-workflow-origins/1"},
		EntityPolicy: "durable-bead-identity", TuplePolicy: "copy-observed-exact", StatusPolicy: "source-name-identity", TargetIDPolicy: "source-id", RoutePolicy: "active-first", UnmappedFields: "opaque-metadata", OtherRecords: "inert-origin", Sidecars: "retain-captured-origins", DependencyPolicy: "exact-typed",
		FieldMappings: []driver.SourcePlaneStringPairV1{{From: "id", To: "id"}, {From: "title", To: "title"}, {From: "description", To: "description"}, {From: "priority", To: "priority"}, {From: "issue_type", To: "type"}, {From: "status", To: "status"}, {From: "metadata", To: "metadata"}, {From: "created_at", To: "created_at"}}}
	return json.Marshal(policy)
}

func auxiliaryInputs(bindingPath, profilePath string) ([]byte, []byte, error) {
	var binding, profile []byte
	var err error
	if bindingPath != "" {
		binding, err = readBounded(bindingPath, driver.SourcePlaneMaxPlanBytes)
		if err != nil {
			return nil, nil, err
		}
	}
	if profilePath != "" {
		profile, err = readBounded(profilePath, driver.SourcePlaneMaxPlanBytes)
		if err != nil {
			return nil, nil, err
		}
	}
	return binding, profile, nil
}

func inputsUnchanged(paths archivePaths, originals [][]byte) error {
	for i, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(data, originals[i]) {
			return errors.New("original receiving input changed during execution")
		}
	}
	return nil
}

const rehearsalSessionLimit = 2 * time.Hour

// Local references are resolved only by exact recorded ID and source project;
// ambiguous same-ID candidates are refused rather than guessed or rekeyed.
func appendExactRelation(plan *driver.SourcePlanePlanV1, archive *dumpformat.Archive, record driver.SourcePlaneRecordV1) error {
	work, release := archive.NativeDeltaAccountingScope()
	defer release()
	if err := work.ChargeNativeDeltaJSON("cli_relation_row", json.RawMessage(record.Raw)); err != nil {
		return err
	}
	var row dumpformat.Record
	if err := decode(record.Raw, &row); err != nil {
		return err
	}
	refuse := func(field string) error {
		return fmt.Errorf("%w: family=%s field=%s row_count=1", driver.ErrUnsupported, row.Source.Table, field)
	}
	fromID, ok := row.Source.Fields["issue_id"].(string)
	if !ok || row.Source.Table != dumpformat.DurableDependencyTable {
		return refuse("issue_id")
	}
	from := driver.SourceEntityKey{Project: row.Project, Plane: dumpformat.DurableEntityTable, ID: fromID}
	relation := driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: row.Project, Table: row.Source.Table, ID: row.ID}, From: from, Kind: fmt.Sprint(row.Body["type"]), PredicateContract: "beads-typed-metadata/1"}
	field, target := "", ""
	for _, name := range []string{"depends_on_issue_id", dumpformat.EphemeralDependencyTargetField, "depends_on_external"} {
		value := row.Source.Fields[name]
		if value == nil || value == "" {
			continue
		}
		text, ok := value.(string)
		if !ok || field != "" {
			return refuse(name)
		}
		field, target = name, text
	}
	if field == "depends_on_external" {
		relation.External = &target
	} else {
		if field != "depends_on_issue_id" {
			return refuse(field)
		}
		var found *driver.SourceEntityKey
		for _, entity := range plan.Entities {
			if entity.Source.ID == target && entity.Source.Plane == dumpformat.DurableEntityTable {
				if found != nil {
					return refuse(field)
				}
				copy := entity.Source
				found = &copy
			}
		}
		if found == nil {
			return refuse(field)
		}
		relation.To = found
	}
	fields, err := fieldCommitmentsAccounted(archive, work, record.Raw)
	if err != nil {
		return err
	}
	if err := work.ChargeNativeDeltaJSON("cli_relation_metadata", row.Source.Fields["metadata"]); err != nil {
		return err
	}
	metadata, err := dumpformat.SourceEdgeMetadata(archive.Manifest().EdgeVersion(), row.ID, relation.Kind, row.Source.Fields["metadata"])
	if err != nil {
		return err
	}
	n, err := dumpformat.NativeDeltaJSONSize(metadata)
	if err != nil {
		return err
	}
	if err := archive.ChargeNativeDeltaBytes("cli_relation_predicate", n-1); err != nil {
		return err
	}
	relation.PredicateJSON, err = json.Marshal(metadata)
	if err != nil {
		return err
	}
	if err := work.ChargeNativeDeltaBytes("cli_relation_origin_hash", 64); err != nil {
		return err
	}
	relation.Origin = driver.SourcePlaneBytesV1{Presence: "object", Size: uint64(len(record.Raw)), SHA256: digest(record.Raw)}
	retained := relation
	retained.PredicateJSON = nil // Its exact buffer has its own reservation.
	if err := archive.ChargeNativeDeltaJSON("cli_mapper_relation", retained); err != nil {
		return err
	}
	relation.Fields = fields
	plan.Relations = append(plan.Relations, relation)
	return nil
}
