package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This first constructor supports one explicitly selected durable identity
// policy. Choosing it is a host decision; it never guesses live workflows,
// runs, principals or relation effects from source names or field values.
type constructionPolicy struct {
	Format           string                           `json:"format"`
	ArchiveSHA256    string                           `json:"archive_sha256"`
	SourceStoreID    string                           `json:"source_store_id"`
	Projects         []string                         `json:"projects"`
	Sources          []dumpformat.SourceIdentity      `json:"sources"`
	Admission        driver.RestoreImportAdmission    `json:"admission"`
	IdempotencyKey   string                           `json:"idempotency_key"`
	Principal        driver.SourcePlanePrincipalV1    `json:"principal"`
	Provider         driver.SourcePlaneProviderV1     `json:"provider"`
	EntityPolicy     string                           `json:"entity_policy"`
	TuplePolicy      string                           `json:"tuple_policy"`
	StatusPolicy     string                           `json:"status_policy"`
	TargetIDPolicy   string                           `json:"target_id_policy"`
	RoutePolicy      string                           `json:"route_policy"`
	FieldMappings    []driver.SourcePlaneStringPairV1 `json:"field_mappings"`
	UnmappedFields   string                           `json:"unmapped_fields"`
	OtherRecords     string                           `json:"other_records"`
	Sidecars         string                           `json:"sidecars"`
	DependencyPolicy string                           `json:"dependency_policy,omitempty"`
}

func prepareRequest(ctx context.Context, args []string) (request, error) {
	var zero request
	flags := flag.NewFlagSet("native-receive prepare-request", flag.ContinueOnError)
	var paths archivePaths
	flags.Var(&paths, "archive", "original native archive; repeat in declared order for joined rehearsal")
	var captureSets archivePaths
	flags.Var(&captureSets, "capture-set", "original native capture-set directory; supply exactly two in declared order")
	policyPath := flags.String("policy", "", "explicit trusted construction policy")
	bindingPath := flags.String("bindings", "", "external ordered original source/project/database/hash bindings")
	profilePath := flags.String("profile", "", "exact mechanical source profile bytes")
	engine := flags.String("driver", "", "memory, sqlite or postgres")
	expectedBinary := flags.String("expected-binary-sha256", "", "required exact executable SHA256")
	if err := flags.Parse(args); err != nil {
		return zero, err
	}
	if flags.NArg() != 0 || (len(paths) == 0) == (len(captureSets) == 0) || len(captureSets) > 0 && len(captureSets) != 2 || *policyPath == "" {
		return zero, errors.New("archive and explicit policy required")
	}
	self, err := os.Executable()
	if err != nil {
		return zero, err
	}
	f, err := os.Open(self)
	if err != nil {
		return zero, err
	}
	h := sha256.New()
	_, hashErr := io.Copy(h, f)
	closeErr := f.Close()
	if hashErr != nil || closeErr != nil || hex.EncodeToString(h.Sum(nil)) != *expectedBinary {
		return zero, errors.New("receiving executable binding differs")
	}
	lifetime := driver.CheckpointLifetimeProcess
	switch *engine {
	case "memory":
	case "sqlite", "postgres":
		lifetime = driver.CheckpointLifetimeRestart
	default:
		return zero, errors.New("explicit receiving driver required")
	}
	if len(captureSets) == 2 {
		return prepareNativeCaptureCollectionRequest(ctx, captureSets, *policyPath, *bindingPath, *profilePath, lifetime)
	}
	originals, err := loadOriginals(paths)
	if err != nil {
		return zero, err
	}
	policy, err := readBounded(*policyPath, driver.SourcePlaneMaxPlanBytes)
	if err != nil {
		return zero, err
	}
	binding, profile, err := auxiliaryInputs(*bindingPath, *profilePath)
	if err != nil {
		return zero, err
	}
	_, result, err := constructForInputs(ctx, originals, policy, binding, profile, lifetime)
	return result, err
}

func constructRequest(ctx context.Context, archiveBytes, policyBytes []byte, lifetime driver.CheckpointLifetime) (request, error) {
	var zero request
	var policy constructionPolicy
	if err := decode(policyBytes, &policy); err != nil {
		return zero, err
	}
	if policy.Format != "plug-native-receiving-policy/1" || policy.EntityPolicy != "durable-bead-identity" || policy.TuplePolicy != "copy-observed-exact" || policy.StatusPolicy != "source-name-identity" || policy.TargetIDPolicy != "source-id" || policy.RoutePolicy != "active-first" || (policy.UnmappedFields != "origin-only" && policy.UnmappedFields != "opaque-metadata") || policy.OtherRecords != "inert-origin" || policy.Sidecars != "retain-captured-origins" || (policy.DependencyPolicy != "" && policy.DependencyPolicy != "exact-typed") {
		return zero, fmt.Errorf("%w: unsupported or missing explicit construction policy", driver.ErrUnsupported)
	}
	if policy.Principal.Key == "" || policy.Principal.UnverifiedOriginOnly || policy.Provider.LogicalImageSHA256 != "" || policy.Provider.LogicalImageSize != 0 || policy.Provider.Lifetime != lifetime || lifetime.Validate() != nil {
		return zero, fmt.Errorf("%w: explicit principal/provider/lifetime required", driver.ErrUnsupported)
	}
	archive, err := dumpformat.Read(bytes.NewReader(archiveBytes))
	if err != nil {
		return zero, err
	}
	if err := strictNativeHistory(archive); err != nil {
		return zero, err
	}
	result, err := constructArchiveRequest(ctx, archive, policyBytes, lifetime)
	if err != nil {
		return zero, err
	}
	profile := driver.RestoreImportProfile{Admission: result.Admission, SourceStoreID: driver.StoreID(result.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: result.IdempotencyKey, ArchiveDigest: result.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano)}
	plan := result.SourcePlan
	plan.Target.StoreID, plan.Target.Incarnation = "pending-new-target", "pending-new-target"
	if _, err := converterroundtrip.BindSourceImport(ctx, bytes.NewReader(archiveBytes), profile, plan); err != nil {
		return zero, err
	}
	return result, nil
}

// The existing explicit policy mapper is shared by both input shapes. This
// function receives only an authenticated current Archive and never reads or
// joins histories. Each caller validates its result through its issuing leaf.
func constructArchiveRequest(ctx context.Context, archive *dumpformat.Archive, policyBytes []byte, lifetime driver.CheckpointLifetime) (request, error) {
	var zero request
	var policy constructionPolicy
	if err := decode(policyBytes, &policy); err != nil {
		return zero, err
	}
	if policy.Format != "plug-native-receiving-policy/1" || policy.EntityPolicy != "durable-bead-identity" || policy.TuplePolicy != "copy-observed-exact" || policy.StatusPolicy != "source-name-identity" || policy.TargetIDPolicy != "source-id" || policy.RoutePolicy != "active-first" || (policy.UnmappedFields != "origin-only" && policy.UnmappedFields != "opaque-metadata") || policy.OtherRecords != "inert-origin" || policy.Sidecars != "retain-captured-origins" || (policy.DependencyPolicy != "" && policy.DependencyPolicy != "exact-typed") {
		return zero, fmt.Errorf("%w: unsupported or missing explicit construction policy", driver.ErrUnsupported)
	}
	if policy.Principal.Key == "" || policy.Principal.UnverifiedOriginOnly || policy.Provider.LogicalImageSHA256 != "" || policy.Provider.LogicalImageSize != 0 || policy.Provider.Lifetime != lifetime || lifetime.Validate() != nil {
		return zero, fmt.Errorf("%w: explicit principal/provider/lifetime required", driver.ErrUnsupported)
	}
	scratch, releaseScratch := archive.NativeDeltaAccountingScope()
	defer releaseScratch()
	projects, sources := []string{}, []dumpformat.SourceIdentity{}
	for _, project := range archive.Manifest().Projects {
		projects = append(projects, project.Prefix)
	}
	metadata, err := archive.NativeSourceHistoryMetadata()
	if err != nil {
		return zero, err
	}
	for _, scope := range metadata {
		if len(scope.Projects) != 1 {
			return zero, driver.ErrUnsupported
		}
		sources = append(sources, dumpformat.SourceIdentity{StoreID: scope.StoreID, Project: scope.Projects[0]})
	}
	if len(sources) == 0 || policy.ArchiveSHA256 != archive.SHA256() || policy.SourceStoreID != archive.Manifest().StoreID || !reflect.DeepEqual(policy.Projects, projects) || !reflect.DeepEqual(policy.Sources, sources) {
		return zero, errors.New("policy external archive/source/project binding differs")
	}
	plan := driver.SourcePlanePlanV1{Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: policy.SourceStoreID, ArchiveSHA256: policy.ArchiveSHA256, Target: driver.SourcePlaneTargetV1{StoreID: "pending-new-target", Incarnation: "pending-new-target", RequireEmpty: true, Lifetime: lifetime, RuntimeContract: driver.SourcePlaneRuntimeContractV1}, Principals: []driver.SourcePlanePrincipalV1{policy.Principal}, Providers: []driver.SourcePlaneProviderV1{policy.Provider}}
	mappings := map[string]string{}
	for _, field := range policy.FieldMappings {
		if field.From == "" || field.To == "" || mappings[field.From] != "" {
			return zero, errors.New("duplicate or empty explicit field mapping")
		}
		mappings[field.From] = field.To
	}
	dispositions := map[string]*driver.SourcePlaneDispositionV1{}
	var edgeRows []driver.SourcePlaneRecordV1
	records, err := scratch.SourcePlaneRecords()
	if err != nil {
		return zero, err
	}
	for _, record := range records {
		if err := func() error {
			work, release := scratch.NativeDeltaAccountingScope()
			defer release()
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := work.ChargeNativeDeltaJSON("cli_mapper_record", json.RawMessage(record.Raw)); err != nil {
				return err
			}
			var row dumpformat.Record
			if err := decode(record.Raw, &row); err != nil {
				return err
			}
			if row.Kind == "edge" && policy.DependencyPolicy == "exact-typed" {
				edgeRows = append(edgeRows, record)
				return nil
			}
			fields, err := fieldCommitmentsAccounted(archive, work, record.Raw)
			if err != nil {
				return err
			}
			if err := work.ChargeNativeDeltaBytes("cli_mapper_origin_hash", 64); err != nil {
				return err
			}
			origin := driver.SourcePlaneBytesV1{Presence: "object", Size: uint64(len(record.Raw)), SHA256: digest(record.Raw)}
			if row.Kind != "bead" {
				project := row.Project
				if row.Source.Identity != nil {
					project = row.Source.Identity.Project
				}
				candidate := driver.SourcePlaneOriginV1{Project: project, Table: row.Source.Table, ID: row.ID, Action: policy.OtherRecords, Raw: origin, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}}
				if err := archive.ChargeNativeDeltaJSON("cli_mapper_origin", candidate); err != nil {
					return err
				}
				candidate.Fields = fields
				plan.Origins = append(plan.Origins, candidate)
				return nil
			}
			if row.Source.Table != dumpformat.DurableEntityTable || row.Project == "" {
				return fmt.Errorf("%w: durable source address unavailable", driver.ErrUnsupported)
			}
			typ, typeOK := row.Source.Fields["issue_type"].(string)
			status, statusOK := row.Source.Fields["status"].(string)
			storage, present := row.Source.Fields["storage_class"]
			storageText := ""
			if storage != nil {
				var ok bool
				storageText, ok = storage.(string)
				if !ok || storageText == "" {
					return errors.New("source storage class must be present SQL NULL or nonempty text")
				}
			}
			ephemeral, err := exactFlag(row.Source.Fields["ephemeral"])
			if err != nil {
				return err
			}
			noHistory, err := exactFlag(row.Source.Fields["no_history"])
			if err != nil {
				return err
			}
			if !present || !typeOK || !statusOK || typ == "" || status == "" {
				return errors.New("source type/status/storage tuple incomplete")
			}
			if err := work.ChargeNativeDeltaJSON("cli_mapper_tuple", []any{typ, storage, ephemeral, noHistory}); err != nil {
				return err
			}
			tuple, err := json.Marshal([]any{typ, storage, ephemeral, noHistory})
			if err != nil {
				return err
			}
			if err := work.ChargeNativeDeltaBytes("cli_mapper_disposition_key", 73); err != nil {
				return err
			}
			key := "observed/" + digest(tuple)
			rules := []driver.SourcePlaneFieldRuleV1{}
			usedMappings := map[string]bool{}
			for _, field := range fields {
				rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: policy.UnmappedFields}
				if policy.UnmappedFields == "opaque-metadata" {
					if err := work.ChargeNativeDeltaBytes("cli_mapper_rule_target", uint64(6+len(field.Name))); err != nil {
						return err
					}
					rule.Target = "beads." + field.Name
				}
				if target, ok := mappings[field.Name]; ok {
					rule.Target, rule.Disposition = target, "exact-map"
					usedMappings[field.Name] = true
				}
				if err := work.ChargeNativeDeltaJSON("cli_mapper_candidate_rule", rule); err != nil {
					return err
				}
				rules = append(rules, rule)
			}
			if len(usedMappings) != len(mappings) {
				return errors.New("explicit mapped field absent from source schema")
			}
			d := dispositions[key]
			if d == nil {
				candidate := driver.SourcePlaneDispositionV1{Key: key, SourceType: typ, StorageClass: storageText, Ephemeral: ephemeral, NoHistory: noHistory, SinkKind: "bead", Schema: "bead/1", Lifetime: "core", Fields: rules, RefusalPolicy: "exact-only"}
				if err := archive.ChargeNativeDeltaJSON("cli_mapper_disposition", candidate); err != nil {
					return err
				}
				d = &candidate
				dispositions[key] = d
			} else if !reflect.DeepEqual(d.Fields, rules) {
				return errors.New("same observed tuple has differing field rules")
			}
			foundStatus := false
			for _, pair := range d.Statuses {
				foundStatus = foundStatus || pair.From == status
			}
			if !foundStatus {
				if err := archive.ChargeNativeDeltaJSON("cli_mapper_status", driver.SourcePlaneStringPairV1{From: status, To: status}); err != nil {
					return err
				}
				d.Statuses = append(d.Statuses, driver.SourcePlaneStringPairV1{From: status, To: status})
			}
			keySource := driver.SourceEntityKey{Project: row.Project, Plane: row.Source.Table, ID: row.ID}
			candidate := driver.SourcePlaneEntityV1{Source: keySource, TargetKind: "bead", TargetID: row.ID, DispositionKey: key, PrincipalKey: policy.Principal.Key, Route: driver.SourcePlaneRouteV1{Project: row.Project, SourceID: row.ID, DurableTargetID: row.ID, Preference: policy.RoutePolicy}, Origin: origin}
			if err := archive.ChargeNativeDeltaJSON("cli_mapper_entity", candidate); err != nil {
				return err
			}
			candidate.Fields = fields
			plan.Entities = append(plan.Entities, candidate)
			return nil
		}(); err != nil {
			return zero, err
		}
	}
	for _, record := range edgeRows {
		if err := appendExactRelation(&plan, archive, record); err != nil {
			return zero, err
		}
	}
	for _, d := range dispositions {
		sort.Slice(d.Statuses, func(i, j int) bool { return d.Statuses[i].From < d.Statuses[j].From })
		plan.Dispositions = append(plan.Dispositions, *d)
	}
	sort.Slice(plan.Dispositions, func(i, j int) bool { return plan.Dispositions[i].Key < plan.Dispositions[j].Key })
	sort.Slice(plan.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Entities[i].Source, plan.Entities[j].Source) < 0
	})
	sort.Slice(plan.Relations, func(i, j int) bool {
		a, b := plan.Relations[i].Source, plan.Relations[j].Source
		if a.Project != b.Project {
			return a.Project < b.Project
		}
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		return a.ID < b.ID
	})
	sidecars, err := archive.SourcePlaneSidecarOrigins()
	if err != nil {
		return zero, err
	}
	plan.Origins = append(plan.Origins, sidecars...)
	sort.Slice(plan.Origins, func(i, j int) bool {
		a, b := plan.Origins[i], plan.Origins[j]
		left, right := []string{a.Project, a.Table, a.ID, a.Path, a.Layer}, []string{b.Project, b.Table, b.ID, b.Path, b.Layer}
		for n := range left {
			if left[n] != right[n] {
				return left[n] < right[n]
			}
		}
		return false
	})
	plan.Target.StoreID, plan.Target.Incarnation = "", ""
	return request{Format: "plug-native-receiving-request/1", PolicySHA256: digest(policyBytes), ArchiveSHA256: policy.ArchiveSHA256, SourceStoreID: policy.SourceStoreID, Projects: projects, Admission: policy.Admission, IdempotencyKey: policy.IdempotencyKey, SourcePlan: plan}, nil
}

func exactFlag(value any) (bool, error) {
	switch value {
	case true, json.Number("1"):
		return true, nil
	case false, json.Number("0"):
		return false, nil
	default:
		return false, errors.New("source flag is not exact boolean or0/1")
	}
}

func fieldCommitments(raw []byte) ([]driver.SourcePlaneFieldV1, error) {
	return fieldCommitmentsAccounted(nil, nil, raw)
}

func fieldCommitmentsAccounted(owner, work *dumpformat.Archive, raw []byte) ([]driver.SourcePlaneFieldV1, error) {
	var wire struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	// This view selects only original raw field tokens; it does not reject the
	// other already validated record fields or normalize embedded JSON text.
	if work != nil {
		if err := work.ChargeNativeDeltaJSON("cli_mapper_field_tokens", json.RawMessage(raw)); err != nil {
			return nil, err
		}
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	result := []driver.SourcePlaneFieldV1{}
	for name, token := range wire.Source.Fields {
		value := bytes.TrimSpace(token)
		presence := "number"
		if len(value) == 0 {
			return nil, errors.New("empty raw source field")
		}
		switch value[0] {
		case '"':
			presence = "text"
			if work != nil {
				if err := work.ChargeNativeDeltaBytes("cli_mapper_text_token", uint64(len(token))); err != nil {
					return nil, err
				}
			}
			var text string
			if err := json.Unmarshal(token, &text); err != nil {
				return nil, err
			}
			token = []byte(text)
		case '{':
			presence = "object"
		case '[':
			presence = "array"
		case 'n':
			presence = "null"
		case 't', 'f':
			presence = "boolean"
		}
		candidate := driver.SourcePlaneFieldV1{Name: name, Value: driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(token)), SHA256: "0000000000000000000000000000000000000000000000000000000000000000"}}
		if owner != nil {
			if err := owner.ChargeNativeDeltaJSON("cli_mapper_field", candidate); err != nil {
				return nil, err
			}
		}
		candidate.Value.SHA256 = digest(token)
		result = append(result, candidate)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}
