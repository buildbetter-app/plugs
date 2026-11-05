package workflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

const SourceStateSchema = "workflow/source-state/1"

// sourceState is immutable import provenance, separate from mutable active
// items and their bounded receipts. Source actors never grant runtime authority.
// Keeping raw records here does not create graph records, versions or events.
type sourceState struct {
	Schema    string                         `json:"schema"`
	Entities  []driver.SourcePlaneEntityV1   `json:"entities"`
	Relations []driver.SourcePlaneRelationV1 `json:"relations"`
	Origins   []driver.SourcePlaneOriginV1   `json:"origins"`
	Records   []driver.SourcePlaneRecordV1   `json:"records"`
}

// PreparedSourceRestore owns an unpublished logical image. Accessors detach
// bytes so a caller cannot change a previously checked provider commitment.
type PreparedSourceRestore struct{ logical []byte }

func (s PreparedSourceRestore) LogicalImage() []byte { return bytes.Clone(s.logical) }
func (s PreparedSourceRestore) SHA256() string       { return sourceImageHash(s.logical) }

func sourceImageHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sourceRefusal(reason string) error {
	return fmt.Errorf("%w: workflow source restore: %s", driver.ErrUnsupported, reason)
}

// NewSourceRuntime accepts only the host facade already bound to this exact
// namespace/origin. Persistent remains the selected store's truthful metadata.
func NewSourceRuntime(state pluginapi.DurableState, binding driver.SourcePlaneProviderCommitV1) (*Provider, error) {
	selected, ok := state.(interface {
		SourceRuntimeBinding() (driver.SourcePlaneProviderCommitV1, error)
	})
	if !ok {
		return nil, sourceRefusal("missing trusted source runtime")
	}
	actual, err := selected.SourceRuntimeBinding()
	if err != nil {
		return nil, err
	}
	if actual != binding || binding.Provider.Alias != Alias || binding.Provider.Instance != "default" || binding.Provider.Namespace != "plugin/workflow/default/durable" || binding.Provider.Schema != SourceStateSchema || binding.Provider.Codec != "workflow/json/1" || binding.Provider.RuntimeContract != driver.SourcePlaneRuntimeContractV1 || binding.Provider.OriginAdapter != "beads-workflow-origins/1" || binding.Origin.Version != driver.SourcePlaneOriginVersionV1 || binding.Origin.Incarnation == "" || binding.Origin.Namespace != binding.Provider.Namespace || binding.Origin.Lifetime != binding.Provider.Lifetime {
		return nil, sourceRefusal("mismatched source runtime binding")
	}
	if err := binding.Provider.Lifetime.Validate(); err != nil {
		return nil, err
	}
	persistent := false
	if marker, ok := state.(persistentState); ok {
		persistent = marker.Persistent()
	}
	return &Provider{state: state, now: time.Now, persistent: persistent, plugin: Alias, instance: "default", incarnation: binding.Origin.Incarnation, source: &binding}, nil
}

type sourceWireRecord struct {
	raw     []byte
	Kind    string                     `json:"kind"`
	ID      string                     `json:"id"`
	Project string                     `json:"project"`
	Body    map[string]json.RawMessage `json:"body"`
	Source  struct {
		Table  string                     `json:"table"`
		Fields map[string]json.RawMessage `json:"fields"`
	} `json:"source"`
}

func sourceText(raw json.RawMessage) (string, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", sourceRefusal("source field is not text")
	}
	return text, nil
}

func sourceBool(raw json.RawMessage) (bool, error) {
	switch string(raw) {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	}
	return false, sourceRefusal("source flag is not a boolean or 0/1")
}

func sourceCommitment(raw []byte, commitment driver.SourcePlaneBytesV1, field bool) error {
	if err := commitment.Validate(); err != nil {
		return err
	}
	if commitment.Presence == "absent" {
		if raw != nil {
			return sourceRefusal("declared absent field is present")
		}
		return nil
	}
	if raw == nil {
		return sourceRefusal("committed source bytes are absent")
	}
	data := raw
	if field {
		trimmed := bytes.TrimSpace(raw)
		kind := "number"
		if len(trimmed) == 0 {
			return sourceRefusal("empty source JSON field")
		}
		switch trimmed[0] {
		case '"':
			kind = "text"
			value, err := sourceText(raw)
			if err != nil {
				return err
			}
			data = []byte(value)
		case '{':
			kind = "object"
		case '[':
			kind = "array"
		case 'n':
			kind = "null"
		case 't', 'f':
			kind = "boolean"
		}
		if commitment.Presence != kind {
			return sourceRefusal("source field presence/type differs")
		}
	} else if commitment.Presence != "object" && commitment.Presence != "bytes" {
		return sourceRefusal("raw record requires exact object/bytes commitment")
	}
	if uint64(len(data)) != commitment.Size || sourceImageHash(data) != commitment.SHA256 {
		return sourceRefusal("source byte commitment differs")
	}
	return nil
}

func sourceFieldCensus(fields map[string]json.RawMessage, commitments []driver.SourcePlaneFieldV1) error {
	seen := make(map[string]bool, len(commitments))
	for _, field := range commitments {
		if seen[field.Name] {
			return sourceRefusal("duplicate field commitment")
		}
		seen[field.Name] = true
		if err := sourceCommitment(fields[field.Name], field.Value, true); err != nil {
			return err
		}
	}
	for name := range fields {
		if !seen[name] {
			return sourceRefusal("incomplete source field census: " + name)
		}
	}
	return nil
}

func sourceWireRecords(input []driver.SourcePlaneRecordV1) (map[string]sourceWireRecord, []driver.SourcePlaneRecordV1, error) {
	if len(input) > driver.SourcePlaneMaxListEntries {
		return nil, nil, sourceRefusal("source record count limit")
	}
	byHash := make(map[string]sourceWireRecord, len(input))
	records := make([]driver.SourcePlaneRecordV1, len(input))
	total := 0
	for i, record := range input {
		total += len(record.Raw)
		if total > driver.SourcePlaneMaxPlanBytes {
			return nil, nil, sourceRefusal("source record byte limit")
		}
		var wire sourceWireRecord
		if err := decodeWorkflowJSON(record.Raw, &wire); err != nil {
			return nil, nil, err
		}
		if wire.Kind != record.Kind || wire.ID != record.ID || wire.Project != record.Project || wire.Source.Table != record.Table || wire.Source.Fields == nil || wire.Body == nil {
			return nil, nil, sourceRefusal("source DTO/wire mirror differs")
		}
		hash := sourceImageHash(record.Raw)
		if _, duplicate := byHash[hash]; duplicate {
			return nil, nil, sourceRefusal("duplicate source record")
		}
		wire.raw = bytes.Clone(record.Raw)
		byHash[hash] = wire
		record.Raw = bytes.Clone(record.Raw)
		records[i] = record
	}
	// Input file traversal is not logical order. Full tuple order and raw
	// bytes make the logical image deterministic without changing any record.
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i], records[j]
		for k, av := range []string{a.Project, a.Table, a.ID, a.Kind} {
			bv := []string{b.Project, b.Table, b.ID, b.Kind}[k]
			if av != bv {
				return av < bv
			}
		}
		return bytes.Compare(a.Raw, b.Raw) < 0
	})
	return byHash, records, nil
}

func sourceMatchedRecord(byHash map[string]sourceWireRecord, commitment driver.SourcePlaneBytesV1) (sourceWireRecord, error) {
	wire, ok := byHash[commitment.SHA256]
	if !ok {
		return wire, sourceRefusal("missing original record")
	}
	return wire, sourceCommitment(wire.raw, commitment, false)
}

// PrepareSourceRestore consumes explicit identities and declarations. It never
// invokes a workflow, reads a store, generates an ID or consults a clock.
// The caller binds the returned bytes in the final plan before host preparation.
func (p *Provider) PrepareSourceRestore(ctx context.Context, plan driver.SourcePlanePlanV1, records []driver.SourcePlaneRecordV1) (PreparedSourceRestore, error) {
	if err := ctx.Err(); err != nil {
		return PreparedSourceRestore{}, err
	}
	encoded, err := driver.EncodeSourcePlanePlanV1(plan)
	if err != nil {
		return PreparedSourceRestore{}, err
	}
	plan, err = driver.DecodeSourcePlanePlanV1(encoded)
	if err != nil {
		return PreparedSourceRestore{}, err
	}
	if len(plan.Providers) != 1 || plan.Providers[0].Alias != Alias || plan.Providers[0].Instance != "default" || plan.Providers[0].Schema != SourceStateSchema || plan.Providers[0].Namespace != "plugin/workflow/default/durable" || plan.Providers[0].Codec != "workflow/json/1" || plan.Providers[0].OriginAdapter != "beads-workflow-origins/1" {
		return PreparedSourceRestore{}, sourceRefusal("unsupported provider schema")
	}
	byHash, raw, err := sourceWireRecords(records)
	if err != nil {
		return PreparedSourceRestore{}, err
	}
	value := persisted{Runs: map[string]runState{}, Items: map[string]itemState{}, Receipts: map[string]receiptState{}, Source: &sourceState{Schema: SourceStateSchema, Entities: plan.Entities, Relations: plan.Relations, Origins: plan.Origins, Records: raw}}
	dispositions := map[string]driver.SourcePlaneDispositionV1{}
	for _, d := range plan.Dispositions {
		dispositions[d.Key] = d
	}
	principals := map[string]model.PrincipalRef{}
	for _, declaration := range plan.Principals {
		if declaration.UnverifiedOriginOnly {
			continue
		}
		var principal model.PrincipalRef
		if err := decodeWorkflowJSON(declaration.PrincipalJSON, &principal); err != nil {
			return PreparedSourceRestore{}, err
		}
		principals[declaration.Key] = principal
	}
	type runKey struct{ project, key string }
	runs := map[runKey]driver.SourcePlaneRunV1{}
	for _, run := range plan.Runs {
		if run.Schema != SourceStateSchema || run.Mode != "ephemeral" || run.State != "active" {
			return PreparedSourceRestore{}, sourceRefusal("unsupported explicit run")
		}
		principal, ok := principals[run.PrincipalKey]
		if !ok {
			return PreparedSourceRestore{}, sourceRefusal("run has no trusted principal")
		}
		key, err := driver.PrincipalKey(principal)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		runs[runKey{run.Project, run.Key}] = run
		value.Runs[run.TargetID] = runState{ID: run.TargetID, Template: run.Template, Project: run.Project, State: run.State, Owner: ownerState{Principal: key, Agent: effectiveAgent(principal, model.WriteMeta{}), Project: run.Project}}
	}
	entities := map[driver.SourceEntityKey]driver.SourcePlaneEntityV1{}
	used := map[string]bool{}
	for _, entity := range plan.Entities {
		if err := ctx.Err(); err != nil {
			return PreparedSourceRestore{}, err
		}
		wire, err := sourceMatchedRecord(byHash, entity.Origin)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		physicalID, err := sourceText(wire.Source.Fields["id"])
		if err != nil || physicalID != entity.Source.ID || wire.Source.Table != entity.Source.Plane || (wire.Project != "" && wire.Project != entity.Source.Project) {
			return PreparedSourceRestore{}, sourceRefusal("source entity address differs")
		}
		if err := sourceFieldCensus(wire.Source.Fields, entity.Fields); err != nil {
			return PreparedSourceRestore{}, err
		}
		used[entity.Origin.SHA256] = true
		entities[entity.Source] = entity
		if entity.TargetKind != "workflow:item" {
			continue
		}
		d := dispositions[entity.DispositionKey]
		if d.Lifetime != string(plan.Providers[0].Lifetime) {
			return PreparedSourceRestore{}, sourceRefusal("active disposition lifetime differs from selected provider")
		}
		item, created, err := sourceItem(entity, d, wire.Source.Fields)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		run := runs[runKey{entity.Source.Project, entity.RunKey}]
		if run.PrincipalKey != entity.PrincipalKey {
			return PreparedSourceRestore{}, sourceRefusal("entity/run principal differs")
		}
		stored := value.Runs[run.TargetID]
		item.RunID, item.Owner = run.TargetID, stored.Owner
		if stored.CreatedAt.IsZero() || created.Before(stored.CreatedAt) {
			stored.CreatedAt = created
		}
		value.Runs[run.TargetID] = stored
		value.Items[entity.TargetID] = item
	}
	for _, declared := range plan.Runs {
		run := value.Runs[declared.TargetID]
		for _, member := range declared.Members {
			run.ItemIDs = append(run.ItemIDs, entities[member].TargetID)
		}
		value.Runs[declared.TargetID] = run
	}
	if err := prepareSourceRelations(ctx, &value, plan, byHash, raw, entities, used); err != nil {
		return PreparedSourceRestore{}, err
	}
	if err := prepareSourceOrigins(ctx, &value, plan, byHash, raw, entities, used); err != nil {
		return PreparedSourceRestore{}, err
	}
	if err := validateSourceGraph(ctx, plan, entities); err != nil {
		return PreparedSourceRestore{}, err
	}
	for hash := range byHash {
		if !used[hash] {
			return PreparedSourceRestore{}, sourceRefusal("source record lacks an explicit disposition")
		}
	}
	logical, err := json.Marshal(value)
	if err != nil {
		return PreparedSourceRestore{}, err
	}
	if err := ctx.Err(); err != nil {
		return PreparedSourceRestore{}, err
	}
	return PreparedSourceRestore{logical: logical}, nil
}
