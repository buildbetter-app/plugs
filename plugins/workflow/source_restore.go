package workflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
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

// Count the exact existing marshalComment selection before a prepared logical
// image is allocated. The wire borrows fields; it does not clone comment bodies
// or call the aggregate marshaler to discover their size.
func (c commentState) NativeDeltaJSONSize() (uint64, error) {
	wire := sourceCommentWire{Body: c.Body, Time: c.Time, Source: c.Source}
	if c.Source == nil {
		wire.Principal = &c.Principal
	}
	return dumpformat.NativeDeltaJSONSize(wire)
}

// SourceReverseEntry identifies one imported workflow item in its physical
// source address domain. An absent item is terminal and has no retained body.
// Present closed items remain present until their whole run completes.
type SourceReverseEntry struct {
	TargetID string
	Present  bool
	Terminal bool
	Item     *ItemView
}

// SourceReverseImage is a detached census of one already pinned logical image.
// Entities includes every imported workflow:item declaration, including terminal
// identities. Present entries exhaust the current active item map. Relations
// contains current workflow-owned edges, including typed parent references.
// OriginSHA256 commits to the canonical immutable source schema, entities,
// relations, origins and original records. Compare it with the admitted initial
// preparation's inspection before using current state for reverse conversion.
// Mutable items, runs and receipts do not contribute to this digest.
type SourceReverseImage struct {
	OriginSHA256 string
	Entities     map[driver.SourceEntityKey]SourceReverseEntry
	Relations    []RelationView
}

// InspectSourceReverseImage never loads a provider, reads a store, samples a
// clock, collects receipts or publishes state. The caller must first bind these
// logical bytes to the original archive/plan and the host origin in the same
// SourcePlaneSnapshot as its core reader. Lifetime and authority remain those
// of that checked binding; neither is inferred from an imported source actor.
// Claims are returned exactly as stored, including expired claims, without
// applying a later wall clock to the pinned observation. The logical image
// must use the canonical encoding written by source preparation/runtime saves.
func InspectSourceReverseImage(logical []byte) (SourceReverseImage, error) {
	var empty SourceReverseImage
	if len(logical) == 0 || len(logical) > driver.SourcePlaneMaxPlanBytes || len(bytes.TrimSpace(logical)) == 0 || bytes.TrimSpace(logical)[0] != '{' {
		return empty, sourceRefusal("invalid reverse logical image")
	}
	var value persisted
	decoder := json.NewDecoder(bytes.NewReader(logical))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return empty, sourceRefusal("malformed reverse logical image: " + err.Error())
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return empty, sourceRefusal("trailing reverse logical image")
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(logical, canonical) {
		return empty, sourceRefusal("noncanonical or unsupported reverse logical fields")
	}
	if value.Source == nil || value.Source.Schema != SourceStateSchema || value.Items == nil || value.Runs == nil || value.Receipts == nil || len(value.Source.Entities) > driver.SourcePlaneMaxListEntries {
		return empty, sourceRefusal("incomplete source-restored reverse image")
	}
	byHash, _, err := sourceWireRecords(value.Source.Records)
	if err != nil {
		return empty, sourceRefusal("invalid reverse source records: " + err.Error())
	}
	result := SourceReverseImage{Entities: make(map[driver.SourceEntityKey]SourceReverseEntry)}
	byTarget := make(map[string]driver.SourcePlaneEntityV1, len(value.Source.Entities))
	sources := make(map[driver.SourceEntityKey]bool, len(value.Source.Entities))
	for _, entity := range value.Source.Entities {
		if err := entity.Source.Validate(); err != nil || sources[entity.Source] || entity.TargetID == "" {
			return empty, sourceRefusal("invalid or duplicate reverse source address")
		}
		if _, duplicate := byTarget[entity.TargetID]; duplicate {
			return empty, sourceRefusal("duplicate reverse target address")
		}
		ref := model.RefByID(entity.TargetID, model.Natural64(0))
		if err := validateRef(ref); err != nil || (entity.TargetKind != "workflow:item" && entity.TargetKind != "bead") || (entity.TargetKind == "workflow:item") != workflowTarget(ref) {
			return empty, sourceRefusal("invalid reverse target binding")
		}
		if entity.TargetKind == "bead" && driver.ValidateHierarchyRef(ref) != nil {
			return empty, sourceRefusal("invalid reverse ordinary target binding")
		}
		wire, err := sourceMatchedRecord(byHash, entity.Origin)
		if err != nil {
			return empty, sourceRefusal("invalid reverse entity origin: " + err.Error())
		}
		id, err := sourceText(wire.Source.Fields["id"])
		if err != nil || id != entity.Source.ID || wire.Source.Table != entity.Source.Plane || !wire.matchesProject(entity.Source.Project) {
			return empty, sourceRefusal("reverse entity origin address differs")
		}
		if err := sourceFieldCensus(wire.Source.Fields, entity.Fields); err != nil {
			return empty, sourceRefusal("invalid reverse entity fields: " + err.Error())
		}
		sources[entity.Source], byTarget[entity.TargetID] = true, entity
		if entity.TargetKind == "workflow:item" {
			if entity.RunKey == "" || entity.PrincipalKey == "" {
				return empty, sourceRefusal("incomplete reverse active binding")
			}
			result.Entities[entity.Source] = SourceReverseEntry{TargetID: entity.TargetID, Terminal: true}
		}
	}
	// Run keys belong to a project; a delimiter-joined key would alias distinct
	// physical source components. Current members must preserve that grouping.
	type runKey struct{ project, key string }
	runs := map[runKey]string{}
	runKeys := map[string]runKey{}
	runCounts := map[string]int{}
	for id, item := range value.Items {
		entity, ok := byTarget[id]
		run, runOK := value.Runs[item.RunID]
		validStatus := item.Status == "open" || item.Status == "closed" || item.Status == "claimed" && item.Claim != nil
		if !ok || entity.TargetKind != "workflow:item" || item.ID != id || item.Project != entity.Source.Project || item.Label != entity.Source.ID || strings.TrimSpace(item.Title) == "" || strings.TrimSpace(item.Type) == "" || !validStatus || item.Generation == 0 || !runOK || run.ID != item.RunID || run.Project != item.Project || run.Owner != item.Owner || item.Owner.Principal == "" || item.Owner.Project != item.Project {
			return empty, sourceRefusal("malformed or unmapped current reverse item")
		}
		key := runKey{entity.Source.Project, entity.RunKey}
		if prior, ok := runs[key]; ok && prior != run.ID {
			return empty, sourceRefusal("reverse source run split across current runs")
		}
		if prior, ok := runKeys[run.ID]; ok && prior != key {
			return empty, sourceRefusal("reverse source runs share a current run")
		}
		runs[key], runKeys[run.ID] = run.ID, key
		runCounts[run.ID]++
		if item.Claim != nil && (item.Claim.Principal.Validate() != nil || item.Claim.Generation == 0 || item.Claim.ExpiresAt.IsZero()) {
			return empty, sourceRefusal("malformed reverse claim")
		}
		for _, comment := range item.Comments {
			if comment.Source != nil {
				if comment.Source.Project != item.Project {
					return empty, sourceRefusal("reverse comment origin is outside its project")
				}
			} else if err := comment.Principal.Validate(); err != nil {
				return empty, sourceRefusal("malformed reverse comment principal")
			}
		}
		view := itemViewAt(item, time.Time{})
		if item.Claim != nil {
			view.Claim = &ClaimView{Principal: item.Claim.Principal, Agent: item.Claim.Agent, ExpiresAt: item.Claim.ExpiresAt, Generation: model.Natural64(item.Claim.Generation)}
		}
		result.Entities[entity.Source] = SourceReverseEntry{TargetID: id, Present: true, Item: &view}
	}
	const runPrefix = "workflow:run/"
	for id, run := range value.Runs {
		if id != run.ID || !strings.HasPrefix(id, runPrefix) || len(id) == len(runPrefix) || run.State != "active" || len(run.ItemIDs) == 0 {
			return empty, sourceRefusal("malformed current reverse run")
		}
		members := map[string]bool{}
		for _, member := range run.ItemIDs {
			item, ok := value.Items[member]
			if !ok || members[member] || item.RunID != id || item.Owner != run.Owner || item.Project != run.Project {
				return empty, sourceRefusal("reverse run has missing or duplicate members")
			}
			members[member] = true
		}
		if len(members) != runCounts[id] {
			return empty, sourceRefusal("reverse run omits a current member")
		}
	}
	known := func(id string) bool {
		entity, ok := byTarget[id]
		return ok && (entity.TargetKind == "bead" || result.Entities[entity.Source].Present)
	}
	type edgeIndexes struct {
		edge              relationState
		outbound, inbound bool
	}
	edges := map[string]edgeIndexes{}
	for id, item := range value.Items {
		for index, links := range [][]relationState{item.Links, item.Inbound} {
			seen := map[string]bool{}
			for _, link := range links {
				if !known(link.From) || !known(link.To.ID) || link.From == link.To.ID || index == 0 && link.From != id || index == 1 && link.To.ID != id {
					return empty, sourceRefusal("unmapped or inconsistent reverse relation")
				}
				if err := validateRelation(RelationSpec{To: link.To, Kind: link.Kind, Metadata: link.Metadata}); err != nil {
					return empty, sourceRefusal("malformed reverse relation: " + err.Error())
				}
				key := relationKey(link)
				if seen[key] {
					return empty, sourceRefusal("duplicate reverse relation index")
				}
				seen[key] = true
				entry := edges[key]
				entry.edge = link
				if index == 0 {
					entry.outbound = true
				} else {
					entry.inbound = true
				}
				edges[key] = entry
			}
		}
	}
	type parentEdge struct{ from, to string }
	parents := map[parentEdge]bool{}
	for _, entry := range edges {
		from, to := byTarget[entry.edge.From], byTarget[entry.edge.To.ID]
		if (from.TargetKind == "workflow:item") != entry.outbound || (to.TargetKind == "workflow:item") != entry.inbound {
			return empty, sourceRefusal("incomplete reverse relation indexes")
		}
		if entry.edge.Kind == "parent-child" {
			parents[parentEdge{entry.edge.From, entry.edge.To.ID}] = true
		}
	}
	// A public parent update can store the typed parent independently of an
	// auxiliary link. Include that current field without inventing a raw edge ID.
	for id, item := range value.Items {
		if item.Parent == nil {
			continue
		}
		if validateRef(*item.Parent) != nil || !known(item.Parent.ID) || item.Parent.ID == id {
			return empty, sourceRefusal("malformed or unmapped reverse parent")
		}
		if !parents[parentEdge{id, item.Parent.ID}] {
			link := relationState{From: id, To: *item.Parent, Kind: "parent-child"}
			edges[relationKey(link)] = edgeIndexes{edge: link}
		}
	}
	keys := make([]string, 0, len(edges))
	for key := range edges {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Relations = append(result.Relations, relationView(edges[key].edge))
	}
	origin, err := json.Marshal(value.Source)
	if err != nil {
		return empty, sourceRefusal("invalid reverse immutable origin: " + err.Error())
	}
	result.OriginSHA256 = sourceImageHash(origin)
	return result, nil
}

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
		Table    string                     `json:"table"`
		Fields   map[string]json.RawMessage `json:"fields"`
		Identity *dumpformat.SourceIdentity `json:"identity,omitempty"`
	} `json:"source"`
}

func (wire sourceWireRecord) matchesProject(project string) bool {
	if wire.Source.Identity != nil {
		return wire.Source.Identity.Project == project && (wire.Project == "" || wire.Project == project)
	}
	return wire.Project == "" || wire.Project == project
}

func sourceText(raw json.RawMessage, contexts ...context.Context) (string, error) {
	if len(contexts) != 0 {
		ctx := contexts[0]
		if budget := dumpformat.NativeDeltaBudgetFromContext(ctx); budget != nil {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			n, err := dumpformat.NativeJSONStringBytes(raw)
			if err != nil {
				return "", err
			}
			if err := budget.ChargeBytes("workflow_source_text", n); err != nil {
				return "", err
			}
		}
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", sourceRefusal("source field is not text")
	}
	return text, nil
}

func sourceEntityID(raw json.RawMessage, ctx context.Context) (string, error) {
	id, err := sourceText(raw, ctx)
	if err != nil {
		if canceled := ctx.Err(); canceled != nil {
			return "", canceled
		}
		var limit *dumpformat.NativeDeltaDecodedLimitError
		if errors.As(err, &limit) {
			return "", err
		}
		// Preserve the original entity-address diagnostic for malformed IDs.
		return "", sourceRefusal("source entity address differs")
	}
	return id, nil
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

func sourceCommitment(raw []byte, commitment driver.SourcePlaneBytesV1, field bool, contexts ...context.Context) error {
	ctx := context.Background()
	if len(contexts) != 0 {
		ctx = contexts[0]
	}
	ctx, release := dumpformat.WithNativeDeltaAccountingScope(ctx)
	defer release()

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
			value, err := sourceText(raw, ctx)
			if err != nil {
				return err
			}
			if err := sourceRestoreChargeBytes(ctx, "workflow_commitment_text_bytes", uint64(len(value))); err != nil {
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
	if err := sourceRestoreChargeBytes(ctx, "workflow_commitment_digest", sha256.Size*2); err != nil {
		return err
	}
	if uint64(len(data)) != commitment.Size || sourceImageHash(data) != commitment.SHA256 {
		return sourceRefusal("source byte commitment differs")
	}
	return nil
}

func sourceFieldCensus(fields map[string]json.RawMessage, commitments []driver.SourcePlaneFieldV1, contexts ...context.Context) error {
	seen := make(map[string]bool, len(commitments))
	for _, field := range commitments {
		if seen[field.Name] {
			return sourceRefusal("duplicate field commitment")
		}
		seen[field.Name] = true
		if err := sourceCommitment(fields[field.Name], field.Value, true, contexts...); err != nil {
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

// Optional accounting uses the receiving operation's existing child scope.
// These helpers do nothing without that budget; original APIs/wire bytes and
// validation remain unchanged. JSON domains are compact encoding/json plus LF;
// raw clones and binary source-plan buffers count their exact byte lengths.
func sourceRestoreChargeJSON(ctx context.Context, phase string, value any) error {
	if budget := dumpformat.NativeDeltaBudgetFromContext(ctx); budget != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return budget.ChargeJSON(phase, value)
	}
	return nil
}

func sourceRestoreChargeBytes(ctx context.Context, phase string, size uint64) error {
	if budget := dumpformat.NativeDeltaBudgetFromContext(ctx); budget != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return budget.ChargeBytes(phase, size)
	}
	return nil
}

func sourceWireRecords(input []driver.SourcePlaneRecordV1, contexts ...context.Context) (map[string]sourceWireRecord, []driver.SourcePlaneRecordV1, error) {
	ctx := context.Background()
	if len(contexts) != 0 {
		ctx = contexts[0]
	}

	if len(input) > driver.SourcePlaneMaxListEntries {
		return nil, nil, sourceRefusal("source record count limit")
	}
	byHash := make(map[string]sourceWireRecord, len(input))
	stores := map[string]string{}
	records := make([]driver.SourcePlaneRecordV1, len(input))
	total := 0
	for i, record := range input {
		total += len(record.Raw)
		if total > driver.SourcePlaneMaxPlanBytes {
			return nil, nil, sourceRefusal("source record byte limit")
		}
		if err := sourceRestoreChargeJSON(ctx, "workflow_wire_decoded", json.RawMessage(record.Raw)); err != nil {
			return nil, nil, err
		}
		var wire sourceWireRecord
		if err := decodeWorkflowJSON(record.Raw, &wire); err != nil {
			return nil, nil, err
		}
		if wire.Kind != record.Kind || wire.ID != record.ID || wire.Project != record.Project || wire.Source.Table != record.Table || wire.Source.Fields == nil || wire.Body == nil {
			return nil, nil, sourceRefusal("source DTO/wire mirror differs")
		}
		if identity := wire.Source.Identity; identity != nil {
			if wire.Project != "" && wire.Project != identity.Project {
				return nil, nil, sourceRefusal("source identity project differs from record")
			}
			if store, exists := stores[identity.Project]; exists && store != identity.StoreID {
				return nil, nil, sourceRefusal("ambiguous source stores for one project")
			}
			stores[identity.Project] = identity.StoreID
		}
		if err := sourceRestoreChargeBytes(ctx, "workflow_wire_hash_keys", sha256.Size*2); err != nil {
			return nil, nil, err
		}
		hash := sourceImageHash(record.Raw)
		if _, duplicate := byHash[hash]; duplicate {
			return nil, nil, sourceRefusal("duplicate source record")
		}
		if err := sourceRestoreChargeBytes(ctx, "workflow_wire_raw_clone", uint64(len(record.Raw))); err != nil {
			return nil, nil, err
		}
		wire.raw = bytes.Clone(record.Raw)
		byHash[hash] = wire
		if err := sourceRestoreChargeBytes(ctx, "workflow_origin_raw_clone", uint64(len(record.Raw))); err != nil {
			return nil, nil, err
		}
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

func sourceMatchedRecord(byHash map[string]sourceWireRecord, commitment driver.SourcePlaneBytesV1, contexts ...context.Context) (sourceWireRecord, error) {
	wire, ok := byHash[commitment.SHA256]
	if !ok {
		return wire, sourceRefusal("missing original record")
	}
	return wire, sourceCommitment(wire.raw, commitment, false, contexts...)
}

// PrepareSourceRestore consumes explicit identities and declarations. It never
// invokes a workflow, reads a store, generates an ID or consults a clock.
// The caller binds the returned bytes in the final plan before host preparation.
func (p *Provider) PrepareSourceRestore(ctx context.Context, plan driver.SourcePlanePlanV1, records []driver.SourcePlaneRecordV1) (PreparedSourceRestore, error) {
	if err := ctx.Err(); err != nil {
		return PreparedSourceRestore{}, err
	}
	// Work owns the detached plan, decoded wires, indexes and projected state.
	// Only the complete prepared logical byte buffer transfers to the caller.
	var work, output *dumpformat.NativeDeltaBudget
	var planBytes uint64
	if parent := dumpformat.NativeDeltaBudgetFromContext(ctx); parent != nil {
		work, output = parent.Scope(), parent.Scope()
		defer work.Release()
		defer output.Release()
		ctx = dumpformat.WithNativeDeltaBudget(ctx, work)
		n, err := dumpformat.NativeSourcePlanePlanSize(plan)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		planBytes = n
		for _, phase := range []string{"workflow_plan_encoded", "workflow_plan_decoded", "workflow_plan_canonical_check"} {
			if err := work.ChargeBytes(phase, n); err != nil {
				return PreparedSourceRestore{}, err
			}
		}
	}
	encoded, err := driver.EncodeSourcePlanePlanV1(plan)
	if err != nil {
		return PreparedSourceRestore{}, err
	}
	plan, err = driver.DecodeSourcePlanePlanV1(encoded)
	if err != nil {
		return PreparedSourceRestore{}, err
	}
	if work != nil {
		encoded = nil
		work.ReleaseBytes("workflow_plan_encoded", planBytes)
		work.ReleaseBytes("workflow_plan_canonical_check", planBytes)
	}
	if len(plan.Providers) != 1 || plan.Providers[0].Alias != Alias || plan.Providers[0].Instance != "default" || plan.Providers[0].Schema != SourceStateSchema || plan.Providers[0].Namespace != "plugin/workflow/default/durable" || plan.Providers[0].Codec != "workflow/json/1" || plan.Providers[0].OriginAdapter != "beads-workflow-origins/1" {
		return PreparedSourceRestore{}, sourceRefusal("unsupported provider schema")
	}
	byHash, raw, err := sourceWireRecords(records, ctx)
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
		if err := sourceRestoreChargeJSON(ctx, "workflow_principal_decoded", json.RawMessage(declaration.PrincipalJSON)); err != nil {
			return PreparedSourceRestore{}, err
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
		if err := sourceRestoreChargeJSON(ctx, "workflow_principal_key_validation", principal); err != nil {
			return PreparedSourceRestore{}, err
		}
		if err := sourceRestoreChargeBytes(ctx, "workflow_principal_keys", uint64(len(principal.Kind))+1+uint64(len(principal.ID))); err != nil {
			return PreparedSourceRestore{}, err
		}
		key, err := driver.PrincipalKey(principal)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		if work != nil {
			n, err := dumpformat.NativeDeltaJSONSize(principal)
			if err != nil {
				return PreparedSourceRestore{}, err
			}
			work.ReleaseBytes("workflow_principal_key_validation", n)
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
		wire, err := sourceMatchedRecord(byHash, entity.Origin, ctx)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		physicalID, err := sourceEntityID(wire.Source.Fields["id"], ctx)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		if physicalID != entity.Source.ID || wire.Source.Table != entity.Source.Plane || !wire.matchesProject(entity.Source.Project) {
			return PreparedSourceRestore{}, sourceRefusal("source entity address differs")
		}
		if err := sourceFieldCensus(wire.Source.Fields, entity.Fields, ctx); err != nil {
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
		item, created, err := sourceItem(ctx, entity, d, wire.Source.Fields)
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
	if output != nil {
		n, err := dumpformat.NativeDeltaJSONSize(value)
		if err != nil {
			return PreparedSourceRestore{}, err
		}
		if err := output.ChargeBytes("workflow_prepared_logical", n-1); err != nil {
			return PreparedSourceRestore{}, err
		}
	}
	logical, err := json.Marshal(value)
	if err != nil {
		return PreparedSourceRestore{}, err
	}
	if err := ctx.Err(); err != nil {
		return PreparedSourceRestore{}, err
	}
	if output != nil {
		output.Retain()
	}
	return PreparedSourceRestore{logical: logical}, nil
}
