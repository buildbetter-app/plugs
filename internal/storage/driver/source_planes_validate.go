package driver

import (
	"bytes"
	"cmp"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

const sourcePlaneEphemeralDependencyTable = "wisp_dependencies"

// CompareSourceEntityKeys and CompareSourceEdgeKeys compare components,
// without delimiter concatenation or Unicode normalization. Source identity
// is never inferred from a target ref or a naked source ID.
func CompareSourceEntityKeys(a, b SourceEntityKey) int {
	if n := cmp.Compare(a.Project, b.Project); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Plane, b.Plane); n != 0 {
		return n
	}
	return cmp.Compare(a.ID, b.ID)
}
func CompareSourceEdgeKeys(a, b SourceEdgeKey) int {
	if n := cmp.Compare(a.Project, b.Project); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Table, b.Table); n != 0 {
		return n
	}
	return cmp.Compare(a.ID, b.ID)
}

func (k SourceEntityKey) Validate() error {
	if !sourcePlaneNames(k.Project, k.ID) || (k.Plane != "issues" && k.Plane != "wisps") {
		return sourcePlaneError("invalid source entity address")
	}
	return nil
}
func (k SourceEdgeKey) Validate() error {
	if !sourcePlaneNames(k.Project, k.ID) || (k.Table != "dependencies" && k.Table != sourcePlaneEphemeralDependencyTable) {
		return sourcePlaneError("invalid source edge address")
	}
	return nil
}

func sourcePlaneNames(values ...string) bool {
	for _, s := range values {
		if s == "" || !utf8.ValidString(s) {
			return false
		}
	}
	return true
}
func (v CheckpointLifetime) Validate() error {
	if v != CheckpointLifetimeProcess && v != CheckpointLifetimeRestart {
		return sourcePlaneError("unsupported checkpoint lifetime")
	}
	return nil
}

func sourcePlaneOrder[T any](items []T, key func(T) []string) error {
	if len(items) > SourcePlaneMaxListEntries {
		return sourcePlaneError("list count limit")
	}
	var previous []string
	for i, item := range items {
		current := key(item)
		for _, part := range current {
			if !utf8.ValidString(part) {
				return sourcePlaneError("invalid UTF-8 set key")
			}
		}
		if i > 0 {
			order := 0
			for j := range current {
				if order = cmp.Compare(previous[j], current[j]); order != 0 {
					break
				}
			}
			if order >= 0 {
				return sourcePlaneError("unsorted or duplicate full tuple")
			}
		}
		previous = current
	}
	return nil
}

func (v SourcePlaneBytesV1) Validate() error {
	if v.Presence == "absent" {
		if v.Size != 0 || v.SHA256 != "" {
			return sourcePlaneError("absent input has bytes")
		}
		return nil
	}
	switch v.Presence {
	case "null", "sql-null", "object", "text", "number", "boolean", "array", "bytes":
	default:
		return sourcePlaneError("unknown raw presence/type")
	}
	if !importDigest(v.SHA256) {
		return sourcePlaneError("invalid raw byte commitment")
	}
	return nil
}

func validateSourcePlaneFields(fields []SourcePlaneFieldV1) error {
	if err := sourcePlaneOrder(fields, func(v SourcePlaneFieldV1) []string { return []string{v.Name} }); err != nil {
		return err
	}
	for _, f := range fields {
		if f.Name == "" {
			return sourcePlaneError("empty source field")
		}
		if err := f.Value.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks the closed plan's identities, sorted sets, ordered run
// membership, single-arm relations, references, injectivity and commitments.
// Provider preparation must additionally verify each declared mapping against
// the actual admitted source and its supported semantic schema. This layer
// supplies no default type/status/run/principal mapping.
func (p SourcePlanePlanV1) Validate() error {
	if p.Version != SourcePlanePlanVersionV1 || p.AddressContract != SourcePlaneAddressContractV1 || ValidateCheckpointKey(p.SourceStoreID) != nil || !importDigest(p.ArchiveSHA256) {
		return sourcePlaneError("invalid plan version/source/archive")
	}
	if ValidateCheckpointKey(p.Target.StoreID) != nil || ValidateCheckpointKey(p.Target.Incarnation) != nil || p.Target.Head != 0 || !p.Target.RequireEmpty || p.Target.RuntimeContract != SourcePlaneRuntimeContractV1 {
		return sourcePlaneError("invalid empty target preconditions")
	}
	if err := p.Target.Lifetime.Validate(); err != nil {
		return err
	}
	if len(p.Providers) == 0 {
		return sourcePlaneError("missing provider participant")
	}
	if err := sourcePlaneOrder(p.Dispositions, func(v SourcePlaneDispositionV1) []string { return []string{v.Key} }); err != nil {
		return err
	}
	dispositions := make(map[string]SourcePlaneDispositionV1)
	dispositionTuples := make(map[struct {
		typ, storage         string
		ephemeral, noHistory bool
	}]bool)
	for _, d := range p.Dispositions {
		if !sourcePlaneNames(d.Key, d.SourceType, d.StorageClass, d.SinkKind, d.Schema, d.Lifetime, d.RefusalPolicy) {
			return sourcePlaneError("incomplete disposition")
		}
		key := struct {
			typ, storage         string
			ephemeral, noHistory bool
		}{d.SourceType, d.StorageClass, d.Ephemeral, d.NoHistory}
		if dispositionTuples[key] {
			return sourcePlaneError("duplicate observed disposition tuple")
		}
		dispositionTuples[key] = true
		if err := sourcePlaneOrder(d.Statuses, func(v SourcePlaneStringPairV1) []string { return []string{v.From} }); err != nil {
			return err
		}
		for _, status := range d.Statuses {
			if !sourcePlaneNames(status.From, status.To) {
				return sourcePlaneError("incomplete status correspondence")
			}
		}
		if err := sourcePlaneOrder(d.Fields, func(v SourcePlaneFieldRuleV1) []string { return []string{v.Source} }); err != nil {
			return err
		}
		for _, f := range d.Fields {
			if !sourcePlaneNames(f.Source, f.Disposition) {
				return sourcePlaneError("incomplete field disposition")
			}
		}
		dispositions[d.Key] = d
	}
	if err := sourcePlaneOrder(p.Principals, func(v SourcePlanePrincipalV1) []string { return []string{v.Key} }); err != nil {
		return err
	}
	principals := make(map[string]bool)
	for _, v := range p.Principals {
		if !sourcePlaneNames(v.Key, v.Source, v.Authority) {
			return sourcePlaneError("incomplete principal disposition")
		}
		if v.UnverifiedOriginOnly {
			if len(v.PrincipalJSON) != 0 {
				return sourcePlaneError("unverified origin cannot grant principal authority")
			}
		} else {
			var principal model.PrincipalRef
			if err := json.Unmarshal(v.PrincipalJSON, &principal); err != nil {
				return sourcePlaneError("invalid trusted principal")
			}
			if _, err := PrincipalKey(principal); err != nil {
				return err
			}
		}
		principals[v.Key] = !v.UnverifiedOriginOnly
	}
	if err := sourcePlaneOrder(p.Runs, func(v SourcePlaneRunV1) []string { return []string{v.Project, v.Key} }); err != nil {
		return err
	}
	type runKey struct{ project, key string }
	runs := make(map[runKey]SourcePlaneRunV1)
	runTargets := make(map[string]bool)
	membership := make(map[SourceEntityKey]runKey)
	for _, r := range p.Runs {
		if !sourcePlaneNames(r.Project, r.Key, r.TargetID, r.Template, r.Schema, r.Mode, r.State, r.PrincipalKey) || !strings.HasPrefix(r.TargetID, "workflow:run/") || !principals[r.PrincipalKey] || runTargets[r.TargetID] {
			return sourcePlaneError("invalid run declaration")
		}
		if len(r.Members) == 0 || len(r.Members) > SourcePlaneMaxListEntries {
			return sourcePlaneError("missing or excessive run membership")
		}
		for _, m := range r.Members {
			if err := m.Validate(); err != nil {
				return err
			}
			if m.Project != r.Project {
				return sourcePlaneError("cross-project run membership")
			}
			if _, exists := membership[m]; exists {
				return sourcePlaneError("duplicate ordered run membership")
			}
			membership[m] = runKey{r.Project, r.Key}
		}
		runs[runKey{r.Project, r.Key}] = r
		runTargets[r.TargetID] = true
	}
	if err := sourcePlaneOrder(p.Entities, func(v SourcePlaneEntityV1) []string { return []string{v.Source.Project, v.Source.Plane, v.Source.ID} }); err != nil {
		return err
	}
	entities := make(map[SourceEntityKey]SourcePlaneEntityV1)
	targets := make(map[struct{ kind, id string }]bool)
	type routeKey struct{ project, id string }
	routes := make(map[routeKey]SourcePlaneRouteV1)
	for _, e := range p.Entities {
		if err := e.Source.Validate(); err != nil {
			return err
		}
		d, ok := dispositions[e.DispositionKey]
		if !ok || !principals[e.PrincipalKey] || e.TargetID == "" || e.TargetKind != d.SinkKind {
			return sourcePlaneError("unbound entity disposition/principal/target")
		}
		target := struct{ kind, id string }{e.TargetKind, e.TargetID}
		if targets[target] {
			return sourcePlaneError("noninjective target mapping")
		}
		targets[target] = true
		if e.TargetKind == "workflow:item" {
			if !strings.HasPrefix(e.TargetID, "workflow:item/") {
				return sourcePlaneError("invalid active target ref")
			}
			if member, ok := membership[e.Source]; !ok || member != (runKey{e.Source.Project, e.RunKey}) {
				return sourcePlaneError("active entity lacks exact ordered run")
			}
			if _, ok := runs[runKey{e.Source.Project, e.RunKey}]; !ok {
				return sourcePlaneError("unknown active run")
			}
		} else if e.TargetKind == "bead" {
			if e.RunKey != "" || strings.HasPrefix(e.TargetID, "workflow:") {
				return sourcePlaneError("durable target has active address")
			}
			if _, exists := membership[e.Source]; exists {
				return sourcePlaneError("durable target has run membership")
			}
		} else {
			return sourcePlaneError("unsupported entity sink")
		}
		r := e.Route
		if r.Project != e.Source.Project || r.SourceID != e.Source.ID || r.Preference != "active-first" || (e.TargetKind == "workflow:item" && r.ActiveTargetID != e.TargetID) || (e.TargetKind == "bead" && r.DurableTargetID != e.TargetID) {
			return sourcePlaneError("invalid source-facing route/inverse")
		}
		key := routeKey{r.Project, r.SourceID}
		if prior, ok := routes[key]; ok && prior != r {
			return sourcePlaneError("inconsistent source-facing collision route")
		}
		routes[key] = r
		if err := e.Origin.Validate(); err != nil {
			return err
		}
		if e.Origin.Presence == "absent" {
			return sourcePlaneError("entity lacks raw origin")
		}
		if err := validateSourcePlaneFields(e.Fields); err != nil {
			return err
		}
		entities[e.Source] = e
	}
	for entity := range membership {
		if _, ok := entities[entity]; !ok {
			return sourcePlaneError("run contains unbound entity")
		}
	}
	for _, r := range routes {
		for _, arm := range []struct{ kind, id string }{{"workflow:item", r.ActiveTargetID}, {"bead", r.DurableTargetID}} {
			if arm.id == "" {
				continue
			}
			found := false
			for _, plane := range []string{"issues", "wisps"} {
				e, ok := entities[SourceEntityKey{Project: r.Project, Plane: plane, ID: r.SourceID}]
				found = found || ok && e.TargetKind == arm.kind && e.TargetID == arm.id
			}
			if !found {
				return sourcePlaneError("route alternative lacks exact inverse source project/ID")
			}
		}
	}
	if err := sourcePlaneOrder(p.Relations, func(v SourcePlaneRelationV1) []string { return []string{v.Source.Project, v.Source.Table, v.Source.ID} }); err != nil {
		return err
	}
	for _, r := range p.Relations {
		if err := r.Source.Validate(); err != nil {
			return err
		}
		plane := "issues"
		if r.Source.Table == sourcePlaneEphemeralDependencyTable {
			plane = "wisps"
		}
		if r.From.Project != r.Source.Project || r.From.Plane != plane {
			return sourcePlaneError("edge source plane mismatch")
		}
		if _, ok := entities[r.From]; !ok {
			return sourcePlaneError("dangling edge source")
		}
		if (r.To == nil) == (r.External == nil) {
			return sourcePlaneError("edge requires one target arm")
		}
		if r.To != nil {
			if err := r.To.Validate(); err != nil {
				return err
			}
			if _, ok := entities[*r.To]; !ok {
				return sourcePlaneError("dangling local edge arm")
			}
		} else if *r.External == "" || !utf8.ValidString(*r.External) {
			return sourcePlaneError("empty external edge arm")
		}
		if !sourcePlaneNames(r.Kind, r.PredicateContract) || (len(r.PredicateJSON) != 0 && !json.Valid(r.PredicateJSON)) {
			return sourcePlaneError("invalid detached edge predicate")
		}
		if err := r.Origin.Validate(); err != nil {
			return err
		}
		if r.Origin.Presence == "absent" {
			return sourcePlaneError("edge lacks raw origin")
		}
		if err := validateSourcePlaneFields(r.Fields); err != nil {
			return err
		}
	}
	if err := sourcePlaneOrder(p.Origins, func(v SourcePlaneOriginV1) []string { return []string{v.Project, v.Table, v.ID, v.Path, v.Layer} }); err != nil {
		return err
	}
	for _, o := range p.Origins {
		if !sourcePlaneNames(o.Table, o.ID, o.Action) {
			return sourcePlaneError("incomplete origin disposition")
		}
		if o.Owner != nil {
			if _, ok := entities[*o.Owner]; !ok {
				return sourcePlaneError("origin has unknown full owner tuple")
			}
		}
		if err := o.Raw.Validate(); err != nil {
			return err
		}
		if err := o.Parsed.Validate(); err != nil {
			return err
		}
		if err := validateSourcePlaneFields(o.Fields); err != nil {
			return err
		}
		seen := make(map[string]bool)
		for _, layer := range o.Precedence {
			if !sourcePlaneNames(layer) || seen[layer] {
				return sourcePlaneError("duplicate/empty ordered source layer")
			}
			seen[layer] = true
		}
	}
	if err := sourcePlaneOrder(p.Providers, func(v SourcePlaneProviderV1) []string { return []string{v.Namespace} }); err != nil {
		return err
	}
	for _, v := range p.Providers {
		if !sourcePlaneNames(v.Alias, v.Instance, v.Schema, v.Codec, v.OriginAdapter) || ValidateCheckpointKey(v.Namespace) != nil || v.RuntimeContract != SourcePlaneRuntimeContractV1 || v.Lifetime != p.Target.Lifetime || v.ExpectedGeneration != 0 || !importDigest(v.LogicalImageSHA256) {
			return sourcePlaneError("invalid provider commitment")
		}
		if err := v.Lifetime.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalJSON preserves absence of the optional binding and rejects a
// present null. Legacy profiles retain the same marshaled bytes and hashes.
func (p *RestoreImportProfile) UnmarshalJSON(data []byte) error {
	fields := map[string]bool{"admission": true, "sourceStoreId": true, "method": true, "idempotencyKey": true, "archiveDigest": true, "derivedDigest": true, "historyMode": true, "historyDigest": true, "historyTime": true, "sourcePlanes": true}
	if err := sourcePlaneJSONObject(data, fields, false); err != nil {
		return err
	}
	type wire RestoreImportProfile
	var value wire
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(&value); err != nil {
		return err
	}
	*p = RestoreImportProfile(value)
	return nil
}
