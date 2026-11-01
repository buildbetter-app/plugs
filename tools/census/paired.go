package census

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

const PairedFormat = "plug-paired-census/1"
const PreservationScope = "source-preservation-and-live-head/1"

// The workflow plane is reported separately from the eight preservation
// metrics. A metric digest can prove that source dispositions were included,
// but it cannot tell a reviewer which workflow family was carried, archived,
// rebuilt or refused. Keep this vocabulary fixed and public so the field is a
// sanitized explanation rather than a second source-data channel.
var workflowDispositionFamilies = []string{
	"\x77isp_child_counters", "\x77isp_comments", "\x77isp_dependencies", "\x77isp_events",
	"\x77isp_labels", "\x77isps",
}

// Workflow dispositions are a public, sanitized ledger. The source archive
// may carry arbitrary reason text, so only this closed vocabulary can cross
// the census boundary.
const (
	workflowDispositionAction = "refuse-if-nonempty"
	workflowDispositionReason = "workflow/overlay/routing/audit/credential mapping is unsupported in this slice"
)

// Stages are deliberately fixed. Two disposable copies of one source are
// observations of one alias, never two inventory entries.
var requiredStages = []string{"schema-inventory", "export", "archive-validation", "memory", "sqlite", "postgres", "reverse", "readback", "history-range"}
var dimensions = []string{"counts", "ids", "prefixes", "counters", "dependencies", "versions", "archives", "digests"}

// BlockCode is closed vocabulary. It cannot carry an error containing private
// rows, paths, machine names, prefixes, credentials or command output.
type BlockCode string

const (
	MissingInventory      BlockCode = "authoritative-inventory-incomplete"
	MissingObservation    BlockCode = "observation-unavailable"
	SchemaDrift           BlockCode = "paired-schema-drift"
	ExportUnavailable     BlockCode = "export-unavailable"
	ArchiveInvalid        BlockCode = "archive-invalid"
	CapabilityUnavailable BlockCode = "driver-capability-unavailable"
	ReverseUnavailable    BlockCode = "reverse-unqualified"
	ReadbackUnavailable   BlockCode = "readback-unqualified"
	HistoryUnavailable    BlockCode = "history-range-unverified"
	AllocatorUnproven     BlockCode = "allocator-continuation-unproven"
	MeasurementInvalid    BlockCode = "measurement-invalid"
	TargetDiffers         BlockCode = "target-differs"
	SourceDiffers         BlockCode = "source-binding-differs"
	FullCardUnqualified   BlockCode = "full-card-twin-unqualified"
)

func validBlock(code BlockCode) bool {
	switch code {
	case MissingInventory, MissingObservation, SchemaDrift, ExportUnavailable, ArchiveInvalid,
		CapabilityUnavailable, ReverseUnavailable, ReadbackUnavailable, HistoryUnavailable,
		AllocatorUnproven, MeasurementInvalid, TargetDiffers, SourceDiffers, FullCardUnqualified:
		return true
	}
	return false
}

// CensusError intentionally discards upstream error text at the measurement
// boundary. Even malformed input and filesystem errors stay safe to log.
type CensusError struct{ Code BlockCode }

func (e CensusError) Error() string { return "paired census: " + string(e.Code) }
func refused(code BlockCode) error  { return CensusError{Code: code} }

type StoreInventory struct {
	Format string `json:"format"`
	// Complete is an owner's inventory assertion, not something discovered by
	// this tool. False keeps the unknown-source boundary visible in the result.
	Complete bool         `json:"complete"`
	Stores   []StoreAlias `json:"stores"`
}

type StoreAlias struct {
	Alias string `json:"alias"`
}

type SchemaPair struct {
	Main          int    `json:"main"`
	Ignored       int    `json:"ignored"`
	MainSHA256    string `json:"mainSHA256"`
	IgnoredSHA256 string `json:"ignoredSHA256"`
}

type Metric struct {
	Dimension string    `json:"dimension"`
	Count     int       `json:"count"`
	SHA256    string    `json:"sha256"`
	Block     BlockCode `json:"block,omitempty"`
}

type NativeHistory struct {
	Head       string `json:"head"`
	Floor      string `json:"floor"`
	Versions   int    `json:"versions"`
	Changesets int    `json:"changesets"`
	SHA256     string `json:"sha256"`
}

type WorkflowDisposition struct {
	Family string `json:"family"`
	Rows   int    `json:"rows"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

// Snapshot exposes counts, digests and the fixed workflow disposition ledger. The
// source's identity, prefix, source-field and content values never leave the
// measuring process.
type Snapshot struct {
	Scope                string                `json:"scope"`
	SourceSHA256         string                `json:"sourceSHA256"`
	Schema               SchemaPair            `json:"schema"`
	Metrics              []Metric              `json:"metrics"`
	WorkflowDispositions []WorkflowDisposition `json:"workflowDispositions"`
	NativeHistory        *NativeHistory        `json:"nativeHistory,omitempty"`
}

type Observation struct {
	Alias    string    `json:"alias"`
	Stage    string    `json:"stage"`
	Snapshot *Snapshot `json:"snapshot,omitempty"`
	Block    BlockCode `json:"block,omitempty"`
}

type PairedInput struct {
	Format       string        `json:"format"`
	Observations []Observation `json:"observations"`
}

type StageResult struct {
	Stage                string                `json:"stage"`
	Status               string                `json:"status"`
	Block                BlockCode             `json:"block,omitempty"`
	Metrics              []Metric              `json:"metrics,omitempty"`
	WorkflowDispositions []WorkflowDisposition `json:"workflowDispositions,omitempty"`
}

type StoreResult struct {
	Alias  string        `json:"alias"`
	Status string        `json:"status"`
	Stages []StageResult `json:"stages"`
}

type PairedResult struct {
	Format          string        `json:"format"`
	Status          string        `json:"status"`
	InventorySHA256 string        `json:"inventorySHA256"`
	InventoryBlock  BlockCode     `json:"inventoryBlock,omitempty"`
	Stores          []StoreResult `json:"stores"`
}

var aliasPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)

func member(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func validSnapshot(s Snapshot) bool {
	if s.Scope != PreservationScope || !sha256Pattern.MatchString(s.SourceSHA256) ||
		s.Schema.Main != ExpectedMainMigrations || s.Schema.Ignored != ExpectedIgnoredMigrations ||
		!sha256Pattern.MatchString(s.Schema.MainSHA256) || !sha256Pattern.MatchString(s.Schema.IgnoredSHA256) || len(s.Metrics) != len(dimensions) || len(s.WorkflowDispositions) != len(workflowDispositionFamilies) {
		return false
	}
	seen := map[string]bool{}
	for _, m := range s.Metrics {
		if !member(dimensions, m.Dimension) || seen[m.Dimension] || m.Count < 0 || !sha256Pattern.MatchString(m.SHA256) || (m.Block != "" && !validBlock(m.Block)) {
			return false
		}
		seen[m.Dimension] = true
	}
	for i, disposition := range s.WorkflowDispositions {
		if disposition.Family != workflowDispositionFamilies[i] || disposition.Rows < 0 || disposition.Action != workflowDispositionAction || disposition.Reason != workflowDispositionReason {
			return false
		}
	}
	if h := s.NativeHistory; h != nil {
		if h.Head == "" || h.Floor == "" || h.Versions < 0 || h.Changesets < 0 || !sha256Pattern.MatchString(h.SHA256) {
			return false
		}
	}
	return true
}

// DecodePairedJSON is strict and bounded. It never returns parser messages,
// which can contain private JSON member names or values.
func DecodePairedJSON(r io.Reader, target any) error {
	if r == nil {
		return refused(MeasurementInvalid)
	}
	raw, err := io.ReadAll(io.LimitReader(r, (8<<20)+1))
	if err != nil || len(raw) > 8<<20 || rejectDuplicateMembers(raw) != nil {
		return refused(MeasurementInvalid)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return refused(MeasurementInvalid)
	}
	var extra any
	if !errors.Is(d.Decode(&extra), io.EOF) {
		return refused(MeasurementInvalid)
	}
	var fields any
	if json.Unmarshal(raw, &fields) != nil || !exactJSONFields(fields, reflect.TypeOf(target)) {
		return refused(MeasurementInvalid)
	}
	return nil
}

// encoding/json otherwise accepts case aliases and omitted scalar fields.
// Require the exact tagged vocabulary, including an explicit zero count.
func exactJSONFields(value any, typ reflect.Type) bool {
	if value == nil || typ == nil {
		return false
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		allowed := map[string]reflect.Type{}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			parts := strings.Split(field.Tag.Get("json"), ",")
			name := parts[0]
			if name == "" || name == "-" {
				return false
			}
			allowed[name] = field.Type
			if len(parts) == 1 {
				if _, present := object[name]; !present {
					return false
				}
			}
		}
		for name, item := range object {
			field, ok := allowed[name]
			if !ok || !exactJSONFields(item, field) {
				return false
			}
		}
	case reflect.Slice:
		items, ok := value.([]any)
		if !ok {
			return false
		}
		for _, item := range items {
			if !exactJSONFields(item, typ.Elem()) {
				return false
			}
		}
	}
	return true
}

// Reconcile accounts for every declared source and every required stage.
// Missing work is a typed BLOCK. A matching digest is limited to this tool's
// measured scope; it never promotes a bounded head into full history parity.
func Reconcile(inventory StoreInventory, input PairedInput) (PairedResult, error) {
	if inventory.Format != PairedFormat || input.Format != PairedFormat || len(inventory.Stores) == 0 {
		return PairedResult{}, refused(MeasurementInvalid)
	}
	aliases := map[string]bool{}
	for _, s := range inventory.Stores {
		if !aliasPattern.MatchString(s.Alias) || aliases[s.Alias] {
			return PairedResult{}, refused(MeasurementInvalid)
		}
		aliases[s.Alias] = true
	}
	observed := map[string]Observation{}
	for _, o := range input.Observations {
		key := o.Alias + "/" + o.Stage
		if !aliases[o.Alias] || !member(requiredStages, o.Stage) {
			return PairedResult{}, refused(MeasurementInvalid)
		}
		if _, duplicate := observed[key]; duplicate {
			return PairedResult{}, refused(MeasurementInvalid)
		}
		if (o.Snapshot == nil) == (o.Block == "") || (o.Block != "" && !validBlock(o.Block)) || (o.Snapshot != nil && !validSnapshot(*o.Snapshot)) {
			return PairedResult{}, refused(MeasurementInvalid)
		}
		observed[key] = o
	}
	canonical := inventory
	canonical.Stores = append([]StoreAlias(nil), inventory.Stores...)
	sort.Slice(canonical.Stores, func(i, j int) bool { return canonical.Stores[i].Alias < canonical.Stores[j].Alias })
	result := PairedResult{Format: PairedFormat, Status: "PASS", InventorySHA256: jsonDigest(canonical)}
	if !inventory.Complete {
		result.InventoryBlock = MissingInventory
		result.Status = "BLOCK"
	}
	for _, item := range canonical.Stores {
		store := StoreResult{Alias: item.Alias, Status: "PASS"}
		baseline := observed[item.Alias+"/archive-validation"].Snapshot
		for _, stage := range requiredStages {
			r := StageResult{Stage: stage, Status: "PASS"}
			o, ok := observed[item.Alias+"/"+stage]
			switch {
			case !ok:
				r.Block = MissingObservation
			case o.Block != "":
				r.Block = o.Block
			case baseline == nil:
				r.Block = MissingObservation
			case o.Snapshot.SourceSHA256 != baseline.SourceSHA256:
				r.Block = SourceDiffers
			case o.Snapshot.Schema != baseline.Schema:
				r.Block = SchemaDrift
			default:
				r.WorkflowDispositions = append([]WorkflowDisposition(nil), o.Snapshot.WorkflowDispositions...)
				if !reflect.DeepEqual(o.Snapshot.WorkflowDispositions, baseline.WorkflowDispositions) {
					r.Block = TargetDiffers
				}
				byName := map[string]Metric{}
				for _, m := range baseline.Metrics {
					byName[m.Dimension] = m
				}
				for _, name := range dimensions {
					var actual Metric
					for _, m := range o.Snapshot.Metrics {
						if m.Dimension == name {
							actual = m
							break
						}
					}
					want := byName[name]
					if actual.Block == "" {
						actual.Block = want.Block
					}
					if actual.Count != want.Count || actual.SHA256 != want.SHA256 {
						actual.Block = TargetDiffers
					}
					if actual.Block != "" {
						r.Status = "BLOCK"
					}
					r.Metrics = append(r.Metrics, actual)
				}
			}
			if r.Block != "" {
				r.Status = "BLOCK"
			}
			if r.Status == "BLOCK" {
				store.Status = "BLOCK"
				result.Status = "BLOCK"
			}
			store.Stages = append(store.Stages, r)
		}
		result.Stores = append(result.Stores, store)
	}
	return result, nil
}
