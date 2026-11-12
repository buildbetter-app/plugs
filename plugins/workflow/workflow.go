// Package workflow implements the compiled-in workflow plugin's Option C
// operational plane. It stores only active runs and bounded terminal receipts
// through pluginapi.DurableState; no graph record or history writer is used.
package workflow

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

const (
	Alias      = "workflow"
	ItemPrefix = "workflow:item/"
	// RunKind and ItemKind are the provider's durable user-content kinds. The
	// native creation guard receives these from the actual create paths; it does
	// not classify method names or a service-side example allowlist.
	RunKind  = "workflow.run"
	ItemKind = "workflow.item"
	stateKey = "workflow/state"
	claimTTL = 30 * time.Minute
	// ClaimTTL is the original duration recorded in workflow claim responses.
	// Replays retain that duration instead of recomputing a remaining lease.
	ClaimTTL = claimTTL
)

type ChecklistItem struct {
	Label    string         `json:"label"`
	Title    string         `json:"title"`
	Type     string         `json:"type,omitempty"`
	Fields   map[string]any `json:"fields,omitempty"`
	Labels   []string       `json:"labels,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Parent   *model.Ref     `json:"parent,omitempty"`
	Links    []RelationSpec `json:"links,omitempty"`
	Blocked  bool           `json:"blocked,omitempty"`
}

// RelationSpec is the typed, plugin-owned auxiliary edge accepted at
// invocation/update boundaries. The workflow provider stores these edges in
// its checkpoint, so an ephemeral endpoint never creates a core link row.
type RelationSpec struct {
	To       model.Ref           `json:"to"`
	Kind     string              `json:"kind"`
	Metadata *model.LinkMetadata `json:"metadata,omitempty"`
}

type RelationView struct {
	From     string              `json:"from"`
	To       model.Ref           `json:"to"`
	Kind     string              `json:"kind"`
	Metadata *model.LinkMetadata `json:"metadata,omitempty"`
}

// LinkRequest is the public typed relation seam used by the standard service
// adapter. A workflow endpoint may be paired with an ordinary bead endpoint;
// the ordinary endpoint is represented only as a reference in this checkpoint.
type LinkRequest struct {
	From     model.Ref           `json:"from"`
	To       model.Ref           `json:"to"`
	Kind     string              `json:"kind"`
	Metadata *model.LinkMetadata `json:"metadata,omitempty"`
}

// PromoteRequest names the active workflow item and the ordinary bead that the
// caller created (or reserved) for it. The provider moves its auxiliary
// relationships in one checkpoint CAS; the core writer owns the ordinary row.
type PromoteRequest struct {
	Item   model.Ref `json:"item"`
	Target model.Ref `json:"target"`
}

type PromoteResult struct {
	Source     model.Ref      `json:"source"`
	Target     model.Ref      `json:"target"`
	Relations  []RelationView `json:"relations,omitempty"`
	ChildCount uint64         `json:"childCount,omitempty"`
	Generation model.Revision `json:"generation"`
}

// DemoteRequest names the durable source and the new run's template and label.
// Projection fields are host-only, read from the authoritative ordinary source
// by Service and published together with its deletion through DemoteAtomic.
type DemoteRequest struct {
	Source       model.Ref      `json:"source"`
	Template     string         `json:"template"`
	Project      string         `json:"-"`
	Label        string         `json:"label"`
	Title        string         `json:"-"`
	Type         string         `json:"-"`
	Fields       map[string]any `json:"-"`
	Labels       []string       `json:"-"`
	Metadata     map[string]any `json:"-"`
	Parent       *model.Ref     `json:"-"`
	Links        []RelationSpec `json:"-"`
	Status       string         `json:"-"`
	Blocked      bool           `json:"-"`
	Comments     []CommentView  `json:"-"`
	Inbound      []RelationView `json:"-"`
	ChildCounter uint64         `json:"-"`
}

type DemoteResult struct {
	Run  RunView  `json:"run"`
	Item ItemView `json:"item"`
}

type InvokeRequest struct {
	Template  string          `json:"template"`
	Project   string          `json:"project"`
	Mode      string          `json:"mode"`
	Checklist []ChecklistItem `json:"checklist"`
}

type RunView struct {
	ID        string    `json:"id"`
	Template  string    `json:"template,omitempty"`
	Project   string    `json:"project,omitempty"`
	Mode      string    `json:"mode,omitempty"`
	State     string    `json:"state"`
	Owner     string    `json:"owner,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}

type ItemView struct {
	ID           string         `json:"id"`
	RunID        string         `json:"runId"`
	CreatedAt    time.Time      `json:"-"`
	Label        string         `json:"label"`
	Title        string         `json:"title"`
	Type         string         `json:"type"`
	Project      string         `json:"project"`
	Status       string         `json:"status"`
	Plane        string         `json:"plane"`
	Kind         string         `json:"kind"`
	Plugin       string         `json:"plugin"`
	Revision     model.Revision `json:"revision"`
	Fields       map[string]any `json:"fields,omitempty"`
	Labels       []string       `json:"labels,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Parent       *model.Ref     `json:"parent,omitempty"`
	Links        []RelationView `json:"links,omitempty"`
	Blocked      bool           `json:"blocked,omitempty"`
	ChildCounter uint64         `json:"childCounter,omitempty"`
	Claim        *ClaimView     `json:"claim,omitempty"`
	Comments     []CommentView  `json:"comments,omitempty"`
}

// ReadySnapshot binds one ready list to its checkpoint generation. ValidUntil
// bounds membership changes caused by a matching claim expiring without a write.
type ReadySnapshot struct {
	Items      []ItemView
	Revision   pluginapi.Revision
	ValidUntil *time.Time
	// OrderAt is the provider clock used for this checkpoint's readiness and
	// claim-expiry decisions. The service uses it for the shared durable and
	// workflow ordering tuple so controlled clocks cannot drift between planes.
	OrderAt time.Time `json:"-"`
	// These fields are service-internal observations from the same provider
	// checkpoint as Items. They are omitted from wire projections.
	BlockedOrdinary map[string]bool               `json:"-"`
	ActiveIDs       map[string]struct{}           `json:"-"`
	OrdinaryParents map[string]OrdinaryParentFact `json:"-"`
	// WorkflowParents includes every caller-visible item, including non-ready
	// intermediate parents. Selection must not infer ancestry from Items alone.
	WorkflowParents map[string]*model.Ref `json:"-"`
}

type ClaimView struct {
	Principal  model.PrincipalRef `json:"principal"`
	Agent      string             `json:"agent,omitempty"`
	ExpiresAt  time.Time          `json:"expiresAt"`
	Generation model.Revision     `json:"generation"`
}

type CommentView struct {
	Body      string               `json:"body"`
	Principal model.PrincipalRef   `json:"principal"`
	Time      time.Time            `json:"time"`
	Source    *SourceCommentOrigin `json:"source,omitempty"`
}

type InvokeResult struct {
	Plane string     `json:"plane"`
	Run   *RunView   `json:"run,omitempty"`
	Items []ItemView `json:"items"`
}

type Provider struct {
	state       pluginapi.DurableState
	now         func() time.Time
	persistent  bool
	plugin      string
	instance    string
	incarnation string
	source      *driver.SourcePlaneProviderCommitV1
	// userContentGuard is installed by the trusted service composition. The
	// provider calls it with the complete set of user-content kinds it is about
	// to create, immediately before mutating its checkpoint image. Receipts,
	// audit entries and other operational state are deliberately not included.
	userContentGuard func([]string) error
	mu               sync.Mutex
}

// UserContentGuard is the common rollback-window admission seam for native
// provider writes. The callback receives actual provider-created kinds rather
// than a service-side allowlist, so a future provider creation path must name
// every user-content record it publishes before its checkpoint CAS.
type UserContentGuard func([]string) error

// BlockerResolver bridges a workflow dependency to the canonical core
// ontology. The workflow provider owns the active endpoint and invokes this
// resolver only for an ordinary durable target; it never creates a core row.
type BlockerResolver interface {
	IsBlocked(context.Context, model.PrincipalRef, model.WriteMeta, model.Ref, string, *model.LinkMetadata) (bool, error)
}

// BlockerPublicationFence describes the core snapshot actually consumed by a
// trusted resolver. A read-only resolver can omit it; writes fail closed unless
// every durable observation can join the checkpoint publication transaction.
type BlockerPublicationFence interface {
	CheckpointHead() (driver.CoreSnapshotExpectation, bool)
}

// BlockerResolverFunc adapts a trusted service closure to BlockerResolver.
type BlockerResolverFunc func(context.Context, model.PrincipalRef, model.WriteMeta, model.Ref, string, *model.LinkMetadata) (bool, error)

func (f BlockerResolverFunc) IsBlocked(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta, ref model.Ref, kind string, metadata *model.LinkMetadata) (bool, error) {
	return f(ctx, by, meta, ref, kind, metadata)
}

// ValidateInvokeRequest checks request shape before capability admission. A
// missing persistent checkpoint is a capability refusal; malformed input must
// retain the ordinary invalid_params contract even on a memory host.
func ValidateInvokeRequest(req InvokeRequest) error {
	if strings.TrimSpace(req.Template) == "" || strings.TrimSpace(req.Project) == "" || len(req.Checklist) == 0 || len(req.Checklist) > 100 {
		return fail("invalid_params", "template, project and one to one hundred checklist items are required")
	}
	seen := make(map[string]bool, len(req.Checklist))
	for _, input := range req.Checklist {
		if strings.TrimSpace(input.Label) == "" || strings.TrimSpace(input.Title) == "" || seen[input.Label] {
			return fail("invalid_params", "checklist labels and titles must be nonempty and labels unique")
		}
		if err := validateLabels(input.Labels); err != nil {
			return err
		}
		if input.Parent != nil && (!input.Parent.IsID() || input.Parent.Branch.String() != "0" || input.Parent.ID == "") {
			return fail("invalid_params", "workflow parent must be a branch-zero ID reference")
		}
		for _, link := range input.Links {
			if err := validateRelation(link); err != nil {
				return err
			}
		}
		seen[input.Label] = true
	}
	return nil
}

func validateLabels(labels []string) error {
	for _, label := range labels {
		if !utf8.ValidString(label) || utf8.RuneCountInString(label) > 255 {
			return fail("invalid_params", "a label must contain at most 255 Unicode scalars")
		}
	}
	return nil
}

func validateRelation(link RelationSpec) error {
	if (link.Kind == "" || len(link.Kind) > 32 || !utf8.ValidString(link.Kind)) || !link.To.IsID() || link.To.Branch.String() != "0" || link.To.ID == "" {
		return fail("invalid_params", "workflow relations require a kind and branch-zero target")
	}
	if link.Metadata != nil {
		if _, err := link.Metadata.ForKind(link.Kind); err != nil {
			return fail("invalid_params", err.Error())
		}
	}
	return nil
}

func validateRef(ref model.Ref) error {
	if err := ref.Validate(); err != nil || !ref.IsID() || ref.Branch.String() != "0" || ref.ID == "" {
		return fail("invalid_params", "workflow relations require a branch-zero ID reference")
	}
	return nil
}

func refValue(value any) (model.Ref, error) {
	if value == nil {
		return model.Ref{}, nil
	}
	if ref, ok := value.(model.Ref); ok {
		return ref, validateRef(ref)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return model.Ref{}, fail("invalid_params", "workflow reference must be an object")
	}
	var ref model.Ref
	if err := json.Unmarshal(data, &ref); err != nil {
		return model.Ref{}, fail("invalid_params", "workflow reference must be an object")
	}
	return ref, validateRef(ref)
}

func stringSliceValue(value any) ([]string, error) {
	if values, ok := value.([]string); ok {
		if err := validateLabels(values); err != nil {
			return nil, err
		}
		return normalizeLabels(values), nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fail("invalid_params", "workflow labels must be an array of strings")
	}
	var values []string
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, fail("invalid_params", "workflow labels must be an array of strings")
	}
	if err := validateLabels(values); err != nil {
		return nil, err
	}
	return normalizeLabels(values), nil
}

func relationSpecValues(value any) ([]RelationSpec, error) {
	if value == nil {
		return nil, nil
	}
	if values, ok := value.([]RelationSpec); ok {
		for _, relation := range values {
			if err := validateRelation(relation); err != nil {
				return nil, err
			}
		}
		return values, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fail("invalid_params", "workflow links must be an array")
	}
	var values []RelationSpec
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, fail("invalid_params", "workflow links must be an array of typed relations")
	}
	for _, relation := range values {
		if err := validateRelation(relation); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func relationKey(link relationState) string {
	data, _ := json.Marshal(struct {
		From     string              `json:"from"`
		To       model.Ref           `json:"to"`
		Kind     string              `json:"kind"`
		Metadata *model.LinkMetadata `json:"metadata,omitempty"`
	}{link.From, link.To, link.Kind, link.Metadata})
	return string(data)
}

func relationSpecState(from string, relation RelationSpec) relationState {
	var metadata *model.LinkMetadata
	if relation.Metadata != nil {
		copy := relation.Metadata.Clone()
		metadata = &copy
	}
	return relationState{From: from, To: relation.To, Kind: relation.Kind, Metadata: metadata}
}

func dedupeRelations(values []relationState) []relationState {
	if len(values) == 0 {
		return nil
	}
	out := make([]relationState, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, relation := range values {
		key := relationKey(relation)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, cloneRelation(relation))
	}
	return out
}

func removeRelation(values []relationState, remove relationState) []relationState {
	key := relationKey(remove)
	out := values[:0]
	for _, relation := range values {
		if relationKey(relation) != key {
			out = append(out, relation)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func hasRelation(values []relationState, want relationState) bool {
	key := relationKey(want)
	for _, value := range values {
		if relationKey(value) == key {
			return true
		}
	}
	return false
}

// Preserve JSON numbers through workflow checkpoints, detached images and
// receipt replay so later public moves retain the original metadata values.
func decodeWorkflowJSON(data []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing workflow JSON image")
	}
	return nil
}

func (item *itemState) UnmarshalJSON(data []byte) error {
	type wire itemState
	var value wire
	if err := decodeWorkflowJSON(data, &value); err != nil {
		return err
	}
	*item = itemState(value)
	return nil
}

func (item *ItemView) UnmarshalJSON(data []byte) error {
	type wire ItemView
	var value wire
	if err := decodeWorkflowJSON(data, &value); err != nil {
		return err
	}
	*item = ItemView(value)
	return nil
}

func cloneMetadata(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var clone map[string]any
	if decodeWorkflowJSON(data, &clone) != nil {
		return nil
	}
	return clone
}

func normalizeLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	out := make([]string, 0, len(labels))
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}

func cloneRef(ref *model.Ref) *model.Ref {
	if ref == nil {
		return nil
	}
	copy := *ref
	return &copy
}

func cloneRelation(link relationState) relationState {
	clone := link
	clone.To = model.RefByID(link.To.ID, link.To.Branch)
	if link.Metadata != nil {
		metadata := link.Metadata.Clone()
		clone.Metadata = &metadata
	}
	return clone
}

func relationView(link relationState) RelationView {
	clone := cloneRelation(link)
	return RelationView{From: clone.From, To: clone.To, Kind: clone.Kind, Metadata: clone.Metadata}
}

// persistentState reports the stronger process-restart guarantee. Ordinary
// invocation may also use an explicit process-lifetime workflow checkpoint;
// neither that capability nor a source binding changes this persistence marker.
type persistentState interface {
	Persistent() bool
}

type persisted struct {
	Runs     map[string]runState     `json:"runs"`
	Items    map[string]itemState    `json:"items"`
	Receipts map[string]receiptState `json:"receipts"`
	Audit    []auditState            `json:"audit,omitempty"`
	Source   *sourceState            `json:"source,omitempty"`
}
type ownerState struct {
	Principal string `json:"principal"`
	Agent     string `json:"agent,omitempty"`
	Project   string `json:"project"`
}
type runState struct {
	ID        string     `json:"id"`
	Template  string     `json:"template"`
	Project   string     `json:"project"`
	Owner     ownerState `json:"owner"`
	ItemIDs   []string   `json:"itemIds"`
	State     string     `json:"state"`
	CreatedAt time.Time  `json:"createdAt"`
}
type itemState struct {
	ID           string          `json:"id"`
	RunID        string          `json:"runId"`
	Label        string          `json:"label"`
	Title        string          `json:"title"`
	Type         string          `json:"type"`
	Project      string          `json:"project"`
	Owner        ownerState      `json:"owner"`
	Status       string          `json:"status"`
	Fields       map[string]any  `json:"fields,omitempty"`
	Labels       []string        `json:"labels,omitempty"`
	Metadata     map[string]any  `json:"metadata,omitempty"`
	Parent       *model.Ref      `json:"parent,omitempty"`
	Links        []relationState `json:"links,omitempty"`
	Inbound      []relationState `json:"inbound,omitempty"`
	Blocked      bool            `json:"blocked,omitempty"`
	ChildCounter uint64          `json:"childCounter,omitempty"`
	Generation   uint64          `json:"generation"`
	Claim        *claimState     `json:"claim,omitempty"`
	Comments     []commentState  `json:"comments,omitempty"`
}

type relationState struct {
	From     string              `json:"from"`
	To       model.Ref           `json:"to"`
	Kind     string              `json:"kind"`
	Metadata *model.LinkMetadata `json:"metadata,omitempty"`
}
type claimState struct {
	Principal  model.PrincipalRef `json:"principal"`
	Agent      string             `json:"agent,omitempty"`
	ExpiresAt  time.Time          `json:"expiresAt"`
	Generation uint64             `json:"generation"`
}
type commentState struct {
	Body      string               `json:"body"`
	Principal model.PrincipalRef   `json:"principal"`
	Time      time.Time            `json:"time"`
	Source    *SourceCommentOrigin `json:"source,omitempty"`
}
type receiptState struct {
	Principal     string          `json:"principal"`
	PrincipalHash string          `json:"principalHash"`
	Agent         string          `json:"agent,omitempty"`
	Project       string          `json:"project"`
	Plugin        string          `json:"plugin"`
	Instance      string          `json:"instance"`
	Incarnation   string          `json:"incarnation"`
	Method        string          `json:"method"`
	Key           string          `json:"key"`
	Digest        string          `json:"digest"`
	RunID         string          `json:"runId,omitempty"`
	ItemID        string          `json:"itemId,omitempty"`
	Terminal      bool            `json:"terminal,omitempty"`
	ExpiresAt     time.Time       `json:"expiresAt"`
	Result        json.RawMessage `json:"result"`
}

// auditState is the durable, identity-only lifecycle ledger. It carries no
// title, field, comment, claim, or run body, so ephemeral deletion remains
// outside permanent bead history while P45 deletion/GC provenance survives.
type auditState struct {
	Event         string    `json:"event"`
	At            time.Time `json:"at"`
	Principal     string    `json:"principal"`
	PrincipalHash string    `json:"principalHash"`
	Agent         string    `json:"agent,omitempty"`
	Project       string    `json:"project"`
	Plugin        string    `json:"plugin"`
	Instance      string    `json:"instance"`
	Incarnation   string    `json:"incarnation"`
	Method        string    `json:"method"`
	Key           string    `json:"key"`
	Digest        string    `json:"digest"`
	RunID         string    `json:"runId,omitempty"`
	ItemID        string    `json:"itemId,omitempty"`
}

func New(state pluginapi.DurableState) (*Provider, error) {
	if state == nil {
		return nil, errors.New("workflow: durable state is required")
	}
	if selected, ok := state.(interface {
		SourceRuntimeBinding() (driver.SourcePlaneProviderCommitV1, error)
	}); ok {
		if binding, err := selected.SourceRuntimeBinding(); err == nil {
			return NewSourceRuntime(state, binding)
		} else if !errors.Is(err, driver.ErrUnsupported) {
			return nil, err
		}
	}
	persistent := false
	if marker, ok := state.(persistentState); ok {
		persistent = marker.Persistent()
	}
	return &Provider{state: state, now: time.Now, persistent: persistent, plugin: Alias, instance: "default"}, nil
}

func (p *Provider) WithClock(now func() time.Time) *Provider {
	if now != nil {
		p.now = now
	}
	return p
}

// WithUserContentGuard installs the trusted host guard for native user-content
// creation. A nil guard restores the default (window disabled) behavior.
func (p *Provider) WithUserContentGuard(guard UserContentGuard) *Provider {
	p.userContentGuard = guard
	return p
}

func (p *Provider) guardUserContent(kinds ...string) error {
	if p.userContentGuard == nil || len(kinds) == 0 {
		return nil
	}
	return p.userContentGuard(append([]string(nil), kinds...))
}

// WithIdentity binds receipts to the plugin instance and store incarnation.
// A new incarnation cannot replay an old response, even when the checkpoint
// namespace is restored into a fresh store.
func (p *Provider) WithIdentity(plugin, instance, incarnation string) *Provider {
	if plugin != "" {
		p.plugin = plugin
	}
	if instance != "" {
		p.instance = instance
	}
	p.incarnation = incarnation
	return p
}

func (p *Provider) readState(ctx context.Context) (persisted, pluginapi.Revision, error) {
	var value persisted
	found, revision, err := p.state.Read(ctx, stateKey, &value)
	if err != nil {
		return persisted{}, "", err
	}
	if !found {
		value = persisted{Runs: map[string]runState{}, Items: map[string]itemState{}, Receipts: map[string]receiptState{}}
	}
	if value.Runs == nil {
		value.Runs = map[string]runState{}
	}
	if value.Items == nil {
		value.Items = map[string]itemState{}
	}
	if value.Receipts == nil {
		value.Receipts = map[string]receiptState{}
	}
	if p.source != nil && (value.Source == nil || value.Source.Schema != SourceStateSchema) {
		return persisted{}, "", sourceRefusal("missing source-restored image")
	}
	return value, revision, nil
}

func (p *Provider) load(ctx context.Context) (persisted, pluginapi.Revision, error) {
	value, revision, err := p.readState(ctx)
	if err != nil {
		return persisted{}, "", err
	}
	// Ordinary workflow access also performs eligible receipt cleanup. Publish
	// it with the revision read alongside this complete map, then carry the new
	// revision into any following mutation. Unfinished runs are never collected.
	if p.collectExpiredReceipts(&value) {
		revision, err = p.state.Put(ctx, stateKey, value, revision)
		if err != nil {
			return persisted{}, "", p.mapSaveError(err)
		}
	}
	return value, revision, nil
}

func (p *Provider) appendAudit(value *persisted, event string, receipt receiptState) {
	value.Audit = append(value.Audit, auditState{
		Event: event, At: p.now().UTC(), Principal: receipt.Principal,
		PrincipalHash: receipt.PrincipalHash, Agent: receipt.Agent, Project: receipt.Project,
		Plugin: receipt.Plugin, Instance: receipt.Instance, Incarnation: receipt.Incarnation,
		Method: receipt.Method, Key: receipt.Key, Digest: receipt.Digest,
		RunID: receipt.RunID, ItemID: receipt.ItemID,
	})
}

func (p *Provider) save(ctx context.Context, value persisted, revision pluginapi.Revision) error {
	_, err := p.state.Put(ctx, stateKey, value, revision)
	return err
}

func (p *Provider) saveWithResolver(ctx context.Context, value persisted, revision pluginapi.Revision, resolver BlockerResolver) error {
	if resolver == nil {
		return p.save(ctx, value, revision)
	}
	fence, ok := resolver.(BlockerPublicationFence)
	if !ok {
		return fail("unsupported", "durable blocker observations require an atomic publication fence")
	}
	head, observed := fence.CheckpointHead()
	if !observed {
		return p.save(ctx, value, revision)
	}
	guarded, ok := p.state.(interface {
		PutAtHead(context.Context, string, any, pluginapi.Revision, driver.CoreSnapshotExpectation) (pluginapi.Revision, error)
	})
	if !ok {
		return fail("unsupported", "durable state cannot atomically fence core blocker observations")
	}
	_, err := guarded.PutAtHead(ctx, stateKey, value, revision, head)
	return err
}

func principalScope(by model.PrincipalRef, agent string) (string, string, error) {
	_ = agent
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		return "", "", err
	}
	data, err := json.Marshal(by)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(data)
	return principal, hex.EncodeToString(digest[:]), nil
}

func fail(code model.ErrorCode, hint string) error {
	return &model.Error{Data: model.ErrorData{Code: code, Hint: hint, Retryable: code == "busy" || code == "write_conflict"}}
}

func newID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw[:]), nil
}

func digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (p *Provider) receiptScope(by model.PrincipalRef, meta model.WriteMeta) (principal, principalHash, agent, project, key string, err error) {
	agent = effectiveAgent(by, meta)
	principal, principalHash, err = principalScope(by, agent)
	if err != nil {
		return "", "", "", "", "", err
	}
	project = meta.Project
	key = principal + "\x00" + principalHash + "\x00" + agent + "\x00" + project + "\x00" + p.plugin + "\x00" + p.instance + "\x00" + p.incarnation + "\x00" + meta.Method + "\x00" + meta.IdempotencyKey
	return principal, principalHash, agent, project, key, nil
}

func (p *Provider) checkReplay(value persisted, by model.PrincipalRef, meta model.WriteMeta, request any, dst any) (bool, error) {
	if meta.IdempotencyKey == "" {
		return false, fail("invalid_params", "idempotency key is required")
	}
	principal, principalHash, agent, project, key, err := p.receiptScope(by, meta)
	if err != nil {
		return false, fail("invalid_params", "invalid principal")
	}
	d, err := digest(request)
	if err != nil {
		return false, err
	}
	receipt, ok := value.Receipts[key]
	if !ok {
		// A restored checkpoint can carry receipts from an earlier store
		// incarnation. Its scoped map key differs, but the same operation must
		// fail explicitly rather than creating a second run or claim.
		for _, previous := range value.Receipts {
			if previous.Principal == principal && previous.PrincipalHash == principalHash && previous.Agent == agent && previous.Project == project && previous.Plugin == p.plugin && previous.Instance == p.instance && previous.Method == meta.Method && previous.Key == meta.IdempotencyKey && previous.Incarnation != p.incarnation && p.receiptRetained(value, previous) {
				return false, fail("idempotency_key_reused", "idempotency receipt belongs to another store incarnation")
			}
		}
		return false, nil
	}
	if receipt.Principal != principal || receipt.PrincipalHash != principalHash || receipt.Agent != agent || receipt.Project != project || receipt.Plugin != p.plugin || receipt.Instance != p.instance || receipt.Incarnation != p.incarnation || receipt.Method != meta.Method || receipt.Key != meta.IdempotencyKey {
		return false, fail("idempotency_key_reused", "idempotency receipt scope changed")
	}
	if !p.receiptRetained(value, receipt) {
		return false, nil
	}
	if receipt.Digest != d {
		return false, fail("idempotency_key_reused", "idempotency key was used for another request")
	}
	if err := json.Unmarshal(receipt.Result, dst); err != nil {
		return false, err
	}
	// Creation provenance is internal to the provider view and deliberately
	// absent from receipt JSON. Recover it from this same checkpoint so a
	// replay keeps the ordering metadata without changing any stored response.
	hydrateCreatedAt := func(item *ItemView) {
		if run, ok := value.Runs[item.RunID]; ok {
			item.CreatedAt = run.CreatedAt.UTC()
		}
	}
	switch result := dst.(type) {
	case *InvokeResult:
		for i := range result.Items {
			hydrateCreatedAt(&result.Items[i])
		}
	case *ItemView:
		hydrateCreatedAt(result)
	case *claimNextResult:
		hydrateCreatedAt(&result.Item)
	case *DemoteResult:
		hydrateCreatedAt(&result.Item)
	}
	return true, nil
}

func (p *Provider) receiptRetained(value persisted, receipt receiptState) bool {
	return p.now().Before(receipt.ExpiresAt) || receipt.RunID != "" && value.Runs[receipt.RunID].ID != ""
}

func receiptKey(p *Provider, by model.PrincipalRef, meta model.WriteMeta) (string, string, string, string, string, error) {
	principal, principalHash, agent, project, key, err := p.receiptScope(by, meta)
	return key, principal, principalHash, agent, project, err
}

func (p *Provider) storeReceipt(value persisted, by model.PrincipalRef, meta model.WriteMeta, request any, result any, runID, itemID string) (receiptState, error) {
	key, principal, principalHash, agent, project, err := receiptKey(p, by, meta)
	if err != nil {
		return receiptState{}, err
	}
	d, err := digest(request)
	if err != nil {
		return receiptState{}, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return receiptState{}, err
	}
	receipt := receiptState{Principal: principal, PrincipalHash: principalHash, Agent: agent, Project: project, Plugin: p.plugin, Instance: p.instance, Incarnation: p.incarnation, Method: meta.Method, Key: meta.IdempotencyKey, Digest: d, RunID: runID, ItemID: itemID, ExpiresAt: p.now().Add(driver.IdempotencyWindow), Result: encoded}
	value.Receipts[key] = receipt
	return receipt, nil
}

func (p *Provider) mapSaveError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, driver.ErrCheckpointConflict) {
		return fail("write_conflict", "workflow state changed during the operation")
	}
	if code, details, ok := driver.ErrorMapping(err); ok {
		return &model.Error{Data: model.ErrorData{Code: code, Details: details, Retryable: code == "busy" || code == "write_conflict"}}
	}
	return err
}

func (p *Provider) Invoke(ctx context.Context, by model.PrincipalRef, req InvokeRequest, meta model.WriteMeta) (InvokeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if req.Mode != "ephemeral" {
		return InvokeResult{}, fail("invalid_params", "workflow provider only owns ephemeral invocation")
	}
	if err := ValidateInvokeRequest(req); err != nil {
		return InvokeResult{}, err
	}
	if !p.checkpointAvailable() || p.source != nil {
		return InvokeResult{}, fail("plugin_durable_state_unavailable", "ephemeral workflows require persistent checkpoint storage")
	}
	value, revision, err := p.load(ctx)
	if err != nil {
		return InvokeResult{}, err
	}
	var replay InvokeResult
	if ok, err := p.checkReplay(value, by, meta, req, &replay); err != nil {
		return InvokeResult{}, err
	} else if ok {
		return replay, nil
	}
	principal, _, agent, _, _, err := p.receiptScope(by, meta)
	if err != nil {
		return InvokeResult{}, fail("invalid_params", "invalid principal")
	}
	runID, err := newID("workflow:run/")
	if err != nil {
		return InvokeResult{}, err
	}
	if err := p.guardUserContent(RunKind, ItemKind); err != nil {
		return InvokeResult{}, err
	}
	run := runState{ID: runID, Template: req.Template, Project: req.Project, Owner: ownerState{Principal: principal, Agent: agent, Project: req.Project}, State: "active", CreatedAt: p.now().UTC()}
	result := InvokeResult{Plane: "ephemeral", Run: &RunView{ID: runID, Template: req.Template, Project: req.Project, Mode: req.Mode, State: run.State, Owner: principal, CreatedAt: run.CreatedAt}, Items: []ItemView{}}
	// Keep the authoritative run timestamp available while constructing item
	// views; the complete run (including ItemIDs) is published below.
	value.Runs[runID] = run
	seen := map[string]bool{}
	for _, input := range req.Checklist {
		seen[input.Label] = true
		itemID, err := newID(ItemPrefix)
		if err != nil {
			return InvokeResult{}, err
		}
		itemType := input.Type
		if itemType == "" {
			itemType = "task"
		}
		if input.Parent != nil && workflowTarget(*input.Parent) {
			parent, ok := value.Items[input.Parent.ID]
			if !ok {
				return InvokeResult{}, fail("not_found", "workflow parent was not found")
			}
			if parent.Owner != run.Owner {
				return InvokeResult{}, fail("forbidden", "workflow parent is outside the owner scope")
			}
		}
		links := make([]relationState, 0, len(input.Links))
		for _, link := range input.Links {
			metadata := link.Metadata
			if metadata != nil {
				copy := metadata.Clone()
				metadata = &copy
			}
			links = append(links, relationState{From: itemID, To: link.To, Kind: link.Kind, Metadata: metadata})
		}
		item := itemState{
			ID: itemID, RunID: runID, Label: input.Label, Title: input.Title, Type: itemType,
			Project: req.Project, Owner: run.Owner, Status: "open", Fields: cloneMetadata(input.Fields), Labels: normalizeLabels(input.Labels),
			Metadata: cloneMetadata(input.Metadata), Parent: cloneRef(input.Parent), Links: links, Blocked: input.Blocked,
		}
		value.Items[itemID] = item
		run.ItemIDs = append(run.ItemIDs, itemID)
		view := p.itemViewIn(value, item)
		result.Items = append(result.Items, view)
	}
	// Maintain plugin-owned child counters and inbound relation indexes in the
	// same checkpoint CAS as invocation. A missing durable target is retained as
	// a mixed-plane edge; a missing workflow target is rejected before publish.
	for _, id := range run.ItemIDs {
		item := value.Items[id]
		for _, link := range item.Links {
			if target, ok := value.Items[link.To.ID]; ok {
				if target.Owner != item.Owner {
					return InvokeResult{}, fail("forbidden", "workflow target is outside the owner scope")
				}
				target.Inbound = append(target.Inbound, cloneRelation(link))
				value.Items[target.ID] = target
			} else if strings.HasPrefix(link.To.ID, ItemPrefix) {
				return InvokeResult{}, fail("not_found", "workflow relation target was not found")
			}
		}
		if item.Parent != nil {
			if parent, ok := value.Items[item.Parent.ID]; ok {
				parent.ChildCounter++
				value.Items[parent.ID] = parent
			}
		}
	}
	for index, id := range run.ItemIDs {
		result.Items[index] = p.itemViewIn(value, value.Items[id])
	}
	value.Runs[runID] = run
	receipt, err := p.storeReceipt(value, by, meta, req, result, runID, "")
	if err != nil {
		return InvokeResult{}, err
	}
	p.appendAudit(&value, "workflow.run.created", receipt)
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
		return InvokeResult{}, err
	}
	return result, nil
}

func (p *Provider) itemView(item itemState) ItemView {
	view := itemViewAt(item, p.now())
	// itemView is used under the provider mutex by every public read/mutation.
	// Re-evaluate dependency blockers against the same loaded image so callers
	// do not mistake a stale persisted flag for the current ready predicate.
	return view
}

func (p *Provider) itemViewIn(value persisted, item itemState) ItemView {
	view := itemViewAt(item, p.now())
	if run, ok := value.Runs[item.RunID]; ok {
		view.CreatedAt = run.CreatedAt.UTC()
	}
	view.Blocked = itemBlocked(value, item)
	return view
}

func itemViewAt(item itemState, now time.Time) ItemView {
	view := ItemView{
		ID: item.ID, RunID: item.RunID, Label: item.Label, Title: item.Title, Type: item.Type,
		Project: item.Project, Status: item.Status, Plane: "ephemeral", Kind: ItemKind,
		Plugin: Alias, Revision: model.Natural64(item.Generation), Fields: cloneMetadata(item.Fields), Labels: append([]string(nil), item.Labels...),
		Metadata: cloneMetadata(item.Metadata), Parent: cloneRef(item.Parent), Blocked: item.Blocked, ChildCounter: item.ChildCounter,
	}
	for _, link := range append(append([]relationState(nil), item.Links...), item.Inbound...) {
		view.Links = append(view.Links, relationView(link))
	}
	if item.Claim != nil && !now.After(item.Claim.ExpiresAt) {
		view.Claim = &ClaimView{Principal: item.Claim.Principal, Agent: item.Claim.Agent, ExpiresAt: item.Claim.ExpiresAt, Generation: model.Natural64(item.Claim.Generation)}
	}
	for _, comment := range item.Comments {
		view.Comments = append(view.Comments, CommentView{Body: comment.Body, Principal: comment.Principal, Time: comment.Time, Source: cloneCommentOrigin(comment.Source)})
	}
	return view
}

func effectiveAgent(by model.PrincipalRef, meta model.WriteMeta) string {
	if by.Kind == "anonymous" && meta.AgentLabel != "" {
		return meta.AgentLabel
	}
	if by.Kind == "anonymous" {
		if meta.Agent != "" {
			return meta.Agent
		}
		return by.ID
	}
	return ""
}

func (p *Provider) authorized(item itemState, by model.PrincipalRef, meta model.WriteMeta) error {
	principal, _, err := principalScope(by, effectiveAgent(by, meta))
	if err != nil || principal != item.Owner.Principal || effectiveAgent(by, meta) != item.Owner.Agent || meta.Project != "" && meta.Project != item.Project {
		return fail("forbidden", "workflow item is outside the current principal or project")
	}
	return nil
}

func relationBlocks(kind string, metadata *model.LinkMetadata) bool {
	switch kind {
	case "blocks", "conditional-blocks":
		return true
	case "waits-for":
		return true
	default:
		return false
	}
}

func itemBlocked(value persisted, item itemState) bool {
	blocked, _ := itemBlockedWithResolver(context.Background(), value, item, model.PrincipalRef{}, model.WriteMeta{}, nil)
	return blocked
}

func itemBlockedWithResolver(ctx context.Context, value persisted, item itemState, by model.PrincipalRef, meta model.WriteMeta, resolver BlockerResolver) (bool, error) {
	if item.Blocked {
		return true, nil
	}
	for _, link := range item.Links {
		if !relationBlocks(link.Kind, link.Metadata) {
			continue
		}
		if workflowTarget(link.To) {
			target, ok := value.Items[link.To.ID]
			if !ok || !workflowRelationSatisfied(value, target, link.Kind, link.Metadata) {
				return true, nil
			}
			continue
		}
		// Durable endpoints are resolved by the canonical core ontology. A
		// missing resolver remains fail-closed for direct provider callers.
		if resolver == nil {
			return true, nil
		}
		blocked, err := resolver.IsBlocked(ctx, by, meta, link.To, link.Kind, link.Metadata)
		if err != nil {
			return false, err
		}
		if blocked {
			return true, nil
		}
	}
	// Inbound edges identify this item as the blocker endpoint. A blocks edge
	// is directed from its dependent to its blocker, so an inbound blocks edge
	// must not make the blocker itself unavailable. Mixed-plane source status is
	// evaluated only when the workflow item is the dependent (the outbound edge
	// above); no core lease, version, or status row is synthesized here.
	return false, nil
}

func itemReady(value persisted, item itemState, now time.Time) bool {
	ready, _ := itemReadyWithResolver(context.Background(), value, item, now, model.PrincipalRef{}, model.WriteMeta{}, nil)
	return ready
}

func itemReadyWithResolver(ctx context.Context, value persisted, item itemState, now time.Time, by model.PrincipalRef, meta model.WriteMeta, resolver BlockerResolver) (bool, error) {
	blocked, err := itemBlockedWithResolver(ctx, value, item, by, meta, resolver)
	if err != nil {
		return false, err
	}
	if blocked {
		return false, nil
	}
	ancestorsReady, err := itemAncestorsReady(ctx, value, item, by, meta, resolver)
	if err != nil || !ancestorsReady {
		return false, err
	}
	if item.Status == "open" {
		return item.Claim == nil || !now.Before(item.Claim.ExpiresAt), nil
	}
	return item.Status == "claimed" && item.Claim != nil && !now.Before(item.Claim.ExpiresAt), nil
}

func (p *Provider) readySnapshot(ctx context.Context, value persisted, revision pluginapi.Revision, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, now time.Time, includeEphemeral bool, resolver BlockerResolver) (ReadySnapshot, error) {
	principal, _, err := principalScope(by, effectiveAgent(by, meta))
	if err != nil {
		return ReadySnapshot{}, fail("invalid_params", "invalid principal")
	}
	snapshot := ReadySnapshot{Items: make([]ItemView, 0), Revision: revision}
	if !includeEphemeral {
		return snapshot, nil
	}
	for _, item := range value.Items {
		if item.Project != project || !matchesType(item.Type, types) || item.Owner.Principal != principal || item.Owner.Agent != effectiveAgent(by, meta) {
			continue
		}
		ready, err := itemReadyWithResolver(ctx, value, item, now, by, meta, resolver)
		if err != nil {
			return ReadySnapshot{}, err
		}
		if ready {
			view := itemViewAt(item, now)
			if run, ok := value.Runs[item.RunID]; ok {
				view.CreatedAt = run.CreatedAt.UTC()
			}
			view.Blocked, err = itemBlockedWithResolver(ctx, value, item, by, meta, resolver)
			if err != nil {
				return ReadySnapshot{}, err
			}
			snapshot.Items = append(snapshot.Items, view)
		} else if (item.Status == "open" || item.Status == "claimed") && item.Claim != nil && now.Before(item.Claim.ExpiresAt) {
			if snapshot.ValidUntil == nil || item.Claim.ExpiresAt.Before(*snapshot.ValidUntil) {
				expiresAt := item.Claim.ExpiresAt
				snapshot.ValidUntil = &expiresAt
			}
		}
	}
	sortItems(snapshot.Items)
	return snapshot, nil
}

func (p *Provider) Get(ctx context.Context, by model.PrincipalRef, id model.Ref, meta model.WriteMeta) (ItemView, error) {
	if !id.IsID() || !strings.HasPrefix(id.ID, ItemPrefix) {
		return ItemView{}, fail("not_found", "workflow item was not found")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.load(ctx)
	if err != nil {
		return ItemView{}, err
	}
	item, ok := value.Items[id.ID]
	if !ok {
		return ItemView{}, fail("not_found", "workflow item was not found")
	}
	if err := p.authorized(item, by, meta); err != nil {
		return ItemView{}, err
	}
	return p.itemViewIn(value, item), nil
}

func (p *Provider) Ready(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, include ...bool) ([]ItemView, error) {
	snapshot, err := p.ReadySnapshot(ctx, by, project, types, meta, include...)
	return snapshot.Items, err
}

// ReadySnapshot reads the list and its generation together under the provider
// lock. Cleanup, when needed, publishes first and supplies the returned revision.
func (p *Provider) ReadySnapshot(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, include ...bool) (ReadySnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, revision, err := p.load(ctx)
	if err != nil {
		return ReadySnapshot{}, err
	}
	includeEphemeral := true
	if len(include) != 0 {
		includeEphemeral = include[0]
	}
	now := p.now().UTC()
	snapshot, err := p.readySnapshot(ctx, value, revision, by, project, types, meta, now, includeEphemeral, nil)
	if err != nil {
		return ReadySnapshot{}, err
	}
	snapshot.OrderAt = now
	return snapshot, nil
}

// ReadySnapshotWithResolver is the service-facing ready seam. Durable
// blockers are resolved through the core ontology while workflow-owned
// blockers remain evaluated against this checkpoint image.
func (p *Provider) ReadySnapshotWithResolver(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, includeEphemeral bool, resolver BlockerResolver) (ReadySnapshot, error) {
	return p.readySnapshotWithResolverAndCoreAt(ctx, by, project, types, meta, includeEphemeral, nil, resolver, p.now().UTC())
}

// ReadySnapshotWithResolverAndCore binds provider ready items, ordinary
// workflow blockers, and active-ID collisions to one loaded provider image.
// The service supplies the complete core ID set and a resolver already bound
// to its pinned reader, so no second provider or core eligibility read can be
// spliced into an aggregate page.
func (p *Provider) ReadySnapshotWithResolverAndCore(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, includeEphemeral bool, ordinaryIDs []string, resolver BlockerResolver) (ReadySnapshot, error) {
	return p.readySnapshotWithResolverAndCoreAt(ctx, by, project, types, meta, includeEphemeral, ordinaryIDs, resolver, p.now().UTC())
}

// ReadySnapshotWithResolverAndCoreAt is the same combined checkpoint read,
// using the service's shared order clock for the 48-hour ready boundary.
func (p *Provider) ReadySnapshotWithResolverAndCoreAt(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, includeEphemeral bool, ordinaryIDs []string, resolver BlockerResolver, now time.Time) (ReadySnapshot, error) {
	return p.readySnapshotWithResolverAndCoreAt(ctx, by, project, types, meta, includeEphemeral, ordinaryIDs, resolver, now)
}

func (p *Provider) readySnapshotWithResolverAndCoreAt(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, includeEphemeral bool, ordinaryIDs []string, resolver BlockerResolver, now time.Time) (ReadySnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, revision, err := p.load(ctx)
	if err != nil {
		return ReadySnapshot{}, err
	}
	snapshot, err := p.readySnapshot(ctx, value, revision, by, project, types, meta, now.UTC(), includeEphemeral, resolver)
	if err != nil {
		return ReadySnapshot{}, err
	}
	snapshot.OrderAt = now.UTC()
	snapshot.BlockedOrdinary = ordinaryBlocked(value, ordinaryIDs, meta)
	snapshot.ActiveIDs = activeIDs(value, by, meta)
	snapshot.WorkflowParents = make(map[string]*model.Ref, len(snapshot.ActiveIDs))
	for id := range snapshot.ActiveIDs {
		parent, err := itemParent(value.Items[id])
		if err != nil {
			return ReadySnapshot{}, err
		}
		snapshot.WorkflowParents[id] = parent
	}
	// Use the accepted typed-parent evaluator on the same checkpoint as Items.
	// Calling OrdinaryParentSnapshot here would reload state under this mutex.
	snapshot.OrdinaryParents = make(map[string]OrdinaryParentFact, len(ordinaryIDs))
	parents := parentReadiness{ctx: ctx, value: value, by: by, meta: meta, resolver: resolver, visiting: map[string]bool{}, done: map[string]bool{}}
	for _, id := range ordinaryIDs {
		fact, err := parents.ordinary(id)
		if err != nil {
			return ReadySnapshot{}, err
		}
		snapshot.OrdinaryParents[id] = fact
	}
	return snapshot, nil
}

// OrdinaryBlocked reports ordinary dependents whose active workflow blocker
// is visible to the caller. The service uses this before emitting core ready
// rows; completed workflow items are satisfied and are never reported.
func (p *Provider) OrdinaryBlocked(ctx context.Context, by model.PrincipalRef, ids []string, meta model.WriteMeta) (map[string]bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.load(ctx)
	if err != nil {
		return nil, err
	}
	return ordinaryBlocked(value, ids, meta), nil
}

// OrdinaryBlockerSnapshot also binds the decision to a host-owned checkpoint
// expectation, so direct ordinary writes cannot race a mixed-plane edge edit.
func (p *Provider) OrdinaryBlockerSnapshot(ctx context.Context, by model.PrincipalRef, ids []string, meta model.WriteMeta) (map[string]bool, driver.CheckpointExpectation, error) {
	blocked, _, expectation, err := p.OrdinaryCloseSnapshot(ctx, by, ids, meta)
	return blocked, expectation, err
}

// OrdinaryCloseSnapshot observes dependencies and open workflow children in
// the same read-only checkpoint. The service combines child facts only for
// actual close operations, leaving claim/readiness projections unchanged.
func (p *Provider) OrdinaryCloseSnapshot(ctx context.Context, by model.PrincipalRef, ids []string, meta model.WriteMeta) (map[string]bool, map[string]bool, driver.CheckpointExpectation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Without a declared workflow checkpoint capability or admitted binding,
	// no active workflow facts can exist for this provider.
	if !p.checkpointAvailable() && p.source == nil {
		return map[string]bool{}, map[string]bool{}, driver.CheckpointExpectation{}, nil
	}
	// An ordinary pre-publication guard must never garbage collect or publish
	// metadata on its own, including when the core write is later refused.
	value, revision, err := p.readState(ctx)
	if err != nil {
		return nil, nil, driver.CheckpointExpectation{}, err
	}
	host, ok := p.state.(interface {
		PrepareCheckpointExpectation(context.Context, string, pluginapi.Revision) (driver.CheckpointExpectation, error)
	})
	if !ok {
		return nil, nil, driver.CheckpointExpectation{}, driver.ErrUnsupported
	}
	expectation, err := host.PrepareCheckpointExpectation(ctx, stateKey, revision)
	if err != nil {
		return nil, nil, driver.CheckpointExpectation{}, err
	}
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}
	children := map[string]bool{}
	for _, item := range value.Items {
		if item.Status == "closed" || meta.Project != "" && item.Project != meta.Project {
			continue
		}
		if item.Parent != nil && item.Parent.IsID() && item.Parent.Branch.IsZero() && wanted[item.Parent.ID] {
			children[item.Parent.ID] = true
		}
		for _, edge := range item.Links {
			if edge.Kind == "parent-child" && edge.To.IsID() && edge.To.Branch.IsZero() && wanted[edge.To.ID] {
				children[edge.To.ID] = true
			}
		}
	}
	return ordinaryBlocked(value, ids, meta), children, expectation, nil
}

// OrdinaryParentSnapshot projects the already-admitted typed mixed parents
// and their readiness from one checkpoint read. It never performs cleanup or
// publication. Ordinary claims join its expectation to the existing Writer
// fence; read callers supply their already-pinned complete ordinary graph.
func (p *Provider) OrdinaryParentSnapshot(ctx context.Context, by model.PrincipalRef, ids []string, meta model.WriteMeta, resolver BlockerResolver) (map[string]OrdinaryParentFact, driver.CheckpointExpectation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	facts := make(map[string]OrdinaryParentFact, len(ids))
	if !p.checkpointAvailable() && p.source == nil {
		return facts, driver.CheckpointExpectation{}, nil
	}
	value, revision, err := p.readState(ctx)
	if err != nil {
		return nil, driver.CheckpointExpectation{}, err
	}
	r := parentReadiness{ctx: ctx, value: value, by: by, meta: meta, resolver: resolver, visiting: map[string]bool{}, done: map[string]bool{}}
	for _, id := range ids {
		fact, err := r.ordinary(id)
		if err != nil {
			return nil, driver.CheckpointExpectation{}, err
		}
		facts[id] = fact
	}
	host, ok := p.state.(interface {
		PrepareCheckpointExpectation(context.Context, string, pluginapi.Revision) (driver.CheckpointExpectation, error)
	})
	if !ok {
		return nil, driver.CheckpointExpectation{}, driver.ErrUnsupported
	}
	expectation, err := host.PrepareCheckpointExpectation(ctx, stateKey, revision)
	return facts, expectation, err
}

// Workflow children are the typed Parent and legacy parent-child edges in
// this checkpoint. Durable children of workflow parents are not admitted by
// the ordinary hierarchy writer. The gate formula matches core waits-for.
func workflowRelationSatisfied(value persisted, target itemState, kind string, metadata *model.LinkMetadata) bool {
	switch kind {
	case "blocks":
		return target.Status == "closed"
	case "conditional-blocks":
		// Workflow close has a success outcome; it is never a failed outcome.
		return false
	case "waits-for":
		terminal := target.Status == "closed"
		if !metadata.HasPredicate() && terminal {
			return true
		}
		if metadata != nil && metadata.AlsoBlocks && !terminal {
			return false
		}
		openChild, terminalChild := false, false
		for _, child := range value.Items {
			member := child.Parent != nil && child.Parent.ID == target.ID
			for _, edge := range child.Links {
				member = member || edge.Kind == "parent-child" && edge.To.ID == target.ID
			}
			if member {
				if child.Status == "closed" {
					terminalChild = true
				} else {
					openChild = true
				}
			}
		}
		return !openChild || metadata != nil && metadata.Gate == "any-children" && terminalChild
	default:
		return true
	}
}

func ordinaryBlocked(value persisted, ids []string, meta model.WriteMeta) map[string]bool {
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	blocked := make(map[string]bool)
	for _, item := range value.Items {
		if meta.Project != "" && item.Project != meta.Project {
			continue
		}
		for _, relation := range item.Inbound {
			if relationBlocks(relation.Kind, relation.Metadata) && !workflowRelationSatisfied(value, item, relation.Kind, relation.Metadata) {
				if _, ok := wanted[relation.From]; ok {
					blocked[relation.From] = true
				}
			}
		}
	}
	return blocked
}

// ActiveIDs returns the provider's live item namespace. The service uses it to
// suppress a colliding durable row from aggregate ready results: an active workflow item
// owns the reserved workflow:item/<id> name until completion.
func (p *Provider) ActiveIDs(ctx context.Context) (map[string]struct{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.load(ctx)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]struct{}, len(value.Items))
	for id := range value.Items {
		ids[id] = struct{}{}
	}
	return ids, nil
}

// Relations returns the workflow-owned view of mixed-plane edges for an
// ordinary endpoint. It is intentionally a read-only scan of the checkpoint:
// no core relation/version/lease row is synthesized for the ephemeral side.
// A relation is exposed only when its workflow endpoint is readable by the
// current principal, so an ordinary point read cannot disclose another
// owner's workflow ID.
func (p *Provider) Relations(ctx context.Context, by model.PrincipalRef, id model.Ref, meta model.WriteMeta) ([]RelationView, error) {
	if err := validateRef(id); err != nil {
		return nil, fail("invalid_params", "mixed-plane relation reads require an ordinary endpoint")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.load(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]RelationView, 0)
	for _, item := range value.Items {
		visible := p.authorized(item, by, meta) == nil
		if !visible {
			continue
		}
		for _, relation := range item.Links {
			if relation.From == id.ID || relation.To.ID == id.ID {
				result = append(result, relationView(relation))
			}
		}
		for _, relation := range item.Inbound {
			if relation.From == id.ID || relation.To.ID == id.ID {
				result = append(result, relationView(relation))
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].From != result[j].From {
			return result[i].From < result[j].From
		}
		if result[i].To.ID != result[j].To.ID {
			return result[i].To.ID < result[j].To.ID
		}
		return result[i].Kind < result[j].Kind
	})
	return dedupeRelationViews(result), nil
}

func dedupeRelationViews(values []RelationView) []RelationView {
	if len(values) < 2 {
		return values
	}
	result := make([]RelationView, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, relation := range values {
		state := relationState{From: relation.From, To: relation.To, Kind: relation.Kind, Metadata: relation.Metadata}
		key := relationKey(state)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, relation)
	}
	return result
}

func metadataValue(value any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	if metadata, ok := value.(map[string]any); ok {
		return cloneMetadata(metadata), nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fail("invalid_params", "workflow metadata must be an object")
	}
	var metadata map[string]any
	if err := json.Unmarshal(data, &metadata); err != nil || metadata == nil {
		return nil, fail("invalid_params", "workflow metadata must be an object")
	}
	return metadata, nil
}

func parseUpdateFields(item itemState, fields map[string]any) (itemState, error) {
	if fields == nil {
		return itemState{}, fail("invalid_params", "update requires fields")
	}
	next := item
	for key, value := range fields {
		switch key {
		case "fields":
			metadata, err := metadataValue(value)
			if err != nil {
				return itemState{}, fail("invalid_params", "workflow fields must be an object")
			}
			next.Fields = metadata
		case "title":
			title, ok := value.(string)
			if !ok || strings.TrimSpace(title) == "" {
				return itemState{}, fail("invalid_params", "workflow title must be a nonempty string")
			}
			next.Title = title
		case "type":
			typ, ok := value.(string)
			if !ok || strings.TrimSpace(typ) == "" {
				return itemState{}, fail("invalid_params", "workflow type must be a nonempty string")
			}
			next.Type = typ
		case "labels":
			labels, err := stringSliceValue(value)
			if err != nil {
				return itemState{}, err
			}
			next.Labels = labels
		case "metadata":
			metadata, err := metadataValue(value)
			if err != nil {
				return itemState{}, err
			}
			next.Metadata = metadata
		case "blocked":
			blocked, ok := value.(bool)
			if !ok {
				return itemState{}, fail("invalid_params", "workflow blocked must be boolean")
			}
			next.Blocked = blocked
		case "parent":
			if value == nil {
				next.Parent = nil
				continue
			}
			parent, err := refValue(value)
			if err != nil {
				return itemState{}, err
			}
			next.Parent = cloneRef(&parent)
		case "links":
			relations, err := relationSpecValues(value)
			if err != nil {
				return itemState{}, err
			}
			next.Links = make([]relationState, 0, len(relations))
			for _, relation := range relations {
				next.Links = append(next.Links, relationSpecState(item.ID, relation))
			}
		default:
			return itemState{}, fail("invalid_params", fmt.Sprintf("workflow field %q is not mutable", key))
		}
	}
	next.Links = dedupeRelations(next.Links)
	return next, nil
}

func sameRef(left, right *model.Ref) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.ID == right.ID && left.Branch.String() == right.Branch.String()
}

func workflowTarget(ref model.Ref) bool {
	return strings.HasPrefix(ref.ID, ItemPrefix)
}

func (p *Provider) applyAuxiliary(value *persisted, old, next itemState) error {
	if next.Parent != nil && workflowTarget(*next.Parent) {
		if next.Parent.ID == next.ID {
			return fail("invalid_params", "workflow item cannot parent itself")
		}
		parent, ok := value.Items[next.Parent.ID]
		if !ok {
			return fail("not_found", "workflow parent was not found")
		}
		if parent.Owner != next.Owner {
			return fail("forbidden", "workflow parent is outside the owner scope")
		}
	}
	for _, relation := range next.Links {
		if workflowTarget(relation.To) {
			if relation.To.ID == next.ID {
				return fail("invalid_params", "workflow item cannot relate to itself")
			}
			target, ok := value.Items[relation.To.ID]
			if !ok {
				return fail("not_found", "workflow relation target was not found")
			}
			if target.Owner != next.Owner {
				return fail("forbidden", "workflow target is outside the owner scope")
			}
		}
	}
	if !sameRef(old.Parent, next.Parent) && old.Parent != nil && workflowTarget(*old.Parent) {
		if parent, ok := value.Items[old.Parent.ID]; ok && parent.ChildCounter > 0 {
			parent.ChildCounter--
			value.Items[parent.ID] = parent
		}
	}
	if !sameRef(old.Parent, next.Parent) && next.Parent != nil && workflowTarget(*next.Parent) {
		parent := value.Items[next.Parent.ID]
		parent.ChildCounter++
		value.Items[parent.ID] = parent
	}
	for _, relation := range old.Links {
		if !workflowTarget(relation.To) {
			continue
		}
		if target, ok := value.Items[relation.To.ID]; ok {
			target.Inbound = removeRelation(target.Inbound, relation)
			value.Items[target.ID] = target
		}
	}
	for _, relation := range next.Links {
		if !workflowTarget(relation.To) {
			continue
		}
		target := value.Items[relation.To.ID]
		if !hasRelation(target.Inbound, relation) {
			target.Inbound = append(target.Inbound, cloneRelation(relation))
			target.Inbound = dedupeRelations(target.Inbound)
			value.Items[target.ID] = target
		}
	}
	return nil
}

// Update mutates only plugin-owned fields. It validates the complete ordered
// image and all relation/parent targets before publishing one checkpoint CAS;
// a failed target lookup therefore leaves the previous image untouched.
func (p *Provider) Update(ctx context.Context, by model.PrincipalRef, id model.Ref, fields map[string]any, meta model.WriteMeta) (model.Commit, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateRef(id); err != nil || !workflowTarget(id) {
		return model.Commit{}, fail("invalid_params", "workflow update requires a workflow item reference")
	}
	if meta.Method == "" {
		meta.Method = "bead.update"
	}
	value, revision, err := p.load(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	request := struct {
		ID     model.Ref      `json:"id"`
		Fields map[string]any `json:"fields"`
	}{id, fields}
	var replay model.Commit
	if ok, err := p.checkReplay(value, by, meta, request, &replay); err != nil {
		return model.Commit{}, err
	} else if ok {
		return replay, nil
	}
	item, ok := value.Items[id.ID]
	if !ok {
		return model.Commit{}, fail("not_found", "workflow item was not found")
	}
	if err := p.authorized(item, by, meta); err != nil {
		return model.Commit{}, err
	}
	next, err := parseUpdateFields(item, fields)
	if err != nil {
		return model.Commit{}, err
	}
	// An equal labels-only proposal is a semantic no-op. Retain its scoped
	// replay receipt so a later replay cannot undo an intervening label edit,
	// but leave the item revision and lifecycle audit unchanged.
	if len(fields) == 1 {
		if _, labelsOnly := fields["labels"]; labelsOnly && slices.Equal(item.Labels, next.Labels) {
			commit := workflowCommit(item.ID, item.Generation)
			if _, err := p.storeReceipt(value, by, meta, request, commit, item.RunID, item.ID); err != nil {
				return model.Commit{}, err
			}
			if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
				return model.Commit{}, err
			}
			return commit, nil
		}
	}
	if err := p.applyAuxiliary(&value, item, next); err != nil {
		return model.Commit{}, err
	}
	next.Generation++
	value.Items[next.ID] = next
	commit := workflowCommit(next.ID, next.Generation)
	receipt, err := p.storeReceipt(value, by, meta, request, commit, next.RunID, next.ID)
	if err != nil {
		return model.Commit{}, err
	}
	p.appendAudit(&value, "workflow.update", receipt)
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
		return model.Commit{}, err
	}
	return commit, nil
}

// mutateCanonicalRelation gives every ordinary relation one typed endpoint
// identity. Metadata replaces values on that edge; it never creates siblings.
func mutateCanonicalRelation(links []relationState, incoming relationState, unlink bool) ([]relationState, bool, error) {
	found := -1
	for i, stored := range links {
		if stored.From != incoming.From || !sameRef(&stored.To, &incoming.To) {
			continue
		}
		if stored.Kind != incoming.Kind {
			if !unlink {
				return nil, false, fail("ontology_guard_failed", fmt.Sprintf("dependency endpoints already have link kind %s", stored.Kind))
			}
			continue
		}
		if found >= 0 {
			return nil, false, fail("invalid_params", "ambiguous stored dependency identity")
		}
		found = i
	}
	if found < 0 {
		if unlink {
			return nil, false, fail("not_found", "workflow relation was not found")
		}
		return append(links, cloneRelation(incoming)), true, nil
	}
	if unlink {
		return append(links[:found:found], links[found+1:]...), true, nil
	}
	previous := links[found]
	if incoming.Metadata == nil {
		incoming.Metadata = previous.Metadata
	} else {
		value := incoming.Metadata.Retaining(previous.Metadata)
		incoming.Metadata = &value
	}
	if relationKey(previous) == relationKey(incoming) {
		return links, false, nil
	}
	result := append([]relationState(nil), links...)
	result[found] = cloneRelation(incoming)
	return result, true, nil
}

func (p *Provider) relationMutation(ctx context.Context, by model.PrincipalRef, req LinkRequest, meta model.WriteMeta, unlink bool) (model.Commit, error) {
	if err := validateRef(req.From); err != nil {
		return model.Commit{}, err
	}
	if err := validateRef(req.To); err != nil || req.Kind == "" || len(req.Kind) > 32 || !utf8.ValidString(req.Kind) {
		return model.Commit{}, fail("invalid_params", "workflow relations require branch-zero IDs and a kind")
	}
	if req.From.ID == req.To.ID {
		return model.Commit{}, fail("invalid_params", "workflow item cannot relate to itself")
	}
	if req.Metadata != nil {
		normalized, err := req.Metadata.ForKind(req.Kind)
		if err != nil {
			return model.Commit{}, fail("invalid_params", err.Error())
		}
		req.Metadata = &normalized
	}
	fromWorkflow, toWorkflow := workflowTarget(req.From), workflowTarget(req.To)
	if !fromWorkflow && !toWorkflow {
		return model.Commit{}, fail("invalid_params", "workflow relation requires a workflow endpoint")
	}
	if meta.Method == "" {
		if unlink {
			meta.Method = "bead.unlink"
		} else {
			meta.Method = "bead.link"
		}
	}
	value, revision, err := p.load(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	request := struct {
		From     model.Ref           `json:"from"`
		To       model.Ref           `json:"to"`
		Kind     string              `json:"kind"`
		Metadata *model.LinkMetadata `json:"metadata,omitempty"`
		Unlink   bool                `json:"unlink"`
	}{req.From, req.To, req.Kind, req.Metadata, unlink}
	var replay model.Commit
	if ok, err := p.checkReplay(value, by, meta, request, &replay); err != nil {
		return model.Commit{}, err
	} else if ok {
		return replay, nil
	}
	var endpoint itemState
	if fromWorkflow {
		var ok bool
		endpoint, ok = value.Items[req.From.ID]
		if !ok {
			return model.Commit{}, fail("not_found", "workflow relation source was not found")
		}
		if err := p.authorized(endpoint, by, meta); err != nil {
			return model.Commit{}, err
		}
	} else {
		var ok bool
		endpoint, ok = value.Items[req.To.ID]
		if !ok {
			return model.Commit{}, fail("not_found", "workflow relation target was not found")
		}
		if err := p.authorized(endpoint, by, meta); err != nil {
			return model.Commit{}, err
		}
	}
	relation := relationState{From: req.From.ID, To: req.To, Kind: req.Kind, Metadata: req.Metadata}
	changed := false
	if fromWorkflow {
		var err error
		endpoint.Links, changed, err = mutateCanonicalRelation(endpoint.Links, relation, unlink)
		if err != nil {
			return model.Commit{}, err
		}
		if toWorkflow {
			target, ok := value.Items[req.To.ID]
			if !ok {
				return model.Commit{}, fail("not_found", "workflow relation target was not found")
			}
			if err := p.authorized(target, by, meta); err != nil {
				return model.Commit{}, err
			}
			target.Inbound, _, err = mutateCanonicalRelation(target.Inbound, relation, unlink)
			if err != nil {
				return model.Commit{}, err
			}
			value.Items[target.ID] = target
		}
	} else {
		var err error
		endpoint.Inbound, changed, err = mutateCanonicalRelation(endpoint.Inbound, relation, unlink)
		if err != nil {
			return model.Commit{}, err
		}
	}
	if changed {
		endpoint.Generation++
		value.Items[endpoint.ID] = endpoint
	}

	commit := workflowCommit(endpoint.ID, endpoint.Generation)
	receipt, err := p.storeReceipt(value, by, meta, request, commit, endpoint.RunID, endpoint.ID)
	if err != nil {
		return model.Commit{}, err
	}
	if changed {
		p.appendAudit(&value, "workflow.relation", receipt)
	}
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
		return model.Commit{}, err
	}
	return commit, nil
}

func (p *Provider) Link(ctx context.Context, by model.PrincipalRef, req LinkRequest, meta model.WriteMeta) (model.Commit, error) {
	return p.relationMutation(ctx, by, req, meta, false)
}

func (p *Provider) Unlink(ctx context.Context, by model.PrincipalRef, req LinkRequest, meta model.WriteMeta) (model.Commit, error) {
	return p.relationMutation(ctx, by, req, meta, true)
}

// Promote removes an active item and rewrites every workflow-owned inbound
// edge to the caller's ordinary target. Outbound and inbound edges are returned
// for the core writer to materialize; no durable lease/version/history row is
// created by this provider operation.
func (p *Provider) Promote(ctx context.Context, by model.PrincipalRef, req PromoteRequest, meta model.WriteMeta) (PromoteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateRef(req.Item); err != nil || !workflowTarget(req.Item) {
		return PromoteResult{}, fail("invalid_params", "promotion requires a workflow item and ordinary target reference")
	}
	if err := validateRef(req.Target); err != nil || workflowTarget(req.Target) {
		return PromoteResult{}, fail("invalid_params", "promotion requires a workflow item and ordinary target reference")
	}
	if req.Item.ID == req.Target.ID {
		return PromoteResult{}, fail("invalid_params", "promotion target must differ from workflow item")
	}
	if meta.Method == "" {
		meta.Method = "x.workflow.run.promote"
	}
	value, revision, err := p.load(ctx)
	if err != nil {
		return PromoteResult{}, err
	}
	var replay PromoteResult
	if ok, err := p.checkReplay(value, by, meta, req, &replay); err != nil {
		return PromoteResult{}, err
	} else if ok {
		return replay, nil
	}
	item, ok := value.Items[req.Item.ID]
	if !ok {
		return PromoteResult{}, fail("not_found", "workflow item was not found")
	}
	if err := p.authorized(item, by, meta); err != nil {
		return PromoteResult{}, err
	}
	if _, collision := value.Items[req.Target.ID]; collision {
		return PromoteResult{}, fail("write_conflict", "promotion target collides with an active workflow item")
	}
	result := PromoteResult{Source: req.Item, Target: req.Target, ChildCount: item.ChildCounter, Generation: model.Natural64(item.Generation)}
	for _, relation := range item.Links {
		moved := cloneRelation(relation)
		moved.From = req.Target.ID
		result.Relations = append(result.Relations, relationView(moved))
	}
	for _, relation := range item.Inbound {
		moved := cloneRelation(relation)
		moved.To = req.Target
		result.Relations = append(result.Relations, relationView(moved))
	}
	if item.Parent != nil && workflowTarget(*item.Parent) {
		if parent, ok := value.Items[item.Parent.ID]; ok && parent.ChildCounter > 0 {
			parent.ChildCounter--
			value.Items[parent.ID] = parent
		}
	}
	for id, other := range value.Items {
		if id == item.ID {
			continue
		}
		changed := false
		for index, relation := range other.Links {
			if relation.To.ID == item.ID {
				relation.To = req.Target
				other.Links[index] = relation
				changed = true
			}
		}
		if other.Parent != nil && other.Parent.ID == item.ID {
			other.Parent = cloneRef(&req.Target)
			changed = true
		}
		// Outbound relations from the promoted item are returned to the
		// ordinary writer. Remove their plugin-side inbound index so a deleted
		// workflow source can never remain visible on a surviving target.
		inbound := append([]relationState(nil), other.Inbound...)
		other.Inbound = other.Inbound[:0]
		for _, relation := range inbound {
			if relation.From == item.ID {
				relation.From = req.Target.ID
				changed = true
			}
			other.Inbound = append(other.Inbound, relation)
		}
		if changed {
			other.Generation++
			value.Items[id] = other
		}
	}
	if run := value.Runs[item.RunID]; run.ID != "" {
		remaining := run.ItemIDs[:0]
		for _, id := range run.ItemIDs {
			if id != item.ID {
				remaining = append(remaining, id)
			}
		}
		if len(remaining) == 0 {
			delete(value.Runs, run.ID)
		} else {
			run.ItemIDs = remaining
			value.Runs[run.ID] = run
		}
	}
	delete(value.Items, item.ID)
	commit := workflowCommit(req.Target.ID, item.Generation)
	result.Generation = commit.Results[0].Revision
	receipt, err := p.storeReceipt(value, by, meta, req, result, item.RunID, item.ID)
	if err != nil {
		return PromoteResult{}, err
	}
	if _, active := value.Runs[item.RunID]; active {
		p.sanitizePromotedItemReceipts(value, item.RunID, item.ID)
	} else {
		p.sanitizeTerminalReceipts(value, item.RunID)
	}
	p.appendAudit(&value, "workflow.promote", receipt)
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
		return PromoteResult{}, err
	}
	return result, nil
}

// Demote creates a new durable ephemeral copy. Core deletion is deliberately
// outside this provider so a caller can stage/validate the source row before
// removing it through the ordinary writer.
func (p *Provider) Demote(ctx context.Context, by model.PrincipalRef, req DemoteRequest, meta model.WriteMeta) (DemoteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := validateRef(req.Source); err != nil || workflowTarget(req.Source) || strings.TrimSpace(req.Project) == "" || strings.TrimSpace(req.Label) == "" || strings.TrimSpace(req.Title) == "" {
		return DemoteResult{}, fail("invalid_params", "demotion requires an ordinary source and project, label, and title")
	}
	if req.Template == "" {
		req.Template = "demoted"
	}
	if req.Type == "" {
		req.Type = "task"
	}
	if err := validateLabels(req.Labels); err != nil {
		return DemoteResult{}, err
	}
	for _, relation := range req.Links {
		if err := validateRelation(relation); err != nil {
			return DemoteResult{}, err
		}
	}
	if req.Parent != nil {
		if err := validateRef(*req.Parent); err != nil {
			return DemoteResult{}, err
		}
	}
	if !p.persistent || p.source != nil {
		return DemoteResult{}, fail("plugin_durable_state_unavailable", "ephemeral workflows require persistent checkpoint storage")
	}
	if meta.Method == "" {
		meta.Method = "x.workflow.run.demote"
	}
	meta.Project = req.Project
	value, revision, err := p.load(ctx)
	if err != nil {
		return DemoteResult{}, err
	}
	var replay DemoteResult
	if ok, err := p.checkReplay(value, by, meta, req, &replay); err != nil {
		return DemoteResult{}, err
	} else if ok {
		return replay, nil
	}
	principal, _, agent, _, _, err := p.receiptScope(by, meta)
	if err != nil {
		return DemoteResult{}, fail("invalid_params", "invalid principal")
	}
	runID, err := newID("workflow:run/")
	if err != nil {
		return DemoteResult{}, err
	}
	itemID, err := newID(ItemPrefix)
	if err != nil {
		return DemoteResult{}, err
	}
	run := runState{ID: runID, Template: req.Template, Project: req.Project, Owner: ownerState{Principal: principal, Agent: agent, Project: req.Project}, ItemIDs: []string{itemID}, State: "active", CreatedAt: p.now().UTC()}
	links := make([]relationState, 0, len(req.Links))
	for _, relation := range req.Links {
		links = append(links, relationSpecState(itemID, relation))
	}
	item := itemState{ID: itemID, RunID: runID, Label: req.Label, Title: req.Title, Type: req.Type, Project: req.Project, Owner: run.Owner, Status: "open", Fields: cloneMetadata(req.Fields), Labels: normalizeLabels(req.Labels), Metadata: cloneMetadata(req.Metadata), Parent: cloneRef(req.Parent), Links: links}
	if req.Status != "" {
		item.Status = req.Status
	}
	if err := p.guardUserContent(RunKind, ItemKind); err != nil {
		return DemoteResult{}, err
	}
	item.Blocked, item.ChildCounter = req.Blocked, req.ChildCounter
	for _, comment := range req.Comments {
		item.Comments = append(item.Comments, commentState{Body: comment.Body, Principal: comment.Principal, Time: comment.Time})
	}
	for _, link := range req.Inbound {
		item.Inbound = append(item.Inbound, relationState{From: link.From, To: model.RefByID(itemID, model.Natural64(0)), Kind: link.Kind, Metadata: link.Metadata})
	}
	value.Items[itemID] = item
	// The source may already have mixed edges indexed on workflow targets.
	// Replace those source references with the new item in this same
	// checkpoint, so demotion cannot leave a stale ordinary endpoint behind.
	for id, target := range value.Items {
		if id == itemID {
			continue
		}
		for index, relation := range target.Links {
			if relation.To.ID == req.Source.ID {
				relation.To = model.RefByID(itemID, model.Natural64(0))
				target.Links[index] = relation
				item.Inbound = append(item.Inbound, cloneRelation(relation))
			}
		}
		if target.Parent != nil && target.Parent.ID == req.Source.ID {
			ref := model.RefByID(itemID, model.Natural64(0))
			target.Parent = &ref
			item.ChildCounter++
		}
		filtered := target.Inbound[:0]
		for _, relation := range target.Inbound {
			if relation.From != req.Source.ID {
				filtered = append(filtered, relation)
			} else {
				moved := cloneRelation(relation)
				moved.From = itemID
				item.Links = append(item.Links, moved)
			}
		}
		target.Inbound = filtered
		value.Items[id] = target
	}
	value.Items[itemID] = item
	if err := p.applyAuxiliary(&value, itemState{ID: itemID}, item); err != nil {
		delete(value.Items, itemID)
		return DemoteResult{}, err
	}
	value.Runs[runID] = run
	result := DemoteResult{Run: RunView{ID: runID, Template: run.Template, Project: run.Project, Mode: "ephemeral", State: run.State, Owner: principal, CreatedAt: run.CreatedAt}, Item: p.itemViewIn(value, item)}
	receipt, err := p.storeReceipt(value, by, meta, req, result, runID, itemID)
	if err != nil {
		return DemoteResult{}, err
	}
	p.appendAudit(&value, "workflow.demote", receipt)
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
		return DemoteResult{}, err
	}
	return result, nil
}

func matchesType(value string, types []string) bool {
	if len(types) == 0 {
		return true
	}
	for _, candidate := range types {
		if candidate == value {
			return true
		}
	}
	return false
}

func sortItems(items []ItemView) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].ID < items[j-1].ID; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
}

type claimNextResult struct {
	Found bool     `json:"found"`
	Item  ItemView `json:"item"`
}

func claimNextRequest(project string, types, resources []string, meta model.WriteMeta) any {
	if meta.ClaimNextIdentity != "" {
		// The service supplies its canonical full selector, including resource
		// filters. Candidate selection must not replace that selector with an ID.
		return struct {
			Project  string `json:"project"`
			Selector string `json:"selector"`
		}{Project: project, Selector: meta.ClaimNextIdentity}
	}
	return struct {
		Project   string   `json:"project"`
		Types     []string `json:"types,omitempty"`
		Resources []string `json:"resources,omitempty"`
	}{Project: project, Types: append([]string(nil), types...), Resources: append([]string(nil), resources...)}
}

// ClaimNext returns (item, found, replayed). A replayed empty result is a
// durable terminal null for this selector and must not probe a later item.
func (p *Provider) ClaimNext(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta) (ItemView, bool, bool, error) {
	return p.claimNext(ctx, by, project, types, nil, meta, nil, nil)
}

// ClaimNextWithResources intersects the selector's optional resource IDs before
// atomically choosing a candidate. The service supplies its full selector in
// ClaimNextIdentity; direct calls without it bind resources in their digest.
func (p *Provider) ClaimNextWithResources(ctx context.Context, by model.PrincipalRef, project string, types, resources []string, meta model.WriteMeta) (ItemView, bool, bool, error) {
	return p.claimNext(ctx, by, project, types, resources, meta, nil, nil)
}

// ClaimNextWithResourcesWithResolver is the service-facing queue seam. It
// evaluates durable blockers through the core ontology before claiming.
func (p *Provider) ClaimNextWithResourcesWithResolver(ctx context.Context, by model.PrincipalRef, project string, types, resources []string, meta model.WriteMeta, resolver BlockerResolver) (ItemView, bool, bool, error) {
	return p.claimNext(ctx, by, project, types, resources, meta, nil, resolver)
}

// ClaimNextSelected commits the candidate chosen and authorized by the ordinary
// service queue walk. Its receipt retains the full selector and wrapped result
// shared by ClaimNext and ReplayClaimNext, never a bare bead.claim response.
func (p *Provider) ClaimNextSelected(ctx context.Context, by model.PrincipalRef, ref model.Ref, project string, types []string, meta model.WriteMeta) (ItemView, bool, bool, error) {
	if ref.Validate() != nil || !ref.IsID() || !strings.HasPrefix(ref.ID, ItemPrefix) || ref.Branch.String() != "0" {
		return ItemView{}, false, false, fail("invalid_params", "workflow claimNext requires a branch-zero item reference")
	}
	return p.claimNext(ctx, by, project, types, nil, meta, &ref, nil)
}

func (p *Provider) ClaimNextSelectedWithResolver(ctx context.Context, by model.PrincipalRef, ref model.Ref, project string, types []string, meta model.WriteMeta, resolver BlockerResolver) (ItemView, bool, bool, error) {
	if ref.Validate() != nil || !ref.IsID() || !strings.HasPrefix(ref.ID, ItemPrefix) || ref.Branch.String() != "0" {
		return ItemView{}, false, false, fail("invalid_params", "workflow claimNext requires a branch-zero item reference")
	}
	return p.claimNext(ctx, by, project, types, nil, meta, &ref, resolver)
}

func (p *Provider) claimNext(ctx context.Context, by model.PrincipalRef, project string, types, resources []string, meta model.WriteMeta, selectedRef *model.Ref, resolver BlockerResolver) (ItemView, bool, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	meta.Project = project
	meta.Method = "bead.claimNext"
	value, revision, err := p.load(ctx)
	if err != nil {
		return ItemView{}, false, false, err
	}
	if meta.IdempotencyKey == "" {
		return ItemView{}, false, false, fail("invalid_params", "idempotency key is required")
	}
	request := claimNextRequest(project, types, resources, meta)
	var replay claimNextResult
	if ok, err := p.checkReplay(value, by, meta, request, &replay); err != nil {
		return ItemView{}, false, false, err
	} else if ok {
		return replay.Item, replay.Found, true, nil
	}
	principal, _, err := principalScope(by, effectiveAgent(by, meta))
	if err != nil {
		return ItemView{}, false, false, fail("invalid_params", "invalid principal")
	}
	var selected *itemState
	for id := range value.Items {
		item := value.Items[id]
		if selectedRef != nil && selectedRef.ID != item.ID {
			continue
		}
		if len(resources) != 0 && !slices.Contains(resources, item.ID) {
			continue
		}
		ready, err := itemReadyWithResolver(ctx, value, item, p.now(), by, meta, resolver)
		if err != nil {
			return ItemView{}, false, false, err
		}
		if item.Project == project && ready && matchesType(item.Type, types) && item.Owner.Principal == principal && item.Owner.Agent == effectiveAgent(by, meta) {
			if selected == nil || item.ID < selected.ID {
				copy := item
				selected = &copy
			}
		}
	}
	if selected == nil {
		if selectedRef != nil {
			return ItemView{}, false, false, fail("not_found", "workflow candidate is no longer ready")
		}
		receipt, err := p.storeReceipt(value, by, meta, request, replay, "", "")
		if err != nil {
			return ItemView{}, false, false, err
		}
		p.appendAudit(&value, "workflow.claim_next.empty", receipt)
		if err := p.mapSaveError(p.saveWithResolver(ctx, value, revision, resolver)); err != nil {
			return ItemView{}, false, false, err
		}
		return ItemView{}, false, false, nil
	}
	selected.Status = "claimed"
	selected.Generation++
	selected.Claim = &claimState{Principal: by, Agent: effectiveAgent(by, meta), ExpiresAt: p.now().UTC().Add(claimTTL), Generation: selected.Generation}
	value.Items[selected.ID] = *selected
	item := p.itemViewIn(value, *selected)
	if resolver != nil {
		item.Blocked, err = itemBlockedWithResolver(ctx, value, *selected, by, meta, resolver)
		if err != nil {
			return ItemView{}, false, false, err
		}
	}
	replay = claimNextResult{Found: true, Item: item}
	receipt, err := p.storeReceipt(value, by, meta, request, replay, selected.RunID, selected.ID)
	if err != nil {
		return ItemView{}, false, false, err
	}
	p.appendAudit(&value, "workflow.claim_next", receipt)
	if err := p.mapSaveError(p.saveWithResolver(ctx, value, revision, resolver)); err != nil {
		return ItemView{}, false, false, err
	}
	return item, true, false, nil
}

// ReplayClaimNext checks only the durable receipt. It never consumes a newly
// ready item, which lets the shared service preserve core-before-plugin queue
// ordering on an idempotent retry.
func (p *Provider) ReplayClaimNext(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta) (ItemView, bool, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	meta.Project = project
	meta.Method = "bead.claimNext"
	value, _, err := p.load(ctx)
	if err != nil {
		return ItemView{}, false, false, err
	}
	request := claimNextRequest(project, types, nil, meta)
	var replay claimNextResult
	ok, err := p.checkReplay(value, by, meta, request, &replay)
	return replay.Item, replay.Found, ok, err
}

func (p *Provider) Claim(ctx context.Context, by model.PrincipalRef, req model.Ref, meta model.WriteMeta) (ItemView, error) {
	return p.claimWithResolver(ctx, by, req, meta, nil)
}

// ClaimWithResolver applies the same durable blocker bridge as ready and
// claimNext. Direct callers without a bridge remain fail-closed.
func (p *Provider) ClaimWithResolver(ctx context.Context, by model.PrincipalRef, req model.Ref, meta model.WriteMeta, resolver BlockerResolver) (ItemView, error) {
	return p.claimWithResolver(ctx, by, req, meta, resolver)
}

func (p *Provider) claimWithResolver(ctx context.Context, by model.PrincipalRef, req model.Ref, meta model.WriteMeta, resolver BlockerResolver) (ItemView, error) {
	if meta.Method == "bead.claimNext" {
		return ItemView{}, fail("invalid_params", "claimNext requires its selector and wrapped result")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, revision, err := p.load(ctx)
	if err != nil {
		return ItemView{}, err
	}
	var replay ItemView
	if ok, err := p.checkReplay(value, by, meta, req, &replay); err != nil {
		return ItemView{}, err
	} else if ok {
		return replay, nil
	}
	item, ok := value.Items[req.ID]
	if !ok {
		return ItemView{}, fail("not_found", "workflow item was not found")
	}
	if err := p.authorized(item, by, meta); err != nil {
		return ItemView{}, err
	}
	ready, err := itemReadyWithResolver(ctx, value, item, p.now(), by, meta, resolver)
	if err != nil {
		return ItemView{}, err
	}
	if !ready {
		return ItemView{}, fail("busy", "workflow item already has a live claim")
	}
	item.Status = "claimed"
	item.Generation++
	item.Claim = &claimState{Principal: by, Agent: effectiveAgent(by, meta), ExpiresAt: p.now().UTC().Add(claimTTL), Generation: item.Generation}
	value.Items[item.ID] = item
	view := p.itemViewIn(value, item)
	receipt, err := p.storeReceipt(value, by, meta, req, view, item.RunID, item.ID)
	if err != nil {
		return ItemView{}, err
	}
	p.appendAudit(&value, "workflow.claim", receipt)
	if err := p.mapSaveError(p.saveWithResolver(ctx, value, revision, resolver)); err != nil {
		return ItemView{}, err
	}
	return view, nil
}

func (p *Provider) Comment(ctx context.Context, by model.PrincipalRef, req model.Ref, body string, meta model.WriteMeta) (model.Commit, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, revision, err := p.load(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	var replay model.Commit
	if ok, err := p.checkReplay(value, by, meta, struct {
		ID   model.Ref `json:"id"`
		Body string    `json:"body"`
	}{req, body}, &replay); err != nil {
		return model.Commit{}, err
	} else if ok {
		return replay, nil
	}
	item, ok := value.Items[req.ID]
	if !ok {
		return model.Commit{}, fail("not_found", "workflow item was not found")
	}
	if err := p.authorized(item, by, meta); err != nil {
		return model.Commit{}, err
	}
	if strings.TrimSpace(body) == "" {
		return model.Commit{}, fail("invalid_params", "comment body is required")
	}
	item.Generation++
	item.Comments = append(item.Comments, commentState{Body: body, Principal: by, Time: p.now().UTC()})
	value.Items[item.ID] = item
	commit := workflowCommit(item.ID, item.Generation)
	receipt, err := p.storeReceipt(value, by, meta, struct {
		ID   model.Ref `json:"id"`
		Body string    `json:"body"`
	}{req, body}, commit, item.RunID, item.ID)
	if err != nil {
		return model.Commit{}, err
	}
	p.appendAudit(&value, "workflow.comment", receipt)
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
		return model.Commit{}, err
	}
	return commit, nil
}

func (p *Provider) Close(ctx context.Context, by model.PrincipalRef, req model.Ref, reason string, meta model.WriteMeta) (model.Commit, error) {
	return p.closeWithResolver(ctx, by, req, reason, meta, nil)
}

func (p *Provider) CloseWithResolver(ctx context.Context, by model.PrincipalRef, req model.Ref, reason string, meta model.WriteMeta, resolver BlockerResolver) (model.Commit, error) {
	return p.closeWithResolver(ctx, by, req, reason, meta, resolver)
}

func (p *Provider) closeWithResolver(ctx context.Context, by model.PrincipalRef, req model.Ref, reason string, meta model.WriteMeta, resolver BlockerResolver) (model.Commit, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, revision, err := p.load(ctx)
	if err != nil {
		return model.Commit{}, err
	}
	var replay model.Commit
	if ok, err := p.checkReplay(value, by, meta, struct {
		ID     model.Ref `json:"id"`
		Reason string    `json:"reason"`
	}{req, reason}, &replay); err != nil {
		return model.Commit{}, err
	} else if ok {
		return replay, nil
	}
	item, ok := value.Items[req.ID]
	if !ok {
		return model.Commit{}, fail("not_found", "workflow item was not found")
	}
	if err := p.authorized(item, by, meta); err != nil {
		return model.Commit{}, err
	}
	if item.Status != "closed" {
		blocked, err := itemBlockedWithResolver(ctx, value, item, by, meta, resolver)
		if err != nil {
			return model.Commit{}, err
		}
		if blocked {
			return model.Commit{}, fail("ontology_guard_failed", "workflow dependency is not satisfied")
		}
	}
	item.Status = "closed"
	item.Claim = nil
	item.Generation++
	value.Items[item.ID] = item
	run := value.Runs[item.RunID]
	if run.ID == "" {
		return model.Commit{}, fail("not_found", "workflow run was not found")
	}
	complete := true
	for _, id := range run.ItemIDs {
		if value.Items[id].Status != "closed" {
			complete = false
			break
		}
	}
	commit := workflowCommit(item.ID, item.Generation)
	if complete {
		run.State = "completed"
		// Reconcile child counters before deleting the run's items. A parent in
		// another active run keeps its exact count; parents in the ending run are
		// deleted with their children and need no surviving index.
		ended := make(map[string]struct{}, len(run.ItemIDs))
		for _, id := range run.ItemIDs {
			ended[id] = struct{}{}
		}
		for _, id := range run.ItemIDs {
			endedItem, ok := value.Items[id]
			if !ok || endedItem.Parent == nil || !workflowTarget(*endedItem.Parent) {
				continue
			}
			if _, endingParent := ended[endedItem.Parent.ID]; endingParent {
				continue
			}
			if parent, ok := value.Items[endedItem.Parent.ID]; ok && parent.ChildCounter > 0 {
				parent.ChildCounter--
				parent.Generation++
				value.Items[parent.ID] = parent
			}
		}
		// Completing a run satisfies and removes any inbound workflow edges that
		// pointed at its soon-to-be-deleted items. Leave unrelated runs intact so
		// a dependent item can become ready after the checkpoint CAS.
		for id, other := range value.Items {
			if _, sameRun := ended[id]; sameRun {
				continue
			}
			changed := false
			if other.Parent != nil {
				if _, parentEnded := ended[other.Parent.ID]; parentEnded {
					other.Parent = nil
					changed = true
				}
			}
			links := make([]relationState, 0, len(other.Links))
			for _, relation := range append([]relationState(nil), other.Links...) {
				if _, targetEnded := ended[relation.To.ID]; !targetEnded {
					links = append(links, relation)
				} else {
					changed = true
				}
			}
			inbound := make([]relationState, 0, len(other.Inbound))
			for _, relation := range append([]relationState(nil), other.Inbound...) {
				if _, sourceEnded := ended[relation.From]; !sourceEnded {
					inbound = append(inbound, relation)
				} else {
					changed = true
				}
			}
			if changed {
				other.Links = links
				other.Inbound = inbound
				other.Generation++
				value.Items[id] = other
			}
		}
		// Delete all operational payloads. Existing receipts become terminal
		// identity-only responses before their bounded retention window starts;
		// titles, comments and claims never survive completion.
		for _, id := range run.ItemIDs {
			delete(value.Items, id)
		}
		delete(value.Runs, run.ID)
		p.sanitizeTerminalReceipts(value, run.ID)
	}
	receipt, err := p.storeReceipt(value, by, meta, struct {
		ID     model.Ref `json:"id"`
		Reason string    `json:"reason"`
	}{req, reason}, commit, run.ID, item.ID)
	if err != nil {
		return model.Commit{}, err
	}
	if complete {
		p.sanitizeTerminalReceipts(value, run.ID)
	}
	p.appendAudit(&value, "workflow.close", receipt)
	if complete {
		p.appendAudit(&value, "workflow.run.completed", receipt)
	}
	if err := p.mapSaveError(p.saveWithResolver(ctx, value, revision, resolver)); err != nil {
		return model.Commit{}, err
	}
	return commit, nil
}

func (p *Provider) sanitizeTerminalReceipts(value persisted, runID string) {
	for key, receipt := range value.Receipts {
		if receipt.RunID != runID {
			continue
		}
		receipt.Terminal = true
		receipt.ExpiresAt = p.now().Add(driver.IdempotencyWindow)
		if receipt.Method == "x.workflow.invoke" {
			terminalInvoke := InvokeResult{Plane: "ephemeral", Run: &RunView{ID: runID, Mode: "ephemeral", State: "completed"}, Items: []ItemView{}}
			receipt.Result, _ = json.Marshal(terminalInvoke)
		} else if receipt.Method == "bead.claimNext" {
			var result claimNextResult
			_ = json.Unmarshal(receipt.Result, &result)
			if !result.Found && receipt.ItemID != "" {
				// Erase legacy bare-item receipts too. Their digest cannot replay
				// as a selector, but completion must still delete their payload.
				var legacy ItemView
				if json.Unmarshal(receipt.Result, &legacy) == nil && legacy.ID == receipt.ItemID {
					result.Item = legacy
				}
			}
			result.Found = receipt.ItemID != ""
			result.Item = ItemView{ID: receipt.ItemID, RunID: runID, Status: "closed", Plane: "ephemeral", Kind: ItemKind, Plugin: Alias, Revision: result.Item.Revision}
			receipt.Result, _ = json.Marshal(result)
		} else if receipt.Method == "bead.claim" {
			var item ItemView
			if json.Unmarshal(receipt.Result, &item) == nil {
				item = ItemView{ID: receipt.ItemID, RunID: runID, Status: "closed", Plane: "ephemeral", Kind: ItemKind, Plugin: Alias, Revision: item.Revision}
				receipt.Result, _ = json.Marshal(item)
			}
		} else if receipt.Method == "tx.apply" {
			var result model.OpResult
			if json.Unmarshal(receipt.Result, &result) == nil {
				result.Record = nil
				receipt.Result, _ = json.Marshal(result)
			} else {
				receipt.Result = nil
			}
		} else if receipt.Method == "x.workflow.run.demote" {
			var result DemoteResult
			_ = json.Unmarshal(receipt.Result, &result)
			result = DemoteResult{
				Run:  RunView{ID: runID, Mode: "ephemeral", State: "completed"},
				Item: ItemView{ID: receipt.ItemID, RunID: runID, Status: "closed", Plane: "ephemeral", Kind: ItemKind, Plugin: Alias, Revision: result.Item.Revision},
			}
			receipt.Result, _ = json.Marshal(result)
		} else if receipt.Method == "x.workflow.run.promote" {
			var result PromoteResult
			_ = json.Unmarshal(receipt.Result, &result)
			result.Relations = nil
			result.ChildCount = 0
			receipt.Result, _ = json.Marshal(result)
		}
		value.Receipts[key] = receipt
	}
}

// A promotion can remove one item while its checklist remains active. Keep
// the live run's invocation receipt, but erase the removed item's projection
// and terminalize its individual operation receipts in the same checkpoint.
func (p *Provider) sanitizePromotedItemReceipts(value persisted, runID, itemID string) {
	removed := persisted{Receipts: map[string]receiptState{}}
	for key, receipt := range value.Receipts {
		if receipt.RunID != runID {
			continue
		}
		if receipt.Method == "x.workflow.invoke" {
			var result InvokeResult
			if json.Unmarshal(receipt.Result, &result) == nil {
				items := make([]ItemView, 0, len(result.Items))
				for _, item := range result.Items {
					if item.ID != itemID {
						items = append(items, item)
					}
				}
				result.Items = items
				receipt.Result, _ = json.Marshal(result)
				value.Receipts[key] = receipt
			}
		} else if receipt.ItemID == itemID {
			removed.Receipts[key] = receipt
		}
	}
	p.sanitizeTerminalReceipts(removed, runID)
	for key, receipt := range removed.Receipts {
		value.Receipts[key] = receipt
	}
}

func workflowCommit(id string, generation uint64) model.Commit {
	return model.Commit{Revision: model.Natural64(0), Results: []model.OpResult{{Ref: model.RefByID(id, model.Natural64(0)), Revision: model.Natural64(generation)}}, Warnings: []model.Warning{}}
}

func (p *Provider) collectExpiredReceipts(value *persisted) bool {
	changed := false
	for key, receipt := range value.Receipts {
		if !p.now().Before(receipt.ExpiresAt) && (receipt.RunID == "" || value.Runs[receipt.RunID].ID == "") {
			p.appendAudit(value, "workflow.receipt.gc", receipt)
			delete(value.Receipts, key)
			changed = true
		}
	}
	return changed
}

// GC uses the same cleanup as ordinary workflow state loads. It deliberately
// never deletes an unfinished run or its operation receipts, regardless of age.
func (p *Provider) GC(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, _, err := p.load(ctx)
	return err
}

// GetWithResolver reports current durable blocker satisfaction through the same
// typed bridge used by ready and claimNext.
func (p *Provider) GetWithResolver(ctx context.Context, by model.PrincipalRef, id model.Ref, meta model.WriteMeta, resolver BlockerResolver) (ItemView, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.load(ctx)
	if err != nil {
		return ItemView{}, err
	}
	item, ok := value.Items[id.ID]
	if !ok {
		return ItemView{}, fail("not_found", "workflow item was not found")
	}
	if err := p.authorized(item, by, meta); err != nil {
		return ItemView{}, err
	}
	view := p.itemViewIn(value, item)
	view.Blocked, err = itemBlockedWithResolver(ctx, value, item, by, meta, resolver)
	return view, err
}
func (p *Provider) ActiveIDsFor(ctx context.Context, by model.PrincipalRef, meta model.WriteMeta) (map[string]struct{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, _, err := p.load(ctx)
	if err != nil {
		return nil, err
	}
	return activeIDs(value, by, meta), nil
}

func activeIDs(value persisted, by model.PrincipalRef, meta model.WriteMeta) map[string]struct{} {
	ids := map[string]struct{}{}
	principal, _, err := principalScope(by, effectiveAgent(by, meta))
	if err != nil {
		return ids
	}
	agent := effectiveAgent(by, meta)
	for id, item := range value.Items {
		if item.Owner.Principal == principal && item.Owner.Agent == agent && (meta.Project == "" || item.Project == meta.Project) {
			ids[id] = struct{}{}
		}
	}
	return ids
}
