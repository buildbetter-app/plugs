// Package workflow implements the compiled-in workflow plugin's Option C
// operational plane. It stores only active runs and bounded terminal receipts
// through pluginapi.DurableState; no graph record or history writer is used.
package workflow

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

const (
	Alias      = "workflow"
	ItemPrefix = "workflow:item/"
	stateKey   = "workflow/state"
	claimTTL   = 30 * time.Minute
	// ClaimTTL is the original duration recorded in workflow claim responses.
	// Replays retain that duration instead of recomputing a remaining lease.
	ClaimTTL = claimTTL
)

type ChecklistItem struct {
	Label string `json:"label"`
	Title string `json:"title"`
	Type  string `json:"type,omitempty"`
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
	ID       string         `json:"id"`
	RunID    string         `json:"runId"`
	Label    string         `json:"label"`
	Title    string         `json:"title"`
	Type     string         `json:"type"`
	Project  string         `json:"project"`
	Status   string         `json:"status"`
	Plane    string         `json:"plane"`
	Kind     string         `json:"kind"`
	Plugin   string         `json:"plugin"`
	Revision model.Revision `json:"revision"`
	Claim    *ClaimView     `json:"claim,omitempty"`
	Comments []CommentView  `json:"comments,omitempty"`
}

// ReadySnapshot binds one ready list to its checkpoint generation. ValidUntil
// bounds membership changes caused by a matching claim expiring without a write.
type ReadySnapshot struct {
	Items      []ItemView
	Revision   pluginapi.Revision
	ValidUntil *time.Time
}

type ClaimView struct {
	Principal  model.PrincipalRef `json:"principal"`
	Agent      string             `json:"agent,omitempty"`
	ExpiresAt  time.Time          `json:"expiresAt"`
	Generation model.Revision     `json:"generation"`
}

type CommentView struct {
	Body      string             `json:"body"`
	Principal model.PrincipalRef `json:"principal"`
	Time      time.Time          `json:"time"`
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
	mu          sync.Mutex
}

// persistentState is a required marker for ephemeral invocation. A CheckpointStore is durable
// only when the selected driver can reopen it after a process restart; the
// in-memory driver deliberately reports false so ephemeral mode fails closed.
type persistentState interface {
	Persistent() bool
}

type persisted struct {
	Runs     map[string]runState     `json:"runs"`
	Items    map[string]itemState    `json:"items"`
	Receipts map[string]receiptState `json:"receipts"`
	Audit    []auditState            `json:"audit,omitempty"`
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
	ID         string         `json:"id"`
	RunID      string         `json:"runId"`
	Label      string         `json:"label"`
	Title      string         `json:"title"`
	Type       string         `json:"type"`
	Project    string         `json:"project"`
	Owner      ownerState     `json:"owner"`
	Status     string         `json:"status"`
	Generation uint64         `json:"generation"`
	Claim      *claimState    `json:"claim,omitempty"`
	Comments   []commentState `json:"comments,omitempty"`
}
type claimState struct {
	Principal  model.PrincipalRef `json:"principal"`
	Agent      string             `json:"agent,omitempty"`
	ExpiresAt  time.Time          `json:"expiresAt"`
	Generation uint64             `json:"generation"`
}
type commentState struct {
	Body      string             `json:"body"`
	Principal model.PrincipalRef `json:"principal"`
	Time      time.Time          `json:"time"`
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

func (p *Provider) load(ctx context.Context) (persisted, pluginapi.Revision, error) {
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
	if !p.persistent {
		return InvokeResult{}, fail("invalid_params", "ephemeral workflows require persistent checkpoint storage")
	}
	if strings.TrimSpace(req.Template) == "" || strings.TrimSpace(req.Project) == "" || len(req.Checklist) == 0 || len(req.Checklist) > 100 {
		return InvokeResult{}, fail("invalid_params", "template, project and one to one hundred checklist items are required")
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
	run := runState{ID: runID, Template: req.Template, Project: req.Project, Owner: ownerState{Principal: principal, Agent: agent, Project: req.Project}, State: "active", CreatedAt: p.now().UTC()}
	result := InvokeResult{Plane: "ephemeral", Run: &RunView{ID: runID, Template: req.Template, Project: req.Project, Mode: req.Mode, State: run.State, Owner: principal, CreatedAt: run.CreatedAt}, Items: []ItemView{}}
	seen := map[string]bool{}
	for _, input := range req.Checklist {
		if strings.TrimSpace(input.Label) == "" || strings.TrimSpace(input.Title) == "" || seen[input.Label] {
			return InvokeResult{}, fail("invalid_params", "checklist labels and titles must be nonempty and labels unique")
		}
		seen[input.Label] = true
		itemID, err := newID(ItemPrefix)
		if err != nil {
			return InvokeResult{}, err
		}
		itemType := input.Type
		if itemType == "" {
			itemType = "task"
		}
		item := itemState{ID: itemID, RunID: runID, Label: input.Label, Title: input.Title, Type: itemType, Project: req.Project, Owner: run.Owner, Status: "open"}
		value.Items[itemID] = item
		run.ItemIDs = append(run.ItemIDs, itemID)
		result.Items = append(result.Items, p.itemView(item))
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
	return itemViewAt(item, p.now())
}

func itemViewAt(item itemState, now time.Time) ItemView {
	view := ItemView{ID: item.ID, RunID: item.RunID, Label: item.Label, Title: item.Title, Type: item.Type, Project: item.Project, Status: item.Status, Plane: "ephemeral", Kind: "workflow.item", Plugin: Alias, Revision: model.Natural64(item.Generation)}
	if item.Claim != nil && !now.After(item.Claim.ExpiresAt) {
		view.Claim = &ClaimView{Principal: item.Claim.Principal, Agent: item.Claim.Agent, ExpiresAt: item.Claim.ExpiresAt, Generation: model.Natural64(item.Claim.Generation)}
	}
	for _, comment := range item.Comments {
		view.Comments = append(view.Comments, CommentView{Body: comment.Body, Principal: comment.Principal, Time: comment.Time})
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

func itemReady(item itemState, now time.Time) bool {
	if item.Status == "open" {
		return item.Claim == nil || !now.Before(item.Claim.ExpiresAt)
	}
	return item.Status == "claimed" && item.Claim != nil && !now.Before(item.Claim.ExpiresAt)
}

func (p *Provider) readySnapshot(value persisted, revision pluginapi.Revision, by model.PrincipalRef, project string, types []string, meta model.WriteMeta, now time.Time) (ReadySnapshot, error) {
	principal, _, err := principalScope(by, effectiveAgent(by, meta))
	if err != nil {
		return ReadySnapshot{}, fail("invalid_params", "invalid principal")
	}
	snapshot := ReadySnapshot{Items: make([]ItemView, 0), Revision: revision}
	for _, item := range value.Items {
		if item.Project != project || !matchesType(item.Type, types) || item.Owner.Principal != principal || item.Owner.Agent != effectiveAgent(by, meta) {
			continue
		}
		if itemReady(item, now) {
			snapshot.Items = append(snapshot.Items, itemViewAt(item, now))
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
	return p.itemView(item), nil
}

func (p *Provider) Ready(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta) ([]ItemView, error) {
	snapshot, err := p.ReadySnapshot(ctx, by, project, types, meta)
	return snapshot.Items, err
}

// ReadySnapshot reads the list and its generation together under the provider
// lock. Cleanup, when needed, publishes first and supplies the returned revision.
func (p *Provider) ReadySnapshot(ctx context.Context, by model.PrincipalRef, project string, types []string, meta model.WriteMeta) (ReadySnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, revision, err := p.load(ctx)
	if err != nil {
		return ReadySnapshot{}, err
	}
	return p.readySnapshot(value, revision, by, project, types, meta, p.now())
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
	return p.claimNext(ctx, by, project, types, nil, meta, nil)
}

// ClaimNextWithResources intersects the selector's optional resource IDs before
// atomically choosing a candidate. The service supplies its full selector in
// ClaimNextIdentity; direct calls without it bind resources in their digest.
func (p *Provider) ClaimNextWithResources(ctx context.Context, by model.PrincipalRef, project string, types, resources []string, meta model.WriteMeta) (ItemView, bool, bool, error) {
	return p.claimNext(ctx, by, project, types, resources, meta, nil)
}

// ClaimNextSelected commits the candidate chosen and authorized by the ordinary
// service queue walk. Its receipt retains the full selector and wrapped result
// shared by ClaimNext and ReplayClaimNext, never a bare bead.claim response.
func (p *Provider) ClaimNextSelected(ctx context.Context, by model.PrincipalRef, ref model.Ref, project string, types []string, meta model.WriteMeta) (ItemView, bool, bool, error) {
	if ref.Validate() != nil || !ref.IsID() || !strings.HasPrefix(ref.ID, ItemPrefix) || ref.Branch.String() != "0" {
		return ItemView{}, false, false, fail("invalid_params", "workflow claimNext requires a branch-zero item reference")
	}
	return p.claimNext(ctx, by, project, types, nil, meta, &ref)
}

func (p *Provider) claimNext(ctx context.Context, by model.PrincipalRef, project string, types, resources []string, meta model.WriteMeta, selectedRef *model.Ref) (ItemView, bool, bool, error) {
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
		if item.Project == project && itemReady(item, p.now()) && matchesType(item.Type, types) && item.Owner.Principal == principal && item.Owner.Agent == effectiveAgent(by, meta) {
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
		if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
			return ItemView{}, false, false, err
		}
		return ItemView{}, false, false, nil
	}
	selected.Status = "claimed"
	selected.Generation++
	selected.Claim = &claimState{Principal: by, Agent: effectiveAgent(by, meta), ExpiresAt: p.now().UTC().Add(claimTTL), Generation: selected.Generation}
	value.Items[selected.ID] = *selected
	item := p.itemView(*selected)
	replay = claimNextResult{Found: true, Item: item}
	receipt, err := p.storeReceipt(value, by, meta, request, replay, selected.RunID, selected.ID)
	if err != nil {
		return ItemView{}, false, false, err
	}
	p.appendAudit(&value, "workflow.claim_next", receipt)
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
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
	if !itemReady(item, p.now()) {
		return ItemView{}, fail("busy", "workflow item already has a live claim")
	}
	item.Status = "claimed"
	item.Generation++
	item.Claim = &claimState{Principal: by, Agent: effectiveAgent(by, meta), ExpiresAt: p.now().UTC().Add(claimTTL), Generation: item.Generation}
	value.Items[item.ID] = item
	view := p.itemView(item)
	receipt, err := p.storeReceipt(value, by, meta, req, view, item.RunID, item.ID)
	if err != nil {
		return ItemView{}, err
	}
	p.appendAudit(&value, "workflow.claim", receipt)
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
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
	item.Status = "closed"
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
	if err := p.mapSaveError(p.save(ctx, value, revision)); err != nil {
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
			result.Item = ItemView{ID: receipt.ItemID, RunID: runID, Status: "closed", Plane: "ephemeral", Kind: "workflow.item", Plugin: Alias, Revision: result.Item.Revision}
			receipt.Result, _ = json.Marshal(result)
		} else if receipt.Method == "bead.claim" {
			var item ItemView
			if json.Unmarshal(receipt.Result, &item) == nil {
				item = ItemView{ID: receipt.ItemID, RunID: runID, Status: "closed", Plane: "ephemeral", Kind: "workflow.item", Plugin: Alias, Revision: item.Revision}
				receipt.Result, _ = json.Marshal(item)
			}
		}
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
