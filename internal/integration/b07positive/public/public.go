package b07public

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var Anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

const Agent = "b07-positive"

type Reply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Data model.ErrorData `json:"data"`
	} `json:"error"`
	Meta struct {
		Replayed bool `json:"replayed"`
	} `json:"_meta"`
}

type Host struct {
	T            *testing.T
	Runtime      *composition.Runtime
	Config       composition.Config
	Root         string
	Scopes       map[string]proof.Scope
	sequence     int
	observations int
}

func NewHost(t *testing.T, engine string) *Host {
	t.Helper()
	h := &Host{T: t, Root: proof.Output(t), Scopes: map[string]proof.Scope{}, Config: composition.Config{Driver: engine, Project: "b07positive", AuthMode: "anonymous", Address: composition.DefaultAddress}}
	if engine == "sqlite" {
		h.Config.DSN = filepath.Join(t.TempDir(), "ordinary-positive.db")
	}
	if engine == "postgres" {
		h.Config.DSN = Postgres(t)
	}
	h.open()
	t.Cleanup(h.close)
	if h.Runtime.Service.SourceRuntimeInstalled() {
		t.Fatal("ordinary fixture installed a source runtime")
	}
	capability, err := driver.RequireWorkflowCheckpointStore(h.Runtime.Store, h.Runtime.Lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.SaveCheckpoint(context.Background(), h.Runtime.Lease, "b07positive/unrelated", 0, []byte(`{"exact":"unrelated namespace"}`)); err != nil {
		t.Fatal(err)
	}
	state, err := pluginhost.NewWorkflowRuntimeState(capability, h.Runtime.Lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(context.Background(), "b07positive/sibling", map[string]any{"exact": "unrelated value inside workflow namespace", "number": 17}, "0"); err != nil {
		t.Fatal(err)
	}
	proof.Save(t, h.Root, "ordinary-binding.json", map[string]any{"engine": engine, "sourceRuntimeInstalled": false, "provider": "workflow/default", "lifetime": capability.WorkflowCheckpointLifetime()})
	return h
}

func (h *Host) open() {
	h.T.Helper()
	r, err := composition.Open(context.Background(), h.Config)
	if err != nil {
		h.T.Fatal(err)
	}
	h.Runtime = r
}

func (h *Host) close() {
	h.T.Helper()
	if h.Runtime != nil {
		if err := h.Runtime.Close(context.Background()); err != nil {
			h.T.Error(err)
		}
		h.Runtime = nil
	}
}

func (h *Host) Reopen() {
	h.T.Helper()
	old := h.Runtime.Store
	epoch := h.Runtime.Lease.Epoch()
	if h.Config.Driver != "memory" {
		h.close()
		h.open()
		if old == h.Runtime.Store || h.Runtime.Lease.Epoch() <= epoch {
			h.T.Fatal("SQL reopen did not replace the store and advance the lease epoch")
		}
	}
	proof.Save(h.T, h.Root, "reopen.json", map[string]any{"oldStore": fmt.Sprintf("%p", old), "newStore": fmt.Sprintf("%p", h.Runtime.Store), "oldEpoch": epoch, "newEpoch": h.Runtime.Lease.Epoch(), "processOnly": h.Config.Driver == "memory"})
}

func (h *Host) Call(method, label string, params map[string]any, want string) Reply {
	h.T.Helper()
	key := proof.Hash([]byte(label))[:32]
	h.Scopes[label] = proof.Scope{method, key}
	meta, ok := params["_meta"].(map[string]any)
	if !ok {
		meta = map[string]any{}
	}
	if _, specified := meta["project"]; !specified {
		meta["project"] = "b07positive"
	}
	meta["agent"], meta["idempotencyKey"] = Agent, key
	params["_meta"] = meta
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		h.T.Fatal(err)
	}
	response := h.Runtime.RPC.Dispatch(context.Background(), request)
	h.sequence++
	proof.Write(h.T, h.Root, fmt.Sprintf("rpc-%04d-request.json", h.sequence), request)
	proof.Write(h.T, h.Root, fmt.Sprintf("rpc-%04d-response.json", h.sequence), response)
	var reply Reply
	if err := json.Unmarshal(response, &reply); err != nil {
		h.T.Fatal(err)
	}
	if want != "" {
		if reply.Error == nil || reply.Error.Data.Code != model.ErrorCode(want) || len(reply.Result) != 0 {
			h.T.Fatalf("%s expected %s: %s", method, want, response)
		}
	} else if reply.Error != nil || len(reply.Result) == 0 {
		h.T.Fatalf("%s failed: %s", method, response)
	}
	return reply
}

func (h *Host) Capture() proof.State {
	h.T.Helper()
	state := proof.Capture(h.T, h.Runtime.Store, Anonymous, Agent, h.Scopes)
	h.observations++
	proof.Save(h.T, h.Root, fmt.Sprintf("state-%04d.json", h.observations), state)
	return state
}

// Run keeps fixture failures attached to the named behavior that exercised it.
func (h *Host) Run(name string, run func(*testing.T)) {
	parent := h.T
	parent.Run(name, func(t *testing.T) {
		h.T = t
		defer func() { h.T = parent }()
		run(t)
	})
}

func (h *Host) Create(label string) model.Ref {
	h.T.Helper()
	reply := h.Call("bead.create", label, map[string]any{"type": "issue", "fields": map[string]any{"title": label}}, "")
	var commit model.Commit
	if err := json.Unmarshal(reply.Result, &commit); err != nil || len(commit.Results) != 1 {
		h.T.Fatal("invalid create result", err)
	}
	return commit.Results[0].Ref
}
