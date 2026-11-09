package authzrpc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// readyProjectFilter binds a finite compiled policy to the current project
// facts before the core Ready traversal starts. The returned template remains
// request-bound; each traversal page reloads facts from its own reader and
// retains the existing final authority fence.
func (s *GuardedService) readyProjectFilter(ctx context.Context, by model.PrincipalRef, snapshot Snapshot, project, method string, action authz.Action) (authz.Caller, *slice, authz.FilterTemplate, error) {
	caller, err := s.checker.callerFor(ctx, by)
	if err != nil {
		return authz.Caller{}, nil, authz.FilterTemplate{}, authzError(err)
	}
	authority, err := s.checker.load(ctx, snapshot, caller)
	if err != nil {
		return caller, nil, authz.FilterTemplate{}, &model.Error{Message: "authorization facts unavailable", Data: model.ErrorData{Code: "authz_unavailable", Retryable: true}}
	}
	if _, found := authority.projectByName(project); !found {
		return caller, authority, authz.FilterTemplate{}, driver.ErrUnsupported
	}
	template, err := s.checker.chain.Compile(authz.ListRequest{Caller: caller, Action: action, ResourceKind: authz.Bead, Method: method})
	if err != nil {
		return caller, authority, authz.FilterTemplate{}, err
	}
	return caller, authority, template, nil
}

// compiledReady is the main-stage guarded finite-policy entry point. The core
// service owns the complete Ready domain traversal and applies this trusted
// predicate before slicing; the fallback below remains available for
// check-only policies and storage adapters without that capability.
func (s *GuardedService) compiledReady(ctx context.Context, by model.PrincipalRef, req service.ReadyRequest) (service.BeadPage, error) {
	readyFilter, compiled, err := s.compiledReadyFilter(ctx, by, req.Project)
	if err != nil {
		return service.BeadPage{}, err
	}
	if !compiled {
		return service.BeadPage{}, driver.ErrUnsupported
	}
	return s.collectReady(ctx, by, req, readyFilter, true)
}

// readyMembershipImage is the semantic source image used by the v2 cursor
// commitment. Parent disclosure and links are intentionally excluded: they
// are projections after membership selection, while source revision, fields,
// workflow plane and identity are the membership inputs.
type readyMembershipImage struct {
	ID            string         `json:"id"`
	Type          string         `json:"type"`
	Project       string         `json:"project"`
	Status        string         `json:"status"`
	Revision      model.Revision `json:"revision"`
	Fields        map[string]any `json:"fields,omitempty"`
	Plane         string         `json:"plane,omitempty"`
	Kind          string         `json:"kind,omitempty"`
	Plugin        string         `json:"plugin,omitempty"`
	WorkflowRunID string         `json:"workflowRunId,omitempty"`
}

func readyMembershipImageOf(item service.Bead) readyMembershipImage {
	return readyMembershipImage{ID: item.ID, Type: item.Type, Project: item.Project, Status: item.Status,
		Revision: item.Revision, Fields: item.Fields, Plane: item.Plane, Kind: item.Kind,
		Plugin: item.Plugin, WorkflowRunID: item.WorkflowRunID}
}

func writeReadyFrame(h interface{ Write([]byte) (int, error) }, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(value)
}

func writeReadyCount(h interface{ Write([]byte) (int, error) }, count uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], count)
	_, _ = h.Write(encoded[:])
}

func readyMembershipPriority(item service.Bead) (int, error) {
	value, present := item.Fields["priority"]
	if !present {
		return 2, nil
	}
	// Natural canonicalizes JSON integer spellings symbolically (including
	// decimal and exponent forms) using the same lossless path as ontology
	// Priority, before the fixed P0 through P4 bound is applied.
	raw, err := json.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("priority is not an integer")
	}
	priority, err := model.Natural(string(raw)).Uint64()
	if err != nil || priority > 4 {
		return 0, fmt.Errorf("priority is not an integer from P0 through P4")
	}
	return int(priority), nil
}

// readyMembershipV2 retains the released plane-separated commitment for
// continuations issued before the combined public Ready order. New cursors
// use v3, which also binds the final interleaving of those two planes.
func readyMembershipV2(items []service.Bead) (string, error) {
	return readyMembershipVersion(items, false)
}

func readyMembershipV3(items []service.Bead) (string, error) {
	return readyMembershipVersion(items, true)
}

func readyMembershipVersion(items []service.Bead, ordered bool) (string, error) {
	core := sha256.New()
	sequence := sha256.New()
	workflow := make([][]byte, 0)
	var coreCount uint64
	for _, item := range items {
		image, err := json.Marshal(readyMembershipImageOf(item))
		if err != nil {
			return "", err
		}
		if ordered {
			writeReadyFrame(sequence, image)
		}
		if item.Kind == "workflow.item" {
			workflow = append(workflow, image)
			continue
		}
		priority, err := readyMembershipPriority(item)
		if err != nil {
			return "", err
		}
		leaf, err := json.Marshal(struct {
			ID       string          `json:"id"`
			Priority int             `json:"priority"`
			Image    json.RawMessage `json:"image"`
		}{ID: item.ID, Priority: priority, Image: image})
		if err != nil {
			return "", err
		}
		writeReadyFrame(core, leaf)
		coreCount++
	}
	coreCommit := sha256.New()
	_, _ = coreCommit.Write([]byte("plug.ready.radix.v2\x00"))
	writeReadyCount(coreCommit, coreCount)
	_, _ = coreCommit.Write([]byte(hex.EncodeToString(core.Sum(nil))))

	membership := sha256.New()
	_, _ = membership.Write([]byte("plug.ready.membership.v2\x00"))
	writeReadyCount(membership, coreCount)
	_, _ = membership.Write([]byte(hex.EncodeToString(coreCommit.Sum(nil))))
	writeReadyCount(membership, uint64(len(workflow)))
	for _, image := range workflow {
		writeReadyFrame(membership, image)
	}
	v2 := "v2:" + hex.EncodeToString(membership.Sum(nil))
	if !ordered {
		return v2, nil
	}
	// The v2 component keeps its semantic source and numeric-priority binding.
	// Framing each authorized image in final public order makes a clock-only
	// durable/workflow swap change the signed continuation even without a write.
	v3 := sha256.New()
	_, _ = v3.Write([]byte("plug.ready.membership.v3\x00"))
	writeReadyFrame(v3, []byte(v2))
	writeReadyCount(v3, uint64(len(items)))
	_, _ = v3.Write(sequence.Sum(nil))
	return "v3:" + hex.EncodeToString(v3.Sum(nil)), nil
}
