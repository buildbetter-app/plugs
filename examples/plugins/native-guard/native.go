// Package nativeguard is a compiled-in title validator and enricher. Its only
// dependencies are the public plugin API, the Go SDK and the standard library.
package nativeguard

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/pluginapi"
	sdk "github.com/buildbetter-app/plugs/sdk/go"
)

const (
	Alias           = "nativeguard"
	Owner           = "nativeguard-enrich"
	NormalizedField = "x_nativeguard_title"
	WordsField      = "x_nativeguard_words"
	MaxTitleRunes   = 80
	MaxResources    = 16
)

//go:embed manifest.json
var manifestJSON []byte

// Item contains only the fields the post subscriber is granted. Revision is
// the record revision observed in the snapshot pinned to the committed write.
type Item struct {
	Resource pluginapi.ResourceRef
	Revision pluginapi.Revision
	Title    string
	Words    int
}

type Observation struct {
	RequestID string
	Revision  pluginapi.Revision
	Items     []Item
}

// Observer is an optional in-process sink. It receives detached values, must
// respect cancellation, and cannot turn its failure into a graph rollback.
type Observer func(context.Context, Observation) error

// Native constructs one instance; the Go composition root supplies its grants.
// No runtime installation, dispatcher, state, secrets or egress is required.
func Native(instance string, observe Observer) (sdk.Native, error) {
	if !pluginapi.ValidName(instance) {
		return sdk.Native{}, fmt.Errorf("invalid native guard instance %q", instance)
	}
	manifest, err := pluginapi.DecodeManifest(manifestJSON)
	if err != nil {
		return sdk.Native{}, err
	}
	return sdk.Native{
		Manifest: manifest.Plugins[0],
		Instance: instance,
		New: func(pluginapi.Host) (sdk.Registration, error) {
			return sdk.Registration{
				Mutators:   map[string]pluginapi.Mutator{"enrich": sdk.MutateFunc(enrich)},
				Validators: map[string]pluginapi.Validator{"title": sdk.ValidateFunc(validate)},
				Posts: map[string]pluginapi.PostSubscriber{"observe": sdk.AfterFunc(func(ctx context.Context, c pluginapi.CommittedChange, reader pluginapi.SnapshotReader) error {
					return after(ctx, c, reader, observe)
				})},
			}, nil
		},
	}, nil
}

// liveChanges emits one atomic post-image per resource. A create followed by
// a patch has two operation changes but must produce only one owned-field patch.
func liveChanges(ctx context.Context, changes []pluginapi.Change) ([]pluginapi.Change, error) {
	out := []pluginapi.Change{}
	seen := map[pluginapi.ResourceRef]bool{}
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if change.Resource.Kind != "bead" {
			return nil, fmt.Errorf("native guard supports bead resources only")
		}
		switch change.Op.Kind {
		case "delete":
			continue
		case "create", "patch":
		default:
			return nil, fmt.Errorf("native guard does not support operation %q", change.Op.Kind)
		}
		if len(change.After) == 0 || bytes.Equal(bytes.TrimSpace(change.After), []byte("null")) {
			continue
		}
		if !seen[change.Resource] {
			seen[change.Resource] = true
			out = append(out, change)
		}
	}
	if len(out) > MaxResources {
		return nil, fmt.Errorf("native guard supports at most %d live resources per write", MaxResources)
	}
	return out, ctx.Err()
}

func fields(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var doc struct {
		Fields map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if doc.Fields == nil {
		return nil, fmt.Errorf("native guard requires a fields object")
	}
	return doc.Fields, nil
}

func titleValue(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", fmt.Errorf("native guard requires string field %s", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}

func enrich(ctx context.Context, c pluginapi.ProposedChange, _ pluginapi.SnapshotReader) ([]pluginapi.Patch, error) {
	changes, err := liveChanges(ctx, c.Changes)
	if err != nil {
		return nil, err
	}
	patches := make([]pluginapi.Patch, 0, len(changes))
	for _, change := range changes {
		body, err := fields(change.After)
		if err != nil {
			return nil, err
		}
		title, err := titleValue(body, "title")
		if err != nil {
			return nil, err
		}
		words := strings.Fields(title)
		normalized, _ := json.Marshal(strings.Join(words, " "))
		count, _ := json.Marshal(len(words))
		patches = append(patches, pluginapi.Patch{Resource: change.Resource, Paths: map[string]json.RawMessage{
			"fields." + NormalizedField: normalized,
			"fields." + WordsField:      count,
		}})
	}
	return patches, ctx.Err()
}

func validate(ctx context.Context, c pluginapi.ProposedChange, _ pluginapi.SnapshotReader) (pluginapi.Verdict, error) {
	changes, err := liveChanges(ctx, c.Changes)
	if err != nil {
		return pluginapi.Verdict{}, err
	}
	verdict := pluginapi.Verdict{Allow: true}
	for _, change := range changes {
		body, err := fields(change.After)
		if err != nil {
			return pluginapi.Verdict{}, err
		}
		title, err := titleValue(body, NormalizedField)
		if err != nil {
			return pluginapi.Verdict{}, err
		}
		if n := utf8.RuneCountInString(title); n == 0 || n > MaxTitleRunes {
			verdict.Allow = false
			verdict.Violations = append(verdict.Violations, pluginapi.Violation{
				Guard: "x.nativeguard.title", Message: "normalized title must contain 1 to 80 Unicode code points",
				Details: map[string]any{"field": NormalizedField, "ref": change.Resource.Ref, "runes": n},
			})
		}
	}
	return verdict, ctx.Err()
}

func after(ctx context.Context, c pluginapi.CommittedChange, reader pluginapi.SnapshotReader, observe Observer) error {
	changes, err := liveChanges(ctx, c.Changes)
	if err != nil {
		return err
	}
	if len(changes) == 0 {
		return nil
	}
	refs := make([]pluginapi.Ref, len(changes))
	for i, change := range changes {
		refs[i] = change.Resource.Ref
	}
	records, err := reader.Get(ctx, refs)
	if err != nil {
		return err
	}
	if len(records) != len(refs) {
		return fmt.Errorf("native guard post read missed a committed record")
	}
	observation := Observation{RequestID: c.RequestID, Revision: c.Revision, Items: make([]Item, 0, len(records))}
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		body, err := fields(record.Body)
		if err != nil {
			return err
		}
		title, err := titleValue(body, NormalizedField)
		if err != nil {
			return err
		}
		var words int
		if err := json.Unmarshal(body[WordsField], &words); err != nil {
			return err
		}
		observation.Items = append(observation.Items, Item{
			Resource: pluginapi.ResourceRef{Kind: record.Kind, Ref: record.Ref}, Revision: record.Revision, Title: title, Words: words,
		})
	}
	if observe != nil {
		return observe(ctx, observation)
	}
	return ctx.Err()
}
