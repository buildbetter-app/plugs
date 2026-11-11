package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestSourceCommentAccountingCountsExistingCustomWire(t *testing.T) {
	for _, source := range []*SourceCommentOrigin{nil, {Project: "demo", Table: "comments", ID: "1", Author: "source-only"}} {
		comment := commentState{Body: "<large>\n世界\u2028" + strings.Repeat("x", 4096), Principal: model.PrincipalRef{Kind: "user", ID: "actor"}, Time: time.Date(2026, 11, 11, 3, 44, 0, 0, time.UTC), Source: source}
		actual, err := json.Marshal(comment)
		if err != nil {
			t.Fatal(err)
		}
		n, err := dumpformat.NativeDeltaJSONSize(comment)
		if err != nil || n != uint64(len(actual))+1 {
			t.Fatal("comment accounting differs from the existing wire", n, len(actual), err)
		}
	}
}

func TestSourceRelationAccountingMatchesOriginalBytesAndReleasesReplacement(t *testing.T) {
	const phase = "workflow_outbound_links"
	root := dumpformat.NewNativeDeltaBudget(0)
	owner := root.Scope()
	defer owner.Release()
	ctx := dumpformat.WithNativeDeltaBudget(context.Background(), owner)
	first := relationState{From: "workflow:item/a", To: model.RefByID("workflow:item/b", model.Natural64(0)), Kind: "waits-for", Metadata: &model.LinkMetadata{Gate: "all-children"}}
	second := relationState{From: first.From, To: model.RefByID("workflow:item/c", model.Natural64(0)), Kind: "blocks"}
	var plain, counted []relationState
	for _, added := range []relationState{first, second, first} {
		var err error
		plain, err = sourceRestoreAppendRelation(context.Background(), phase, plain, added)
		if err != nil {
			t.Fatal(err)
		}
		counted, err = sourceRestoreAppendRelation(ctx, phase, counted, added)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := json.Marshal(plain)
		got, _ := json.Marshal(counted)
		if !bytes.Equal(got, want) {
			t.Fatal("optional accounting changed relation bytes", string(got), string(want))
		}
		var retained uint64
		for _, relation := range counted {
			n, err := dumpformat.NativeDeltaJSONSize(relation)
			if err != nil {
				t.Fatal(err)
			}
			retained += n
		}
		usage := root.Usage()
		if usage.CurrentAccountedDecodedBytes != retained || usage.CurrentCategories[phase] != retained {
			t.Fatal("replacement retained old relations or lost a clone", usage, retained)
		}
		for name := range usage.CurrentCategories {
			if strings.HasPrefix(name, phase+"_") {
				t.Fatal("relation scratch outlived its helper", name, usage)
			}
		}
	}
	// Both original and counted paths detach the metadata in each kept link.
	first.Metadata.Gate = "any-children"
	if counted[0].Metadata.Gate != "all-children" || plain[0].Metadata.Gate != "all-children" {
		t.Fatal("relation clone aliases caller metadata")
	}
	owner.Release()
	if root.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatal("relation owner leaked", root.Usage())
	}
}

func TestSourceRelationAccountingFailurePreservesPreviousOwner(t *testing.T) {
	const phase = "workflow_inbound_links"
	old := relationState{From: "workflow:item/a", To: model.RefByID("workflow:item/b", model.Natural64(0)), Kind: "blocks"}
	added := relationState{From: "workflow:item/c", To: old.To, Kind: "waits-for", Metadata: &model.LinkMetadata{Gate: "all-children"}}
	measure := dumpformat.NewNativeDeltaBudget(0)
	measuredOwner := measure.Scope()
	if err := measuredOwner.ChargeJSON(phase, old); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceRestoreAppendRelation(dumpformat.WithNativeDeltaBudget(context.Background(), measuredOwner), phase, []relationState{old}, added); err != nil {
		t.Fatal(err)
	}
	peak := measure.Usage().PeakAccountedDecodedBytes
	measuredOwner.Release()
	oldBytes, err := dumpformat.NativeDeltaJSONSize(old)
	if err != nil {
		t.Fatal(err)
	}
	for _, cancelMidway := range []bool{false, true} {
		root := dumpformat.NewNativeDeltaBudget(peak - 1)
		owner := root.Scope()
		if err := owner.ChargeJSON(phase, old); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if cancelMidway {
			root.Observe(func(event dumpformat.NativeDeltaAllocationEvent) {
				if event.Action == "reserve" && event.Phase == phase+"_key_buffer" {
					cancel()
				}
			})
		}
		result, err := sourceRestoreAppendRelation(dumpformat.WithNativeDeltaBudget(ctx, owner), phase, []relationState{old}, added)
		cancel()
		var refusal *dumpformat.NativeDeltaDecodedLimitError
		if cancelMidway && !errors.Is(err, context.Canceled) || !cancelMidway && (!errors.As(err, &refusal) || refusal.MeasuredBytes <= refusal.LimitBytes || refusal.Overflow) {
			t.Fatal("missing cancellation or measured refusal", err)
		}
		if result != nil || root.Usage().CurrentAccountedDecodedBytes != oldBytes {
			t.Fatal("failed candidate released old state or retained scratch", root.Usage(), oldBytes)
		}
		owner.Release()
		if root.Usage().CurrentAccountedDecodedBytes != 0 {
			t.Fatal("failed relation ownership did not unwind")
		}
	}
}
