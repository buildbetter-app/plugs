package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const freshFieldProfileSHA = "ff5514c3f022d2c1ea09843e0eec2c380e69a085e9e9265a52b07fbc0113893b"

type freshNativeWindow struct {
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
}

type freshNativeOperation struct {
	Kind                string                  `json:"kind"`
	Actor               model.PrincipalRef      `json:"actor"`
	NativeActor         string                  `json:"native_actor"`
	Project             string                  `json:"project"`
	Parent              string                  `json:"parent,omitempty"`
	Target              string                  `json:"target,omitempty"`
	Fields              map[string]any          `json:"fields,omitempty"`
	Text                string                  `json:"text,omitempty"`
	Clock               time.Time               `json:"clock"`
	ExpectedID          string                  `json:"expected_id,omitempty"`
	DerivedID           string                  `json:"independently_derived_id,omitempty"`
	NativeRequest       []string                `json:"native_request"`
	NativeResponse      json.RawMessage         `json:"native_response"`
	NativeCreateJournal json.RawMessage         `json:"native_create_journal,omitempty"`
	Before              driver.OfflineAllocator `json:"allocator_before"`
	After               driver.OfflineAllocator `json:"allocator_after"`
	Window              freshNativeWindow       `json:"native_window"`
}

type freshNativeComment struct {
	Item   string    `json:"item"`
	Body   string    `json:"body"`
	Author string    `json:"author"`
	Time   time.Time `json:"time"`
}

type freshNativeItem struct {
	Document pipeline.Document `json:"document"`
	Native   json.RawMessage   `json:"native"`
}

type freshNativeRequest struct {
	Format             string                 `json:"format"`
	RunID              string                 `json:"run_id"`
	Case               string                 `json:"case"`
	Engine             string                 `json:"engine"`
	Mode               string                 `json:"mode"`
	FirstSequence      json.RawMessage        `json:"restore_first_sequence,omitempty"`
	CaptureSHA256      string                 `json:"capture_sha256"`
	RecipeExportSHA256 string                 `json:"recipe_export_sha256"`
	NativeCLISHA256    string                 `json:"native_cli_sha256"`
	ProducerSHA256     string                 `json:"producer_sha256"`
	Operations         []freshNativeOperation `json:"operations"`
	Items              []freshNativeItem      `json:"items"`
	Comments           []freshNativeComment   `json:"comments"`
}

// These facts come only from native SQL. The independent candidate derivation
// never calls the receiver, pipeline allocator, or a driver minting helper.
type freshNativeFacts struct {
	Allocator driver.OfflineAllocator `json:"allocator"`
	IDs       []string                `json:"all_native_ids"`
	RootCount uint64                  `json:"native_top_level_count"`
}

func freshDerivedID(f freshNativeFacts, op freshNativeOperation) (string, error) {
	a := f.Allocator
	if a.Validate() != nil || a.Mode != "hash" || a.MinHashLength < 3 || a.MaxHashLength > 8 || a.MinHashLength > a.MaxHashLength || op.Clock.IsZero() {
		return "", fmt.Errorf("unbound native hash policy/clock")
	}
	occupied := map[string]bool{}
	for _, id := range f.IDs {
		if id == "" || occupied[id] {
			return "", fmt.Errorf("native occupancy repeats/omits an identity")
		}
		occupied[id] = true
	}
	if op.Kind == "child" {
		if !occupied[op.Parent] {
			return "", fmt.Errorf("native child parent absent")
		}
		last := a.Children[op.Parent]
		for id := range occupied {
			suffix, ok := strings.CutPrefix(id, op.Parent+".")
			if !ok || strings.Contains(suffix, ".") {
				continue
			}
			n, err := strconv.ParseUint(suffix, 10, 64)
			if err == nil && n > last {
				last = n
			}
		}
		if last == math.MaxUint64 {
			return "", fmt.Errorf("native child counter exhausted")
		}
		return op.Parent + "." + strconv.FormatUint(last+1, 10), nil
	}
	if op.Kind != "root" {
		return "", fmt.Errorf("only item creates allocate IDs")
	}
	length := int(a.MaxHashLength)
	for n := int(a.MinHashLength); n <= int(a.MaxHashLength); n++ {
		if 1-math.Exp(-float64(f.RootCount)*float64(f.RootCount)/(2*math.Pow(36, float64(n)))) <= a.MaxCollisionProb {
			length = n
			break
		}
	}
	title, titleOK := op.Fields["title"].(string)
	description, descriptionOK := op.Fields["description"].(string)
	if !titleOK || !descriptionOK || op.NativeActor == "" {
		return "", fmt.Errorf("native original hash tuple missing")
	}
	width := map[int]int{3: 2, 4: 3, 5: 4, 6: 4, 7: 5, 8: 5}
	for n := length; n <= int(a.MaxHashLength); n++ {
		for nonce := 0; nonce < 10; nonce++ {
			preimage := fmt.Sprintf("%s|%s|%s|%d|%d", title, description, op.NativeActor, op.Clock.UnixNano(), nonce)
			digest := sha256.Sum256([]byte(preimage))
			text := new(big.Int).SetBytes(digest[:width[n]]).Text(36)
			if len(text) < n {
				text = strings.Repeat("0", n-len(text)) + text
			}
			if len(text) > n {
				text = text[len(text)-n:]
			}
			candidate := a.Prefix + "-" + text
			if !occupied[candidate] {
				return candidate, nil
			}
		}
	}
	return "", fmt.Errorf("native hash candidate space exhausted")
}

// The finite native response adapter lives in the same-process consumer.
// Here the expected Plug representation is authored from the original request
// tuple and original native response clock, before any target is opened.
func freshExpectedItem(op freshNativeOperation) pipeline.Document {
	fields := map[string]any{}
	for name, value := range op.Fields {
		fields[name] = value
	}
	fields["status"], fields["template"] = "open", false
	when := op.Clock
	doc := pipeline.Document{ID: op.ExpectedID, Type: "task", Project: op.Project, Fields: fields, CreatedAt: &when}
	var native struct {
		Labels []string `json:"labels"`
	}
	if json.Unmarshal(op.NativeResponse, &native) == nil && len(native.Labels) > 0 {
		doc.Labels = native.Labels
		labels := make([]any, len(native.Labels))
		for index, label := range native.Labels {
			labels[index] = label
		}
		fields["labels"] = labels
	}
	if op.Parent != "" {
		parent := model.RefByID(op.Parent, model.Natural64(0))
		doc.Parent = &parent
	}
	return doc
}
