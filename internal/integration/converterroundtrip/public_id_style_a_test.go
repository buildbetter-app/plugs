package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are assertions from one native recording. No expected ID is supplied
// to the Writer, allocator, importer or shared lifecycle.
type publicIDOperation struct {
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
	Window              publicIDWindow          `json:"native_window"`
}

type publicIDWindow struct {
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended"`
}

type publicIDRequest struct {
	Format             string              `json:"format"`
	RunID              string              `json:"run_id"`
	Case               string              `json:"case"`
	Engine             string              `json:"engine"`
	Mode               string              `json:"mode"`
	FirstSequence      json.RawMessage     `json:"restore_first_sequence,omitempty"`
	CaptureSHA256      string              `json:"capture_sha256"`
	RecipeExportSHA256 string              `json:"recipe_export_sha256"`
	NativeCLISHA256    string              `json:"native_cli_sha256"`
	ProducerSHA256     string              `json:"producer_sha256"`
	Operations         []publicIDOperation `json:"operations"`
	Items              []publicIDItem      `json:"items"`
	Comments           []publicIDComment   `json:"comments"`
}

type publicIDItem struct {
	Document pipeline.Document `json:"document"`
	// Full native response, including default/omitted fields, remains bound.
	Native json.RawMessage `json:"native"`
}

type publicIDComment struct {
	Item   string    `json:"item"`
	Body   string    `json:"body"`
	Author string    `json:"author"`
	Time   time.Time `json:"time"`
}

func publicIDDecode(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("consumer input has trailing JSON")
	}
	return nil
}

func publicIDInput(t *testing.T) publicIDRequest {
	t.Helper()
	path := os.Getenv("PLUG_ID_STYLE_A_REQUEST")
	if !filepath.IsAbs(path) || publicProofHashFile(t, path) != os.Getenv("PLUG_ID_STYLE_A_REQUEST_SHA256") {
		t.Fatal("native continuation input binding differs")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var request publicIDRequest
	if err := publicIDDecode(raw, &request); err != nil {
		t.Fatal(err)
	}
	if request.Format != "public-id-style-a-same-run/1" || request.RunID == "" || request.RunID != os.Getenv("GITHUB_RUN_ID") || request.Engine != os.Getenv("PLUG_PUBLIC_PROOF_DRIVER") || len(request.CaptureSHA256) != 64 || len(request.RecipeExportSHA256) != 64 || len(request.NativeCLISHA256) != 64 || len(request.ProducerSHA256) != 64 || len(request.Operations) != 5 || len(request.Items) != 3 || len(request.Comments) != 2 {
		t.Fatal("native case/source/operation census differs")
	}
	if err := publicIDValidateRequest(request); err != nil {
		t.Fatal(err)
	}
	return request
}

func TestPublicIDStyleASameRunContinuation(t *testing.T) {
	selected := os.Getenv("PLUG_ID_STYLE_A_RUN")
	if selected == "" {
		t.Skip("explicit same-run native consumer inputs required")
	}
	if selected != "1" {
		t.Fatal("invalid same-run native consumer opt-in")
	}
	publicProofGuard(t)
	t.Setenv("PATH", "/tmp/plug-swarm/bin/worker-safe:"+os.Getenv("PATH"))
	consumer := publicIDConsumer{request: publicIDInput(t)}
	runPublicNativeExternalCapture(t, consumer.write, consumer.assertReopened)
	if !consumer.wrote || !consumer.asserted {
		t.Fatal("actual native comparison/post-write reopen assertion incomplete")
	}
	clockCount, positive, maxGap := 0, 0, time.Duration(0)
	for _, operation := range consumer.request.Operations {
		if operation.Kind != "comment" {
			gap, err := publicIDNativeCreateClock(operation)
			if err != nil {
				t.Fatal(err)
			}
			clockCount++
			if gap > 0 {
				positive++
			}
			if gap > maxGap {
				maxGap = gap
			}
		}
	}
	publicProofJSON(t, filepath.Join(os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT"), "id-result.json"), map[string]any{
		"case": consumer.request.Case, "equality": true, "native_capture_sha256": consumer.request.CaptureSHA256,
		"restore_mode": consumer.request.Mode, "restore_first_sequence": consumer.request.FirstSequence,
		"exact_initial_rows": len(consumer.initial), "continued_items": len(consumer.request.Items),
		"new_comment_tuple_multiset": len(consumer.request.Comments), "per_write_events": len(consumer.request.Operations),
		"post_write_reopen_asserted": consumer.asserted, "independent_review": "PENDING",
		"native_create_clock_gaps": map[string]any{"items": clockCount, "positive": positive, "maximum_nanoseconds": maxGap.Nanoseconds()},
	})
}

// Host configuration uses the native fixture's explicit open status. It
// changes neither the imported source policy nor any stored ontology record.
func publicIDWriter(store driver.Store, lease driver.ActiveLease, when time.Time) (*pipeline.Writer, error) {
	document := ontology.DefaultDocument()
	document.Statuses["open"] = ontology.Status{Name: "Open", Category: ontology.Active}
	workflow := document.Workflows["issue"]
	workflow.Statuses = append(workflow.Statuses, "open")
	workflow.Initial = "open"
	document.Workflows["issue"] = workflow
	task := document.Types["issue"]
	task.Name = "Task"
	task.Fields["owner"] = ontology.Field{Type: "string"}
	document.Types["task"] = task
	compiled, problems := ontology.Compile(document, nil, nil)
	if len(problems) != 0 {
		return nil, fmt.Errorf("native continuation host ontology: %v", problems)
	}
	host := service.NewOntology(compiled.Effective)
	// Ordinary create/comment events use the pipeline's real ordered plan.
	// This adapter fails closed if a future status-changing operation needs a
	// durable ontology source; this fixture never installs or patches one.
	audit := service.NewOntologyAudit(nil)
	return pipeline.New(store, lease,
		pipeline.WithClock(func() time.Time { return when }),
		pipeline.WithOntology(host), pipeline.WithOntologyAudit(audit),
		pipeline.WithIDGenerator(func() (string, error) { return "", fmt.Errorf("native allocator bypassed") }),
	), nil
}

type publicIDConsumer struct {
	request      publicIDRequest
	initial      []driver.Record
	beforeReopen publicIDObservation
	wrote        bool
	asserted     bool
}

func (c *publicIDConsumer) write(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
	if c.wrote || publicIDProfile(c.request, profile) != nil || report.Archive == nil || report.Archive.SHA256() != c.request.CaptureSHA256 {
		return fmt.Errorf("write phase repeated or source/profile differs")
	}
	var err error
	c.initial, err = publicIDCensus(ctx, store)
	if err != nil {
		return err
	}
	output := os.Getenv("PLUG_PUBLIC_PROOF_OUTPUT")
	publicProofJSON(t, filepath.Join(output, "id-initial-records.json"), c.initial)
	for index, operation := range c.request.Operations {
		publicProofGuard(t)
		if err := publicIDValidateOperation(operation, index); err != nil {
			return err
		}
		if err := publicIDAllocator(ctx, store, operation.Project, operation.Before); err != nil {
			return fmt.Errorf("operation %d before allocation: %w", index, err)
		}
		op := model.Op{}
		if operation.Kind == "comment" {
			op = model.Op{Kind: model.OpComment, Comment: &model.CommentOp{Target: model.RefByID(operation.Target, model.Natural64(0)), Body: operation.Text}}
		} else {
			create := &model.CreateOp{Type: "task", Project: operation.Project, Fields: operation.Fields}
			if operation.Kind == "child" {
				parent := model.RefByID(operation.Parent, model.Natural64(0))
				create.Parent = &parent
			}
			op = model.Op{Kind: model.OpCreate, Create: create}
		}
		writer, err := publicIDWriter(store, lease, operation.Clock)
		if err != nil {
			return err
		}
		meta := publicIDMeta(c.request.Case, index, operation.Project)
		changes := model.ChangeSet{Ops: []model.Op{op}}
		window := publicIDWindow{Started: time.Now().UTC()}
		commit, err := writer.Apply(ctx, operation.Actor, changes, meta)
		window.Ended = time.Now().UTC()
		publicProofJSON(t, filepath.Join(output, fmt.Sprintf("id-operation-%02d.json", index)), map[string]any{"native": operation, "plug_request": changes, "plug_commit": commit, "plug_error": fmt.Sprint(err), "plug_window": window})
		if err != nil {
			return fmt.Errorf("actual write %d: %w", index, err)
		}
		if len(commit.Results) != 1 || operation.Kind != "comment" && commit.Results[0].Ref.ID != operation.ExpectedID {
			return fmt.Errorf("operation %d actual allocated ID differs", index)
		}
		if err := publicIDAllocator(ctx, store, operation.Project, operation.After); err != nil {
			return fmt.Errorf("operation %d after allocation: %w", index, err)
		}
		beforeReplay, err := publicIDCensus(ctx, store)
		if err != nil {
			return err
		}
		fresh, err := publicIDWriter(store, lease, operation.Clock)
		if err != nil {
			return err
		}
		replayed, err := fresh.ApplyWithOutcome(ctx, operation.Actor, changes, meta)
		afterReplay, readErr := publicIDCensus(ctx, store)
		if err != nil || readErr != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, commit) || !reflect.DeepEqual(beforeReplay, afterReplay) {
			return fmt.Errorf("operation %d actual replay changed identity/state: %v %v", index, err, readErr)
		}
		if err := publicIDPreserved(c.initial, afterReplay); err != nil {
			return err
		}
		if _, err := publicIDEvent(ctx, store, operation, meta); err != nil {
			return err
		}
	}
	c.beforeReopen, err = c.observe(t, ctx, store)
	if err != nil {
		return err
	}
	publicProofJSON(t, filepath.Join(output, "id-before-postwrite-reopen.json"), c.beforeReopen)
	c.wrote = true
	return nil
}

func publicIDMeta(caseName string, index int, project string) model.WriteMeta {
	return model.WriteMeta{Method: "tx.apply", Project: project, IdempotencyKey: hashBytes([]byte(fmt.Sprintf("%s/%d", caseName, index)))}
}
