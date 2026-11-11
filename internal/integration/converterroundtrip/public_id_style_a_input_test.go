package converterroundtrip

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func publicIDValidateOperation(operation publicIDOperation, index int) error {
	order := []string{"root", "child", "comment", "comment", "child"}
	start := operation.Window.Started
	// The pinned native comment column/response has whole-second precision.
	// Keep its original stamp, with only the observation window at that precision.
	if operation.Kind == "comment" {
		start = start.Truncate(time.Second)
	}
	if index < 0 || index >= len(order) || operation.Kind != order[index] || operation.Project == "" || operation.Clock.IsZero() || operation.Window.Started.IsZero() || operation.Window.Ended.Before(operation.Window.Started) || operation.Clock.Before(start) || operation.Clock.After(operation.Window.Ended) {
		return fmt.Errorf("operation %d native phase/clock/census differs", index)
	}
	actor, err := driver.PrincipalKey(operation.Actor)
	if err != nil || actor != operation.NativeActor {
		return fmt.Errorf("native request actor differs")
	}
	if operation.Before.Validate() != nil || operation.After.Validate() != nil || operation.Before.Prefix != operation.Project || operation.After.Prefix != operation.Project {
		return fmt.Errorf("native preallocation policy/counter declaration invalid")
	}
	var native map[string]any
	if err := publicIDDecode(operation.NativeResponse, &native); err != nil {
		return err
	}
	when, err := time.Parse(time.RFC3339Nano, fmt.Sprint(native["created_at"]))
	if err != nil || !when.Equal(operation.Clock) {
		return fmt.Errorf("native original response clock differs from operation WithClock")
	}
	args := []string{"--actor", actor}
	if operation.Kind == "comment" {
		args = append(args, "comments", "add", operation.Target, operation.Text, "--author", actor, "--json")
		if len(native) != 5 || native["issue_id"] != operation.Target || native["text"] != operation.Text || native["author"] != actor || native["id"] == nil || operation.ExpectedID != "" || operation.Parent != "" || len(operation.Fields) != 0 || operation.Text == "" || operation.Target == "" || len(operation.NativeCreateJournal) != 0 {
			return fmt.Errorf("native comment tuple/field census differs")
		}
	} else {
		title, titleOK := operation.Fields["title"].(string)
		description, descriptionOK := operation.Fields["description"].(string)
		priority, priorityOK := operation.Fields["priority"].(json.Number)
		owner, ownerOK := operation.Fields["owner"].(string)
		if !titleOK || title == "" || !descriptionOK || !priorityOK || priority != "2" || !ownerOK || owner != "public@example.invalid" || len(operation.Fields) != 4 || operation.ExpectedID == "" || operation.DerivedID != operation.ExpectedID {
			return fmt.Errorf("native create original field tuple/independent ID derivation differs")
		}
		args = append(args, "create", "--title", title, "--description", description, "--priority", string(priority), "--type", "task", "--status", "open")
		if operation.Kind == "child" {
			if operation.Parent == "" {
				return fmt.Errorf("native child parent missing")
			}
			args = append(args, "--parent", operation.Parent)
		} else if operation.Parent != "" {
			return fmt.Errorf("native root acquired parent")
		}
		args = append(args, "--json")
		if native["id"] != operation.ExpectedID || native["title"] != title || native["description"] != description || native["priority"] != priority || native["owner"] != owner || native["created_by"] != actor || native["status"] != "open" || native["issue_type"] != "task" {
			return fmt.Errorf("native original create response differs from request tuple")
		}
		if _, err := publicIDNativeCreateClock(operation); err != nil {
			return err
		}
		if err := publicIDNativeCreateFields(native); err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(args, operation.NativeRequest) {
		return fmt.Errorf("native original argv differs from Plug operation tuple")
	}
	return nil
}

// This finite adapter checks all source response fields. An additional or
// non-default source field is a named comparison failure, never dropped by a
// broad normalization. The native task type is also the live host task type.
func publicIDNativeCreateFields(native map[string]any) error {
	represented := map[string]bool{}
	for _, name := range []string{"id", "title", "description", "status", "priority", "issue_type", "owner", "created_at", "created_by", "updated_at", "labels"} {
		represented[name] = true
	}
	profile, err := publicIDFieldsProfile()
	if err != nil {
		return err
	}
	zero := map[string]bool{}
	for _, name := range profile.Zero {
		zero[name] = true
	}
	if _, err := publicIDNativeLabels(native); err != nil {
		return err
	}
	for name, value := range native {
		if represented[name] {
			continue
		}
		if name == "storage_class" && value == "versioned" {
			continue
		}
		if !zero[name] {
			return fmt.Errorf("unmapped native continued item field %s", name)
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		switch string(raw) {
		case `""`, "null", "false", "0", "[]", "{}":
		default:
			return fmt.Errorf("non-default native continued item field %s", name)
		}
	}
	return nil
}

type publicIDFieldProfile struct {
	Format       string   `json:"format"`
	SourceCommit string   `json:"source_commit"`
	Zero         []string `json:"zero_create_fields"`
	Tables       []string `json:"native_issue_tables"`
	Journal      string   `json:"native_journal_table"`
	Environment  []string `json:"native_continuation_environment"`
}

func publicIDFieldsProfile() (publicIDFieldProfile, error) {
	var profile publicIDFieldProfile
	raw, err := os.ReadFile(os.Getenv("PLUG_ID_STYLE_A_FIELD_PROFILE"))
	if err != nil || hashBytes(raw) != "ff5514c3f022d2c1ea09843e0eec2c380e69a085e9e9265a52b07fbc0113893b" || publicIDDecode(raw, &profile) != nil || profile.Format != "public-id-style-a-field-profile/1" || profile.SourceCommit != "ef3f1566ccc2dddafc55d86cff44ce4200237eb8" || profile.Journal == "" {
		return profile, fmt.Errorf("immutable native field profile differs")
	}
	return profile, nil
}

func publicIDJournalQuery() (string, error) {
	profile, err := publicIDFieldsProfile()
	if err != nil {
		return "", err
	}
	return "SELECT * FROM `" + profile.Journal + "` WHERE issue_id = ? ORDER BY seq", nil
}

func publicIDNativeLabels(native map[string]any) ([]string, error) {
	value, present := native["labels"]
	if !present {
		return nil, nil
	}
	values, ok := value.([]any)
	if !ok || len(values) == 0 {
		return nil, fmt.Errorf("native inherited labels have an invalid representation")
	}
	labels := make([]string, len(values))
	for index, value := range values {
		label, ok := value.(string)
		if !ok || label == "" || index > 0 && label <= labels[index-1] {
			return nil, fmt.Errorf("native inherited labels are not a distinct ordered set")
		}
		labels[index] = label
	}
	return labels, nil
}

func publicIDExpectedDocument(operation publicIDOperation) pipeline.Document {
	fields := map[string]any{}
	for name, value := range operation.Fields {
		fields[name] = value
	}
	fields["status"] = "open"
	// Native is_template=false is the fixed Plug template field.
	fields["template"] = false
	when := operation.Clock
	document := pipeline.Document{ID: operation.ExpectedID, Type: "task", Project: operation.Project, Fields: fields, CreatedAt: &when}
	var native map[string]any
	if publicIDDecode(operation.NativeResponse, &native) == nil {
		if labels, err := publicIDNativeLabels(native); err == nil && len(labels) > 0 {
			document.Labels = labels
			fields["labels"] = native["labels"]
		}
	}
	if operation.Parent != "" {
		parent := model.RefByID(operation.Parent, model.Natural64(0))
		document.Parent = &parent
	}
	return document
}

func publicIDValidateRequest(request publicIDRequest) error {
	if len(request.Operations) != 5 || len(request.Items) != 3 || len(request.Comments) != 2 {
		return fmt.Errorf("native request operation/item/comment census differs")
	}
	items := map[string]publicIDItem{}
	for _, item := range request.Items {
		if item.Document.ID == "" || items[item.Document.ID].Document.ID != "" || len(item.Document.Comments) != 0 {
			return fmt.Errorf("native expected item census repeats/contains comments")
		}
		items[item.Document.ID] = item
	}
	comments := map[string]int{}
	for _, comment := range request.Comments {
		raw, err := json.Marshal(comment)
		if err != nil {
			return err
		}
		comments[string(raw)]++
	}
	for index, operation := range request.Operations {
		if err := publicIDValidateOperation(operation, index); err != nil {
			return fmt.Errorf("operation %d: %w", index, err)
		}
		if index > 0 && !reflect.DeepEqual(request.Operations[index-1].After, operation.Before) {
			return fmt.Errorf("native allocator chain is discontinuous")
		}
		if operation.Kind != "comment" {
			item, found := items[operation.ExpectedID]
			if !found || !reflect.DeepEqual(publicIDExpectedDocument(operation), item.Document) {
				return fmt.Errorf("native expected document differs from original tuple")
			}
			var a, b any
			if publicIDDecode(operation.NativeResponse, &a) != nil || publicIDDecode(item.Native, &b) != nil || !reflect.DeepEqual(a, b) {
				return fmt.Errorf("native item original response differs")
			}
			delete(items, operation.ExpectedID)
		} else {
			comment := publicIDComment{Item: operation.Target, Body: operation.Text, Author: operation.NativeActor, Time: operation.Clock}
			raw, err := json.Marshal(comment)
			if err != nil {
				return err
			}
			comments[string(raw)]--
		}
	}
	if len(items) != 0 {
		return fmt.Errorf("native expected item leftovers")
	}
	for _, count := range comments {
		if count != 0 {
			return fmt.Errorf("native expected comment multiset leftovers")
		}
	}
	if request.Operations[1].Parent != request.Operations[4].Parent || request.Operations[2].Target != request.Operations[0].ExpectedID || request.Operations[3].Target != request.Operations[0].ExpectedID {
		return fmt.Errorf("native case parent/comment routing differs")
	}
	return nil
}

// Keep the numeric JSON wire explicit; native row IDs are never parsed through
// float64 in either the original tuple or comparison.
func publicIDDecimal(value uint64) json.Number { return json.Number(strconv.FormatUint(value, 10)) }

// The explicit 13:29 ruling applies only to native create-time updated_at.
// Its original value stays bound; no value is substituted into the target.
func publicIDNativeCreateClock(operation publicIDOperation) (time.Duration, error) {
	var native map[string]any
	if err := publicIDDecode(operation.NativeResponse, &native); err != nil {
		return 0, err
	}
	updated, err := time.Parse(time.RFC3339Nano, fmt.Sprint(native["updated_at"]))
	if err != nil || updated.Before(operation.Clock) || updated.After(operation.Window.Ended) {
		return 0, fmt.Errorf("native create-time updated_at precedes created_at or falls outside its operation")
	}
	var journal struct {
		Query     string      `json:"query"`
		Arguments []any       `json:"arguments"`
		Columns   []string    `json:"columns"`
		Types     []string    `json:"database_types"`
		Rows      [][]*string `json:"rows"`
	}
	query, err := publicIDJournalQuery()
	if err != nil || publicIDDecode(operation.NativeCreateJournal, &journal) != nil || journal.Query != query || !reflect.DeepEqual(journal.Arguments, []any{operation.ExpectedID}) || len(journal.Columns) != 8 || len(journal.Types) != 8 {
		return 0, fmt.Errorf("native create write census missing/extra")
	}
	columns := map[string]int{}
	for index, name := range journal.Columns {
		if _, exists := columns[name]; exists || journal.Types[index] == "" {
			return 0, fmt.Errorf("native create journal schema repeats/omits a column")
		}
		columns[name] = index
	}
	for _, name := range []string{"seq", "ts", "op", "issue_id", "actor", "issue_json", "dep_json", "comment_json"} {
		if _, exists := columns[name]; !exists {
			return 0, fmt.Errorf("native create journal schema is incomplete")
		}
	}
	want := []string{"create"}
	labels, err := publicIDNativeLabels(native)
	if err != nil {
		return 0, err
	}
	if operation.Kind == "child" {
		want = append(want, "dep_add")
		for range labels {
			want = append(want, "update")
		}
	} else if len(labels) != 0 {
		return 0, fmt.Errorf("native root gained unrequested labels")
	}
	if len(journal.Rows) != len(want) {
		return 0, fmt.Errorf("native create write census missing/extra")
	}
	var last uint64
	for index, row := range journal.Rows {
		if len(row) != len(columns) {
			return 0, fmt.Errorf("native create journal row width differs")
		}
		cell := func(name string) string {
			if row[columns[name]] == nil {
				return ""
			}
			return *row[columns[name]]
		}
		seq, err := strconv.ParseUint(cell("seq"), 10, 64)
		if err != nil || seq <= last || cell("op") != want[index] || cell("issue_id") != operation.ExpectedID || cell("actor") != operation.NativeActor || cell("ts") == "" || strings.TrimSpace(cell("issue_json")) == "" || cell("comment_json") != "" {
			return 0, fmt.Errorf("native item had a missing/misattributed/additional create-time write")
		}
		last = seq
		if want[index] == "dep_add" {
			var edge struct {
				Kind     string `json:"kind"`
				Target   string `json:"target"`
				Metadata string `json:"metadata"`
			}
			if publicIDDecode([]byte(cell("dep_json")), &edge) != nil || edge.Kind != "parent-child" || edge.Target != operation.Parent || edge.Metadata != "" {
				return 0, fmt.Errorf("native create journal parent edge differs")
			}
		} else if cell("dep_json") != "" {
			return 0, fmt.Errorf("native create journal gained an unexpected dependency")
		}
	}
	// In the bound native source, create stamps CreatedAt before minting,
	// normalizes UpdatedAt before INSERT and journals that INSERT first.
	// Parent/label records follow that insert in the same transaction. This
	// complete census excludes a write between those two original clock reads;
	// second-granular journal ts values are not used as higher precision clocks.
	return updated.Sub(operation.Clock), nil
}
