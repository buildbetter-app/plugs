package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func authoredMixedMemoryFixture(t *testing.T) []byte {
	return authoredMixedMemoryFixtureFrom(t, admittedFixtureBytes(t))
}

func authoredMixedMemoryFixtureFrom(t *testing.T, admitted []byte) []byte {
	t.Helper()
	return rewriteData(t, func(m map[string][]byte) {
		reader := tar.NewReader(bytes.NewReader(admitted))
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			m[header.Name], err = io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
		}
		lines := bytes.TrimSuffix(m["records/memory.ndjson"], []byte{'\n'})
		rows := []map[string]any{
			{"kind": "memory", "id": "secret", "project": "", "body": map[string]any{"key": "secret", "value": map[string]any{"$secret": "config/kv.memory.secret"}, "classification": "secret"}, "source": map[string]any{"table": "config", "fields": map[string]any{"key": "kv.memory.secret", "value": map[string]any{"$secret": "config/kv.memory.secret"}}}},
			{"kind": "memory", "id": "object", "project": "", "body": map[string]any{"key": "object", "value": map[string]any{"x": 1}, "classification": "public"}, "source": map[string]any{"table": "config", "fields": map[string]any{"key": "kv.memory.object", "value": map[string]any{"x": 1}}}},
			{"kind": "memory", "id": "unicode", "project": "", "body": map[string]any{"key": "unicode", "value": "  π\n終  ", "classification": "public"}, "source": map[string]any{"table": "config", "fields": map[string]any{"key": "kv.memory.unicode", "value": "  π\n終  "}}},
		}
		for _, control := range []struct {
			id    string
			value any
		}{
			{"number", json.Number("9007199254740993")},
			{"null", nil},
			{"array", []any{json.Number("9007199254740993"), nil, " π "}},
			{"boolean", false},
		} {
			rows = append(rows, map[string]any{"kind": "memory", "id": control.id, "project": "", "body": map[string]any{"key": control.id, "classification": "public", "value": control.value}, "source": map[string]any{"table": "config", "fields": map[string]any{"key": "kv.memory." + control.id, "value": control.value}}})
		}
		for _, row := range rows {
			b, _ := json.Marshal(row)
			lines = append(lines, '\n')
			lines = append(lines, b...)
		}
		ordered := bytes.Split(lines, []byte{'\n'})
		sort.Slice(ordered, func(i, j int) bool {
			var a, b map[string]any
			decode(t, ordered[i], &a)
			decode(t, ordered[j], &b)
			return a["id"].(string) < b["id"].(string)
		})
		m["records/memory.ndjson"] = append(bytes.Join(ordered, []byte{'\n'}), '\n')
		changeManifest(t, m, func(manifest map[string]any) {
			for _, entry := range manifest["kind_registry"].([]any) {
				kind := entry.(map[string]any)
				if kind["name"] == "memory" {
					kind["count"] = 1 + len(rows)
				}
			}
			for _, entry := range manifest["sources"].([]any) {
				source := entry.(map[string]any)
				if source["family"] == "config" {
					source["rows"] = 2 + len(rows)
				}
			}
		})
	})
}

// Authored, intentionally unrun source control for the imported public-memory
// contract across every real store implementation.
func TestImportedBeadsMemoriesPublicReadsAllDrivers(t *testing.T) {
	data := authoredMixedMemoryFixture(t)
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			anonymous := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			profile, _ := admittedImportProfile(t, data)
			store, lease, reopen := openIdentityTarget(t, engine)
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			check := func() {
				before := reverseState(t, store)
				historyBefore := importedMemoryHistory(t, store)
				reader, err := store.Read(ctx, driver.View{Incarnation: before.Meta.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				retained, err := reader.Blob(ctx, driver.Hash(archive.SHA256()))
				if err != nil || !bytes.Equal(retained, data) {
					reader.Close()
					t.Fatalf("retained archive changed: %v", err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				ontology := service.NewOntology(nil)
				writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
				svc := service.New(store, lease, writer, service.WithOntology(ontology))
				id := model.RefByID("source/memory/fixture", model.Natural64(0))
				got, err := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: id})
				if err != nil {
					t.Fatal(err)
				}
				if got.ID != id.ID || got.Key != "fixture" || got.Content != "A complete synthetic memory" {
					t.Fatalf("unexpected imported memory: %+v", got)
				}
				if !sameReverseJSON(got.Metadata, map[string]any{"plug.restore.source_id": "fixture", "plug.restore.source_table": "config", "plug.restore.source": map[string]any{"key": "kv.memory.fixture", "value": "A complete synthetic memory"}, "plug.restore.archive_sha256": archive.SHA256()}) {
					t.Fatal("public memory provenance changed")
				}
				page, err := svc.MemoryList(ctx, anonymous, service.MemoryListRequest{Limit: 100})
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, item := range page.Items {
					if item.ID == id.ID {
						found = true
					}
				}
				if !found {
					t.Fatalf("imported memory %s missing from list", id.ID)
				}
				unicodeID := model.RefByID("source/memory/unicode", model.Natural64(0))
				unicode, err := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: unicodeID})
				if err != nil || unicode.Key != "unicode" || unicode.Content != "  π\n終  " {
					t.Fatalf("unicode memory: %+v %v", unicode, err)
				}
				if unicode.ID != unicodeID.ID || !sameReverseJSON(unicode.Metadata, map[string]any{"plug.restore.source_id": "unicode", "plug.restore.source_table": "config", "plug.restore.source": map[string]any{"key": "kv.memory.unicode", "value": "  π\n終  "}, "plug.restore.archive_sha256": archive.SHA256()}) {
					t.Fatal("unicode identity/provenance changed")
				}
				for _, denied := range []string{"secret", "object", "number", "null", "array", "boolean"} {
					_, err := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: model.RefByID("source/memory/"+denied, model.Natural64(0))})
					var typed *model.Error
					if !errors.As(err, &typed) || typed.Data.Code != "not_found" {
						t.Fatalf("ineligible %s: %v", denied, err)
					}
				}
				if len(page.Items) != 2 {
					t.Fatalf("eligible list count: %d", len(page.Items))
				}
				if !reflect.DeepEqual(page.Items, []service.Memory{got, unicode}) {
					t.Fatal("list differs from exact get values or ordering")
				}
				after := reverseState(t, store)
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("read mutated restored state")
				}
				for _, source := range archive.Records("memory") {
					if _, eligible := source.Body["value"].(string); source.Body["classification"] == "public" && eligible {
						continue
					}
					want := cloneRow(source.Body)
					want["source_id"], want["source_table"], want["source"] = source.ID, source.Source.Table, source.Source.Fields
					found := false
					for _, record := range after.Records {
						if record.Ref.ID != "source/memory/"+source.ID {
							continue
						}
						found = true
						var body map[string]any
						decode(t, record.Body, &body)
						encoded, err := json.Marshal(want)
						if err != nil {
							t.Fatal(err)
						}
						if record.Kind != "memory" || !record.Ref.Branch.IsZero() || !bytes.Equal(record.Body, encoded) || !sameReverseJSON(want, body) {
							t.Fatalf("ineligible raw row changed: %s", source.ID)
						}
					}
					if !found {
						t.Fatalf("ineligible raw row missing: %s", source.ID)
					}
				}
				first, err := svc.MemoryList(ctx, anonymous, service.MemoryListRequest{Limit: 1})
				if err != nil || len(first.Items) != 1 || first.Next == nil {
					t.Fatalf("first memory page: %+v %v", first, err)
				}
				second, err := svc.MemoryList(ctx, anonymous, service.MemoryListRequest{Limit: 1, Position: first.Next})
				if err != nil || len(second.Items) != 1 || second.Next != nil || first.Items[0].ID == second.Items[0].ID {
					t.Fatalf("second memory page: %+v %v", second, err)
				}
				if !reflect.DeepEqual(first.Items, page.Items[:1]) || !reflect.DeepEqual(second.Items, page.Items[1:]) {
					t.Fatal("pagination omitted/reordered eligible rows")
				}
				reversed, err := ReverseRestored(ctx, store)
				if err != nil {
					t.Fatal(err)
				}
				original, err := reverseconverter.ConvertArchive(data)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(reversed.Result.JSONL, original.JSONL) || !reflect.DeepEqual(reversed.Result.Projection, original.Projection) {
					t.Fatal("reverse changed the complete admitted source projection")
				}
				// Compare actual JSON values to the admitted source, independently
				// of the converter's projection types (objects must not become text).
				memoryValues := map[string]any{}
				decoder := json.NewDecoder(bytes.NewReader(reversed.Result.JSONL))
				decoder.UseNumber()
				for {
					var row map[string]any
					err := decoder.Decode(&row)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if row["_type"] == "memory" {
						memoryValues[row["key"].(string)] = row["value"]
					}
				}
				for _, source := range archive.Records("memory") {
					if !sameReverseJSON(memoryValues[source.ID], source.Body["value"]) {
						t.Fatalf("reverse changed memory JSON type/value: %s", source.ID)
					}
				}
				if !reflect.DeepEqual(historyBefore, importedMemoryHistory(t, store)) {
					t.Fatal("public reads/reverse changed history")
				}
			}
			check()
			beforeReopen := reverseState(t, store)
			historyBeforeReopen := importedMemoryHistory(t, store)
			store, lease = reopen()
			if !reflect.DeepEqual(beforeReopen, reverseState(t, store)) {
				t.Fatal("reopen changed imported records")
			}
			if !reflect.DeepEqual(historyBeforeReopen, importedMemoryHistory(t, store)) {
				t.Fatal("reopen changed imported history")
			}
			check()
			// Every canonical field and the complete provenance map participate
			// in the inverse check; a plausible imported prefix grants no bypass.
			state := reverseState(t, store)
			var originalRecord driver.Record
			for _, record := range state.Records {
				if record.Ref.ID == "source/memory/fixture" {
					originalRecord = record
				}
			}
			for _, field := range []string{"id", "key", "content", "metadata-source", "metadata-extra", "schema-extra"} {
				var document map[string]any
				decode(t, originalRecord.Body, &document)
				switch field {
				case "metadata-source":
					document["metadata"].(map[string]any)["plug.restore.source_id"] = "wrong"
				case "metadata-extra":
					document["metadata"].(map[string]any)["extra"] = "wrong"
				case "schema-extra":
					document["extra"] = "wrong"
				default:
					document[field] = "wrong"
				}
				mutated := originalRecord
				mutated.Body = encode(t, document)
				putImportedMemoryControl(t, store, lease, mutated)
				_, err := ReverseRestored(ctx, store)
				var refusal ReverseRefusal
				if !errors.As(err, &refusal) || refusal.Reason != "changed_source_mirror" {
					t.Fatalf("tampered %s reverse: %v", field, err)
				}
				putImportedMemoryControl(t, store, lease, originalRecord)
				if _, err := ReverseRestored(ctx, store); err != nil {
					t.Fatalf("restored %s mirror: %v", field, err)
				}
			}
			for _, name := range []string{"secret", "object", "null"} {
				var original driver.Record
				for _, record := range state.Records {
					if record.Ref.ID == "source/memory/"+name {
						original = record
					}
				}
				var body map[string]any
				decode(t, original.Body, &body)
				body["value"] = "changed"
				mutated := original
				mutated.Body = encode(t, body)
				putImportedMemoryControl(t, store, lease, mutated)
				_, err := ReverseRestored(ctx, store)
				var refusal ReverseRefusal
				if !errors.As(err, &refusal) || refusal.Reason != "changed_source_mirror" {
					t.Fatalf("tampered raw %s: %v", name, err)
				}
				putImportedMemoryControl(t, store, lease, original)
				if _, err := ReverseRestored(ctx, store); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestJointImportedMemoriesPublicReadReverseAndConservationAllDrivers(t *testing.T) {
	data := authoredMixedMemoryFixtureFrom(t, sourceReverseFixture(t, false))
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	// Derive the expected reference from the documented physical tuple,
	// independently of restore's mirror-ID helper.
	expectedID := func(source dumpformat.Record) string {
		key, err := json.Marshal([]string{"memory", source.Project, "config", source.ID})
		if err != nil {
			t.Fatal(err)
		}
		return "source/memory/" + digestBytes(key)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			profile := sourceReverseProfile(t, data, store, lease, false)
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile)
			if err != nil {
				t.Fatal(err)
			}
			anonymous := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			serviceForStore := func() *service.Service {
				ontology := service.NewOntology(nil)
				return service.New(store, lease, pipeline.New(store, lease, pipeline.WithOntology(ontology)), service.WithOntology(ontology))
			}
			check := func() {
				t.Helper()
				before, history := reverseState(t, store), importedMemoryHistory(t, store)
				capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
				if err != nil {
					t.Fatal(err)
				}
				namespace := report.SourcePlanes.Providers[0].Provider.Namespace
				checkpoint, err := capability.LoadCheckpoint(ctx, namespace)
				if err != nil {
					t.Fatal(err)
				}
				svc := serviceForStore()
				var eligible []service.Memory
				for _, source := range archive.Records("memory") {
					id := model.RefByID(expectedID(source), model.Natural64(0))
					got, err := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: id})
					content, text := source.Body["value"].(string)
					if source.Body["classification"] != "public" || !text {
						var typed *model.Error
						if !errors.As(err, &typed) || typed.Data.Code != "not_found" {
							t.Fatalf("ineligible joint memory %s: %v", source.ID, err)
						}
						continue
					}
					wantMetadata := map[string]any{"plug.restore.source_id": source.ID, "plug.restore.source_table": "config", "plug.restore.source": source.Source.Fields, "plug.restore.source_project": source.Project, "plug.restore.archive_sha256": archive.SHA256()}
					if err != nil || got.ID != id.ID || got.Key != source.Body["key"] || got.Content != content || !sameReverseJSON(got.Metadata, wantMetadata) {
						t.Fatalf("joint public memory identity/value/provenance %s: %+v %v", source.ID, got, err)
					}
					eligible = append(eligible, got)
				}
				if len(eligible) != 2 {
					t.Fatalf("fixture eligibility changed: %d", len(eligible))
				}
				page, err := svc.MemoryList(ctx, anonymous, service.MemoryListRequest{Limit: 100})
				if err != nil || !reflect.DeepEqual(page.Items, eligible) || page.Next != nil {
					t.Fatalf("joint public list differs from exact Get values: %+v %v", page, err)
				}
				for _, all := range []bool{false, true} {
					reversed, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: all})
					if err != nil {
						t.Fatalf("joint public-memory reverse all=%v: %v", all, err)
					}
					// Compare the real JSONL values to source rows; do not use Projection.
					values := map[string]any{}
					decoder := json.NewDecoder(bytes.NewReader(reversed.Result.JSONL))
					decoder.UseNumber()
					for {
						var row map[string]any
						if err := decoder.Decode(&row); err == io.EOF {
							break
						} else if err != nil {
							t.Fatal(err)
						}
						if row["_type"] == "memory" {
							key := row["key"].(string)
							if _, duplicate := values[key]; duplicate {
								t.Fatal("duplicate reversed memory", key)
							}
							values[key] = row["value"]
						}
					}
					// Config rows have a separate, documented Beads memory namespace.
					// Assert their exact keys/values as well as every source memory.
					configValues := map[string]any{
						"plug.config/github.token": `{"$secret":"config/github.token"}`,
						"plug.config/project_id":   "synthetic-project",
					}
					if len(values) != len(archive.Records("memory"))+len(configValues) {
						t.Fatalf("reverse memory census: %d", len(values))
					}
					for key, want := range configValues {
						if got, present := values[key]; !present || !sameReverseJSON(got, want) {
							t.Fatal("reverse changed config reference", key)
						}
					}
					for _, source := range archive.Records("memory") {
						if got, present := values[source.ID]; !present || !sameReverseJSON(got, source.Body["value"]) {
							t.Fatal("reverse changed exact memory JSON type/value", source.ID)
						}
					}
				}
				later, err := capability.LoadCheckpoint(ctx, namespace)
				if err != nil || !reflect.DeepEqual(checkpoint, later) || !reflect.DeepEqual(before, reverseState(t, store)) || !reflect.DeepEqual(history, importedMemoryHistory(t, store)) {
					t.Fatalf("joint memory reads/reverse mutated core/history/checkpoint: %v", err)
				}
			}
			check()
			beforeReopen, historyBefore := reverseState(t, store), importedMemoryHistory(t, store)
			store, lease = reopen()
			if !reflect.DeepEqual(beforeReopen, reverseState(t, store)) || !reflect.DeepEqual(historyBefore, importedMemoryHistory(t, store)) {
				t.Fatal("reopen changed joint memory state/history")
			}
			check()
			original := map[string]driver.Record{}
			for _, row := range reverseState(t, store).Records {
				for _, source := range archive.Records("memory") {
					if row.Ref.ID == expectedID(source) {
						original[source.ID] = row
					}
				}
			}
			for _, control := range []string{"public-project", "public-source", "public-extra", "raw-project", "raw-missing-project", "raw-extra", "raw-missing-metadata", "raw-missing-source-id", "raw-classification-shape"} {
				t.Run(control, func(t *testing.T) {
					id := "fixture"
					if control[:3] == "raw" {
						id = "number"
					}
					row := original[id]
					var body map[string]any
					decode(t, row.Body, &body)
					switch control {
					case "public-project":
						body["source_project"] = "wrong"
					case "public-source":
						body["source"].(map[string]any)["value"] = "wrong"
					case "public-extra", "raw-extra":
						body["extra"] = true
					case "raw-project":
						body["source_project"] = "wrong"
					case "raw-missing-project":
						delete(body, "source_project")
					case "raw-missing-metadata":
						delete(body, "metadata")
					case "raw-missing-source-id":
						delete(body, "source_id")
					case "raw-classification-shape":
						body["classification"] = map[string]any{"unexpected": true}
						body["metadata"].(map[string]any)["beads.classification"] = map[string]any{"unexpected": true}
					}
					mutated := row
					mutated.Body = encode(t, body)
					putImportedMemoryControl(t, store, lease, mutated)
					before, history := reverseState(t, store), importedMemoryHistory(t, store)
					if id == "number" {
						svc := serviceForStore()
						_, getErr := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: row.Ref})
						_, listErr := svc.MemoryList(ctx, anonymous, service.MemoryListRequest{})
						for _, err := range []error{getErr, listErr} {
							var typed *model.Error
							if !errors.As(err, &typed) || typed.Data.Code != "internal_error" {
								t.Fatalf("malformed joint row hidden: %v", err)
							}
						}
					}
					result, err := ReverseRestored(ctx, store)
					var refusal ReverseRefusal
					if !errors.As(err, &refusal) || len(result.Result.JSONL) != 0 || !reflect.DeepEqual(before, reverseState(t, store)) || !reflect.DeepEqual(history, importedMemoryHistory(t, store)) {
						t.Fatalf("tampered joint memory accepted or mutated state: %v", err)
					}
					putImportedMemoryControl(t, store, lease, row)
				})
			}
		})
	}
}

// This exercises the repaired path through an actual converter import, a
// native dump and native restore into a fresh store. SQL stores are physically
// reopened twice. Memory keeps its documented process-lifetime store and
// refreshes its lease. Public reads must retain trusted historical data while
// hidden source rows remain denied.
func TestNativeRestoredImportedMemoryPublicReadsAfterTwoReopensAllDrivers(t *testing.T) {
	data := nativeSafeImportedMemoryFixture(t)
	sourceArchive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	anonymous := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			source, sourceLease, _ := openIdentityTarget(t, engine)
			profile := sourceReverseProfile(t, data, source, sourceLease, false)
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), source, sourceLease, profile); err != nil {
				t.Fatal(err)
			}
			nativeBytes, manifest, err := restore.Dump(ctx, source, "head")
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Scope != "plug-native-store" || manifest.History != "head" {
				t.Fatalf("converter import did not produce a native head archive: %+v", manifest)
			}
			nativeArchive, err := dumpformat.Read(bytes.NewReader(nativeBytes))
			if err != nil {
				t.Fatal(err)
			}
			root, ok := nativeArchive.NativeMember("blobs/" + sourceArchive.SHA256())
			if !ok || !bytes.Equal(root, data) {
				t.Fatalf("native dump omitted authenticated converter source root: present=%t bytes=%d want=%d", ok, len(root), len(data))
			}

			target, targetLease, reopen := openIdentityTarget(t, engine)
			manager := restore.NewManager(target, targetLease, restore.PresenceVerifierFunc(func(context.Context, string, string, restore.Presence) error {
				return nil
			}))
			manifestJSON, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			var manifestMap map[string]any
			if err := json.Unmarshal(manifestJSON, &manifestMap); err != nil {
				t.Fatal(err)
			}
			presence := restore.Presence{"mode": "presence_check", "challenge": "p23-native-memory", "proof": "p23-native-memory"}
			principal := anonymous
			status, err := manager.BeginAs(ctx, principal, manifestMap, "head", presence)
			if err != nil {
				t.Fatal(err)
			}
			for page, offset := uint64(0), 0; offset < len(nativeBytes); page++ {
				end := offset + 1<<20
				if end > len(nativeBytes) {
					end = len(nativeBytes)
				}
				chunk := nativeBytes[offset:end]
				sum := sha256.Sum256(chunk)
				status, err = manager.ChunkAs(ctx, principal, status.RestoreID, page, chunk, hex.EncodeToString(sum[:]), presence)
				if err != nil {
					t.Fatal(err)
				}
				offset = end
			}
			manifestSum := sha256.Sum256(manifestJSON)
			if _, err := manager.Finish(ctx, status.RestoreID, hex.EncodeToString(manifestSum[:]), principal, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: strings.Repeat("p", 32)}, presence); err != nil {
				var typed *model.Error
				if errors.As(err, &typed) {
					t.Fatalf("native restore finish: code=%s hint=%s details=%v", typed.Data.Code, typed.Data.Hint, typed.Data.Details)
				}
				t.Fatal(err)
			}
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			var fixtureID, secretID string
			for _, row := range archive.Records("memory") {
				switch row.ID {
				case "fixture":
					fixtureID = nativeMemoryTargetID(row)
				case "secret":
					secretID = nativeMemoryTargetID(row)
				}
			}
			if fixtureID == "" || secretID == "" {
				t.Fatal("native-safe fixture omitted fixture or hidden memory")
			}

			check := func() {
				t.Helper()
				ontology := service.NewOntology(nil)
				svc := service.New(target, targetLease, pipeline.New(target, targetLease, pipeline.WithOntology(ontology)), service.WithOntology(ontology))
				id := model.RefByID(fixtureID, model.Natural64(0))
				got, err := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: id})
				if err != nil || got.ID != id.ID || got.Key != "fixture" || got.Content != "A complete synthetic memory" {
					t.Fatalf("native-restored public memory read: %+v %v", got, err)
				}
				page, err := svc.MemoryList(ctx, anonymous, service.MemoryListRequest{Limit: 100})
				if err != nil || len(page.Items) != 2 || !containsMemoryID(page.Items, id.ID) {
					t.Fatalf("native-restored public memory list: %+v %v", page, err)
				}
				if _, err := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: model.RefByID(secretID, model.Natural64(0))}); !isNotFoundMemory(err) {
					t.Fatalf("hidden native-restored memory became visible: %v", err)
				}
			}
			check()
			for reopenCount := 0; reopenCount < 2; reopenCount++ {
				target, targetLease = reopen()
				check()
			}
			t.Logf("P23_NATIVE_MEMORY_READBACK engine=%s fixture=%s hidden=%s phases=initial,reopen-1,reopen-2 public_get_list=pass hidden_not_found=pass strict_mutation=not_run", engine, fixtureID, secretID)
		})
	}
}

func TestNativeRestoredConverterEvidenceReverseAllDrivers(t *testing.T) {
	data := admittedFixtureBytes(t)
	sourceArchive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			source, sourceLease, _ := openIdentityTarget(t, engine)
			profile, _ := admittedImportProfile(t, data)
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), source, sourceLease, profile); err != nil {
				t.Fatal(err)
			}
			nativeBytes, manifest, err := restore.Dump(ctx, source, "head")
			if err != nil {
				t.Fatal(err)
			}
			nativeArchive, err := dumpformat.Read(bytes.NewReader(nativeBytes))
			if err != nil {
				t.Fatal(err)
			}
			root, ok := nativeArchive.NativeMember("blobs/" + sourceArchive.SHA256())
			if !ok || !bytes.Equal(root, data) {
				t.Fatalf("native dump omitted source root: present=%t bytes=%d want=%d", ok, len(root), len(data))
			}
			target, targetLease, reopen := openIdentityTarget(t, engine)
			restoreNativeHeadForTest(t, ctx, target, targetLease, nativeBytes, manifest)
			for i := 0; i < 2; i++ {
				target, targetLease = reopen()
			}
			reversed, err := ReverseRestored(ctx, target)
			if err != nil {
				t.Fatalf("reverse after native restore and two reopens: %v", err)
			}
			original, err := reverseconverter.ConvertArchive(data)
			if err != nil || !bytes.Equal(reversed.Result.JSONL, original.JSONL) || !reflect.DeepEqual(reversed.Result.Projection, original.Projection) {
				t.Fatalf("native-restored reverse differs from source: %v", err)
			}
			tampered := tamperNativeEvidenceRootForTest(t, nativeBytes)
			tamperedArchive, err := dumpformat.Read(bytes.NewReader(tampered))
			if err != nil {
				t.Fatal(err)
			}
			badTarget, badLease, _ := openIdentityTarget(t, engine)
			restoreNativeHeadForTest(t, ctx, badTarget, badLease, tampered, tamperedArchive.Manifest())
			_, err = ReverseRestored(ctx, badTarget)
			var refusal ReverseRefusal
			if !errors.As(err, &refusal) || refusal.Reason != "invalid_source_evidence" {
				t.Fatalf("tampered native evidence was accepted: %v", err)
			}
		})
	}
}

func TestNativeDumpConverterEvidenceSizeImpactTwoProjectFixture(t *testing.T) {
	data := sourceReverseFixture(t, false)
	sourceArchive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceArchive.Manifest().Projects) != 2 {
		t.Fatalf("size fixture must contain two projects: %d", len(sourceArchive.Manifest().Projects))
	}
	ctx := context.Background()
	source, lease, _ := openIdentityTarget(t, "memory")
	profile := sourceReverseProfile(t, data, source, lease, false)
	if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), source, lease, profile); err != nil {
		t.Fatal(err)
	}
	nativeBytes, _, err := restore.Dump(ctx, source, "head")
	if err != nil {
		t.Fatal(err)
	}
	nativeArchive, err := dumpformat.Read(bytes.NewReader(nativeBytes))
	if err != nil {
		t.Fatal(err)
	}
	blobBytes, blobCount := 0, 0
	for path, file := range nativeArchive.Manifest().Files {
		if !strings.HasPrefix(path, "blobs/") {
			continue
		}
		blobCount++
		blobBytes += int(file.Size)
	}
	if blobCount == 0 || blobBytes == 0 {
		t.Fatal("two-project native dump did not retain converter evidence blobs")
	}
	t.Logf("P25_NATIVE_REVERSE_EVIDENCE_SIZE projects=2 native_archive_bytes=%d evidence_blob_count=%d evidence_blob_payload_bytes=%d payload_fraction=%.6f", len(nativeBytes), blobCount, blobBytes, float64(blobBytes)/float64(len(nativeBytes)))
}

func restoreNativeHeadForTest(t *testing.T, ctx context.Context, target driver.Store, lease driver.ActiveLease, nativeBytes []byte, manifest dumpformat.Manifest) {
	t.Helper()
	manager := restore.NewManager(target, lease, restore.PresenceVerifierFunc(func(context.Context, string, string, restore.Presence) error { return nil }))
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var manifestMap map[string]any
	if err := json.Unmarshal(manifestJSON, &manifestMap); err != nil {
		t.Fatal(err)
	}
	presence := restore.Presence{"mode": "presence_check", "challenge": "p25-native", "proof": "p25-native"}
	principal := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	status, err := manager.BeginAs(ctx, principal, manifestMap, "head", presence)
	if err != nil {
		t.Fatal(err)
	}
	for page, offset := uint64(0), 0; offset < len(nativeBytes); page++ {
		end := offset + 1<<20
		if end > len(nativeBytes) {
			end = len(nativeBytes)
		}
		chunk := nativeBytes[offset:end]
		sum := sha256.Sum256(chunk)
		status, err = manager.ChunkAs(ctx, principal, status.RestoreID, page, chunk, hex.EncodeToString(sum[:]), presence)
		if err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	manifestSum := sha256.Sum256(manifestJSON)
	if _, err := manager.Finish(ctx, status.RestoreID, hex.EncodeToString(manifestSum[:]), principal, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: strings.Repeat("p", 32)}, presence); err != nil {
		t.Fatal(err)
	}
}

func tamperNativeEvidenceRootForTest(t *testing.T, input []byte) []byte {
	t.Helper()
	members := map[string][]byte{}
	reader := tar.NewReader(bytes.NewReader(input))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		members[header.Name], err = io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
	}
	var manifest dumpformat.Manifest
	decode(t, members["manifest.json"], &manifest)
	originalLines := bytes.Split(bytes.TrimSpace(members["records/native.ndjson"]), []byte{'\n'})
	var lines [][]byte
	for _, line := range bytes.Split(bytes.TrimSpace(members["records/native.ndjson"]), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		decode(t, line, &record)
		ref, _ := record["ref"].(map[string]any)
		if ref["id"] == "restore/archive" {
			var body map[string]any
			decode(t, encode(t, record["body"]), &body)
			body["archive_sha256"] = strings.Repeat("0", 64)
			record["body"] = body
		}
		lines = append(lines, bytes.TrimSuffix(encode(t, record), []byte{'\n'}))
	}
	members["records/native.ndjson"] = append(bytes.Join(lines, []byte{'\n'}), '\n')
	if len(lines) != manifest.RecordCount {
		t.Fatalf("native tamper line census changed: lines=%d manifest=%d original=%d", len(lines), manifest.RecordCount, len(originalLines))
	}
	for path, data := range members {
		if path == "manifest.json" {
			continue
		}
		manifest.Files[path] = dumpformat.File{SHA256: digestBytes(data), Size: int64(len(data))}
	}
	paths := make([]string, 0, len(members)-1)
	for path := range members {
		if path != "manifest.json" {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		_, _ = io.WriteString(h, path)
		_, _ = h.Write(members[path])
	}
	manifest.NativeDigest = hex.EncodeToString(h.Sum(nil))
	members["manifest.json"] = encode(t, manifest)
	var out bytes.Buffer
	writer := tar.NewWriter(&out)
	sort.Strings(paths)
	paths = append([]string{"manifest.json"}, paths...)
	for _, path := range paths {
		if err := writer.WriteHeader(&tar.Header{Name: path, Mode: 0o600, Size: int64(len(members[path])), ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(members[path]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func nativeSafeImportedMemoryFixture(t *testing.T) []byte {
	t.Helper()
	base := authoredMixedMemoryFixtureFrom(t, sourceReverseFixture(t, false))
	return rewriteTarData(t, base, func(members map[string][]byte) {
		var kept [][]byte
		originalCount := 0
		for _, line := range bytes.Split(bytes.TrimSpace(members["records/memory.ndjson"]), []byte{'\n'}) {
			originalCount++
			var row map[string]any
			decode(t, line, &row)
			id, _ := row["id"].(string)
			if id == "fixture" || id == "unicode" || id == "secret" {
				kept = append(kept, line)
			}
		}
		members["records/memory.ndjson"] = append(bytes.Join(kept, []byte{'\n'}), '\n')
		changeManifest(t, members, func(manifest map[string]any) {
			for _, entry := range manifest["kind_registry"].([]any) {
				kind := entry.(map[string]any)
				if kind["name"] == "memory" {
					kind["count"] = len(kept)
				}
			}
			for _, entry := range manifest["sources"].([]any) {
				source := entry.(map[string]any)
				if source["family"] == "config" {
					rows, err := source["rows"].(json.Number).Int64()
					if err != nil {
						t.Fatal(err)
					}
					source["rows"] = rows - int64(originalCount-len(kept))
				}
			}
		})
	})
}

func nativeMemoryTargetID(source dumpformat.Record) string {
	key, _ := json.Marshal([]string{"memory", source.Project, "config", source.ID})
	return "source/memory/" + digestBytes(key)
}

func containsMemoryID(items []service.Memory, id string) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}

func rewriteTarData(t *testing.T, input []byte, change func(map[string][]byte)) []byte {
	t.Helper()
	members := map[string][]byte{}
	reader := tar.NewReader(bytes.NewReader(input))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		members[header.Name], err = io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
	}
	change(members)
	var manifest map[string]any
	decode(t, members["manifest.json"], &manifest)
	files := manifest["files"].(map[string]any)
	for path, data := range members {
		if path == "manifest.json" {
			continue
		}
		if _, ok := files[path]; ok {
			files[path] = map[string]any{"sha256": digestBytes(data), "size": len(data)}
		}
	}
	members["manifest.json"] = encode(t, manifest)
	var out bytes.Buffer
	writer := tar.NewWriter(&out)
	names := make([]string, 0, len(members)-1)
	for name := range members {
		if name != "manifest.json" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	names = append([]string{"manifest.json"}, names...)
	for _, name := range names {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(members[name]))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(members[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func isNotFoundMemory(err error) bool {
	var typed *model.Error
	return errors.As(err, &typed) && typed.Data.Code == "not_found"
}

func TestImportedMemoryReverseRejectsInvalidSourceShapes(t *testing.T) {
	data := authoredMixedMemoryFixture(t)
	for _, control := range []string{"unknown-classification", "missing-value", "fake-secret", "extra-secret-field", "source-mismatch", "extra-source-field"} {
		t.Run(control, func(t *testing.T) {
			dump, err := reverseconverter.ReadBytes(data)
			if err != nil {
				t.Fatal(err)
			}
			for i, record := range dump.Records["memory"] {
				if record.ID != "secret" {
					continue
				}
				switch control {
				case "unknown-classification":
					record.Body["classification"] = "private"
				case "missing-value":
					delete(record.Body, "value")
				case "fake-secret":
					record.Body["value"] = map[string]any{"$secret": "config/kv.memory.wrong"}
					record.Source.Fields["value"] = record.Body["value"]
				case "extra-secret-field":
					record.Body["value"] = map[string]any{"$secret": "config/kv.memory.secret", "extra": true}
					record.Source.Fields["value"] = record.Body["value"]
				case "source-mismatch":
					record.Source.Fields["value"] = nil
				case "extra-source-field":
					record.Source.Fields["extra"] = true
				}
				dump.Records["memory"][i] = record
			}
			_, err = reverseconverter.Convert(dump)
			var refusal *reverseconverter.Refusal
			if !errors.As(err, &refusal) {
				t.Fatalf("invalid reverse %s accepted: %v", control, err)
			}
		})
	}
}

func putImportedMemoryControl(t *testing.T, store driver.Store, lease driver.ActiveLease, record driver.Record) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	key := driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref}
	_, err = tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}})
	if err != nil {
		t.Fatal(err)
	}
	previous, err := tx.Get(ctx, []model.Ref{record.Ref})
	if err != nil || len(previous) != 1 {
		t.Fatalf("control preimage: %v", err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: restorePrincipal, LockKeys: []driver.LockKey{key}, Changes: []model.ChangeRecord{{Ref: record.Ref, Op: model.OpPatch, FromRevision: previous[0].Revision, ChangedPaths: []string{"/"}}}}); err != nil {
		t.Fatal(err)
	}
}

func importedMemoryHistory(t *testing.T, store driver.Store) any {
	t.Helper()
	ctx := context.Background()
	state := reverseState(t, store)
	r, err := store.Read(ctx, driver.View{Incarnation: state.Meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	versions := map[string]driver.Page[driver.Version]{}
	for _, record := range state.Records {
		page, err := r.Versions(ctx, record.Ref, driver.PageReq{Limit: 1000})
		if err != nil || page.NextCursor != nil {
			t.Fatalf("incomplete versions for %s: %v", record.Ref.ID, err)
		}
		versions[record.Ref.ID] = page
	}
	var cursor *driver.Cursor
	if !state.Meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: state.Meta.Incarnation, Revision: state.Meta.HistoryFloor, Index: model.Natural64(0)}
	}
	changes, err := r.Changesets(ctx, cursor, 1000)
	if err != nil || len(changes) >= 1000 {
		t.Fatalf("incomplete changeset history: %v", err)
	}
	return struct {
		Versions map[string]driver.Page[driver.Version]
		Changes  []model.Changeset
	}{versions, changes}
}

func TestImportedMemoryMalformedMarkersRefuseWithoutWritesAllDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			for _, control := range []string{"unmarked", "source-id-only", "wrong-table", "wrong-ref", "wrong-source-value", "fake-secret", "missing-value"} {
				t.Run(control, func(t *testing.T) {
					ctx := context.Background()
					store, lease, _ := openIdentityTarget(t, engine)
					ref := model.RefByID("source/memory/control", model.Natural64(0))
					body := map[string]any{"key": "control", "value": map[string]any{"x": 1}, "classification": "public", "source_id": "control", "source_table": "config", "source": map[string]any{"key": "kv.memory.control", "value": map[string]any{"x": 1}}}
					switch control {
					case "unmarked":
						body = map[string]any{"key": "control", "value": 1}
					case "source-id-only":
						body = map[string]any{"source_id": "control"}
					case "wrong-table":
						body["source_table"] = "metadata"
					case "wrong-ref":
						ref = model.RefByID("ordinary", model.Natural64(0))
					case "wrong-source-value":
						body["source"].(map[string]any)["value"] = 2
					case "fake-secret":
						body["classification"] = "secret"
					case "missing-value":
						delete(body, "value")
					}
					meta, err := store.Meta(ctx)
					if err != nil {
						t.Fatal(err)
					}
					tx, err := store.Write(ctx, lease)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Close()
					key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
					if _, err := tx.Lock(ctx, []driver.LockRequest{{Key: key, Mode: driver.LockUpdate}}); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Head(ctx); err != nil {
						t.Fatal(err)
					}
					if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "memory", Body: encode(t, body)}); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: restorePrincipal, LockKeys: []driver.LockKey{key}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/"}}}}); err != nil {
						t.Fatal(err)
					}
					before, historyBefore := reverseState(t, store), importedMemoryHistory(t, store)
					ontology := service.NewOntology(nil)
					svc := service.New(store, lease, pipeline.New(store, lease, pipeline.WithOntology(ontology)), service.WithOntology(ontology))
					anonymous := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
					_, getErr := svc.MemoryGet(ctx, anonymous, service.MemoryGetRequest{ID: ref})
					_, listErr := svc.MemoryList(ctx, anonymous, service.MemoryListRequest{})
					for _, err := range []error{getErr, listErr} {
						var typed *model.Error
						if !errors.As(err, &typed) || typed.Data.Code != "internal_error" {
							t.Fatalf("malformed %s hidden: %v", control, err)
						}
					}
					if !reflect.DeepEqual(before, reverseState(t, store)) || !reflect.DeepEqual(historyBefore, importedMemoryHistory(t, store)) {
						t.Fatalf("malformed %s read changed state or history", control)
					}
				})
			}
		})
	}
}

func TestLegacyMemoryRawMirrorReverseUnchangedAllDrivers(t *testing.T) {
	data := supportedBytes(t, nil)
	verification := supportedVerification(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, _ := openIdentityTarget(t, engine)
			if _, err := RestoreWithLease(ctx, bytes.NewReader(data), store, lease, verification); err != nil {
				t.Fatal(err)
			}
			before := reverseState(t, store)
			reversed, err := ReverseRestored(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := reverseconverter.ConvertArchive(data)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reversed.Result.JSONL, expected.JSONL) {
				t.Fatal("legacy reverse output changed")
			}
			for _, row := range before.Records {
				if row.Ref.ID == "source/memory/fixture" {
					var raw map[string]any
					decode(t, row.Body, &raw)
					if len(raw) != 6 || raw["value"] != "A complete synthetic memory" || raw["id"] != nil || raw["content"] != nil {
						t.Fatal("legacy raw memory schema changed")
					}
				}
			}
			if !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("legacy reverse mutated records")
			}
		})
	}
}
