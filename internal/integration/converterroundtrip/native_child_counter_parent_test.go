package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const publicCounterChild = "clockready-wisp-wf0.1"
const publicCounterParent = "clockready-wisp-wf0"

// This is the unchanged committed 9aea fixture from actual public capture
// c774/run43216037385. Its original SQL and create receipt are the oracle;
// neither a generated store nor the receiver's output supplies expectations.
// The transient original Dolt directory was not retained by the producer.
func publicChildCounterCapture(t *testing.T) *dumpformat.Archive {
	t.Helper()
	raw, err := os.ReadFile("testdata/native-ready-c774-43216037385.tar.gz")
	if err != nil || hashBytes(raw) != "30741ec284a9dfffadde302cf8fdccac4b51f04ee14f3cf26c1e6539d722a47f" {
		t.Fatal("actual public fixture differs", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	want := map[string]string{
		"physical/capture-set/capture-set.json":                "c2048f8bfd934f7501ceff3c3127af44cf79678b549f7d1b1d75955a2781364e",
		"physical/capture-set/head.tar":                        "f6e9529714ba0945e0cefdba1fc4438454bdda4375d141a826de18b0eca8aae4",
		"physical/capture-set/parts/00000000000000000001.part": "62a20786d96bec2e3ec8e9f9b641e07567679c5c6eafdff06aa0cc0ead62f40a",
		"states/final-before-capture.json":                     "a7f902dade3fbaaae9985b0135e6a4f693e954061dad12454b0cfe293c9a3401",
		"states/final-after-capture.json":                      "a7f902dade3fbaaae9985b0135e6a4f693e954061dad12454b0cfe293c9a3401",
		"commands/127-create-ordinary_child.stdout":            "997ad5c2283ccebd9c8ead66eb7c09d3e687d3fbc940172bbc66890147e1f8f1",
	}
	files := map[string][]byte{}
	reader := tar.NewReader(gz)
	for {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, segment := range strings.Split(h.Name, "/") {
			if segment == "poll.log" || segment == ".artifact" {
				t.Fatal("excluded member before payload read")
			}
		}
		name := strings.TrimPrefix(h.Name, "b01-ready/")
		if want[name] == "" {
			continue
		}
		if h.Typeflag != tar.TypeReg || h.Size < 0 || h.Size > 4194304 || files[name] != nil {
			t.Fatal("public input member type/size/census differs", name)
		}
		data, err := io.ReadAll(io.LimitReader(reader, h.Size+1))
		if err != nil || int64(len(data)) != h.Size || hashBytes(data) != want[name] {
			t.Fatal("original public input member differs", name, err)
		}
		files[name] = data
	}
	if len(files) != len(want) || !bytes.Equal(files["states/final-before-capture.json"], files["states/final-after-capture.json"]) {
		t.Fatal("public source census or before/after SQL differs")
	}
	var state struct {
		Tables map[string]dumpformat.NativeTable `json:"tables"`
	}
	decode(t, files["states/final-before-capture.json"], &state)
	find := func(table, column, id string) dumpformat.Row {
		t.Helper()
		var found dumpformat.Row
		for _, row := range state.Tables[table].Rows {
			if row[column] == id {
				if found != nil {
					t.Fatal("duplicate independent SQL identity", table, id)
				}
				found = row
			}
		}
		return found
	}
	child, parent := find("issues", "id", publicCounterChild), find("wisps", "id", publicCounterParent)
	counter := find("wisp_child_counters", "parent_id", publicCounterParent)
	var created struct {
		ID string `json:"id"`
	}
	decode(t, files["commands/127-create-ordinary_child.stdout"], &created)
	if child == nil || parent == nil || find("issues", "id", publicCounterParent) != nil || counter["last_child"] != json.Number("1") || created.ID != publicCounterChild {
		t.Fatal("independent child/active-only-parent/counter/create witness differs")
	}
	// The established portable issue contract rebuilds these two operational
	// SQL columns. Every other child field remains an exact SQL expectation.
	childFields := dumpformat.Row{}
	for key, value := range child {
		if key != "row_lock" && key != "is_blocked" {
			childFields[key] = value
		}
	}
	prefix := "physical/capture-set/"
	a, err := dumpformat.ReadNativeCaptureSet(context.Background(), bytes.NewReader(files[prefix+"capture-set.json"]), bytes.NewReader(files[prefix+"head.tar"]), func(_ context.Context, n uint64) (io.ReadCloser, error) {
		data, ok := files[prefix+fmt.Sprintf("parts/%020d.part", n)]
		if !ok {
			return nil, os.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}, dumpformat.NativeCaptureSetOptions{PrepareReceiving: true})
	if err != nil {
		t.Fatal("prepare authentic capture with active-only counter parent", err)
	}
	t.Cleanup(a.ReleaseNativeDecoded)
	if a.SHA256() != want[prefix+"capture-set.json"] || len(a.Manifest().Projects) != 1 || a.Manifest().Projects[0].ChildCounters[publicCounterParent] != 1 {
		t.Fatal("prepared source identity or parent counter differs")
	}
	childFound, parentFound, counterFound := false, false, false
	for _, r := range a.Records("bead") {
		if r.ID == publicCounterParent {
			t.Fatal("active parent synthesized as durable issue")
		}
		if r.ID == publicCounterChild {
			childFound = reflect.DeepEqual(r.Source.Fields, childFields)
		}
	}
	for _, r := range a.Records("beads-archive") {
		if r.Source.Table == "wisps" && r.Source.Fields["id"] == publicCounterParent {
			parentFound = reflect.DeepEqual(r.Source.Fields, parent)
		}
		if r.Source.Table == "wisp_child_counters" && r.Source.Fields["parent_id"] == publicCounterParent {
			counterFound = reflect.DeepEqual(r.Source.Fields, counter)
		}
	}
	if !childFound || !parentFound || !counterFound {
		t.Fatal("prepared exact original child, parent or raw counter changed")
	}
	return a
}

func TestPublicNativeChildCounterActiveParentReceivingAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			a := publicChildCounterCapture(t)
			ctx := a.WithNativeDeltaAccounting(context.Background())
			store, initial, reopen := openIdentityTarget(t, engine)
			if err := initial.Release(ctx); err != nil {
				t.Fatal(err)
			}
			op, lease, release, err := HoldRestoreLease(ctx, store, "public-counter-parent")
			if err != nil {
				t.Fatal(err)
			}
			release = sync.OnceValue(release)
			defer func() {
				if err := release(); err != nil {
					t.Error(err)
				}
			}()
			plan := func() driver.SourcePlanePlanV1 {
				work, release := a.NativeDeltaAccountingScope()
				defer release()
				return nativeHistoryPlanFromArchive(t, work, store, lease)
			}()
			plan.Dispositions[0].StorageClass = ""
			plan.Dispositions[0].Statuses = nil
			for _, status := range a.Manifest().Projects[0].Statuses {
				plan.Dispositions[0].Statuses = append(plan.Dispositions[0].Statuses, driver.SourcePlaneStringPairV1{From: status.Name, To: status.Name})
			}
			sort.Slice(plan.Dispositions[0].Statuses, func(i, j int) bool {
				return plan.Dispositions[0].Statuses[i].From < plan.Dispositions[0].Statuses[j].From
			})
			for _, row := range a.Records("bead") {
				storage, present := row.Source.Fields["storage_class"]
				if row.Source.Fields["issue_type"] != "task" || !plan.Dispositions[0].MatchesStorageClass(storage, present) {
					t.Fatal("host intent differs from actual SQL")
				}
			}
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(a.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: a.SHA256(), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format("2006-01-02T15:04:05.999999999Z07:00")}
			profile, err = BindNativeCaptureSetImport(op, a, profile, plan)
			if err != nil {
				t.Fatal("bind authentic active-parent capture", err)
			}
			report, err := RestoreNativeCaptureSetImportWithLease(op, a, store, lease, profile)
			if err != nil {
				t.Fatal("import authentic active-parent capture", err)
			}
			check := func(ctx context.Context, store driver.Store, report Report) {
				t.Helper()
				if err := VerifyNativeCaptureSetReadback(ctx, store, a, report); err != nil {
					t.Fatal("complete native table and snapshot readback", err)
				}
				r, err := store.Read(ctx, driver.View{Incarnation: report.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				rows, err := r.Get(ctx, []model.Ref{model.RefByID(publicCounterChild, model.Natural64(0))})
				if err != nil || len(rows) != 1 {
					t.Fatal("exact imported child absent", err)
				}
				doc, err := pipeline.DecodeDocument(rows[0].Body)
				if err != nil || rows[0].Kind != "bead" || doc.ID != publicCounterChild || doc.Project != "clockready" {
					t.Fatal("child identity or number changed", err)
				}
				allocator, err := r.(driver.AllocatorReader).LoadAllocator(ctx, "clockready")
				if err != nil || allocator.Children[publicCounterParent] != 1 {
					t.Fatal("actual parent counter changed", allocator, err)
				}
			}
			check(op, store, report)
			if err := release(); err != nil {
				t.Fatal(err)
			}
			store, _ = reopen()
			_, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) {
				t.Fatal("authentic import recovery differs", err)
			}
			check(ctx, store, recovered)
			if recovered.Archive != nil {
				recovered.Archive.ReleaseNativeDecoded()
			}
			a.ReleaseNativeDecoded()
			usage, _ := a.NativeDecodedUsage()
			if usage.LimitBytes != 2147483648 || usage.CurrentAccountedDecodedBytes != 0 || len(usage.CurrentCategories) != 0 || len(usage.CurrentCharges) != 0 {
				t.Fatal("final receiving accounting differs", usage)
			}
			t.Logf("public_child_counter_receiving=PASS engine=%s child=%s parent=%s child_number=1 native_parent_counter=1 source_sha256=%s before_after_SQL_equal=true imported_and_reopened_full_readback=true after_release_bytes=0", engine, publicCounterChild, publicCounterParent, a.SHA256())
		})
	}
}
