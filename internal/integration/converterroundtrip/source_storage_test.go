package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These changed inputs are explicitly synthetic. Real producer NULL fields
// are never rewritten to make them satisfy a disposition policy.
func jointStorageFixture(t *testing.T, mode string) []byte {
	t.Helper()
	base := jointSourceFixture(t)
	return rewriteData(t, func(members map[string][]byte) {
		reader := tar.NewReader(bytes.NewReader(base))
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
		for _, name := range []string{"records/bead.ndjson", "records/beads-archive.ndjson"} {
			var records []dumpformat.Record
			for _, line := range bytes.Split(bytes.TrimSpace(members[name]), []byte{'\n'}) {
				if len(line) == 0 {
					continue
				}
				var record dumpformat.Record
				decode(t, line, &record)
				if record.Source.Table == dumpformat.DurableEntityTable || record.Source.Table == dumpformat.EphemeralEntityTable {
					columnMode := mode
					if strings.HasPrefix(mode, "active-") {
						columnMode = "null"
						if record.Source.Table == dumpformat.EphemeralEntityTable {
							columnMode = strings.TrimPrefix(mode, "active-")
						}
					}
					switch columnMode {
					case "null":
						record.Source.Fields["storage_class"] = nil
					case "empty-text":
						record.Source.Fields["storage_class"] = ""
					case "absent":
						delete(record.Source.Fields, "storage_class")
					default:
						t.Fatal("unknown fixture storage mode")
					}
					if record.Kind == "beads-archive" {
						record.ID = record.Source.Table + "/" + digestBytes(encode(t, record.Source.Fields))
					}
				}
				records = append(records, record)
			}
			sort.SliceStable(records, func(i, j int) bool {
				if records[i].Project != records[j].Project {
					return records[i].Project < records[j].Project
				}
				return records[i].ID < records[j].ID
			})
			var data []byte
			for _, record := range records {
				data = append(data, encode(t, record)...)
			}
			members[name] = data
		}
	})
}

func TestJointSourceNullStorageClassExactPolicyAcrossThreeStores(t *testing.T) {
	data := jointStorageFixture(t, "null")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			nullPolicy := func(plan *driver.SourcePlanePlanV1) {
				for i := range plan.Dispositions {
					plan.Dispositions[i].StorageClass = ""
				}
			}
			p := jointProfileWithPlan(t, data, store, lease, nullPolicy)
			archive, err := dumpformat.Read(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			const namespace = "plugin/workflow/default/durable"
			before := savedEmptyJointTarget(t, store, archive, p)
			beforeCheckpoint, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"empty-text", "absent", "active-empty-text", "active-absent"} {
				t.Run(mode, func(t *testing.T) {
					changed := jointStorageFixture(t, mode)
					profile, plan := jointUnboundProfileWithPlan(t, changed, store, lease, nullPolicy)
					reason := "core type/storage/flags"
					if mode == "active-empty-text" {
						reason = "source type/storage/flags"
					} else if mode == "active-absent" {
						reason = "source storage_class column absent"
					}
					if _, err := BindSourceImport(ctx, bytes.NewReader(changed), profile, plan); !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), reason) {
						t.Fatal("raw empty/absent storage column aliased SQL NULL or refused at unrelated boundary", err)
					}
				})
			}
			for _, mode := range []string{"durable-text", "active-text", "durable-flags", "active-flags"} {
				t.Run(mode, func(t *testing.T) {
					plan, err := p.SourcePlanes.Plan()
					if err != nil {
						t.Fatal(err)
					}
					index := 0
					if strings.HasPrefix(mode, "durable") {
						index = 1
					}
					if strings.HasSuffix(mode, "text") {
						plan.Dispositions[index].StorageClass = "versioned"
					} else {
						plan.Dispositions[index].NoHistory = !plan.Dispositions[index].NoHistory
					}
					binding, err := driver.NewSourcePlaneImportBindingV1(plan)
					if err != nil {
						t.Fatal("negative policy lost valid structural baseline", err)
					}
					bad := p
					bad.SourcePlanes = &binding
					target := &jointTargetAccess{Store: store}
					if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), target, lease, bad); !errors.Is(err, driver.ErrUnsupported) || !strings.Contains(err.Error(), "type/storage/flags") {
						t.Fatal("wrong storage/flag policy was admitted or refused at unrelated boundary", err)
					}
					if target.metaReads != 0 || target.writes != 0 {
						t.Fatal("invalid policy reached target store")
					}
				})
			}
			checkpoint, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || !reflect.DeepEqual(before, savedEmptyJointTarget(t, store, archive, p)) || !reflect.DeepEqual(beforeCheckpoint, checkpoint) {
				t.Fatal("null/text/absent/flag refusal changed target", err)
			}
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal("exact null policy failed publication after refusals", err)
			}
			plan, err := p.SourcePlanes.Plan()
			if err != nil {
				t.Fatal(err)
			}
			for _, entity := range plan.Entities {
				found := false
				for _, field := range entity.Fields {
					if field.Name == "storage_class" {
						found = true
						if field.Value != jointCommitment([]byte("null"), "null") {
							t.Fatal("SQL NULL origin/hash was replaced with text")
						}
					}
				}
				if !found {
					t.Fatal("null storage field omitted from exact commitment census")
				}
			}
			_, server := sourcePublicService(t, store, lease, p, report, &sourcePublicPolicy{})
			sourcePublicCall(t, server, p.Admission.Actor, "bead.update", 401, map[string]any{"id": model.RefByID("demo-7", model.Natural64(0)), "fields": map[string]any{"title": "live null-storage source"}}, "")
			conserved := savedLabelImport(t, store, archive, p, "")
			checkpoint, err = capability.LoadCheckpoint(ctx, namespace)
			if err != nil {
				t.Fatal(err)
			}
			store, lease = reopen()
			bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if err != nil || !found || !reflect.DeepEqual(bound.SourcePlanes, p.SourcePlanes) || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(conserved, savedLabelImport(t, store, archive, p, "")) {
				t.Fatal("reopen lost exact null policy or retained source bytes", err)
			}
			capability, err = driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			later, err := capability.LoadCheckpoint(ctx, namespace)
			if err != nil || !reflect.DeepEqual(checkpoint, later) {
				t.Fatal("reopen rewrote source checkpoint", err)
			}
		})
	}
}
