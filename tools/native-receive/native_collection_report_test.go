package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func collectionReportFixture() converterroundtrip.Report {
	when := time.Date(2100, 1, 2, 3, 4, 5, 123, time.UTC)
	ttl := model.Natural64(1234)
	return converterroundtrip.Report{
		Receipt: &driver.IdempotencyRecord{
			RequestHash: "request<&>", ResponsePending: true,
			Commit: model.Commit{
				Revision:    model.Natural64(195),
				Results:     []model.OpResult{{Ref: model.RefByID("item<&>", model.Natural64(0)), Revision: model.Natural64(194), Record: json.RawMessage(`{ "exact":9007199254740993,"text":"<&>" }`), SetBy: map[string]string{"status": "caller"}}},
				Warnings:    []model.Warning{{Code: "ontology_drifted", Message: "observed<&>", Details: map[string]any{"count": json.Number("9007199254740993"), "nested": []any{nil, true, "line\u2028separator"}}}},
				WouldCommit: true, Violations: []model.Violation{{Guard: "guard", Message: "message", Details: map[string]any{"field": "value"}}},
				LeaseExpiresAt: &when, LeaseTtlMs: &ttl,
			},
			Changeset: model.Changeset{
				Revision: model.Natural64(195), Branch: model.Natural64(0), Incarnation: "incarnation",
				Kind: model.ChangesetRestore, Time: when, Principal: model.PrincipalRef{Kind: "user", ID: "restore-admin"},
				Chain: []string{}, IdempotencyKey: "key", Mutators: []string{"restore"},
				Changes: []model.ChangeRecord{{Ref: model.RefByID("item<&>", model.Natural64(0)), Op: model.OpCreate, FromRevision: model.Natural64(0), ChangedPaths: []string{"/status"}}},
			},
		},
		NativeFamilies:  []converterroundtrip.NativeFamilyReport{{Source: dumpformat.SourceIdentity{StoreID: "ops-store", Project: "ops"}, Family: "issues", History: "retained-native-commit", SourceRows: 3, ImportedRows: 3}},
		NativeSchemas:   []converterroundtrip.NativeSchemaReport{{}},
		NativeHistories: []converterroundtrip.NativeHistoryReport{{Source: dumpformat.SourceIdentity{StoreID: "ops-store", Project: "ops"}, Complete: true, VerifiedCommitCount: 97}},
		SourcePlanes:    &driver.SourcePlaneCommitBindingV1{},
	}
}

func assertCollectionReportScopeReleased(t *testing.T, budget *dumpformat.NativeDeltaBudget) {
	t.Helper()
	usage := budget.Usage()
	if usage.CurrentAccountedDecodedBytes != 0 || len(usage.CurrentCategories) != 0 || len(usage.CurrentCharges) != 0 {
		t.Fatal("report commitment retained reservations", usage)
	}
}

func TestNativeCollectionReportCommitmentCountsOriginalReceiptAndEveryChannel(t *testing.T) {
	for _, empty := range []bool{false, true} {
		report := collectionReportFixture()
		if empty {
			report.Receipt.Commit.Results = []model.OpResult{}
			report.Receipt.Commit.Warnings = []model.Warning{}
			report.Receipt.Commit.Violations = nil
			report.Receipt.Commit.WouldCommit = false
			report.Receipt.Commit.LeaseExpiresAt, report.Receipt.Commit.LeaseTtlMs = nil, nil
			report.Receipt.Changeset.Changes = nil
		}
		original := []any{report.Receipt, report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes}
		raw, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		counted := []any{(*nativeCollectionReceiptJSON)(report.Receipt), report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes}
		n, err := dumpformat.NativeDeltaJSONSize(counted)
		if err != nil || n != uint64(len(raw)+1) {
			t.Fatal("borrowed receipt accounting differs from independent actual encoding", n, len(raw)+1, err)
		}
		receiptRaw, err := json.Marshal(report.Receipt)
		if err != nil {
			t.Fatal(err)
		}
		receiptSize := uint64(len(receiptRaw) + 1)
		admitted := n + 2*receiptSize
		budget := dumpformat.NewNativeDeltaBudget(admitted)
		got, err := nativeCollectionReportCommitment(budget, report)
		want := sha256.Sum256(append(raw, '\n'))
		if err != nil || got != hex.EncodeToString(want[:]) || budget.Usage().PeakAccountedDecodedBytes != admitted {
			t.Fatal("full original report commitment/admission differs", got, err, budget.Usage())
		}
		assertCollectionReportScopeReleased(t, budget)
		for _, boundary := range []struct {
			bytes uint64
			phase string
		}{{n - 1, "cli_collection_report_encoding"}, {n + receiptSize - 1, "cli_collection_receipt_encoding"}, {admitted - 1, "cli_collection_receipt_validation"}} {
			refused := dumpformat.NewNativeDeltaBudget(boundary.bytes)
			got, err = nativeCollectionReportCommitment(refused, report)
			var limit *dumpformat.NativeDeltaDecodedLimitError
			if got != "" || !errors.As(err, &limit) || limit.Phase != boundary.phase {
				t.Fatal("report encoding crossed its exact admission boundary", got, err)
			}
			assertCollectionReportScopeReleased(t, refused)
		}
	}
	for _, mutate := range []func(*converterroundtrip.Report){
		func(r *converterroundtrip.Report) { r.Receipt.RequestHash += "changed" },
		func(r *converterroundtrip.Report) { r.Receipt.Commit.Results[0].SetBy["status"] = "changed" },
		func(r *converterroundtrip.Report) { r.Receipt.Changeset.Changes[0].ChangedPaths[0] = "/changed" },
		func(r *converterroundtrip.Report) { r.Receipt.ResponsePending = false },
		func(r *converterroundtrip.Report) { r.NativeFamilies[0].ImportedRows++ },
		func(r *converterroundtrip.Report) { r.NativeSchemas = nil },
		func(r *converterroundtrip.Report) { r.NativeHistories[0].VerifiedCommitCount++ },
		func(r *converterroundtrip.Report) { r.SourcePlanes = nil },
	} {
		budget := dumpformat.NewNativeDeltaBudget(0)
		report := collectionReportFixture()
		before, err := nativeCollectionReportCommitment(budget, report)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&report)
		after, err := nativeCollectionReportCommitment(budget, report)
		if err != nil || before == after {
			t.Fatal("receipt/report channel was omitted from commitment", err)
		}
		assertCollectionReportScopeReleased(t, budget)
	}
}

func TestNativeCollectionReportCommitmentAdmitsBeforeValidatingEncoding(t *testing.T) {
	report := collectionReportFixture()
	report.Receipt.Commit.Results = nil // its actual model marshaler must refuse
	counted := []any{(*nativeCollectionReceiptJSON)(report.Receipt), report.NativeFamilies, report.NativeSchemas, report.NativeHistories, report.SourcePlanes}
	n, err := dumpformat.NativeDeltaJSONSize(counted)
	if err != nil {
		t.Fatal(err)
	}
	budget := dumpformat.NewNativeDeltaBudget(n - 1)
	got, err := nativeCollectionReportCommitment(budget, report)
	var limit *dumpformat.NativeDeltaDecodedLimitError
	if got != "" || !errors.As(err, &limit) || limit.Phase != "cli_collection_report_encoding" {
		t.Fatal("serializer ran before the refused admission", got, err)
	}
	assertCollectionReportScopeReleased(t, budget)
	receiptSize, err := dumpformat.NativeDeltaJSONSize((*nativeCollectionReceiptJSON)(report.Receipt))
	if err != nil {
		t.Fatal(err)
	}
	budget = dumpformat.NewNativeDeltaBudget(n + 2*receiptSize)
	got, err = nativeCollectionReportCommitment(budget, report)
	if got != "" || err == nil || errors.As(err, &limit) {
		t.Fatal("actual validating model serializer was bypassed", got, err)
	}
	assertCollectionReportScopeReleased(t, budget)
}
