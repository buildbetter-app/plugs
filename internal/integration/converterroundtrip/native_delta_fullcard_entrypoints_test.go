package converterroundtrip

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// These are the three original public CP11 archives from producer 53d90913,
// run 42864951650. They are independent of the retained native delta pair.
type fullCardReceivingCase struct {
	name, file, sha256, storeID string
	bytes, images               int
	complete                    bool
}

var fullCardReceivingCases = []fullCardReceivingCase{
	{"loose_missing_root", "public-cp11-loose-missing-root.tar", "d381b6b254947f9d466e0d1354e9c7b05263308aca6c0ad30f217638cbfd8ec0", "native/91ccd21e62d3d09e427d402c84ee044c313a52df276a4fd1cc0ca2a1831e15ac", 293888, 12, false},
	{"darc_missing_root", "public-cp11-darc-missing-root.tar", "b1f7f3f5d5d7f3132a0eff8a22d414957e43eaf699e45515b356686ea6be9ba7", "native/204b86c9994252ee931c1fd6ade945f638e30cfe171db15ff5121525d6d68602", 293888, 12, false},
	{"darc_complete", "public-cp11-darc-complete.tar", "9d2012775a493a5281ae72380e0376aa8bb16818ab28fa9b346252153ea67fa5", "native/c04f8d05426fe59879c4cc86588aa97dfd3b14845c94676eab40c81944a73a95", 312320, 14, true},
}

func fullCardReceivingArchive(t *testing.T, test fullCardReceivingCase) (*dumpformat.Archive, []byte) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "native-delta-fullcard-entrypoints", test.file))
	if err != nil || len(raw) != test.bytes || hashBytes(raw) != test.sha256 {
		t.Fatal("original current public CP11 archive binding differs", err)
	}
	archive, err := dumpformat.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal("original current public CP11 archive", err)
	}
	manifest := archive.Manifest()
	scopes := archive.NativeSourceHistories()
	if manifest.StoreID != test.storeID || manifest.MainVersion != 66 || manifest.IgnoredVersion != 26 || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != "public" || len(scopes) != 1 {
		t.Fatal("current public source identity or schema differs")
	}
	scope := scopes[0]
	if scope.StoreID != test.storeID || len(scope.Projects) != 1 || scope.Projects[0] != "public" || scope.Complete != test.complete || len(scope.Commits) != test.images || len(scope.Images) != test.images || len(scope.Current) != 30 || len(scope.Gaps) != 0 {
		t.Fatal("current public source history or family census differs")
	}
	if test.complete {
		if scope.HistoryCoverage != nil {
			t.Fatal("complete original gained a history boundary")
		}
	} else {
		coverage := scope.HistoryCoverage
		if coverage == nil || coverage.VerifiedCommitCount != uint64(test.images) || len(coverage.Boundaries) != 1 || coverage.Boundaries[0].Reason != "missing_root_chunk" || coverage.Boundaries[0].CommitOrdinalFromHead != 13 || coverage.EventsAttachment.Rows != 2 {
			t.Fatal("original missing-root boundary or events attachment differs")
		}
	}
	return archive, raw
}

// This callable library-report adapter exercises the current public originals
// through real library publication, independent cell readback, populated-target
// refusal, and recovery. It emits public-<case>-library-<engine>.json through the
// existing helper; persistent drivers really close and reopen.
func TestNativeFullCardLibraryReport(t *testing.T) {
	for _, test := range fullCardReceivingCases {
		t.Run(test.name, func(t *testing.T) {
			_, input := fullCardReceivingArchive(t, test)
			exerciseNativeHistoryReceivingLibrary(t, input, test.name, test.complete)
			outputs := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
			if outputs == "" {
				t.Fatal("callable library-report adapter requires normal CI report output")
			}
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				path := filepath.Join(outputs, "public-"+test.name+"-library-"+engine+".json")
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal("required recovered library report missing", err)
				}
				var result struct {
					Engine        string `json:"engine"`
					ArchiveSHA256 string `json:"archive_sha256"`
					PolicySHA256  string `json:"policy_sha256"`
					Report        Report `json:"committed_report"`
				}
				decode(t, raw, &result)
				if result.Engine != engine || result.ArchiveSHA256 != test.sha256 || result.PolicySHA256 != hashBytes(nativeCommitReceivingPolicy(t, input, engine)) {
					t.Fatal("recovered library report input binding differs")
				}
				assertFullCardRecoveredReport(t, result.Report)
			}
		})
	}
}

func assertFullCardRecoveredReport(t *testing.T, report Report) {
	t.Helper()
	if !report.Recovered || report.Receipt == nil || report.IdempotencyKey == "" || report.RequestHash == "" || report.IdempotencyKey != report.Receipt.Changeset.IdempotencyKey || report.RequestHash != report.Receipt.RequestHash {
		t.Fatal("recovered report lost exact idempotency/request/committed receipt binding")
	}
}

func TestNativeFullCardDefaultCLI(t *testing.T) {
	binary, binaryHash := receivingBinary(t)
	for _, test := range fullCardReceivingCases {
		t.Run(test.name, func(t *testing.T) {
			t.Run("postgres", func(t *testing.T) {
				archive, input := fullCardReceivingArchive(t, test)
				// Existing normal CI fixture: pinned image, loopback binding,
				// owned cleanup, and fatal setup errors rather than skips.
				target := identityPostgresDSN(t)
				policy := nativeCommitReceivingPolicy(t, input, "postgres")
				name := "fullcard-" + test.name + "-postgres"
				request := prepareReceivingRequest(t, binary, binaryHash, "postgres", input, policy, name)
				stdout, stderr, err := executeReceiving(t, binary, binaryHash, "postgres", target, input, request, name, policy)
				if err != nil {
					t.Fatalf("current public PostgreSQL default receiving: %v %s", err, stderr)
				}
				var result receivingReceipt
				if err := decodeNativeSchemaFixture(stdout, &result); err != nil {
					t.Fatal("current public PostgreSQL receipt", err)
				}
				if result.Format != "plug-native-receiving-receipt/1" || result.BinarySHA256 != binaryHash || result.PolicySHA256 != hashBytes(policy) || result.RequestSHA256 != hashBytes(encode(t, request)) || result.ArchiveSHA256 != test.sha256 || result.Driver != "postgres" || result.HistoryMode != "full" || result.Report.Receipt == nil || result.CrossProjectIDCollisions != 0 {
					t.Fatal("current public executable/input/receipt binding differs")
				}
				assertNativeReceivingHistoryReport(t, archive, result.Report, test.complete)
				assertFullCardRecoveredReport(t, result.Report)
				if result.Report.IdempotencyKey != request.IdempotencyKey {
					t.Fatal("recovered default-CLI report idempotency key differs")
				}
				when, err := time.Parse(time.RFC3339Nano, result.RestoreTime)
				if err != nil {
					t.Fatal("current public committed restore clock", err)
				}
				currentRows := assertNativeCommitFamilyReports(t, archive, when, result.Report.NativeFamilies)
				opaqueFields := 0
				for _, count := range nativeCommitOpaqueCounts(t, archive, request.SourcePlan) {
					opaqueFields += count
				}
				families, historyTables, historyRows := 0, 0, 0
				for _, scope := range archive.NativeSourceHistories() {
					families += len(scope.Current)
					for _, image := range scope.Images {
						historyTables += len(image.Tables)
						for _, table := range image.Tables {
							historyRows += len(table.Rows)
						}
					}
				}
				v := result.Verification
				if !v.ArchiveBlob || v.Reopen != "driver-reopened" || v.CurrentFamilies != families || v.CurrentRows != currentRows || v.OpaqueMetadataFields != opaqueFields || v.HistoryImages != test.images || v.HistoryTables != historyTables || v.HistoryRows != historyRows {
					t.Fatal("current public persistent readback census differs")
				}
				// This independent reopen invokes all original current/history
				// cell, source-plane, schema/default, opaque and coverage checks.
				before := inspectNativeHistoryReceivingTarget(t, "postgres", target, input, result.Report, test.complete)
				stdout, stderr, err = executeReceiving(t, binary, binaryHash, "postgres", target, input, request, name+"-populated", policy)
				if err == nil || len(stdout) != 0 || !strings.Contains(string(stderr), "not empty") {
					t.Fatal("populated current public PostgreSQL target must refuse without a receipt", err, string(stderr))
				}
				after := inspectNativeHistoryReceivingTarget(t, "postgres", target, input, result.Report, test.complete)
				if !bytes.Equal(before, after) {
					t.Fatal("populated refusal changed core or provider checkpoint")
				}
			})
		})
	}
}
