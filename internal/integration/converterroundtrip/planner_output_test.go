package converterroundtrip

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Output evidence keeps payloads as []byte, including derived values and schema
// defaults. JSON therefore base64-encodes the original buffers losslessly.
// Serializing RawMessage here would normalize bytes before the comparison.
type receivingPlannerRecordOutput struct {
	LockKey string
	RefJSON []byte
	Kind    string
	Body    []byte
}

type receivingPlannerDerivedOutput struct {
	Table driver.DerivedTable
	Key   string
	Value []byte
}

type receivingPlannerDefaultOutput struct {
	TargetKind string
	Field      string
	Value      []byte
	Count      int
}

type receivingPlannerSchemaOutput struct {
	Source               dumpformat.SourceIdentity
	Profile              dumpformat.NativeSchemaProfile
	SchemaDefaultCount   int
	PlugDefaults         []receivingPlannerDefaultOutput
	TargetOmissions      []NativeTargetOmission
	OpaqueMetadataFields int
}

type receivingPlannerOutput struct {
	Format          string
	Fixture         string
	ArchiveSHA256   string
	Profile         []byte
	When            string
	Records         []receivingPlannerRecordOutput
	Derived         []receivingPlannerDerivedOutput
	DerivedDigest   string
	Blobs           map[string][]byte
	Allocators      []driver.OfflineAllocator
	Sentinels       []driver.Sentinel
	NativeFamilies  []NativeFamilyReport
	NativeSchemas   []receivingPlannerSchemaOutput
	NativeHistories []NativeHistoryReport
}

func receivingPlannerOutputBytes(t *testing.T, fixture string, archive *dumpformat.Archive, profile driver.RestoreImportProfile, when time.Time) []byte {
	t.Helper()
	plan, err := buildPlanWithProfile(archive, &profile, when)
	if err != nil {
		t.Fatal(err)
	}
	profileBytes, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	output := receivingPlannerOutput{
		Format: "receiving-plan-output/1", Fixture: fixture, ArchiveSHA256: archive.SHA256(),
		Profile: profileBytes, When: when.UTC().Format(time.RFC3339Nano),
		DerivedDigest: plan.derivedDigest, Blobs: plan.blobs, Allocators: plan.allocators,
		Sentinels: plan.sentinels, NativeFamilies: plan.nativeFamilies, NativeHistories: plan.nativeHistories,
	}
	if plan.records != nil {
		output.Records = make([]receivingPlannerRecordOutput, len(plan.records))
		for i, record := range plan.records {
			key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &record.ref})
			if err != nil {
				t.Fatal(err)
			}
			ref, err := json.Marshal(record.ref)
			if err != nil {
				t.Fatal(err)
			}
			output.Records[i] = receivingPlannerRecordOutput{key, ref, record.kind, record.body}
		}
	}
	if plan.derived != nil {
		output.Derived = make([]receivingPlannerDerivedOutput, len(plan.derived))
		for i, row := range plan.derived {
			output.Derived[i] = receivingPlannerDerivedOutput{row.Table, row.Key, []byte(row.Value)}
		}
	}
	if plan.nativeSchemas != nil {
		output.NativeSchemas = make([]receivingPlannerSchemaOutput, len(plan.nativeSchemas))
		for i, report := range plan.nativeSchemas {
			item := receivingPlannerSchemaOutput{
				Source: report.Source, Profile: report.Profile, SchemaDefaultCount: report.SchemaDefaultCount,
				TargetOmissions: report.TargetOmissions, OpaqueMetadataFields: report.OpaqueMetadataFields,
			}
			if report.PlugDefaults != nil {
				item.PlugDefaults = make([]receivingPlannerDefaultOutput, len(report.PlugDefaults))
				for j, value := range report.PlugDefaults {
					item.PlugDefaults[j] = receivingPlannerDefaultOutput{value.TargetKind, value.Field, []byte(value.Value), value.Count}
				}
			}
			output.NativeSchemas[i] = item
		}
	}
	raw, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// These are the original undeclared-output members of owning predecessor run
// 43049722835 at test-only child 10f22dd33523, sole parent 53742cc774c7. They
// contain actual unchanged-planner outputs, including its complete inputs.
func TestReceivingPlannerPublicOutputMatchesPredecessor(t *testing.T) {
	compressed, err := os.ReadFile("testdata/planner-predecessor-53742cc.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(compressed) != "e02f1f762c1b5d0f6ae64b19fc8772bee2d234650d363934b80204903f73aac4" {
		t.Fatal("frozen predecessor output container changed")
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal(readErr, closeErr)
	}
	originals := tarMembers(t, data)
	if len(originals) != 4 {
		t.Fatal("original public-output membership changed")
	}
	for _, fixture := range []struct {
		name, sha256 string
		complete     bool
		version      int
	}{
		{"portable-complete-false", "7c4d852f339d7b03af131d55481e4726b9a345630ec46029faba698c75f4db49", false, 0},
		{"portable-complete-true", "2505f2b9daa279e684ad91d53fed96126daf3cf2e76bb803edcf06704db92971", true, 0},
		{"native-own-version-66", "ac8ce532bd66333b539998b4eb7bda065bc10a3b07e38ee665b7524343bd1f14", false, 66},
		{"native-own-version-72", "ec572e6d46519756303c108e1d07c90c05cb2bc49e638d1eb253c682bb4f87a9", false, 72},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			original := originals[fixture.name+".json"]
			if hashBytes(original) != fixture.sha256 {
				t.Fatal("original predecessor output bytes changed")
			}
			var input struct {
				Format, Fixture, ArchiveSHA256, When string
				Profile                             []byte
			}
			if err := json.Unmarshal(original, &input); err != nil {
				t.Fatal(err)
			}
			var archive *dumpformat.Archive
			if fixture.version == 0 {
				_, archive = publicSourceFixture(t, fixture.complete)
			} else {
				archive, _ = actualOwnVersionArchive(t, fixture.version)
			}
			if input.Format != "receiving-plan-output/1" || input.Fixture != fixture.name || input.ArchiveSHA256 != archive.SHA256() {
				t.Fatal("original fixture/input identity changed")
			}
			var profile driver.RestoreImportProfile
			decoder := json.NewDecoder(bytes.NewReader(input.Profile))
			decoder.UseNumber()
			if err := decoder.Decode(&profile); err != nil {
				t.Fatal(err)
			}
			clock, err := time.Parse(time.RFC3339Nano, input.When)
			if err != nil || clock.IsZero() || profile.ArchiveDigest != archive.SHA256() {
				t.Fatal("original full request/clock binding changed", err)
			}
			actual := receivingPlannerOutputBytes(t, fixture.name, archive, profile, clock)
			if !bytes.Equal(original, actual) {
				t.Fatalf("actual planner output differs from predecessor: before=%s/%d after=%s/%d", fixture.sha256, len(original), hashBytes(actual), len(actual))
			}
			t.Logf("RECEIVING_PLANNER_BYTE_EQUAL fixture=%s archive=%s bytes=%d sha256=%s records-derived-blobs-allocators-reports=byte-identical", fixture.name, archive.SHA256(), len(actual), hashBytes(actual))
		})
	}
}
