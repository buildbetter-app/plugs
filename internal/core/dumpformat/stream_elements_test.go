package dumpformat

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestStreamJSONElementsVisitsArrayMembersWithoutContainerDecode(t *testing.T) {
	data := []byte(`[{"id":1,"value":["a",2]}, {"id":2,"value":{"ok":true}}]`)
	var got [][]byte
	if err := streamJSONElements(data, func(raw []byte) error {
		got = append(got, append([]byte(nil), raw...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0]) != `{"id":1,"value":["a",2]}` || string(got[1]) != `{"id":2,"value":{"ok":true}}` {
		t.Fatalf("raw elements = %q", got)
	}
}

func TestStreamJSONElementsRejectsNonArray(t *testing.T) {
	if err := streamJSONElements([]byte(`{"id":1}`), func([]byte) error { return nil }); err == nil {
		t.Fatal("non-array configuration was accepted")
	}
}

func TestStreamJSONElementsAppliesPerElementJSONBudget(t *testing.T) {
	const elementCount = 20000
	var data bytes.Buffer
	data.WriteByte('[')
	for i := 0; i < elementCount; i++ {
		if i != 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"row":"`)
		data.WriteString(strings.Repeat("x", 1024))
		data.WriteString(`"}`)
	}
	data.WriteString("]")
	if uint64(data.Len()) <= driver.NativeHeadRecordBytes {
		t.Fatalf("fixture must exceed one per-value limit: %d", data.Len())
	}
	count := 0
	if err := streamJSONElements(data.Bytes(), func(raw []byte) error {
		if _, err := driver.ReverseJSONElementFootprint(raw, 1); err != nil {
			return err
		}
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != elementCount {
		t.Fatalf("visited %d elements, want %d", count, elementCount)
	}
}

func TestStreamJSONElementsReportsOversizeElement(t *testing.T) {
	payload := strings.NewReader(strings.Repeat("x", driver.ReverseJSONElementBytes))
	input := io.MultiReader(strings.NewReader(`[{},{"private-key":"`), payload, strings.NewReader(`"}]`))
	err := visitJSONElements(input, true, configAccounting{}, func(raw []byte, _ uint64) error {
		if string(raw) != "{}" {
			t.Fatal("oversize element reached visitor")
		}
		return nil
	})
	var limit *driver.ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.Resource != "value_bytes" ||
		limit.Measured != driver.ReverseJSONElementBytes+1 || limit.Limit != driver.ReverseJSONElementBytes ||
		limit.Member != SourceConfigMember || limit.JSONPath != "$[]" || limit.ElementIndex == nil || *limit.ElementIndex != 1 ||
		strings.Contains(err.Error(), "private-key") {
		t.Fatalf("oversize element error = %v", err)
	}
}

func TestStreamConfigReaderAcceptsSingleTwentyMiBElement(t *testing.T) {
	const size = 20 << 20
	input := io.MultiReader(
		strings.NewReader(`[{"input":{"family":"project-config","path":"/public/p49","state":"json-config","precedence":0},"values":{"row":"`),
		strings.NewReader(strings.Repeat("x", size)),
		strings.NewReader(`"}}]`))
	budget := NewNativeDeltaBudget(0)
	var visited int
	err := streamConfigReader(input, true, configAccounting{decoded: budget}, func(entry externalEntry, raw []byte) error {
		visited++
		value, ok := entry.Values["row"].(string)
		if !ok || len(value) != size || len(raw) <= size {
			t.Fatal("streamed config data changed")
		}
		return nil
	})
	if err != nil || visited != 1 || budget.Usage().CurrentAccountedDecodedBytes != 0 {
		t.Fatalf("large element: visited=%d, err=%v", visited, err)
	}
}

func TestStreamConfigEntriesPreservesOrdinaryArchiveValueRange(t *testing.T) {
	leaf := strings.Repeat("x", driver.NativeHeadRecordBytes)
	data := []byte(`[{"input":{"family":"project-config","path":"/public/large","state":"json-config","precedence":0},"values":{"row":"` + leaf + `"}}]`)
	visited := 0
	err := streamConfigEntries(data, func(entry externalEntry, _ []byte) error {
		if entry.Input.Path != "/public/large" || entry.Values["row"] != leaf {
			t.Fatal("ordinary entry changed")
		}
		visited++
		return nil
	})
	if err != nil || visited != 1 {
		t.Fatalf("ordinary config read: %d entries, %v", visited, err)
	}
}

func TestStreamJSONElementsSharesCaptureDecodedBudget(t *testing.T) {
	const limit = 512 << 10
	decoded := NewNativeDeltaBudget(limit)
	const retained = limit - (64 << 10)
	if err := decoded.ChargeBytes("head_member_payloads", retained); err != nil {
		t.Fatal(err)
	}
	data := []byte(`[{"input":{"family":"project-config","path":"/public/head-window","state":"json-config","precedence":0},"values":{"row":"` + strings.Repeat("x", 16<<10) + `"}}]`)
	err := streamConfigReader(bytes.NewReader(data), true, configAccounting{decoded: decoded}, func(externalEntry, []byte) error {
		t.Fatal("oversized live config reached its visitor")
		return nil
	})
	var refusal *NativeDeltaDecodedLimitError
	if !errors.As(err, &refusal) || refusal.LimitBytes != limit || refusal.MeasuredBytes <= limit {
		t.Fatalf("expected shared decoded budget refusal, got %v", err)
	}
	if got := decoded.Usage().CurrentAccountedDecodedBytes; got != retained {
		t.Fatalf("failed pass retained %d shared bytes", got)
	}
}

func TestStreamConfigReaderPreservesStrictWireChecks(t *testing.T) {
	const entry = `{"identity":{"store_id":"public","project":"ops"},"input":{"family":"project-config","path":"/public/p49","state":"json-config","precedence":0},"values":{"nested":[null,true,false,1e20,{"path":"\ud83d\ude00"}]}}`
	decode := func(data string, bounded bool) (externalEntry, error) {
		var result externalEntry
		err := streamConfigReader(strings.NewReader("["+data+"]"), bounded, configAccounting{}, func(got externalEntry, _ []byte) error {
			result = got
			return nil
		})
		return result, err
	}
	want, err := decode(entry, false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decode(entry, true)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("streamed config differs from strict archive reader: %+v, %v", got, err)
	}
	for _, tc := range []struct{ name, from, to string }{
		{"duplicate dynamic key", `"path":"\ud83d\ude00"`, `"p\u0061th":false,"path":"\ud83d\ude00"`},
		{"unknown fixed field", `"family":`, `"Family":`},
		{"missing required field", `,"precedence":0`, ``},
		{"fractional precedence", `"precedence":0`, `"precedence":0.5`},
		{"null typed value", `"path":"/public/p49"`, `"path":null`},
		{"invalid identity", `"project":"ops"`, `"project":" ops"`},
		{"scalar identity", `{"store_id":"public","project":"ops"}`, `true`},
		{"array identity", `{"store_id":"public","project":"ops"}`, `[]`},
		{"empty identity", `{"store_id":"public","project":"ops"}`, `{}`},
		{"wrong identity type", `"project":"ops"`, `"project":0`},
		{"null identity field", `"project":"ops"`, `"project":null`},
		{"unknown identity field", `"project":"ops"`, `"project":"ops","extra":false`},
		{"unpaired surrogate", `\ud83d\ude00`, `\ud83d`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := strings.Replace(entry, tc.from, tc.to, 1)
			if bad == entry {
				t.Fatal("fixture mutation did not alter wire")
			}
			for _, bounded := range []bool{false, true} {
				if _, err := decode(bad, bounded); err == nil {
					t.Fatalf("bounded=%t accepted an invalid configuration entry", bounded)
				}
			}
		})
	}
}
