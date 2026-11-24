package dumpformat

import (
	"bytes"
	"errors"
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
		if _, err := driver.ReverseJSONFootprint(raw, 1); err != nil {
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
	data := []byte(`[{"row":"` + strings.Repeat("x", driver.NativeHeadRecordBytes) + `"}]`)
	err := streamJSONElements(data, func(raw []byte) error {
		_, err := driver.ReverseJSONFootprint(raw, 1)
		return err
	})
	var limit *driver.ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.Resource != "value_bytes" || limit.Measured <= limit.Limit {
		t.Fatalf("oversize element error = %v", err)
	}
}
