package embedded

import (
	"database/sql/driver"
	"math"
	"testing"
	"time"
)

type opaquePhysicalValue struct{ internal any }

type physicalValuer struct{}

func (physicalValuer) Value() (driver.Value, error) { return "hidden conversion", nil }

func TestPhysicalDriverCellBoundary(t *testing.T) {
	t.Run("unsupported_objects_refuse", func(t *testing.T) {
		for _, value := range []any{opaquePhysicalValue{internal: map[string]int{"x": 1}}, &opaquePhysicalValue{}, map[string]int{"x": 1}, []any{1}, physicalValuer{}} {
			if _, err := physicalCell(value); err == nil {
				t.Errorf("non-scalar object escaped boundary: %T", value)
			}
		}
	})
	t.Run("binary_buffer_owned", func(t *testing.T) {
		original := []byte{0, 255}
		value, err := physicalCell(original)
		if err != nil {
			t.Fatal(err)
		}
		original[0] = 9
		if string(value.([]byte)) != string([]byte{0, 255}) {
			t.Fatal("driver returned an engine-owned binary buffer")
		}
	})
	t.Run("exact_scalar_and_unsigned_max", func(t *testing.T) {
		for _, value := range []any{nil, true, "text", int8(-1), int64(math.MinInt64), uint64(math.MaxUint64), float64(0.5), time.Unix(12, 345).UTC()} {
			got, err := physicalCell(value)
			if err != nil || got != value {
				t.Errorf("detached scalar changed: type=%T error=%v", value, err)
			}
		}
	})
}
