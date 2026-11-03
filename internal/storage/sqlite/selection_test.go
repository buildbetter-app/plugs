package sqlite

import (
	"context"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"path/filepath"
	"testing"
)

func TestSelectionUnsupported(t *testing.T) {
	conformance.RunSelectionRefusal(t, func(t *testing.T) driver.Store {
		s, err := (Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "selection.db")})
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
