package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestStagedReadOverlay(t *testing.T) {
	conformance.RunStagedReadOverlay(t, func(t *testing.T) driver.Store {
		t.Helper()
		store, err := (Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "staged.db")})
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}
