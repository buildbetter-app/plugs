package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) driver.Store {
		t.Helper()
		s, err := (Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "store.db")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	})
}
