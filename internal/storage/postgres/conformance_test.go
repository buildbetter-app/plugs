package postgres

import (
	"context"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func factory(t *testing.T) driver.Store {
	t.Helper()
	s, err := Driver{}.Open(context.Background(), driver.Config{DSN: freshDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func TestSharedConformance(t *testing.T) { conformance.Run(t, factory) }
