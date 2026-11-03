package postgres

import (
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"testing"
)

func TestSelectionUnsupported(t *testing.T) { conformance.RunSelectionRefusal(t, factory) }
