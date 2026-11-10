package postgres

import (
	"github.com/buildbetter-app/plugs/internal/storage/conformance/readyorder"
	"testing"
)

func TestReadyOrderRealWriterMetadata(t *testing.T) { readyorder.RunWriterMetadata(t, factory) }
