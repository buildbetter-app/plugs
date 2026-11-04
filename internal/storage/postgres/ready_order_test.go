package postgres

import (
	"github.com/buildbetter-app/plugs/internal/storage/conformance/readyorder"
	"testing"
)

func TestReadyOrderRealWriterCertified(t *testing.T) { readyorder.RunWriter(t, factory) }
