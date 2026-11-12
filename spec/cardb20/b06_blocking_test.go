package cardb20

import "testing"

func TestB06MixedBlockingDiagnostics(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b06RunBlockingPlug(t, engine) })
	}
}
