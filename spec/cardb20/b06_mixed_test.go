package cardb20

import "testing"

func TestB06MixedPlanePublicCycles(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b06RunMixedPlug(t, engine) })
	}
}
