package cardb20

import "testing"

func TestB14DefaultPublicGateAndHistory(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b14Public(t, engine) })
	}
}

func TestB14DefaultPublicMixedGateAndWaits(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b14MixedPublic(t, engine) })
	}
}
