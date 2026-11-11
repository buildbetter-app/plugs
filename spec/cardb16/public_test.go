package cardb20

import "testing"

func TestB16PublicNativeCounterpartDurableLabels(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b16DurableVector(t, engine) })
	}
}

func TestB16PublicActiveLabelNoopAndTwoPlaneRename(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b16ActiveVector(t, engine) })
	}
}
