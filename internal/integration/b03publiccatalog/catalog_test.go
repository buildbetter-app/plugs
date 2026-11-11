package cardb20

import "testing"

func TestB03PublicNonemptyCatalogControls(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b03PlugCatalog(t, engine) })
	}
}
