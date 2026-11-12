package cardb20

import "testing"

func TestB07PublicDependencyEffectSequences(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { effectPublic(t, engine) })
	}
}

func TestB07PublicParentSchedulingSequences(t *testing.T) {
	for _, engine := range []string{"memory","sqlite","postgres"} {
		for _, route := range []string{"durable","durable-active","active-durable","active-active"} {
			t.Run(engine+"/"+route,func(t *testing.T) { effectParentPublic(t,engine,route) })
		}
	}
}


func TestB07PublicExternalTargetSequences(t *testing.T) {
	for _, engine := range []string{"memory","sqlite","postgres"} {
		t.Run(engine,func(t *testing.T) { effectExternalPublic(t,engine) })
	}
}
