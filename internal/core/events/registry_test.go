package events

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type registryProjector func()
type registryHandler func(context.Context, Delivery) error

func (registryProjector) Apply(_ context.Context, _ ChangeDelivery) error { return nil }
func (fn registryHandler) Deliver(ctx context.Context, item Delivery) error {
	return fn(ctx, item)
}

func projectionBinding(id string) Binding {
	return Binding{
		ID:        id,
		Class:     Projection,
		Filter:    Filter{Types: []string{"bead.created"}, Resources: []string{"bead/pl-a"}},
		Projector: registryProjector(func() {}),
	}
}

func TestProjectionRegistryRejectsInvalidAndDuplicateBindings(t *testing.T) {
	registry := NewProjectionRegistry()
	if err := registry.Register(Binding{ID: "listener", Class: Listener, Handler: registryHandler(func(context.Context, Delivery) error { return nil })}); err == nil {
		t.Fatal("non-projection binding registered")
	}
	if err := registry.Register(Binding{ID: "missing-projector", Class: Projection}); err == nil {
		t.Fatal("projection without a projector registered")
	}
	if err := registry.Register(projectionBinding("same")); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(projectionBinding("same")); !errors.Is(err, ErrProjectionDuplicate) {
		t.Fatalf("duplicate error=%v", err)
	}
	if err := registry.Remove("missing"); !errors.Is(err, ErrProjectionNotFound) {
		t.Fatalf("missing removal error=%v", err)
	}
}

func TestProjectionRegistryReturnsDetachedSortedSnapshots(t *testing.T) {
	registry := NewProjectionRegistry()
	first := projectionBinding("zulu")
	second := projectionBinding("alpha")
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(second); err != nil {
		t.Fatal(err)
	}
	first.Filter.Types[0] = "forged"
	first.Filter.Resources[0] = "forged/resource"
	bindings := registry.Bindings()
	if got := []string{bindings[0].ID, bindings[1].ID}; !reflect.DeepEqual(got, []string{"alpha", "zulu"}) {
		t.Fatalf("ids=%v", got)
	}
	if got := bindings[1].Filter; !reflect.DeepEqual(got, Filter{Types: []string{"bead.created"}, Resources: []string{"bead/pl-a"}}) {
		t.Fatalf("stored filter was aliased: %+v", got)
	}
	bindings[0].Filter.Types[0] = "mutated-snapshot"
	stored, ok := registry.Get("alpha")
	if !ok || stored.Filter.Types[0] != "bead.created" {
		t.Fatalf("snapshot mutation leaked: %+v %v", stored, ok)
	}
	if err := registry.Remove("alpha"); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Get("alpha"); ok {
		t.Fatal("removed projection remained registered")
	}
}
