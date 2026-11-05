package driver

import (
	"bytes"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func TestReadyOrderPublicationOwnsEncodedSnapshot(t *testing.T) {
	var absent ReadyOrderPublication
	if !absent.IsZero() {
		t.Fatal("zero publication is present")
	}
	if _, err := absent.Data(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("absent publication decoded: %v", err)
	}
	fields := map[string]any{"title": "original"}
	data := ReadyOrderPublicationData{
		Incarnation: "inc", Base: model.Natural64(0), Revision: model.Natural64(1),
		Mutations: []ReadyOrderMutation{{
			Ref: model.RefByID("row", model.Natural64(0)), SourceDigest: SelectionDigest([]byte("source")),
			Item: &ReadyMembershipItem{ID: "row", Revision: model.Natural64(1), Fields: fields},
		}},
		Added: []DerivedRow{{Key: "ready", Value: []byte("snapshot")}},
	}
	publication, err := NewReadyOrderPublication(data)
	if err != nil || publication.IsZero() {
		t.Fatalf("valid publication absent: %v", err)
	}
	digest := publication.Digest()
	fields["title"] = "changed input"
	data.Added[0].Value[0] = 'X'
	first, err := publication.Data()
	if err != nil {
		t.Fatal(err)
	}
	if first.Mutations[0].Item.Fields["title"] != "original" || !bytes.Equal(first.Added[0].Value, []byte("snapshot")) {
		t.Fatal("constructor input changed the encoded publication")
	}
	first.Mutations[0].Item.Fields["title"] = "changed output"
	first.Added[0].Key = "changed key"
	first.Added[0].Value[0] = 'Y'
	second, err := publication.Data()
	if err != nil {
		t.Fatal(err)
	}
	if publication.IsZero() || publication.Digest() != digest || second.Mutations[0].Item.Fields["title"] != "original" || second.Added[0].Key != "ready" || !bytes.Equal(second.Added[0].Value, []byte("snapshot")) {
		t.Fatal("decoded aliases changed the immutable publication")
	}
}
