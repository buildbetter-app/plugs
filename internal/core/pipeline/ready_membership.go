package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// ReadyMembershipLeaf is the full semantic leaf for both selection projection
// publication and maintained Ready order. Revision is the effective SOURCE
// revision: startup uses the imported/pinned source revision; a changed source
// uses the actual H+1 allocated after the complete publication lock census.
func ReadyMembershipLeaf(doc Document, revision model.Revision) (string, error) {
	status, _ := doc.Fields["status"].(string)
	item := driver.ReadyMembershipItem{ID: doc.ID, Type: doc.Type, Project: doc.Project, Status: status, Revision: revision, Fields: doc.Fields}
	leaf, err := driver.ReadyOrderLeafFor(item, 0)
	if err != nil {
		return "", err
	}
	return leaf.Digest, nil
}

// BindReadySelectionRows binds a core-created typed counting row to the full
// semantic item. Re-encode its logical bytes and value digest together. The
// source observation envelope is deliberately unchanged: before Commit it
// still guards the old source revision; the semantic leaf binds allocated H+1.
// It acquires no locks and introduces no driver JSON decoding or late reads.
func BindReadySelectionRows(doc Document, revision model.Revision, rows []driver.DerivedRow) error {
	leaf, err := ReadyMembershipLeaf(doc, revision)
	if err != nil {
		return err
	}
	for i, row := range rows {
		if row.Index == nil || row.Index.Bead == nil {
			continue
		}
		if row.Index.Bead.Ref.ID != doc.ID {
			return fmt.Errorf("Ready membership source identity mismatch")
		}
		index := *row.Index
		bead := *index.Bead
		bead.MembershipLeaf = leaf
		index.Bead = &bead
		body, err := json.Marshal(bead)
		if err != nil {
			return err
		}
		row.Value = body
		index.ValueDigest = driver.SelectionDigest(body)
		row.Index = &index
		if err = index.Validate(index.Row.Table, row); err != nil {
			return err
		}
		rows[i] = row
	}
	return nil
}
