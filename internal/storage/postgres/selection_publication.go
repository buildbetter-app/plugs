package postgres

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) prepareSelectionPublication(ctx context.Context) error {
	if err := t.validateSelectionWrites(ctx, t.derived); err != nil {
		return err
	}
	if err := t.sql.QueryRowContext(ctx, `SELECT selection_virgin FROM plug_postgres.meta WHERE singleton`).Scan(&t.selectionVirgin); err != nil {
		return err
	}
	return t.prepareSelectionProof(ctx)
}
func (t *transaction) committedSelectionState(state driver.LockState, revision model.Revision) driver.LockState {
	if state.Derived != nil {
		row := state.Derived.Clone()
		state.Derived = &row
		if row.Index != nil && t.selectionSourceChanged(row.Index.Source.Key) {
			row.Index.Source.Revision = revision
		}
	}
	return state
}

type selectionSourceHeader struct {
	Key      string `json:"key"`
	Revision string `json:"revision"`
	Present  bool   `json:"present"`
	Digest   string `json:"digest"`
	Kind     string `json:"kind"`
}
type selectionDBFact struct {
	RowKey                            string `json:"row_key"`
	Ordinal                           int    `json:"ordinal"`
	Ref, Target                       *string
	Subject, Scope, Role, Name, Value string
	ScopeKind                         string
	ProjectName                       *string
	Active                            bool
	Depth                             int64
	LinkKind                          string
}

const selectionFactJSONLayout = `row_key text,ordinal integer,"Ref" text,"Target" text,"Subject" text,"Scope" text,"ScopeKind" text,"ProjectName" text,"Role" text,"Name" text,"Value" text,"Active" boolean,"Depth" bigint,"LinkKind" text`
const selectionFactJSONColumns = `decode(row_key,'hex') row_key,ordinal,decode("Ref",'hex') ref_key,decode("Target",'hex') target_key,decode("Subject",'hex') subject,decode("Scope",'hex') scope,decode("ScopeKind",'hex') scope_kind,decode("ProjectName",'hex') project_name,decode("Role",'hex') role,decode("Name",'hex') name,decode("Value",'hex') value,"Active" active,"Depth" depth,decode("LinkKind",'hex') link_kind`
const selectionFactColumns = `row_key,ordinal,ref_key,target_key,subject,scope,scope_kind,project_name,role,name,value,active,depth,link_kind`

func selectionFactsRows(id string, set *driver.SelectionFacts) []selectionDBFact {
	rows := make([]selectionDBFact, 0, len(set.Rows))
	for i, f := range set.Rows {
		r := selectionDBFact{RowKey: selectionHex(id), Ordinal: i, Subject: selectionHex(f.Subject), Scope: selectionHex(f.Scope), Role: selectionHex(f.Role), Name: selectionHex(f.Name), Value: selectionHex(f.Value), Active: f.Active, Depth: int64(f.Depth)}
		r.ScopeKind = selectionHex(string(f.ScopeKind))
		if f.ProjectName != nil {
			r.ProjectName = selectionHexPtr(*f.ProjectName)
		}
		if f.Ref != nil {
			r.Ref = selectionHexPtr(selectionRefKey(*f.Ref))
		}
		if f.Target != nil {
			r.Target = selectionHexPtr(selectionRefKey(*f.Target))
		}
		if f.Link != nil {
			r.Target = selectionHexPtr(selectionRefKey(f.Link.To))
			r.LinkKind = selectionHex(f.Link.Kind)
		}
		rows = append(rows, r)
	}
	return rows
}

// Publish only through the ordinary fenced SQL transaction. Header-only source
// observations allow selectors to reject stale facts without loading source
// bodies. Existing logical states and historical annotations remain authoritative.
func (t *transaction) writeSelection(ctx context.Context, revision model.Revision) error {
	inc := t.meta.Incarnation
	if t.restore != nil {
		inc = t.restore.Incarnation
	}
	changed := make(map[string]selectionSourceHeader)
	for id, r := range t.records {
		h := selectionSourceHeader{Key: selectionHex(id), Revision: revision.String(), Present: r != nil}
		if r != nil {
			h.Digest = selectionHex(driver.SelectionDigest(r.Body))
			h.Kind = r.Kind
		} else {
			h.Kind = t.identities[id].Kind
		}
		changed[id] = h
	}
	for id, s := range t.derived {
		h := selectionSourceHeader{Key: selectionHex(id), Revision: revision.String(), Present: s.Present}
		if s.Present && s.Derived != nil {
			h.Digest = selectionHex(driver.SelectionDigest(s.Derived.Value))
		}
		changed[id] = h
	}
	for id := range t.bumps {
		changed[id] = selectionSourceHeader{Key: selectionHex(id), Revision: revision.String(), Present: true}
	}
	indexes := make([]selectionDBRow, 0, len(t.derived))
	facts := []selectionDBFact{}
	rowKeys := make([][]byte, 0, len(t.derived))
	for id, s := range t.derived {
		rowKeys = append(rowKeys, []byte(id))
		if !s.Present || s.Derived == nil || s.Derived.Index == nil {
			continue
		}
		published := t.committedSelectionState(s, revision)
		x := published.Derived.Index
		row, err := selectionRow(id, *published.Derived)
		if err != nil {
			return err
		}
		// Only durable publication needs the binary owned annotation. Staged
		// thin relations and coverage checks transport semantic columns alone.
		var annotation bytes.Buffer
		if err := gob.NewEncoder(&annotation).Encode(x); err != nil {
			return err
		}
		row.Annotation = selectionHex(annotation.String())
		indexes = append(indexes, row)
		if x.Facts != nil {
			facts = append(facts, selectionFactsRows(id, x.Facts)...)
		}
		sourceID, _ := keyID(x.Source.Key)
		if _, ok := changed[sourceID]; !ok {
			changed[sourceID] = selectionSourceHeader{Key: selectionHex(sourceID), Revision: x.Source.Revision.String(), Present: x.Source.Present, Digest: selectionHex(x.Source.Digest)}
		}
	}
	// Source kind is envelope metadata, never inferred from an encoded key or
	// a document. Preserve it when an index alone introduces a source header.
	ids := make([][]byte, 0, len(changed))
	for id := range changed {
		ids = append(ids, []byte(id))
	}
	if len(ids) > 0 {
		rows, err := t.sql.QueryContext(ctx, `SELECT key,kind FROM plug_postgres.records WHERE key=ANY($1::bytea[])`, ids)
		if err != nil {
			return err
		}
		for rows.Next() {
			var key []byte
			var kind string
			if err = rows.Scan(&key, &kind); err != nil {
				break
			}
			h := changed[string(key)]
			h.Kind = kind
			changed[string(key)] = h
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err != nil {
			return err
		}
	}
	headers := make([]selectionSourceHeader, 0, len(changed))
	sourceKeys := make([][]byte, 0, len(changed))
	for id, h := range changed {
		headers = append(headers, h)
		sourceKeys = append(sourceKeys, []byte(id))
	}
	if len(headers) > 0 {
		if _, err := t.sql.ExecContext(ctx, `UPDATE plug_postgres.selection_sources SET end_revision=$2 WHERE incarnation=$1 AND end_revision IS NULL AND key=ANY($3::bytea[])`, inc, revision.String(), sourceKeys); err != nil {
			return err
		}
		for start := 0; start < len(headers); start += publicationBatchRows {
			end := min(start+publicationBatchRows, len(headers))
			raw, _ := json.Marshal(headers[start:end])
			if _, err := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.selection_sources(incarnation,key,start_revision,revision,present,digest,kind) SELECT $1,decode(key,'hex'),$2,revision::numeric,present,decode(digest,'hex'),kind FROM jsonb_to_recordset($3::jsonb) AS s(key text,revision text,present boolean,digest text,kind text)`, inc, revision.String(), string(raw)); err != nil {
				return err
			}
		}
	}
	if len(rowKeys) > 0 {
		if _, err := t.sql.ExecContext(ctx, `UPDATE plug_postgres.selection_rows SET end_revision=$2 WHERE incarnation=$1 AND end_revision IS NULL AND row_key=ANY($3::bytea[])`, inc, revision.String(), rowKeys); err != nil {
			return err
		}
	}
	for start := 0; start < len(indexes); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(indexes))
		if _, err := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.selection_rows(incarnation,start_revision,`+selectionColumns+`,annotation) SELECT $1,$2,`+selectionJSONColumns+`,decode(annotation,'hex') FROM jsonb_to_recordset($3::jsonb) AS p(`+selectionJSONLayout+`)`, inc, revision.String(), selectionJSON(indexes[start:end])); err != nil {
			return err
		}
	}
	for start := 0; start < len(facts); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(facts))
		if _, err := t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.selection_facts(incarnation,start_revision,`+selectionFactColumns+`) SELECT $1,$2,`+selectionFactJSONColumns+` FROM jsonb_to_recordset($3::jsonb) AS f(`+selectionFactJSONLayout+`)`, inc, revision.String(), selectionJSON(facts[start:end])); err != nil {
			return err
		}
	}
	return t.writeSelectionCoverage(ctx, revision, inc)
}
