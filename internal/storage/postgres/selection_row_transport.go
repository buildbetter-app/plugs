package postgres

import "encoding/hex"

// selectionRowArrayArgs transports the already validated scalar representation
// without expanding it into JSON and parsing the recordset again in PostgreSQL.
// Every array has one entry per original row. Optional nil and present empty
// bytes remain distinct, and the owned annotation bytes pass through unchanged.
func selectionRowArrayArgs(rows []selectionDBRow) ([]any, error) {
	const byteColumns = 13
	columns := make([][][]byte, byteColumns)
	for i := range columns {
		columns[i] = make([][]byte, len(rows))
	}
	revisions := make([]string, len(rows))
	present := make([]bool, len(rows))
	templates := make([]*bool, len(rows))
	priorities := make([]*int64, len(rows))
	factKinds := make([]*string, len(rows))
	families := make([]string, len(rows))
	sourceKinds := make([]string, len(rows))
	sourceTables := make([]string, len(rows))
	layouts := make([]int64, len(rows))
	annotations := make([][]byte, len(rows))
	for i, row := range rows {
		encoded := [byteColumns]*string{
			&row.RowKey, &row.SourceKey, &row.SourceDigest, row.BeadKey,
			row.Ref, row.ID, row.Type, row.Project, row.Parent, row.Status,
			row.Title, row.Leaf, &row.ValueDigest,
		}
		for column, value := range encoded {
			if value == nil {
				continue
			}
			decoded, err := hex.DecodeString(*value)
			if err != nil {
				return nil, err
			}
			columns[column][i] = decoded
		}
		revisions[i], present[i] = row.SourceRevision, row.SourcePresent
		templates[i], priorities[i], factKinds[i] = row.Template, row.Priority, row.FactKind
		families[i], sourceKinds[i], sourceTables[i] = row.FamilyTable, row.SourceKind, row.SourceTable
		layouts[i], annotations[i] = int64(row.Layout), row.Annotation
	}
	// Preserve selectionColumns order, followed by the binary annotation. The
	// text revision is cast by SQL, retaining the full unsigned revision domain.
	return []any{
		columns[0], columns[1], revisions, present, columns[2], columns[3],
		columns[4], columns[5], columns[6], columns[7], columns[8], columns[9],
		columns[10], templates, priorities, columns[11], factKinds, families,
		sourceKinds, sourceTables, columns[12], layouts, annotations,
	}, nil
}

const selectionRowArrayColumns = `row_key,source_key,source_revision::numeric,source_present,source_digest,bead_key,ref,bead_id,bead_type,project,parent_key,status,title,template_value,priority,leaf,fact_kind,family_table,source_kind,source_table,value_digest,layout,annotation`

const selectionRowArrayRelation = `unnest($3::bytea[],$4::bytea[],$5::text[],$6::boolean[],$7::bytea[],$8::bytea[],$9::bytea[],$10::bytea[],$11::bytea[],$12::bytea[],$13::bytea[],$14::bytea[],$15::bytea[],$16::boolean[],$17::bigint[],$18::bytea[],$19::text[],$20::text[],$21::text[],$22::text[],$23::bytea[],$24::bigint[],$25::bytea[])`
