package postgres

import "encoding/hex"

// selectionFactArrayArgs transports the existing scalar representation without
// expanding it into JSON and asking PostgreSQL to parse it back into columns.
// Every byte field was hex-encoded by selectionFactsRows. Keep nil optional
// values distinct from present empty bytes, and retain the original row order.
// The caller still owns the bounded statement size and transaction boundaries.
func selectionFactArrayArgs(facts []selectionDBFact) ([]any, error) {
	const byteColumns = 11
	columns := make([][][]byte, byteColumns)
	for i := range columns {
		columns[i] = make([][]byte, len(facts))
	}
	ordinals := make([]int64, len(facts))
	active := make([]bool, len(facts))
	depths := make([]int64, len(facts))
	for i, fact := range facts {
		encoded := [byteColumns]*string{
			&fact.RowKey, fact.Ref, fact.Target, &fact.Subject, &fact.Scope,
			&fact.ScopeKind, fact.ProjectName, &fact.Role, &fact.Name,
			&fact.Value, &fact.LinkKind,
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
		ordinals[i], active[i], depths[i] = int64(fact.Ordinal), fact.Active, fact.Depth
	}
	// This order is the existing selectionFactColumns order. All arrays have
	// exactly len(facts) elements; unnest cannot pad or multiply a row.
	return []any{
		columns[0], ordinals, columns[1], columns[2], columns[3], columns[4],
		columns[5], columns[6], columns[7], columns[8], columns[9], active,
		depths, columns[10],
	}, nil
}

const selectionFactArrayRelation = `unnest($3::bytea[],$4::bigint[],$5::bytea[],$6::bytea[],$7::bytea[],$8::bytea[],$9::bytea[],$10::bytea[],$11::bytea[],$12::bytea[],$13::bytea[],$14::boolean[],$15::bigint[],$16::bytea[])`
