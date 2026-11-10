package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

// writeSelectionSourceHeaders keeps the existing bounded publication statements
// while transporting the validated header scalars without a JSON recordset.
// Equal-length arrays preserve every row, including empty non-NULL byte values;
// SQL still casts revision text over the full unsigned revision domain.
func writeSelectionSourceHeaders(ctx context.Context, q sourceindex.SQL, incarnation string, at model.Revision, headers []selectionSourceHeader) error {
	for start := 0; start < len(headers); start += publicationBatchRows {
		end := min(start+publicationBatchRows, len(headers))
		// The old JSON conversion replaces invalid UTF-8 and refuses NULs.
		// Keep that exact conversion for exceptional kind text instead of
		// changing normalization or the SQL refusal through a text array.
		legacyKind := false
		for _, header := range headers[start:end] {
			if !utf8.ValidString(header.Kind) || strings.IndexByte(header.Kind, 0) >= 0 {
				legacyKind = true
				break
			}
		}
		if legacyKind {
			raw, _ := json.Marshal(headers[start:end])
			if _, err := q.ExecContext(ctx, `INSERT INTO plug_postgres.selection_sources(incarnation,key,start_revision,revision,present,digest,kind) SELECT $1,decode(key,'hex'),$2,revision::numeric,present,decode(digest,'hex'),kind FROM jsonb_to_recordset($3::jsonb) AS s(key text,revision text,present boolean,digest text,kind text)`, incarnation, at.String(), string(raw)); err != nil {
				return err
			}
			continue
		}
		keys, digests := make([][]byte, end-start), make([][]byte, end-start)
		revisions, kinds := make([]string, end-start), make([]string, end-start)
		present := make([]bool, end-start)
		for i, header := range headers[start:end] {
			var err error
			if keys[i], err = hex.DecodeString(header.Key); err != nil {
				return err
			}
			if digests[i], err = hex.DecodeString(header.Digest); err != nil {
				return err
			}
			revisions[i], present[i], kinds[i] = header.Revision, header.Present, header.Kind
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO plug_postgres.selection_sources(incarnation,key,start_revision,revision,present,digest,kind) SELECT $1,key,$2,revision::numeric,present,digest,kind FROM unnest($3::bytea[],$4::text[],$5::boolean[],$6::bytea[],$7::text[]) AS s(key,revision,present,digest,kind)`, incarnation, at.String(), keys, revisions, present, digests, kinds); err != nil {
			return err
		}
	}
	return nil
}
