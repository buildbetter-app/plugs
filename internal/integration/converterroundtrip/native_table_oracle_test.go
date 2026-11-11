package converterroundtrip

import (
	"encoding/json"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Exact predecessor table serializer remains only an independent test oracle.
type nativeTableBody struct {
	Source  dumpformat.SourceIdentity `json:"source_identity"`
	Table   string                    `json:"table"`
	History string                    `json:"history"`
	Columns []string                  `json:"columns"`
	Rows    []dumpformat.Row          `json:"rows"`
}

func nativeTableRecord(identity dumpformat.SourceIdentity, table, history string, value dumpformat.NativeTable) (driver.Record, error) {
	tuple, err := json.Marshal([]string{identity.StoreID, identity.Project, table})
	if err != nil {
		return driver.Record{}, err
	}
	prefix := driver.NativeTableHistoryRecordPrefix
	if history != "retained-native-commit" {
		prefix = "source/native-current/"
	}
	body, err := json.Marshal(nativeTableBody{Source: identity, Table: table, History: history, Columns: value.Columns, Rows: value.Rows})
	return driver.Record{Ref: model.RefByID(prefix+hashBytes(tuple), model.Natural64(0)), Kind: "beads-archive", Body: body}, err
}
