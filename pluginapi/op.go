package pluginapi

import (
	"encoding/json"
	"fmt"
)

// Op transports the flat operation object without depending on the internal
// operation union. The host validates its complete schema before dispatch.
func (o Op) MarshalJSON() ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := decodeJSON(o.Payload, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("operation payload must be a JSON object")
	}
	var kind string
	if err := json.Unmarshal(fields["kind"], &kind); err != nil || kind == "" || kind != o.Kind {
		return nil, fmt.Errorf("operation kind does not match payload")
	}
	return append([]byte(nil), o.Payload...), nil
}

func (o *Op) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := decodeJSON(data, &fields); err != nil || fields == nil {
		return fmt.Errorf("operation must be a JSON object")
	}
	var kind string
	if err := json.Unmarshal(fields["kind"], &kind); err != nil || kind == "" {
		return fmt.Errorf("operation requires a string kind")
	}
	*o = Op{Kind: kind, Payload: append(json.RawMessage(nil), data...)}
	return nil
}
