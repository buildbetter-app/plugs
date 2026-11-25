package driver

import (
	"bytes"
	"encoding/json"
)

// SourcePlaneReverseImageLimits bounds the existing workflow reverse wire
// sections. The binary plan can expand through JSON escaping and raw source
// records can also expand through base64 and repeated identity fields.
func SourcePlaneReverseImageLimits() (plan, records, mutable, aggregate uint64) {
	raw := uint64(SourcePlaneMaxPlanBytes)
	escape := uint64(len(`\u0000`))
	plan = raw * escape
	records = raw*escape + ((raw+2)/3)*4 + uint64(SourcePlaneMaxListEntries)*uint64(len(`{"Kind":"","Project":"","Table":"","ID":"","Raw":""},`))
	mutable = raw
	framing := uint64(len(`{"runs":,"items":,"receipts":,"audit":,"source":{"schema":"workflow/source-state/1","entities":,"relations":,"origins":,"records":}}`))
	return plan, records, mutable, plan + records + mutable*4 + framing
}

// SourcePlaneCheckpointEnvelopeBytes is the aggregate ceiling of the
// workflow reverse wire plus room for its typed origin and revision.
func SourcePlaneCheckpointEnvelopeBytes() uint64 {
	_, _, _, image := SourcePlaneReverseImageLimits()
	return image + (1 << 20)
}

const SourcePlaneWorkflowNamespaceV1 = "plugin/workflow/default/durable"

func SourcePlaneCheckpointSizeError(size, limit, index uint64) error {
	return LocateReverseJSONLimit(&ReverseJSONLimitError{
		Resource: "checkpoint_bytes", Measured: size, Limit: limit,
	}, "checkpoint/source.json", "$", index)
}

// ReadSourcePlaneCheckpointEnvelope borrows the logical image from the pinned
// checkpoint bytes. The typed outer fields are small. The workflow reader
// admits the logical image's separate section limits before decoding it.
func ReadSourcePlaneCheckpointEnvelope(cp Checkpoint) (SourcePlaneOriginBindingV1, uint64, json.RawMessage, error) {
	var origin SourcePlaneOriginBindingV1
	if size, limit := uint64(len(cp.Payload)), SourcePlaneCheckpointEnvelopeBytes(); size > limit {
		return origin, 0, nil, SourcePlaneCheckpointSizeError(size, limit, 0)
	}
	if !json.Valid(cp.Payload) {
		return origin, 0, nil, sourcePlaneError("invalid checkpoint JSON")
	}
	envelope, err := checkpointFields(cp.Payload, 2)
	if err != nil || len(envelope["sourceOrigin"]) == 0 || len(envelope["sourceOrigin"]) > 1<<20 {
		return origin, 0, nil, sourcePlaneError("invalid checkpoint envelope")
	}
	originFields := map[string]bool{"version": true, "jointPlanDigest": true, "requestHash": true, "storeId": true, "incarnation": true, "namespace": true, "initialGeneration": true, "logicalImageSHA256": true, "lifetime": true}
	if err := sourcePlaneJSONObject(envelope["sourceOrigin"], originFields, true); err != nil {
		return origin, 0, nil, err
	}
	if err := json.Unmarshal(envelope["sourceOrigin"], &origin); err != nil {
		return origin, 0, nil, err
	}
	values, err := checkpointFields(envelope["values"], 1)
	if err != nil {
		return origin, 0, nil, err
	}
	item, err := checkpointFields(values["workflow/state"], 2)
	if err != nil {
		return origin, 0, nil, err
	}
	var revision uint64
	value := item["value"]
	if len(item["revision"]) > 20 || json.Unmarshal(item["revision"], &revision) != nil || revision == 0 || len(value) == 0 || value[0] != '{' {
		return origin, 0, nil, sourcePlaneError("invalid workflow checkpoint state")
	}
	return origin, revision, value, nil
}

// The envelope has already passed lexical JSON validation. Find its fields
// without copying a raw message or building a decoder buffer for the image.
func checkpointFields(raw []byte, count int) (map[string][]byte, error) {
	raw = trimCheckpointSpace(raw)
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return nil, sourcePlaneError("expected checkpoint object")
	}
	fields := make(map[string][]byte, count)
	for raw = trimCheckpointSpace(raw[1 : len(raw)-1]); len(raw) != 0; {
		if count != 0 && len(fields) >= count {
			return nil, sourcePlaneError("unexpected checkpoint field")
		}
		keyEnd := checkpointValueEnd(raw)
		var key string
		if keyEnd > 256 || json.Unmarshal(raw[:keyEnd], &key) != nil || fields[key] != nil {
			return nil, sourcePlaneError("invalid or duplicate checkpoint field")
		}
		raw = trimCheckpointSpace(raw[keyEnd:])
		if len(raw) == 0 || raw[0] != ':' {
			return nil, sourcePlaneError("missing checkpoint field value")
		}
		raw = trimCheckpointSpace(raw[1:])
		end := checkpointValueEnd(raw)
		fields[key] = trimCheckpointSpace(raw[:end])
		raw = trimCheckpointSpace(raw[end:])
		if len(raw) != 0 {
			raw = trimCheckpointSpace(raw[1:]) // validated comma
		}
	}
	if count != 0 && len(fields) != count {
		return nil, sourcePlaneError("incomplete checkpoint object")
	}
	return fields, nil
}

func trimCheckpointSpace(raw []byte) []byte {
	return bytes.Trim(raw, " \t\r\n")
}

func checkpointValueEnd(raw []byte) int {
	depth := 0
	quoted, escaped := false, false
	for i, char := range raw {
		switch {
		case escaped:
			escaped = false
		case quoted:
			escaped = char == '\\'
			quoted = char != '"'
		case char == '"':
			quoted = true
		case char == '[' || char == '{':
			depth++
		case char == ']' || char == '}':
			depth--
		case depth == 0 && (char == ':' || char == ','):
			return i
		}
	}
	return len(raw)
}
