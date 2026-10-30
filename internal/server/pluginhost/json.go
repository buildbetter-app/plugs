package pluginhost

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func decode(data []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}
func strictParams(data []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	d.UseNumber()
	return d.Decode(dst)
}
func assign(value, dst any) error {
	if dst == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return decode(data, dst)
}

// Core's detached JSON callback values encode absent RawMessage images as
// null. Public before/after images use omission for that same absence.
func copyImage(data json.RawMessage) json.RawMessage {
	if len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	return append(json.RawMessage(nil), data...)
}
