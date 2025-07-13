package util

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/tidwall/sjson"
)

// ToJsonBytes converts a struct, map, string, or []byte into a valid JSON []byte.
func ToJsonBytes(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		if json.Valid(val) {
			return val, nil
		}
		return nil, fmt.Errorf("invalid JSON []byte")

	case string:
		if json.Valid([]byte(val)) {
			return []byte(val), nil
		}
		return nil, fmt.Errorf("invalid JSON string")

	default:
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(val); err != nil {
			return nil, err
		}
		return bytes.TrimSpace(buf.Bytes()), nil
	}
}

// SetJsonField converts any input to JSON and sets a field using sjson.
func SetJsonField(obj interface{}, path string, value interface{}) ([]byte, error) {
	jsonBytes, err := ToJsonBytes(obj)
	if err != nil {
		return nil, fmt.Errorf("to json failed: %w", err)
	}
	return sjson.SetBytes(jsonBytes, path, value)
}
