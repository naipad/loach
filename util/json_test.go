package util

import (
	"encoding/json"
	"testing"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestSetJsonField(t *testing.T) {
	user := User{Name: "Bob", Age: 25}

	modified, err := SetJsonField(user, "name", "Alice")
	if err != nil {
		t.Fatalf("SetJsonField failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(modified, &result); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if result["name"] != "Alice" {
		t.Errorf("Expected name to be 'Alice', got %v", result["name"])
	}
	if result["age"] != float64(25) { // JSON numbers are float64
		t.Errorf("Expected age to be 25, got %v", result["age"])
	}
}

func TestToJsonBytes(t *testing.T) {
	// Test with struct
	user := User{Name: "Charlie", Age: 40}
	jsonBytes, err := ToJsonBytes(user)
	if err != nil {
		t.Fatalf("ToJsonBytes failed: %v", err)
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &obj); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if obj["name"] != "Charlie" || obj["age"] != float64(40) {
		t.Errorf("Unexpected JSON output: %v", obj)
	}

	// Test with string
	jsonStr := `{"key":"value"}`
	jsonBytes, err = ToJsonBytes(jsonStr)
	if err != nil {
		t.Fatalf("ToJsonBytes failed for string: %v", err)
	}
	if string(jsonBytes) != jsonStr {
		t.Errorf("Expected %s, got %s", jsonStr, string(jsonBytes))
	}

	// Test with []byte
	jsonInput := []byte(`{"a":123}`)
	jsonBytes, err = ToJsonBytes(jsonInput)
	if err != nil {
		t.Fatalf("ToJsonBytes failed for []byte: %v", err)
	}
	expected := `{"a":123}`
	if string(jsonBytes) != expected {
		t.Errorf("Expected %s, got %s", expected, string(jsonBytes))
	}
}
