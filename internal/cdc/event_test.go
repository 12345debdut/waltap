package cdc

import (
	"encoding/json"
	"testing"
	"time"
)

func TestChangeEvent_JSONRoundTrip(t *testing.T) {
	original := ChangeEvent{
		Action: "UPDATE",
		Schema: "public",
		Table:  "users",
		New:    map[string]any{"id": float64(1), "name": "alice", "email": "alice@new.com"},
		Old:    map[string]any{"id": float64(1), "name": "alice", "email": "alice@old.com"},
		Timestamp: time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		LSN:    "0/1234567",
		Xid:    42,
	}

	// Marshal
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Unmarshal
	var decoded ChangeEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Verify fields
	if decoded.Action != original.Action {
		t.Errorf("Action = %q, want %q", decoded.Action, original.Action)
	}
	if decoded.Schema != original.Schema {
		t.Errorf("Schema = %q, want %q", decoded.Schema, original.Schema)
	}
	if decoded.Table != original.Table {
		t.Errorf("Table = %q, want %q", decoded.Table, original.Table)
	}
	if decoded.LSN != original.LSN {
		t.Errorf("LSN = %q, want %q", decoded.LSN, original.LSN)
	}
	if decoded.Xid != original.Xid {
		t.Errorf("Xid = %d, want %d", decoded.Xid, original.Xid)
	}

	// Verify nested maps
	if decoded.New["name"] != "alice" {
		t.Errorf("New[name] = %v, want alice", decoded.New["name"])
	}
	if decoded.Old["email"] != "alice@old.com" {
		t.Errorf("Old[email] = %v, want alice@old.com", decoded.Old["email"])
	}
}

func TestChangeEvent_JSON_OmitsEmptyFields(t *testing.T) {
	// INSERT has no Old, DELETE has no New
	insert := ChangeEvent{
		Action:    "INSERT",
		Schema:    "public",
		Table:     "users",
		New:       map[string]any{"id": float64(1)},
		Timestamp: time.Now(),
		LSN:       "0/1",
		Xid:       1,
	}

	data, err := json.Marshal(insert)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	jsonStr := string(data)

	// Should have "new" but not "old"
	if !contains(jsonStr, `"new"`) {
		t.Error("INSERT JSON should contain 'new' field")
	}
	if contains(jsonStr, `"old"`) {
		t.Error("INSERT JSON should NOT contain 'old' field (omitempty)")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
