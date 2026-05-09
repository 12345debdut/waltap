package cdc

import (
	"testing"
	"time"
)

func TestNewFilter_Empty(t *testing.T) {
	f := NewFilter("", "")

	if len(f.Tables) != 0 {
		t.Errorf("expected empty tables, got %d", len(f.Tables))
	}
	if len(f.Actions) != 0 {
		t.Errorf("expected empty actions, got %d", len(f.Actions))
	}
}

func TestNewFilter_ParsesTables(t *testing.T) {
	f := NewFilter("users, orders, products", "")

	if len(f.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(f.Tables))
	}
	for _, want := range []string{"users", "orders", "products"} {
		if _, ok := f.Tables[want]; !ok {
			t.Errorf("expected table %q in filter", want)
		}
	}
}

func TestNewFilter_ParsesActions(t *testing.T) {
	f := NewFilter("", "insert,DELETE, update")

	if len(f.Actions) != 3 {
		t.Fatalf("expected 3 actions, got %d", len(f.Actions))
	}
	// Actions should be uppercased
	for _, want := range []string{"INSERT", "DELETE", "UPDATE"} {
		if _, ok := f.Actions[want]; !ok {
			t.Errorf("expected action %q in filter", want)
		}
	}
}

func TestFilter_Match(t *testing.T) {
	tests := []struct {
		name    string
		tables  string
		actions string
		event   ChangeEvent
		want    bool
	}{
		{
			name:    "empty filter matches everything",
			tables:  "",
			actions: "",
			event:   makeEvent("INSERT", "public", "users"),
			want:    true,
		},
		{
			name:    "table filter matches",
			tables:  "users",
			actions: "",
			event:   makeEvent("INSERT", "public", "users"),
			want:    true,
		},
		{
			name:    "table filter rejects",
			tables:  "orders",
			actions: "",
			event:   makeEvent("INSERT", "public", "users"),
			want:    false,
		},
		{
			name:    "action filter matches",
			tables:  "",
			actions: "INSERT,UPDATE",
			event:   makeEvent("INSERT", "public", "users"),
			want:    true,
		},
		{
			name:    "action filter rejects",
			tables:  "",
			actions: "INSERT,UPDATE",
			event:   makeEvent("DELETE", "public", "users"),
			want:    false,
		},
		{
			name:    "both filters must match - pass",
			tables:  "orders",
			actions: "INSERT",
			event:   makeEvent("INSERT", "public", "orders"),
			want:    true,
		},
		{
			name:    "both filters - table fails",
			tables:  "orders",
			actions: "INSERT",
			event:   makeEvent("INSERT", "public", "users"),
			want:    false,
		},
		{
			name:    "both filters - action fails",
			tables:  "orders",
			actions: "INSERT",
			event:   makeEvent("DELETE", "public", "orders"),
			want:    false,
		},
		{
			name:    "case insensitive table match",
			tables:  "Users",
			actions: "",
			event:   makeEvent("INSERT", "public", "users"),
			want:    true,
		},
		{
			name:    "qualified table match",
			tables:  "public.users",
			actions: "",
			event:   makeEvent("INSERT", "public", "users"),
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFilter(tt.tables, tt.actions)
			got := f.Match(tt.event)
			if got != tt.want {
				t.Errorf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilter_TableList(t *testing.T) {
	t.Run("empty filter returns nil", func(t *testing.T) {
		f := NewFilter("", "")
		if got := f.TableList(); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("returns table names", func(t *testing.T) {
		f := NewFilter("users,orders", "")
		got := f.TableList()
		if len(got) != 2 {
			t.Fatalf("expected 2 tables, got %d", len(got))
		}
	})
}

func makeEvent(action, schema, table string) ChangeEvent {
	return ChangeEvent{
		Action:    action,
		Schema:    schema,
		Table:     table,
		Timestamp: time.Now(),
	}
}
