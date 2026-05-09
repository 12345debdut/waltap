package cdc

import "strings"

// Filter controls which change events pass through to the sink.
//
// Filtering happens at two levels:
//
//  1. Postgres-side: If Tables is set, the publication is created with
//     FOR TABLE ... instead of FOR ALL TABLES. Postgres won't even decode
//     WAL for excluded tables — zero CPU wasted on our side.
//
//  2. Go-side: After decoding, we check Actions (and Tables as a safety net).
//     This handles action filtering (Postgres publications can't filter by
//     action type in older versions) and lets us change filters without
//     recreating the publication.
//
// An empty filter means "pass everything through."
type Filter struct {
	// Tables is the set of table names to include (e.g., "users", "orders").
	// If empty, all tables pass. Names are matched case-insensitively.
	// Format: just the table name (matched against RelationName),
	// or "schema.table" for schema-qualified matching.
	Tables map[string]struct{}

	// Actions is the set of actions to include ("INSERT", "UPDATE", "DELETE").
	// If empty, all actions pass.
	Actions map[string]struct{}
}

// NewFilter creates a filter from comma-separated table and action strings.
//
// Examples:
//
//	NewFilter("users,orders", "INSERT,UPDATE")  → only inserts/updates on users and orders
//	NewFilter("", "DELETE")                     → only deletes on all tables
//	NewFilter("users", "")                      → all actions on users only
//	NewFilter("", "")                           → no filtering (everything passes)
func NewFilter(tables, actions string) Filter {
	f := Filter{}

	if tables != "" {
		f.Tables = make(map[string]struct{})
		for _, t := range strings.Split(tables, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				f.Tables[strings.ToLower(t)] = struct{}{}
			}
		}
	}

	if actions != "" {
		f.Actions = make(map[string]struct{})
		for _, a := range strings.Split(actions, ",") {
			a = strings.TrimSpace(a)
			if a != "" {
				f.Actions[strings.ToUpper(a)] = struct{}{}
			}
		}
	}

	return f
}

// Match returns true if the event should be forwarded to the sink.
func (f Filter) Match(event ChangeEvent) bool {
	// Check action filter
	if len(f.Actions) > 0 {
		if _, ok := f.Actions[event.Action]; !ok {
			return false
		}
	}

	// Check table filter
	if len(f.Tables) > 0 {
		tableLower := strings.ToLower(event.Table)
		qualifiedLower := strings.ToLower(event.Schema + "." + event.Table)

		_, matchTable := f.Tables[tableLower]
		_, matchQualified := f.Tables[qualifiedLower]

		if !matchTable && !matchQualified {
			return false
		}
	}

	return true
}

// TableList returns the table names for use in CREATE PUBLICATION.
// Returns nil if no table filter is set (meaning FOR ALL TABLES).
func (f Filter) TableList() []string {
	if len(f.Tables) == 0 {
		return nil
	}
	tables := make([]string, 0, len(f.Tables))
	for t := range f.Tables {
		tables = append(tables, t)
	}
	return tables
}
