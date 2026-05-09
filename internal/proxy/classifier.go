package proxy

import "strings"

// ClassifyQuery determines if a SQL statement is a read or write operation.
//
// This is deliberately simple — production proxies like PgBouncer,
// ProxySQL, and Vitess all use similar heuristics. The alternative
// (full SQL parsing) is complex and brittle.
//
// How it works:
//   - Strip leading whitespace and comments
//   - Check the first keyword
//   - SELECT, SHOW, EXPLAIN, SET, RESET, DISCARD → read (route to replica)
//   - BEGIN, START, COMMIT, ROLLBACK, SAVEPOINT → transaction control
//   - Everything else → write (route to primary)
//
// The key insight: we don't need to be perfect. Misrouting a read
// to the primary is safe (just suboptimal). Misrouting a write to
// the replica will fail with "cannot execute in a read-only transaction"
// — the application gets a clear error. So we only need to be correct
// for writes, which is easy: anything that isn't obviously a read
// goes to the primary.
func ClassifyQuery(sql string) QueryType {
	// Strip leading whitespace
	sql = strings.TrimSpace(sql)

	// Skip leading comments (both -- and /* */ styles)
	sql = stripLeadingComments(sql)
	sql = strings.TrimSpace(sql)

	if sql == "" {
		return QueryRead // empty queries are harmless
	}

	// Find the first word (case-insensitive)
	firstWord := strings.ToUpper(firstToken(sql))

	switch firstWord {
	case "SELECT", "SHOW", "EXPLAIN", "SET", "RESET", "DISCARD":
		return QueryRead

	case "BEGIN", "START":
		return QueryTransaction

	case "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return QueryTransaction

	// COPY TO (reading) vs COPY FROM (writing)
	case "COPY":
		upper := strings.ToUpper(sql)
		if strings.Contains(upper, " TO ") {
			return QueryRead
		}
		return QueryWrite

	default:
		// INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE,
		// GRANT, REVOKE, VACUUM, ANALYZE, etc.
		return QueryWrite
	}
}

// QueryType represents the routing decision for a SQL query.
type QueryType int

const (
	QueryRead        QueryType = iota // Route to replica
	QueryWrite                        // Route to primary
	QueryTransaction                  // Transaction control — handled specially
)

func (q QueryType) String() string {
	switch q {
	case QueryRead:
		return "READ"
	case QueryWrite:
		return "WRITE"
	case QueryTransaction:
		return "TXN"
	default:
		return "UNKNOWN"
	}
}

// firstToken extracts the first whitespace-delimited token from s.
func firstToken(s string) string {
	for i, c := range s {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(' {
			return s[:i]
		}
	}
	return s
}

// stripLeadingComments removes SQL comments from the start of the string.
//
// Supports:
//   - Line comments: -- ...
//   - Block comments: /* ... */ (including nested)
func stripLeadingComments(sql string) string {
	for {
		sql = strings.TrimSpace(sql)
		if strings.HasPrefix(sql, "--") {
			// Skip to end of line
			idx := strings.IndexByte(sql, '\n')
			if idx == -1 {
				return ""
			}
			sql = sql[idx+1:]
			continue
		}
		if strings.HasPrefix(sql, "/*") {
			// Find matching close, handling nesting
			depth := 0
			i := 0
			for i < len(sql)-1 {
				if sql[i] == '/' && sql[i+1] == '*' {
					depth++
					i += 2
				} else if sql[i] == '*' && sql[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
			sql = sql[i:]
			continue
		}
		break
	}
	return sql
}
