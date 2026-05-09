package proxy

import "testing"

func TestClassifyQuery(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want QueryType
	}{
		// Reads
		{"simple select", "SELECT * FROM users", QueryRead},
		{"select lower", "select 1", QueryRead},
		{"select with whitespace", "  SELECT id FROM orders  ", QueryRead},
		{"show", "SHOW server_version", QueryRead},
		{"explain", "EXPLAIN ANALYZE SELECT * FROM users", QueryRead},
		{"set", "SET statement_timeout = '5s'", QueryRead},
		{"reset", "RESET ALL", QueryRead},
		{"discard", "DISCARD ALL", QueryRead},

		// Writes
		{"insert", "INSERT INTO users(name) VALUES('alice')", QueryWrite},
		{"update", "UPDATE users SET name = 'bob' WHERE id = 1", QueryWrite},
		{"delete", "DELETE FROM users WHERE id = 1", QueryWrite},
		{"create table", "CREATE TABLE foo (id int)", QueryWrite},
		{"alter table", "ALTER TABLE users ADD COLUMN age int", QueryWrite},
		{"drop table", "DROP TABLE users", QueryWrite},
		{"truncate", "TRUNCATE users", QueryWrite},

		// Transaction control
		{"begin", "BEGIN", QueryTransaction},
		{"start transaction", "START TRANSACTION", QueryTransaction},
		{"commit", "COMMIT", QueryTransaction},
		{"rollback", "ROLLBACK", QueryTransaction},
		{"savepoint", "SAVEPOINT sp1", QueryTransaction},
		{"release", "RELEASE SAVEPOINT sp1", QueryTransaction},
		{"end", "END", QueryTransaction},

		// Comments
		{"line comment before select", "-- get users\nSELECT * FROM users", QueryRead},
		{"block comment before insert", "/* audit */ INSERT INTO log(msg) VALUES('hi')", QueryWrite},
		{"nested comment", "/* outer /* inner */ still outer */ SELECT 1", QueryRead},

		// Edge cases
		{"empty string", "", QueryRead},
		{"copy to", "COPY users TO STDOUT", QueryRead},
		{"copy from", "COPY users FROM STDIN", QueryWrite},
		{"case insensitive", "sElEcT 1", QueryRead},
		{"with leading newlines", "\n\n  SELECT 1", QueryRead},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyQuery(tt.sql)
			if got != tt.want {
				t.Errorf("ClassifyQuery(%q) = %s, want %s", tt.sql, got, tt.want)
			}
		})
	}
}
