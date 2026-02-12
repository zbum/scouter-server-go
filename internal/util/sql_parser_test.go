package util

import (
	"testing"
)

func TestParseTableInfo(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{"empty", "", ""},
		{"simple select", "SELECT * FROM users", "USERS(R)"},
		{"select with join", "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id", "ORDERS(R),USERS(R)"},
		{"insert", "INSERT INTO users (name) VALUES ('test')", "USERS(C)"},
		{"update", "UPDATE users SET name = 'test' WHERE id = 1", "USERS(U)"},
		{"delete", "DELETE FROM users WHERE id = 1", "USERS(D)"},
		{"insert select", "INSERT INTO backup SELECT * FROM users", "BACKUP(C),USERS(R)"},
		{"merge", "MERGE INTO target USING source ON target.id = source.id", "SOURCE(R),TARGET(U)"},
		{"select subquery", "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)", "ORDERS(R),USERS(R)"},
		{"left join", "SELECT * FROM a LEFT JOIN b ON a.id = b.id", "A(R),B(R)"},
		{"update with from", "UPDATE t1 SET col = t2.col FROM t2 WHERE t1.id = t2.id", "T1(U),T2(R)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseTableInfo(tt.sql)
			if result != tt.expected {
				t.Errorf("ParseTableInfo(%q) = %q, want %q", tt.sql, result, tt.expected)
			}
		})
	}
}
