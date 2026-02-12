package util

import (
	"sort"
	"strings"
	"unicode"
)

// ParseTableInfo extracts table names from a SQL statement.
// Returns format: "table1(R),table2(C)" where R=Read, C=Create/Update/Delete.
func ParseTableInfo(sql string) string {
	if sql == "" {
		return ""
	}

	sql = strings.TrimSpace(sql)
	upper := strings.ToUpper(sql)

	tables := make(map[string]string) // tableName -> operation (R/C/U/D)

	tokens := tokenize(upper)
	if len(tokens) == 0 {
		return ""
	}

	switch tokens[0] {
	case "SELECT":
		parseSelect(tokens, tables)
	case "INSERT":
		parseInsert(tokens, tables)
	case "UPDATE":
		parseUpdate(tokens, tables)
	case "DELETE":
		parseDelete(tokens, tables)
	case "MERGE":
		parseMerge(tokens, tables)
	default:
		return ""
	}

	if len(tables) == 0 {
		return ""
	}

	// Sort and format
	var result []string
	for name, op := range tables {
		result = append(result, name+"("+op+")")
	}
	sort.Strings(result)
	return strings.Join(result, ",")
}

func tokenize(sql string) []string {
	var tokens []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inString {
			if ch == stringChar {
				inString = false
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inString = true
			stringChar = ch
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			continue
		}
		if ch == '(' || ch == ')' || ch == ',' || ch == ';' {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			tokens = append(tokens, string(ch))
			continue
		}
		if unicode.IsSpace(rune(ch)) {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteByte(ch)
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

func isTableName(token string) bool {
	if token == "" {
		return false
	}
	keywords := map[string]bool{
		"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true,
		"INSERT": true, "INTO": true, "VALUES": true, "UPDATE": true, "SET": true,
		"DELETE": true, "JOIN": true, "LEFT": true, "RIGHT": true, "INNER": true,
		"OUTER": true, "CROSS": true, "ON": true, "AS": true, "IN": true,
		"NOT": true, "NULL": true, "IS": true, "LIKE": true, "BETWEEN": true,
		"GROUP": true, "BY": true, "ORDER": true, "HAVING": true, "LIMIT": true,
		"OFFSET": true, "UNION": true, "ALL": true, "DISTINCT": true, "EXISTS": true,
		"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
		"CREATE": true, "ALTER": true, "DROP": true, "TABLE": true, "INDEX": true,
		"MERGE": true, "USING": true, "MATCHED": true, "WITH": true,
		"(": true, ")": true, ",": true, ";": true, "*": true,
		"FULL": true, "NATURAL": true, "FETCH": true, "FIRST": true, "NEXT": true,
		"ROWS": true, "ONLY": true, "FOR": true,
	}
	if keywords[token] {
		return false
	}
	// Must start with letter or underscore
	ch := token[0]
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
}

func parseSelect(tokens []string, tables map[string]string) {
	for i := 0; i < len(tokens); i++ {
		t := tokens[i]
		if t == "FROM" || t == "JOIN" {
			if i+1 < len(tokens) && isTableName(tokens[i+1]) {
				tables[tokens[i+1]] = "R"
			}
		}
	}
}

func parseInsert(tokens []string, tables map[string]string) {
	for i := 0; i < len(tokens); i++ {
		if tokens[i] == "INTO" && i+1 < len(tokens) && isTableName(tokens[i+1]) {
			tables[tokens[i+1]] = "C"
			break
		}
	}
	// Check for SELECT subquery (INSERT INTO ... SELECT ... FROM ...)
	for i := 0; i < len(tokens); i++ {
		if tokens[i] == "SELECT" {
			parseSelect(tokens[i:], tables)
			break
		}
	}
}

func parseUpdate(tokens []string, tables map[string]string) {
	if len(tokens) > 1 && isTableName(tokens[1]) {
		tables[tokens[1]] = "U"
	}
	// Check for FROM in subqueries
	for i := 2; i < len(tokens); i++ {
		if tokens[i] == "FROM" || tokens[i] == "JOIN" {
			if i+1 < len(tokens) && isTableName(tokens[i+1]) {
				tables[tokens[i+1]] = "R"
			}
		}
	}
}

func parseDelete(tokens []string, tables map[string]string) {
	for i := 0; i < len(tokens); i++ {
		if tokens[i] == "FROM" && i+1 < len(tokens) && isTableName(tokens[i+1]) {
			tables[tokens[i+1]] = "D"
			break
		}
	}
}

func parseMerge(tokens []string, tables map[string]string) {
	for i := 0; i < len(tokens); i++ {
		if tokens[i] == "INTO" && i+1 < len(tokens) && isTableName(tokens[i+1]) {
			tables[tokens[i+1]] = "U"
		}
		if tokens[i] == "USING" && i+1 < len(tokens) && isTableName(tokens[i+1]) {
			tables[tokens[i+1]] = "R"
		}
	}
}
