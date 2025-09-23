package sqlsqlite

import (
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
)

// === DatabaseTransformer 元素级转换接口实现 ===
var _ DatabaseTransformer = (*sqliteSql)(nil)

// EscapeTableName implements DatabaseTransformer for SQLite
// 只有当表名是SQLite关键字时才转义表名（使用方括号）
func (s *sqliteSql) EscapeTableName(tableName string) string {
	// 移除已有的方括号
	trimmed := strings.Trim(tableName, "[]")

	// 检查是否是SQLite关键字，如果是则转义
	if isSQLiteKeyword(trimmed) {
		return fmt.Sprintf("[%s]", trimmed)
	}

	// 不是关键字，直接返回
	return trimmed
}

// isSQLiteKeyword 检查给定的字符串是否是SQLite关键字
func isSQLiteKeyword(word string) bool {
	// SQLite关键字列表（不区分大小写）
	keywords := map[string]bool{
		"abort": true, "action": true, "add": true, "after": true, "all": true,
		"alter": true, "analyze": true, "and": true, "as": true, "asc": true,
		"attach": true, "autoincrement": true, "before": true, "begin": true, "between": true,
		"by": true, "cascade": true, "case": true, "cast": true, "check": true,
		"collate": true, "column": true, "commit": true, "conflict": true, "constraint": true,
		"create": true, "cross": true, "current_date": true, "current_time": true, "current_timestamp": true,
		"database": true, "default": true, "deferrable": true, "deferred": true, "delete": true,
		"desc": true, "detach": true, "distinct": true, "drop": true, "each": true,
		"else": true, "end": true, "escape": true, "except": true, "exclusive": true,
		"exists": true, "explain": true, "fail": true, "for": true, "foreign": true,
		"from": true, "full": true, "glob": true, "group": true, "having": true,
		"if": true, "ignore": true, "immediate": true, "in": true, "index": true,
		"indexed": true, "initially": true, "inner": true, "insert": true, "instead": true,
		"intersect": true, "into": true, "is": true, "isnull": true, "join": true,
		"key": true, "left": true, "like": true, "limit": true, "match": true,
		"natural": true, "no": true, "not": true, "notnull": true, "null": true,
		"of": true, "offset": true, "on": true, "or": true, "order": true,
		"outer": true, "plan": true, "pragma": true, "primary": true, "query": true,
		"raise": true, "recursive": true, "references": true, "regexp": true, "reindex": true,
		"release": true, "rename": true, "replace": true, "restrict": true, "right": true,
		"rollback": true, "row": true, "savepoint": true, "select": true, "set": true,
		"table": true, "temp": true, "temporary": true, "then": true, "to": true,
		"transaction": true, "trigger": true, "union": true, "unique": true, "update": true,
		"using": true, "vacuum": true, "values": true, "view": true, "virtual": true,
		"when": true, "where": true, "with": true, "without": true}

	// 不区分大小写检查
	return keywords[strings.ToLower(word)]
}

func (t *sqliteSql) EscapeColumnName(column string) string {
	if isSQLiteKeyword(column) {
		return fmt.Sprintf("[%s]", column)
	}
	return column
}

// BuildPagination implements DatabaseTransformer for SQLite
// 构建SQLite的分页语句（标准LIMIT/OFFSET）
func (s *sqliteSql) BuildPagination(limit, offset string) string {
	if limit == "" && offset == "" {
		return ""
	}

	var parts []string
	if limit != "" {
		parts = append(parts, fmt.Sprintf("LIMIT %s", limit))
	}
	if offset != "" {
		parts = append(parts, fmt.Sprintf("OFFSET %s", offset))
	}
	return strings.Join(parts, " ")
}

// SupportsBatch implements DatabaseTransformer for SQLite
func (s *sqliteSql) SupportsBatch() bool {
	return true // SQLite支持批量操作
}

// BuildReplaceSQL implements DatabaseTransformer for SQLite
// SQLite 支持原生 REPLACE INTO 语法
func (s *sqliteSql) BuildReplaceSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
	// 构建 SQLite REPLACE INTO 语句
	var sb strings.Builder
	sb.WriteString("REPLACE INTO ")
	sb.WriteString(s.EscapeTableName(tableName))
	sb.WriteString(" (")

	columnNames, err := bs.GetFieldsString(s, false)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(columnNames)
	sb.WriteString(") VALUES (")

	columnValues, fields, err := bs.GetValuesString(s, true)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(columnValues)
	sb.WriteString(")")

	return sb.String(), fields, nil
}

// BuildUpsertSQL implements DatabaseTransformer for SQLite
// SQLite 使用 INSERT ... ON CONFLICT 语法
func (s *sqliteSql) BuildUpsertSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
	var sb strings.Builder

	// 构建 INSERT 部分
	sb.WriteString("INSERT INTO ")
	sb.WriteString(s.EscapeTableName(tableName))
	sb.WriteString(" (")

	// 构建列名
	columnNames, err := bs.GetFieldsString(s, false)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(columnNames)
	sb.WriteString(") VALUES (")

	// 构建 INSERT VALUES 占位符
	columnValues, paramOrder, err := bs.GetValuesString(s, true)
	if err != nil {
		return "", nil, err
	}

	sb.WriteString(columnValues)
	sb.WriteString(")")

	// 构建 ON CONFLICT 部分
	// 获取主键字段用于冲突检测
	pkFields, err := bs.GetPkFieldsString(s, true)
	if err != nil {
		return "", nil, err
	}

	if pkFields != "" {
		sb.WriteString(" ON CONFLICT(")
		sb.WriteString(pkFields)
		sb.WriteString(") DO UPDATE SET ")

		// 构建 UPDATE SET 部分
		updateAssignStr, updateFields, err := bs.GetAssignString(s, false)
		if err != nil {
			return "", nil, err
		}
		if updateAssignStr != "" {
			sb.WriteString(updateAssignStr)
			paramOrder = append(paramOrder, updateFields...)
		}
	}

	return sb.String(), paramOrder, nil
}
