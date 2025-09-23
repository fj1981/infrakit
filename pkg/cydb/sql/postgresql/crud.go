package sqlpostgresql

import (
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
)

var _ DatabaseTransformer = (*postgresqlSql)(nil)

// EscapeTableName escapes a table name according to PostgreSQL rules
// 只有当表名是PostgreSQL关键字时才转义
func (t *postgresqlSql) EscapeTableName(tableName string) string {
	trimmed := strings.TrimSpace(tableName)
	if isPostgreSQLKeyword(trimmed) {
		return fmt.Sprintf("\"%s\"", trimmed)
	}
	// 不是关键字，直接返回
	return trimmed
}

// EscapeColumn escapes a column name according to PostgreSQL rules
// 只有当列名是PostgreSQL关键字时才转义
func (t *postgresqlSql) EscapeColumnName(column string) string {
	if isPostgreSQLKeyword(column) {
		return fmt.Sprintf("\"%s\"", column)
	}
	// 不是关键字，直接返回
	return column
}

// isPostgreSQLKeyword 检查给定的字符串是否是PostgreSQL保留关键字
func isPostgreSQLKeyword(word string) bool {
	// PostgreSQL真正的保留关键字列表（不区分大小写）
	keywords := map[string]bool{
		"all": true, "analyse": true, "analyze": true, "and": true, "any": true,
		"array": true, "as": true, "asc": true, "asymmetric": true, "authorization": true,
		"between": true, "binary": true, "both": true, "case": true, "cast": true,
		"check": true, "collate": true, "column": true, "concurrently": true, "constraint": true,
		"create": true, "cross": true, "current_catalog": true, "current_date": true, "current_role": true,
		"current_schema": true, "current_time": true, "current_timestamp": true, "current_user": true, "default": true,
		"deferrable": true, "desc": true, "distinct": true, "do": true, "else": true,
		"end": true, "except": true, "false": true, "fetch": true, "for": true,
		"foreign": true, "freeze": true, "from": true, "full": true, "grant": true,
		"group": true, "having": true, "ilike": true, "in": true, "initially": true,
		"inner": true, "intersect": true, "into": true, "is": true, "isnull": true,
		"join": true, "lateral": true, "leading": true, "left": true, "like": true,
		"limit": true, "localtime": true, "localtimestamp": true, "natural": true, "not": true,
		"notnull": true, "null": true, "offset": true, "on": true, "only": true,
		"or": true, "order": true, "outer": true, "overlaps": true, "placing": true,
		"primary": true, "references": true, "returning": true, "right": true, "select": true,
		"session_user": true, "similar": true, "some": true, "symmetric": true, "table": true,
		"tablesample": true, "then": true, "to": true, "trailing": true, "true": true,
		"union": true, "unique": true, "user": true, "using": true, "variadic": true,
		"verbose": true, "when": true, "where": true, "window": true, "with": true,
	}

	// 不区分大小写检查
	return keywords[strings.ToLower(word)]
}

// BuildPagination builds a pagination clause for PostgreSQL
func (s *postgresqlSql) BuildPagination(limit string, offset string) string {
	if limit == "" && offset == "" {
		return ""
	}
	// MySQL使用标准LIMIT/OFFSET语法
	var parts []string
	if limit != "" {
		parts = append(parts, fmt.Sprintf("LIMIT %s", limit))
	}
	if offset != "" {
		parts = append(parts, fmt.Sprintf("OFFSET %s", offset))
	}
	return strings.Join(parts, " ")
}

// BuildReplaceSQL builds a REPLACE SQL statement for PostgreSQL
// PostgreSQL uses INSERT ... ON CONFLICT ... DO NOTHING for REPLACE functionality
func (t *postgresqlSql) BuildReplaceSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
	// 构建 PostgreSQL INSERT ... ON CONFLICT ... DO NOTHING 语句
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(t.EscapeTableName(tableName))
	sb.WriteString(" (")

	columnNames, err := bs.GetFieldsString(t, false)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(columnNames)
	sb.WriteString(") VALUES (")

	valuesStr, fields, err := bs.GetValuesString(t, true)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(valuesStr)
	sb.WriteString(")")
	paramOrder = fields

	primaryKeys, err := bs.GetPkFieldsString(t, false)
	if err != nil {
		return "", nil, err
	}
	// 添加 ON CONFLICT 子句
	if primaryKeys != "" {
		sb.WriteString(" ON CONFLICT (")
		sb.WriteString(primaryKeys)
		sb.WriteString(") DO NOTHING")
	}
	return sb.String(), paramOrder, nil
}

// BuildUpsertSQL implements DatabaseTransformer for PostgreSQL
// PostgreSQL 使用 INSERT ... ON CONFLICT ... DO UPDATE 语法
func (t *postgresqlSql) BuildUpsertSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(t.EscapeTableName(tableName))
	sb.WriteString(" (")

	// 构建列名
	columnNames, err := bs.GetFieldsString(t, false)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(columnNames)
	sb.WriteString(") VALUES (")

	// 构建 INSERT VALUES 占位符
	columnValues, fields, err := bs.GetValuesString(t, true)
	if err != nil {
		return "", nil, err
	}
	sb.WriteString(columnValues)
	paramOrder = fields
	sb.WriteString(")")

	primaryKeys, err := bs.GetPkFieldsString(t, false)
	if err != nil {
		return "", nil, err
	}
	// 添加 ON CONFLICT 子句
	if primaryKeys != "" {
		sb.WriteString(" ON CONFLICT (")
		sb.WriteString(primaryKeys)
		sb.WriteString(")")

		updateAssignStr, updateFields, err := bs.GetAssignString(t, false)
		if err != nil {
			return "", nil, err
		}
		if updateAssignStr != "" {
			sb.WriteString(" DO UPDATE SET ")
			sb.WriteString(updateAssignStr)
			paramOrder = append(paramOrder, updateFields...)
		} else {
			sb.WriteString(" DO NOTHING")
		}
	}
	return sb.String(), paramOrder, nil
}

func (t *postgresqlSql) PreProcess(query string, _ ...int) string {
	// Replace backticks with double quotes for PostgreSQL
	query = strings.Replace(query, "`", "\"", -1)
	return query
}

// SupportsBatch implements DatabaseTransformer for PostgreSQL
func (t *postgresqlSql) SupportsBatch() bool {
	return true // PostgreSQL supports batch operations
}
