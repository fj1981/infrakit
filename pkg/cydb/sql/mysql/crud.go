package sqlmysql

import (
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
)

// === DatabaseTransformer 元素级转换接口实现 ===
var _ DatabaseTransformer = (*mysqlSql)(nil)

// EscapeTableName implements DatabaseTransformer for MySQL
// 只有当表名是MySQL关键字时才转义表名（使用反引号）
func (s *mysqlSql) EscapeTableName(tableName string) string {
	if isMySQLKeyword(tableName) {
		return fmt.Sprintf("`%s`", tableName)
	}
	return tableName
}

// EscapeColumnName implements DatabaseTransformer for MySQL
func (s *mysqlSql) EscapeColumnName(column string) string {
	if isMySQLKeyword(column) {
		return fmt.Sprintf("`%s`", column)
	}
	return column
}

func isMySQLKeyword(word string) bool {
	return IsMySQLKeyword(word)
}

// BuildPagination implements DatabaseTransformer for MySQL
// 构建MySQL的分页语句（标准LIMIT/OFFSET）
func (s *mysqlSql) BuildPagination(limit string, offset string) string {
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

// SupportsBatch implements DatabaseTransformer for MySQL
func (s *mysqlSql) SupportsBatch() bool {
	return true // MySQL支持批量操作
}

// BuildReplaceSQL implements DatabaseTransformer for MySQL
// MySQL 支持原生 REPLACE INTO 语法
func (s *mysqlSql) BuildReplaceSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
	// 构建 MySQL REPLACE INTO 语句
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

// BuildUpsertSQL implements DatabaseTransformer for MySQL
// MySQL 使用 INSERT ... ON DUPLICATE KEY UPDATE 语法
func (s *mysqlSql) BuildUpsertSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
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
	sb.WriteString(") VALUES ")

	// 构建 INSERT VALUES 占位符
	columnValues, paramOrder, err := bs.GetValuesString(s, true)
	if err != nil {
		return "", nil, err
	}

	sb.WriteString(columnValues)

	// 构建 ON DUPLICATE KEY UPDATE 部分
	updateAssignStr, updateFields, err := bs.GetAssignString(s, false)
	if err != nil {
		return "", nil, err
	}
	if updateAssignStr != "" {
		sb.WriteString(" ON DUPLICATE KEY UPDATE ")
		sb.WriteString(updateAssignStr)
		paramOrder = append(paramOrder, updateFields...)
	}

	return sb.String(), paramOrder, nil
}
