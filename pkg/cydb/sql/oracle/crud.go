package sqloracle

import (
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
)

// === DatabaseTransformer 元素级转换接口实现 ===
var _ DatabaseTransformer = (*oracleSql)(nil)

// EscapeTableName implements DatabaseTransformer for Oracle
// 转义Oracle表名（大写转换和保留字处理）
func (s *oracleSql) EscapeTableName(tableName string) string {
	upper := strings.ToUpper(tableName)
	if isOracleKeyword(upper) {
		return fmt.Sprintf(`"%s"`, upper)
	}
	return upper
}

func isOracleKeyword(word string) bool {
	_, ok := oracleReservedKeywords[strings.ToUpper(word)]
	return ok
}

// EscapeColumn implements DatabaseTransformer for Oracle
// 转义Oracle列名（大写转换和保留字处理）
func (s *oracleSql) EscapeColumnName(column string) string {
	if isOracleKeyword(column) {
		return fmt.Sprintf(`"%s"`, column)
	}
	return column
}

// BuildPagination implements DatabaseTransformer for Oracle
// 构建Oracle的分页语句（OFFSET/FETCH语法）
func (s *oracleSql) BuildPagination(limit string, offset string) string {
	if limit == "" && offset == "" {
		return ""
	}
	// Oracle 12c+使用OFFSET/FETCH语法
	var parts []string
	if offset != "" {
		parts = append(parts, fmt.Sprintf("OFFSET %s ROWS", offset))
	}
	if limit != "" {
		parts = append(parts, fmt.Sprintf("FETCH NEXT %s ROWS ONLY", limit))
	}

	return strings.Join(parts, " ")
}

// SupportsBatch implements DatabaseTransformer for Oracle
func (s *oracleSql) SupportsBatch() bool {
	return true // Oracle支持批量操作
}

// BuildReplaceSQL implements DatabaseTransformer for Oracle
// Oracle 使用 MERGE 语句实现 REPLACE 功能
func (s *oracleSql) BuildReplaceSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
	var sb strings.Builder
	paramOrder = make([]string, 0)
	tableEscaped := s.EscapeTableName(tableName)

	// 获取字段列表
	columnNames, err := bs.GetFieldsString(s, false)
	if err != nil {
		return "", nil, err
	}

	// 获取主键字段
	primaryKeys, err := bs.GetPkFieldsString(s, false)
	if err != nil {
		return "", nil, err
	}

	// 获取值列表
	columnValues, fields, err := bs.GetValuesString(s, true)
	if err != nil {
		return "", nil, err
	}
	paramOrder = append(paramOrder, fields...)

	// 构建 MERGE 语句
	sb.WriteString("MERGE INTO ")
	sb.WriteString(tableEscaped)
	sb.WriteString(" target USING (SELECT ")

	// 构建 SELECT 子句 - 使用占位符值
	sb.WriteString(columnValues)
	sb.WriteString(" FROM DUAL) source ON (")

	// 构建 ON 条件（主键匹配）
	if primaryKeys == "" {
		// 如果没有主键，使用所有字段
		sb.WriteString(columnNames)
	} else {
		sb.WriteString(primaryKeys)
	}
	sb.WriteString(")")

	// 获取更新字段
	updateAssignStr, updateFields, err := bs.GetAssignString(s, false)
	if err != nil {
		return "", nil, err
	}

	// WHEN MATCHED THEN UPDATE
	if updateAssignStr != "" {
		sb.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		sb.WriteString(updateAssignStr)
		paramOrder = append(paramOrder, updateFields...)
	}

	// WHEN NOT MATCHED THEN INSERT
	sb.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	sb.WriteString(columnNames)
	sb.WriteString(") VALUES (")
	sb.WriteString(columnValues)
	sb.WriteString(")")

	return sb.String(), paramOrder, nil
}

// BuildUpsertSQL implements DatabaseTransformer for Oracle
// Oracle 使用 MERGE 语句实现 UPSERT 功能
func (s *oracleSql) BuildUpsertSQL(tableName string, bs BuildSql) (sql string, paramOrder []string, err error) {
	// Oracle 的 UPSERT 和 REPLACE 逻辑基本相同，都使用 MERGE
	return s.BuildReplaceSQL(tableName, bs)
}
