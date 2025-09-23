package sqlmysql

import (
	"fmt"
	"slices"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

func (s *mysqlSql) GetSortedSqlFunc(funcName SortFuncName) (SortedSqlFunc, error) {
	switch funcName {
	case FuncNameSortTables:
		return s.SortTables, nil
	default:
		return nil, nil
	}
}

func (s *mysqlSql) SortTables(cli DatabaseClient, tableNames []string) ([]*SqlContent, error) {
	// 构建表和它们的外键依赖关系
	tableDependencies := make(map[string]map[string]struct{})
	inDegree := make(map[string]int)
	createTableSQLMap := make(map[string]string) // 存储处理后的 SQL 语句
	oldTableSQLMap := make(map[string]string)    // 存储原始的 SQL 语句

	for _, tableName := range tableNames {
		fullTableName := fmt.Sprintf("`%s`.`%s`", cli.Database(), tableName)
		sql := fmt.Sprintf("SHOW CREATE TABLE %s", fullTableName)
		v, err := cli.Query(sql)
		if err != nil {
			return nil, err
		}
		if len(v) == 0 || v[0]["Create Table"] == nil {
			return nil, fmt.Errorf("no create table SQL for table: %s", tableName)
		}
		createTableSQL := strings.TrimSpace(cyutil.ToStr(v[0]["Create Table"]))
		oldTableSQLMap[tableName] = createTableSQL // 保存原始 SQL

		// 处理 SQL 语句，移除换行符、回车符和反引号
		processedSQL := strings.ReplaceAll(createTableSQL, "\n", " ")
		processedSQL = strings.ReplaceAll(processedSQL, "\r", "")
		processedSQL = strings.ReplaceAll(processedSQL, "`", "")
		createTableSQLMap[tableName] = processedSQL // 保存处理后的 SQL

		// 解析外键约束并记录依赖关系
		parts := strings.Split(processedSQL, "ENGINE=")
		if len(parts) > 1 {
			foreignKeyDefs := parts[0]
			words := strings.Fields(foreignKeyDefs)
			for i := 0; i < len(words)-6; i++ { // 增加索引范围以涵盖更多关键字
				if words[i] == "FOREIGN" && words[i+1] == "KEY" && words[i+3] == "REFERENCES" {
					referencedTable := words[i+4]
					if slices.Contains(tableNames, referencedTable) {
						if referencedTable == tableName {
							// slog.Errorf("self reference detected: %s", tableName)
							continue
						}
						var dependencies map[string]struct{}
						var ok bool
						if dependencies, ok = tableDependencies[tableName]; !ok {
							dependencies = make(map[string]struct{})
							tableDependencies[tableName] = dependencies
						}
						if _, ok = dependencies[referencedTable]; !ok {
							dependencies[referencedTable] = struct{}{}
							tableDependencies[tableName] = dependencies
							inDegree[referencedTable]++
						}
					}
				}
			}
		}
	}

	// 拓扑排序
	var sortedTables []string
	var noDependencyTables []string
	for _, table := range tableNames {
		if len(tableDependencies[table]) == 0 {
			noDependencyTables = append(noDependencyTables, table)
		}
	}
	// 拓扑排序
	for len(noDependencyTables) > 0 {
		current := noDependencyTables[0]
		sortedTables = append(sortedTables, current)
		noDependencyTables = noDependencyTables[1:]

		// 更新入度计数
		for dependent, dependencies := range tableDependencies {
			delete(dependencies, current)
			if len(tableDependencies[dependent]) == 0 {
				noDependencyTables = append(noDependencyTables, dependent)
				delete(tableDependencies, dependent)
			}
		}
	}

	// 检查是否有环存在（无法拓扑排序）
	if len(sortedTables) != len(tableNames) {
		// 找出所有未被排序的表，这些表构成了环
		unsortedTables := make([]string, 0, len(tableNames)-len(sortedTables))
		for table := range tableDependencies {
			unsortedTables = append(unsortedTables, table)
		}
		return nil, fmt.Errorf("circular dependency detected among tables: %v", unsortedTables)
	}

	// 根据排序结果构建建表语句列表
	// var sortedCreateTableSQLs []string
	var originalCreateTableSQLs []*SqlContent
	for _, table := range sortedTables {
		// sortedCreateTableSQLs = append(sortedCreateTableSQLs, createTableSQLMap[table])
		originalCreateTableSQLs = append(originalCreateTableSQLs, &SqlContent{Name: table, Content: oldTableSQLMap[table]})
	}
	return originalCreateTableSQLs, nil
}
