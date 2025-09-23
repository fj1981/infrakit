package sqloracle

import (
	"errors"
	"fmt"
	"strings"

	"github.com/duke-git/lancet/v2/slice"
	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit
)

func (s *oracleSql) GetSortedSqlFunc(funcName SortFuncName) (SortedSqlFunc, error) {
	switch funcName {
	case FuncNameSortTables:
		return s.sortTables, nil
	case FuncNameSortFunctions:
		return s.sortFunctions, nil
	default:
		return nil, nil
	}
}

func (s *oracleSql) sortTables(j DatabaseClient, tableNames []string) ([]*SqlContent, error) {
	database := strings.ToUpper(j.Database())
	oldTableMap := map[string]string{}
	// Convert all table names to uppercase for consistent case handling
	tableNames = slice.Map(tableNames, func(index int, item string) string {
		newName := strings.ToUpper(ConvertReservedKeywords(item))
		oldTableMap[newName] = item
		return newName
	})
	if len(tableNames) == 0 {
		return nil, nil
	}

	// Create IN clause for SQL query
	inTableNames := slice.Map(tableNames, func(index int, item string) string {
		return "'" + item + "'"
	})
	inClause := strings.Join(inTableNames, ",")

	// Query to get foreign key relationships between tables
	query := fmt.Sprintf(`
		SELECT a.table_name, a.constraint_name, a.r_owner, a.r_constraint_name, 
		       c_pk.table_name as referenced_table
		FROM all_constraints a
		JOIN all_constraints c_pk ON a.r_constraint_name = c_pk.constraint_name 
		  AND a.r_owner = c_pk.owner
		WHERE a.owner = :schema AND a.constraint_type = 'R'
		AND a.table_name IN (%s)
		AND c_pk.table_name IN (%s)`,
		inClause, inClause)

	paramMap := map[string]interface{}{
		"schema": strings.ToUpper(database),
	}

	result, err := j.NQuery(query, paramMap)
	if err != nil {
		return nil, err
	}

	// Create a set of all tables for quick lookup
	tableSet := make(map[string]bool)
	for _, name := range tableNames {
		tableSet[name] = true
	}

	// Build dependency graph
	depGraph := make(map[string]map[string]struct{})
	for _, row := range result {
		tableName := cyutil.GetStr(row, "TABLE_NAME", true)
		referencedTable := cyutil.GetStr(row, "REFERENCED_TABLE", true)

		// Skip invalid or self-referencing relationships
		if tableName == "" || referencedTable == "" || tableName == referencedTable {
			continue
		}

		// Ensure consistent case handling
		tableName = strings.ToUpper(tableName)
		referencedTable = strings.ToUpper(referencedTable)

		// Skip if either table is not in our list
		if !tableSet[tableName] || !tableSet[referencedTable] {
			continue
		}

		// Add to dependency graph
		if _, exists := depGraph[tableName]; !exists {
			depGraph[tableName] = make(map[string]struct{})
		}
		depGraph[tableName][referencedTable] = struct{}{}
	}

	sortedTables, err := cyutil.GraphSort(tableNames, depGraph)
	if err != nil {
		return nil, err
	}

	// Convert tables to CreateSql objects
	var sqls []*SqlContent
	for _, table := range sortedTables {
		sql, err := s.getCreateTableSQL(j, database, table)
		if err != nil {
			return nil, err
		}
		if oldName, ok := oldTableMap[table]; ok {
			sqls = append(sqls, &SqlContent{Name: oldName, Content: sql})
		} else {
			return nil, errors.New("table " + table + " not found")
		}
	}

	return sqls, nil
}

func (s *oracleSql) sortFunctions(j DatabaseClient, functionNames []string) ([]*SqlContent, error) {
	database := strings.ToUpper(j.Database())
	funcMap := map[string]struct{}{}
	functionNames = slice.Map(functionNames, func(index int, item string) string {
		v := strings.ToUpper(item)
		funcMap[v] = struct{}{}
		return v
	})
	sql := `
	SELECT 
    d.name,
    d.referenced_name
FROM 
    all_dependencies d
WHERE 
    d.type = 'FUNCTION'                    
    AND d.referenced_type = 'FUNCTION'     
    AND d.referenced_owner = :schema          
ORDER BY 
    d.name, d.referenced_name;`

	result, err := j.NQuery(sql, map[string]interface{}{"schema": database})
	if err != nil {
		return nil, err
	}
	depGraph := make(map[string]map[string]struct{})
	for _, row := range result {
		funcName := cyutil.GetStr(row, "NAME", true)
		if _, ok := funcMap[funcName]; !ok {
			continue
		}
		referencedFuncName := cyutil.GetStr(row, "REFERENCED_NAME", true)
		if _, ok := funcMap[referencedFuncName]; !ok {
			continue
		}
		if _, ok := depGraph[funcName]; !ok {
			depGraph[funcName] = make(map[string]struct{})
		}
		depGraph[funcName][referencedFuncName] = struct{}{}
	}
	sortedFunctions, err := cyutil.GraphSort(functionNames, depGraph)
	if err != nil {
		return nil, err
	}
	var sqls []*SqlContent
	for _, funcName := range sortedFunctions {
		if _, ok := funcMap[funcName]; !ok {
			return nil, errors.New("function " + funcName + " not found")
		}
		sql, err := s.getCreateFunctionSql(j, database, funcName)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, &SqlContent{Name: funcName, Content: sql})
	}
	return sqls, nil
}
