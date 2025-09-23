package sqlpostgresql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/duke-git/lancet/v2/slice"
	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

func (s *postgresqlSql) GetSortedSqlFunc(funcName SortFuncName) (SortedSqlFunc, error) {
	switch funcName {
	case FuncNameSortTables:
		return s.sortTables, nil
	case FuncNameSortFunctions:
		return s.sortFunctions, nil
	default:
		return nil, nil
	}
}

func (s *postgresqlSql) sortTables(j DatabaseClient, tableNames []string) ([]*SqlContent, error) {
	database := j.Database()
	oldTableMap := map[string]string{}
	// Convert all table names to lowercase for consistent case handling in PostgreSQL
	tableNames = slice.Map(tableNames, func(index int, item string) string {
		newName := strings.ToLower(item)
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
		SELECT 
			kcu.table_name, 
			ccu.table_name AS referenced_table
		FROM information_schema.table_constraints tc 
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name 
		JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name 
		WHERE tc.constraint_type = 'FOREIGN KEY' 
		AND kcu.table_name IN (%s) 
		AND ccu.table_name IN (%s)`,
		inClause, inClause)

	result, err := j.Query(query)
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
		tableName := strings.ToLower(cyutil.GetStr(row, "table_name"))
		referencedTable := strings.ToLower(cyutil.GetStr(row, "referenced_table"))

		// Skip invalid or self-referencing relationships
		if tableName == "" || referencedTable == "" || tableName == referencedTable {
			continue
		}

		// Skip if either table is not in our list
		if !tableSet[tableName] || !tableSet[referencedTable] {
			continue
		}

		// Add to dependency graph - the table depends on the referenced table
		if _, exists := depGraph[tableName]; !exists {
			depGraph[tableName] = make(map[string]struct{})
		}
		depGraph[tableName][referencedTable] = struct{}{}
	}

	// Sort tables based on dependencies
	sortedTables, err := cyutil.GraphSort(tableNames, depGraph)
	if err != nil {
		return nil, err
	}

	// Convert tables to SqlContent objects
	var sqls []*SqlContent
	for _, table := range sortedTables {
		sql, err := s.GetCreateTableSql(j, database, table)
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

func (s *postgresqlSql) sortFunctions(j DatabaseClient, functionNames []string) ([]*SqlContent, error) {
	database := j.Database()
	funcMap := map[string]string{}

	// Convert all function names to lowercase for consistent case handling in PostgreSQL
	functionNames = slice.Map(functionNames, func(index int, item string) string {
		v := strings.ToLower(item)
		funcMap[v] = item // Store original name for later reference
		return v
	})

	if len(functionNames) == 0 {
		return nil, nil
	}

	// Create IN clause for SQL query
	inFuncNames := slice.Map(functionNames, func(index int, item string) string {
		return "'" + item + "'"
	})
	inClause := strings.Join(inFuncNames, ",")

	// Query to get function dependencies
	// PostgreSQL stores function dependencies in pg_depend
	query := fmt.Sprintf(`
		SELECT 
			caller.proname AS caller_function,
			called.proname AS called_function
		FROM pg_catalog.pg_proc caller
		JOIN pg_catalog.pg_depend dep ON dep.objid = caller.oid
		JOIN pg_catalog.pg_proc called ON dep.refobjid = called.oid
		JOIN pg_catalog.pg_namespace ns ON caller.pronamespace = ns.oid
		WHERE ns.nspname = '%s'
		AND caller.proname IN (%s)
		AND called.proname IN (%s)
		AND caller.proname <> called.proname`,
		database, inClause, inClause)

	result, err := j.Query(query)
	if err != nil {
		return nil, err
	}

	// Build dependency graph
	depGraph := make(map[string]map[string]struct{})
	for _, row := range result {
		callerFunc := strings.ToLower(cyutil.GetStr(row, "caller_function"))
		calledFunc := strings.ToLower(cyutil.GetStr(row, "called_function"))

		// Skip invalid relationships
		if callerFunc == "" || calledFunc == "" || callerFunc == calledFunc {
			continue
		}

		// Add to dependency graph - the caller depends on the called function
		if _, exists := depGraph[callerFunc]; !exists {
			depGraph[callerFunc] = make(map[string]struct{})
		}
		depGraph[callerFunc][calledFunc] = struct{}{}
	}

	// Sort functions based on dependencies
	sortedFunctions, err := cyutil.GraphSort(functionNames, depGraph)
	if err != nil {
		return nil, err
	}

	// Convert functions to SqlContent objects
	var sqls []*SqlContent
	for _, funcName := range sortedFunctions {
		originalName, ok := funcMap[funcName]
		if !ok {
			return nil, errors.New("function " + funcName + " not found")
		}

		sql, err := s.GetCreateFunctionSql(j, database, funcName)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, &SqlContent{Name: originalName, Content: sql})
	}

	return sqls, nil
}
