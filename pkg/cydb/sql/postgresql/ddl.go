package sqlpostgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
)

// ColumnInfo 表字段信息
type ColumnInfo struct {
	ColumnName             string         `db:"column_name"`
	DataType               string         `db:"data_type"`
	CharacterMaximumLength sql.NullInt64  `db:"character_maximum_length"`
	IsNullable             string         `db:"is_nullable"`
	ColumnDefault          sql.NullString `db:"column_default"`
	DefaultExpr            sql.NullString `db:"default_expr"`
}

// PrimaryKeyInfo 主键信息
type PrimaryKeyInfo struct {
	ColumnName string `db:"column_name"`
}

// ForeignKeyInfo 外键信息
type ForeignKeyInfo struct {
	ColumnName         string `db:"column_name"`
	ForeignTableSchema string `db:"foreign_table_schema"`
	ForeignTableName   string `db:"foreign_table_name"`
	ForeignColumnName  string `db:"foreign_column_name"`
	DeleteRule         string `db:"delete_rule"`
	UpdateRule         string `db:"update_rule"`
}

// IndexInfo 索引信息
type IndexInfo struct {
	IndexName  string `db:"index_name"`
	ColumnName string `db:"column_name"`
	IsUnique   string `db:"is_unique"`
}

// TriggerInfo 触发器信息
type TriggerInfo struct {
	TriggerName       string `db:"trigger_name"`
	TriggerDefinition string `db:"trigger_definition"`
}

// ViewInfo 视图信息
type ViewInfo struct {
	ViewDefinition string `db:"view_definition"`
}

// ProcedureInfo 存储过程信息
type ProcedureInfo struct {
	ProcDefinition string `db:"proc_definition"`
	ProcName       string `db:"proc_name"`
	ProcKind       string `db:"proc_kind"`
}

// FunctionInfo 函数信息
type FunctionInfo struct {
	FuncDefinition string `db:"func_definition"`
}

// EventInfo 事件信息
type EventInfo struct {
	JobName  string `db:"jobname"`
	Schedule string `db:"schedule"`
	Command  string `db:"command"`
}

func (s *postgresqlSql) GetDDLSqlFunc(funcName DDLSqlFuncName) (DDLSqlFunc, error) {
	switch funcName {
	case FuncNameGetCreateTableSql:
		return func(cli DatabaseClient, tableName ...string) (*SqlContent, error) {
			db, table := GetDBAndTable(cli, tableName...)
			if table == "" {
				return &SqlContent{}, errors.New("table name is empty")
			}
			r, err := s.GetCreateTableSql(cli, db, table)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: tableName[0], Content: r}, nil
		}, nil
	case FuncNameGetCreateViewSql:
		return func(cli DatabaseClient, viewName ...string) (*SqlContent, error) {
			db, view := GetDBAndTable(cli, viewName...)
			if view == "" {
				return nil, errors.New("view name is empty")
			}
			r, err := s.GetCreateViewSql(cli, db, view)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: viewName[0], Content: r}, nil
		}, nil
	case FuncNameGetCreateProcedureSql:
		return func(cli DatabaseClient, procName ...string) (*SqlContent, error) {
			db, proc := GetDBAndTable(cli, procName...)
			if proc == "" {
				return nil, errors.New("procedure name is empty")
			}
			r, err := s.GetCreateProcedureSql(cli, db, proc)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: procName[0], Content: r}, nil
		}, nil
	case FuncNameGetCreateFunctionSql:
		return func(cli DatabaseClient, funcName ...string) (*SqlContent, error) {
			db, func_ := GetDBAndTable(cli, funcName...)
			if func_ == "" {
				return nil, errors.New("function name is empty")
			}
			r, err := s.GetCreateFunctionSql(cli, db, func_)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: funcName[0], Content: r}, nil
		}, nil
	case FuncNameGetTableEventSql:
		return func(cli DatabaseClient, eventName ...string) (*SqlContent, error) {
			db, event := GetDBAndTable(cli, eventName...)
			if event == "" {
				return nil, errors.New("event name is empty")
			}
			r, err := s.GetCreateEventSql(cli, db, event)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: eventName[0], Content: r}, nil
		}, nil
	case FuncNameGetBeginSql:
		return func(cli DatabaseClient, name ...string) (*SqlContent, error) {
			db, _ := GetDBAndTable(cli, name...)
			if db == "" {
				return nil, errors.New("database name is empty")
			}
			// PostgreSQL uses SET search_path instead of USE
			// PostgreSQL doesn't have FOREIGN_KEY_CHECKS, but we can disable triggers temporarily
			return &SqlContent{Name: db, Content: fmt.Sprintf("SET search_path TO \"%s\";\nSET session_replication_role = 'replica';\n", db)}, nil
		}, nil
	case FuncNameGetEndSql:
		return func(cli DatabaseClient, _ ...string) (*SqlContent, error) {
			// Restore normal trigger behavior
			return &SqlContent{Name: "", Content: "\nSET session_replication_role = 'origin';\n"}, nil
		}, nil
	default:
		return nil, nil
	}
}

// getPrimaryKeyColumns retrieves primary key columns for a table
func (s *postgresqlSql) getPrimaryKeyColumns(cli DatabaseClient, database, tableName string) ([]PrimaryKeyInfo, error) {
	pkQuery := fmt.Sprintf(
		"SELECT a.attname as column_name "+
			"FROM pg_index i "+
			"JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "+
			"WHERE i.indrelid = '%s'::regclass AND i.indisprimary",
		tableName)

	var pkColumns []PrimaryKeyInfo
	err := cli.Select(&pkColumns, pkQuery)
	if err != nil {
		return nil, err
	}

	return pkColumns, nil
}

// getForeignKeyConstraints retrieves foreign key constraints for a table
func (s *postgresqlSql) getForeignKeyConstraints(cli DatabaseClient, database, tableName string) ([]ForeignKeyInfo, error) {
	fkQuery := fmt.Sprintf(
		"SELECT "+
			"kcu.column_name, "+
			"ccu.table_schema AS foreign_table_schema, "+
			"ccu.table_name AS foreign_table_name, "+
			"ccu.column_name AS foreign_column_name, "+
			"rc.delete_rule, rc.update_rule "+
			"FROM information_schema.table_constraints tc "+
			"JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name "+
			"JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name "+
			"JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name "+
			"WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '%s'",
		tableName)

	var fkConstraints []ForeignKeyInfo
	err := cli.Select(&fkConstraints, fkQuery)
	if err != nil {
		return nil, err
	}

	return fkConstraints, nil
}

// getTableIndexes retrieves indexes for a table (excluding primary keys)
func (s *postgresqlSql) getTableIndexes(cli DatabaseClient, database, tableName string) ([]IndexInfo, error) {
	indexQuery := fmt.Sprintf(
		"SELECT "+
			"i.relname as index_name, "+
			"a.attname as column_name, "+
			"ix.indisunique as is_unique "+
			"FROM pg_catalog.pg_class t "+
			"JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid "+
			"JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid "+
			"JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) "+
			"JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace "+
			"WHERE t.relkind = 'r' AND  t.relname = '%s' AND NOT ix.indisprimary",
		tableName)

	var indexes []IndexInfo
	err := cli.Select(&indexes, indexQuery)
	if err != nil {
		return nil, err
	}

	return indexes, nil
}

// getTableColumns retrieves column information for a table
func (s *postgresqlSql) getTableColumns(cli DatabaseClient, database, tableName string) ([]ColumnInfo, error) {
	columnsQuery := fmt.Sprintf(
		"SELECT column_name, data_type, character_maximum_length, is_nullable, column_default, "+
			"(SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d "+
			"WHERE d.adrelid = c.table_name::regclass AND d.adnum = c.ordinal_position) as default_expr "+
			"FROM information_schema.columns c "+
			"WHERE table_name = '%s' ORDER BY ordinal_position",
		tableName)

	var columns []ColumnInfo
	err := cli.Select(&columns, columnsQuery)
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (s *postgresqlSql) GetCreateTableSql(cli DatabaseClient, database, tableName string) (string, error) {
	// Get table columns
	columns, err := s.getTableColumns(cli, database, tableName)
	if err != nil {
		return "", err
	}

	// Get primary key information
	pkColumns, err := s.getPrimaryKeyColumns(cli, database, tableName)
	if err != nil {
		return "", err
	}

	// Get foreign key information
	fkConstraints, err := s.getForeignKeyConstraints(cli, database, tableName)
	if err != nil {
		return "", err
	}

	// Get index information
	indexes, err := s.getTableIndexes(cli, database, tableName)
	if err != nil {
		return "", err
	}

	// Build CREATE TABLE statement
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (\n", database, tableName))

	// Add columns
	for i, col := range columns {
		colName := col.ColumnName
		dataType := col.DataType
		var maxLength string
		if col.CharacterMaximumLength.Valid {
			maxLength = fmt.Sprintf("%d", col.CharacterMaximumLength.Int64)
		}
		isNullable := col.IsNullable
		var defaultValue, defaultExpr string
		if col.ColumnDefault.Valid {
			defaultValue = col.ColumnDefault.String
		}
		if col.DefaultExpr.Valid {
			defaultExpr = col.DefaultExpr.String
		}

		if i > 0 {
			sb.WriteString(",\n")
		}

		sb.WriteString(fmt.Sprintf("  \"%s\" %s", colName, dataType))

		// Add length for character types if specified
		if maxLength != "" && (strings.Contains(dataType, "char") || strings.Contains(dataType, "text")) {
			sb.WriteString(fmt.Sprintf("(%s)", maxLength))
		}

		// Add NOT NULL constraint if applicable
		if isNullable == "NO" {
			sb.WriteString(" NOT NULL")
		}

		// Add default value if specified
		if defaultValue != "" || defaultExpr != "" {
			if defaultExpr != "" {
				sb.WriteString(fmt.Sprintf(" DEFAULT %s", defaultExpr))
			} else {
				sb.WriteString(fmt.Sprintf(" DEFAULT %s", defaultValue))
			}
		}
	}

	// Add primary key constraint
	if len(pkColumns) > 0 {
		sb.WriteString(",\n  PRIMARY KEY (")
		for i, pk := range pkColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("\"%s\"", pk.ColumnName))
		}
		sb.WriteString(")")
	}

	// Add foreign key constraints
	for _, fk := range fkConstraints {
		colName := fk.ColumnName
		foreignSchema := fk.ForeignTableSchema
		foreignTable := fk.ForeignTableName
		foreignColumn := fk.ForeignColumnName
		deleteRule := fk.DeleteRule
		updateRule := fk.UpdateRule

		sb.WriteString(fmt.Sprintf(",\n  FOREIGN KEY (\"%s\") REFERENCES \"%s\".\"%s\" (\"%s\")",
			colName, foreignSchema, foreignTable, foreignColumn))

		// Add ON DELETE rule
		if deleteRule != "NO ACTION" {
			sb.WriteString(fmt.Sprintf(" ON DELETE %s", deleteRule))
		}

		// Add ON UPDATE rule
		if updateRule != "NO ACTION" {
			sb.WriteString(fmt.Sprintf(" ON UPDATE %s", updateRule))
		}
	}

	sb.WriteString("\n);\n")

	// Add indexes (excluding primary key which is already handled)
	indexMap := make(map[string][]IndexInfo)
	for _, idx := range indexes {
		indexName := idx.IndexName
		if _, ok := indexMap[indexName]; !ok {
			indexMap[indexName] = []IndexInfo{}
		}
		indexMap[indexName] = append(indexMap[indexName], idx)
	}

	for idxName, idxColumns := range indexMap {
		isUnique := false
		if len(idxColumns) > 0 {
			isUnique = idxColumns[0].IsUnique == "t"
		}

		if isUnique {
			sb.WriteString(fmt.Sprintf("\nCREATE UNIQUE INDEX \"%s\" ON \"%s\".\"%s\" (",
				idxName, database, tableName))
		} else {
			sb.WriteString(fmt.Sprintf("\nCREATE INDEX \"%s\" ON \"%s\".\"%s\" (",
				idxName, database, tableName))
		}

		for i, col := range idxColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("\"%s\"", col.ColumnName))
		}
		sb.WriteString(");\n")
	}

	// Get trigger SQL
	triggerSql, err := s.GetCreateTriggerSql(cli, database, tableName)
	if err != nil {
		return "", err
	}

	// Combine everything
	if len(triggerSql) > 0 {
		sb.WriteString("\n")
		sb.WriteString(strings.Join(triggerSql, "\n"))
	}

	return sb.String(), nil
}

func (s *postgresqlSql) GetCreateTriggerSql(cli DatabaseClient, database, tableName string) ([]string, error) {
	// PostgreSQL stores trigger definitions in pg_trigger and related catalogs
	triggerQuery := fmt.Sprintf(
		"SELECT t.tgname AS trigger_name, "+
			"pg_catalog.pg_get_triggerdef(t.oid, true) AS trigger_definition "+
			"FROM pg_catalog.pg_trigger t "+
			"JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid "+
			"JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "+
			"WHERE NOT t.tgisinternal "+
			"AND c.relname = '%s'",
		tableName)

	var triggers []TriggerInfo
	err := cli.Select(&triggers, triggerQuery)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, trigger := range triggers {
		triggerDef := trigger.TriggerDefinition
		if triggerDef != "" {
			// Add semicolon if not present
			if !strings.HasSuffix(triggerDef, ";") {
				triggerDef += ";"
			}
			ret = append(ret, triggerDef)
		}
	}

	return ret, nil
}

func (s *postgresqlSql) GetCreateViewSql(cli DatabaseClient, database, viewName string) (string, error) {
	// PostgreSQL stores view definitions in pg_views catalog
	viewQuery := fmt.Sprintf(
		"SELECT pg_catalog.pg_get_viewdef(c.oid, true) AS view_definition "+
			"FROM pg_catalog.pg_class c "+
			"JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "+
			"WHERE c.relkind = 'v' "+
			"AND c.relname = '%s'",
		viewName)

	var views []ViewInfo
	err := cli.Select(&views, viewQuery)
	if err != nil {
		return "", err
	}

	if len(views) > 0 {
		viewDef := views[0].ViewDefinition
		if viewDef != "" {
			// Format as CREATE VIEW statement
			return fmt.Sprintf("CREATE OR REPLACE VIEW \"%s\" AS\n%s;",
				viewName, viewDef), nil
		}
	}

	return "", fmt.Errorf("view %s not found or definition is empty", viewName)
}

func (s *postgresqlSql) GetCreateProcedureSql(cli DatabaseClient, database, procName string) (string, error) {
	// PostgreSQL stores procedure definitions in pg_proc catalog
	procQuery := fmt.Sprintf(
		"SELECT pg_catalog.pg_get_functiondef(p.oid) AS proc_definition, "+
			"p.proname AS proc_name, "+
			"p.prokind AS proc_kind "+
			"FROM pg_catalog.pg_proc p "+
			"JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid "+
			"WHERE p.prokind = 'p' "+
			"AND p.proname = '%s'",
		procName)

	var procedures []ProcedureInfo
	err := cli.Select(&procedures, procQuery)
	if err != nil {
		return "", err
	}

	if len(procedures) == 0 {
		return "", fmt.Errorf("procedure %s not found", procName)
	}

	procDef := procedures[0].ProcDefinition
	if procDef == "" {
		return "", fmt.Errorf("empty procedure definition for %s", procName)
	}

	// PostgreSQL's pg_get_functiondef already returns the complete CREATE PROCEDURE statement
	return procDef, nil
}

func (s *postgresqlSql) GetCreateFunctionSql(cli DatabaseClient, database, funcName string) (string, error) {
	// PostgreSQL stores function definitions in pg_proc catalog
	funcQuery := fmt.Sprintf(
		"SELECT pg_catalog.pg_get_functiondef(p.oid) AS func_definition "+
			"FROM pg_catalog.pg_proc p "+
			"JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid "+
			"WHERE p.prokind = 'f' "+
			"AND p.proname = '%s'",
		funcName)

	var functions []FunctionInfo
	err := cli.Select(&functions, funcQuery)
	if err != nil {
		return "", err
	}

	if len(functions) == 0 {
		return "", fmt.Errorf("function %s not found", funcName)
	}

	funcDef := functions[0].FuncDefinition
	if funcDef == "" {
		return "", fmt.Errorf("empty function definition for %s", funcName)
	}

	// PostgreSQL's pg_get_functiondef already returns the complete CREATE FUNCTION statement
	return funcDef, nil
}

func (s *postgresqlSql) GetCreateEventSql(cli DatabaseClient, database, eventName string) (string, error) {
	// PostgreSQL doesn't have events like MySQL, but it can use pg_cron extension for scheduled jobs
	// Check if pg_cron extension is available
	extensionQuery := "SELECT extname FROM pg_extension WHERE extname = 'pg_cron'"
	var extensions []struct {
		Extname string `db:"extname"`
	}
	err := cli.Select(&extensions, extensionQuery)
	if err != nil {
		return "", err
	}

	if len(extensions) == 0 {
		return "", fmt.Errorf("pg_cron extension not installed, PostgreSQL doesn't support events like MySQL")
	}

	// Try to find a scheduled job with the given name
	cronQuery := fmt.Sprintf(
		"SELECT jobname, schedule, command "+
			"FROM cron.job "+
			"WHERE jobname = '%s'",
		eventName)

	var events []EventInfo
	err = cli.Select(&events, cronQuery)
	if err != nil {
		// If the query fails, it might be because the cron schema is not accessible
		return "", fmt.Errorf("could not query cron jobs: %v", err)
	}

	if len(events) == 0 {
		return "", fmt.Errorf("scheduled job %s not found", eventName)
	}

	// Format as a comment explaining how to recreate this in PostgreSQL
	jobName := events[0].JobName
	schedule := events[0].Schedule
	command := events[0].Command

	eventSQL := fmt.Sprintf(
		"-- PostgreSQL scheduled job using pg_cron\n"+
			"-- To create this job, run:\n"+
			"SELECT cron.schedule('%s', '%s', '%s');\n"+
			"-- To remove this job, run:\n"+
			"SELECT cron.unschedule('%s');\n",
		schedule, jobName, command, jobName)

	return eventSQL, nil
}
