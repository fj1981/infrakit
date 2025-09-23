package sqlsqlite

import (
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/jmoiron/sqlx"
	_ "github.com/logoove/sqlite"
)

type sqliteSql struct {
}

// GetReplaceSql implements cydb.SQLDialect.
func (s *sqliteSql) GetReplaceSql(cli DatabaseClient, table string, rd *RowData) (string, error) {
	panic("unimplemented")
}

func init() {
	sqlx.BindDriver("sqlite", sqlx.QUESTION)
	RegisterSqlDialect("sqlite", &sqliteSql{})
}

var _ SQLDialect = (*sqliteSql)(nil)

// GetDDLSqlFunc implements database.ISql.
func (s *sqliteSql) GetDDLSqlFunc(funcName DDLSqlFuncName) (DDLSqlFunc, error) {
	panic("unimplemented")
}

// GetSortedSqlFunc implements database.ISql.
func (s *sqliteSql) GetSortedSqlFunc(funcName SortFuncName) (SortedSqlFunc, error) {
	panic("unimplemented")
}

// GetTableColumns implements database.ISql.
func (s *sqliteSql) GetTableColumns(cli DatabaseClient, database string, tableName string) ([]*DBColumn, error) {
	// SQLite stores table schema information in the sqlite_master table and PRAGMA table_info
	// First check if the table exists
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
	rows, err := cli.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("error checking if table exists: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Use PRAGMA table_info to get column information
	query = fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err = cli.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting table columns: %w", err)
	}

	var columns []*DBColumn
	for _, row := range rows {
		// PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
		var colName, dataType string
		var nullable bool
		var isPK bool

		colName = cyutil.GetStr(row, "name")
		dataType = cyutil.GetStr(row, "type")
		nullable = cyutil.GetInt(row, "notnull") == 0
		isPK = cyutil.GetInt(row, "pk") == 1

		column := &DBColumn{
			Name:        colName,
			OrgDataType: dataType,
			Nullable:    nullable,
		}

		// Set column key if it's a primary key
		if isPK {
			column.ColumnKey = "PRI"
		}

		// Map SQLite data types to DBFieldType
		// SQLite has dynamic typing, but we can infer from type affinity
		dataTypeLower := strings.ToLower(dataType)

		// Check for integer types
		if strings.Contains(dataTypeLower, "int") {
			column.DBFieldType = DBFieldTypeInt
		} else if strings.Contains(dataTypeLower, "real") || strings.Contains(dataTypeLower, "float") ||
			strings.Contains(dataTypeLower, "double") || strings.Contains(dataTypeLower, "decimal") {
			column.DBFieldType = DBFieldTypeFloat
		} else if strings.Contains(dataTypeLower, "date") || strings.Contains(dataTypeLower, "time") {
			column.DBFieldType = DBFieldTypeTime
		} else if strings.Contains(dataTypeLower, "blob") || strings.Contains(dataTypeLower, "binary") {
			column.DBFieldType = DBFieldTypeBinary
		} else if strings.Contains(dataTypeLower, "json") {
			column.DBFieldType = DBFieldTypeJson
		} else if strings.Contains(dataTypeLower, "bool") || strings.Contains(dataTypeLower, "boolean") {
			column.DBFieldType = DBFieldTypeInt
		} else {
			// Default to string for text and other types
			column.DBFieldType = DBFieldTypeString
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// IsTableExist implements database.ISql.
func (s *sqliteSql) IsTableExist(cli DatabaseClient, tableName string) (bool, error) {
	// SQLite stores table information in the sqlite_master table
	query := "SELECT count(*) as count FROM sqlite_master WHERE type='table' AND name=?"
	var count int
	if err := cli.Get(&count, query, tableName); err != nil {
		return false, fmt.Errorf("error checking if table exists: %w", err)
	}

	return count > 0, nil
}

// MakeSureDBExists implements database.ISql.
func (s *sqliteSql) MakeSureDBExists(cli DatabaseClient, dbName string) error {
	// For SQLite, the database is created automatically when connecting to it
	// if it doesn't exist. We just need to make sure we can connect to it.

	// If we already have a connection, we're good
	if cli != nil {
		return nil
	}

	// Otherwise, try to open a connection to the database file
	db, err := sqlx.Open("sqlite", dbName)
	if err != nil {
		return fmt.Errorf("failed to ensure SQLite database exists: %w", err)
	}
	defer db.Close()

	// Ping the database to verify connection
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("failed to connect to SQLite database: %w", err)
	}

	return nil
}

// PreProcess implements database.ISql.
func (s *sqliteSql) PreProcess(sql string, param ...int) string {
	return sql
}

// GetConnectStr implements database.ISql.
func (s *sqliteSql) GetConnectStr(dbConn *DBConnection) (string, string) {
	return "sqlite", dbConn.Path
}

func (s *sqliteSql) GetDefaultTypeName(tp DefaultDBFieldType) string {
	switch tp {
	case DefaultDBFieldTypeString:
		return "VARCHAR(255)"
	case DefaultDBFieldTypeInt:
		return "INT"
	case DefaultDBFieldTypeFloat:
		return "FLOAT"
	case DefaultDBFieldTypeBool:
		return "TINYINT(1)"
	case DefaultDBFieldTypeTime:
		return "DATETIME"

	default:
		return ""
	}
}
