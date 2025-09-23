package sqlpostgresql

import (
	"fmt"
	"net/url"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
	_ "github.com/lib/pq"
)

type postgresqlSql struct {
}

func init() {
	RegisterSqlDialect("postgresql", &postgresqlSql{})
}

var _ SQLDialect = (*postgresqlSql)(nil)

func (s *postgresqlSql) GetTableColumns(cli DatabaseClient, database, tableName string) ([]*DBColumn, error) {
	// Get column information using the getTableColumns function
	colInfos, err := s.getTableColumns(cli, database, tableName)
	if err != nil {
		return nil, err
	}

	// Get primary key information
	pkInfos, err := s.getPrimaryKeyColumns(cli, database, tableName)
	if err != nil {
		return nil, err
	}

	// Create a map of primary key columns for quick lookup
	pkMap := make(map[string]bool)
	for _, pk := range pkInfos {
		pkMap[pk.ColumnName] = true
	}

	// Assemble the DBColumn objects
	columns := make([]*DBColumn, 0, len(colInfos))
	for _, colInfo := range colInfos {
		var constraintType string
		if pkMap[colInfo.ColumnName] {
			constraintType = "PK"
		}

		// Set column key based on constraint type (PK, FK, etc)
		columnKey := ""
		if constraintType == "PK" {
			columnKey = "PRI"
		}

		// Map PostgreSQL data types to DBFieldType
		dataType := colInfo.DataType
		var fieldType DBFieldType
		switch strings.ToLower(dataType) {
		case "integer", "smallint", "bigint", "serial", "bigserial":
			fieldType = DBFieldTypeInt
		case "numeric", "decimal", "real", "double precision":
			fieldType = DBFieldTypeFloat
		case "date", "timestamp", "timestamptz", "time", "timetz":
			fieldType = DBFieldTypeTime
		case "bytea":
			fieldType = DBFieldTypeBinary
		case "json", "jsonb":
			fieldType = DBFieldTypeJson
		case "bit", "bit varying":
			fieldType = DBFieldTypeBit
		case "boolean":
			// Map boolean to int for compatibility
			fieldType = DBFieldTypeInt
		default:
			// text, varchar, char, etc.
			fieldType = DBFieldTypeString
		}

		// Create DBColumn object and append to result
		columns = append(columns, &DBColumn{
			Name:        colInfo.ColumnName,
			DBFieldType: fieldType,
			ColumnKey:   columnKey,
			OrgDataType: dataType,
			Nullable:    colInfo.IsNullable == "YES",
		})
	}
	return columns, nil
}

func (s *postgresqlSql) IsTableExist(cli DatabaseClient, tableName string) (bool, error) {
	// In PostgreSQL, we should check both table_name and table_schema (default is 'public')
	// This query checks if the table exists in the public schema or the current schema
	sql := "SELECT COUNT(1) as count FROM information_schema.tables WHERE table_name = :tableName AND table_schema IN ('public', current_schema())"
	params := map[string]interface{}{"tableName": tableName}
	result, err := InternalNQueryOne(cli, sql, params)
	if err != nil {
		return false, err
	}

	count := cyutil.GetInt(result, "count")
	return count > 0, nil
}

func (s *postgresqlSql) FormatValue(cli DatabaseClient, fd *FieldData) (string, error) {
	return FormatValue(fd.Data, fd.Type)
}

func (s *postgresqlSql) GetReplaceSql(cli DatabaseClient, table string, rd *RowData) (string, error) {
	var sb strings.Builder

	// PostgreSQL doesn't support REPLACE INTO directly
	// Instead, we use INSERT ... ON CONFLICT ... DO UPDATE

	// Find primary key fields to use in the ON CONFLICT clause
	pkFields := make([]*FieldData, 0)
	nonPkFields := make([]*FieldData, 0)

	for _, fd := range rd.Data {
		if fd.IsPK {
			pkFields = append(pkFields, fd)
		} else {
			nonPkFields = append(nonPkFields, fd)
		}
	}

	// If no PK fields found, use all fields for conflict detection
	// This is a fallback and might not work correctly in all cases
	if len(pkFields) == 0 {
		pkFields = rd.Data
		nonPkFields = rd.Data
	}

	// Start building the INSERT statement
	sb.WriteString(fmt.Sprintf("INSERT INTO \"%s\" (", table))

	// Build column names
	for i, fd := range rd.Data {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("\"%s\"", fd.Name))
	}

	sb.WriteString(") VALUES (")

	// Build values
	for i, fd := range rd.Data {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmtVal, err := s.FormatValue(cli, fd)
		if err != nil {
			return "", err
		}
		sb.WriteString(fmtVal)
	}

	// Add ON CONFLICT clause
	sb.WriteString(") ON CONFLICT (")

	// Add PK fields to conflict clause
	for i, fd := range pkFields {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("\"%s\"", fd.Name))
	}

	// Add DO UPDATE SET clause
	sb.WriteString(") DO UPDATE SET ")

	// Update all non-PK fields
	updateCount := 0
	for _, fd := range nonPkFields {
		// Skip primary key fields in the update clause
		if fd.IsPK {
			continue
		}

		if updateCount > 0 {
			sb.WriteString(", ")
		}

		fmtVal, err := s.FormatValue(cli, fd)
		if err != nil {
			return "", err
		}

		sb.WriteString(fmt.Sprintf("\"%s\" = %s", fd.Name, fmtVal))
		updateCount++
	}

	// If no fields to update, use a dummy update that doesn't change anything
	if updateCount == 0 {
		// Find a field to use for dummy update
		if len(rd.Data) > 0 {
			fd := rd.Data[0]
			sb.WriteString(fmt.Sprintf("\"%s\" = \"%s\"", fd.Name, fd.Name))
		}
	}

	sb.WriteString(";")
	return sb.String(), nil
}

func (s *postgresqlSql) MakeSureDBExists(cli DatabaseClient, dbName string) error {
	// PostgreSQL doesn't support IF NOT EXISTS in CREATE DATABASE before version 9.1
	// We need to check if the database exists first
	checkSQL := "SELECT COUNT(1) FROM pg_database WHERE datname = :dbname"
	params := map[string]interface{}{"dbname": strings.ToLower(dbName)}

	result, err := cli.NQueryOne(checkSQL, params)
	if err != nil {
		return err
	}

	count := cyutil.GetInt(result, "count")
	if count == 0 {
		// Database doesn't exist, create it
		// Note: PostgreSQL identifiers are case-sensitive unless quoted
		_, err = cli.Excute(fmt.Sprintf("CREATE DATABASE \"%s\"", dbName))
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *postgresqlSql) GetConnectStr(dbConn *DBConnection) (string, string) {
	// PostgreSQL connection string format: "postgres://username:password@host:port/dbname?param=value"
	if dbConn.DBName == "" {
		dbConn.DBName = "postgres"
	}
	// Start with base URL
	baseURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		dbConn.Un, url.QueryEscape(dbConn.Pw), dbConn.Host, dbConn.Port, dbConn.DBName)

	// Add query parameters
	params := make([]string, 0)

	// Add SSL mode if specified, default to disable
	sslMode := "disable"
	if dbConn.SSLMode != "" {
		sslMode = dbConn.SSLMode
	}
	params = append(params, fmt.Sprintf("sslmode=%s", sslMode))

	// Add schema if specified
	if dbConn.Schema != "" {
		params = append(params, fmt.Sprintf("search_path=%s", dbConn.Schema))
	}

	// Combine URL and parameters
	connStr := baseURL
	if len(params) > 0 {
		connStr += "?" + strings.Join(params, "&")
	}

	return "postgres", connStr
}

func (s *postgresqlSql) GetDefaultTypeName(tp DefaultDBFieldType) string {
	switch tp {
	case DefaultDBFieldTypeString:
		return "TEXT"
	case DefaultDBFieldTypeInt:
		return "INTEGER"
	case DefaultDBFieldTypeFloat:
		return "NUMERIC"
	case DefaultDBFieldTypeBool:
		return "BOOLEAN"
	case DefaultDBFieldTypeTime:
		return "TIMESTAMP"
	case DefaultDBFieldTypeBinary:
		return "BYTEA"
	case DefaultDBFieldTypeJson:
		return "JSONB"
	case DefaultDBFieldTypeBit:
		return "BIT"
	default:
		return "TEXT"
	}
}
