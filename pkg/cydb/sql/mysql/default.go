package sqlmysql

import (
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
	_ "github.com/go-sql-driver/mysql"
)

func NewCopy() DatabaseTransformer {
	return &mysqlSql{}
}

type mysqlSql struct {
}

func init() {
	RegisterSqlDialect("mysql", &mysqlSql{})
}

func (s *mysqlSql) GetTableColumns(cli DatabaseClient, database, tableName string) ([]*DBColumn, error) {
	query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE,COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", database, tableName)
	rows, err := cli.Query(query)
	if err != nil {
		return nil, err
	}

	columns := make([]*DBColumn, 0)
	for _, row := range rows {
		var colName, dataType, columnKey string
		colName = cyutil.ToStr(row["COLUMN_NAME"])
		dataType = cyutil.ToStr(row["DATA_TYPE"])
		columnKey = cyutil.ToStr(row["COLUMN_KEY"])

		// Map MySQL data types to Go reflect.Kind.
		switch strings.ToLower(dataType) {
		case "tinyint", "smallint", "mediumint", "int", "bigint":
			columns = append(columns, &DBColumn{Name: colName, DBFieldType: DBFieldTypeInt, ColumnKey: columnKey, OrgDataType: dataType})
		case "float", "double", "decimal":
			columns = append(columns, &DBColumn{Name: colName, DBFieldType: DBFieldTypeFloat, ColumnKey: columnKey, OrgDataType: dataType})
		case "date", "datetime", "timestamp":
			columns = append(columns, &DBColumn{Name: colName, DBFieldType: DBFieldTypeTime, ColumnKey: columnKey, OrgDataType: dataType})
		case "blob", "binary", "varbinary", "longblob", "mediumblob", "tinyblob":
			columns = append(columns, &DBColumn{Name: colName, DBFieldType: DBFieldTypeBinary, ColumnKey: columnKey, OrgDataType: dataType})
		case "json":
			columns = append(columns, &DBColumn{Name: colName, DBFieldType: DBFieldTypeJson, ColumnKey: columnKey, OrgDataType: dataType})
		case "bit":
			columns = append(columns, &DBColumn{Name: colName, DBFieldType: DBFieldTypeBit, ColumnKey: columnKey, OrgDataType: dataType})
		default:
			columns = append(columns, &DBColumn{Name: colName, DBFieldType: DBFieldTypeString, ColumnKey: columnKey, OrgDataType: dataType})
		}
	}
	return columns, nil
}

func (s *mysqlSql) IsTableExist(cli DatabaseClient, tableName string) (bool, error) {
	sql := "SELECT COUNT(1) as count FROM information_schema.tables WHERE table_name = :tableName AND table_schema = :database;"
	params := map[string]interface{}{"tableName": tableName, "database": cli.Database()}
	result, err := cli.NQueryOne(sql, params)
	if err != nil {
		return false, err
	}

	count := cyutil.GetInt(result, "count")
	return count > 0, nil
}

func (s *mysqlSql) PreProcess(sql string, _ ...int) string {
	sql = strings.TrimSpace(sql)
	return strings.TrimSuffix(sql, ";") + "\n"
}

func (s *mysqlSql) FormatValue(cli DatabaseClient, fd *FieldData) (string, error) {
	return FormatValue(fd.Data, fd.Type)
}

func (s *mysqlSql) GetReplaceSql(cli DatabaseClient, table string, rd *RowData) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("REPLACE INTO `%s` (", table))

	// Build column names
	for i, fd := range rd.Data {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("`%s`", fd.Name))
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

	sb.WriteString(");")
	return sb.String(), nil
}

func (s *mysqlSql) MakeSureDBExists(cli DatabaseClient, dbName string) error {
	_, err := cli.Excute(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
	if err != nil {
		return err
	}
	return nil
}

func (s *mysqlSql) GetConnectStr(dbConn *DBConnection) (string, string) {
	return "mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConn.Un, dbConn.Pw, dbConn.Host, dbConn.Port, dbConn.DBName)
}

func (s *mysqlSql) GetDefaultTypeName(tp DefaultDBFieldType) string {
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
