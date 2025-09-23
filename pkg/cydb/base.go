package cydb

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fj1981/infrakit/pkg/cyutil"
)

// Config is the main configuration struct, holding a list of database connections.
type Config struct {
	Connections []DBConnection `yaml:"connections"`
}

// DBConnection is a universal struct for any database connection.
// It includes fields for MySQL, Oracle, PostgreSQL, and SQLite.
type DBConnection struct {
	// Key is a unique identifier for the connection (e.g., "primary_db", "log_db").
	Key string `yaml:"key"`
	// Type specifies the database type, e.g., "mysql", "oracle", "sqlite", "postgresql".
	Type string `yaml:"type"`

	// Fields for MySQL, PostgreSQL and Oracle
	Host   string `yaml:"host,omitempty"`
	Port   int    `yaml:"port,omitempty"`
	Un     string `yaml:"un,omitempty"`
	Pw     string `yaml:"pw,omitempty"`
	DBName string `yaml:"dbname,omitempty"`

	// Fields for Oracle
	Service string `yaml:"service,omitempty"`
	Role    string `yaml:"role,omitempty"`

	// Field for SQLite
	Path string `yaml:"path,omitempty"`

	// Fields for PostgreSQL
	SSLMode string `yaml:"sslmode,omitempty"` // disable, require, verify-ca, verify-full
	Schema  string `yaml:"schema,omitempty"`  // default schema (search_path)
}

func GetDBAndTable(cli DatabaseClient, name ...string) (string, string) {
	if len(name) == 1 {
		return cli.Database(), name[0]
	}
	if len(name) == 2 {
		return name[0], name[1]
	}
	return cli.Database(), ""
}

// Helper function to format values based on their expected type.
func FormatValue(v interface{}, colType DBFieldType) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	switch colType {
	case DBFieldTypeString:
		escaped := cyutil.ToStr(v)
		escaped = strings.ReplaceAll(escaped, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `'`, `\'`)
		escaped = strings.ReplaceAll(escaped, "`", "\\`")
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		escaped = strings.ReplaceAll(escaped, "\n", `\n`)
		escaped = strings.ReplaceAll(escaped, "\r", `\r`)
		return fmt.Sprintf(`'%s'`, escaped), nil
	case DBFieldTypeInt:
		switch v := v.(type) {
		case int:
			return fmt.Sprintf("%d", v), nil
		case int32:
			return fmt.Sprintf("%d", v), nil
		case int64:
			return fmt.Sprintf("%d", v), nil
		case uint:
			return fmt.Sprintf("%d", v), nil
		case uint32:
			return fmt.Sprintf("%d", v), nil
		case uint64:
			return fmt.Sprintf("%d", v), nil
		case int8:
			return fmt.Sprintf("%d", v), nil
		case int16:
			return fmt.Sprintf("%d", v), nil
		case uint8:
			return fmt.Sprintf("%d", v), nil
		case uint16:
			return fmt.Sprintf("%d", v), nil
		case float32:
			return fmt.Sprintf("%d", int64(v)), nil
		case float64:
			return fmt.Sprintf("%d", int64(v)), nil
		case string:
			return fmt.Sprintf("'%s'", v), nil
		}
	case DBFieldTypeFloat:
		switch v := v.(type) {
		case float32:
			return fmt.Sprintf("%.15g", float64(v)), nil
		case float64:
			return fmt.Sprintf("%.15g", v), nil
		}
	case DBFieldTypeTime:
		if v, ok := v.(time.Time); ok {
			return fmt.Sprintf(`'%s'`, v.Format("2006-01-02 15:04:05")), nil
		}
		if v, ok := v.(string); ok {
			return fmt.Sprintf(`'%s'`, v), nil
		}
	case DBFieldTypeBinary:
		if v, ok := v.([]byte); ok {
			if len(v) == 0 {
				return "''", nil
			}
			return fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString(v))), nil
		}
		if v, ok := v.(string); ok {
			if v == "" {
				return "''", nil
			}
			return fmt.Sprintf("0x%s", strings.ToUpper(hex.EncodeToString([]byte(v)))), nil
		}
	case DBFieldTypeJson:
		if v, ok := v.(string); ok {
			return fmt.Sprintf("'%s'", v), nil
		}
	case DBFieldTypeBit:
		if v, ok := v.(string); ok {
			num := 0
			switch v {
			case "\x01":
				num = 1
			case "\x00":
				num = 0
			default:
				return "", fmt.Errorf("unknown bit value: %v", v)
			}
			// 将整数转换成二进制字符串
			binStr := fmt.Sprintf("b'%b'", num)
			return binStr, nil
		}
	}
	if v, ok := v.(string); ok {
		return fmt.Sprintf("'%s'", v), nil
	}
	return "", fmt.Errorf("unknown type: %v, colType: %v", v, colType)
}

func InParam(ids []string) string {
	r := []string{}
	for _, l := range ids {
		r = append(r, "'"+l+"'")
	}
	return strings.Join(r, ",")
}

func InParam2(ids [][]string) string {
	r := []string{}
	for _, l := range ids {
		r = append(r, "("+InParam(l)+")")
	}
	return strings.Join(r, ",")
}

func InMapParam(ids map[string]struct{}) string {
	r := []string{}
	for k := range ids {
		r = append(r, "'"+k+"'")
	}
	return strings.Join(r, ",")
}

type RowData struct {
	Data []*FieldData
}

func (r *RowData) GetData() map[string]interface{} {
	ret := make(map[string]interface{})
	for _, fd := range r.Data {
		ret[fd.Name] = fd.Data
	}
	return ret
}
func (r *RowData) GetReplaceSql(cli DatabaseClient, table string) (string, error) {
	if sqlFunc, ok := sqlFuncMap[cli.DBType()]; ok {
		return sqlFunc.GetReplaceSql(cli, table, r)
	}
	return "", errors.New("not support db type: " + cli.DBType())
}

var sqlFuncMap = map[string]SQLDialect{}
var sqlTransformerMap = map[string]DatabaseTransformer{}

func RegisterSqlDialect(dbtype string, sql any) {
	if _, ok := sql.(SQLDialect); ok {
		sqlFuncMap[dbtype] = sql.(SQLDialect)
	}
	if _, ok := sql.(DatabaseTransformer); ok {
		sqlTransformerMap[dbtype] = sql.(DatabaseTransformer)
	}
}

func GetSqlDialect(dbtype string) (SQLDialect, bool) {
	r, ok := sqlFuncMap[dbtype]
	return r, ok
}

func GetSupportSqlDialect() []string {
	ret := make([]string, 0)
	for k := range sqlFuncMap {
		ret = append(ret, k)
	}
	return ret
}

func GetSqlTransformer(dbtype string) (DatabaseTransformer, bool) {
	r, ok := sqlTransformerMap[dbtype]
	return r, ok
}

func EscapeLikePattern(pattern string, t LikePatternType) string {
	if t == None {
		return pattern
	}
	// First, escape the escape character itself
	pattern = strings.ReplaceAll(pattern, `\`, `\\`)
	// Then escape all '%' and '_' characters
	pattern = strings.ReplaceAll(pattern, `%`, `\%`)
	pattern = strings.ReplaceAll(pattern, `_`, `\_`)
	switch t {
	case Contains:
		pattern = "%" + pattern + "%"
	case StartsWith:
		pattern = pattern + "%"
	case EndsWith:
		pattern = "%" + pattern
	}
	return pattern
}
