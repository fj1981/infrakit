package sqloracle

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/jmoiron/sqlx"
	go_ora "github.com/sijms/go-ora/v2"
)

func init() {
	sqlx.BindDriver("oracle", sqlx.NAMED)
	RegisterSqlDialect("oracle", &oracleSql{})
}

// Oracle reserved keywords that need to be quoted
// Based on Oracle Database 21c SQL Language Reference
// https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Oracle-SQL-Reserved-Words.html
var oracleReservedKeywords = map[string]struct{}{
	// 大写版本
	"ACCESS": {}, "ADD": {}, "ALL": {}, "ALTER": {}, "AND": {}, "ANY": {},
	"AS": {}, "ASC": {}, "AUDIT": {}, "BETWEEN": {}, "BY": {}, "CHAR": {},
	"CHECK": {}, "CLUSTER": {}, "COLUMN": {}, "COLUMN_VALUE": {}, "COMMENT": {}, "COMPRESS": {},
	"CONNECT": {}, "CREATE": {}, "CURRENT": {}, "DATE": {}, "DECIMAL": {}, "DEFAULT": {},
	"DELETE": {}, "DESC": {}, "DISTINCT": {}, "DROP": {}, "ELSE": {}, "EXCLUSIVE": {},
	"EXISTS": {}, "FILE": {}, "FLOAT": {}, "FOR": {}, "FROM": {}, "GRANT": {},
	"GROUP": {}, "HAVING": {}, "IDENTIFIED": {}, "IMMEDIATE": {}, "IN": {}, "INCREMENT": {},
	"INDEX": {}, "INITIAL": {}, "INSERT": {}, "INTEGER": {}, "INTERSECT": {}, "INTO": {},
	"IS": {}, "LEVEL": {}, "LIKE": {}, "LOCK": {}, "LONG": {}, "MAXEXTENTS": {},
	"MINUS": {}, "MLSLABEL": {}, "MODE": {}, "MODIFY": {}, "NESTED_TABLE_ID": {}, "NOAUDIT": {},
	"NOCOMPRESS": {}, "NOT": {}, "NOWAIT": {}, "NULL": {}, "NUMBER": {}, "OF": {},
	"OFFLINE": {}, "ON": {}, "ONLINE": {}, "OPTION": {}, "OR": {}, "ORDER": {},
	"PCTFREE": {}, "PRIOR": {}, "PUBLIC": {}, "RAW": {}, "RENAME": {}, "RESOURCE": {},
	"REVOKE": {}, "ROW": {}, "ROWID": {}, "ROWNUM": {}, "ROWS": {}, "SELECT": {},
	"SESSION": {}, "SET": {}, "SHARE": {}, "SIZE": {}, "SMALLINT": {}, "START": {},
	"SUCCESSFUL": {}, "SYNONYM": {}, "SYSDATE": {}, "TABLE": {}, "THEN": {}, "TO": {},
	"TRIGGER": {}, "UID": {}, "UNION": {}, "UNIQUE": {}, "UPDATE": {}, "USER": {},
	"VALIDATE": {}, "VALUES": {}, "VARCHAR": {}, "VARCHAR2": {}, "VIEW": {}, "WHENEVER": {},
	"WHERE": {}, "WITH": {},

	// 小写版本
	"access": {}, "add": {}, "all": {}, "alter": {}, "and": {}, "any": {},
	"as": {}, "asc": {}, "audit": {}, "between": {}, "by": {}, "char": {},
	"check": {}, "cluster": {}, "column": {}, "column_value": {}, "comment": {}, "compress": {},
	"connect": {}, "create": {}, "current": {}, "date": {}, "decimal": {}, "default": {},
	"delete": {}, "desc": {}, "distinct": {}, "drop": {}, "else": {}, "exclusive": {},
	"exists": {}, "file": {}, "float": {}, "for": {}, "from": {}, "grant": {},
	"group": {}, "having": {}, "identified": {}, "immediate": {}, "in": {}, "increment": {},
	"index": {}, "initial": {}, "insert": {}, "integer": {}, "intersect": {}, "into": {},
	"is": {}, "level": {}, "like": {}, "lock": {}, "long": {}, "maxextents": {},
	"minus": {}, "mlslabel": {}, "mode": {}, "modify": {}, "nested_table_id": {}, "noaudit": {},
	"nocompress": {}, "not": {}, "nowait": {}, "null": {}, "number": {}, "of": {},
	"offline": {}, "on": {}, "online": {}, "option": {}, "or": {}, "order": {},
	"pctfree": {}, "prior": {}, "public": {}, "raw": {}, "rename": {}, "resource": {},
	"revoke": {}, "row": {}, "rowid": {}, "rownum": {}, "rows": {}, "select": {},
	"session": {}, "set": {}, "share": {}, "size": {}, "smallint": {}, "start": {},
	"successful": {}, "synonym": {}, "sysdate": {}, "table": {}, "then": {}, "to": {},
	"trigger": {}, "uid": {}, "union": {}, "unique": {}, "update": {}, "user": {},
	"validate": {}, "values": {}, "varchar": {}, "varchar2": {}, "view": {}, "whenever": {},
	"where": {}, "with": {},
}

type oracleSql struct {
}

// ReadSQLFile implements database.ISql.

func ConvertReservedKeywords(name string) string {
	if _, ok := oracleReservedKeywords[name]; ok {
		return name + "_"
	}
	return strings.ReplaceAll(name, "-", "_")
}

func WrapperSqlIngoreExist(sql string) string {
	sql = strings.ReplaceAll(sql, "'", "''")
	if len(strings.TrimSpace(sql)) == 0 {
		return ""
	}
	return fmt.Sprintf("BEGIN\n  EXECUTE IMMEDIATE '%s';\nEXCEPTION\n  WHEN OTHERS THEN\n    IF SQLCODE = -955 OR SQLCODE = -1408 OR SQLCODE = -02256 THEN\n      NULL; \n    ELSE\n      RAISE; \n    END IF;\nEND;\n/\n", sql)
}

func (s *oracleSql) substractDefault(v sql.NullString) string {
	if !v.Valid || v.String == "" {
		return ""
	}

	// Clean up the default value by removing newlines and extra spaces
	defaultVal := strings.ReplaceAll(v.String, "\n", " ")
	defaultVal = strings.TrimSpace(defaultVal)

	// Find the last occurrence of a single quote
	lastQuoteIndex := strings.LastIndex(defaultVal, "'")

	// Handle comments based on quotes
	if lastQuoteIndex >= 0 {
		// Look for comments after the last quote
		if commentIndex := strings.Index(defaultVal[lastQuoteIndex+1:], "--"); commentIndex >= 0 {
			defaultVal = strings.TrimSpace(defaultVal[:lastQuoteIndex+1+commentIndex])
		}
	} else {
		// No quotes, just look for comments
		if commentIndex := strings.Index(defaultVal, "--"); commentIndex >= 0 {
			defaultVal = strings.TrimSpace(defaultVal[:commentIndex])
		}
	}

	// Check if it's a quoted string after comment removal
	if strings.HasPrefix(defaultVal, "'") && strings.HasSuffix(defaultVal, "'") {
		// Keep the quotes intact as they're part of the SQL syntax for string literals
		return defaultVal
	}

	// Handle special Oracle default values
	if strings.ToUpper(defaultVal) == "NULL" {
		return "NULL"
	}

	return defaultVal
}

func buildDataType(c ColumnInfo) string {
	switch t := strings.ToUpper(c.DataType); t {
	case "NUMBER":
		if !c.DataPrecision.Valid {
			return "NUMBER"
		}
		if !c.DataScale.Valid || c.DataScale.Int64 == 0 {
			return fmt.Sprintf("NUMBER(%d)", c.DataPrecision.Int64)
		}
		return fmt.Sprintf("NUMBER(%d,%d)", c.DataPrecision.Int64, c.DataScale.Int64)

	case "CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2":
		/*
			if c.CharUsed.Valid && c.CharUsed.String == "C" {
				return fmt.Sprintf("%s(%d CHAR)", t, c.CharLength.Int64)
			}
		*/
		return fmt.Sprintf("%s(%d)", t, c.CharLength.Int64)
	default:
		return t
	}
}

// isConnectionClosedError 检查错误是否是连接已关闭错误
func isConnectionClosedError(err error) bool {
	if err == nil {
		return false
	}
	// 检查 Oracle 驱动返回的连接关闭错误
	return strings.Contains(err.Error(), "connection was closed") ||
		strings.Contains(err.Error(), "ORA-00028") ||
		strings.Contains(err.Error(), "DPI-1080")
}

func AddSemicolon(sql string) string {
	if sql == "" {
		return ""
	}
	sql = strings.TrimSpace(sql)
	if strings.HasSuffix(sql, ";") {
		return sql + "\n"
	}
	return sql + ";\n"
}

func (s *oracleSql) GetTableColumns(j DatabaseClient, database, tableName string) ([]*DBColumn, error) {

	tableName = ConvertReservedKeywords(tableName)
	tableName = strings.ToUpper(tableName)
	database = strings.ToUpper(database)
	cols, err := getColumns(j, database, tableName)
	if err != nil {
		return nil, err
	}
	constraints, err := getConstraints(j, database, tableName)
	if err != nil {
		return nil, err
	}
	pkMap := make(map[string]struct{})
	for _, constraint := range constraints {
		if constraint.ConstraintType == "P" {
			cn := strings.ToUpper(constraint.ColumnName)
			pkMap[cn] = struct{}{}
		}
	}
	uqMap := make(map[string]struct{})
	for _, constraint := range constraints {
		if constraint.ConstraintType == "U" {
			cn := strings.ToUpper(constraint.ColumnName)
			uqMap[cn] = struct{}{}
		}
	}

	// Map to store column information
	columns := []*DBColumn{}

	// Process each column
	for _, col := range cols {
		columnName := strings.ToUpper(col.ColumnName)
		if columnName == "" {
			continue
		}

		dataType := strings.ToUpper(col.DataType)
		isPrimaryKey := false
		if _, ok := pkMap[columnName]; ok {
			isPrimaryKey = true
		}

		// Map Oracle data types to internal DBFieldType
		var fieldType DBFieldType
		switch dataType {
		case "NUMBER":
			// For Oracle NUMBER type, check if it's an integer or float
			dataScale := col.DataScale
			if dataScale.Valid && dataScale.Int64 != 0 {
				fieldType = DBFieldTypeFloat // Decimal number
			} else {
				fieldType = DBFieldTypeInt // Integer number
			}
		case "VARCHAR2", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR2", "CLOB", "NCLOB":
			fieldType = DBFieldTypeString
		case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
			fieldType = DBFieldTypeTime
		case "BLOB", "BFILE", "RAW", "LONG RAW":
			fieldType = DBFieldTypeBinary
		case "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
			fieldType = DBFieldTypeFloat
		case "XMLTYPE":
			// Map XML to string for compatibility
			fieldType = DBFieldTypeString
		default:
			if strings.Contains(dataType, "TIMESTAMP(") {
				fieldType = DBFieldTypeTime
			} else {
				fieldType = DBFieldTypeString
			}
		}

		// Create column key (PRI for primary key, empty otherwise)
		columnKey := ""
		if isPrimaryKey {
			columnKey = "PRI"
		}
		if _, ok := uqMap[columnName]; ok {
			columnKey = "UQ"
		}
		// Add column to the map
		columns = append(columns, &DBColumn{
			Name:        columnName,
			DBFieldType: fieldType,
			ColumnKey:   columnKey,
			OrgDataType: buildDataType(col),
			Nullable:    col.Nullable != "N",
		})
	}

	return columns, nil
}

func (s *oracleSql) IsTableExist(j DatabaseClient, tableName string) (bool, error) {
	tableName = ConvertReservedKeywords(tableName)
	// Query to check if the table exists in the user's schema
	query := `
		SELECT COUNT(*) as count 
		FROM all_tables 
		WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
		AND table_name = UPPER(:tableName)
	`

	// Execute the query with parameters
	params := map[string]interface{}{
		"tableName": tableName,
	}

	// Query the database
	result, err := j.NQuery(query, params)
	if err != nil {
		return false, err
	}

	// If we got results and count > 0, table exists
	if len(result) > 0 {
		if count := cyutil.GetInt(result[0], "COUNT", true); count > 0 {
			return true, nil
		}
	}

	return false, nil
}

func (s *oracleSql) PreProcess(sql string, param ...int) string {
	if len(param) == 0 {
		re := regexp.MustCompile("`([\\d\\w-_ ]+)`")

		// 替换函数，对匹配到的内容进行回调处理
		sql = re.ReplaceAllStringFunc(sql, func(match string) string {
			// 去掉匹配到的字符串的包裹字符（即去掉``）
			content := strings.Trim(match, "`")
			// 调用回调函数处理内容
			processedContent := ConvertReservedKeywords(content)
			// 返回处理后的结果
			return processedContent
		})
	}
	sql = strings.TrimSpace(sql)
	if strings.HasSuffix(sql, "END;") {
		return sql
	}
	return strings.TrimSuffix(sql, ";") + "\n"
}

type valRet struct {
	val  string
	keep bool
}

func hexStr(d interface{}) (string, error) {
	switch v := d.(type) {
	case []byte:
		return strings.ToUpper(hex.EncodeToString(v)), nil
	case string:
		return strings.ToUpper(hex.EncodeToString([]byte(v))), nil
	default:
		return "", errors.New("invalid data type")
	}
}
func oracleFormatValue(owner string, fd *FieldData) (*valRet, error) {
	if fd.Data == nil {
		if !fd.Nullable {
			switch fd.Type {
			case DBFieldTypeString, DBFieldTypeJson:
				return &valRet{val: " "}, nil
			case DBFieldTypeInt:
				return &valRet{val: "0"}, nil
			case DBFieldTypeFloat:
				return &valRet{val: "0.0"}, nil
			case DBFieldTypeTime:
				switch fd.OrgDataType {
				case "DATE":
					return &valRet{val: "TO_DATE('1970-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"}, nil
				case "TIMESTAMP":
					return &valRet{val: "TO_TIMESTAMP('1970-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"}, nil
				default:
					if strings.Contains(fd.OrgDataType, "TIMESTAMP(") {
						return &valRet{val: "TO_TIMESTAMP('1970-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"}, nil
					}
					return &valRet{val: "SYSDATE"}, nil
				}
			default:
				return &valRet{val: "' '", keep: true}, nil
			}
		}
		return &valRet{val: "NULL"}, nil
	}
	dataStr := cyutil.ToStr(fd.Data)
	switch fd.Type {
	case DBFieldTypeString:
		fallthrough
	case DBFieldTypeJson:
		// No single quotes, use regular quoting
		return wrapperValue(owner, dataStr, false), nil

	case DBFieldTypeInt:
		switch v := fd.Data.(type) {
		case string:
			return &valRet{val: v}, nil
		default:
			val, err := FormatValue(fd.Data, fd.Type)
			if err != nil {
				return nil, err
			}
			return &valRet{val: val}, nil
		}
	case DBFieldTypeFloat:
		switch v := fd.Data.(type) {
		case string:
			return &valRet{val: v}, nil
		default:
			val, err := FormatValue(fd.Data, fd.Type)
			if err != nil {
				return nil, err
			}
			return &valRet{val: val}, nil
		}
	case DBFieldTypeTime:
		{
			val, err := FormatValue(fd.Data, fd.Type)
			if err != nil {
				return nil, err
			}
			switch fd.OrgDataType {
			case "DATE":
				return &valRet{val: "TO_DATE(" + val + ", 'SYYYY-MM-DD HH24:MI:SS')"}, nil
			case "TIMESTAMP":
				return &valRet{val: "TO_TIMESTAMP(" + val + ", 'SYYYY-MM-DD HH24:MI:SS')"}, nil
			default:
				if strings.Contains(fd.OrgDataType, "TIMESTAMP(") {
					return &valRet{val: "TO_TIMESTAMP(" + val + ", 'SYYYY-MM-DD HH24:MI:SS')"}, nil
				}

				return &valRet{val: val}, nil
			}
		}
	case DBFieldTypeBinary:
		ret, err := hexStr(fd.Data)
		if err != nil {
			return nil, err
		}
		if len(ret) > maxStrSize {
			return &valRet{val: ret, keep: true}, nil
		}
		return &valRet{val: "HEXTORAW('" + ret + "')"}, nil
	default:
		val, err := FormatValue(fd.Data, fd.Type)
		if err != nil {
			return nil, err
		}
		return &valRet{val: val}, nil
	}
}

type oracleRowData struct {
	*RowData
	varMode         bool
	varDefs         []string
	fields          []string
	clobCreateList  []string
	rawCreateList   []string
	blobReleaseList []string
}

func wrapperWithBase64(owner string, str string) string {
	hexStr := cyutil.Base64Encode(str)
	return owner + ".DECODE_BASE64('" + hexStr + "')"
}

func wrapperValue(owner string, str string, forceBase64 bool) *valRet {
	if len(str) > maxStrSize {
		return &valRet{val: str, keep: true}
	}
	if forceBase64 || strings.Contains(str, "\n") ||
		strings.Contains(str, "\r") ||
		strings.Contains(str, "#{") ||
		strings.Contains(str, "\\") ||
		strings.Contains(str, "\"") ||
		strings.Contains(str, "/") ||
		strings.Contains(str, "`") {
		return &valRet{val: wrapperWithBase64(owner, str)}
	}
	str = strings.ReplaceAll(str, "'", "''")
	return &valRet{val: "'" + str + "'"}
}

func removeSurroundingApostrophes(str string) string {
	if len(str) < 2 {
		return str
	}
	apostrophe := byte('\'') // Convert rune to byte for comparison
	if str[0] == apostrophe && str[len(str)-1] == apostrophe {
		return str[1 : len(str)-1]
	}
	return str
}

const maxStrSize = 2000

func splitStr(str string, size int) []string {
	if len(str) < size {
		return []string{str}
	}
	begin := 0
	end := len(str)
	var result []string
	for ; begin < end; begin += size {
		tend := begin + size
		if tend > end {
			tend = end
		}
		result = append(result, str[begin:tend])
	}
	return result
}
func ClobArrDef(owner string, fd *FieldData) (string, bool) {
	templ := `
	 arr_%s StringArray := StringArray(
        %s
    );
	`
	strs := splitStr(cyutil.ToStr(fd.Data), maxStrSize)
	if len(strs) == 0 {
		return "", false
	}

	for i, str := range strs {
		r := wrapperValue(owner, str, true)
		strs[i] = r.val
	}
	return fmt.Sprintf(templ, fd.Name, strings.Join(strs, ",\n")), true
}

func BlobArrDef(owner string, fd *FieldData) (string, bool) {
	templ := `
	 arr_%s RawArray := RawArray(
        %s
    );
	`
	strs := splitStr(cyutil.ToStr(fd.Data), maxStrSize)
	if len(strs) == 0 {
		return "", false
	}

	for i, str := range strs {
		r, err := hexStr(str)
		if err != nil {
			return "", false
		}
		strs[i] = "HEXTORAW('" + r + "')"
	}
	return fmt.Sprintf(templ, fd.Name, strings.Join(strs, ",\n")), true
}

func ClobCreateStr(fd *FieldData) (string, string) {
	templ := `
    DBMS_LOB.CREATETEMPORARY(%s, TRUE);
    FOR i IN 1..%s.COUNT LOOP
        strBuffer := %s(i);
        amount := LENGTH(strBuffer);
        DBMS_LOB.WRITEAPPEND(%s, amount, strBuffer);
    END LOOP;
 `
	return fmt.Sprintf(templ, "v_"+fd.Name, "arr_"+fd.Name, "arr_"+fd.Name, "v_"+fd.Name),
		fmt.Sprintf("DBMS_LOB.FREETEMPORARY(%s);", "v_"+fd.Name)
}

func BlobCreateStr(fd *FieldData) (string, string) {
	templ := `
    DBMS_LOB.CREATETEMPORARY(%s, TRUE);
    FOR i IN 1..%s.COUNT LOOP
        rawBuffer := %s(i);
        amount := UTL_RAW.LENGTH(rawBuffer);
        DBMS_LOB.WRITEAPPEND(%s, amount, rawBuffer);
    END LOOP;
 `
	return fmt.Sprintf(templ, "v_"+fd.Name, "arr_"+fd.Name, "arr_"+fd.Name, "v_"+fd.Name),
		fmt.Sprintf("DBMS_LOB.FREETEMPORARY(%s);", "v_"+fd.Name)
}

func InitFromRowData(owner string, r *RowData) *oracleRowData {
	ord := &oracleRowData{
		RowData: r,
	}
	for _, fd := range r.Data {
		ord.fields = append(ord.fields, "\""+strings.ToUpper(fd.Name)+"\"")
		fmtVal, err := oracleFormatValue(owner, fd)
		if err != nil {
			return nil
		}
		dyClob := false
		dyRaw := false
		if fmtVal.keep {
			if fd.Type == DBFieldTypeBinary {
				dyRaw = true
			} else {
				dyClob = true
			}
			ord.varMode = true
		} else if len(fmtVal.val) > 300 {
			ord.varMode = true
		}

		if dyClob {
			arrDef, ok := ClobArrDef(owner, fd)
			if ok {
				ord.varDefs = append(ord.varDefs, arrDef)
				createStr, releaseStr := ClobCreateStr(fd)
				if createStr != "" {
					ord.clobCreateList = append(ord.clobCreateList, createStr)
				}
				if releaseStr != "" {
					ord.blobReleaseList = append(ord.blobReleaseList, releaseStr)
				}
			}
			ord.varDefs = append(ord.varDefs, fmt.Sprintf("%s %s;", "v_"+fd.Name, fd.OrgDataType))
		} else if dyRaw {
			arrDef, ok := BlobArrDef(owner, fd)
			if ok {
				ord.varDefs = append(ord.varDefs, arrDef)
				createStr, releaseStr := BlobCreateStr(fd)
				if createStr != "" {
					ord.rawCreateList = append(ord.rawCreateList, createStr)
				}
				if releaseStr != "" {
					ord.blobReleaseList = append(ord.blobReleaseList, releaseStr)
				}
			}
			ord.varDefs = append(ord.varDefs, fmt.Sprintf("%s %s;", "v_"+fd.Name, fd.OrgDataType))
		} else {
			ord.varDefs = append(ord.varDefs, fmt.Sprintf("%s %s := %s;", "v_"+fd.Name, fd.OrgDataType, fmtVal.val))
		}

	}
	if len(ord.blobReleaseList) > 0 {
		ord.varDefs = append([]string{"amount BINARY_INTEGER;"}, ord.varDefs...)
	}
	if len(ord.clobCreateList) > 0 {
		ord.varDefs = append([]string{"TYPE StringArray IS TABLE OF VARCHAR2(32767);", "strBuffer VARCHAR2(32767);"}, ord.varDefs...)
	}
	if len(ord.rawCreateList) > 0 {
		ord.varDefs = append([]string{"TYPE RawArray IS TABLE OF RAW(32767);", "rawBuffer RAW(32767);"}, ord.varDefs...)
	}
	return ord
}

func (r *oracleRowData) fieldsStr(pre string) (string, error) {
	vFields := []string{}
	for _, fd := range r.Data {
		vFields = append(vFields, pre+"\""+strings.ToUpper(fd.Name)+"\"")
	}
	return strings.Join(vFields, ","), nil
}

func (r *oracleRowData) insertValuesStr(owner string, varMode bool) (string, error) {
	vValues := []string{}
	for _, fd := range r.Data {

		if varMode {
			vValues = append(vValues, "v_"+fd.Name)
		} else {
			fmtVal, err := oracleFormatValue(owner, fd)
			if err != nil {
				return "", err
			}
			vValues = append(vValues, fmtVal.val)
		}
	}
	return strings.Join(vValues, ","), nil
}

func (r *oracleRowData) selectValuesStr(owner string) (string, error) {
	vValues := []string{}
	for _, fd := range r.Data {
		fmtVal, err := oracleFormatValue(owner, fd)
		if err != nil {
			return "", err
		}
		vValues = append(vValues, fmt.Sprintf("%s AS %s", fmtVal.val, "\""+strings.ToUpper(fd.Name)+"\""))
	}
	return strings.Join(vValues, ","), nil
}

func (r *oracleRowData) selectVarsStr() (string, error) {
	vValues := []string{}
	for _, fd := range r.Data {
		vValues = append(vValues, fmt.Sprintf("%s AS %s", "v_"+fd.Name, "\""+strings.ToUpper(fd.Name)+"\""))
	}
	return strings.Join(vValues, ","), nil
}

func (r *oracleRowData) getVarsDefStr() (string, error) {
	return strings.Join(r.varDefs, "\n"), nil
}

func (r *oracleRowData) setValuesByFieldsStr(t2 string) (string, error) {
	vFields := []string{}
	for _, fd := range r.Data {
		if fd.IsPK || fd.IsUQ {
			continue
		}
		fdName := "\"" + strings.ToUpper(fd.Name) + "\""
		vFields = append(vFields, fmt.Sprintf("%s=%s%s", fdName, t2, fdName))
	}
	return strings.Join(vFields, ","), nil
}

func (r *oracleRowData) conditionStr(t1, t2 string) (string, error) {
	vFields := []string{}
	for _, fd := range r.Data {
		if fd.IsPK || fd.IsUQ {
			fdName := "\"" + strings.ToUpper(fd.Name) + "\""
			vFields = append(vFields, fmt.Sprintf("%s%s=%s%s", t1, fdName, t2, fdName))
		}
	}
	return strings.Join(vFields, " AND "), nil
}

func (r *oracleRowData) blobCreateStr() (string, error) {
	v := []string{strings.Join(r.rawCreateList, "\n"), strings.Join(r.clobCreateList, "\n")}
	return strings.Join(v, "\n"), nil
}

func (r *oracleRowData) blobReleaseStr() (string, error) {
	return strings.Join(r.blobReleaseList, "\n"), nil
}

func (s *oracleSql) GetReplaceSql(cli DatabaseClient, table string, rd *RowData) (ret string, err error) {
	schema := cli.Database()
	table = ConvertReservedKeywords(table)
	schema = strings.ToUpper(schema)
	table = strings.ToUpper(table)
	fullTable := schema + "." + table
	if len(rd.Data) == 0 {
		return "", errors.New("no data provided for replace operation")
	}
	ord := InitFromRowData(schema, rd)
	if ord == nil {
		return "", errors.New("init from row data failed")
	}
	OnConditionStr, err := ord.conditionStr("e.", "s.")
	if err != nil {
		return "", err
	}

	blobCreateStr, err := ord.blobCreateStr()
	if err != nil {
		return "", err
	}
	blobReleaseStr, err := ord.blobReleaseStr()
	if err != nil {
		return "", err
	}
	var setValuesByFieldsStr string
	var insertFieldsStr string
	var values string
	var fieldsStr string
	var varsDefStr string
	sb := strings.Builder{}
	if OnConditionStr == "" {
		insertFieldsStr, err = ord.fieldsStr("")
		if err != nil {
			return "", err
		}
		if ord.varMode {
			values, err = ord.insertValuesStr(schema, true)
			if err != nil {
				return "", err
			}
			varsDefStr, err = ord.getVarsDefStr()
			if err != nil {
				return "", err
			}
			sb.WriteString("DECLARE\n")
			sb.WriteString(varsDefStr)
			sb.WriteString("\nBEGIN\n")
			sb.WriteString(blobCreateStr)
			sb.WriteString("INSERT INTO " + fullTable + " (" + insertFieldsStr + ") VALUES (" + values + ");\n")
			sb.WriteString("COMMIT;\n")
			sb.WriteString(blobReleaseStr)
			sb.WriteString("\nEND;\n/\n")
			ret = sb.String()
			return
		} else {
			values, err = ord.insertValuesStr(schema, false)
			if err != nil {
				return "", err
			}
			sb.WriteString("INSERT INTO " + fullTable + " (" + insertFieldsStr + ") VALUES (" + values + ");")
			ret = sb.String()
			return
		}
	} else {
		fieldsStr, err = ord.fieldsStr("s.")
		if err != nil {
			return "", err
		}
		setValuesByFieldsStr, err = ord.setValuesByFieldsStr("s.")
		if err != nil {
			return "", err
		}
		insertFieldsStr, err = ord.fieldsStr("")
		if err != nil {
			return "", err
		}
		if ord.varMode {
			var selectVarsStr string
			selectVarsStr, err = ord.selectVarsStr()
			if err != nil {
				return "", err
			}
			varsDefStr, err = ord.getVarsDefStr()
			if err != nil {
				return "", err
			}
			sb.WriteString("DECLARE\n")
			sb.WriteString(varsDefStr)
			sb.WriteString("\nBEGIN\n")
			sb.WriteString(blobCreateStr)
			sb.WriteString("MERGE INTO " + fullTable + " e USING (SELECT " + selectVarsStr + " FROM dual) s ON (" + OnConditionStr + ")")
			if setValuesByFieldsStr != "" {
				sb.WriteString(" WHEN MATCHED THEN UPDATE SET ")
				sb.WriteString(setValuesByFieldsStr)
			}
			sb.WriteString(" WHEN NOT MATCHED THEN INSERT (" + insertFieldsStr + ") VALUES (" + fieldsStr + ");\n")
			sb.WriteString("COMMIT;\n")
			sb.WriteString(blobReleaseStr)
			sb.WriteString("\nEND;\n/\n")
			ret = sb.String()
			return
		} else {
			var selectValuesStr string
			selectValuesStr, err = ord.selectValuesStr(schema)
			if err != nil {
				return "", err
			}

			sb.WriteString("MERGE INTO " + fullTable + " e USING (SELECT " + selectValuesStr + " FROM dual) s ON (" + OnConditionStr + ")")
			if setValuesByFieldsStr != "" {
				sb.WriteString(" WHEN MATCHED THEN UPDATE SET ")
				sb.WriteString(setValuesByFieldsStr)
			}
			sb.WriteString(" WHEN NOT MATCHED THEN INSERT (" + insertFieldsStr + ") VALUES (" + fieldsStr + ");")
			ret = sb.String()
			return
		}
	}
}

func (s *oracleSql) MakeSureDBExists(cli DatabaseClient, dbName string) error {
	dbName = strings.ToUpper(dbName)
	var userCount int
	err := cli.Get(&userCount, "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1", dbName)
	if err != nil {
		// Just log the error but don't fail the connection
		cylog.Errorf("Error checking for schema user %s: %v", dbName, err)
		return err
	} else if userCount == 0 {
		// User doesn't exist, create it
		_, err = cli.Excute(fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", dbName, cli.PW()))
		if err != nil {
			return err
		} else {
			// Grant necessary privileges to the new user
			_, err = cli.Excute(fmt.Sprintf("GRANT CONNECT, RESOURCE TO %s", dbName))
			if err != nil {
				return err
			}
			_, err = cli.Excute(fmt.Sprintf("ALTER USER %s QUOTA UNLIMITED ON USERS", dbName))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *oracleSql) GetConnectStr(dbConn *DBConnection) (string, string) {
	return "oracle", go_ora.BuildUrl(dbConn.Host, dbConn.Port, dbConn.Service, dbConn.Un, dbConn.Pw, nil)
}

func (s *oracleSql) GetDefaultTypeName(tp DefaultDBFieldType) string {
	switch tp {
	case DefaultDBFieldTypeString:
		return "VARCHAR2(255)"
	case DefaultDBFieldTypeInt:
		return "NUMBER"
	case DefaultDBFieldTypeFloat:
		return "FLOAT"
	case DefaultDBFieldTypeBool:
		return "NUMBER(1)"
	case DefaultDBFieldTypeTime:
		return "TIMESTAMP"
	default:
		return ""
	}
}
