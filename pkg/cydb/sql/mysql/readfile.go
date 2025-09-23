package sqlmysql

import (
	"fmt"
	"io"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

// MySQLType implements the SQLType interface for MySQL
type MySQLType int

const (
	MySQLTypeUnknown MySQLType = iota
	MySQLTypeSelect
	MySQLTypeInsert
	MySQLTypeUpdate
	MySQLTypeDelete
	MySQLTypeCreateTable
	MySQLTypeCreateIndex
	MySQLTypeCreateView
	MySQLTypeCreateFunction
	MySQLTypeCreateProcedure
	MySQLTypeCreateTrigger
	MySQLTypeDrop
	MySQLTypeAlter
	MySQLTypeTransaction
	MySQLTypeOther
)

// Verify that MySQLType implements SQLType interface
var _ SQLType = MySQLType(0)

func (t MySQLType) IsNull() bool {
	return t == MySQLTypeUnknown
}

// AsInt returns the integer representation of the SQL type
func (t MySQLType) AsInt() int {
	return int(t)
}

// String returns the string representation of the SQL type
func (t MySQLType) String() string {
	switch t {
	case MySQLTypeSelect:
		return "Select"
	case MySQLTypeInsert:
		return "Insert"
	case MySQLTypeUpdate:
		return "Update"
	case MySQLTypeDelete:
		return "Delete"
	case MySQLTypeCreateTable:
		return "CreateTable"
	case MySQLTypeCreateIndex:
		return "CreateIndex"
	case MySQLTypeCreateView:
		return "CreateView"
	case MySQLTypeCreateFunction:
		return "CreateFunction"
	case MySQLTypeCreateProcedure:
		return "CreateProcedure"
	case MySQLTypeCreateTrigger:
		return "CreateTrigger"
	case MySQLTypeDrop:
		return "Drop"
	case MySQLTypeAlter:
		return "Alter"
	case MySQLTypeTransaction:
		return "Transaction"
	case MySQLTypeOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// IsBlock determines if the SQL type represents a block statement
func (t MySQLType) IsBlock() bool {
	return t == MySQLTypeCreateFunction ||
		t == MySQLTypeCreateProcedure ||
		t == MySQLTypeCreateTrigger ||
		t == MySQLTypeTransaction
}

// MySQLExtraData implements the ExtraData interface for MySQL
type MySQLExtraData struct {
	Delimiter      string
	InDelimiterDef bool
	EndMarks       map[string]struct{}
}

func (e *MySQLExtraData) IsBlockEnd(line string) bool {
	if e == nil {
		return strings.HasSuffix(line, ";")
	}
	if strings.HasSuffix(line, e.Delimiter) {
		return true
	}
	return false
}

// 关键字映射
var createTargets = map[string]SQLType{
	"TABLE":     MySQLTypeCreateTable,
	"INDEX":     MySQLTypeCreateIndex,
	"VIEW":      MySQLTypeCreateView,
	"FUNCTION":  MySQLTypeCreateFunction,
	"PROCEDURE": MySQLTypeCreateProcedure,
	"TRIGGER":   MySQLTypeCreateTrigger,
}

var keywordToInfo = map[string]SQLType{
	"SELECT": MySQLTypeSelect,
	"INSERT": MySQLTypeInsert,
	"UPDATE": MySQLTypeUpdate,
	"DELETE": MySQLTypeDelete,
	"DROP":   MySQLTypeDrop,
	"ALTER":  MySQLTypeAlter,
	"BEGIN":  MySQLTypeTransaction,
	"START":  MySQLTypeTransaction,
}

func (s *mysqlSql) preProcessSqlLine(ctx *ParseContext, pline *string) (PreRetType, error) {
	if ctx.CurrentLineNum == 59 {
		fmt.Println("ctx.CurrentLineNum", ctx.CurrentLineNum)
	}
	if strings.HasPrefix(*pline, "DELIMITER") {
		f := strings.Fields(*pline)
		if len(f) > 1 {
			if f[1] == ";" {
				ctx.Extra = nil
			} else {
				ctx.Extra = &MySQLExtraData{
					Delimiter: f[1],
				}
			}
			return PreRetTypeReturn, nil
		}
	}
	delim := ""
	if ctx.Extra != nil {
		delim = ctx.Extra.(*MySQLExtraData).Delimiter
	}
	if delim != "" && strings.HasSuffix(*pline, delim) {
		content := ctx.CurrentStmt.String()
		ctx.CurrentStmt.Reset()
		ctx.CurrentStmt.WriteString("DELIMITER " + delim + "\n")
		ctx.CurrentStmt.WriteString(content)
		ctx.CurrentStmt.WriteString("\nEND" + delim + "\n")
		ctx.CurrentStmt.WriteString("DELIMITER ;")
		return PreRetTypeCallCallback, nil
	}
	return PreRetTypeContinue, nil
}

// processMySQLWord processes MySQL keywords during SQL parsing
func processMySQLWord(ctx *ParseContext, word *cyutil.WordInfo) (bool, error) {
	// Handle trace words (like CREATE followed by TABLE, INDEX, etc.)
	if ctx.TraceWord != nil {
		ctx.TraceWord.Skip++

		// Try to match with the next keys
		if sqlType, ok := ctx.TraceWord.NextKeys[word.Word]; ok {
			switch ctx.TraceWord.Key {
			case "CREATE":
				ctx.CurrentType = sqlType
				ctx.TraceWord = nil
			default:
				return false, NewSQLParseError("UnknownTraceWord",
					fmt.Sprintf("unknown trace word: %s", ctx.TraceWord.Key),
					ctx.CurrentLineNum, ctx.LineContent)
			}
		}
		return false, nil
	}

	// Handle BEGIN keyword for transactions
	if word.Word == "BEGIN" || word.Word == "START" {
		if ctx.CurrentType == nil {
			ctx.CurrentType = MySQLTypeTransaction
		}
		ctx.TypeWord = word.Word
		return false, nil
	}

	// Handle END, COMMIT, and ROLLBACK keywords for transactions and blocks
	if extra, ok := ctx.Extra.(*MySQLExtraData); ok && extra != nil && len(extra.EndMarks) > 0 {
		if _, ok := extra.EndMarks[word.Word]; ok {
			return true, nil // Return true to execute callback
		}
	}

	// Process keywords for statements without a type yet
	if ctx.IsCurrentTypeNull() {
		if word.Word == "CREATE" {
			nextKeys := createTargets
			ctx.TraceWord = &TraceWord{
				Key:      word.Word,
				Skip:     0,
				NextKeys: nextKeys,
			}
		} else {
			// Direct lookup for known keywords
			if sqlType, ok := keywordToInfo[word.Word]; ok {
				ctx.CurrentType = sqlType
				ctx.TypeWord = word.Word
			} else {
				// For unknown keywords, default to Other type
				ctx.CurrentType = MySQLTypeOther
				ctx.TypeWord = word.Word
			}
		}
	}
	return false, nil
}

// ReadSQLFile reads SQL statements from a reader and calls the callback for each statement
func (s *mysqlSql) ReadSQLFile(r io.Reader, callback FuncSQLStatementCallback, options ...func(*ReadSQLFileOptions)) error {
	// Use the ReadSQLFile function from base_read.go with MySQL-specific word callback
	return ReadSQLFile(r, callback, append(options, WithWordCallback(processMySQLWord), WithPreProcessSqlLine(s.preProcessSqlLine))...)
}
