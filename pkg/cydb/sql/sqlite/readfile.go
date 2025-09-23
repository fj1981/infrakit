package sqlsqlite

import (
	"fmt"
	"io"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

// Verify that SQLiteType implements SQLType interface
var _ SQLType = SQLiteType(0)

// SQLiteType implements the SQLType interface for SQLite
type SQLiteType int

const (
	SQLiteTypeUnknown SQLiteType = iota
	SQLiteTypeSelect
	SQLiteTypeInsert
	SQLiteTypeUpdate
	SQLiteTypeDelete
	SQLiteTypeCreateTable
	SQLiteTypeCreateIndex
	SQLiteTypeCreateView
	SQLiteTypeCreateTrigger
	SQLiteTypeDrop
	SQLiteTypeAlter
	SQLiteTypeProcedure
	SQLiteTypeTransaction
	SQLiteTypeOther
)

func (t SQLiteType) IsNull() bool {
	return t == SQLiteTypeUnknown
}

// AsInt returns the integer representation of the SQL type
func (t SQLiteType) AsInt() int {
	return int(t)
}

// String returns the string representation of the SQL type
func (t SQLiteType) String() string {
	switch t {
	case SQLiteTypeSelect:
		return "Select"
	case SQLiteTypeInsert:
		return "Insert"
	case SQLiteTypeUpdate:
		return "Update"
	case SQLiteTypeDelete:
		return "Delete"
	case SQLiteTypeCreateTable:
		return "CreateTable"
	case SQLiteTypeCreateIndex:
		return "CreateIndex"
	case SQLiteTypeCreateView:
		return "CreateView"
	case SQLiteTypeCreateTrigger:
		return "CreateTrigger"
	case SQLiteTypeDrop:
		return "Drop"
	case SQLiteTypeAlter:
		return "Alter"
	case SQLiteTypeProcedure:
		return "Procedure"
	case SQLiteTypeTransaction:
		return "Transaction"
	case SQLiteTypeOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// IsBlock determines if the SQL type represents a block statement
func (t SQLiteType) IsBlock() bool {
	return t == SQLiteTypeTransaction || t == SQLiteTypeProcedure
}

// SQLiteExtraData implements the ExtraData interface for SQLite
type SQLiteExtraData struct {
	EndMarks map[string]struct{}
}

func (e *SQLiteExtraData) IsBlockEnd(line string) bool {
	if e == nil || len(e.EndMarks) == 0 {
		return strings.HasSuffix(line, ";")
	}
	return false
}

// 关键字映射

// 关键字映射
var createTargets = map[string]SQLType{
	"TABLE":     SQLiteTypeCreateTable,
	"INDEX":     SQLiteTypeCreateIndex,
	"VIEW":      SQLiteTypeCreateView,
	"TRIGGER":   SQLiteTypeCreateTrigger,
	"PROCEDURE": SQLiteTypeProcedure,
}

var keywordToInfo = map[string]SQLType{
	"WITH":   SQLiteTypeOther,
	"SELECT": SQLiteTypeSelect,
	"INSERT": SQLiteTypeInsert,
	"UPDATE": SQLiteTypeUpdate,
	"DELETE": SQLiteTypeDelete,
	"DROP":   SQLiteTypeDrop,
	"ALTER":  SQLiteTypeAlter,
	"BEGIN":  SQLiteTypeTransaction,
}

// processSQLiteWord processes SQLite keywords during SQL parsing
func processSQLiteWord(ctx *ParseContext, word *cyutil.WordInfo) (bool, error) {
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
	if word.Word == "BEGIN" {
		extra := &SQLiteExtraData{
			EndMarks: map[string]struct{}{
				"COMMIT": {},
				"END":    {},
			},
		}
		ctx.Extra = extra
		if ctx.CurrentType == nil {
			ctx.CurrentType = SQLiteTypeTransaction
		}
		ctx.TypeWord = word.Word
		return false, nil
	}

	// Handle END and COMMIT keywords for transactions
	if extra, ok := ctx.Extra.(*SQLiteExtraData); ok && extra != nil && len(extra.EndMarks) > 0 {
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
				ctx.CurrentType = SQLiteTypeOther
				ctx.TypeWord = word.Word
			}
		}
	}
	return false, nil
}

// ReadSQLFile reads SQL statements from a reader and calls the callback for each statement
func (s *sqliteSql) ReadSQLFile(r io.Reader, callback FuncSQLStatementCallback, options ...func(*ReadSQLFileOptions)) error {
	// Use the ReadSQLFile function from base_read.go with SQLite-specific word callback
	return ReadSQLFile(r, callback, append(options, WithWordCallback(processSQLiteWord))...)
}
