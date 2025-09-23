package sqlpostgresql

import (
	"fmt"
	"io"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

// Verify that PostgreSQLType implements SQLType interface
var _ SQLType = PostgreSQLType(0)

// PostgreSQLType implements the SQLType interface for PostgreSQL
type PostgreSQLType int

const (
	PostgreSQLTypeUnknown PostgreSQLType = iota
	PostgreSQLTypeSelect
	PostgreSQLTypeInsert
	PostgreSQLTypeUpdate
	PostgreSQLTypeDelete
	PostgreSQLTypeCreateTable
	PostgreSQLTypeCreateIndex
	PostgreSQLTypeCreateView
	PostgreSQLTypeCreateFunction
	PostgreSQLTypeCreateProcedure
	PostgreSQLTypeCreateTrigger
	PostgreSQLTypeDrop
	PostgreSQLTypeAlter
	PostgreSQLTypeTransaction
	PostgreSQLTypeOther
)

func (t PostgreSQLType) IsNull() bool {
	return t == PostgreSQLTypeUnknown
}

// AsInt returns the integer representation of the SQL type
func (t PostgreSQLType) AsInt() int {
	return int(t)
}

// String returns the string representation of the SQL type
func (t PostgreSQLType) String() string {
	switch t {
	case PostgreSQLTypeSelect:
		return "Select"
	case PostgreSQLTypeInsert:
		return "Insert"
	case PostgreSQLTypeUpdate:
		return "Update"
	case PostgreSQLTypeDelete:
		return "Delete"
	case PostgreSQLTypeCreateTable:
		return "CreateTable"
	case PostgreSQLTypeCreateIndex:
		return "CreateIndex"
	case PostgreSQLTypeCreateView:
		return "CreateView"
	case PostgreSQLTypeCreateFunction:
		return "CreateFunction"
	case PostgreSQLTypeCreateProcedure:
		return "CreateProcedure"
	case PostgreSQLTypeCreateTrigger:
		return "CreateTrigger"
	case PostgreSQLTypeDrop:
		return "Drop"
	case PostgreSQLTypeAlter:
		return "Alter"
	case PostgreSQLTypeTransaction:
		return "Transaction"
	case PostgreSQLTypeOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// IsBlock determines if the SQL type represents a block statement
func (t PostgreSQLType) IsBlock() bool {
	return t == PostgreSQLTypeCreateFunction ||
		t == PostgreSQLTypeCreateProcedure ||
		t == PostgreSQLTypeCreateTrigger ||
		t == PostgreSQLTypeTransaction
}

// PostgreSQLExtraData implements the ExtraData interface for PostgreSQL
type PostgreSQLExtraData struct {
	EndMarks      map[string]struct{}
	DollarQuote   string
	InDollarQuote bool
}

func (e *PostgreSQLExtraData) IsBlockEnd(line string) bool {
	if e == nil || len(e.EndMarks) == 0 {
		// Check for dollar-quoted string end if active
		if e != nil && e.InDollarQuote && e.DollarQuote != "" {
			if strings.Contains(line, e.DollarQuote) {
				e.InDollarQuote = false
				e.DollarQuote = ""
			}
			return false
		}
		return strings.HasSuffix(line, ";")
	}
	return false
}

// 关键字映射
var createTargets = map[string]SQLType{
	"TABLE":     PostgreSQLTypeCreateTable,
	"INDEX":     PostgreSQLTypeCreateIndex,
	"VIEW":      PostgreSQLTypeCreateView,
	"FUNCTION":  PostgreSQLTypeCreateFunction,
	"PROCEDURE": PostgreSQLTypeCreateProcedure,
	"TRIGGER":   PostgreSQLTypeCreateTrigger,
}

var keywordToInfo = map[string]struct {
	sqlType      SQLType
	blockKeyword bool
}{
	"WITH":    {PostgreSQLTypeOther, false},
	"SELECT":  {PostgreSQLTypeSelect, false},
	"INSERT":  {PostgreSQLTypeInsert, false},
	"UPDATE":  {PostgreSQLTypeUpdate, false},
	"DELETE":  {PostgreSQLTypeDelete, false},
	"DROP":    {PostgreSQLTypeDrop, false},
	"ALTER":   {PostgreSQLTypeAlter, false},
	"BEGIN":   {PostgreSQLTypeTransaction, true},
	"DO":      {PostgreSQLTypeOther, true},
	"DECLARE": {PostgreSQLTypeOther, true},
}

// processPostgresqlWord processes PostgreSQL keywords during SQL parsing
func processPostgresqlWord(ctx *ParseContext, word *cyutil.WordInfo) (bool, error) {
	// Check for dollar-quoted strings (PostgreSQL specific feature)
	if extra, ok := ctx.Extra.(*PostgreSQLExtraData); ok && extra != nil {
		if extra.InDollarQuote {
			// Already in a dollar-quoted string, check if this is the end
			if strings.Contains(word.Word, extra.DollarQuote) {
				extra.InDollarQuote = false
				extra.DollarQuote = ""
			}
			return false, nil
		} else if strings.HasPrefix(word.Word, "$") && strings.HasSuffix(word.Word, "$") {
			// This is the start of a dollar-quoted string
			extra.InDollarQuote = true
			extra.DollarQuote = word.Word
			return false, nil
		}
	} else {
		// Initialize extra data if not exists
		ctx.Extra = &PostgreSQLExtraData{
			EndMarks: map[string]struct{}{
				"COMMIT": {},
				"END":    {},
			},
			InDollarQuote: false,
		}
	}

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
		if ctx.CurrentType == nil {
			ctx.CurrentType = PostgreSQLTypeTransaction
		}
		ctx.TypeWord = word.Word
		return false, nil
	}

	// Handle END and COMMIT keywords for transactions
	if extra, ok := ctx.Extra.(*PostgreSQLExtraData); ok && extra != nil && len(extra.EndMarks) > 0 {
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
			if info, ok := keywordToInfo[word.Word]; ok {
				ctx.CurrentType = info.sqlType
				ctx.TypeWord = word.Word
			} else {
				// For unknown keywords, default to Other type
				ctx.CurrentType = PostgreSQLTypeOther
				ctx.TypeWord = word.Word
			}
		}
	}
	return false, nil
}

// ReadSQLFile reads SQL statements from a reader and calls the callback for each statement
func (s *postgresqlSql) ReadSQLFile(r io.Reader, callback FuncSQLStatementCallback, options ...func(*ReadSQLFileOptions)) error {
	// Use the ReadSQLFile function with PostgreSQL-specific word callback
	return ReadSQLFile(r, callback, append(options, WithWordCallback(processPostgresqlWord))...)
}
