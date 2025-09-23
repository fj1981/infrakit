package sqloracle

import (
	"fmt"
	"io"
	"strings"

	stack "github.com/duke-git/lancet/v2/datastructure/stack"
	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

type OracleType int

const (
	OracleTypeUnknown OracleType = iota
	OracleTypeSelect
	OracleTypeInsert
	OracleTypeUpdate
	OracleTypeDelete
	OracleTypeCreateTable
	OracleTypeCreateIndex
	OracleTypeCreateView
	OracleTypeCreateFunction
	OracleTypeCreateProcedure
	OracleTypeCreateTrigger
	OracleTypeCreatePackage
	OracleTypeDrop
	OracleTypeAlter
	OracleTypeMerge
	OracleTypeOther
)

// AsInt 实现 SQLType 接口，返回 SQL 类型的整数表示
func (t OracleType) AsInt() int {
	return int(t)
}

func (t OracleType) IsNull() bool {
	return t == OracleTypeUnknown
}

// String 实现 SQLType 接口，返回 SQL 类型的字符串表示
func (t OracleType) String() string {
	switch t {
	case OracleTypeSelect:
		return "Select"
	case OracleTypeInsert:
		return "Insert"
	case OracleTypeUpdate:
		return "Update"
	case OracleTypeDelete:
		return "Delete"
	case OracleTypeCreateTable:
		return "CreateTable"
	case OracleTypeCreateIndex:
		return "CreateIndex"
	case OracleTypeCreateView:
		return "CreateView"
	case OracleTypeCreateFunction:
		return "CreateFunction"
	case OracleTypeCreateProcedure:
		return "CreateProcedure"
	case OracleTypeCreateTrigger:
		return "CreateTrigger"
	case OracleTypeCreatePackage:
		return "CreatePackage"
	case OracleTypeDrop:
		return "Drop"
	case OracleTypeAlter:
		return "Alter"
	case OracleTypeMerge:
		return "Merge"
	case OracleTypeOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// IsBlock 实现 SQLType 接口，判断该 SQL 类型是否为块级语句
func (t OracleType) IsBlock() bool {
	return t == OracleTypeCreateFunction ||
		t == OracleTypeCreateProcedure ||
		t == OracleTypeCreateTrigger ||
		t == OracleTypeCreatePackage ||
		t == OracleTypeOther
}

// 确保 OracleType 实现 SQLType 接口
var _ SQLType = OracleType(0)

type OracleExtraData struct {
	stack *stack.ArrayStack[string]
}

// IsEmpty checks if the extra data is empty
func (e *OracleExtraData) IsBlockEnd(line string) bool {
	if e == nil || e.stack == nil || e.stack.Size() == 0 {
		return strings.HasSuffix(line, ";")
	}
	return false
}

func (s *oracleSql) ReadSQLFile(r io.Reader, callback FuncSQLStatementCallback, options ...func(*ReadSQLFileOptions)) error {
	return ReadSQLFile(r, callback, append(options, WithWordCallback(processOracleWord))...)
}

func updateType(ctx *ParseContext, type_ SQLType, word string) {
	if ctx.CurrentType != nil {
		if ctx.CurrentType != OracleTypeUnknown && ctx.CurrentType != OracleTypeOther {
			return
		}
	}
	ctx.CurrentType = type_
	ctx.TypeWord = word
}

var createTargets = map[string]SQLType{
	"TABLE":     OracleTypeCreateTable,
	"INDEX":     OracleTypeCreateIndex,
	"VIEW":      OracleTypeCreateView,
	"FUNCTION":  OracleTypeCreateFunction,
	"PROCEDURE": OracleTypeCreateProcedure,
	"TRIGGER":   OracleTypeCreateTrigger,
	"PACKAGE":   OracleTypeCreatePackage,
}

// keywordToInfo: 其他关键字 → (SQLType, BlockKeywordType)
var keywordToInfo = map[string]struct {
	sqlType      SQLType
	blockKeyword BlockKeywordType
}{
	"SELECT":  {OracleTypeSelect, NotBlockKeyword},
	"INSERT":  {OracleTypeInsert, NotBlockKeyword},
	"UPDATE":  {OracleTypeUpdate, NotBlockKeyword},
	"DELETE":  {OracleTypeDelete, NotBlockKeyword},
	"MERGE":   {OracleTypeMerge, NotBlockKeyword},
	"DROP":    {OracleTypeDrop, NotBlockKeyword},
	"BEGIN":   {OracleTypeOther, StartBlock},
	"DECLARE": {OracleTypeOther, StartBlock},
	"IF":      {OracleTypeOther, StartBlock},
	"LOOP":    {OracleTypeOther, StartBlock},
	"CASE":    {OracleTypeOther, StartBlock},
	"WHILE":   {OracleTypeOther, StartBlock},
	"END":     {OracleTypeOther, EndBlock},
	"WITH":    {OracleTypeOther, NotBlockKeyword},
}

var endTargets = map[string]SQLType{
	"IF":        OracleTypeOther,
	"LOOP":      OracleTypeOther,
	"CASE":      OracleTypeOther,
	"FUNCTION":  OracleTypeOther,
	"PROCEDURE": OracleTypeOther,
	"PACKAGE":   OracleTypeOther,
	"TRIGGER":   OracleTypeOther,
	"WITH":      OracleTypeOther,
}

func isBlockType(word string) bool {
	if v, ok := keywordToInfo[word]; ok {
		return v.blockKeyword == StartBlock
	}
	return false
}

func Stack(ctx *ParseContext) *stack.ArrayStack[string] {
	if ctx.Extra == nil {
		ctx.Extra = &OracleExtraData{
			stack: stack.NewArrayStack[string](),
		}
	}
	return ctx.Extra.(*OracleExtraData).stack
}

func processOracleWord(ctx *ParseContext, word *cyutil.WordInfo) (bool, error) {
	if ctx.TraceWord != nil {
		ctx.TraceWord.Skip++
		if tp, ok := ctx.TraceWord.NextKeys[word.Word]; ok {
			switch ctx.TraceWord.Key {
			case "CREATE":
				updateType(ctx, tp, word.Word)
				if tp.IsBlock() {
					Stack(ctx).Push(word.Word)
				}
			case "END":
				ctx.TraceWord = nil
				v, err := Stack(ctx).Peak()
				if err != nil {
					return false, fmt.Errorf("[%d]block stack peak error: %v", ctx.CurrentLineNum, err)
				}
				if *v != word.Word {
					return false, fmt.Errorf("[%d]block stack peak not match: %s != %s", ctx.CurrentLineNum, *v, word.Word)
				}
				Stack(ctx).Pop()
			default:
				return false, fmt.Errorf("[%d]unknown trace word: %s", ctx.CurrentLineNum, ctx.TraceWord.Key)
			}
		}
		return false, nil
	}
	if ctx.IsCurrentTypeNull() {
		if word.Word == "CREATE" {
			ctx.TraceWord = &TraceWord{
				Key:      word.Word,
				Skip:     0,
				NextKeys: createTargets,
			}
		} else {
			if type_, ok := keywordToInfo[word.Word]; ok {
				updateType(ctx, type_.sqlType, word.Word)
				if type_.blockKeyword == StartBlock {
					if word.Word == "CASE" {
						if Stack(ctx).Size() >= 1 {
							Stack(ctx).Push(word.Word)
						}
					} else {
						Stack(ctx).Push(word.Word)
					}
				}
			} else {
				return false, fmt.Errorf("[%d]unknown keyword: %s", ctx.CurrentLineNum, word.Word)
			}
		}
	} else {
		if kewordInfo, ok := keywordToInfo[word.Word]; ok {
			if word.Word == "BEGIN" {
				if Stack(ctx).Size() == 1 {
					Stack(ctx).Pop()
				}
			}
			if kewordInfo.blockKeyword == StartBlock {
				Stack(ctx).Push(word.Word)
			}
		}

		if word.Word == "END" {
			switch Stack(ctx).Size() {
			case 0:
				return false, nil
			case 1:
				if isBlockType(ctx.TypeWord) {
					Stack(ctx).Clear()
					return true, nil
				}
			}
			v, err := Stack(ctx).Peak()
			if err != nil {
				return false, fmt.Errorf("[%d]block stack peak error: %v", ctx.CurrentLineNum, err)
			}
			if _, ok := endTargets[*v]; ok {
				ctx.TraceWord = &TraceWord{
					Key:      word.Word,
					Skip:     0,
					NextKeys: endTargets,
				}
			} else {
				Stack(ctx).Pop()
			}

		}
	}
	return false, nil
}

type BlockKeywordType int

const (
	NotBlockKeyword BlockKeywordType = iota
	StartBlock
	EndBlock
)
