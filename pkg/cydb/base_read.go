// Package cydb provides database operations and SQL parsing utilities.
package cydb

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/fj1981/infrakit/pkg/cyutil"
)

// SQLType 定义了 SQL 类型的接口
// 实现此接口的类型可以表示不同的 SQL 语句类型（如 SELECT, INSERT 等）
type SQLType interface {
	// AsInt 返回 SQL 类型的整数表示
	AsInt() int
	// String 返回 SQL 类型的字符串表示
	String() string
	// IsBlock 判断该 SQL 类型是否为块级语句（如存储过程、函数等）
	IsBlock() bool

	// IsNull 判断该 SQL 类型是否为空
	IsNull() bool
}

// ExtraData 定义了额外数据的接口
// 用于在解析过程中存储特定于方言的额外信息
type ExtraData interface {
	IsBlockEnd(string) bool
}

// TraceWord 用于跟踪关键字的结构体
// 在解析 SQL 语句时用于识别和处理特定的关键字
type TraceWord struct {
	// Key 表示当前关键字
	Key string
	// Skip 表示跳过的字符数
	Skip int
	// NextKeys 表示可能的下一个关键字及其对应的 SQL 类型
	NextKeys map[string]SQLType
}

// ParseContext 用于解析 SQL 文件的上下文结构体
// 存储解析过程中的状态和中间结果
type ParseContext struct {
	// CurrentStmt 当前正在构建的 SQL 语句
	CurrentStmt strings.Builder
	// StartLineNum 当前语句的起始行号
	StartLineNum int
	// CurrentLineNum 当前处理的行号
	CurrentLineNum int
	// LineContent 当前行的内容
	LineContent string
	// HasContent 标记当前是否有内容正在处理
	HasContent bool
	// BlockIndex 块索引，用于标识 SQL 语句块
	BlockIndex int
	// CurrentType 当前 SQL 语句的类型
	CurrentType SQLType
	// TypeWord 类型关键字
	TypeWord string
	// TraceWord 用于跟踪关键字
	TraceWord *TraceWord
	// ExpectedStringEnd 期望的字符串结束标记
	ExpectedStringEnd string
	// IsMultiLineComment 是否在多行注释中
	IsMultiLineComment bool
	// Filter SQL 过滤器
	Filter []string
	// Extra 额外的特定于方言的数据
	Extra ExtraData
	// WordCallback 处理单词的回调函数
	WordCallback ProcessWordCallback
	// PreProcessSqlLine 预处理 SQL 行的回调函数
	PreProcessSqlLine FuncPreProcessSqlLine
}

func (pc *ParseContext) IsBlockEnd(line string) bool {
	if pc.Extra == nil {
		return strings.HasSuffix(line, ";")
	}
	return pc.Extra.IsBlockEnd(line)
}

func (pc *ParseContext) IsCurrentTypeNull() bool {
	return pc.CurrentType == nil || pc.CurrentType.IsNull()
}

// Reset 重置 ParseContext 的所有字段到初始状态
// 在处理完一个 SQL 语句后调用，准备处理下一个语句
func (pc *ParseContext) Reset() {
	pc.CurrentStmt.Reset()
	pc.StartLineNum = 0
	pc.LineContent = ""
	pc.HasContent = false
	pc.CurrentType = nil
	pc.TypeWord = ""
	pc.TraceWord = nil
	pc.ExpectedStringEnd = ""
	pc.IsMultiLineComment = false
	pc.Extra = nil
}

// SQLParseError 定义了SQL解析过程中的错误
// 提供详细的错误信息，包括行号和错误类型
type SQLParseError struct {
	// Message 错误消息
	Message string
	// LineNum 发生错误的行号
	LineNum int
	// LineText 发生错误的行文本
	LineText string
	// ErrorType 错误类型
	ErrorType string
}

// Error 实现 error 接口，返回格式化的错误信息
func (e *SQLParseError) Error() string {
	if e.LineNum > 0 {
		return fmt.Sprintf("%s: %s (line %d: %s)", e.ErrorType, e.Message, e.LineNum, e.LineText)
	}
	return fmt.Sprintf("%s: %s", e.ErrorType, e.Message)
}

// NewSQLParseError 创建一个新的SQL解析错误
// 提供详细的上下文信息以便于调试
func NewSQLParseError(errorType, message string, lineNum int, lineText string) error {
	return &SQLParseError{
		Message:   message,
		LineNum:   lineNum,
		LineText:  lineText,
		ErrorType: errorType,
	}
}

// ReadSQLCallback 用于处理完整的 SQL 语句
// 当一个 SQL 语句解析完成后调用此函数处理结果
func (ctx *ParseContext) ReadSQLCallback(callback FuncSQLStatementCallback) error {
	// 获取并清理当前语句内容
	stmtContent := strings.TrimSpace(ctx.CurrentStmt.String())
	defer func() {
		// 处理完成后重置上下文，准备处理下一个语句
		ctx.Reset()
	}()

	// 如果语句为空，则不处理
	if stmtContent == "" || stmtContent == ";" {
		return nil
	}
	typeStr := ""
	if ctx.CurrentType != nil {
		typeStr = ctx.CurrentType.String()
	} else {
		typeStr = "Other"
	}
	// 增加块索引并创建 SQL 语句对象
	ctx.BlockIndex++
	stmt := &SQLStatement{
		Index:     ctx.BlockIndex,
		StartLine: ctx.StartLineNum,
		EndLine:   ctx.CurrentLineNum,
		Type:      typeStr,
		Content:   stmtContent,
	}
	// 调用回调函数处理 SQL 语句
	return callback(stmt)
}

// ReadSQLFileOptions 定义了读取 SQL 文件的选项
type ReadSQLFileOptions struct {
	// BufferSize 扫描器的缓冲区大小
	BufferSize int
	// MigrationMode 迁移模式过滤器 ("Up", "Down" 或空字符串表示不过滤)
	MigrationMode string
	// Filter SQL 过滤器，用于过滤特定类型的 SQL 语句
	Filter []string

	// WordCallback 单词回调函数，用于处理 SQL 语句中的单词
	WordCallback ProcessWordCallback

	// PreProcessSqlLine 预处理 SQL 行的回调函数
	PreProcessSqlLine FuncPreProcessSqlLine
}

// DefaultBufferSize 默认缓冲区大小 (10MB)
// 可以通过 ReadSQLFileOptions 进行自定义
const DefaultBufferSize = 10 * 1024 * 1024

// 迁移注释标记
const (
	// MigrationCommentPrefix 迁移注释前缀
	MigrationCommentPrefix = "-- +migrate"
	// MigrationUp 向上迁移标记
	MigrationUp = "Up"
	// MigrationDown 向下迁移标记
	MigrationDown = "Down"
)

// WithBufferSize 设置缓冲区大小的选项函数
func WithBufferSize(size int) func(*ReadSQLFileOptions) {
	return func(opts *ReadSQLFileOptions) {
		opts.BufferSize = size
	}
}

// WithFilter 设置 SQL 过滤器的选项函数
func WithFilter(filter []string) func(*ReadSQLFileOptions) {
	return func(opts *ReadSQLFileOptions) {
		opts.Filter = filter
	}
}

// WithMigrationMode 设置迁移模式的选项函数
// mode 可以是 "Up"、"Down" 或空字符串（表示不过滤）
func WithMigrationMode(mode string) func(*ReadSQLFileOptions) {
	return func(opts *ReadSQLFileOptions) {
		opts.MigrationMode = mode
	}
}

// WithWordCallback 设置单词回调的选项函数
func WithWordCallback(callback ProcessWordCallback) func(*ReadSQLFileOptions) {
	return func(opts *ReadSQLFileOptions) {
		opts.WordCallback = callback
	}
}

// WithPreProcessSqlLine 设置预处理 SQL 行的选项函数
func WithPreProcessSqlLine(callback FuncPreProcessSqlLine) func(*ReadSQLFileOptions) {
	return func(opts *ReadSQLFileOptions) {
		opts.PreProcessSqlLine = callback
	}
}

// NewReadSQLFileOptions 创建一个新的 ReadSQLFileOptions 实例并应用选项函数
func NewReadSQLFileOptions(options ...func(*ReadSQLFileOptions)) ReadSQLFileOptions {
	// 设置默认值
	opts := ReadSQLFileOptions{
		BufferSize: DefaultBufferSize,
	}

	// 应用选项函数
	for _, option := range options {
		option(&opts)
	}

	return opts
}

// ReadSQLFileWithOptions 使用自定义选项从文件中读取 SQL 语句
// 支持配置缓冲区大小等选项，提供更灵活的控制
// 使用可变参数函数列表来设置选项，更符合 With 模式的设计理念
func ReadSQLFile(r io.Reader, callback FuncSQLStatementCallback, optionFuncs ...func(*ReadSQLFileOptions)) error {
	if callback == nil {
		return NewSQLParseError("CallbackError", "SQL statement callback cannot be nil", 0, "")
	}

	// 创建选项并应用选项函数
	options := NewReadSQLFileOptions(optionFuncs...)

	scanner := bufio.NewScanner(r)

	// 使用配置的缓冲区大小，如果未指定则使用默认值
	bufferSize := options.BufferSize
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}

	buf := make([]byte, bufferSize)
	scanner.Buffer(buf, bufferSize)

	ctx := &ParseContext{Filter: options.Filter, WordCallback: options.WordCallback, PreProcessSqlLine: options.PreProcessSqlLine}

	// 初始化迁移状态机
	currentMigrationState := ""         // 初始状态为空
	hasFoundMigrationDirective := false // 是否找到过迁移指令
	processMigration := options.MigrationMode != ""

	// 逐行扫描输入
	for scanner.Scan() {
		ctx.CurrentLineNum++
		fullLine := scanner.Text()

		// 检查迁移注释
		if strings.Contains(fullLine, MigrationCommentPrefix) {
			trimmedLine := strings.TrimSpace(fullLine)
			if strings.HasPrefix(trimmedLine, MigrationCommentPrefix) {
				// 提取迁移模式
				parts := strings.Fields(trimmedLine)
				if len(parts) >= 3 && parts[0]+" "+parts[1] == MigrationCommentPrefix {
					// 更新状态机状态
					currentMigrationState = parts[2]
					hasFoundMigrationDirective = true
				}
			}
			continue // 跳过迁移注释行
		}

		// 如果设置了迁移模式过滤
		if processMigration {
			// 如果还没有找到任何迁移指令，则跳过所有行直到找到第一个迁移指令
			if !hasFoundMigrationDirective {
				continue
			}

			// 如果当前状态与目标模式不匹配，跳过该行
			// 状态会保持不变直到遇到下一个迁移指令
			if currentMigrationState != options.MigrationMode {
				continue // 跳过不匹配的迁移模式下的 SQL
			}
		}

		// 按分号分割行，处理可能包含多个语句的行
		parts := strings.SplitAfter(fullLine, ";")
		final := len(parts) - 1
		for i, part := range parts {
			ctx.LineContent = part
			if err := ctx.processLine(callback, i, final); err != nil {
				// 包装错误以提供更多上下文
				if _, ok := err.(*SQLParseError); !ok {
					err = NewSQLParseError("ProcessError", err.Error(), ctx.CurrentLineNum, ctx.LineContent)
				}
				return err
			}
		}
	}

	// 处理最后一个可能未终止的语句
	if ctx.HasContent {
		// 如果设置了迁移模式过滤
		if processMigration {
			// 只在找到迁移指令且当前状态与目标模式匹配时处理
			if hasFoundMigrationDirective && currentMigrationState == options.MigrationMode {
				if err := ctx.ReadSQLCallback(callback); err != nil {
					return NewSQLParseError("CallbackError", err.Error(), ctx.CurrentLineNum, "")
				}
			}
		} else {
			// 如果没有设置迁移模式过滤，则正常处理
			if err := ctx.ReadSQLCallback(callback); err != nil {
				return NewSQLParseError("CallbackError", err.Error(), ctx.CurrentLineNum, "")
			}
		}
	}

	// 检查扫描器是否有错误
	if err := scanner.Err(); err != nil {
		return NewSQLParseError("ScannerError", err.Error(), ctx.CurrentLineNum, "")
	}

	return nil
}

// ProcessWordCallback 用于处理单词的回调函数类型
// 在遍历 SQL 语句中的单词时调用
type ProcessWordCallback func(ctx *ParseContext, word *cyutil.WordInfo) (bool, error)

// FuncPreProcessSqlLine 用于预处理 SQL 行的回调函数类型 返回 true 则跳过该行
type PreRetType int

const (
	PreRetTypeReturn = iota
	PreRetTypeCallCallback
	PreRetTypeContinue
)

type FuncPreProcessSqlLine func(ctx *ParseContext, pline *string) (PreRetType, error)

// processLine 处理单行内容
// 这是 SQL 解析的核心函数，处理注释、字符串和 SQL 关键字
func (ctx *ParseContext) processLine(callback FuncSQLStatementCallback, cur, final int) error {
	if callback == nil {
		return NewSQLParseError("CallbackError", "SQL statement callback cannot be nil", ctx.CurrentLineNum, ctx.LineContent)
	}

	// 清理行内容
	pline := strings.TrimSpace(ctx.LineContent)
	if pline == "" {
		return nil
	}

	// 检查是否为语句终止符
	if pline == "/" {
		if ctx.HasContent {
			return ctx.ReadSQLCallback(callback)
		}
		return nil
	}

	// 处理字符串内容和注释
	// ReplaceSqlStringContent 替换字符串内容为占位符，避免误解析字符串中的关键字
	pline, ctx.ExpectedStringEnd = cyutil.ReplaceSqlStringContent(pline, ctx.ExpectedStringEnd)
	// RemoveSqlComments 移除 SQL 注释，处理多行注释状态
	pline, ctx.IsMultiLineComment = cyutil.RemoveSqlComments(pline, ctx.IsMultiLineComment)
	pline = strings.TrimSpace(pline)
	if pline == "" {
		return nil
	}

	// 如果这是第一行有内容的行，记录起始行号
	if !ctx.HasContent {
		ctx.HasContent = true
		ctx.StartLineNum = ctx.CurrentLineNum
	}
	if ctx.PreProcessSqlLine != nil {
		ret, err := ctx.PreProcessSqlLine(ctx, &pline)
		if err != nil {
			return err
		}
		switch ret {
		case PreRetTypeReturn:
			return nil
		case PreRetTypeCallCallback:
			return ctx.ReadSQLCallback(callback)
		}
	}
	ctx.CurrentStmt.WriteString(ctx.LineContent)
	if cur == final {
		ctx.CurrentStmt.WriteString("\n")
	}

	// 遍历行中的单词，调用 WordCallback 处理
	err := cyutil.TraverseString(pline, "() \t;", func(word *cyutil.WordInfo) error {
		if ctx.WordCallback == nil {
			return NewSQLParseError("CallbackError", "WordCallback is nil", ctx.CurrentLineNum, ctx.LineContent)
		}
		cb, err := ctx.WordCallback(ctx, word)
		if err != nil {
			return err
		}
		if cb {
			return ctx.ReadSQLCallback(callback)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 如果额外数据为空且行以终止符结束，处理完整语句
	if ctx.IsBlockEnd(pline) {
		return ctx.ReadSQLCallback(callback)
	}
	return nil
}
