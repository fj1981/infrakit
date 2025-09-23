package cydb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/duke-git/lancet/v2/slice"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

// SQLOperationType defines the type of SQL operation (SELECT, INSERT, UPDATE, DELETE).

type OP string

const (
	// 比较运算符
	OP_EQ          OP = "="
	OP_NEQ         OP = "!="
	OP_GT          OP = ">"
	OP_GTE         OP = ">="
	OP_LT          OP = "<"
	OP_LTE         OP = "<="
	OP_LIKE        OP = "LIKE"
	OP_IN          OP = "IN"
	OP_NOT_IN      OP = "NOT IN"
	OP_IS_NULL     OP = "IS NULL"
	OP_IS_NOT_NULL OP = "IS NOT NULL"
	OP_BETWEEN     OP = "BETWEEN"
	OP_EXISTS      OP = "EXISTS"
	OP_NOT_EXISTS  OP = "NOT EXISTS"

	// 算术运算符
	OP_ADD      OP = "+"
	OP_SUBTRACT OP = "-"
	OP_MULTIPLY OP = "*"
	OP_DIVIDE   OP = "/"
	OP_MOD      OP = "%"
)

type Value interface {
	GetValue() (string, error)
	needTransform() bool
}

type ParameterValue struct {
	Name string // 参数名
}

func (pv *ParameterValue) ToSQL(dt DatabaseTransformer) (string, error) {
	return fmt.Sprintf(":%s", pv.Name), nil
}

func (pv *ParameterValue) SetAlias(alias string) Expression {
	return pv
}
func (pv *ParameterValue) GetFields() []string {
	return nil
}
func (pv *ParameterValue) GetValue() (string, error) {
	return fmt.Sprintf(":%s", pv.Name), nil
}
func (pv *ParameterValue) needTransform() bool {
	return false
}

// LiteralValue 表示直接嵌入的值
type LiteralValue struct {
	Value  any  // 实际值
	Embed  bool // 是否直接嵌入
	Fields []string
}

func (lv *LiteralValue) SetAlias(alias string) Expression {
	return lv
}
func (lv *LiteralValue) GetFields() []string {
	return lv.Fields
}

func (lv *LiteralValue) ToSQL(dt DatabaseTransformer) (string, error) {
	return lv.GetValue()
}

func (lv *LiteralValue) GetValue() (string, error) {
	if lv.Embed {
		return fmt.Sprintf("%v", lv.Value), nil
	}
	// 根据值的类型进行不同处理
	switch v := lv.Value.(type) {
	case string:
		// 如果是函数调用或已格式化的表达式
		if strings.Contains(v, "(") || strings.Contains(v, ")") {
			return v, nil
		}
		// 普通字符串需要加引号并转义
		v = strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", v), nil
	case nil:
		// NULL 值
		return "NULL", nil
	default:
		return cyutil.ToString(v), nil
	}
}

func (lv *LiteralValue) needTransform() bool {
	return !lv.Embed
}

// Expression 表示SQL表达式接口
type Expression interface {
	ToSQL(dt DatabaseTransformer) (string, error)
	GetFields() []string
	SetAlias(alias string) Expression
}

// SimpleExpr 表示简单字段表达式
type SimpleExpr struct {
	Field  string     // 字段名
	Table  string     // 表名（可选）
	Schema string     // 模式名（可选）
	Alias  string     // 别名（可选）
	Value  Expression // 值（可选）
}

// ToSQL 将简单表达式转换为SQL字符串
func (se *SimpleExpr) ToSQL(dt DatabaseTransformer) (string, error) {
	field := dt.EscapeColumnName(se.Field)
	if field == "" {
		return "", errors.New("field is empty")
	}
	schema := se.Schema
	table := dt.EscapeTableName(se.Table)
	sb := strings.Builder{}
	if schema != "" {
		sb.WriteString(schema)
		sb.WriteString(".")
	}
	if table != "" {
		sb.WriteString(table)
		sb.WriteString(".")
	}
	sb.WriteString(field)
	alias := dt.EscapeColumnName(se.Alias)
	if alias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(alias)
	}
	return sb.String(), nil
}

func (se *SimpleExpr) toFieldsStr(dt DatabaseTransformer) (string, error) {
	field := dt.EscapeColumnName(se.Field)
	if field == "" {
		return "", errors.New("field is empty")
	}
	schema := se.Schema
	table := dt.EscapeTableName(se.Table)
	sb := strings.Builder{}
	if schema != "" {
		sb.WriteString(schema)
		sb.WriteString(".")
	}
	if table != "" {
		sb.WriteString(table)
		sb.WriteString(".")
	}
	sb.WriteString(field)
	return sb.String(), nil
}

func (pv *SimpleExpr) toValueStr(dt DatabaseTransformer) (string, string, error) {
	if pv.Value != nil {
		v, err := pv.Value.ToSQL(dt)
		if err != nil {
			return "", "", err
		}
		return v, pv.Field, nil
	}
	return fmt.Sprintf(":%s", pv.Field), pv.Field, nil
}

func (se *SimpleExpr) toAssignStr(dt DatabaseTransformer) (string, string, error) {
	field, err := se.toFieldsStr(dt)
	if err != nil {
		return "", "", err
	}
	value, field2, err := se.toValueStr(dt)
	if err != nil {
		return "", "", err
	}
	return fmt.Sprintf("%s = %s", field, value), field2, nil
}

func (se *SimpleExpr) getMapKey() string {
	return fmt.Sprintf("%s.%s", se.Table, se.Field)
}

// GetFields 返回表达式中涉及的字段名
func (se *SimpleExpr) GetFields() []string {
	return []string{se.Field}
}

func (se *SimpleExpr) SetAlias(alias string) Expression {
	se.Alias = alias
	return se
}

// ArithExpr 表示算术表达式
type ArithExpr struct {
	Left  Expression // 左侧表达式
	Right Expression // 右侧表达式
	Op    string     // 运算符（+, -, *, /, %）
	Alias string     // 别名（可选）
}

// 获取运算符的优先级
func getOperatorPrecedence(op string) int {
	// 运算符优先级（数字越大，优先级越高）
	switch op {
	case "+", "-":
		return 1
	case "*", "/", "%":
		return 2
	default:
		return 0 // 未知运算符
	}
}

// ToSQL 将算术表达式转换为SQL字符串
func (ae *ArithExpr) ToSQL(dt DatabaseTransformer) (string, error) {
	// 递归处理左右两侧表达式
	left, err := ae.Left.ToSQL(dt)
	if err != nil {
		return "", err
	}

	right, err := ae.Right.ToSQL(dt)
	if err != nil {
		return "", err
	}

	// 获取当前运算符的优先级
	currentPrecedence := getOperatorPrecedence(ae.Op)

	// 检查左侧表达式是否需要括号
	if leftArith, ok := ae.Left.(*ArithExpr); ok {
		leftPrecedence := getOperatorPrecedence(leftArith.Op)
		if leftPrecedence < currentPrecedence {
			// 如果左侧表达式的优先级低于当前运算符，需要括号
			left = fmt.Sprintf("(%s)", left)
		}
	}

	// 检查右侧表达式是否需要括号
	if rightArith, ok := ae.Right.(*ArithExpr); ok {
		rightPrecedence := getOperatorPrecedence(rightArith.Op)
		// 对于同优先级的运算符，只有减法和除法需要特殊处理
		if rightPrecedence < currentPrecedence ||
			(rightPrecedence == currentPrecedence && (ae.Op == "-" || ae.Op == "/")) {
			// 如果右侧表达式的优先级低于当前运算符，需要括号
			right = fmt.Sprintf("(%s)", right)
		}
	}

	// 生成算术表达式，不再总是添加括号
	sql := fmt.Sprintf("%s %s %s", left, ae.Op, right)

	// 如果有别名，添加 AS 子句
	if ae.Alias != "" {
		// 当有别名时，整个表达式需要括号
		sql = fmt.Sprintf("(%s) AS %s", sql, dt.EscapeColumnName(ae.Alias))
	}

	return sql, nil
}

// GetFields 返回表达式中涉及的字段名
func (ae *ArithExpr) GetFields() []string {
	// 合并左右两侧表达式的字段
	fields := ae.Left.GetFields()
	fields = append(fields, ae.Right.GetFields()...)
	return fields
}

// GetAlias 返回表达式的别名
func (ae *ArithExpr) GetAlias() string {
	return ae.Alias
}

// SetAlias 设置表达式的别名
func (ae *ArithExpr) SetAlias(alias string) Expression {
	ae.Alias = alias
	return ae
}

// FuncExpr 表示函数调用表达式
type FuncExpr struct {
	Name     string       // 函数名称，如 SUM, COUNT, MAX 等
	Args     []Expression // 函数参数
	Distinct bool         // 是否使用 DISTINCT
	Alias    string       // 别名（可选）
}

// WhenThenClause 表示 CASE 表达式中的 WHEN-THEN 子句
type WhenThenClause struct {
	When Expression // WHEN 条件
	Then Expression // THEN 结果
}

// CaseExpr 表示 SQL CASE 表达式
type CaseExpr struct {
	Value       Expression       // 用于简单 CASE 表达式的比较值，搜索 CASE 表达式为 nil
	WhenClauses []WhenThenClause // WHEN-THEN 子句列表
	ElseClause  Expression       // ELSE 子句（可选）
	Alias       string           // 别名（可选）
}

// ToSQL 将函数表达式转换为SQL字符串
func (fe *FuncExpr) ToSQL(dt DatabaseTransformer) (string, error) {
	// 处理函数参数
	argStrs := make([]string, 0, len(fe.Args))
	for _, arg := range fe.Args {
		argStr, err := arg.ToSQL(dt)
		if err != nil {
			return "", err
		}
		argStrs = append(argStrs, argStr)
	}

	// 构建函数调用字符串
	args := strings.Join(argStrs, ", ")

	// 基本函数调用格式
	sql := ""

	// 如果有 DISTINCT 关键字
	if fe.Distinct && len(fe.Args) > 0 {
		sql = fmt.Sprintf("%s(DISTINCT %s)", fe.Name, args)
	} else {
		sql = fmt.Sprintf("%s(%s)", fe.Name, args)
	}

	// 如果有别名，添加 AS 子句
	if fe.Alias != "" {
		sql = fmt.Sprintf("%s AS %s", sql, dt.EscapeColumnName(fe.Alias))
	}

	return sql, nil
}

// GetFields 返回表达式中涉及的字段名
func (fe *FuncExpr) GetFields() []string {
	var fields []string
	for _, arg := range fe.Args {
		fields = append(fields, arg.GetFields()...)
	}
	return fields
}

// GetAlias 返回表达式的别名
func (fe *FuncExpr) GetAlias() string {
	return fe.Alias
}

// SetAlias 设置表达式的别名
func (fe *FuncExpr) SetAlias(alias string) Expression {
	fe.Alias = alias
	return fe
}

// ToSQL 将 CASE 表达式转换为 SQL 字符串
func (ce *CaseExpr) ToSQL(dt DatabaseTransformer) (string, error) {
	var sql strings.Builder
	sql.WriteString("CASE")

	// 简单 CASE 表达式
	if ce.Value != nil {
		valueSQL, err := ce.Value.ToSQL(dt)
		if err != nil {
			return "", err
		}
		sql.WriteString(" ")
		sql.WriteString(valueSQL)
	}

	// WHEN-THEN 子句
	for _, wt := range ce.WhenClauses {
		whenSQL, err := wt.When.ToSQL(dt)
		if err != nil {
			return "", err
		}
		thenSQL, err := wt.Then.ToSQL(dt)
		if err != nil {
			return "", err
		}

		sql.WriteString(" WHEN ")
		sql.WriteString(whenSQL)
		sql.WriteString(" THEN ")
		sql.WriteString(thenSQL)
	}

	// ELSE 子句
	if ce.ElseClause != nil {
		elseSQL, err := ce.ElseClause.ToSQL(dt)
		if err != nil {
			return "", err
		}
		sql.WriteString(" ELSE ")
		sql.WriteString(elseSQL)
	}

	sql.WriteString(" END")

	// 如果有别名，添加 AS 子句
	if ce.Alias != "" {
		sql.WriteString(" AS ")
		sql.WriteString(dt.EscapeColumnName(ce.Alias))
	}

	return sql.String(), nil
}

// GetFields 返回表达式中涉及的字段名
func (ce *CaseExpr) GetFields() []string {
	var fields []string

	if ce.Value != nil {
		fields = append(fields, ce.Value.GetFields()...)
	}

	for _, wt := range ce.WhenClauses {
		fields = append(fields, wt.When.GetFields()...)
		fields = append(fields, wt.Then.GetFields()...)
	}

	if ce.ElseClause != nil {
		fields = append(fields, ce.ElseClause.GetFields()...)
	}

	return fields
}

// SetAlias 设置表达式的别名
func (ce *CaseExpr) SetAlias(alias string) Expression {
	ce.Alias = alias
	return ce
}

// 表达式创建辅助函数

// FIELD 创建字段表达式
func FIELD(name string) Expression {
	str := strings.Split(name, ".")
	switch len(str) {
	case 1:
		return &SimpleExpr{Field: name}
	case 2:
		return &SimpleExpr{
			Table: str[0],
			Field: str[1],
		}
	case 3:
		return &SimpleExpr{
			Schema: str[0],
			Table:  str[1],
			Field:  str[2],
		}
	default:
		return &SimpleExpr{Field: name}
	}
}

// CONST 创建常量表达式
func CONST(value any) Expression {
	// 将常量包装为简单表达式
	return &LiteralValue{Value: value}
}

// ADD 创建加法表达式
func ADD(left, right Expression) Expression {
	return &ArithExpr{
		Left:  left,
		Right: right,
		Op:    string(OP_ADD),
	}
}

// SUB 创建减法表达式
func SUB(left, right Expression) Expression {
	return &ArithExpr{
		Left:  left,
		Right: right,
		Op:    string(OP_SUBTRACT),
	}
}

// MUL 创建乘法表达式
func MUL(left, right Expression) Expression {
	return &ArithExpr{
		Left:  left,
		Right: right,
		Op:    string(OP_MULTIPLY),
	}
}

// DIV 创建除法表达式
func DIV(left, right Expression) Expression {
	return &ArithExpr{
		Left:  left,
		Right: right,
		Op:    string(OP_DIVIDE),
	}
}

// MOD 创建取模表达式
func MOD(left, right Expression) Expression {
	return &ArithExpr{
		Left:  left,
		Right: right,
		Op:    string(OP_MOD),
	}
}

// FUNC 创建函数调用表达式
func FUNC(name string, args ...Expression) Expression {
	return &FuncExpr{
		Name: name,
		Args: args,
	}
}

// FUNC_DISTINCT 创建带 DISTINCT 的函数调用表达式
func FUNC_DISTINCT(name string, args ...Expression) Expression {
	return &FuncExpr{
		Name:     name,
		Args:     args,
		Distinct: true,
	}
}

// 常用函数辅助函数

// SUM 创建 SUM 函数表达式
func SUM(arg Expression) Expression {
	return FUNC("SUM", arg)
}

// COUNT 创建 COUNT 函数表达式
func COUNT(arg Expression) Expression {
	return FUNC("COUNT", arg)
}

// COUNT_DISTINCT 创建 COUNT(DISTINCT ...) 函数表达式
func COUNT_DISTINCT(arg Expression) Expression {
	return FUNC_DISTINCT("COUNT", arg)
}

// MAX 创建 MAX 函数表达式
func MAX(arg Expression) Expression {
	return FUNC("MAX", arg)
}

// MIN 创建 MIN 函数表达式
func MIN(arg Expression) Expression {
	return FUNC("MIN", arg)
}

// AVG 创建 AVG 函数表达式
func AVG(arg Expression) Expression {
	return FUNC("AVG", arg)
}

// COALESCE 创建 COALESCE 函数表达式
func COALESCE(args ...Expression) Expression {
	return FUNC("COALESCE", args...)
}

// IFNULL 创建 IFNULL 函数表达式
func IFNULL(arg1, arg2 Expression) Expression {
	return FUNC("IFNULL", arg1, arg2)
}

// CONCAT 创建 CONCAT 函数表达式
func CONCAT(args ...Expression) Expression {
	return FUNC("CONCAT", args...)
}

// CASE 创建简单 CASE 表达式
func CASE(value Expression, whenThenPairs ...Expression) Expression {
	if len(whenThenPairs)%2 != 0 {
		panic("CASE expression requires even number of when-then arguments")
	}

	ce := &CaseExpr{
		Value:       value,
		WhenClauses: make([]WhenThenClause, 0, len(whenThenPairs)/2),
	}

	for i := 0; i < len(whenThenPairs); i += 2 {
		ce.WhenClauses = append(ce.WhenClauses, WhenThenClause{
			When: whenThenPairs[i],
			Then: whenThenPairs[i+1],
		})
	}

	return ce
}

// CASE_WHEN 创建搜索 CASE 表达式
func CASE_WHEN(whenThenPairs ...Expression) Expression {
	if len(whenThenPairs)%2 != 0 {
		panic("CASE expression requires even number of when-then arguments")
	}

	ce := &CaseExpr{
		WhenClauses: make([]WhenThenClause, 0, len(whenThenPairs)/2),
	}

	for i := 0; i < len(whenThenPairs); i += 2 {
		ce.WhenClauses = append(ce.WhenClauses, WhenThenClause{
			When: whenThenPairs[i],
			Then: whenThenPairs[i+1],
		})
	}

	return ce
}

// ELSE 为 CASE 表达式添加 ELSE 子句
func (ce *CaseExpr) ELSE(elseClause Expression) Expression {
	ce.ElseClause = elseClause
	return ce
}

// Where represents a SQL where condition with field, operator, and value
// Short and clear name for SQL condition construction
type whereItem struct {
	Left  interface{} // 左侧表达式或字段，可以是 string、Expression 或 ColumnExpr
	Right interface{} // 右侧值，可以是 Expression、ValueType（ParameterValue/LiteralValue）、SQLBuilder 或原始值
	Op    OP          // 操作符
}

func (w *whereItem) ToCondition(dt DatabaseTransformer) (*Condition, error) {
	// 处理特殊操作符
	switch w.Op {
	case OP_IS_NULL, OP_IS_NOT_NULL:
		// 这些操作符不需要右侧值
		leftSQL, leftFields, err := w.processLeft(dt)
		if err != nil {
			return nil, err
		}
		return &Condition{
			Condition: fmt.Sprintf("%s %s", leftSQL, string(w.Op)),
			Fields:    leftFields,
		}, nil

	case OP_BETWEEN:
		// BETWEEN 操作符需要两个值
		leftSQL, leftFields, err := w.processLeft(dt)
		if err != nil {
			return nil, err
		}

		// 检查右侧是否为切片且包含两个元素
		values, ok := w.Right.([]any)
		if !ok || len(values) != 2 {
			return nil, errors.New("BETWEEN operator requires exactly two values as a slice")
		}

		// 处理两个值
		// 创建临时值处理第一个值
		literalValue1 := &LiteralValue{Value: values[0]}
		start, err := literalValue1.ToSQL(dt)
		if err != nil {
			return nil, err
		}

		// 创建临时值处理第二个值
		literalValue2 := &LiteralValue{Value: values[1]}
		end, err := literalValue2.ToSQL(dt)
		if err != nil {
			return nil, err
		}

		allFields := append(leftFields, literalValue1.GetFields()...)
		allFields = append(allFields, literalValue2.GetFields()...)

		return &Condition{
			Condition: fmt.Sprintf("%s BETWEEN %s AND %s", leftSQL, start, end),
			Fields:    allFields,
		}, nil

	case OP_IN, OP_NOT_IN:
		// IN 操作符需要一个值列表
		leftSQL, leftFields, err := w.processLeft(dt)
		if err != nil {
			return nil, err
		}

		// 处理右侧值列表
		valueList, valueFields, err := w.processInValues(dt)
		if err != nil {
			return nil, err
		}

		allFields := append(leftFields, valueFields...)

		return &Condition{
			Condition: fmt.Sprintf("%s %s (%s)", leftSQL, string(w.Op), valueList),
			Fields:    allFields,
		}, nil

	case OP_EXISTS, OP_NOT_EXISTS:
		if expr, ok := w.Right.(Expression); ok {
			r, err := expr.ToSQL(dt)
			if err != nil {
				return nil, err
			}
			return &Condition{
				Condition: fmt.Sprintf("%s %s", string(w.Op), r),
				Fields:    expr.GetFields(),
			}, nil
		} else if sb, ok := w.Right.(SQLBuilder); ok {
			r, err := sb.Type(SQLOperationSelect).Build(dt)
			if err != nil {
				return nil, err
			}
			return &Condition{
				Condition: fmt.Sprintf("%s (%s)", string(w.Op), r.SQL),
				Fields:    r.ParamOrder,
			}, nil
		} else if subquery, ok := w.Right.(string); ok {
			// 如果值是字符串，假设它已经是一个有效的子查询
			return &Condition{
				Condition: fmt.Sprintf("%s (%s)", string(w.Op), subquery),
				Fields:    []string{},
			}, nil
		} else {
			return nil, fmt.Errorf("%s operator requires a subquery", w.Op)
		}

	default:
		// 处理标准二元操作符
		leftSQL, leftFields, err := w.processLeft(dt)
		if err != nil {
			return nil, err
		}

		rightSQL, rightFields, err := w.processRight(dt, leftFields)
		if err != nil {
			return nil, err
		}

		allFields := append(leftFields, rightFields...)

		return &Condition{
			Condition: fmt.Sprintf("%s %s %s", leftSQL, string(w.Op), rightSQL),
			Fields:    allFields,
		}, nil
	}
}

// processLeft 处理左侧表达式或字段
func (w *whereItem) processLeft(dt DatabaseTransformer) (string, []string, error) {
	switch left := w.Left.(type) {
	case string:
		// 如果是字符串，则将其视为字段名
		return dt.EscapeColumnName(left), []string{left}, nil

	case Expression:
		// 如果是表达式，则调用其 ToSQL 方法
		sql, err := left.ToSQL(dt)
		return sql, left.GetFields(), err

	case SQLBuilder:
		// 如果是 SQL 构建器，则构建子查询
		result, err := left.Build(dt)
		if err != nil {
			return "", nil, err
		}
		return result.SQL, result.ParamOrder, nil

	default:
		// 其他类型，直接转换为字符串
		return cyutil.ToString(left), []string{}, nil
	}
}

// processRight 处理右侧值
func (w *whereItem) processRight(dt DatabaseTransformer, leftFields []string) (string, []string, error) {
	switch right := w.Right.(type) {
	case Expression:
		// 如果是表达式，直接使用其 ToSQL 方法
		rightSQL, err := right.ToSQL(dt)
		if err != nil {
			return "", nil, err
		}
		return rightSQL, right.GetFields(), nil
	case SQLBuilder:
		// 如果是 SQL 构建器，则构建子查询
		result, err := right.Build(dt)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("(%s)", result.SQL), result.ParamOrder, nil

	case string:
		// 如果是字符串
		if strings.HasPrefix(right, ":") {
			// 参数占位符形式（以冒号开头）
			paramName := right[1:]
			return right, []string{paramName}, nil
		}
		r, err := (&LiteralValue{Value: right}).ToSQL(dt)
		if err != nil {
			return "", nil, err
		}
		return r, []string{}, nil

	case []any:
		// 如果是切片，特殊处理（用于 IN 操作符）
		values := make([]string, 0, len(right))
		params := make([]string, 0)

		for _, v := range right {
			switch val := v.(type) {
			case Expression:
				sql, err := val.ToSQL(dt)
				if err != nil {
					return "", nil, err
				}
				values = append(values, sql)
				params = append(params, val.GetFields()...)

			default:
				// 其他类型创建一个字面值
				lv := &LiteralValue{Value: val}
				sql, err := lv.ToSQL(dt)
				if err != nil {
					return "", nil, err
				}
				values = append(values, sql)
				params = append(params, lv.GetFields()...)
			}
		}

		return fmt.Sprintf("(%s)", strings.Join(values, ", ")), params, nil

	default:
		// 默认情况下，创建一个参数值
		paramName := ""
		if len(leftFields) > 0 {
			paramName = leftFields[0]
		} else {
			paramName = fmt.Sprintf("%v", right)
		}

		// 创建参数值
		paramValue := &ParameterValue{
			Name: paramName,
		}

		r, err := paramValue.ToSQL(dt)
		if err != nil {
			return "", nil, err
		}
		return r, []string{paramName}, nil
	}
}

// processInValues 处理 IN 操作符的值列表
func (w *whereItem) processInValues(dt DatabaseTransformer) (string, []string, error) {
	switch values := w.Right.(type) {
	case []any:
		// 处理值列表
		valueStrings := make([]string, 0, len(values))
		var allFields []string

		// 处理每个值
		for _, val := range values {
			// 判断值的类型
			var valStr string
			var fields []string
			var err error

			// 如果是表达式
			if expr, ok := val.(Expression); ok {
				valStr, err = expr.ToSQL(dt)
				if err != nil {
					return "", nil, err
				}
				fields = expr.GetFields()
			} else {
				// 默认情况下，创建一个直接值
				lv := &LiteralValue{Value: val}
				valStr, err = lv.ToSQL(dt)
				if err != nil {
					return "", nil, err
				}
				fields = lv.GetFields()
			}

			valueStrings = append(valueStrings, valStr)
			allFields = append(allFields, fields...)
		}

		return strings.Join(valueStrings, ", "), allFields, nil
	case SQLBuilder:
		r, err := values.Build(dt)
		if err != nil {
			return "", nil, err
		}
		return r.SQL, r.ParamOrder, nil
	case Expression:
		// 如果是子查询表达式
		valueSQL, err := values.ToSQL(dt)
		if err != nil {
			return "", nil, err
		}
		return valueSQL, values.GetFields(), nil

	default:
		return "", nil, errors.New("IN operator requires a slice of values or a subquery expression")
	}
}

type whereCondition struct {
	condition string
}

func (w *whereCondition) ToCondition(dt DatabaseTransformer) (*Condition, error) {
	return &Condition{
		Condition: w.condition,
		Fields:    []string{},
	}, nil
}

type WhereOP string

const (
	WhereOPAnd WhereOP = "AND"
	WhereOPOr  WhereOP = "OR"
)

type whereGroup struct {
	operator   WhereOP // "AND" 或 "OR"
	conditions []Where // 条件列表
}

func (w *whereGroup) ToCondition(dt DatabaseTransformer) (*Condition, error) {
	if w == nil {
		return nil, nil
	}
	conditions := []string{}
	fields := []string{}
	for _, v := range w.conditions {
		if v == nil {
			return nil, errors.New("where condition is nil")
		}
		c, err := v.ToCondition(dt)
		if err != nil {
			return nil, err
		}
		if c.Condition != "" {
			conditions = append(conditions, c.Condition)
			fields = append(fields, c.Fields...)
		}
	}
	if len(conditions) == 0 {
		return nil, nil
	}
	return &Condition{
		Condition: fmt.Sprintf("(%s)", strings.Join(conditions, " "+string(w.operator)+" ")),
		Fields:    fields,
	}, nil
}

type joinClause struct {
	joinType     string // INNER, LEFT, RIGHT, FULL
	table        TableSource
	onConditions []Where
}

// sqlBuilder implements SQLBuilder interface with database compatibility
type sqlBuilder struct {
	operationType SQLOperationType
	count         []Expression
	table         TableSource
	delTables     []TableSource
	database      string
	distinct      bool
	values        [][]Expression
	updates       []Expression
	subQueryData  SQLBuilder
	columns       []Expression
	primaryKeys   []Expression
	from          TableSource
	whereClause   *whereGroup
	joins         []joinClause
	groupBy       []Expression
	having        Where
	orderBy       []OrderBy
	limitValue    string
	offsetValue   string
}

// OffsetExpr implements SQLBuilder.
func (qb *sqlBuilder) OffsetExpr(offset Expression) SQLBuilder {
	panic("unimplemented")
}

func Builder() SQLBuilder {
	return &sqlBuilder{}
}

type WhereOptionFunc func(*whereItem) *whereItem

// WithParameter 创建参数值
func WithParameter(name string) WhereOptionFunc {
	return func(w *whereItem) *whereItem {
		// 将右侧值转换为参数值
		w.Right = &ParameterValue{
			Name: name,
		}
		return w
	}
}

// WithLiteral 创建直接嵌入的值
func WithLiteral(v any) WhereOptionFunc {
	return func(w *whereItem) *whereItem {
		// 将右侧值转换为直接嵌入的值
		w.Right = &LiteralValue{Value: v}
		return w
	}
}

var WithValue = WithLiteral

func WithNativeValue(v any) WhereOptionFunc {
	return func(w *whereItem) *whereItem {
		w.Right = v
		return w
	}
}

func createWhere(field any, op OP, f ...WhereOptionFunc) Where {
	switch f := field.(type) {
	case string:
		if field != "" {
			parsedField, err := parseExpression(f)
			if err != nil {
				panic(err)
			}
			field = parsedField
		}
	case Expression:
	default:
		panic("field must be string or Expression")
	}
	w := &whereItem{
		Left: field,
		Op:   op,
	}
	for _, f := range f {
		f(w)
	}
	return w
}

func EQ(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_EQ, f...)
}

// RawFieldCondition 创建原始字段条件（字段名不转义）
func WHERE(field string, value any, ops ...OP) Where {
	op := OP_EQ
	if len(ops) > 0 {
		op = ops[0]
	}
	return &whereItem{
		Left:  field,
		Right: value,
		Op:    op,
	}
}

func NEQ(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_NEQ, f...)
}

func GT(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_GT, f...)
}

func GTE(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_GTE, f...)
}

func LT(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_LT, f...)
}

func LTE(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_LTE, f...)
}

func LIKE(field string, value string, pt LikePatternType, f ...WhereOptionFunc) Where {
	value = EscapeLikePattern(value, pt)
	return createWhere(field, OP_LIKE, append(f, WithLiteral(value))...)
}
func LIKER(field string, value string, f ...WhereOptionFunc) Where {
	return LIKE(field, value, EndsWith, f...)
}
func LIKEL(field string, value string, f ...WhereOptionFunc) Where {
	return LIKE(field, value, StartsWith, f...)
}
func LIKEC(field string, value string, f ...WhereOptionFunc) Where {
	return LIKE(field, value, Contains, f...)
}

func IN(field string, values []any, f ...WhereOptionFunc) Where {
	if len(values) == 1 {
		if _, ok := values[0].(SQLBuilder); ok {
			return createWhere(field, OP_IN, append(f, WithNativeValue(values[0]))...)
		}
		if _, ok := values[0].(Expression); ok {
			return createWhere(field, OP_IN, append(f, WithNativeValue(values[0]))...)
		}
	}
	return createWhere(field, OP_IN, append(f, WithNativeValue(values))...)
}

func NOT_IN(field string, values []any, f ...WhereOptionFunc) Where {
	if _, ok := values[0].(SQLBuilder); ok {
		return createWhere(field, OP_IN, append(f, WithNativeValue(values[0]))...)
	}
	if _, ok := values[0].(Expression); ok {
		return createWhere(field, OP_IN, append(f, WithNativeValue(values[0]))...)
	}
	return createWhere(field, OP_NOT_IN, append(f, WithNativeValue(values))...)
}

func IS_NULL(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_IS_NULL, f...)
}

func IS_NOT_NULL(field string, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_IS_NOT_NULL, f...)
}

func BETWEEN(field string, start, end any, f ...WhereOptionFunc) Where {
	return createWhere(field, OP_BETWEEN, append(f, WithNativeValue([]any{start, end}))...)
}

// EXISTS 创建 EXISTS 条件，用于检查子查询是否返回结果
// subquery 可以是 SQLBuilder 或者已经格式化的 SQL 字符串
func EXISTS(subquery Expression, f ...WhereOptionFunc) Where {
	return createWhere("", OP_EXISTS, append(f, WithNativeValue(subquery))...)
}

// NOT_EXISTS 创建 NOT EXISTS 条件，用于检查子查询是否不返回结果
// subquery 可以是 SQLBuilder 或者已经格式化的 SQL 字符串
func NOT_EXISTS(subquery Expression, f ...WhereOptionFunc) Where {
	return createWhere("", OP_NOT_EXISTS, append(f, WithNativeValue(subquery))...)
}

type Table struct {
	Schema string // 数据库/schema名称
	Name   string // 表名
	Alias  string // 表别名
}

func (t *Table) GetAlias() string {
	if t.Alias != "" {
		return t.Alias
	}
	return t.Name
}

func (t *Table) ToSQL(dt DatabaseTransformer) (string, error) {
	name := dt.EscapeTableName(t.Name)
	if t.Alias != "" {
		return name + " " + dt.EscapeTableName(t.Alias), nil
	}
	return name, nil
}
func ALIAS(table string, alias string) TableSource {
	table1 := strings.Split(table, ".")
	if len(table1) == 1 {
		return &Table{
			Name:  table1[0],
			Alias: alias,
		}
	}
	if len(table1) == 2 {
		return &Table{
			Schema: table1[0],
			Name:   table1[1],
			Alias:  alias,
		}
	}
	return TABLE(table, alias)
}

func TABLE(param ...string) TableSource {
	parts := []string{}
	for _, v := range param {
		parts = append(parts, strings.Split(v, ".")...)
	}
	switch len(parts) {
	case 0:
		return nil
	case 1:
		return &Table{
			Name: parts[0],
		}
	case 2:
		return &Table{
			Schema: parts[0],
			Name:   parts[1],
		}
	case 3:
		return &Table{
			Schema: parts[0],
			Name:   parts[1],
			Alias:  parts[2],
		}
	default:
		return nil
	}
}

func SUBQUERY(builder SQLBuilder, alias ...string) TableSource {
	if len(alias) == 0 {
		return &SubQuery{
			Builder: builder,
		}
	}
	return &SubQuery{
		Builder: builder,
		Alias:   alias[0],
	}
}

type SubQuery struct {
	Builder SQLBuilder
	Alias   string
	fields  []string
}

func (s *SubQuery) GetAlias() string {
	return s.Alias
}

func (s *SubQuery) SetAlias(alias string) Expression {
	s.Alias = alias
	return s
}

func (s *SubQuery) GetFields() []string {
	return s.fields
}

func (s *SubQuery) ToSQL(dt DatabaseTransformer) (string, error) {
	con, err := s.ToCondition(dt)
	if err != nil {
		return "", err
	}
	return con.Condition, nil
}

func (s *SubQuery) ToCondition(dt DatabaseTransformer) (*Condition, error) {
	b, err := s.Builder.Type(SQLOperationSelect).Build(dt)
	if err != nil {
		return nil, err
	}
	s.fields = b.ParamOrder
	alias := s.Alias
	if alias != "" {
		alias = " AS " + alias
	}

	return &Condition{
		Condition: "(" + b.SQL + ")" + alias,
		Fields:    s.fields,
	}, nil
}

func AS(v ...string) Expression {
	l := len(v)
	if l == 0 {
		return nil
	}
	str := strings.Join(v, " ")
	expr, err := parseExpression(str)
	if err != nil {
		panic(err)
	}
	return expr
}

// === 逻辑组合便利函数 ===

// AND 创建AND条件组合的WhereClause
// 注意：这些是条件组合函数，不是OP操作符
func AND(conditions ...Where) Where {
	// 过滤掉 nil 条件
	var validConditions []Where
	for _, c := range conditions {
		if c != nil {
			// 扁平化嵌套的 AND 条件
			if wg, ok := c.(*whereGroup); ok && wg.operator == WhereOPAnd {
				validConditions = append(validConditions, wg.conditions...)
			} else {
				validConditions = append(validConditions, c)
			}
		}
	}

	// 如果没有有效条件，返回 nil
	if len(validConditions) == 0 {
		return nil
	}

	// 如果只有一个条件，直接返回该条件
	if len(validConditions) == 1 {
		return validConditions[0]
	}

	// 返回合并后的条件组
	return &whereGroup{
		operator:   WhereOPAnd,
		conditions: validConditions,
	}
}

// OR 创建OR条件组合的WhereClause
func OR(conditions ...Where) Where {
	// 过滤掉 nil 条件
	var validConditions []Where
	for _, c := range conditions {
		if c != nil {
			// 扁平化嵌套的 OR 条件
			if wg, ok := c.(*whereGroup); ok && wg.operator == WhereOPOr {
				validConditions = append(validConditions, wg.conditions...)
			} else {
				validConditions = append(validConditions, c)
			}
		}
	}

	// 如果没有有效条件，返回 nil
	if len(validConditions) == 0 {
		return nil
	}

	// 如果只有一个条件，直接返回该条件
	if len(validConditions) == 1 {
		return validConditions[0]
	}

	// 返回合并后的条件组
	return &whereGroup{
		operator:   WhereOPOr,
		conditions: validConditions,
	}
}

// Combine 组合AND和OR条件
func COMBINE(andConditions []Where, orConditions []Where) Where {
	if len(andConditions) == 0 {
		return OR(orConditions...)
	}
	if len(orConditions) == 0 {
		return AND(andConditions...)
	}
	return &whereGroup{
		operator:   WhereOPAnd,
		conditions: append(andConditions, OR(orConditions...)),
	}
}

type FuncWithBuilder func(SQLBuilder) SQLBuilder

func WithDatabase(database string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Database(database)
	}
}

func WithTable(table any) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Table(table)
	}
}

func WithFields(fields any, reset ...bool) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Fields(fields, reset...)
	}
}

func WithSelect(selects any) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Select(selects)
	}
}
func WithCount(field string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Count(field)
	}
}

func WithEQ(field ...string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		ws := slice.Map(field, func(_ int, f string) Where {
			return EQ(f)
		})
		return swc.Where(AND(ws...))
	}
}
func WithNEQ(field ...string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		ws := slice.Map(field, func(_ int, f string) Where {
			return NEQ(f)
		})
		return swc.Where(AND(ws...))
	}
}

func WithGTE(field ...string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		ws := slice.Map(field, func(_ int, f string) Where {
			return GTE(f)
		})
		return swc.Where(AND(ws...))
	}
}

func WithLTE(field ...string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		ws := slice.Map(field, func(_ int, f string) Where {
			return LTE(f)
		})
		return swc.Where(AND(ws...))
	}
}

func WithLT(field ...string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		ws := slice.Map(field, func(_ int, f string) Where {
			return LT(f)
		})
		return swc.Where(AND(ws...))
	}
}

func WithGT(field ...string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		ws := slice.Map(field, func(_ int, f string) Where {
			return GT(f)
		})
		return swc.Where(AND(ws...))
	}
}

func WithLike(field string, value string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Where(LIKE(field, value, Contains))
	}
}

func WithLikeR(field string, value string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Where(LIKE(field, value, EndsWith))
	}
}

func WithLikeL(field string, value string) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Where(LIKE(field, value, StartsWith))
	}
}

func WithBetween(field string, start, end any) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Where(BETWEEN(field, start, end))
	}
}

func WithIn(field string, values []any) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Where(IN(field, values))
	}
}

func WithNotIn(field string, values []any) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Where(NOT_IN(field, values))
	}
}

func WithWhere(where ...Where) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.WhereAnd(where...)
	}
}

func WithWhereAnd(where ...Where) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		if len(where) == 0 {
			return swc
		}
		return swc.WhereAnd(where...)
	}
}

func WithWhereOr(where ...Where) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		if len(where) == 0 {
			return swc
		}
		return swc.WhereOr(where...)
	}
}

func WithLimit(limit int) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Limit(limit)
	}
}

func WithOffset(offset int) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Offset(offset)
	}
}

func toExpressions(field any) []Expression {
	ce := []Expression{}
	switch v := field.(type) {
	case string:
		parts := strings.Split(v, ",")
		for _, p := range parts {
			c, err := parseExpression(p)
			if err != nil {
				panic(err)
			}
			ce = append(ce, c)
		}
	case Expression:
		ce = append(ce, v)
	case []Expression:
		ce = append(ce, v...)
	default:
		panic("unsupported type: " + fmt.Sprintf("%T", field))
	}
	return ce
}

func ASC(field any) OrderBy {
	return OrderBy{
		Column:    toExpressions(field),
		Direction: "ASC",
	}
}

func DESC(field any) OrderBy {
	return OrderBy{
		Column:    toExpressions(field),
		Direction: "DESC",
	}
}

func WithOrderBy(orderBy ...OrderBy) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.OrderBy(orderBy...)
	}
}

func toExpression(field any) (Expression, error) {
	if field == nil {
		return nil, nil
	}
	switch v := field.(type) {
	case string:
		return parseExpression(v)
	case Expression:
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", field)
	}
}

func ON(leftField any, rightField any, op ...string) Where {
	left, err := toExpression(leftField)
	if err != nil {
		panic(err)
	}
	right, err := toExpression(rightField)
	if err != nil {
		panic(err)
	}
	defaultOp := OP_EQ
	if len(op) > 0 {
		defaultOp = OP(op[0])
	}
	return &whereItem{
		Left:  left,
		Right: right,
		Op:    defaultOp,
	}
}

func WithLeftJoin(tableSrc TableSource, on ...Where) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.LeftJoin(tableSrc, on...)
	}
}

func WithRightJoin(tableSrc TableSource, on ...Where) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.RightJoin(tableSrc, on...)
	}
}

func WithJoin(tableSrc TableSource, on ...Where) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Join(tableSrc, on...)
	}
}

func WithInnerJoin(tableSrc TableSource, on ...Where) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.InnerJoin(tableSrc, on...)
	}
}

func WithDistinct(distinct bool) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Distinct(distinct)
	}
}

func WithValues(data ...[]Expression) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Values(data...)
	}
}

func WithValuesAppend(data ...[]Expression) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.ValuesAppend(data...)
	}
}

func WithSubQueryValues(builder SQLBuilder) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.SubQueryValues(builder)
	}
}
func WithUpdate(columns any) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.Update(columns)
	}
}
func WithUpdateExpr(updates ...Expression) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.UpdateExpr(updates...)
	}
}

func WithGroupBy(columns any) FuncWithBuilder {
	return func(swc SQLBuilder) SQLBuilder {
		return swc.GroupBy(columns)
	}
}

func (qb *sqlBuilder) Type(t SQLOperationType) SQLBuilder {
	qb.operationType = t
	return qb
}

func (qb *sqlBuilder) Database(db string) SQLBuilder {
	qb.database = db
	return qb
}

func (qb *sqlBuilder) Table(tableSrc any) SQLBuilder {
	switch v := tableSrc.(type) {
	case TableSource:
		qb.table = v
	case string:
		qb.table = TABLE(v)
	default:
		panic("unsupported type")
	}
	return qb
}

// Fields implements SQLBuilder.
func (qb *sqlBuilder) Fields(columns any, reset ...bool) SQLBuilder {
	r := []Expression{}

	switch v := columns.(type) {
	case []string:
		for _, vv := range v {
			parts := strings.Split(vv, ",")
			for _, p := range parts {
				c, err := parseExpression(p)
				if err != nil {
					panic(err)
				}
				r = append(r, c)
			}
		}
	case string:
		parts := strings.Split(v, ",")
		for _, p := range parts {
			c, err := parseExpression(p)
			if err != nil {
				panic(err)
			}
			r = append(r, c)
		}
	case Expression:
		r = append(r, v)
	case []Expression:
		r = append(r, v...)
	default:
		panic("unsupported type: " + fmt.Sprintf("%T", columns))
	}

	if len(reset) > 0 && reset[0] {
		qb.columns = r
	} else {
		qb.columns = append(qb.columns, r...)
	}
	return qb
}

// PrimaryKeys implements SQLBuilder.
func (qb *sqlBuilder) PrimaryKeys(keys ...string) SQLBuilder {
	for _, k := range keys {
		p, err := parseExpression(k)
		if err != nil {
			panic(err)
		}
		qb.primaryKeys = append(qb.primaryKeys, p)
	}
	return qb
}
func (qb *sqlBuilder) Select(columns any) SQLBuilder {
	if qb.operationType != SQLOperationUnknown {
		qb.operationType = SQLOperationSelect
	}
	qb.columns = []Expression{}
	switch v := columns.(type) {
	case string:
		parts := strings.Split(v, ",")
		for _, p := range parts {
			c, err := parseExpression(p)
			if err != nil {
				panic(err)
			}
			qb.columns = append(qb.columns, c)
		}
	case []string:
		{
			for _, vv := range v {
				parts := strings.Split(vv, ", ")
				for _, p := range parts {
					c, err := parseExpression(p)
					if err != nil {
						panic(err)
					}
					qb.columns = append(qb.columns, c)
				}
			}
		}
	case Expression:
		qb.columns = append(qb.columns, v)
	case []Expression:
		qb.columns = append(qb.columns, v...)
	default:
		panic("unsupported type")
	}
	return qb
}

func (qb *sqlBuilder) From(table any) SQLBuilder {
	if t, ok := table.(TableSource); ok {
		qb.from = t
	}
	if t, ok := table.(string); ok {
		qb.from = TABLE(t)
	}

	return qb
}

func (qb *sqlBuilder) Count(field string) SQLBuilder {
	parts := strings.Split(field, ", ")
	for _, p := range parts {
		c, err := parseExpression(p)
		if err != nil {
			panic(err)
		}
		qb.count = append(qb.count, c)
	}
	return qb
}

func MergeWhere(w1 Where, w2 Where) Where {
	if w1 == nil {
		return w2
	}
	if w2 == nil {
		return w1
	}
	return AND(w1, w2)
}

func (qb *sqlBuilder) Where(fv Where, reset ...bool) SQLBuilder {
	if qb.whereClause == nil {
		qb.whereClause = &whereGroup{operator: WhereOPAnd, conditions: []Where{}}
	}
	if len(reset) > 0 && reset[0] {
		qb.whereClause.conditions = []Where{fv}
	} else {
		fvs := []Where{}
		if whereGroup, ok := fv.(*whereGroup); ok {
			if whereGroup.operator == WhereOPOr {
				fvs = append(fvs, whereGroup)
			} else {
				fvs = append(fvs, whereGroup.conditions...)
			}
		} else {
			if fv != nil {
				fvs = append(fvs, fv)
			}
		}
		if qb.whereClause.operator == WhereOPOr {
			qb.whereClause = &whereGroup{operator: WhereOPAnd, conditions: append([]Where{OR(qb.whereClause.conditions...)}, fvs...)}
		} else {
			qb.whereClause.conditions = append(qb.whereClause.conditions, fvs...)
		}
	}
	return qb
}

func (qb *sqlBuilder) WhereAnd(fvs ...Where) SQLBuilder {
	qb.Where(AND(fvs...))
	return qb
}

func (qb *sqlBuilder) WhereOr(fvs ...Where) SQLBuilder {
	qb.Where(OR(fvs...))
	return qb
}

func (qb *sqlBuilder) addJoin(joinType string, tableSrc TableSource, fvs ...Where) SQLBuilder {
	qb.joins = append(qb.joins, joinClause{
		joinType:     joinType,
		table:        tableSrc,
		onConditions: fvs,
	})
	return qb
}

func (qb *sqlBuilder) Join(tableSrc TableSource, fvs ...Where) SQLBuilder {
	return qb.addJoin("JOIN", tableSrc, fvs...)
}

func (qb *sqlBuilder) LeftJoin(tableSrc TableSource, fvs ...Where) SQLBuilder {
	return qb.addJoin("LEFT JOIN", tableSrc, fvs...)
}

func (qb *sqlBuilder) RightJoin(tableSrc TableSource, fvs ...Where) SQLBuilder {
	return qb.addJoin("RIGHT JOIN", tableSrc, fvs...)
}

func (qb *sqlBuilder) InnerJoin(tableSrc TableSource, fvs ...Where) SQLBuilder {
	return qb.addJoin("INNER JOIN", tableSrc, fvs...)
}

func (qb *sqlBuilder) GroupBy(columns any) SQLBuilder {
	ce := []Expression{}
	switch v := columns.(type) {
	case string:
		parts := strings.Split(v, ",")
		for _, p := range parts {
			c, err := parseExpression(p)
			if err != nil {
				panic(err)
			}
			ce = append(ce, c)
		}
	case Expression:
		ce = append(ce, v)
	case []Expression:
		ce = append(ce, v...)
	default:
		panic("unsupported type")
	}

	qb.groupBy = append(qb.groupBy, ce...)
	return qb
}

func (qb *sqlBuilder) Having(fv Where) SQLBuilder {
	qb.having = fv
	return qb
}

func (qb *sqlBuilder) HavingAnd(fv ...Where) SQLBuilder {
	qb.having = AND(fv...)
	return qb
}

func (qb *sqlBuilder) HavingOr(fv ...Where) SQLBuilder {
	qb.having = OR(fv...)
	return qb
}

func (qb *sqlBuilder) OrderBy(orderBy ...OrderBy) SQLBuilder {
	qb.orderBy = append(qb.orderBy, orderBy...)
	return qb
}

func (qb *sqlBuilder) Limit(limit int) SQLBuilder {
	qb.limitValue = cyutil.ToString(limit)
	return qb
}

func (qb *sqlBuilder) Offset(offset int) SQLBuilder {
	qb.offsetValue = cyutil.ToString(offset)
	return qb
}

func (qb *sqlBuilder) Distinct(distinct bool) SQLBuilder {
	qb.distinct = distinct
	return qb
}

// Data sets the data for the SQL builder
func (qb *sqlBuilder) Values(data ...[]Expression) SQLBuilder {
	qb.values = data
	return qb
}

func (qb *sqlBuilder) ValuesAppend(data ...[]Expression) SQLBuilder {
	qb.values = append(qb.values, data...)
	return qb
}
func (qb *sqlBuilder) SubQueryValues(builder SQLBuilder) SQLBuilder {
	qb.subQueryData = builder
	return qb
}

func (qb *sqlBuilder) Update(columns any) SQLBuilder {
	updates := []Expression{}
	switch v := columns.(type) {
	case string:
		parts := strings.Split(v, ",")
		for _, p := range parts {
			c, err := parseExpression(p)
			if err != nil {
				panic(err)
			}
			updates = append(updates, c)
		}
	case Expression:
		updates = append(updates, v)
	case []Expression:
		updates = append(updates, v...)
	default:
		panic("unsupported type")
	}
	return qb.UpdateExpr(updates...)
}

func (qb *sqlBuilder) UpdateExpr(updates ...Expression) SQLBuilder {
	if qb.operationType == SQLOperationUnknown {
		qb.operationType = SQLOperationUpdate
	}
	qb.updates = updates
	return qb
}

func (qb *sqlBuilder) LimitPlaceholder(limit string) SQLBuilder {
	qb.limitValue = limit
	return qb
}

func (qb *sqlBuilder) OffsetPlaceholder(offset string) SQLBuilder {
	qb.offsetValue = offset
	return qb
}

func (s *sqlBuilder) getFieldsString(columns []Expression, dt DatabaseTransformer, skipAS bool) (string, error) {
	var sb strings.Builder
	for _, col := range columns {
		var colStr string
		var err error
		if skipAS {
			col2, ok := col.(*SimpleExpr)
			if !ok {
				return "", fmt.Errorf("unsupported column type: %T", col)
			}
			colStr, err = col2.toFieldsStr(dt)
			if err != nil {
				return "", err
			}
		} else {
			colStr, err = col.ToSQL(dt)
			if err != nil {
				return "", err
			}
		}
		sb.WriteString(colStr)
		sb.WriteString(", ")
	}
	return strings.TrimSuffix(sb.String(), ", "), nil
}

func (s *sqlBuilder) GetFieldsString(dt DatabaseTransformer, skipAS bool) (string, error) {
	return s.getFieldsString(s.columns, dt, skipAS)
}

func (s *sqlBuilder) GetPkFieldsString(dt DatabaseTransformer, skipAS bool) (string, error) {
	return s.getFieldsString(s.primaryKeys, dt, skipAS)
}

func (s *sqlBuilder) getPrimaryKeys(dt DatabaseTransformer) (map[string]struct{}, error) {
	r := map[string]struct{}{}
	for _, col := range s.primaryKeys {
		if col2, ok := col.(*SimpleExpr); ok {
			colStr, err := col2.ToSQL(dt)
			if err != nil {
				return nil, err
			}
			r[colStr] = struct{}{}
		}
	}
	return r, nil
}

func (s *sqlBuilder) GetValuesString(dt DatabaseTransformer, all bool) (string, []string, error) {
	var fields = make([]string, 0, len(s.columns))
	var sb strings.Builder
	ld := len(s.values)
	if ld != 0 {
		for i, dd := range s.values {

			sb.WriteString("(")
			for j, col := range dd {
				if j != 0 {
					sb.WriteString(", ")
				}
				colStr, err := col.ToSQL(dt)
				if err != nil {
					return "", nil, err
				}
				sb.WriteString(colStr)
				fd := col.GetFields()
				if len(fd) != 0 {
					fields = append(fields, fd...)
				}
			}
			sb.WriteString(")")
			if i < ld-1 {
				sb.WriteString(", ")
			}
		}
		return sb.String(), fields, nil
	}
	if s.subQueryData != nil {
		br, err := s.subQueryData.Build(dt)
		if err != nil {
			return "", nil, err
		}
		return br.SQL, br.ParamOrder, nil
	}
	if len(s.columns) == 0 {
		return "", nil, fmt.Errorf("no columns")
	}
	sb.WriteString("(")
	for i, col := range s.columns {
		if col2, ok := col.(*SimpleExpr); ok {
			if i != 0 {
				sb.WriteString(", ")
			}
			colStr, field, err := col2.toValueStr(dt)
			if err != nil {
				return "", nil, err
			}
			sb.WriteString(colStr)
			fields = append(fields, field)
		} else {
			return "", nil, fmt.Errorf("unsupported column type: %T", col)
		}
	}
	sb.WriteString(")")
	return sb.String(), fields, nil
}

func (s *sqlBuilder) GetAssignString(dt DatabaseTransformer, all bool) (string, []string, error) {
	var sb strings.Builder
	var fields = make([]string, 0, len(s.columns))
	if len(s.updates) > 0 {
		for i, col := range s.updates {
			if col2, ok := col.(*SimpleExpr); ok {
				if i != 0 {
					sb.WriteString(", ")
				}
				colStr, field, err := col2.toAssignStr(dt)
				if err != nil {
					return "", nil, err
				}
				sb.WriteString(colStr)
				fields = append(fields, field)
			} else {
				return "", nil, fmt.Errorf("unsupported column type: %T", col)
			}
		}
		return sb.String(), fields, nil
	}

	for i, col := range s.columns {
		if col2, ok := col.(*SimpleExpr); ok {
			if i != 0 {
				sb.WriteString(", ")
			}
			colStr, field, err := col2.toAssignStr(dt)
			if err != nil {
				return "", nil, err
			}
			sb.WriteString(colStr)
			fields = append(fields, field)
		} else {
			return "", nil, fmt.Errorf("unsupported column type: %T", col)
		}
	}
	return sb.String(), fields, nil
}

func (s *sqlBuilder) Build(dt DatabaseTransformer) (*BuildResult, error) {
	if s.operationType == SQLOperationUnknown {
		s.operationType = SQLOperationSelect
	}
	switch s.operationType {
	case SQLOperationSelect:
		return s.buildSelect(dt, false)
	case SQLOperationCount:
		return s.buildSelect(dt, true)
	case SQLOperationInsert:
		return s.buildInsert(dt)
	case SQLOperationUpdate:
		return s.buildUpdate(dt)
	case SQLOperationReplace:
		return s.buildReplace(dt)
	case SQLOperationUpsert:
		return s.buildUpsert(dt)
	case SQLOperationDelete:
		return s.buildDelete(dt)
	default:
		return nil, &DatabaseError{
			Code:    ErrCodeUnsupported,
			Message: fmt.Sprintf("Unsupported operation type: %s", s.operationType),
		}
	}
}

func (s *sqlBuilder) getTableName(dt DatabaseTransformer) (string, error) {
	if s.from != nil {
		return s.from.ToSQL(dt)
	}
	if s.table != nil {
		return s.table.ToSQL(dt)
	}
	return "", errors.New("table is nil")
}

// buildInsert 构建INSERT查询语句
func (s *sqlBuilder) buildInsert(dt DatabaseTransformer) (*BuildResult, error) {
	table, err := s.getTableName(dt)
	if err != nil {
		return nil, err
	}
	if table == "" {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "Table name is required for INSERT operation",
		}
	}
	if len(s.columns) == 0 {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "At least one column is required for INSERT operation",
		}
	}
	var sql strings.Builder
	paramOrder := make([]string, 0)
	// 构建INSERT INTO部分
	sql.WriteString("INSERT INTO ")
	sql.WriteString(table)
	// 构建列部分
	sql.WriteString(" (")
	columns, err := s.GetFieldsString(dt, false)
	if err != nil {
		return nil, err
	}
	sql.WriteString(columns)
	sql.WriteString(")")
	// 构建VALUES部分

	values, fields, err := s.GetValuesString(dt, false)
	if strings.HasPrefix(values, "SELECT") {
		sql.WriteString(" ")
		sql.WriteString(values)
	} else {
		sql.WriteString(" VALUES ")
		sql.WriteString(values)
	}
	if err != nil {
		return nil, err
	}
	paramOrder = append(paramOrder, fields...)
	return &BuildResult{
		SQL:        sql.String(),
		ParamOrder: paramOrder,
	}, nil
}

func (s *sqlBuilder) writeWhereOrHaving(sb *strings.Builder, w Where, dt DatabaseTransformer, isHaving bool) (paramOrder []string, err error) {
	condition, err := w.ToCondition(dt)
	if err != nil || condition == nil {
		return nil, err
	}
	if condition.Condition != "" {
		if isHaving {
			sb.WriteString(" HAVING ")
		} else {
			sb.WriteString(" WHERE ")
		}
		condition.Condition = strings.TrimPrefix(condition.Condition, "(")
		condition.Condition = strings.TrimSuffix(condition.Condition, ")")
		sb.WriteString(condition.Condition)
		paramOrder = append(paramOrder, condition.Fields...)
	}
	return paramOrder, nil
}

// buildUpdate 构建UPDATE查询语句
func (s *sqlBuilder) buildUpdate(dt DatabaseTransformer) (*BuildResult, error) {
	table, err := s.getTableName(dt)
	if err != nil {
		return nil, err
	}
	if table == "" {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "Table name is required for UPDATE operation",
		}
	}
	var sql strings.Builder
	paramOrder := make([]string, 0)
	// 构建UPDATE部分
	sql.WriteString("UPDATE ")
	sql.WriteString(dt.EscapeTableName(table))
	err = s.writeJoin(&sql, dt)
	if err != nil {
		return nil, err
	}
	// 构建SET部分
	sql.WriteString(" SET ")
	assignString, fields, err := s.GetAssignString(dt, false)
	if err != nil {
		return nil, err
	}
	if assignString == "" {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "At least one column is required for UPDATE operation",
		}
	}
	sql.WriteString(assignString)
	paramOrder = append(paramOrder, fields...)
	paramOrder2, err := s.writeWhereOrHaving(&sql, s.whereClause, dt, false)
	if err != nil {
		return nil, err
	}
	paramOrder = append(paramOrder, paramOrder2...)
	return &BuildResult{
		SQL:        sql.String(),
		ParamOrder: paramOrder,
	}, nil
}

// buildDelete 构建DELETE查询语句
func (s *sqlBuilder) buildDelete(dt DatabaseTransformer) (*BuildResult, error) {
	table, err := s.getTableName(dt)
	if err != nil {
		return nil, err
	}
	if table == "" {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "Table name is required for DELETE operation",
		}
	}

	var sql strings.Builder

	// 构建DELETE FROM部分
	sql.WriteString("DELETE FROM ")
	tableSrc, err := s.getTableName(dt)
	if err != nil {
		return nil, err
	}
	sql.WriteString(dt.EscapeTableName(tableSrc))
	err = s.writeJoin(&sql, dt)
	if err != nil {
		return nil, err
	}

	paramOrder, err := s.writeWhereOrHaving(&sql, s.whereClause, dt, false)
	if err != nil {
		return nil, err
	}

	err = s.writeOrder(&sql, dt, false)
	if err != nil {
		return nil, err
	}
	// 构建LIMIT和OFFSET部分
	err = s.writePagination(&sql, dt, false)
	if err != nil {
		return nil, err
	}
	return &BuildResult{
		SQL:        sql.String(),
		ParamOrder: paramOrder,
	}, nil
}

// buildReplace 构建REPLACE查询语句
func (s *sqlBuilder) buildReplace(dt DatabaseTransformer) (*BuildResult, error) {
	table, err := s.getTableName(dt)
	if err != nil {
		return nil, err
	}
	if table == "" {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "Table name is required for REPLACE operation",
		}
	}

	if len(s.columns) == 0 {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "At least one column is required for REPLACE operation",
		}
	}
	// 直接调用数据库特定的REPLACE实现，传递主键信息
	sql, paramOrder, err := dt.BuildReplaceSQL(table, s)
	if err != nil {
		return nil, err
	}
	return &BuildResult{
		SQL:        sql,
		ParamOrder: paramOrder,
	}, nil
}

// buildUpsert 构建UPSERT查询语句
func (s *sqlBuilder) buildUpsert(dt DatabaseTransformer) (*BuildResult, error) {
	table, err := s.getTableName(dt)
	if err != nil {
		return nil, err
	}
	if table == "" {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "Table name is required for UPSERT operation",
		}
	}

	if len(s.columns) == 0 {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "At least one column is required for UPSERT operation",
		}
	}

	// 直接调用数据库特定的UPSERT实现，传递主键信息
	sql, paramOrder, err := dt.BuildUpsertSQL(table, s)
	if err != nil {
		return nil, err
	}

	return &BuildResult{
		SQL:        sql,
		ParamOrder: paramOrder,
	}, nil
}

func (s *sqlBuilder) writeJoin(sql *strings.Builder, dt DatabaseTransformer) error {
	for _, join := range s.joins {
		sql.WriteString(" ")
		sql.WriteString(join.joinType)
		sql.WriteString(" ")
		table, err := join.table.ToSQL(dt)
		if err != nil {
			return err
		}
		sql.WriteString(dt.EscapeTableName(table))

		if len(join.onConditions) > 0 {
			sql.WriteString(" ON ")
			onClauses := make([]string, 0, len(join.onConditions))

			for _, onCondition := range join.onConditions {
				str, err := onCondition.ToCondition(dt)
				if err != nil {
					return err
				}
				onClauses = append(onClauses, str.Condition)
			}

			sql.WriteString(strings.Join(onClauses, " AND "))
		}
	}
	return nil
}

func (s *sqlBuilder) writeOrder(sql *strings.Builder, dt DatabaseTransformer, countMode bool) error {
	if !countMode && len(s.orderBy) > 0 {
		sql.WriteString(" ORDER BY ")
		orderClauses := make([]string, len(s.orderBy))

		for i, order := range s.orderBy {
			orderColumnStr, err := s.getFieldsString(order.Column, dt, true)
			if err != nil {
				return err
			}
			if order.Direction == "" {
				orderClauses[i] = orderColumnStr
			} else {
				orderClauses[i] = orderColumnStr + " " + order.Direction
			}
		}
		sql.WriteString(strings.Join(orderClauses, ", "))
	}
	return nil
}

func (s *sqlBuilder) writePagination(sql *strings.Builder, dt DatabaseTransformer, countMode bool) error {
	// 构建LIMIT和OFFSET部分
	if !countMode {
		pagination := dt.BuildPagination(s.limitValue, s.offsetValue)
		if pagination != "" {
			sql.WriteString(" ")
			sql.WriteString(pagination)
		}
	}
	return nil
}

// buildSelect 构建SELECT查询语句
func (s *sqlBuilder) buildSelect(dt DatabaseTransformer, countMode bool) (*BuildResult, error) {
	var sql strings.Builder
	paramOrder := []string{}

	// 构建SELECT部分
	sql.WriteString("SELECT ")

	if s.distinct {
		sql.WriteString("DISTINCT ")
	}
	if countMode {
		if len(s.count) > 0 {
			countFieldsStr, err := s.getFieldsString(s.count, dt, true)
			if err != nil {
				return nil, err
			}
			if s.distinct {
				sql.WriteString(fmt.Sprintf("COUNT(DISTINCT %s) AS COUNT", countFieldsStr))
			} else {
				sql.WriteString(fmt.Sprintf("COUNT(%s) AS COUNT", countFieldsStr))
			}
		} else {
			sql.WriteString("COUNT(1) AS COUNT")
		}
	} else if len(s.columns) == 0 {
		sql.WriteString("*")
	} else {
		columnsFields, err := s.getFieldsString(s.columns, dt, false)
		if err != nil {
			return nil, err
		}
		sql.WriteString(columnsFields)
	}
	tableSrc, err := s.getTableName(dt)
	if err != nil {
		return nil, err
	}
	if tableSrc != "" {
		sql.WriteString(" FROM ")
		sql.WriteString(tableSrc)
	} else {
		return nil, &DatabaseError{
			Code:    ErrCodeInvalidParam,
			Message: "Table name is required for SELECT operation",
		}
	}
	err = s.writeJoin(&sql, dt)
	if err != nil {
		return nil, err
	}

	paramOrder2, err := s.writeWhereOrHaving(&sql, s.whereClause, dt, false)
	if err != nil {
		return nil, err
	}
	paramOrder = append(paramOrder, paramOrder2...)

	// 构建GROUP BY部分
	if len(s.groupBy) > 0 {
		sql.WriteString(" GROUP BY ")
		groupBy, err := s.getFieldsString(s.groupBy, dt, true)
		if err != nil {
			return nil, err
		}
		sql.WriteString(groupBy)
	}
	// 构建HAVING部分
	if s.having != nil {
		paramOrder2, err := s.writeWhereOrHaving(&sql, s.having, dt, true)
		if err != nil {
			return nil, err
		}
		paramOrder = append(paramOrder, paramOrder2...)
	}

	err = s.writeOrder(&sql, dt, countMode)
	if err != nil {
		return nil, err
	}
	// 构建LIMIT和OFFSET部分
	err = s.writePagination(&sql, dt, countMode)
	if err != nil {
		return nil, err
	}
	return &BuildResult{
		SQL:        sql.String(),
		ParamOrder: paramOrder,
	}, nil
}
