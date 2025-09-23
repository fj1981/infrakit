package cydb

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	_ "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"
)

func IsMySQLKeyword(word string) bool {
	// MySQL真正的保留关键字列表（不区分大小写）
	keywords := map[string]bool{
		"accessible": true, "add": true, "all": true, "alter": true, "analyze": true,
		"and": true, "as": true, "asc": true, "asensitive": true, "before": true,
		"between": true, "bigint": true, "binary": true, "blob": true, "both": true,
		"by": true, "call": true, "cascade": true, "case": true, "change": true,
		"char": true, "character": true, "check": true, "collate": true, "column": true,
		"condition": true, "constraint": true, "continue": true, "convert": true, "create": true,
		"cross": true, "current_date": true, "current_time": true, "current_timestamp": true, "current_user": true,
		"cursor": true, "database": true, "databases": true, "day_hour": true, "day_microsecond": true,
		"day_minute": true, "day_second": true, "dec": true, "decimal": true, "declare": true,
		"default": true, "delayed": true, "delete": true, "desc": true, "describe": true,
		"deterministic": true, "distinct": true, "distinctrow": true, "div": true, "double": true,
		"drop": true, "dual": true, "each": true, "else": true, "elseif": true,
		"enclosed": true, "escaped": true, "exists": true, "exit": true, "explain": true,
		"false": true, "fetch": true, "float": true, "float4": true, "float8": true,
		"for": true, "force": true, "foreign": true, "from": true, "fulltext": true,
		"general": true, "grant": true, "group": true, "having": true, "high_priority": true,
		"hour_microsecond": true, "hour_minute": true, "hour_second": true, "if": true, "ignore": true,
		"in": true, "index": true, "infile": true, "inner": true, "inout": true,
		"insensitive": true, "insert": true, "int": true, "int1": true, "int2": true,
		"int3": true, "int4": true, "int8": true, "integer": true, "interval": true,
		"into": true, "is": true, "iterate": true, "join": true, "key": true,
		"keys": true, "kill": true, "leading": true, "leave": true, "left": true,
		"like": true, "limit": true, "linear": true, "lines": true, "load": true,
		"localtime": true, "localtimestamp": true, "lock": true, "long": true, "longblob": true,
		"longtext": true, "loop": true, "low_priority": true, "master_bind": true, "master_ssl_verify_server_cert": true,
		"match": true, "maxvalue": true, "mediumblob": true, "mediumint": true, "mediumtext": true,
		"middleint": true, "minute_microsecond": true, "minute_second": true, "mod": true, "modifies": true,
		"natural": true, "not": true, "no_write_to_binlog": true, "null": true, "numeric": true,
		"on": true, "optimize": true, "option": true, "optionally": true, "or": true,
		"order": true, "out": true, "outer": true, "outfile": true, "partition": true,
		"precision": true, "primary": true, "procedure": true, "purge": true, "range": true,
		"read": true, "reads": true, "read_write": true, "real": true, "references": true,
		"regexp": true, "release": true, "rename": true, "repeat": true, "replace": true,
		"require": true, "resignal": true, "restrict": true, "return": true, "revoke": true,
		"right": true, "rlike": true, "schema": true, "schemas": true, "second_microsecond": true,
		"select": true, "sensitive": true, "separator": true, "set": true, "show": true,
		"signal": true, "smallint": true, "spatial": true, "specific": true, "sql": true,
		"sqlexception": true, "sqlstate": true, "sqlwarning": true, "sql_big_result": true, "sql_calc_found_rows": true,
		"sql_small_result": true, "ssl": true, "starting": true, "straight_join": true, "table": true,
		"terminated": true, "then": true, "tinyblob": true, "tinyint": true, "tinytext": true,
		"to": true, "trailing": true, "trigger": true, "true": true, "undo": true,
		"union": true, "unique": true, "unlock": true, "unsigned": true, "update": true,
		"usage": true, "use": true, "using": true, "utc_date": true, "utc_time": true,
		"utc_timestamp": true, "values": true, "varbinary": true, "varchar": true, "varcharacter": true,
		"varying": true, "where": true, "while": true, "with": true, "write": true,
		"xor": true, "year_month": true, "zerofill": true,
	}

	// 不区分大小写检查
	return keywords[strings.ToLower(word)]
}

func parseExpression(expr string) (Expression, error) {
	if IsMySQLKeyword(expr) {
		expr = "`" + expr + "`"
	}
	sql := "SELECT " + expr
	builder, err := ParseMySQL(sql)
	if err != nil {
		return nil, err
	}
	if b, ok := builder.(*sqlBuilder); ok {
		if len(b.columns) > 0 {
			return b.columns[0], nil
		} else {
			return nil, cylog.Error("not support")
		}
	}
	return nil, cylog.Error("not support")
}

type ParseMysqlContext struct {
	mu     sync.Mutex
	values []string
}

func (ctx *ParseMysqlContext) GetParamValue(index int) string {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if index >= len(ctx.values) {
		return ""
	}
	return ctx.values[index]
}

func (ctx *ParseMysqlContext) GetParamExpr(index int) *ParameterValue {
	label := ctx.GetParamValue(index)
	if label == "" {
		return nil
	}
	return &ParameterValue{
		Name: label[1:],
	}
}

// ParseMySQL parses a SQL statement and returns a SQLBuilder
func ParseMySQL(sql string) (SQLBuilder, error) {
	// Preprocess SQL to handle parameter placeholders like :active
	// Store parameter placeholders and replace them with values that the parser can handle
	paramPlaceholders := make([]string, 0)
	processedSQL := preprocessSQLParams(sql, &paramPlaceholders)
	ctx := &ParseMysqlContext{
		values: paramPlaceholders,
	}

	p := parser.New()
	stmts, _, err := p.Parse(processedSQL, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statement found")
	}

	// Create a new builder
	builder := Builder()

	// Process the statement based on its type
	switch stmt := stmts[0].(type) {
	case *ast.SelectStmt:
		return buildSelectSQL(ctx, builder, stmt)
	case *ast.InsertStmt:
		return buildInsertSQL(ctx, builder, stmt)
	case *ast.UpdateStmt:
		return buildUpdateSQL(ctx, builder, stmt)
	case *ast.DeleteStmt:
		return buildDeleteSQL(ctx, builder, stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func toTableSource(ctx *ParseMysqlContext, tbl ast.ResultSetNode) (TableSource, error) {
	if src, ok := tbl.(*ast.TableSource); ok {
		if tn, ok := src.Source.(*ast.TableName); ok {
			alias := src.AsName.O
			schema := tn.Schema.O
			table := tn.Name.O
			return TABLE(schema, table, alias), nil
		} else if sub, ok := src.Source.(*ast.SelectStmt); ok {
			// Handle subquery
			subBuilder := Builder()
			subBuilder, err := buildSelectSQL(ctx, subBuilder, sub)
			if err != nil {
				return nil, err
			}

			alias := src.AsName.O
			return SUBQUERY(subBuilder, alias), nil
		}
	}
	return nil, cylog.Error("not support")
}

func buildOrderBy(ctx *ParseMysqlContext, builder SQLBuilder, orderBy *ast.OrderByClause) (SQLBuilder, error) {
	for _, item := range orderBy.Items {
		if col, ok := item.Expr.(*ast.ColumnNameExpr); ok {
			expr := &SimpleExpr{
				Schema: col.Name.Schema.O,
				Table:  col.Name.Table.O,
				Field:  col.Name.Name.O,
			}
			if item.Desc {
				builder = builder.OrderBy(DESC(expr))
			} else {
				builder = builder.OrderBy(ASC(expr))
			}
		}
	}
	return builder, nil
}

// buildSelectSQL builds a SQLBuilder from a SELECT statement
func buildSelectSQL(ctx *ParseMysqlContext, builder SQLBuilder, stmt *ast.SelectStmt) (SQLBuilder, error) {
	builder = builder.Type(SQLOperationSelect)
	// Process columns
	columns := extractSelectColumns(ctx, stmt)
	if len(columns) > 0 {
		builder = builder.Select(columns)
	}
	if stmt.Distinct {
		builder = builder.Distinct(true)
	}
	// Process FROM clause
	if stmt.From != nil {
		if err := buildTableSource(ctx, builder, stmt.From); err != nil {
			return nil, err
		}
	}

	// Process WHERE clause
	if stmt.Where != nil {
		whereCond, err := buildWhereCondition(ctx, stmt.Where)
		if err != nil {
			return nil, err
		}
		builder = builder.Where(whereCond)
	}

	// Process GROUP BY
	if stmt.GroupBy != nil {
		groupByColumns := []Expression{}
		for _, item := range stmt.GroupBy.Items {
			if col, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				expr := &SimpleExpr{
					Schema: col.Name.Schema.O,
					Table:  col.Name.Table.O,
					Field:  col.Name.Name.O,
				}
				groupByColumns = append(groupByColumns, expr)
			}
		}
		if len(groupByColumns) > 0 {
			builder = builder.GroupBy(groupByColumns)
		}
	}

	// Process HAVING
	if stmt.Having != nil {
		havingCond, err := buildWhereCondition(ctx, stmt.Having.Expr)
		if err != nil {
			return nil, err
		}
		builder = builder.Having(havingCond)
	}

	// Process ORDER BY
	if stmt.OrderBy != nil {
		buildOrderBy(ctx, builder, stmt.OrderBy)
	}

	// Process LIMIT and OFFSET
	if stmt.Limit != nil {
		buildLimit(ctx, builder, stmt.Limit)
	}

	return builder, nil
}

func columnNameToSimpleExpr(col *ast.ColumnName) *SimpleExpr {
	return &SimpleExpr{
		Schema: col.Schema.O,
		Table:  col.Table.O,
		Field:  col.Name.O,
	}
}

func assignToSimpleExpr(ctx *ParseMysqlContext, col *ast.Assignment) (*SimpleExpr, error) {
	se := columnNameToSimpleExpr(col.Column)
	if se != nil {
		var err error
		se.Value, err = buildExpression(ctx, col.Expr)
		if err != nil {
			return nil, err
		}
	}
	return se, nil
}

// buildInsertSQL builds a SQLBuilder from an INSERT statement
func buildInsertSQL(ctx *ParseMysqlContext, builder SQLBuilder, stmt *ast.InsertStmt) (SQLBuilder, error) {
	if stmt.IsReplace {
		builder = builder.Type(SQLOperationReplace)
	} else if len(stmt.OnDuplicate) > 0 {
		builder = builder.Type(SQLOperationUpsert)
		updates := make([]Expression, 0, len(stmt.OnDuplicate))
		for _, assign := range stmt.OnDuplicate {
			se, err := assignToSimpleExpr(ctx, assign)
			if err != nil {
				return nil, err
			}
			updates = append(updates, se)
		}
		builder = builder.UpdateExpr(updates...)
	} else {
		builder = builder.Type(SQLOperationInsert)
	}
	// Set table name
	if stmt.Table != nil && stmt.Table.TableRefs != nil && stmt.Table.TableRefs.Left != nil {
		tblSource, err := toTableSource(ctx, stmt.Table.TableRefs.Left)
		if err != nil {
			return nil, err
		}
		builder = builder.Table(tblSource)
	}
	// Set columns
	columns := make([]Expression, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		if col == nil {
			return nil, fmt.Errorf("column is nil")
		}
		v := columnNameToSimpleExpr(col)
		columns = append(columns, v)
	}
	if len(columns) > 0 {
		builder = builder.Fields(columns)
	}
	if stmt.Select != nil {
		selectStmt, ok := stmt.Select.(*ast.SelectStmt)
		if !ok {
			return nil, fmt.Errorf("select is not *ast.SelectStmt")
		}
		subBuilder, err := buildSelectSQL(ctx, Builder(), selectStmt)
		if err != nil {
			return nil, err
		}
		builder = builder.SubQueryValues(subBuilder)
	} else {
		for i, list := range stmt.Lists {
			if list == nil {
				return nil, fmt.Errorf("list %d is nil", i)
			}
			values := make([]Expression, 0, len(list))
			var i int = 0
			for _, item := range list {
				if pe, ok := item.(ast.ParamMarkerExpr); ok {
					pe.SetOrder(i)
					i++
				}
				expr, err := buildExpression(ctx, item)
				if err != nil {
					return nil, err
				}
				values = append(values, expr)
			}
			builder = builder.ValuesAppend(values)
		}
	}
	return builder, nil
}

func buildTableSource(ctx *ParseMysqlContext, builder SQLBuilder, stmt *ast.TableRefsClause) error {
	if stmt == nil {
		return nil
	}
	var rets []ast.ResultSetNode
	if stmt.TableRefs != nil {
		rets = extractTables(stmt)
	}
	for i, ret := range rets {
		if i == 0 {
			ts, err := toTableSource(ctx, ret)
			if err != nil {
				return err
			}
			builder = builder.Table(ts)
			continue
		}

		if join, ok := ret.(*ast.Join); ok {
			if join.Right == nil {
				continue
			}
			ts, err := toTableSource(ctx, join.Right)
			if err != nil {
				return err
			}

			// Extract ON conditions
			var onConditions []Where
			if join.On != nil && join.On.Expr != nil {

				if binOp, ok := join.On.Expr.(*ast.BinaryOperationExpr); ok {
					if leftCol, ok := binOp.L.(*ast.ColumnNameExpr); ok {
						left := columnNameToSimpleExpr(leftCol.Name)
						if rightCol, ok := binOp.R.(*ast.ColumnNameExpr); ok {
							right := columnNameToSimpleExpr(rightCol.Name)
							op, err := getOP(binOp.Op)
							if err != nil {
								return err
							}
							onConditions = append(onConditions, ON(left, right, string(op)))
						}
					}
				}
			}

			// Add the appropriate join type
			switch join.Tp {
			case ast.LeftJoin:
				builder = builder.LeftJoin(ts, onConditions...)
			case ast.RightJoin:
				builder = builder.RightJoin(ts, onConditions...)
			default: // Inner join or other types
				builder = builder.Join(ts, onConditions...)
			}
		}

	}
	return nil
}

// buildUpdateSQL builds a SQLBuilder from an UPDATE statement
func buildUpdateSQL(ctx *ParseMysqlContext, builder SQLBuilder, stmt *ast.UpdateStmt) (SQLBuilder, error) {
	builder = builder.Type(SQLOperationUpdate)
	// Set table name
	if err := buildTableSource(ctx, builder, stmt.TableRefs); err != nil {
		return nil, err
	}

	values := make([]Expression, 0, len(stmt.List))
	for _, item := range stmt.List {
		expr, err := assignToSimpleExpr(ctx, item)
		if err != nil {
			return nil, err
		}
		values = append(values, expr)
	}
	builder = builder.UpdateExpr(values...)
	// Set WHERE condition
	if stmt.Where != nil {
		whereCond, err := buildWhereCondition(ctx, stmt.Where)
		if err != nil {
			return nil, err
		}
		builder = builder.Where(whereCond)
	}

	return builder, nil
}

func buildLimit(ctx *ParseMysqlContext, builder SQLBuilder, stmt *ast.Limit) (SQLBuilder, error) {
	if stmt.Count != nil {
		if pm, ok := stmt.Count.(*test_driver.ParamMarkerExpr); ok {
			cm := ctx.GetParamValue(pm.Order)
			if cm == "" {
				return nil, errors.New("param marker is empty")
			}
			builder = builder.LimitPlaceholder(cm)
		} else {
			var buf strings.Builder
			ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
			stmt.Count.Restore(ctx)
			val := buf.String()
			if i, err := strconv.Atoi(val); err == nil {
				builder = builder.Limit(i)
			}
		}
	}
	if stmt.Offset != nil {
		if pm, ok := stmt.Offset.(*test_driver.ParamMarkerExpr); ok {
			cm := ctx.GetParamValue(pm.Order)
			if cm == "" {
				return nil, errors.New("param marker is empty")
			}
			builder = builder.OffsetPlaceholder(cm)
		} else {
			var buf strings.Builder
			ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
			stmt.Offset.Restore(ctx)
			val := buf.String()
			if i, err := strconv.Atoi(val); err == nil {
				builder = builder.Offset(i)
			}
		}
	}
	return builder, nil
}

// buildDeleteSQL builds a SQLBuilder from a DELETE statement
func buildDeleteSQL(ctx *ParseMysqlContext, builder SQLBuilder, stmt *ast.DeleteStmt) (SQLBuilder, error) {
	builder = builder.Type(SQLOperationDelete)
	// Set table name
	if stmt.TableRefs != nil {
		if err := buildTableSource(ctx, builder, stmt.TableRefs); err != nil {
			return nil, err
		}
	}

	// Set WHERE condition
	if stmt.Where != nil {
		whereCond, err := buildWhereCondition(ctx, stmt.Where)
		if err != nil {
			return nil, err
		}
		builder = builder.Where(whereCond)
	}
	if stmt.Order != nil {
		buildOrderBy(ctx, builder, stmt.Order)
	}

	if stmt.Limit != nil {
		buildLimit(ctx, builder, stmt.Limit)
	}
	return builder, nil
}

func getOP(op opcode.Op) (OP, error) {
	var buf strings.Builder
	ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)

	// Create a binary operation expression with the operator
	expr := &ast.BinaryOperationExpr{Op: op}

	// Restore just the operator part
	if err := expr.Op.Restore(ctx); err != nil {
		return "", err
	}
	return OP(buf.String()), nil
}

func buildExpression(ctx *ParseMysqlContext, expr ast.ExprNode) (Expression, error) {
	switch x := expr.(type) {
	case *ast.ColumnNameExpr:
		// Handle column references
		return &SimpleExpr{
			Schema: x.Name.Schema.O,
			Table:  x.Name.Table.O,
			Field:  x.Name.Name.O,
		}, nil

	case *ast.BinaryOperationExpr:
		// Handle arithmetic operations
		left, err := buildExpression(ctx, x.L)
		if err != nil {
			return nil, err
		}
		right, err := buildExpression(ctx, x.R)
		if err != nil {
			return nil, err
		}

		// Map the operator
		op, err := getOP(x.Op)
		if err != nil {
			return nil, err
		}
		return &ArithExpr{
			Left:  left,
			Right: right,
			Op:    string(op),
		}, nil

	case *ast.FuncCallExpr:
		// Handle function calls
		args := make([]Expression, 0, len(x.Args))
		for _, arg := range x.Args {
			expr, err := buildExpression(ctx, arg)
			if err != nil {
				return nil, err
			}
			args = append(args, expr)
		}

		return &FuncExpr{
			Name: x.FnName.O,
			Args: args,
		}, nil

	case *ast.AggregateFuncExpr:
		// Handle aggregate functions (SUM, COUNT, etc.)
		args := make([]Expression, 0, len(x.Args))
		for _, arg := range x.Args {
			expr, err := buildExpression(ctx, arg)
			if err != nil {
				return nil, err
			}
			args = append(args, expr)
		}

		return &FuncExpr{
			Name:     x.F,
			Args:     args,
			Distinct: x.Distinct,
		}, nil
	case *test_driver.ParamMarkerExpr:
		return ctx.GetParamExpr(x.Order), nil
	case *test_driver.ValueExpr:
		switch d := x.Datum.GetValue().(type) {
		case string:
			if strings.HasPrefix(d, "[@]:") {
				return &ParameterValue{Name: d[4:]}, nil
			}
			return &LiteralValue{Value: d}, nil
		default:
			return &LiteralValue{Value: d}, nil
		}

	case *ast.ParenthesesExpr:
		// Handle parenthesized expressions
		return buildExpression(ctx, x.Expr)

	case *ast.UnaryOperationExpr:
		// Handle unary operations (like -1, +2)
		operand, err := buildExpression(ctx, x.V)
		if err != nil {
			return nil, err
		}

		switch x.Op {
		case opcode.Plus:
			// Unary plus doesn't change the value
			return operand, nil
		case opcode.Minus:
			// For unary minus, create an arithmetic expression with 0 - operand
			zero := &LiteralValue{Value: 0}
			return &ArithExpr{
				Left:  zero,
				Right: operand,
				Op:    string(OP_SUBTRACT),
			}, nil
		default:
			return nil, fmt.Errorf("unsupported unary operator: %v", x.Op)
		}

	case *ast.CaseExpr:
		// 创建 CaseExpr 结构体
		caseExpr := &CaseExpr{
			WhenClauses: make([]WhenThenClause, 0, len(x.WhenClauses)),
		}

		// 处理 CASE 表达式的值部分（简单 CASE 表达式）
		if x.Value != nil {
			valueExpr, err := buildExpression(ctx, x.Value)
			if err != nil {
				return nil, err
			}
			caseExpr.Value = valueExpr
		}

		// 处理 WHEN...THEN 子句
		for _, whenClause := range x.WhenClauses {
			whenExpr, err := buildExpression(ctx, whenClause.Expr)
			if err != nil {
				return nil, err
			}

			thenExpr, err := buildExpression(ctx, whenClause.Result)
			if err != nil {
				return nil, err
			}

			caseExpr.WhenClauses = append(caseExpr.WhenClauses, WhenThenClause{
				When: whenExpr,
				Then: thenExpr,
			})
		}

		// 处理 ELSE 子句
		if x.ElseClause != nil {
			elseExpr, err := buildExpression(ctx, x.ElseClause)
			if err != nil {
				return nil, err
			}
			caseExpr.ElseClause = elseExpr
		}

		return caseExpr, nil

	case *ast.SubqueryExpr:
		// Handle subqueries
		if selectStmt, ok := x.Query.(*ast.SelectStmt); ok {
			subBuilder := Builder()
			subBuilder, err := buildSelectSQL(ctx, subBuilder, selectStmt)
			if err != nil {
				return nil, err
			}
			return &SubQuery{Builder: subBuilder}, nil
		}
		return nil, cylog.Error("unsupported subquery type")

	default:
		// For unsupported expressions, convert to string representation
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
		if err := expr.Restore(ctx); err != nil {
			return nil, err
		}
		return &LiteralValue{Value: buf.String(), Embed: true}, nil
	}
}

func buildWhereCondition(ctx *ParseMysqlContext, expr ast.ExprNode) (Where, error) {
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		switch x.Op {
		case opcode.LogicAnd:
			left, err := buildWhereCondition(ctx, x.L)
			if err != nil {
				return nil, err
			}
			right, err := buildWhereCondition(ctx, x.R)
			if err != nil {
				return nil, err
			}
			return AND(left, right), nil
		case opcode.LogicOr:
			left, err := buildWhereCondition(ctx, x.L)
			if err != nil {
				return nil, err
			}
			right, err := buildWhereCondition(ctx, x.R)
			if err != nil {
				return nil, err
			}
			return OR(left, right), nil
		}
		if x.L != nil && x.R != nil {
			l, err := buildExpression(ctx, x.L)
			if err != nil {
				return nil, err
			}

			op, err := getOP(x.Op)
			if err != nil {
				return nil, err
			}
			v, err := buildExpression(ctx, x.R)
			if err != nil {
				return nil, err
			}
			return createWhere(l, op, WithNativeValue(v)), nil
		}
		return nil, cylog.Error("unsupported  expression")
	case *ast.PatternInExpr:
		if col, ok := x.Expr.(*ast.ColumnNameExpr); ok {
			// Handle subquery case
			if x.Sel != nil {
				if subquery, ok := x.Sel.(*ast.SubqueryExpr); ok {
					if selectStmt, ok := subquery.Query.(*ast.SelectStmt); ok {
						subBuilder := Builder()
						var err error
						subBuilder, err = buildSelectSQL(ctx, subBuilder, selectStmt)
						if err != nil {
							return nil, err
						}
						if x.Not {
							return NOT_IN(col.Name.Name.O, []any{subBuilder}), nil
						}
						return IN(col.Name.Name.O, []any{subBuilder}), nil
					}
				}
			}

			// Handle value list case
			values := make([]any, 0, len(x.List))
			for _, item := range x.List {
				var buf strings.Builder
				ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
				item.Restore(ctx)
				values = append(values, buf.String())
			}

			if x.Not {
				return NOT_IN(col.Name.Name.O, values), nil
			}
			return IN(col.Name.Name.O, values), nil
		}

		// Default case: create a raw condition
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
		x.Restore(ctx)
		return &whereCondition{condition: buf.String()}, nil

	case *ast.PatternLikeOrIlikeExpr:
		if col, ok := x.Expr.(*ast.ColumnNameExpr); ok {
			var buf strings.Builder
			ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
			x.Pattern.Restore(ctx)
			pattern := buf.String()

			// Remove quotes if present
			if len(pattern) >= 2 && pattern[0] == '\'' && pattern[len(pattern)-1] == '\'' {
				pattern = pattern[1 : len(pattern)-1]
			}

			return LIKE(col.Name.Name.O, pattern, None), nil
		}

		// Default case: create a raw condition
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
		x.Restore(ctx)
		return &whereCondition{condition: buf.String()}, nil

	case *ast.IsNullExpr:
		if col, ok := x.Expr.(*ast.ColumnNameExpr); ok {
			if x.Not {
				return IS_NOT_NULL(col.Name.Name.O), nil
			}
			return IS_NULL(col.Name.Name.O), nil
		}

		// Default case: create a raw condition
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
		x.Restore(ctx)
		return &whereCondition{condition: buf.String()}, nil

	case *ast.SubqueryExpr:
		if selectStmt, ok := x.Query.(*ast.SelectStmt); ok {
			subBuilder := Builder()
			var err error
			subBuilder, err = buildSelectSQL(ctx, subBuilder, selectStmt)
			if err != nil {
				return nil, err
			}

			// For EXISTS subqueries
			return EXISTS(&SubQuery{Builder: subBuilder}), nil
		}

		// Default case: create a raw condition
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
		x.Restore(ctx)
		return &whereCondition{condition: buf.String()}, nil

	case *ast.ParenthesesExpr:
		return buildWhereCondition(ctx, x.Expr)

	case *ast.ColumnNameExpr:
		// For a simple column reference, we'll create a basic condition
		return EQ(x.Name.Name.O), nil
	case *ast.ExistsSubqueryExpr:

		subBuilder, err := buildExpression(ctx, x.Sel)
		if err != nil {
			return nil, err
		}
		return EXISTS(subBuilder), nil

	default:
		// For complex expressions, we'll use a raw condition
		var buf strings.Builder
		ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &buf)
		x.Restore(ctx)
		return &whereCondition{condition: buf.String()}, nil
	}
}

// extractSelectColumns extracts column names from a SELECT statement
func extractSelectColumns(ctx *ParseMysqlContext, stmt *ast.SelectStmt) []Expression {
	var cols []Expression
	for _, field := range stmt.Fields.Fields {
		if field.WildCard != nil {
			// 处理通配符表达式，例如 t.* 或 *
			wildcard := &SimpleExpr{
				Schema: field.WildCard.Schema.O,
				Table:  field.WildCard.Table.O,
				Field:  "*",
			}
			cols = append(cols, wildcard)
			continue
		}
		e, err := buildExpression(ctx, field.Expr)
		if err != nil {
			return nil
		}
		if e != nil {
			if field.AsName.O != "" {
				e.SetAlias(field.AsName.O)
			}
			cols = append(cols, e)
		}
	}
	return cols
}

// extractTables extracts tables from a FROM clause
func extractTables(from *ast.TableRefsClause) []ast.ResultSetNode {
	var tables []ast.ResultSetNode

	if from == nil || from.TableRefs == nil {
		return tables
	}

	// Recursively extract table references
	extractTableRefs(from.TableRefs, &tables)

	return tables
}

// extractTableRefs recursively extracts table references
func extractTableRefs(node ast.ResultSetNode, tables *[]ast.ResultSetNode) {
	*tables = append([]ast.ResultSetNode{node}, *tables...)
	if node == nil {
		return
	}
	if n, ok := node.(*ast.Join); ok {
		if n.Left != nil {
			extractTableRefs(n.Left, tables)
		}
	}
}

// preprocessSQLParams replaces parameter placeholders like :active with temporary values
// that the TiDB parser can handle, and stores the original placeholders in the provided map
func preprocessSQLParams(sql string, paramPlaceholders *[]string) string {
	// Regular expression to match parameter placeholders like :active
	paramRegex := regexp.MustCompile(`:(\w+)\b`)

	// Replace each parameter placeholder with a temporary value
	processedSQL := paramRegex.ReplaceAllStringFunc(sql, func(match string) string {
		*paramPlaceholders = append(*paramPlaceholders, match)
		return "?"
	})

	return processedSQL
}

// TestParseMySQL tests the SQL parsing functionality
func TestParseMySQL() {
	//getField("a.id as B")
	dt, _ := GetSqlTransformer("mysql")
	/*
		builder := Builder().Select("key").From("t").Where(IN("t.id", []any{"1", "2"}))
		rc, err := builder.Type(SQLOperationSelect).Build(dt)
		if err != nil {
			panic(err)
		}
		fmt.Println(rc)

		expr, err := parseExpression("key")
		if err != nil {
			panic(err)
		}
		fmt.Println(expr)
	*/
	sqls := []string{
		`UPDATE products p JOIN inventory i ON p.id = i.product_id SET p.stock = i.quantity WHERE i.updated_at > '2023-01-01'`,
		`SELECT a.id, b.name, c.name, d.name FROM A a JOIN B b ON a.id = b.id JOIN C c ON b.id = c.id JOIN D d ON c.id = d.id`,
		`SELECT a.id as A FROM A a LIMIT :limit OFFSET :offset`,
		`INSERT INTO users (name, age, email) VALUES (:name,NOW(), :email)`,
		`SELECT CONCAT(name, '1') AS name,  
               age,
               user_id
        FROM   users
        WHERE  active + 1 + 12 = :active
		AND  SUBSTRING_INDEX(flags, ',', 1) = 'vip' 
	
	`,
		`
SELECT  u.name,
        u.age
FROM    (
        SELECT CONCAT(name, '1') AS name,  
               age,
               user_id
        FROM   users
        WHERE  active + 1 = :active
		AND  SUBSTRING_INDEX(flags, ',', 1) = 'vip' 
        ) AS u
LEFT JOIN orders o
       ON u.user_id = o.user_id
WHERE  u.age > 18
  AND  u.name IN (SELECT name FROM whitelist)
  AND  EXISTS (SELECT 1
               FROM logs
               WHERE logs.user_id = u.user_id)
GROUP BY u.age, u.name
HAVING COUNT(o.id) > 1
ORDER BY u.age DESC
LIMIT 10 OFFSET 5;
	`}

	builder, err := ParseMySQL(sqls[3])
	if err != nil {
		panic(err)
	}

	// Output the result
	result, err := builder.Build(dt)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Generated SQL: %s\n", result.SQL)
	fmt.Printf("Parameters: %v\n", result.ParamOrder)
}
