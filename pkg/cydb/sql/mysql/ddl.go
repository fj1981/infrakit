package sqlmysql

import (
	"errors"
	"fmt"
	"strings"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

func (s *mysqlSql) GetDDLSqlFunc(funcName DDLSqlFuncName) (DDLSqlFunc, error) {
	switch funcName {
	case FuncNameGetCreateTableSql:
		return func(cli DatabaseClient, tableName ...string) (*SqlContent, error) {
			db, table := GetDBAndTable(cli, tableName...)
			if table == "" {
				return &SqlContent{}, errors.New("table name is empty")
			}
			r, err := s.GetCreateTableSql(cli, db, table)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: tableName[0], Content: r}, nil
		}, nil
	case FuncNameGetCreateViewSql:
		return func(cli DatabaseClient, viewName ...string) (*SqlContent, error) {
			db, view := GetDBAndTable(cli, viewName...)
			if view == "" {
				return nil, errors.New("view name is empty")
			}
			r, err := s.GetCreateViewSql(cli, db, view)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: viewName[0], Content: r}, nil
		}, nil
	case FuncNameGetCreateProcedureSql:
		return func(cli DatabaseClient, procName ...string) (*SqlContent, error) {
			db, proc := GetDBAndTable(cli, procName...)
			if proc == "" {
				return nil, errors.New("procedure name is empty")
			}
			r, err := s.GetCreateProcedureSql(cli, db, proc)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: procName[0], Content: r}, nil
		}, nil
	case FuncNameGetCreateFunctionSql:
		return func(cli DatabaseClient, funcName ...string) (*SqlContent, error) {
			db, func_ := GetDBAndTable(cli, funcName...)
			if func_ == "" {
				return nil, errors.New("function name is empty")
			}
			r, err := s.GetCreateFunctionSql(cli, db, func_)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: funcName[0], Content: r}, nil
		}, nil
	case FuncNameGetTableEventSql:
		return func(cli DatabaseClient, eventName ...string) (*SqlContent, error) {
			db, event := GetDBAndTable(cli, eventName...)
			if event == "" {
				return nil, errors.New("event name is empty")
			}
			r, err := s.GetCreateEventSql(cli, db, event)
			if err != nil {
				return nil, err
			}
			return &SqlContent{Name: eventName[0], Content: r}, nil
		}, nil
	case FuncNameGetBeginSql:
		return func(cli DatabaseClient, name ...string) (*SqlContent, error) {
			db, _ := GetDBAndTable(cli, name...)
			if db == "" {
				return nil, errors.New("database name is empty")
			}
			return &SqlContent{Name: db, Content: fmt.Sprintf("USE %s;\n SET FOREIGN_KEY_CHECKS = 0;\n", db)}, nil
		}, nil
	case FuncNameGetEndSql:
		return func(cli DatabaseClient, _ ...string) (*SqlContent, error) {
			return &SqlContent{Name: "", Content: "\nSET FOREIGN_KEY_CHECKS = 1;\n"}, nil
		}, nil
	default:
		return nil, nil
	}
}

func (s *mysqlSql) GetCreateTableSql(cli DatabaseClient, database, tableName string) (string, error) {
	fullTableName := fmt.Sprintf("`%s`.`%s`", database, tableName)
	sql := fmt.Sprintf("SHOW CREATE TABLE %s", fullTableName)
	v, err := cli.Query(sql)
	if err != nil {
		return "", err
	}
	if len(v) == 0 {
		return "", errors.New("create table sql is empty")
	}
	createTableSQL := cyutil.ToStr(v[0]["Create Table"])
	if createTableSQL == "" {
		return "", errors.New("create table sql is empty")
	}
	triggerSql, err := s.GetCreateTriggerSql(cli, database, tableName)
	if err != nil {
		return "", err
	}
	rets := append([]string{createTableSQL}, triggerSql...)
	return strings.Join(rets, "\n"), nil
}

func (s *mysqlSql) GetCreateTriggerSql(cli DatabaseClient, database, tableName string) ([]string, error) {
	triggerSQL := fmt.Sprintf("SHOW TRIGGERS FROM `%s` WHERE `Table` = '%s'", database, tableName)
	vTriggers, err := cli.Query(triggerSQL)
	if err != nil {
		return nil, err
	}
	ret := []string{}
	for _, trigger := range vTriggers {
		triggerName := cyutil.ToStr(trigger["Trigger"])
		createTriggerSQL := fmt.Sprintf("SHOW CREATE TRIGGER `%s`.`%s`", database, triggerName)
		vt, err := cli.Query(createTriggerSQL)
		if err != nil {
			return nil, err
		}
		if len(vt) > 0 {
			createTriggerSQL = cyutil.ToStr(vt[0]["sql_original_statement"])
			if createTriggerSQL == "" {
				continue
			}
			ret = append(ret, createTriggerSQL)
		}
	}
	return ret, nil
}

func (s *mysqlSql) GetCreateViewSql(cli DatabaseClient, database, viewName string) (string, error) {
	createViewSQL := fmt.Sprintf("SHOW CREATE VIEW `%s`;", viewName)
	result, err := cli.Query(createViewSQL)
	if err != nil {
		return "", err
	}

	if len(result) == 0 {
		return "", fmt.Errorf("view %s not found", viewName)
	}

	// 从结果中提取创建视图的SQL语句
	viewSQL := cyutil.ToStr(result[0]["Create View"])
	if viewSQL == "" {
		viewSQL = cyutil.ToStr(result[0]["Create View"])
		if viewSQL == "" {
			return "", fmt.Errorf("empty create view SQL for %s", viewName)
		}
	}

	return viewSQL, nil
}

func (s *mysqlSql) GetCreateProcedureSql(cli DatabaseClient, database, procName string) (string, error) {
	createProcSQL := fmt.Sprintf("SHOW CREATE PROCEDURE `%s`;", procName)
	result, err := cli.Query(createProcSQL)
	if err != nil {
		return "", err
	}

	if len(result) == 0 {
		return "", fmt.Errorf("procedure %s not found", procName)
	}

	procSQL := cyutil.ToStr(result[0]["Create Procedure"])
	if procSQL == "" {
		procSQL = cyutil.ToStr(result[0]["Create procedure"])
		if procSQL == "" {
			return "", fmt.Errorf("empty create procedure SQL for %s", procName)
		}
	}
	return procSQL, nil
}

func (s *mysqlSql) GetCreateFunctionSql(cli DatabaseClient, database, funcName string) (string, error) {
	// 获取函数创建SQL
	createFuncSQL := fmt.Sprintf("SHOW CREATE FUNCTION `%s`;", funcName)
	result, err := cli.Query(createFuncSQL)
	if err != nil {
		return "", err
	}

	if len(result) == 0 {
		return "", fmt.Errorf("function %s not found", funcName)
	}

	// 从结果中提取创建函数的SQL语句
	funcSQL := cyutil.ToStr(result[0]["Create Function"])
	if funcSQL == "" {
		// 尝试不同的列名（MySQL版本可能有差异）
		funcSQL = cyutil.ToStr(result[0]["Create function"])
		if funcSQL == "" {
			return "", fmt.Errorf("empty create function SQL for %s", funcName)
		}
	}
	return funcSQL, nil
}

func (s *mysqlSql) GetCreateEventSql(cli DatabaseClient, database, eventName string) (string, error) {
	// 获取事件创建SQL
	createEventSQL := fmt.Sprintf("SHOW CREATE EVENT `%s`;", eventName)
	result, err := cli.Query(createEventSQL)
	if err != nil {
		return "", err
	}

	if len(result) == 0 {
		return "", fmt.Errorf("event %s not found", eventName)
	}

	// 从结果中提取创建事件的SQL语句
	eventSQL := cyutil.ToStr(result[0]["Create Event"])
	if eventSQL == "" {
		// 尝试不同的列名（MySQL版本可能有差异）
		eventSQL = cyutil.ToStr(result[0]["Create event"])
		if eventSQL == "" {
			return "", fmt.Errorf("empty create event SQL for %s", eventName)
		}
	}
	return eventSQL, nil
}
