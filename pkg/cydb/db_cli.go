package cydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/jmoiron/sqlx"
	"github.com/patrickmn/go-cache"
)

type IDBOperWrapper interface {
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	MustExec(query string, args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type txCount struct {
	count map[string]int
	lock  sync.Mutex
}

func (tc *txCount) Inc(key string) int {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if _, ok := tc.count[key]; !ok {
		tc.count[key] = 0
	}
	tc.count[key]++
	return tc.count[key]
}

func (tc *txCount) Dec(key string) int {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if _, ok := tc.count[key]; !ok {
		return -1
	}
	tc.count[key]--
	if tc.count[key] == 0 {
		delete(tc.count, key)
		return 0
	}
	return tc.count[key]
}

func (tc *txCount) Clear(key string) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	delete(tc.count, key)
}

var once sync.Once
var _log *cylog.Logger

func DBLog() *cylog.Logger {
	once.Do(func() {
		_log = cylog.New(cylog.WithCallerSkip(2))
	})
	return _log
}

type DBCli struct {
	cli      IDBOperWrapper
	dbtype   string
	key      string
	database string
	un       string
	pw       string
}

var gTxCount = &txCount{
	count: make(map[string]int),
}

type PARAMS = map[string]interface{}

// NewDBCli creates a new DBCli instance. This is useful for testing purposes.
func NewDBCli(cli IDBOperWrapper, dbtype, key, database, un, pw string) *DBCli {
	return &DBCli{
		cli:      cli,
		dbtype:   dbtype,
		key:      key,
		database: database,
		un:       un,
		pw:       pw,
	}
}

func (d *DBCli) Database() string {
	return d.database
}

func (d *DBCli) DBType() string {
	return d.dbtype
}

func (d *DBCli) PW() string {
	return d.pw
}

func (d *DBCli) GetDB() *sqlx.DB {
	if db, ok := d.cli.(*sqlx.DB); ok {
		return db
	}
	return nil
}

func (d *DBCli) Key() string {
	return d.key
}

func (d *DBCli) Close() error {
	if db, ok := d.cli.(*sqlx.DB); ok {
		return db.Close()
	}
	return nil
}

func (d *DBCli) BeginX() (r *DBCli, err error) {
	defer func() {
		if err == nil {
			gTxCount.Inc(r.key)
		}
	}()
	db, ok := d.cli.(*sqlx.DB)
	if !ok {
		if _, ok := d.cli.(*sqlx.Tx); ok {
			return d, nil
		}
		return nil, errors.New("not support db type: " + d.dbtype)
	}
	tx, err := db.Beginx()
	if err != nil {
		return nil, err
	}
	key := d.key + cyutil.NanoID() + "_T"
	return &DBCli{
		cli:      tx,
		dbtype:   d.dbtype,
		database: d.database,
		un:       d.un,
		pw:       d.pw,
		key:      key,
	}, nil
}

func (d *DBCli) Rollback() error {
	if tx, ok := d.cli.(*sqlx.Tx); ok {
		gTxCount.Clear(d.key)
		return tx.Rollback()
	}
	return nil
}

func (d *DBCli) Commit() error {
	if tx, ok := d.cli.(*sqlx.Tx); ok {
		n := gTxCount.Dec(d.key)
		if n == 0 {
			return tx.Commit()
		}
	}
	return nil
}
func (d *DBCli) WithTransaction(fn func(tx *DBCli) error) (err error) {
	tx, err := d.BeginX()
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %v\nStack trace:\n%s", r, debug.Stack())
		}
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()
	err = fn(tx)
	return err
}

func NormalData(d interface{}) interface{} {
	switch v := d.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.DateTime)
	default:
		return v
	}
}

func NormalData2(d interface{}) interface{} {
	switch v := d.(type) {
	case int64:
		return cyutil.ToStr(v)
	case float64:
		return cyutil.ToStr(v)
	default:
		return NormalData(d)
	}
}

func (cli *DBCli) scanSQLRow(rows *sqlx.Rows) (map[string]interface{}, error) {
	row := make(map[string]interface{})
	if err := rows.MapScan(row); err != nil {
		return nil, err
	}
	ret := make(map[string]interface{})
	for k, v := range row {
		if nil == v {
			ret[k] = nil
			continue
		}
		ret[k] = NormalData(v)
	}
	return ret, nil
}

func (d *DBCli) IsTableExist(tableName string) (bool, error) {
	if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
		r, e := sqlFunc.IsTableExist(d, tableName)
		if e != nil {
			return false, e
		}
		return r, nil
	}
	return false, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) GetTableColumns(tableName string) ([]*DBColumn, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", d.dbtype, d.database, tableName)
	if cachedColumns, found := tableColumnsCache.Get(cacheKey); found {
		return cachedColumns.([]*DBColumn), nil
	}
	if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
		columns, err := sqlFunc.GetTableColumns(d, d.database, tableName)
		if err != nil {
			return nil, err
		}
		tableColumnsCache.Set(cacheKey, columns, 2*time.Minute)
		return columns, nil
	}
	return nil, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) GetPK(tableName string) ([]string, error) {
	cols, err := d.GetTableColumns(tableName)
	if err != nil {
		return nil, err
	}
	var pk []string
	for _, col := range cols {
		if col.ColumnKey == "PRI" {
			pk = append(pk, col.Name)
		}
	}
	return pk, nil
}

func getDBColumn(cols []*DBColumn, name string) (int, *DBColumn) {
	for i, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return i, col
		}
	}
	return -1, nil
}

func (d *DBCli) procssTravel(row map[string]any, dcs []*DBColumn, fn func(*DBCli, *RowData) error) error {
	fds := RowData{Data: make([]*FieldData, 0, len(row))}
	for k, v := range row {
		index, col := getDBColumn(dcs, k)
		if col == nil {
			return errors.New("column not found: " + k)
		}
		fds.Data = append(fds.Data, &FieldData{
			Name:        col.Name,
			Type:        col.DBFieldType,
			OrgDataType: col.OrgDataType,
			Data:        v,
			IsPK:        col.ColumnKey == "PRI",
			IsUQ:        col.ColumnKey == "UQ",
			Nullable:    col.Nullable,
			Index:       index,
		})
	}
	slices.SortFunc(fds.Data, func(a, b *FieldData) int {
		return a.Index - b.Index
	})
	if err := fn(d, &fds); err != nil {
		return err
	}
	return nil
}

func (d *DBCli) TravelQuery(tableName string, selectSQL string, fn func(*DBCli, *RowData) error) error {
	if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
		selectSQL = sqlFunc.PreProcess(selectSQL)
	}
	cols, err := d.GetTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("getting table columns failed: %v", err)
	}
	rows, err := d.cli.Queryx(selectSQL)
	if err != nil {
		return fmt.Errorf("query execution failed: %s | %s", err.Error(), selectSQL)
	}
	defer rows.Close()
	for rows.Next() {
		row := make(map[string]interface{})
		if err := rows.MapScan(row); err != nil {
			return fmt.Errorf("row scan failed: %v", err)
		}
		err = d.procssTravel(row, cols, fn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DBCli) TravelData(tableName string, data []map[string]interface{}, fn func(*DBCli, *RowData) error) error {
	cols, err := d.GetTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("getting table columns failed: %v", err)
	}
	for _, row := range data {
		err := d.procssTravel(row, cols, fn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DBCli) GetDDLSql(funcName DDLSqlFuncName, name ...string) (*SqlContent, error) {
	if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
		f, err := sqlFunc.GetDDLSqlFunc(funcName)
		if err != nil {
			return nil, err
		}
		if f != nil {
			return f(d, name...)
		}
		return nil, nil
	}
	return nil, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) GetSortedSql(funcName SortFuncName, database string, names []string) ([]*SqlContent, error) {
	if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
		f, err := sqlFunc.GetSortedSqlFunc(funcName)
		if err != nil {
			return nil, err
		}
		if f != nil {
			return f(d, names)
		}
		return nil, nil
	}
	return nil, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) preProcess(sql string) (string, error) {
	if d.dbtype == "mysql" {
		return sql, nil
	}
	builder, err := ParseMySQL(sql)
	if err != nil {
		return "", err
	}
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		ret, err := builder.Build(sqlFunc)
		if err != nil {
			return "", err
		}
		return ret.SQL, nil
	}
	return "", errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) nQuery(sql string, data interface{}) ([]map[string]interface{}, error) {
	DBLog().Debug("nQuery sql", "sql", sql, "data", data)
	rows, err := d.cli.NamedQuery(sql, data)
	if err != nil {
		return nil, fmt.Errorf("failed to nQuery: %s | %s", sql, err)
	}
	r := []map[string]interface{}{}
	defer rows.Close()
	for rows.Next() {
		l, err := d.scanSQLRow(rows)
		if err != nil {
			break
		}
		r = append(r, l)
	}
	return r, nil
}
func (d *DBCli) NQuery(sql string, data interface{}) ([]map[string]interface{}, error) {
	sql, err := d.preProcess(sql)
	if err != nil {
		return nil, err
	}
	return d.nQuery(sql, data)
}

func InternalNQuery(cli DatabaseClient, sql string, data interface{}) ([]map[string]interface{}, error) {
	if cli2, ok := cli.(*DBCli); ok {
		return cli2.nQuery(sql, data)
	}
	return nil, errors.New("not support db type: " + cli.DBType())
}

func (d *DBCli) query(sql string, arguments ...interface{}) ([]map[string]interface{}, error) {
	DBLog().Debug("query sql", "sql", sql, "arguments", arguments)
	rows, err := d.cli.Queryx(sql, arguments...)
	if err != nil {
		return nil, fmt.Errorf("[query]: %s | => %s", sql, err)
	}
	r := []map[string]interface{}{}
	defer rows.Close()
	for rows.Next() {
		l, err := d.scanSQLRow(rows)
		if err != nil {
			break
		}
		r = append(r, l)
	}
	return r, nil
}

func (d *DBCli) Query(sql string, arguments ...interface{}) ([]map[string]interface{}, error) {
	/* sql, err := d.preProcess(sql)
	if err != nil {
		return nil, err
	}*/
	return d.query(sql, arguments...)
}

func InternalQuery(cli DatabaseClient, sql string, arguments ...interface{}) ([]map[string]interface{}, error) {
	if cli2, ok := cli.(*DBCli); ok {
		return cli2.query(sql, arguments...)
	}
	return nil, errors.New("not support db type: " + cli.DBType())
}

func (d *DBCli) queryOne(sql string, args ...interface{}) (map[string]interface{}, error) {
	DBLog().Debug("queryOne sql", "sql", sql, "args", args)
	rows, err := d.cli.Queryx(sql, args...)
	if err != nil {
		return nil, fmt.Errorf("[queryOne]: %s | => %s", sql, err)
	}
	defer rows.Close()
	if rows.Next() {
		l, err := d.scanSQLRow(rows)
		return l, err
	}
	return nil, nil
}

func (d *DBCli) QueryOne(sql string, args ...interface{}) (map[string]interface{}, error) {
	sql, err := d.preProcess(sql)
	if err != nil {
		return nil, err
	}
	return d.queryOne(sql, args...)
}

func (d *DBCli) nQueryOne(sql string, data interface{}) (map[string]interface{}, error) {
	DBLog().Debug("query sql", "sql", sql, "data", data)
	rows, err := d.cli.NamedQuery(sql, data)
	if err != nil {
		return nil, fmt.Errorf("[nQueryOne]: %s | => %s", sql, err)
	}
	defer rows.Close()
	if rows.Next() {
		l, err := d.scanSQLRow(rows)
		return l, err
	}
	return nil, nil
}

func InternalNQueryOne(cli DatabaseClient, sql string, data interface{}) (map[string]interface{}, error) {
	if cli2, ok := cli.(*DBCli); ok {
		return cli2.nQueryOne(sql, data)
	}
	return nil, errors.New("not support db type: " + cli.DBType())
}

func (d *DBCli) NQueryOne(sql string, data interface{}) (map[string]interface{}, error) {
	sql, err := d.preProcess(sql)
	if err != nil {
		return nil, err
	}
	return d.nQueryOne(sql, data)
}

func InternalQueryOne(cli DatabaseClient, sql string, arguments ...interface{}) (map[string]interface{}, error) {
	if cli2, ok := cli.(*DBCli); ok {
		return cli2.queryOne(sql, arguments...)
	}
	return nil, errors.New("not support db type: " + cli.DBType())
}

func (d *DBCli) nExcute(sql string, data interface{}) (int64, error) {
	DBLog().Debug("excute sql", "sql", sql, "data", data)
	r, err := d.cli.NamedExec(sql, data)
	if err != nil {
		return 0, fmt.Errorf("[nExcute]: %s | => %s | %v", sql, err, data)
	}
	var rowsAffected int64
	if r != nil {
		rowsAffected, err = r.RowsAffected()
	}
	return rowsAffected, err
}

func (d *DBCli) NExcute(sql string, data interface{}) (int64, error) {
	sql, err := d.preProcess(sql)
	if err != nil {
		return 0, err
	}
	return d.nExcute(sql, data)
}
func InternalExcute(cli DatabaseClient, sql string, arguments ...interface{}) (int64, error) {
	if cli2, ok := cli.(*DBCli); ok {
		return cli2.excute(sql, arguments...)
	}
	return 0, errors.New("not support db type: " + cli.DBType())
}

func (d *DBCli) excute(sql string, arguments ...interface{}) (int64, error) {
	DBLog().Debug("excute sql", "sql", sql, "arguments", arguments)
	r, err := d.cli.Exec(sql, arguments...)
	if err != nil {
		return 0, fmt.Errorf("[excute]: %s | => %s | %v", sql, err, arguments)
	}
	var rowsAffected int64
	if r != nil {
		rowsAffected, err = r.RowsAffected()
	}
	return rowsAffected, err
}

func (d *DBCli) Excute(sql string, arguments ...interface{}) (int64, error) {
	sql, err := d.preProcess(sql)
	if err != nil {
		return 0, err
	}
	return d.excute(sql, arguments...)
}

func (d *DBCli) filterFields(tableName string, fields []string) []string {
	cols, err := d.GetTableColumns(tableName)
	if err != nil {
		return fields
	}
	s := map[string]struct{}{}
	for _, col := range cols {
		s[strings.ToLower(col.Name)] = struct{}{}
	}
	var r []string
	for _, field := range fields {
		if _, ok := s[strings.ToLower(field)]; ok {
			r = append(r, field)
		}
	}
	return r
}

func (d *DBCli) Insert(tableName string, data map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		fields := maputil.Keys(data)
		fields = d.filterFields(tableName, fields)
		builder := Builder().Database(d.Database()).Table(tableName).Fields(fields)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationInsert).Build(sqlFunc)
		if err != nil {
			return 0, err
		}
		return d.nExcute(sqlContent.SQL, data)
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) BatchInsert(tableName string, data []map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		// 使用事务处理批量插入
		var totalAffected int64

		err := d.WithTransaction(func(tx *DBCli) error {
			for _, item := range data {

				fields := maputil.Keys(item)
				fields = tx.filterFields(tableName, fields)
				builder := Builder().Database(d.Database()).Table(tableName).Fields(fields)
				for _, c := range cc {
					builder = c(builder)
				}
				sqlContent, err := builder.Type(SQLOperationInsert).Build(sqlFunc)
				if err != nil {
					return err
				}

				affected, err := tx.nExcute(sqlContent.SQL, item)
				if err != nil {
					return err
				}
				totalAffected += affected
			}
			return nil
		})

		if err != nil {
			return 0, err
		}
		return totalAffected, nil
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) Update(tableName string, data map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {

		fields := maputil.Keys(data)
		fields = d.filterFields(tableName, fields)
		builder := Builder().Database(d.Database()).Table(tableName).Fields(fields)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationUpdate).Build(sqlFunc)
		if err != nil {
			return 0, err
		}
		return d.nExcute(sqlContent.SQL, data)
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) BatchUpdate(tableName string, data []map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {

		// 使用事务处理批量更新
		var totalAffected int64

		err := d.WithTransaction(func(tx *DBCli) error {
			for _, item := range data {
				fields := maputil.Keys(item)
				fields = tx.filterFields(tableName, fields)
				builder := Builder().Database(tx.Database()).Table(tableName).Fields(fields)
				for _, c := range cc {
					builder = c(builder)
				}
				sqlContent, err := builder.Type(SQLOperationUpdate).Build(sqlFunc)
				if err != nil {
					return err
				}

				affected, err := tx.nExcute(sqlContent.SQL, item)
				if err != nil {
					return err
				}
				totalAffected += affected
			}
			return nil
		})

		if err != nil {
			return 0, err
		}
		return totalAffected, nil
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) Upsert(tableName string, data map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		fields := maputil.Keys(data)
		fields = d.filterFields(tableName, fields)
		builder := Builder().Database(d.Database()).Table(tableName).Fields(fields)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationUpsert).Build(sqlFunc)
		if err != nil {
			return 0, err
		}
		return d.nExcute(sqlContent.SQL, data)
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) Replace(tableName string, data map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	pk, err := d.GetPK(tableName)
	if err != nil {
		return 0, err
	}
	if len(pk) == 0 {
		return 0, errors.New("table " + tableName + " has no primary key")
	}
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		fields := maputil.Keys(data)
		fields = d.filterFields(tableName, fields)
		builder := Builder().Database(d.Database()).Table(tableName).Fields(fields).PrimaryKeys(pk...)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationReplace).Build(sqlFunc)
		if err != nil {
			return 0, err
		}
		return d.nExcute(sqlContent.SQL, data)
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) BatchReplace(tableName string, data []map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	pk, err := d.GetPK(tableName)
	if err != nil {
		return 0, err
	}
	if len(pk) == 0 {
		return 0, errors.New("table " + tableName + " has no primary key")
	}
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		var totalAffected int64
		err := d.WithTransaction(func(tx *DBCli) error {
			for _, item := range data {
				fields := maputil.Keys(item)
				fields = tx.filterFields(tableName, fields)
				builder := Builder().Database(tx.Database()).Table(tableName).Fields(fields).PrimaryKeys(pk...)
				for _, c := range cc {
					builder = c(builder)
				}
				sqlContent, err := builder.Type(SQLOperationReplace).Build(sqlFunc)
				if err != nil {
					return err
				}

				affected, err := tx.nExcute(sqlContent.SQL, item)
				if err != nil {
					return err
				}
				totalAffected += affected
			}
			return nil
		})

		if err != nil {
			return 0, err
		}
		return totalAffected, nil
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) List(tableName any, data map[string]interface{}, cc ...FuncWithBuilder) ([]map[string]interface{}, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		builder := Builder().Database(d.Database()).Table(tableName)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationSelect).Build(sqlFunc)
		if err != nil {
			return nil, err
		}
		return d.nQuery(sqlContent.SQL, data)
	}
	return nil, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) ListWithPage(tableName any, data map[string]interface{}, pageIndex int, pageSize int, cc ...FuncWithBuilder) ([]map[string]interface{}, int, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {

		builder := Builder().Database(d.Database()).Table(tableName)
		for _, c := range cc {
			builder = c(builder)
		}
		countSqlContent, err := builder.Type(SQLOperationCount).Build(sqlFunc)
		if err != nil {
			return nil, 0, err
		}
		// 1. 获取总数
		var totalCount int
		r, err := d.nQueryOne(countSqlContent.SQL, data)
		if err != nil {
			return nil, 0, err
		}
		totalCount = cyutil.GetInt(r, "COUNT")
		if totalCount == 0 {
			return nil, 0, nil
		}
		offset := (pageIndex - 1) * pageSize
		if offset > totalCount {
			return nil, totalCount, nil
		}
		builder = builder.Limit(pageSize).Offset(offset)
		dataSqlContent, err := builder.Type(SQLOperationSelect).Build(sqlFunc)
		if err != nil {
			return nil, 0, err
		}

		results, err := d.nQuery(dataSqlContent.SQL, data)
		if err != nil {
			return nil, 0, err
		}

		return results, totalCount, nil
	}

	return nil, 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) Delete(tableName any, data map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		builder := Builder().Database(d.Database()).Table(tableName)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationDelete).Build(sqlFunc)
		if err != nil {
			return 0, err
		}
		return d.nExcute(sqlContent.SQL, data)
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) First(tableName any, data map[string]interface{}, cc ...FuncWithBuilder) (map[string]interface{}, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		builder := Builder().Database(d.Database()).Table(tableName).Limit(1)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationSelect).Build(sqlFunc)
		if err != nil {
			return nil, err
		}
		return d.nQueryOne(sqlContent.SQL, data)
	}

	return nil, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) Count(tableName any, data map[string]interface{}, cc ...FuncWithBuilder) (int64, error) {
	if sqlFunc, ok := GetSqlTransformer(d.dbtype); ok {
		builder := Builder().Database(d.Database()).Table(tableName).Select("COUNT(1) AS count").Limit(1)
		for _, c := range cc {
			builder = c(builder)
		}
		sqlContent, err := builder.Type(SQLOperationSelect).Build(sqlFunc)
		if err != nil {
			return 0, err
		}
		result, err := d.nQueryOne(sqlContent.SQL, data)
		if err != nil {
			return 0, err
		}
		return cyutil.GetInt64(result, "count"), nil
	}
	return 0, errors.New("not support db type: " + d.dbtype)
}

func (d *DBCli) Exists(tableName string, data map[string]interface{}, cc ...FuncWithBuilder) (bool, error) {
	count, err := d.Count(tableName, data, cc...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// readSQLFile reads and returns the content of an SQL file.
func (d *DBCli) ReadSQLFile(r io.Reader, callback FuncSQLStatementCallback, options ...func(*ReadSQLFileOptions)) error {
	if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
		return sqlFunc.ReadSQLFile(r, callback, options...)
	}
	return errors.New("not support db type: " + d.dbtype)
}

var (
	// 创建一个默认过期时间为2分钟的缓存，每5分钟清理一次过期项
	tableColumnsCache = cache.New(2*time.Minute, 5*time.Minute)
)

func (d *DBCli) FieldExists(tableName string, fieldName string) (bool, error) {
	// Get all columns for the specified table
	columns, err := d.GetTableColumns(tableName)
	if err != nil {
		return false, err
	}

	// Convert fieldName to lowercase for case-insensitive comparison
	fieldNameLower := strings.ToLower(fieldName)

	// Check if the specified field exists in the columns map (case-insensitive)
	for _, colName := range columns {
		if strings.EqualFold(colName.Name, fieldNameLower) {
			return true, nil
		}
	}

	return false, nil
}

func (d *DBCli) Select(dest interface{}, query string, args ...interface{}) error {
	err := d.cli.Select(dest, query, args...)
	if err != nil {
		return fmt.Errorf("[Select]: %s | => %s", query, err)
	}
	return nil
}

func (d *DBCli) Get(dest interface{}, query string, args ...interface{}) error {
	err := d.cli.Get(dest, query, args...)
	if err != nil {
		return fmt.Errorf("[Get]: %s | => %s", query, err)
	}
	return nil
}

func (d *DBCli) MakeSureDBExists(dbName string) error {
	if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
		err := sqlFunc.MakeSureDBExists(d, dbName)
		if err != nil {
			return fmt.Errorf("[MakeSureDBExists]: %s | => %s", dbName, err)
		}
		return nil
	}
	return nil
}
