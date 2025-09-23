package cydb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"
)

// DatabaseError represents a database-specific error with error codes
type DatabaseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
	Cause   error  `json:"-"`
}

func (e *DatabaseError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("DB_%s: %s (%s)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("DB_%s: %s", e.Code, e.Message)
}

func (e *DatabaseError) Unwrap() error {
	return e.Cause
}

// Common database error codes
const (
	ErrCodeConnection   = "CONNECTION_FAILED"
	ErrCodeQuery        = "QUERY_FAILED"
	ErrCodeTransaction  = "TRANSACTION_FAILED"
	ErrCodeConstraint   = "CONSTRAINT_VIOLATION"
	ErrCodeNotFound     = "NOT_FOUND"
	ErrCodeDuplicate    = "DUPLICATE_KEY"
	ErrCodeTimeout      = "TIMEOUT"
	ErrCodeInvalidParam = "INVALID_PARAMETER"
	ErrCodeUnsupported  = "UNSUPPORTED_OPERATION"
)

// NewDatabaseError creates a new DatabaseError
func NewDatabaseError(code, message string) *DatabaseError {
	return &DatabaseError{Code: code, Message: message}
}

// WithDetails adds details to the error
func (e *DatabaseError) WithDetails(details string) *DatabaseError {
	e.Details = details
	return e
}

// WithCause adds the underlying cause
func (e *DatabaseError) WithCause(cause error) *DatabaseError {
	e.Cause = cause
	return e
}

// DatabaseMetadata provides database metadata information
type DatabaseMetadata interface {
	// Type returns the database type (mysql, oracle, sqlite, etc.)
	Type() string
	// Name returns the database name
	Name() string
	// Version returns the database version (if available)
	Version(ctx context.Context) (string, error)
}

// QueryExecutor handles query execution
type QueryExecutor interface {
	// Query executes a query and returns raw results
	Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error)
	// QueryRow executes a query that is expected to return at most one row
	QueryRow(ctx context.Context, sql string, args ...any) (map[string]any, error)
	// Exec executes a statement that doesn't return rows
	Exec(ctx context.Context, sql string, args ...any) (sql.Result, error)
}

// TypedQueryExecutor provides type-safe query execution
type TypedQueryExecutor interface {
	// Select scans query results into a slice
	Select(ctx context.Context, dest any, query string, args ...any) error
	// Get scans a single row into dest
	Get(ctx context.Context, dest any, query string, args ...any) error
}

// Transaction represents a database transaction
type Transaction interface {
	QueryExecutor
	TypedQueryExecutor
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back the transaction
	Rollback() error
}

// TransactionManager handles transaction lifecycle
type TransactionManager interface {
	// Begin starts a new transaction
	Begin(ctx context.Context) (Transaction, error)
	// BeginTx starts a new transaction with options
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
}

// SchemaInspector provides database schema information
type SchemaInspector interface {
	// GetTableColumns returns column information for a table
	GetTableColumns(ctx context.Context, database, tableName string) ([]*DBColumn, error)
	// IsTableExist checks if a table exists
	IsTableExist(ctx context.Context, tableName string) (bool, error)
	// GetTableNames returns all table names in the database
	GetTableNames(ctx context.Context, database string) ([]string, error)
}

// DatabaseClient represents the main database client interface
// This is a minimal interface that existing DBCli can implement
type DatabaseClient interface {
	// Basic metadata
	DBType() string
	Database() string

	// Core functionality that exists in current DBCli
	Query(sql string, args ...interface{}) ([]map[string]interface{}, error)
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
	Excute(sql string, arguments ...interface{}) (int64, error)

	// Connection management
	Close() error

	// Legacy compatibility
	PW() string
	NQuery(sql string, data any) ([]map[string]any, error)
	NQueryOne(sql string, data any) (map[string]any, error)
}

type DefaultDBFieldType int

const (
	DefaultDBFieldTypeString DefaultDBFieldType = iota
	DefaultDBFieldTypeInt
	DefaultDBFieldTypeFloat
	DefaultDBFieldTypeBool
	DefaultDBFieldTypeTime
	DefaultDBFieldTypeBinary
	DefaultDBFieldTypeJson
	DefaultDBFieldTypeBit
)

// SQLDialect handles database-specific SQL generation
// This matches the existing ISql interface for compatibility
type SQLDialect interface {
	// DDL operations
	GetDDLSqlFunc(funcName DDLSqlFuncName) (DDLSqlFunc, error)
	GetSortedSqlFunc(funcName SortFuncName) (SortedSqlFunc, error)

	// Schema operations - using DatabaseClient instead of specific type
	GetTableColumns(cli DatabaseClient, database, tableName string) ([]*DBColumn, error)
	IsTableExist(cli DatabaseClient, tableName string) (bool, error)

	// SQL preprocessing and utilities
	PreProcess(sql string, param ...int) string
	GetReplaceSql(cli DatabaseClient, table string, rd *RowData) (string, error)
	MakeSureDBExists(cli DatabaseClient, dbName string) error
	ReadSQLFile(r io.Reader, callback FuncSQLStatementCallback, options ...func(*ReadSQLFileOptions)) error
	GetConnectStr(dbConn *DBConnection) (string, string)
	GetDefaultTypeName(tp DefaultDBFieldType) string
}

// CRUDOperations provides high-level CRUD operations
type CRUDOperations interface {
	// Insert inserts a single record
	Insert(ctx context.Context, tableName string, data map[string]any) (int64, error)
	// BatchInsert inserts multiple records
	BatchInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error)
	// Update updates records matching the condition
	Update(ctx context.Context, tableName string, data map[string]any, condition map[string]any) (int64, error)
	// Delete deletes records matching the condition
	Delete(ctx context.Context, tableName string, condition map[string]any) (int64, error)
	// Replace replaces a record (upsert operation)
	Replace(ctx context.Context, tableName string, data map[string]any) (int64, error)
}

// DatabaseManager manages multiple database connections
type DatabaseManager interface {
	// GetClient returns a database client by key
	GetClient(key string) (DatabaseClient, error)
	// AddClient adds a new database client
	AddClient(key string, client DatabaseClient)
	// RemoveClient removes a database client
	RemoveClient(key string)
	// CloseAll closes all database connections
	CloseAll() error
	// ListClients returns all client keys
	ListClients() []string
}

// ResultSet represents query results with better type safety
type ResultSet[T any] struct {
	Data       []T           `json:"data"`
	TotalCount int64         `json:"total_count,omitempty"`
	Page       int           `json:"page,omitempty"`
	PageSize   int           `json:"page_size,omitempty"`
	Metadata   QueryMetadata `json:"metadata,omitempty"`
}

// QueryMetadata contains additional information about the query execution
type QueryMetadata struct {
	ExecutionTime time.Duration `json:"execution_time"`
	RowsAffected  int64         `json:"rows_affected"`
	QueryHash     string        `json:"query_hash,omitempty"`
}

// BuildResult 查询构建结果，包含命名参数支持
type BuildResult struct {
	SQL        string   `json:"sql"`         // 生成的SQL语句
	ParamOrder []string `json:"param_order"` // 参数顺序列表（用于维持顺序）
}

type BuildSql interface {
	GetFieldsString(dt DatabaseTransformer, skipAS bool) (string, error)
	GetPkFieldsString(dt DatabaseTransformer, skipAS bool) (string, error)
	GetValuesString(dt DatabaseTransformer, all bool) (string, []string, error)
	GetAssignString(dt DatabaseTransformer, all bool) (string, []string, error)
}

type DatabaseTransformer interface {
	EscapeTableName(tableName string) string
	EscapeColumnName(columnName string) string
	BuildPagination(limit, offset string) string
	SupportsBatch() bool

	BuildReplaceSQL(tableName string, buildSql BuildSql) (string, []string, error)
	BuildUpsertSQL(tableName string, buildSql BuildSql) (string, []string, error)
}

type Condition struct {
	Condition string
	Fields    []string
}

type Where interface {
	ToCondition(dt DatabaseTransformer) (*Condition, error)
}

type OrderBy struct {
	Column    []Expression
	Direction string // ASC, DESC
}

type SQLOperationType string

const (
	SQLOperationUnknown SQLOperationType = ""
	SQLOperationSelect  SQLOperationType = "SELECT"
	SQLOperationCount   SQLOperationType = "COUNT"
	SQLOperationInsert  SQLOperationType = "INSERT"
	SQLOperationUpdate  SQLOperationType = "UPDATE"
	SQLOperationDelete  SQLOperationType = "DELETE"
	SQLOperationReplace SQLOperationType = "REPLACE"
	SQLOperationUpsert  SQLOperationType = "UPSERT"
)

type TableSource interface {
	GetAlias() string
	ToSQL(dt DatabaseTransformer) (string, error)
}

// SQLBuilder provides a fluent interface for building queries
type SQLBuilder interface {
	Type(t SQLOperationType) SQLBuilder
	Database(db string) SQLBuilder
	Table(table any) SQLBuilder
	Fields(columns any, reset ...bool) SQLBuilder
	// PrimaryKeys 指定主键字段，用于REPLACE和UPSERT操作
	PrimaryKeys(keys ...string) SQLBuilder
	Distinct(distinct bool) SQLBuilder
	Values(data ...[]Expression) SQLBuilder
	ValuesAppend(data ...[]Expression) SQLBuilder
	SubQueryValues(builder SQLBuilder) SQLBuilder
	Update(columns any) SQLBuilder
	UpdateExpr(updates ...Expression) SQLBuilder
	Count(fields string) SQLBuilder
	Select(fields any) SQLBuilder
	From(tableSrc any) SQLBuilder
	Where(fv Where, reset ...bool) SQLBuilder
	WhereAnd(fvs ...Where) SQLBuilder
	WhereOr(fvs ...Where) SQLBuilder
	Join(tableSrc TableSource, condition ...Where) SQLBuilder
	LeftJoin(tableSrc TableSource, condition ...Where) SQLBuilder
	RightJoin(tableSrc TableSource, condition ...Where) SQLBuilder
	InnerJoin(tableSrc TableSource, condition ...Where) SQLBuilder
	GroupBy(columns any) SQLBuilder
	Having(fv Where) SQLBuilder
	HavingAnd(fvs ...Where) SQLBuilder
	HavingOr(fvs ...Where) SQLBuilder
	OrderBy(orderBy ...OrderBy) SQLBuilder
	Limit(limit int) SQLBuilder
	LimitPlaceholder(limit string) SQLBuilder
	Offset(offset int) SQLBuilder
	OffsetPlaceholder(offset string) SQLBuilder

	// 统一的构建方法，通过选项控制不同的构建方式
	Build(dt DatabaseTransformer) (*BuildResult, error)
}

// LikePatternType defines the type of pattern matching for a LIKE query.
type LikePatternType int

const (
	// None does not add any wildcards.
	None LikePatternType = iota
	// Escape
	Escape
	// Contains matches if the pattern is anywhere in the string (e.g. %pattern%).
	Contains
	// StartsWith matches if the string starts with the pattern (e.g. pattern%).
	StartsWith
	// EndsWith matches if the string ends with the pattern (e.g. %pattern).
	EndsWith
)

type SQLStatement struct {
	Index     int
	StartLine int
	EndLine   int
	Type      string
	Content   string
}

type SqlContent struct {
	Name    string
	Content string
}

type DBFieldType int

const (
	DBFieldTypeString DBFieldType = iota
	DBFieldTypeInt
	DBFieldTypeFloat
	DBFieldTypeTime
	DBFieldTypeBinary
	DBFieldTypeJson
	DBFieldTypeBit
)

type DBColumn struct {
	Name        string
	DBFieldType DBFieldType
	ColumnKey   string
	OrgDataType string
	Nullable    bool
}

type FieldData struct {
	Name        string
	Type        DBFieldType
	OrgDataType string
	Data        interface{}
	IsPK        bool
	IsUQ        bool
	Nullable    bool
	Index       int
}

type DDLSqlFuncName string

const (
	FuncNameGetCreateTableSql     DDLSqlFuncName = "GetCreateTableSql"
	FuncNameGetCreateViewSql      DDLSqlFuncName = "GetCreateViewSql"
	FuncNameGetCreateProcedureSql DDLSqlFuncName = "GetCreateProcedureSql"
	FuncNameGetCreateFunctionSql  DDLSqlFuncName = "GetCreateFunctionSql"
	FuncNameGetTableEventSql      DDLSqlFuncName = "GetTableEventSql"
	FuncNameGetBeginSql           DDLSqlFuncName = "GetBeginSql"
	FuncNameGetEndSql             DDLSqlFuncName = "GetEndSql"
)

type CRUDSqlFuncName string

const (
	FuncNameGetInsertSql  CRUDSqlFuncName = "GetInsertSql"
	FuncNameGetSelectSql  CRUDSqlFuncName = "GetSelectSql"
	FuncNameGetUpdateSql  CRUDSqlFuncName = "GetUpdateSql"
	FuncNameGetDeleteSql  CRUDSqlFuncName = "GetDeleteSql"
	FuncNameGetReplaceSql CRUDSqlFuncName = "GetReplaceSql"
)

type SortFuncName string

const (
	FuncNameSortTables     SortFuncName = "SortTables"
	FuncNameSortFunctions  SortFuncName = "SortFunctions"
	FuncNameSortProcedures SortFuncName = "SortProcedures"
)

type DDLSqlFunc func(cli DatabaseClient, name ...string) (*SqlContent, error)
type SortedSqlFunc func(cli DatabaseClient, names []string) ([]*SqlContent, error)
type FuncSQLStatementCallback func(sqlStatement *SQLStatement) error
