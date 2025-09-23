package sqlsqlite

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCollector struct {
	stmts []*SQLStatement
}

func (c *testCollector) callback(s *SQLStatement) error {
	fmt.Printf("\n################## Statement[%d] %s %d->%d: \n%s\n###################\n", s.Index, s.Type, s.StartLine, s.EndLine, s.Content)
	c.stmts = append(c.stmts, s)
	return nil
}

func TestReadSQLFile(t *testing.T) {
	fullSQL := `-- 📌 Statement 1: Simple INSERT
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- 📌 Statement 2: Multi-line INSERT
INSERT INTO messages (id, content) VALUES (
    101,
    'This is a long message
    that spans multiple lines.'
);

-- 📌 Statement 3: INSERT with SELECT
INSERT INTO audit_log (user_id, action, created_at)
SELECT id, 'LOGIN', datetime('now') FROM failed_logins WHERE attempt_time > datetime('now', '-1 day');

-- 📌 Statement 4: CREATE TABLE
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

-- 📌 Statement 5: CREATE INDEX
CREATE INDEX idx_users_email ON users(email);

-- 📌 Statement 6: CREATE VIEW
CREATE VIEW user_summary AS
SELECT id, name, 'Active User' AS status_desc
FROM users
WHERE status = 'A';

-- 📌 Statement 7: Simple SELECT
SELECT * FROM users WHERE id = 1;

-- 📌 Statement 8: Complex SELECT with JOIN
SELECT u.name, m.content
FROM users u
JOIN messages m ON u.id = m.user_id
WHERE u.status = 'A';

-- 📌 Statement 9: UPDATE statement
UPDATE users SET email = 'newemail@example.com' WHERE id = 1;

-- 📌 Statement 10: DELETE statement
DELETE FROM users WHERE status = 'I';

-- 📌 Statement 11: Multi-line comment and single line comment
/*
This is a multi-line comment
that spans several lines.
It should be ignored by the parser.
*/
-- This is a single line comment
SELECT 'Hello World';

-- 📌 Statement 12: CREATE TRIGGER
CREATE TRIGGER update_updated_at 
    AFTER UPDATE ON users
BEGIN
    UPDATE users SET updated_at = datetime('now') WHERE id = NEW.id;
END;

-- 📌 Statement 13: Transaction statements
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- 📌 Statement 14: ALTER TABLE
ALTER TABLE users ADD COLUMN phone TEXT;

-- 📌 Statement 15: DROP TABLE
DROP TABLE IF EXISTS temp_table;

-- 📌 Statement 16: String with various escape sequences
INSERT INTO logs (message) VALUES ('Error: File not found.');

-- 📌 Statement 17: CREATE TABLE with foreign key
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    amount REAL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- 📌 Statement 18: WITH clause (CTE)
WITH sales_summary AS (
    SELECT user_id, SUM(amount) as total_sales
    FROM orders
    GROUP BY user_id
)
SELECT u.name, s.total_sales
FROM users u
JOIN sales_summary s ON u.id = s.user_id
ORDER BY s.total_sales DESC;

-- 📌 Statement 19: Multi-statement line
INSERT INTO temp_table VALUES (1); INSERT INTO temp_table VALUES (2);

-- 📌 Statement 20: Statement without semicolon at the end
SELECT * FROM users`

	col := &testCollector{}
	err := new(sqliteSql).ReadSQLFile(strings.NewReader(fullSQL), col.callback)
	require.NoError(t, err)
	require.Len(t, col.stmts, 21)

	// Verify key statements
	assert.Equal(t, "Insert", col.stmts[0].Type)
	assert.Contains(t, col.stmts[0].Content, "INSERT INTO users")
	assert.Equal(t, 2, col.stmts[0].StartLine)

	assert.Equal(t, "CreateTable", col.stmts[3].Type)
	assert.Contains(t, col.stmts[3].Content, "CREATE TABLE users")

	assert.Equal(t, "CreateTrigger", col.stmts[11].Type)
	assert.Contains(t, col.stmts[11].Content, "CREATE TRIGGER update_updated_at")

	assert.Equal(t, "Transaction", col.stmts[12].Type)
	assert.Contains(t, col.stmts[12].Content, "BEGIN")

	assert.Equal(t, "Insert", col.stmts[19].Type)
	assert.Contains(t, col.stmts[19].Content, "INSERT INTO temp_table")
}
