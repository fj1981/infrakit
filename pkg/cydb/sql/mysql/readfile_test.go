package sqlmysql

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
SELECT id, 'LOGIN', NOW() FROM failed_logins WHERE attempt_time > DATE_SUB(NOW(), INTERVAL 1 DAY);

-- 📌 Statement 4: CREATE TABLE
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE
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

-- 📌 Statement 11: Multi-line comment mixed with SQL
/*
This is a multi-line comment
that spans several lines.
*/
SELECT 'Hello' AS greeting; -- This is an inline comment

-- 📌 Statement 12: CREATE TRIGGER
DELIMITER //
CREATE TRIGGER update_updated_at 
    AFTER UPDATE ON users
    FOR EACH ROW
BEGIN
    UPDATE users SET updated_at = NOW() WHERE id = NEW.id;
END//
DELIMITER ;

-- 📌 Statement 13: Transaction statements
START TRANSACTION;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- 📌 Statement 14: ALTER TABLE
ALTER TABLE users ADD COLUMN phone VARCHAR(20);

-- 📌 Statement 15: DROP TABLE
DROP TABLE IF EXISTS temp_table;

-- 📌 Statement 16: String with various escape sequences
INSERT INTO logs (message) VALUES ('Error: File not found at C:\\temp\\file.txt. Error code: ''ERR_404''');

-- 📌 Statement 17: CREATE TABLE with foreign key
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2),
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

-- 📌 Statement 20: CREATE PROCEDURE with custom delimiter
DELIMITER $$
CREATE PROCEDURE process_orders(IN customer_id INT)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE order_id INT;
    DECLARE cur CURSOR FOR SELECT id FROM orders WHERE user_id = customer_id;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN cur;
    
    read_loop: LOOP
        FETCH cur INTO order_id;
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        UPDATE orders SET status = 'PROCESSED' WHERE id = order_id;
    END LOOP;
    
    CLOSE cur;
    
    SELECT 'Orders processed' AS message;
END$$
DELIMITER ;

-- 📌 Statement 21: CREATE FUNCTION with custom delimiter
DELIMITER $$
CREATE FUNCTION calculate_tax(income DECIMAL(10,2), tax_rate DECIMAL(5,2)) 
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    DECLARE tax_amount DECIMAL(10,2);
    
    IF income < 1000 THEN
        SET tax_amount = 0;
    ELSE
        SET tax_amount = income * tax_rate;
    END IF;
    
    RETURN tax_amount;
END$$
DELIMITER ;

-- 📌 Statement 22: Statement without semicolon at the end
SELECT * FROM users`

	col := &testCollector{}
	err := new(mysqlSql).ReadSQLFile(strings.NewReader(fullSQL), col.callback)
	require.NoError(t, err)

	// We expect more statements than the raw count due to DELIMITER statements
	// being processed separately
	require.GreaterOrEqual(t, len(col.stmts), 22)

	// Verify key statements
	assert.Equal(t, "Insert", col.stmts[0].Type)
	assert.Contains(t, col.stmts[0].Content, "INSERT INTO users")
	assert.Equal(t, 2, col.stmts[0].StartLine)

	// Find CREATE TABLE statement
	var createTableStmt *SQLStatement
	for _, stmt := range col.stmts {
		if stmt.Type == "CreateTable" && strings.Contains(stmt.Content, "CREATE TABLE users") {
			createTableStmt = stmt
			break
		}
	}
	require.NotNil(t, createTableStmt, "CREATE TABLE statement not found")

	// Find CREATE TRIGGER statement
	var createTriggerStmt *SQLStatement
	for _, stmt := range col.stmts {
		if stmt.Type == "CreateTrigger" && strings.Contains(stmt.Content, "CREATE TRIGGER update_updated_at") {
			createTriggerStmt = stmt
			break
		}
	}
	require.NotNil(t, createTriggerStmt, "CREATE TRIGGER statement not found")

	// Find Transaction statement
	var transactionStmt *SQLStatement
	for _, stmt := range col.stmts {
		if stmt.Type == "Transaction" && strings.Contains(stmt.Content, "START TRANSACTION") {
			transactionStmt = stmt
			break
		}
	}
	require.NotNil(t, transactionStmt, "Transaction statement not found")

	// Find CREATE PROCEDURE statement
	var createProcedureStmt *SQLStatement
	for _, stmt := range col.stmts {
		if stmt.Type == "CreateProcedure" && strings.Contains(stmt.Content, "CREATE PROCEDURE process_orders") {
			createProcedureStmt = stmt
			break
		}
	}
	require.NotNil(t, createProcedureStmt, "CREATE PROCEDURE statement not found")

	// Find CREATE FUNCTION statement
	var createFunctionStmt *SQLStatement
	for _, stmt := range col.stmts {
		if stmt.Type == "CreateFunction" && strings.Contains(stmt.Content, "CREATE FUNCTION calculate_tax") {
			createFunctionStmt = stmt
			break
		}
	}
	require.NotNil(t, createFunctionStmt, "CREATE FUNCTION statement not found")
}
