package cydb_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/fj1981/infrakit/pkg/cydb"
	_ "github.com/fj1981/infrakit
)

// Helper function to test SQL parsing
func testSQLParsing(t *testing.T, sql string, expectedOperation string, expectedTable string) {
	// Parse the SQL statement
	builder, err := cydb.ParseMySQL(sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}
	if builder == nil {
		t.Fatal("Builder is nil")
	}

	// Get the SQL transformer
	dt, _ := cydb.GetSqlTransformer("mysql")

	// Build the SQL
	result, err := builder.Build(dt)
	if err != nil {
		t.Fatalf("Failed to build SQL: %v", err)
	}
	if result.SQL == "" {
		t.Fatal("Generated SQL is empty")
	}
	fmt.Printf(" Expected SQL: %s\n", sql)
	fmt.Printf("Generated SQL: %s\n", result.SQL)
	// Check that the SQL contains the expected operation type
	if !strings.Contains(strings.ToUpper(result.SQL), expectedOperation) {
		t.Errorf("Expected SQL to contain operation '%s', but got: %s", expectedOperation, result.SQL)
	}

	// Check that the SQL contains the expected table name
	if !strings.Contains(result.SQL, expectedTable) {
		t.Errorf("Expected SQL to contain table '%s', but got: %s", expectedTable, result.SQL)
	}
}

func TestParseInsertSQL(t *testing.T) {
	// Simple INSERT tests
	t.Run("Simple INSERT", func(t *testing.T) {
		sql := "INSERT INTO users (name, age, email) VALUES ('John', 25, 'john@example.com')"
		testSQLParsing(t, sql, "INSERT", "users")
	})

	t.Run("INSERT with multiple values", func(t *testing.T) {
		sql := "INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30)"
		testSQLParsing(t, sql, "INSERT", "users")
	})

	// Complex INSERT tests
	t.Run("INSERT with subquery", func(t *testing.T) {
		sql := "INSERT INTO user_backup (id, name, age) SELECT id, name, age FROM users WHERE created_at < '2023-01-01'"
		testSQLParsing(t, sql, "INSERT", "user_backup")
	})

	t.Run("INSERT with expressions", func(t *testing.T) {
		sql := "INSERT INTO stats (date, total_sales, avg_price) VALUES (CURRENT_DATE(), SUM(price), AVG(price))"
		testSQLParsing(t, sql, "INSERT", "stats")
	})

	t.Run("INSERT with ON DUPLICATE KEY UPDATE", func(t *testing.T) {
		sql := "INSERT INTO products (id, name, stock) VALUES (1, 'Product A', 100) ON DUPLICATE KEY UPDATE stock = stock + 100"
		testSQLParsing(t, sql, "INSERT", "products")
	})

	t.Run("INSERT with parameter placeholders", func(t *testing.T) {
		sql := "INSERT INTO users (name, age, email) VALUES (:name, :age, :email)"
		testSQLParsing(t, sql, "INSERT", "users")
	})
}

func TestParseUpdateSQL(t *testing.T) {

	// Simple UPDATE tests
	t.Run("Simple UPDATE", func(t *testing.T) {
		sql := "UPDATE users SET name = 'John', age = 25 WHERE id = 1"
		testSQLParsing(t, sql, "UPDATE", "users")
	})

	t.Run("UPDATE without WHERE", func(t *testing.T) {
		sql := "UPDATE products SET price = price * 1.1"
		testSQLParsing(t, sql, "UPDATE", "products")
	})

	// Complex UPDATE tests
	t.Run("UPDATE with JOIN", func(t *testing.T) {
		sql := "UPDATE products p JOIN inventory i ON p.id = i.product_id SET p.stock = i.quantity WHERE i.updated_at > '2023-01-01'"
		testSQLParsing(t, sql, "UPDATE", "products")
	})

	t.Run("UPDATE with subquery", func(t *testing.T) {
		sql := "UPDATE employees SET salary = salary * 1.1 WHERE department_id IN (SELECT id FROM departments WHERE performance_rating > 8)"
		testSQLParsing(t, sql, "UPDATE", "employees")
	})

	t.Run("UPDATE with CASE expression", func(t *testing.T) {
		sql := "UPDATE products SET price = CASE WHEN category = 'electronics' THEN price * 0.9 WHEN category = 'clothing' THEN price * 0.8 ELSE price * 0.95 END"
		testSQLParsing(t, sql, "UPDATE", "products")
	})

	t.Run("UPDATE with parameter placeholders", func(t *testing.T) {
		sql := "UPDATE users SET name = :name, age = :age WHERE id = :id"
		testSQLParsing(t, sql, "UPDATE", "users")
	})
}

func TestParseSelectSQL(t *testing.T) {

	// Simple SELECT tests
	t.Run("Simple SELECT all columns", func(t *testing.T) {
		sql := "SELECT * FROM users"
		testSQLParsing(t, sql, "SELECT", "users")
	})

	t.Run("SELECT specific columns", func(t *testing.T) {
		sql := "SELECT id, name, email FROM users"
		testSQLParsing(t, sql, "SELECT", "users")
	})

	t.Run("SELECT with WHERE clause", func(t *testing.T) {
		sql := "SELECT * FROM products WHERE price > 100"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with table alias", func(t *testing.T) {
		sql := "SELECT u.id, u.name FROM users u WHERE u.active = 1"
		testSQLParsing(t, sql, "SELECT", "users")
	})

	// Complex SELECT tests with JOINs
	t.Run("SELECT with INNER JOIN", func(t *testing.T) {
		sql := "SELECT u.name, o.order_date FROM users u INNER JOIN orders o ON u.id = o.user_id"
		testSQLParsing(t, sql, "SELECT", "users")
	})

	t.Run("SELECT with LEFT JOIN", func(t *testing.T) {
		sql := "SELECT p.name, c.name AS category_name FROM products p LEFT JOIN categories c ON p.category_id = c.id"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with multiple JOINs", func(t *testing.T) {
		sql := "SELECT o.id, c.name, p.name FROM orders o JOIN customers c ON o.customer_id = c.id JOIN products p ON o.product_id = p.id"
		testSQLParsing(t, sql, "SELECT", "orders")
	})

	// SELECT with subqueries
	t.Run("SELECT with subquery in WHERE", func(t *testing.T) {
		sql := "SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE active = 1)"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with subquery in FROM", func(t *testing.T) {
		sql := "SELECT t.name, t.count FROM (SELECT category_id, COUNT(*) as count FROM products GROUP BY category_id) AS t JOIN categories c ON t.category_id = c.id"
		testSQLParsing(t, sql, "SELECT", "categories")
	})

	t.Run("SELECT with EXISTS subquery", func(t *testing.T) {
		sql := "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
		testSQLParsing(t, sql, "SELECT", "users")
	})

	// SELECT with GROUP BY, HAVING, ORDER BY
	t.Run("SELECT with GROUP BY", func(t *testing.T) {
		sql := "SELECT category_id, COUNT(*) FROM products GROUP BY category_id"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with GROUP BY and HAVING", func(t *testing.T) {
		sql := "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department HAVING AVG(salary) > 50000"
		testSQLParsing(t, sql, "SELECT", "employees")
	})

	t.Run("SELECT with ORDER BY", func(t *testing.T) {
		sql := "SELECT * FROM products ORDER BY price DESC, name ASC"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with LIMIT and OFFSET", func(t *testing.T) {
		sql := "SELECT * FROM products ORDER BY created_at DESC LIMIT 10 OFFSET 20"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	// SELECT with aggregate functions
	t.Run("SELECT with aggregate functions", func(t *testing.T) {
		sql := "SELECT COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM products"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with DISTINCT", func(t *testing.T) {
		sql := "SELECT DISTINCT category_id FROM products"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with CASE expression", func(t *testing.T) {
		sql := "SELECT id, name, CASE WHEN price > 100 THEN 'expensive' WHEN price > 50 THEN 'moderate' ELSE 'cheap' END AS price_category FROM products"
		testSQLParsing(t, sql, "SELECT", "products")
	})

	t.Run("SELECT with parameter placeholders", func(t *testing.T) {
		sql := "SELECT * FROM users WHERE status = :status AND created_at > :start_date"
		testSQLParsing(t, sql, "SELECT", "users")
	})
}

func TestParseDeleteSQL(t *testing.T) {

	// Simple DELETE tests
	t.Run("Simple DELETE", func(t *testing.T) {
		sql := "DELETE FROM users WHERE id = 1"
		testSQLParsing(t, sql, "DELETE", "users")
	})

	t.Run("DELETE without WHERE", func(t *testing.T) {
		sql := "DELETE FROM temp_logs"
		testSQLParsing(t, sql, "DELETE", "temp_logs")
	})

	// Complex DELETE tests
	t.Run("DELETE with JOIN", func(t *testing.T) {
		sql := "DELETE o FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.status = 'inactive'"
		testSQLParsing(t, sql, "DELETE", "orders")
	})

	t.Run("DELETE with subquery", func(t *testing.T) {
		sql := "DELETE FROM products WHERE id IN (SELECT product_id FROM inventory WHERE stock = 0 AND last_sold < DATE_SUB(NOW(), INTERVAL 1 YEAR))"
		testSQLParsing(t, sql, "DELETE", "products")
	})

	t.Run("DELETE with LIMIT", func(t *testing.T) {
		sql := "DELETE FROM logs WHERE created_at < '2023-01-01' ORDER BY created_at LIMIT 1000"
		testSQLParsing(t, sql, "DELETE", "logs")
	})

	t.Run("DELETE with parameter placeholders", func(t *testing.T) {
		sql := "DELETE FROM users WHERE id = :id OR email = :email"
		testSQLParsing(t, sql, "DELETE", "users")
	})
}
