package sqloracle

import (
	"fmt"
	"strings"
	"testing"

	cydb "github.com/fj1981/infrakit/pkg/cydb"
	"github.com/gookit/goutil/testutil/assert"
	"github.com/stretchr/testify/require"
)

type testCollector struct{ stmts []*cydb.SQLStatement }

func (c *testCollector) callback(s *cydb.SQLStatement) error {
	fmt.Printf("\n################## Statement[%d] %s %d->%d: \n%s\n###################\n", s.Index, s.Type, s.StartLine, s.EndLine, s.Content)
	c.stmts = append(c.stmts, s)
	return nil
}

func TestReadSQLFile(t *testing.T) {
	fullSQL := `-- 📌 Statement 1: Simple INSERT
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- 📌 Statement 2: Multi-line INSERT with escaped quotes
INSERT INTO messages (id, content) VALUES (
	  101,
	  '这是一条很长的消息，
	它跨越了多行，
	并且包含两个单引号：''escaped''，
	以及结束。'
	);

-- 📌 Statement 3: INSERT with SELECT
INSERT INTO audit_log (user_id, action, created_at)
	SELECT id, 'LOGIN', SYSDATE FROM failed_logins WHERE attempt_time > SYSDATE - 1;

-- 📌 Statement 4: INSERT ALL (Multi-table insert)
INSERT ALL
	  INTO stats (type, value) VALUES ('clicks', 100)
	  INTO stats (type, value) VALUES ('views', 500)
	SELECT 1 FROM dual;

-- 📌 Statement 5: CREATE OR REPLACE VIEW
CREATE OR REPLACE VIEW user_summary AS
	SELECT u.id, u.name, 'Active User' AS status_desc
	FROM users u
	WHERE u.status = 'A';

-- 📌 Statement 6: Anonymous PL/SQL Block with loop and conditional
BEGIN
	  FOR rec IN (SELECT id, name FROM users WHERE id < 3) LOOP
		IF rec.name = 'Alice' THEN
		  DBMS_OUTPUT.PUT_LINE(
			'Hello Alice! 
			 This is a message
			 with ''single quotes''.'
		  );
		ELSE
		  INSERT INTO temp_log(msg) VALUES ('User: ' || rec.name);
		END IF;
	  END LOOP;
	  COMMIT;
	END;
	;

-- 📌 Statement 7: Another Anonymous Block
BEGIN
	  DBMS_OUTPUT.PUT_LINE('Done.');
	END;
	;

-- 📌 Statement 8: CREATE PROCEDURE (Complex PL/SQL with nested blocks)
CREATE OR REPLACE PROCEDURE process_orders (
    p_customer_id IN NUMBER,
    p_discount_rate IN NUMBER DEFAULT 0.1
) IS
    CURSOR order_cur IS
        SELECT order_id, total_amount
        FROM orders
        WHERE customer_id = p_customer_id AND status = 'PENDING';

    v_total_discount NUMBER := 0;
    v_count NUMBER := 0;

    -- Nested procedure
    PROCEDURE apply_discount(p_order_id IN NUMBER, p_amount IN OUT NUMBER) IS
    BEGIN
        p_amount := p_amount * (1 - p_discount_rate);
        v_total_discount := v_total_discount + (p_amount * p_discount_rate);
    END apply_discount;

BEGIN
    FOR order_rec IN order_cur LOOP
        v_count := v_count + 1;
        apply_discount(order_rec.order_id, order_rec.total_amount);
        
        UPDATE orders 
        SET status = 'PROCESSED', final_amount = order_rec.total_amount
        WHERE order_id = order_rec.order_id;
        
        -- Nested IF block
        IF v_count > 10 THEN
            DBMS_OUTPUT.PUT_LINE('Processed 10 orders, applying bonus...');
            v_total_discount := v_total_discount + 100;
        END IF;
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Processed ' || v_count || ' orders. Total discount: ' || v_total_discount);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END process_orders;
/

-- 📌 Statement 9: CREATE FUNCTION with complex logic
CREATE OR REPLACE FUNCTION calculate_tax (
    p_income IN NUMBER,
    p_state IN VARCHAR2
) RETURN NUMBER IS
    v_tax_rate NUMBER;
    v_deduction NUMBER := 0;
BEGIN
    CASE p_state
        WHEN 'CA' THEN
            v_tax_rate := 0.08;
            v_deduction := 500;
        WHEN 'NY' THEN
            v_tax_rate := 0.06;
            v_deduction := 300;
        ELSE
            v_tax_rate := 0.05;
    END CASE;

    RETURN GREATEST((p_income - v_deduction) * v_tax_rate, 0);
END calculate_tax;
/

-- 📌 Statement 10: CREATE TRIGGER
CREATE OR REPLACE TRIGGER log_user_changes
    BEFORE UPDATE ON users
    FOR EACH ROW
DECLARE
    v_action VARCHAR2(10);
BEGIN
    IF :OLD.status != :NEW.status THEN
        IF :NEW.status = 'A' THEN
            v_action := 'ACTIVATE';
        ELSIF :NEW.status = 'I' THEN
            v_action := 'DEACTIVATE';
        ELSE
            v_action := 'UPDATE';
        END IF;

        INSERT INTO user_audit (user_id, action, old_status, new_status, change_date)
        VALUES (:NEW.id, v_action, :OLD.status, :NEW.status, SYSDATE);
    END IF;
END;
/

-- 📌 Statement 11: Anonymous Block with nested IF and CASE
DECLARE
    v_score NUMBER := 85;
    v_grade VARCHAR2(2);
    v_feedback VARCHAR2(100);
BEGIN
    IF v_score >= 90 THEN
        v_grade := 'A';
        v_feedback := 'Excellent!';
    ELSIF v_score >= 80 THEN
        v_grade := 'B';
        v_feedback := 'Good job!';
        -- Nested CASE
        CASE 
            WHEN v_score >= 85 THEN
                v_feedback := v_feedback || ' (Top of B range)';
            ELSE
                v_feedback := v_feedback || ' (Solid B)';
        END CASE;
    ELSIF v_score >= 70 THEN
        v_grade := 'C';
        v_feedback := 'Needs improvement.';
    ELSE
        v_grade := 'F';
        v_feedback := 'Failed.';
    END IF;

    DBMS_OUTPUT.PUT_LINE('Grade: ' || v_grade || ', Feedback: ' || v_feedback);
END;
/

-- 📌 Statement 12: Multi-line comment and single line comment
/*
This is a multi-line comment
that spans several lines.
It should be ignored by the parser.
*/
-- This is a single line comment
SELECT 'Hello World' FROM dual; -- Another comment at the end

-- 📌 Statement 13: String with various escape sequences
INSERT INTO logs (message) VALUES ('Error: File not found at C:\temp\file.txt. Error code: ''ERR_404''');

-- 📌 Statement 14: Anonymous Block with exception handling
BEGIN
    UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;
    UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;
    
    -- Simulate an error
    -- RAISE_APPLICATION_ERROR(-20001, 'Insufficient funds');
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Transaction failed: ' || SQLERRM);
END;
/

-- 📌 Statement 15: CREATE PACKAGE SPECIFICATION
CREATE OR REPLACE PACKAGE employee_pkg AS
    -- Public constants
    MIN_SALARY CONSTANT NUMBER := 30000;
    MAX_SALARY CONSTANT NUMBER := 200000;

    -- Public procedures and functions
    PROCEDURE hire_employee (
        p_name IN VARCHAR2,
        p_job IN VARCHAR2,
        p_salary IN NUMBER
    );

    FUNCTION get_employee_count RETURN NUMBER;

    -- Record type
    TYPE emp_record IS RECORD (
        emp_id   NUMBER,
        emp_name VARCHAR2(100),
        salary   NUMBER
    );

END employee_pkg;
/

-- 📌 Statement 16: CREATE PACKAGE BODY
CREATE OR REPLACE PACKAGE BODY employee_pkg AS
    -- Private variable
    g_employee_count NUMBER := 0;

    -- Private procedure
    PROCEDURE validate_salary(p_salary IN NUMBER) IS
    BEGIN
        IF p_salary < MIN_SALARY OR p_salary > MAX_SALARY THEN
            RAISE_APPLICATION_ERROR(-20002, 'Salary out of range');
        END IF;
    END validate_salary;

    -- Implementation of public procedure
    PROCEDURE hire_employee (
        p_name IN VARCHAR2,
        p_job IN VARCHAR2,
        p_salary IN NUMBER
    ) IS
    BEGIN
        validate_salary(p_salary);
        INSERT INTO employees (name, job, salary) VALUES (p_name, p_job, p_salary);
        g_employee_count := g_employee_count + 1;
    END hire_employee;

    -- Implementation of public function
    FUNCTION get_employee_count RETURN NUMBER IS
    BEGIN
        RETURN g_employee_count;
    END get_employee_count;

END employee_pkg;
/

-- 📌 Statement 17: Anonymous Block calling package
DECLARE
    v_count NUMBER;
BEGIN
    employee_pkg.hire_employee('John Doe', 'Developer', 75000);
    v_count := employee_pkg.get_employee_count;
    DBMS_OUTPUT.PUT_LINE('Total employees: ' || v_count);
END;
/

-- 📌 Statement 18: INSERT with subquery and complex WHERE
INSERT INTO high_value_customers (customer_id, total_spent)
SELECT c.id, SUM(o.amount)
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.order_date >= ADD_MONTHS(SYSDATE, -12)
  AND c.status = 'ACTIVE'
GROUP BY c.id
HAVING SUM(o.amount) > 10000;

-- 📌 Statement 19: MERGE statement (UPSERT)
MERGE INTO inventory tgt
USING (SELECT product_id, SUM(quantity) as total_qty FROM sales GROUP BY product_id) src
ON (tgt.product_id = src.product_id)
WHEN MATCHED THEN
    UPDATE SET tgt.stock = tgt.stock - src.total_qty
    WHERE tgt.stock >= src.total_qty
WHEN NOT MATCHED THEN
    INSERT (product_id, stock) VALUES (src.product_id, src.total_qty * 2);

-- 📌 Statement 20: Complex SELECT with CTE, Window Function, and CASE
WITH sales_summary AS (
    SELECT 
        s.region,
        s.salesperson,
        SUM(s.amount) as total_sales,
        RANK() OVER (PARTITION BY s.region ORDER BY SUM(s.amount) DESC) as sales_rank
    FROM sales s
    WHERE s.sale_date >= DATE '2025-01-01'
    GROUP BY s.region, s.salesperson
)
SELECT 
    region,
    salesperson,
    total_sales,
    CASE 
        WHEN sales_rank = 1 THEN 'Top Performer'
        WHEN sales_rank <= 3 THEN 'High Performer'
        ELSE 'Average Performer'
    END as performance_level
FROM sales_summary
ORDER BY region, sales_rank;`

	col := &testCollector{}
	err := new(oracleSql).ReadSQLFile(strings.NewReader(fullSQL), col.callback)
	require.NoError(t, err)
	require.Len(t, col.stmts, 20)

	// 仅做关键断言，保证行号递增即可
	assert.Equal(t, "Insert", col.stmts[0].Type)
	assert.Contains(t, col.stmts[0].Content, "INSERT INTO users")
	assert.Equal(t, 2, col.stmts[0].StartLine)

	assert.Equal(t, "Other", col.stmts[5].Type)
	assert.Contains(t, col.stmts[5].Content, "BEGIN\n\t  FOR rec IN")

	assert.Equal(t, "CreateProcedure", col.stmts[7].Type)
	assert.Contains(t, col.stmts[7].Content, "CREATE OR REPLACE PROCEDURE process_orders")

	assert.Equal(t, "Other", col.stmts[19].Type)
	assert.Contains(t, col.stmts[19].Content, "WITH sales_summary AS")
}
