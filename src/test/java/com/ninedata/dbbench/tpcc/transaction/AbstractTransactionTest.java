package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AbstractTransaction SQL Generation Tests")
class AbstractTransactionTest {

    // Mock adapter for MySQL (supports LIMIT syntax)
    static class MockMySQLAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "MySQL"; }
        @Override public boolean supportsLimitSyntax() { return true; }
        @Override public boolean requiresRowIdForLimitForUpdate() { return false; }
    }

    // Mock adapter for PostgreSQL (supports LIMIT syntax)
    static class MockPostgreSQLAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "PostgreSQL"; }
        @Override public boolean supportsLimitSyntax() { return true; }
        @Override public boolean requiresRowIdForLimitForUpdate() { return false; }
    }

    // Mock adapter for Oracle 11g (requires ROWID for LIMIT FOR UPDATE)
    static class MockOracle11gAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "Oracle"; }
        @Override public boolean supportsLimitSyntax() { return false; }
        @Override public boolean requiresRowIdForLimitForUpdate() { return true; }
    }

    // Mock adapter for DB2 (uses FETCH FIRST syntax)
    static class MockDB2Adapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "DB2"; }
        @Override public boolean supportsLimitSyntax() { return false; }
        @Override public boolean requiresRowIdForLimitForUpdate() { return false; }
    }

    // Mock adapter for SQL Server (uses TOP syntax)
    static class MockSQLServerAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "SQL Server"; }
        @Override public boolean supportsLimitSyntax() { return false; }
        @Override public boolean requiresRowIdForLimitForUpdate() { return false; }
    }

    // Mock adapter for SQLite (no FOR UPDATE support)
    static class MockSQLiteAdapter implements DatabaseAdapter {
        @Override public void initialize() {}
        @Override public Connection getConnection() { return null; }
        @Override public void close() {}
        @Override public void createSchema() {}
        @Override public void dropSchema() {}
        @Override public Map<String, Object> collectMetrics() { return new HashMap<>(); }
        @Override public String getDatabaseType() { return "SQLite"; }
        @Override public boolean supportsLimitSyntax() { return true; }
        @Override public boolean requiresRowIdForLimitForUpdate() { return false; }
        @Override public boolean supportsForUpdate() { return false; }
    }

    // Testable transaction class to expose protected methods
    static class TestableTransaction extends AbstractTransaction {
        public TestableTransaction(DatabaseAdapter adapter) {
            super(adapter, 1, 1);
        }

        @Override
        public String getName() { return "TEST"; }

        @Override
        protected boolean doExecute(Connection conn) { return true; }

        public String testBuildSelectFirstRowQuery(String baseQuery) {
            return buildSelectFirstRowQuery(baseQuery);
        }

        public String testBuildSelectFirstRowForUpdateQuery(String baseQuery) {
            return buildSelectFirstRowForUpdateQuery(baseQuery);
        }

        public String testBuildSelectForUpdateQuery(String baseQuery) {
            return buildSelectForUpdateQuery(baseQuery);
        }
    }

    // ==================== MySQL Tests ====================

    @Test
    @DisplayName("MySQL: Should use LIMIT 1 for SELECT first row")
    void testMySQLSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockMySQLAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";

        String result = tx.testBuildSelectFirstRowQuery(baseQuery);

        assertEquals(baseQuery + " LIMIT 1", result);
    }

    @Test
    @DisplayName("MySQL: Should use LIMIT 1 FOR UPDATE for SELECT first row for update")
    void testMySQLSelectFirstRowForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockMySQLAdapter());
        String baseQuery = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ?";

        String result = tx.testBuildSelectFirstRowForUpdateQuery(baseQuery);

        assertEquals(baseQuery + " LIMIT 1 FOR UPDATE", result);
    }

    @Test
    @DisplayName("MySQL: Should append FOR UPDATE for SELECT for update")
    void testMySQLSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockMySQLAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";

        String result = tx.testBuildSelectForUpdateQuery(baseQuery);

        assertEquals(baseQuery + " FOR UPDATE", result);
    }

    // ==================== PostgreSQL Tests ====================

    @Test
    @DisplayName("PostgreSQL: Should use LIMIT 1 for SELECT first row")
    void testPostgreSQLSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockPostgreSQLAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";

        String result = tx.testBuildSelectFirstRowQuery(baseQuery);

        assertEquals(baseQuery + " LIMIT 1", result);
    }

    @Test
    @DisplayName("PostgreSQL: Should use LIMIT 1 FOR UPDATE for SELECT first row for update")
    void testPostgreSQLSelectFirstRowForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockPostgreSQLAdapter());
        String baseQuery = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ?";

        String result = tx.testBuildSelectFirstRowForUpdateQuery(baseQuery);

        assertEquals(baseQuery + " LIMIT 1 FOR UPDATE", result);
    }

    // ==================== Oracle 11g Tests ====================

    @Test
    @DisplayName("Oracle 11g: Should use ROWNUM subquery for SELECT first row")
    void testOracle11gSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockOracle11gAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? ORDER BY c_first";

        String result = tx.testBuildSelectFirstRowQuery(baseQuery);

        assertTrue(result.contains("ROWNUM = 1"));
        assertFalse(result.contains("LIMIT"));
        assertFalse(result.contains("FETCH FIRST"));
    }

    @Test
    @DisplayName("Oracle 11g: Should use ROWID-based subquery for SELECT first row for update")
    void testOracle11gSelectFirstRowForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockOracle11gAdapter());
        String baseQuery = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id";

        String result = tx.testBuildSelectFirstRowForUpdateQuery(baseQuery);

        assertTrue(result.contains("ROWID"), "Should use ROWID for Oracle 11g");
        assertTrue(result.contains("ROWNUM = 1"), "Should use ROWNUM = 1");
        assertTrue(result.contains("FOR UPDATE"), "Should end with FOR UPDATE");
        assertFalse(result.contains("FETCH FIRST"), "Should NOT use FETCH FIRST");
    }

    @Test
    @DisplayName("Oracle 11g: Should append FOR UPDATE for SELECT for update")
    void testOracle11gSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockOracle11gAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";

        String result = tx.testBuildSelectForUpdateQuery(baseQuery);

        assertEquals(baseQuery + " FOR UPDATE", result);
    }

    // ==================== DB2 Tests ====================

    @Test
    @DisplayName("DB2: Should use FETCH FIRST 1 ROWS ONLY for SELECT first row")
    void testDB2SelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockDB2Adapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";

        String result = tx.testBuildSelectFirstRowQuery(baseQuery);

        assertEquals(baseQuery + " FETCH FIRST 1 ROWS ONLY", result);
    }

    @Test
    @DisplayName("DB2: Should use FETCH FIRST 1 ROWS ONLY FOR UPDATE for SELECT first row for update")
    void testDB2SelectFirstRowForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockDB2Adapter());
        String baseQuery = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ?";

        String result = tx.testBuildSelectFirstRowForUpdateQuery(baseQuery);

        assertEquals(baseQuery + " FETCH FIRST 1 ROWS ONLY FOR UPDATE", result);
    }

    @Test
    @DisplayName("DB2: Should append FOR UPDATE for SELECT for update")
    void testDB2SelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockDB2Adapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";

        String result = tx.testBuildSelectForUpdateQuery(baseQuery);

        assertEquals(baseQuery + " FOR UPDATE", result);
    }

    // ==================== SQL Server Tests ====================

    @Test
    @DisplayName("SQL Server: Should use TOP 1 for SELECT first row")
    void testSQLServerSelectFirstRow() {
        TestableTransaction tx = new TestableTransaction(new MockSQLServerAdapter());
        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ?";

        String result = tx.testBuildSelectFirstRowQuery(baseQuery);

        assertTrue(result.contains("SELECT TOP 1"));
        assertFalse(result.contains("LIMIT"));
    }

    @Test
    @DisplayName("SQL Server: Should use TOP 1 with lock hints for SELECT first row for update")
    void testSQLServerSelectFirstRowForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockSQLServerAdapter());
        String baseQuery = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ?";

        String result = tx.testBuildSelectFirstRowForUpdateQuery(baseQuery);

        assertTrue(result.contains("SELECT TOP 1"));
        assertTrue(result.contains("WITH (UPDLOCK, ROWLOCK)"));
        assertFalse(result.contains("FOR UPDATE"));
    }

    @Test
    @DisplayName("SQL Server: Should use lock hints for SELECT for update")
    void testSQLServerSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockSQLServerAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";

        String result = tx.testBuildSelectForUpdateQuery(baseQuery);

        assertTrue(result.contains("WITH (UPDLOCK, ROWLOCK)"));
        assertFalse(result.contains("FOR UPDATE"));
    }

    // ==================== SQLite Tests ====================

    @Test
    @DisplayName("SQLite: Should use LIMIT 1 without FOR UPDATE for SELECT first row for update")
    void testSQLiteSelectFirstRowForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockSQLiteAdapter());
        String baseQuery = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id";

        String result = tx.testBuildSelectFirstRowForUpdateQuery(baseQuery);

        assertEquals(baseQuery + " LIMIT 1", result);
        assertFalse(result.contains("FOR UPDATE"));
    }

    @Test
    @DisplayName("SQLite: Should not append FOR UPDATE for SELECT for update")
    void testSQLiteSelectForUpdate() {
        TestableTransaction tx = new TestableTransaction(new MockSQLiteAdapter());
        String baseQuery = "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?";

        String result = tx.testBuildSelectForUpdateQuery(baseQuery);

        assertEquals(baseQuery, result);
        assertFalse(result.contains("FOR UPDATE"));
    }

    // ==================== Transaction Properties Tests ====================

    @Test
    @DisplayName("Should initialize with correct warehouse and district IDs")
    void testTransactionInitialization() {
        DatabaseAdapter adapter = new MockMySQLAdapter();
        AbstractTransaction tx = new TestableTransaction(adapter) {
            @Override
            public String getName() { return "TEST"; }

            @Override
            protected boolean doExecute(Connection conn) { return true; }
        };

        assertEquals(1, tx.getWarehouseId());
        assertEquals(1, tx.getDistrictId());
    }

    @Test
    @DisplayName("Should detect LIMIT syntax support correctly")
    void testLimitSyntaxSupport() {
        TestableTransaction mysqlTx = new TestableTransaction(new MockMySQLAdapter());
        TestableTransaction oracleTx = new TestableTransaction(new MockOracle11gAdapter());

        assertTrue(mysqlTx.isUseLimitSyntax());
        assertFalse(oracleTx.isUseLimitSyntax());
    }

    @Test
    @DisplayName("Should detect ROWID requirement correctly")
    void testRowIdRequirement() {
        TestableTransaction mysqlTx = new TestableTransaction(new MockMySQLAdapter());
        TestableTransaction oracleTx = new TestableTransaction(new MockOracle11gAdapter());

        assertFalse(mysqlTx.isUseRowIdForLimitForUpdate());
        assertTrue(oracleTx.isUseRowIdForLimitForUpdate());
    }
}
