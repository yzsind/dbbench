package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OracleSqlGenerationTest {

    static class MockOracleAdapter implements DatabaseAdapter {
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

    static class TestableTransaction extends AbstractTransaction {
        public TestableTransaction(DatabaseAdapter adapter) {
            super(adapter, 1, 1);
        }

        @Override
        public String getName() { return "TEST"; }

        @Override
        protected boolean doExecute(Connection conn) { return true; }

        public String testBuildSelectFirstRowForUpdateQuery(String baseQuery) {
            return buildSelectFirstRowForUpdateQuery(baseQuery);
        }

        public String testBuildSelectFirstRowQuery(String baseQuery) {
            return buildSelectFirstRowQuery(baseQuery);
        }
    }

    @Test
    void testOracleSelectFirstRowForUpdateQuery() {
        MockOracleAdapter adapter = new MockOracleAdapter();
        TestableTransaction tx = new TestableTransaction(adapter);

        String baseQuery = "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id";
        String result = tx.testBuildSelectFirstRowForUpdateQuery(baseQuery);

        System.out.println("Base Query: " + baseQuery);
        System.out.println("Result: " + result);

        // Should use ROWID-based subquery for Oracle 11g compatibility
        assertTrue(result.contains("ROWID"), "Should use ROWID for Oracle");
        assertTrue(result.contains("ROWNUM = 1"), "Should use ROWNUM = 1");
        assertTrue(result.contains("FOR UPDATE"), "Should end with FOR UPDATE");
        assertFalse(result.contains("FETCH FIRST"), "Should NOT use FETCH FIRST (Oracle 11g incompatible)");

        // Verify the structure: SELECT ... FROM table WHERE ROWID = (SELECT ROWID FROM (...) WHERE ROWNUM = 1) FOR UPDATE
        String expected = "SELECT no_o_id FROM new_order WHERE ROWID = (SELECT ROWID FROM (SELECT ROWID FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id) WHERE ROWNUM = 1) FOR UPDATE";
        assertEquals(expected, result);
    }

    @Test
    void testOracleSelectFirstRowQuery() {
        MockOracleAdapter adapter = new MockOracleAdapter();
        TestableTransaction tx = new TestableTransaction(adapter);

        String baseQuery = "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first";
        String result = tx.testBuildSelectFirstRowQuery(baseQuery);

        System.out.println("Base Query: " + baseQuery);
        System.out.println("Result: " + result);

        // Should use ROWNUM subquery for Oracle
        assertTrue(result.contains("ROWNUM = 1"), "Should use ROWNUM = 1");
        assertFalse(result.contains("FETCH FIRST"), "Should NOT use FETCH FIRST");

        String expected = "SELECT * FROM (" + baseQuery + ") WHERE ROWNUM = 1";
        assertEquals(expected, result);
    }
}
