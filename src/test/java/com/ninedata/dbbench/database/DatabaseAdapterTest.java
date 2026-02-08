package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DatabaseAdapter Interface Tests")
class DatabaseAdapterTest {

    @Test
    @DisplayName("MySQL adapter should support LIMIT syntax")
    void testMySQLSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("mysql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertTrue(adapter.supportsLimitSyntax());
        assertFalse(adapter.requiresRowIdForLimitForUpdate());
    }

    @Test
    @DisplayName("PostgreSQL adapter should support LIMIT syntax")
    void testPostgreSQLSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("postgresql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertTrue(adapter.supportsLimitSyntax());
        assertFalse(adapter.requiresRowIdForLimitForUpdate());
    }

    @Test
    @DisplayName("Oracle adapter should not support LIMIT syntax")
    void testOracleNoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("oracle");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertFalse(adapter.supportsLimitSyntax());
        // Oracle 11g compatibility requires ROWID
        assertTrue(adapter.requiresRowIdForLimitForUpdate());
    }

    @Test
    @DisplayName("DB2 adapter should not support LIMIT syntax")
    void testDB2NoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("db2");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertFalse(adapter.supportsLimitSyntax());
        assertFalse(adapter.requiresRowIdForLimitForUpdate());
    }

    @Test
    @DisplayName("SQL Server adapter should not support LIMIT syntax")
    void testSQLServerNoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("sqlserver");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertFalse(adapter.supportsLimitSyntax());
        assertFalse(adapter.requiresRowIdForLimitForUpdate());
    }

    @Test
    @DisplayName("TiDB adapter should support LIMIT syntax (MySQL compatible)")
    void testTiDBSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("tidb");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertTrue(adapter.supportsLimitSyntax());
        assertFalse(adapter.requiresRowIdForLimitForUpdate());
    }

    @Test
    @DisplayName("OceanBase adapter should support LIMIT syntax")
    void testOceanBaseSupportsLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("oceanbase");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertTrue(adapter.supportsLimitSyntax());
        assertFalse(adapter.requiresRowIdForLimitForUpdate());
    }

    @Test
    @DisplayName("Dameng adapter should not support LIMIT syntax")
    void testDamengNoLimit() {
        DatabaseConfig config = new DatabaseConfig();
        config.setType("dameng");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertFalse(adapter.supportsLimitSyntax());
    }

    @Test
    @DisplayName("All adapters should return correct database type")
    void testDatabaseTypes() {
        DatabaseConfig config = new DatabaseConfig();

        config.setType("mysql");
        assertEquals("MySQL", DatabaseFactory.create(config).getDatabaseType());

        config.setType("postgresql");
        assertEquals("PostgreSQL", DatabaseFactory.create(config).getDatabaseType());

        config.setType("oracle");
        assertEquals("Oracle", DatabaseFactory.create(config).getDatabaseType());

        config.setType("sqlserver");
        assertEquals("SQL Server", DatabaseFactory.create(config).getDatabaseType());

        config.setType("db2");
        assertEquals("DB2", DatabaseFactory.create(config).getDatabaseType());

        config.setType("dameng");
        assertEquals("Dameng", DatabaseFactory.create(config).getDatabaseType());

        config.setType("tidb");
        assertEquals("TiDB", DatabaseFactory.create(config).getDatabaseType());

        config.setType("oceanbase");
        assertEquals("OceanBase", DatabaseFactory.create(config).getDatabaseType());
    }

    @Test
    @DisplayName("Default collectHostMetrics interface method should return empty map")
    void testDefaultCollectHostMetrics() throws Exception {
        // Test the default interface method directly using a mock adapter
        DatabaseAdapter mockAdapter = new DatabaseAdapter() {
            @Override public void initialize() {}
            @Override public java.sql.Connection getConnection() { return null; }
            @Override public void close() {}
            @Override public void createSchema() {}
            @Override public void dropSchema() {}
            @Override public java.util.Map<String, Object> collectMetrics() { return new java.util.HashMap<>(); }
            @Override public String getDatabaseType() { return "Mock"; }
        };

        // Default implementation should return empty map
        assertTrue(mockAdapter.collectHostMetrics().isEmpty());
    }
}
