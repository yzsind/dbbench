package com.ninedata.dbbench.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DatabaseConfig Tests")
class DatabaseConfigTest {

    private DatabaseConfig config;

    @BeforeEach
    void setUp() {
        config = new DatabaseConfig();
    }

    @Test
    @DisplayName("Should have correct default values")
    void testDefaultValues() {
        assertEquals("mysql", config.getType());
        assertTrue(config.getJdbcUrl().contains("mysql"));
        assertEquals("sysbench", config.getUsername());
        assertEquals("sysbench", config.getPassword());
        assertEquals(50, config.getPool().getSize());
        assertEquals(10, config.getPool().getMinIdle());
    }

    @Test
    @DisplayName("Should return correct driver for MySQL")
    void testMySQLDriver() {
        config.setType("mysql");
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for MySQL (case insensitive)")
    void testMySQLDriverCaseInsensitive() {
        config.setType("MySQL");
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());

        config.setType("MYSQL");
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for PostgreSQL")
    void testPostgreSQLDriver() {
        config.setType("postgresql");
        assertEquals("org.postgresql.Driver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for Oracle")
    void testOracleDriver() {
        config.setType("oracle");
        assertEquals("oracle.jdbc.OracleDriver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for SQL Server")
    void testSQLServerDriver() {
        config.setType("sqlserver");
        assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for DB2")
    void testDB2Driver() {
        config.setType("db2");
        assertEquals("com.ibm.db2.jcc.DB2Driver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for Dameng")
    void testDamengDriver() {
        config.setType("dameng");
        assertEquals("dm.jdbc.driver.DmDriver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for OceanBase")
    void testOceanBaseDriver() {
        config.setType("oceanbase");
        assertEquals("com.oceanbase.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return correct driver for TiDB")
    void testTiDBDriver() {
        config.setType("tidb");
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should return MySQL driver for unknown type")
    void testUnknownTypeDriver() {
        config.setType("unknown");
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    @DisplayName("Should allow setting all properties")
    void testSetProperties() {
        config.setType("postgresql");
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/test");
        config.setUsername("testuser");
        config.setPassword("testpass");
        config.getPool().setSize(100);
        config.getPool().setMinIdle(20);

        assertEquals("postgresql", config.getType());
        assertEquals("jdbc:postgresql://localhost:5432/test", config.getJdbcUrl());
        assertEquals("testuser", config.getUsername());
        assertEquals("testpass", config.getPassword());
        assertEquals(100, config.getPool().getSize());
        assertEquals(20, config.getPool().getMinIdle());
    }

    @Test
    @DisplayName("Should have non-null pool config by default")
    void testPoolConfigNotNull() {
        assertNotNull(config.getPool());
    }
}
