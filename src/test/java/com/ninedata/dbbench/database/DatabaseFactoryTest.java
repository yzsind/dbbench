package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DatabaseFactory Tests")
class DatabaseFactoryTest {

    private DatabaseConfig config;

    @BeforeEach
    void setUp() {
        config = new DatabaseConfig();
    }

    @Test
    @DisplayName("Should create MySQLAdapter for mysql type")
    void testCreateMySQLAdapter() {
        config.setType("mysql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof MySQLAdapter);
        assertEquals("MySQL", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create PostgreSQLAdapter for postgresql type")
    void testCreatePostgreSQLAdapter() {
        config.setType("postgresql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof PostgreSQLAdapter);
        assertEquals("PostgreSQL", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create PostgreSQLAdapter for postgres type")
    void testCreatePostgresAdapter() {
        config.setType("postgres");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof PostgreSQLAdapter);
    }

    @Test
    @DisplayName("Should create OracleAdapter for oracle type")
    void testCreateOracleAdapter() {
        config.setType("oracle");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof OracleAdapter);
        assertEquals("Oracle", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create SQLServerAdapter for sqlserver type")
    void testCreateSQLServerAdapter() {
        config.setType("sqlserver");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof SQLServerAdapter);
        assertEquals("SQL Server", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create SQLServerAdapter for mssql type")
    void testCreateMSSQLAdapter() {
        config.setType("mssql");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof SQLServerAdapter);
    }

    @Test
    @DisplayName("Should create DB2Adapter for db2 type")
    void testCreateDB2Adapter() {
        config.setType("db2");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof DB2Adapter);
        assertEquals("DB2", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create DamengAdapter for dameng type")
    void testCreateDamengAdapter() {
        config.setType("dameng");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof DamengAdapter);
        assertEquals("Dameng", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create DamengAdapter for dm type")
    void testCreateDMAdapter() {
        config.setType("dm");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof DamengAdapter);
    }

    @Test
    @DisplayName("Should create OceanBaseAdapter for oceanbase type")
    void testCreateOceanBaseAdapter() {
        config.setType("oceanbase");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof OceanBaseAdapter);
        assertEquals("OceanBase", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should create TiDBAdapter for tidb type")
    void testCreateTiDBAdapter() {
        config.setType("tidb");
        DatabaseAdapter adapter = DatabaseFactory.create(config);

        assertNotNull(adapter);
        assertTrue(adapter instanceof TiDBAdapter);
        assertEquals("TiDB", adapter.getDatabaseType());
    }

    @Test
    @DisplayName("Should throw exception for unsupported type")
    void testUnsupportedType() {
        config.setType("unsupported");

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> DatabaseFactory.create(config)
        );

        assertTrue(exception.getMessage().contains("Unsupported database type"));
        assertTrue(exception.getMessage().contains("unsupported"));
    }

    @Test
    @DisplayName("Should be case insensitive for type")
    void testCaseInsensitive() {
        config.setType("MYSQL");
        DatabaseAdapter adapter1 = DatabaseFactory.create(config);
        assertTrue(adapter1 instanceof MySQLAdapter);

        config.setType("MySQL");
        DatabaseAdapter adapter2 = DatabaseFactory.create(config);
        assertTrue(adapter2 instanceof MySQLAdapter);

        config.setType("PostgreSQL");
        DatabaseAdapter adapter3 = DatabaseFactory.create(config);
        assertTrue(adapter3 instanceof PostgreSQLAdapter);
    }
}
