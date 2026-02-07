package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;

public class DatabaseFactory {

    public static DatabaseAdapter create(DatabaseConfig config) {
        return switch (config.getType().toLowerCase()) {
            case "mysql" -> new MySQLAdapter(config);
            case "postgresql", "postgres" -> new PostgreSQLAdapter(config);
            case "oracle" -> new OracleAdapter(config);
            case "sqlserver", "mssql" -> new SQLServerAdapter(config);
            case "db2" -> new DB2Adapter(config);
            case "dameng", "dm" -> new DamengAdapter(config);
            case "oceanbase" -> new OceanBaseAdapter(config);
            case "tidb" -> new TiDBAdapter(config);
            default -> throw new IllegalArgumentException("Unsupported database type: " + config.getType());
        };
    }
}
