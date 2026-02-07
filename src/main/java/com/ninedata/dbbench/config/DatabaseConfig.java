package com.ninedata.dbbench.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "db")
public class DatabaseConfig {
    private String type = "mysql";
    private String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/tpcc?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true";
    private String username = "sysbench";
    private String password = "sysbench";
    private PoolConfig pool = new PoolConfig();

    @Data
    public static class PoolConfig {
        private int size = 50;
        private int minIdle = 10;
    }

    public String getDriverClassName() {
        return switch (type.toLowerCase()) {
            case "mysql", "tidb" -> "com.mysql.cj.jdbc.Driver";
            case "postgresql" -> "org.postgresql.Driver";
            case "oracle" -> "oracle.jdbc.OracleDriver";
            case "sqlserver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            case "db2" -> "com.ibm.db2.jcc.DB2Driver";
            case "dameng" -> "dm.jdbc.driver.DmDriver";
            case "oceanbase" -> "com.oceanbase.jdbc.Driver";
            default -> "com.mysql.cj.jdbc.Driver";
        };
    }
}
