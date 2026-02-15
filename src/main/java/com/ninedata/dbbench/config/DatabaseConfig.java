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
    private SshConfig ssh = new SshConfig();

    @Data
    public static class PoolConfig {
        private int size = 50;
        private int minIdle = 10;
    }

    @Data
    public static class SshConfig {
        private boolean enabled = false;
        private String host = "";
        private int port = 22;
        private String username = "root";
        private String password = "";
        private String privateKey = "";
        private String passphrase = "";
    }

    /**
     * Get the effective SSH host - uses explicit SSH host if set, otherwise extracts from JDBC URL.
     */
    public String getEffectiveSshHost() {
        if (ssh.getHost() != null && !ssh.getHost().isBlank()) {
            return ssh.getHost();
        }
        // Try to extract host from JDBC URL
        try {
            String url = jdbcUrl;
            // Remove jdbc: prefix and protocol
            int slashIdx = url.indexOf("//");
            if (slashIdx >= 0) {
                String hostPart = url.substring(slashIdx + 2);
                // Remove path/query
                int pathIdx = hostPart.indexOf('/');
                if (pathIdx >= 0) hostPart = hostPart.substring(0, pathIdx);
                int queryIdx = hostPart.indexOf('?');
                if (queryIdx >= 0) hostPart = hostPart.substring(0, queryIdx);
                // Remove port
                int colonIdx = hostPart.lastIndexOf(':');
                if (colonIdx >= 0) hostPart = hostPart.substring(0, colonIdx);
                return hostPart;
            }
        } catch (Exception ignored) {}
        return "";
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
            case "sqlite" -> "org.sqlite.JDBC";
            case "yashandb", "yashan" -> "com.yashandb.jdbc.Driver";
            case "gbase8s", "gbase" -> "com.gbasedbt.jdbc.Driver";
            case "sybase", "ase" -> "com.sybase.jdbc4.jdbc.SybDriver";
            case "hana", "saphana" -> "com.sap.db.jdbc.Driver";
            default -> "com.mysql.cj.jdbc.Driver";
        };
    }
}
