package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DamengAdapter extends OracleAdapter {

    public DamengAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "Dameng";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Buffer pool hit ratio from V$BUFFERPOOL
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT RATIODBCR as hit_ratio FROM V$BUFFERPOOL"
                );
                if (rs.next()) {
                    metrics.put("buffer_pool_hit_ratio",
                        Math.round(rs.getDouble("hit_ratio") * 100.0) / 100.0);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Dameng buffer pool stats: {}", e.getMessage());
            }

            // Active sessions from V$SESSIONS
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as active_count FROM V$SESSIONS WHERE STATE = 'ACTIVE'"
                );
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("active_count"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Dameng session stats: {}", e.getMessage());
            }

            // Lock waits from V$LOCK
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as lock_waits FROM V$LOCK WHERE BLOCKED = 1"
                );
                if (rs.next()) {
                    metrics.put("lock_waits", rs.getLong("lock_waits"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Dameng lock stats: {}", e.getMessage());
            }

            // Row operation stats from V$SYSSTAT
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT NAME, STAT_VAL FROM V$SYSSTAT " +
                    "WHERE NAME IN ('physical reads', 'physical writes', 'select count', " +
                    "'insert count', 'update count', 'delete count')"
                );
                while (rs.next()) {
                    String name = rs.getString("NAME");
                    long value = rs.getLong("STAT_VAL");
                    switch (name) {
                        case "physical reads" -> metrics.put("physical_reads", value);
                        case "physical writes" -> metrics.put("physical_writes", value);
                        case "select count" -> metrics.put("rows_read", value);
                        case "insert count" -> metrics.put("rows_inserted", value);
                        case "update count" -> metrics.put("rows_updated", value);
                        case "delete count" -> metrics.put("rows_deleted", value);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Dameng sysstat: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // CPU and memory from V$SYSTEMINFO
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT NAME, STAT_VAL FROM V$SYSTEMINFO " +
                    "WHERE NAME IN ('OS_CPU_USER', 'OS_CPU_SYS', 'OS_MEM_TOTAL', 'OS_MEM_FREE')"
                );
                while (rs.next()) {
                    String name = rs.getString("NAME");
                    long value = rs.getLong("STAT_VAL");
                    switch (name) {
                        case "OS_CPU_USER" -> metrics.put("cpuUsage", (double) value);
                        case "OS_CPU_SYS" -> metrics.put("cpuSystem", (double) value);
                        case "OS_MEM_TOTAL" -> metrics.put("totalMemory", value);
                        case "OS_MEM_FREE" -> metrics.put("freeMemory", value);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Dameng system info: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }
}
