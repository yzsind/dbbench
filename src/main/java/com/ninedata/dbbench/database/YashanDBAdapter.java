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
public class YashanDBAdapter extends OracleAdapter {

    public YashanDBAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "YashanDB";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Active sessions
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as cnt FROM V$SESSION WHERE STATUS = 'ACTIVE'"
                );
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("cnt"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get YashanDB session stats: {}", e.getMessage());
            }

            // Buffer cache hit ratio from V$SYSSTAT
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT (1 - (phy.value / NULLIF(cur.value + con.value, 0))) * 100 AS hit_ratio
                    FROM V$SYSSTAT phy, V$SYSSTAT cur, V$SYSSTAT con
                    WHERE phy.name = 'physical reads'
                    AND cur.name = 'db block gets'
                    AND con.name = 'consistent gets'
                """);
                if (rs.next()) {
                    double hitRatio = rs.getDouble("hit_ratio");
                    if (!rs.wasNull()) {
                        metrics.put("buffer_pool_hit_ratio", Math.round(hitRatio * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get YashanDB buffer cache stats: {}", e.getMessage());
            }

            // Lock waits
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as lock_waits FROM V$SESSION WHERE BLOCKING_SESSION IS NOT NULL"
                );
                if (rs.next()) {
                    metrics.put("lock_waits", rs.getLong("lock_waits"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get YashanDB lock stats: {}", e.getMessage());
            }

            // Row operation stats
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT NAME, VALUE FROM V$SYSSTAT WHERE NAME IN " +
                    "('physical reads', 'physical writes', 'rows inserted', 'rows updated', 'rows deleted')"
                );
                while (rs.next()) {
                    String name = rs.getString("NAME");
                    long value = rs.getLong("VALUE");
                    switch (name) {
                        case "physical reads" -> metrics.put("physical_reads", value);
                        case "physical writes" -> metrics.put("physical_writes", value);
                        case "rows inserted" -> metrics.put("rows_inserted", value);
                        case "rows updated" -> metrics.put("rows_updated", value);
                        case "rows deleted" -> metrics.put("rows_deleted", value);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get YashanDB sysstat: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // YashanDB supports V$OSSTAT similar to Oracle
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT STAT_NAME, VALUE FROM V$OSSTAT WHERE STAT_NAME IN " +
                    "('NUM_CPUS', 'PHYSICAL_MEMORY_BYTES', 'FREE_MEMORY_BYTES')"
                );
                long memTotal = 0, memFree = 0;
                while (rs.next()) {
                    String name = rs.getString("STAT_NAME");
                    long value = rs.getLong("VALUE");
                    switch (name) {
                        case "NUM_CPUS" -> metrics.put("cpuCores", value);
                        case "PHYSICAL_MEMORY_BYTES" -> { memTotal = value; metrics.put("memoryTotal", value / (1024 * 1024)); }
                        case "FREE_MEMORY_BYTES" -> { memFree = value; metrics.put("memoryFree", value / (1024 * 1024)); }
                    }
                }
                if (memTotal > 0) {
                    double memUsage = ((double)(memTotal - memFree) / memTotal) * 100;
                    metrics.put("memoryUsagePercent", Math.round(memUsage * 100.0) / 100.0);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get YashanDB OS stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }
}
