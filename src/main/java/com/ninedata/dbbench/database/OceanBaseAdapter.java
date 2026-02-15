package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

@Slf4j
public class OceanBaseAdapter extends MySQLAdapter {

    public OceanBaseAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "OceanBase";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        // Start with MySQL-compatible base metrics
        Map<String, Object> metrics = super.collectMetrics();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Memstore usage
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT SUM(ACTIVE_SPAN) as active_mem, " +
                    "SUM(FREEZE_TRIGGER) as freeze_trigger, " +
                    "SUM(MEMSTORE_LIMIT) as mem_limit " +
                    "FROM GV$OB_MEMSTORE"
                );
                if (rs.next()) {
                    long activeMem = rs.getLong("active_mem");
                    long memLimit = rs.getLong("mem_limit");
                    metrics.put("memstore_active_bytes", activeMem);
                    metrics.put("memstore_limit_bytes", memLimit);
                    if (memLimit > 0) {
                        double usage = ((double) activeMem / memLimit) * 100;
                        metrics.put("memstore_usage_pct", Math.round(usage * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get OceanBase memstore stats: {}", e.getMessage());
            }

            // SQL audit aggregation (recent 10 seconds)
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as request_count, " +
                    "AVG(ELAPSED_TIME) as avg_elapsed_us, " +
                    "MAX(ELAPSED_TIME) as max_elapsed_us " +
                    "FROM GV$OB_SQL_AUDIT " +
                    "WHERE REQUEST_TIME > (UNIX_TIMESTAMP() - 10) * 1000000"
                );
                if (rs.next()) {
                    metrics.put("ob_request_count_10s", rs.getLong("request_count"));
                    metrics.put("ob_avg_rt_us", Math.round(rs.getDouble("avg_elapsed_us") * 100.0) / 100.0);
                    metrics.put("ob_max_rt_us", rs.getLong("max_elapsed_us"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get OceanBase SQL audit stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = super.collectHostMetrics();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // OceanBase server resource stats
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT SVR_IP, CPU_CAPACITY, CPU_ASSIGNED, " +
                    "MEM_CAPACITY, MEM_ASSIGNED " +
                    "FROM GV$OB_SERVERS"
                );
                long totalCpuCapacity = 0, totalCpuAssigned = 0;
                long totalMemCapacity = 0, totalMemAssigned = 0;
                int serverCount = 0;
                while (rs.next()) {
                    totalCpuCapacity += rs.getLong("CPU_CAPACITY");
                    totalCpuAssigned += rs.getLong("CPU_ASSIGNED");
                    totalMemCapacity += rs.getLong("MEM_CAPACITY");
                    totalMemAssigned += rs.getLong("MEM_ASSIGNED");
                    serverCount++;
                }
                if (serverCount > 0) {
                    metrics.put("ob_server_count", serverCount);
                    metrics.put("ob_cpu_capacity", totalCpuCapacity);
                    metrics.put("ob_cpu_assigned", totalCpuAssigned);
                    metrics.put("ob_mem_capacity_bytes", totalMemCapacity);
                    metrics.put("ob_mem_assigned_bytes", totalMemAssigned);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get OceanBase server stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }
}
