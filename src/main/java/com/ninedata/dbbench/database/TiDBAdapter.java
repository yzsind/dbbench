package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

@Slf4j
public class TiDBAdapter extends MySQLAdapter {

    public TiDBAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "TiDB";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        // Start with MySQL-compatible base metrics (SHOW GLOBAL STATUS works in TiDB)
        Map<String, Object> metrics = super.collectMetrics();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // TiDB cluster node count
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT TYPE, COUNT(*) as cnt FROM INFORMATION_SCHEMA.CLUSTER_INFO GROUP BY TYPE"
                );
                while (rs.next()) {
                    String type = rs.getString("TYPE");
                    long count = rs.getLong("cnt");
                    metrics.put("cluster_" + type.toLowerCase() + "_count", count);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get TiDB cluster info: {}", e.getMessage());
            }

            // TiKV store status
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as store_count, " +
                    "SUM(AVAILABLE_SIZE) as available_bytes, " +
                    "SUM(CAPACITY) as capacity_bytes " +
                    "FROM INFORMATION_SCHEMA.TIKV_STORE_STATUS"
                );
                if (rs.next()) {
                    metrics.put("tikv_store_count", rs.getLong("store_count"));
                    metrics.put("tikv_available_bytes", rs.getLong("available_bytes"));
                    metrics.put("tikv_capacity_bytes", rs.getLong("capacity_bytes"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get TiKV store status: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = super.collectHostMetrics();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // TiKV region statistics
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as region_count, " +
                    "SUM(APPROXIMATE_SIZE) as total_size_mb, " +
                    "SUM(APPROXIMATE_KEYS) as total_keys " +
                    "FROM INFORMATION_SCHEMA.TIKV_REGION_STATUS"
                );
                if (rs.next()) {
                    metrics.put("tikv_region_count", rs.getLong("region_count"));
                    metrics.put("tikv_total_size_mb", rs.getLong("total_size_mb"));
                    metrics.put("tikv_total_keys", rs.getLong("total_keys"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get TiKV region stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }
}
