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
public class HANAAdapter extends AbstractDatabaseAdapter {

    public HANAAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "HANA";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Active connections
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as cnt FROM M_CONNECTIONS WHERE CONNECTION_STATUS = 'RUNNING'"
                );
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("cnt"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get HANA connection stats: {}", e.getMessage());
            }

            // Total connections
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as cnt FROM M_CONNECTIONS"
                );
                if (rs.next()) {
                    metrics.put("total_connections", rs.getLong("cnt"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get HANA total connections: {}", e.getMessage());
            }

            // Row store/column store memory
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT SUM(MEMORY_SIZE_IN_TOTAL) as total_mem, " +
                    "SUM(MEMORY_SIZE_IN_MAIN) as main_mem " +
                    "FROM M_CS_ALL_COLUMNS"
                );
                if (rs.next()) {
                    metrics.put("cs_total_memory", rs.getLong("total_mem"));
                    metrics.put("cs_main_memory", rs.getLong("main_mem"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get HANA column store stats: {}", e.getMessage());
            }

            // Lock waits
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as lock_waits FROM M_LOCK_WAITS_STATISTICS"
                );
                if (rs.next()) {
                    metrics.put("lock_waits", rs.getLong("lock_waits"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get HANA lock stats: {}", e.getMessage());
            }

            // SQL plan cache hit ratio
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT PLAN_CACHE_HIT_RATIO as hit_ratio FROM M_SQL_PLAN_CACHE_OVERVIEW"
                );
                if (rs.next()) {
                    metrics.put("plan_cache_hit_ratio", Math.round(rs.getDouble("hit_ratio") * 100.0) / 100.0);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get HANA plan cache stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Host resource utilization
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT TOTAL_CPU_USER_TIME, TOTAL_CPU_SYSTEM_TIME, TOTAL_CPU_IDLE_TIME, " +
                    "USED_PHYSICAL_MEMORY, FREE_PHYSICAL_MEMORY " +
                    "FROM M_HOST_RESOURCE_UTILIZATION"
                );
                if (rs.next()) {
                    long cpuUser = rs.getLong("TOTAL_CPU_USER_TIME");
                    long cpuSys = rs.getLong("TOTAL_CPU_SYSTEM_TIME");
                    long cpuIdle = rs.getLong("TOTAL_CPU_IDLE_TIME");
                    long total = cpuUser + cpuSys + cpuIdle;
                    if (total > 0) {
                        double cpuUsage = ((cpuUser + cpuSys) * 100.0) / total;
                        metrics.put("cpuUsage", Math.round(cpuUsage * 100.0) / 100.0);
                    }
                    long usedMem = rs.getLong("USED_PHYSICAL_MEMORY");
                    long freeMem = rs.getLong("FREE_PHYSICAL_MEMORY");
                    metrics.put("memoryTotal", (usedMem + freeMem) / (1024 * 1024));
                    metrics.put("memoryUsed", usedMem / (1024 * 1024));
                    metrics.put("memoryFree", freeMem / (1024 * 1024));
                    if (usedMem + freeMem > 0) {
                        double memUsage = (usedMem * 100.0) / (usedMem + freeMem);
                        metrics.put("memoryUsagePercent", Math.round(memUsage * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get HANA host resource stats: {}", e.getMessage());
            }

            // Disk I/O
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT SUM(TOTAL_READ_SIZE) as read_bytes, SUM(TOTAL_WRITE_SIZE) as write_bytes " +
                    "FROM M_VOLUME_IO_TOTAL_STATISTICS"
                );
                if (rs.next()) {
                    metrics.put("diskReadBytes", rs.getLong("read_bytes"));
                    metrics.put("diskWriteBytes", rs.getLong("write_bytes"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get HANA disk I/O stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    protected String[] getCreateTableStatements() {
        return new String[]{
            "CREATE TABLE warehouse (w_id INTEGER NOT NULL, w_name NVARCHAR(10), w_street_1 NVARCHAR(20), w_street_2 NVARCHAR(20), w_city NVARCHAR(20), w_state NVARCHAR(2), w_zip NVARCHAR(9), w_tax DECIMAL(4,4), w_ytd DECIMAL(12,2), PRIMARY KEY (w_id))",
            "CREATE TABLE district (d_id INTEGER NOT NULL, d_w_id INTEGER NOT NULL, d_name NVARCHAR(10), d_street_1 NVARCHAR(20), d_street_2 NVARCHAR(20), d_city NVARCHAR(20), d_state NVARCHAR(2), d_zip NVARCHAR(9), d_tax DECIMAL(4,4), d_ytd DECIMAL(12,2), d_next_o_id INTEGER, PRIMARY KEY (d_w_id, d_id))",
            "CREATE TABLE customer (c_id INTEGER NOT NULL, c_d_id INTEGER NOT NULL, c_w_id INTEGER NOT NULL, c_first NVARCHAR(16), c_middle NVARCHAR(2), c_last NVARCHAR(16), c_street_1 NVARCHAR(20), c_street_2 NVARCHAR(20), c_city NVARCHAR(20), c_state NVARCHAR(2), c_zip NVARCHAR(9), c_phone NVARCHAR(16), c_since TIMESTAMP, c_credit NVARCHAR(2), c_credit_lim DECIMAL(12,2), c_discount DECIMAL(4,4), c_balance DECIMAL(12,2), c_ytd_payment DECIMAL(12,2), c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data NVARCHAR(500), PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)",
            "CREATE TABLE item (i_id INTEGER NOT NULL, i_im_id INTEGER, i_name NVARCHAR(24), i_price DECIMAL(5,2), i_data NVARCHAR(50), PRIMARY KEY (i_id))",
            "CREATE TABLE stock (s_i_id INTEGER NOT NULL, s_w_id INTEGER NOT NULL, s_quantity INTEGER, s_dist_01 NVARCHAR(24), s_dist_02 NVARCHAR(24), s_dist_03 NVARCHAR(24), s_dist_04 NVARCHAR(24), s_dist_05 NVARCHAR(24), s_dist_06 NVARCHAR(24), s_dist_07 NVARCHAR(24), s_dist_08 NVARCHAR(24), s_dist_09 NVARCHAR(24), s_dist_10 NVARCHAR(24), s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data NVARCHAR(50), PRIMARY KEY (s_w_id, s_i_id))",
            "CREATE TABLE history (h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER, h_w_id INTEGER, h_date TIMESTAMP, h_amount DECIMAL(6,2), h_data NVARCHAR(24))",
            "CREATE TABLE oorder (o_id INTEGER NOT NULL, o_d_id INTEGER NOT NULL, o_w_id INTEGER NOT NULL, o_c_id INTEGER, o_entry_d TIMESTAMP, o_carrier_id INTEGER, o_ol_cnt INTEGER, o_all_local INTEGER, PRIMARY KEY (o_w_id, o_d_id, o_id))",
            "CREATE INDEX idx_order_customer ON oorder (o_w_id, o_d_id, o_c_id, o_id)",
            "CREATE TABLE new_order (no_o_id INTEGER NOT NULL, no_d_id INTEGER NOT NULL, no_w_id INTEGER NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
            "CREATE TABLE order_line (ol_o_id INTEGER NOT NULL, ol_d_id INTEGER NOT NULL, ol_w_id INTEGER NOT NULL, ol_number INTEGER NOT NULL, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_delivery_d TIMESTAMP, ol_quantity INTEGER, ol_amount DECIMAL(6,2), ol_dist_info NVARCHAR(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
        };
    }

    @Override
    protected String getDropTableStatement(String tableName) {
        return "DROP TABLE " + tableName + " CASCADE";
    }
}
