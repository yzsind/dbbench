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
public class OracleAdapter extends AbstractDatabaseAdapter {
    public OracleAdapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "Oracle"; }

    @Override
    public boolean supportsLimitSyntax() {
        return false; // Oracle uses ROWNUM or FETCH FIRST (12c+)
    }

    @Override
    public boolean requiresRowIdForLimitForUpdate() {
        return true; // Oracle 11g requires ROWID-based subquery for SELECT ... FOR UPDATE with LIMIT
    }

    @Override
    protected String getDropTableStatement(String tableName) {
        // Oracle doesn't support IF EXISTS, use plain DROP TABLE
        return "DROP TABLE " + tableName + " CASCADE CONSTRAINTS";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Session statistics
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as active_sessions FROM V$SESSION WHERE STATUS = 'ACTIVE' AND TYPE = 'USER'"
                );
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("active_sessions"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get session stats: {}", e.getMessage());
            }

            // Buffer cache hit ratio
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT (1 - (phy.value / (cur.value + con.value))) * 100 AS hit_ratio
                    FROM V$SYSSTAT phy, V$SYSSTAT cur, V$SYSSTAT con
                    WHERE phy.name = 'physical reads'
                    AND cur.name = 'db block gets'
                    AND con.name = 'consistent gets'
                    AND (cur.value + con.value) > 0
                """);
                if (rs.next()) {
                    metrics.put("buffer_pool_hit_ratio", Math.round(rs.getDouble("hit_ratio") * 100.0) / 100.0);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get buffer cache stats: {}", e.getMessage());
            }

            // Wait statistics
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as lock_waits FROM V$SESSION WHERE BLOCKING_SESSION IS NOT NULL"
                );
                if (rs.next()) {
                    metrics.put("lock_waits", rs.getLong("lock_waits"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get lock stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Oracle V$OSSTAT provides OS-level metrics
            String sql = """
            SELECT 
                (SELECT VALUE FROM V$SYSMETRIC WHERE METRIC_NAME = 'Host CPU Utilization (%)') AS CPU_USAGE,
                (SELECT VALUE FROM V$OSSTAT WHERE STAT_NAME = 'NUM_CPUS') AS CPU_CORES,
                (SELECT VALUE FROM V$OSSTAT WHERE STAT_NAME = 'PHYSICAL_MEMORY_BYTES') AS MEM_TOTAL,
                (SELECT VALUE FROM V$OSSTAT WHERE STAT_NAME = 'FREE_MEMORY_BYTES') AS MEM_FREE
            FROM DUAL
            """;
            try (ResultSet rs = stmt.executeQuery(sql)) {

                    if (rs.next()) {
                        double cpuUsage = rs.getDouble("CPU_USAGE");
                        long cpuCores = rs.getLong("CPU_CORES");
                        long memTotal = rs.getLong("MEM_TOTAL");
                        long memFree = rs.getLong("MEM_FREE");

                        // 填充指标数据
                        metrics.put("cpuUsage", Math.round(cpuUsage * 100.0) / 100.0); // 保留两位小数
                        metrics.put("cpuCores", cpuCores);
                        metrics.put("memoryTotal", memTotal / (1024 * 1024)); // B -> MB
                        metrics.put("memoryFree", memFree / (1024 * 1024));   // B -> MB

                        // 计算内存使用率 (可选)
                        if (memTotal > 0) {
                            double memUsage = ((double)(memTotal - memFree) / memTotal) * 100;
                            metrics.put("memoryUsagePercent", Math.round(memUsage * 100.0) / 100.0);
                        }
                    }
            } catch (SQLException e) {
                log.debug("Could not get OS stats from V$OSSTAT: {}", e.getMessage());
            }

            // I/O statistics
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT SUM(PHYRDS) as physical_reads, SUM(PHYWRTS) as physical_writes,
                           SUM(PHYBLKRD) as blocks_read, SUM(PHYBLKWRT) as blocks_written
                    FROM V$FILESTAT
                """);
                if (rs.next()) {
                    // Oracle block size is typically 8KB
                    long blockSize = 8192;
                    metrics.put("diskReadBytes", rs.getLong("blocks_read") * blockSize);
                    metrics.put("diskWriteBytes", rs.getLong("blocks_written") * blockSize);
                    metrics.put("physicalReads", rs.getLong("physical_reads"));
                    metrics.put("physicalWrites", rs.getLong("physical_writes"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get I/O stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override protected String[] getCreateTableStatements() {
        return new String[]{
            "CREATE TABLE warehouse (w_id NUMBER NOT NULL, w_name VARCHAR2(10), w_street_1 VARCHAR2(20), w_street_2 VARCHAR2(20), w_city VARCHAR2(20), w_state CHAR(2), w_zip CHAR(9), w_tax NUMBER(4,4), w_ytd NUMBER(12,2), PRIMARY KEY (w_id))",
            "CREATE TABLE district (d_id NUMBER NOT NULL, d_w_id NUMBER NOT NULL, d_name VARCHAR2(10), d_street_1 VARCHAR2(20), d_street_2 VARCHAR2(20), d_city VARCHAR2(20), d_state CHAR(2), d_zip CHAR(9), d_tax NUMBER(4,4), d_ytd NUMBER(12,2), d_next_o_id NUMBER, PRIMARY KEY (d_w_id, d_id))",
            "CREATE TABLE customer (c_id NUMBER NOT NULL, c_d_id NUMBER NOT NULL, c_w_id NUMBER NOT NULL, c_first VARCHAR2(16), c_middle CHAR(2), c_last VARCHAR2(16), c_street_1 VARCHAR2(20), c_street_2 VARCHAR2(20), c_city VARCHAR2(20), c_state CHAR(2), c_zip CHAR(9), c_phone CHAR(16), c_since DATE, c_credit CHAR(2), c_credit_lim NUMBER(12,2), c_discount NUMBER(4,4), c_balance NUMBER(12,2), c_ytd_payment NUMBER(12,2), c_payment_cnt NUMBER, c_delivery_cnt NUMBER, c_data VARCHAR2(500), PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "CREATE TABLE item (i_id NUMBER NOT NULL, i_im_id NUMBER, i_name VARCHAR2(24), i_price NUMBER(5,2), i_data VARCHAR2(50), PRIMARY KEY (i_id))",
            "CREATE TABLE stock (s_i_id NUMBER NOT NULL, s_w_id NUMBER NOT NULL, s_quantity NUMBER, s_dist_01 CHAR(24), s_dist_02 CHAR(24), s_dist_03 CHAR(24), s_dist_04 CHAR(24), s_dist_05 CHAR(24), s_dist_06 CHAR(24), s_dist_07 CHAR(24), s_dist_08 CHAR(24), s_dist_09 CHAR(24), s_dist_10 CHAR(24), s_ytd NUMBER, s_order_cnt NUMBER, s_remote_cnt NUMBER, s_data VARCHAR2(50), PRIMARY KEY (s_w_id, s_i_id))",
            "CREATE TABLE history (h_c_id NUMBER, h_c_d_id NUMBER, h_c_w_id NUMBER, h_d_id NUMBER, h_w_id NUMBER, h_date DATE, h_amount NUMBER(6,2), h_data VARCHAR2(24))",
            "CREATE TABLE oorder (o_id NUMBER NOT NULL, o_d_id NUMBER NOT NULL, o_w_id NUMBER NOT NULL, o_c_id NUMBER, o_entry_d DATE, o_carrier_id NUMBER, o_ol_cnt NUMBER, o_all_local NUMBER, PRIMARY KEY (o_w_id, o_d_id, o_id))",
            "CREATE TABLE new_order (no_o_id NUMBER NOT NULL, no_d_id NUMBER NOT NULL, no_w_id NUMBER NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
            "CREATE TABLE order_line (ol_o_id NUMBER NOT NULL, ol_d_id NUMBER NOT NULL, ol_w_id NUMBER NOT NULL, ol_number NUMBER NOT NULL, ol_i_id NUMBER, ol_supply_w_id NUMBER, ol_delivery_d DATE, ol_quantity NUMBER, ol_amount NUMBER(6,2), ol_dist_info CHAR(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
        };
    }
}
