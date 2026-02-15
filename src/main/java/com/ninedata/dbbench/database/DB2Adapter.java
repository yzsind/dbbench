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
public class DB2Adapter extends AbstractDatabaseAdapter {

    public DB2Adapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "DB2";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Active connections
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) AS active_connections FROM SYSIBMADM.APPLICATIONS WHERE APPL_STATUS = 'UOWEXEC'"
                );
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("active_connections"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get active connections: {}", e.getMessage());
            }

            // Buffer pool hit ratio
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        POOL_DATA_L_READS + POOL_INDEX_L_READS AS logical_reads,
                        POOL_DATA_P_READS + POOL_INDEX_P_READS AS physical_reads
                    FROM TABLE(MON_GET_BUFFERPOOL(NULL, -2)) AS T
                """);
                long totalLogical = 0;
                long totalPhysical = 0;
                while (rs.next()) {
                    totalLogical += rs.getLong("logical_reads");
                    totalPhysical += rs.getLong("physical_reads");
                }
                rs.close();

                if (totalLogical > 0) {
                    double hitRatio = ((double) (totalLogical - totalPhysical) / totalLogical) * 100;
                    metrics.put("buffer_pool_hit_ratio", Math.round(hitRatio * 100.0) / 100.0);
                }
            } catch (SQLException e) {
                log.debug("Could not get buffer pool metrics: {}", e.getMessage());
            }

            // Lock waits
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT LOCK_WAITS, LOCK_WAIT_TIME FROM TABLE(MON_GET_DATABASE(-2)) AS T"
                );
                if (rs.next()) {
                    metrics.put("lock_waits", rs.getLong("LOCK_WAITS"));
                    metrics.put("lock_wait_time_ms", rs.getLong("LOCK_WAIT_TIME") / 1000);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get lock metrics: {}", e.getMessage());
            }

            // Row operations
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        ROWS_READ, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED
                    FROM TABLE(MON_GET_DATABASE(-2)) AS T
                """);
                if (rs.next()) {
                    metrics.put("rows_read", rs.getLong("ROWS_READ"));
                    metrics.put("rows_inserted", rs.getLong("ROWS_INSERTED"));
                    metrics.put("rows_updated", rs.getLong("ROWS_UPDATED"));
                    metrics.put("rows_deleted", rs.getLong("ROWS_DELETED"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get row metrics: {}", e.getMessage());
            }

            // Total connections
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) AS total_connections FROM SYSIBMADM.APPLICATIONS"
                );
                if (rs.next()) {
                    metrics.put("total_connections", rs.getLong("total_connections"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get total connections: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // DB2 system metrics from ENV_SYS_RESOURCES
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        CPU_TOTAL, CPU_USER, CPU_SYSTEM, CPU_IDLE,
                        MEMORY_TOTAL, MEMORY_FREE,
                        CPU_USAGE_TOTAL
                    FROM SYSIBMADM.ENV_SYS_RESOURCES
                """);
                if (rs.next()) {
                    metrics.put("cpuUsage", Math.round(rs.getDouble("CPU_USAGE_TOTAL") * 100.0) / 100.0);
                    long memTotal = rs.getLong("MEMORY_TOTAL");
                    long memFree = rs.getLong("MEMORY_FREE");
                    metrics.put("memoryTotal", memTotal / 1024); // Convert to MB
                    metrics.put("memoryFree", memFree / 1024);
                    metrics.put("memoryUsed", (memTotal - memFree) / 1024);
                    if (memTotal > 0) {
                        metrics.put("memoryUsage", Math.round(((memTotal - memFree) * 100.0 / memTotal) * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get system resources: {}", e.getMessage());
            }

            // I/O statistics from MON_GET_TABLESPACE
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        SUM(POOL_DATA_P_READS + POOL_INDEX_P_READS + POOL_XDA_P_READS) as physical_reads,
                        SUM(POOL_DATA_WRITES + POOL_INDEX_WRITES + POOL_XDA_WRITES) as physical_writes
                    FROM TABLE(MON_GET_TABLESPACE(NULL, -2)) AS T
                """);
                if (rs.next()) {
                    // DB2 page size is typically 4KB-32KB, assume 8KB
                    long pageSize = 8192;
                    metrics.put("diskReadBytes", rs.getLong("physical_reads") * pageSize);
                    metrics.put("diskWriteBytes", rs.getLong("physical_writes") * pageSize);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get I/O stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public boolean supportsLimitSyntax() {
        return false; // DB2 uses FETCH FIRST n ROWS ONLY
    }

    @Override
    protected String getDropTableStatement(String tableName) {
        // DB2 doesn't support IF EXISTS
        return "DROP TABLE " + tableName;
    }

    @Override
    protected String[] getCreateTableStatements() {
        return new String[]{
            """
            CREATE TABLE warehouse (
                w_id INT NOT NULL,
                w_name VARCHAR(10),
                w_street_1 VARCHAR(20),
                w_street_2 VARCHAR(20),
                w_city VARCHAR(20),
                w_state CHAR(2),
                w_zip CHAR(9),
                w_tax DECIMAL(4,4),
                w_ytd DECIMAL(12,2),
                PRIMARY KEY (w_id)
            )
            """,
            """
            CREATE TABLE district (
                d_id INT NOT NULL,
                d_w_id INT NOT NULL,
                d_name VARCHAR(10),
                d_street_1 VARCHAR(20),
                d_street_2 VARCHAR(20),
                d_city VARCHAR(20),
                d_state CHAR(2),
                d_zip CHAR(9),
                d_tax DECIMAL(4,4),
                d_ytd DECIMAL(12,2),
                d_next_o_id INT,
                PRIMARY KEY (d_w_id, d_id)
            )
            """,
            """
            CREATE TABLE customer (
                c_id INT NOT NULL,
                c_d_id INT NOT NULL,
                c_w_id INT NOT NULL,
                c_first VARCHAR(16),
                c_middle CHAR(2),
                c_last VARCHAR(16),
                c_street_1 VARCHAR(20),
                c_street_2 VARCHAR(20),
                c_city VARCHAR(20),
                c_state CHAR(2),
                c_zip CHAR(9),
                c_phone CHAR(16),
                c_since TIMESTAMP,
                c_credit CHAR(2),
                c_credit_lim DECIMAL(12,2),
                c_discount DECIMAL(4,4),
                c_balance DECIMAL(12,2),
                c_ytd_payment DECIMAL(12,2),
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data VARCHAR(500),
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            )
            """,
            """
            CREATE TABLE item (
                i_id INT NOT NULL,
                i_im_id INT,
                i_name VARCHAR(24),
                i_price DECIMAL(5,2),
                i_data VARCHAR(50),
                PRIMARY KEY (i_id)
            )
            """,
            """
            CREATE TABLE stock (
                s_i_id INT NOT NULL,
                s_w_id INT NOT NULL,
                s_quantity INT,
                s_dist_01 CHAR(24),
                s_dist_02 CHAR(24),
                s_dist_03 CHAR(24),
                s_dist_04 CHAR(24),
                s_dist_05 CHAR(24),
                s_dist_06 CHAR(24),
                s_dist_07 CHAR(24),
                s_dist_08 CHAR(24),
                s_dist_09 CHAR(24),
                s_dist_10 CHAR(24),
                s_ytd INT,
                s_order_cnt INT,
                s_remote_cnt INT,
                s_data VARCHAR(50),
                PRIMARY KEY (s_w_id, s_i_id)
            )
            """,
            """
            CREATE TABLE history (
                h_c_id INT,
                h_c_d_id INT,
                h_c_w_id INT,
                h_d_id INT,
                h_w_id INT,
                h_date TIMESTAMP,
                h_amount DECIMAL(6,2),
                h_data VARCHAR(24)
            )
            """,
            """
            CREATE TABLE oorder (
                o_id INT NOT NULL,
                o_d_id INT NOT NULL,
                o_w_id INT NOT NULL,
                o_c_id INT,
                o_entry_d TIMESTAMP,
                o_carrier_id INT,
                o_ol_cnt INT,
                o_all_local INT,
                PRIMARY KEY (o_w_id, o_d_id, o_id)
            )
            """,
            """
            CREATE TABLE new_order (
                no_o_id INT NOT NULL,
                no_d_id INT NOT NULL,
                no_w_id INT NOT NULL,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id)
            )
            """,
            """
            CREATE TABLE order_line (
                ol_o_id INT NOT NULL,
                ol_d_id INT NOT NULL,
                ol_w_id INT NOT NULL,
                ol_number INT NOT NULL,
                ol_i_id INT,
                ol_supply_w_id INT,
                ol_delivery_d TIMESTAMP,
                ol_quantity INT,
                ol_amount DECIMAL(6,2),
                ol_dist_info CHAR(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            )
            """
        };
    }

    @Override
    protected String[] getCreateIndexStatements() {
        return new String[]{
            "CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)",
            "CREATE INDEX idx_order_customer ON oorder (o_w_id, o_d_id, o_c_id, o_id)"
        };
    }
}
