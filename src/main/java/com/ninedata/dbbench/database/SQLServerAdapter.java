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
public class SQLServerAdapter extends AbstractDatabaseAdapter {
    public SQLServerAdapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "SQL Server"; }

    @Override
    public boolean supportsLimitSyntax() {
        return false; // SQL Server uses TOP instead of LIMIT
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Active connections
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as active_connections FROM sys.dm_exec_sessions WHERE is_user_process = 1");
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("active_connections"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get connection stats: {}", e.getMessage());
            }

            // Buffer cache hit ratio
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        (a.cntr_value * 1.0 / b.cntr_value) * 100.0 as buffer_hit_ratio
                    FROM sys.dm_os_performance_counters a
                    JOIN sys.dm_os_performance_counters b ON a.object_name = b.object_name
                    WHERE a.counter_name = 'Buffer cache hit ratio'
                    AND b.counter_name = 'Buffer cache hit ratio base'
                    AND b.cntr_value > 0
                    """);
                if (rs.next()) {
                    double hitRatio = rs.getDouble("buffer_hit_ratio");
                    metrics.put("buffer_pool_hit_ratio", Math.round(hitRatio * 100.0) / 100.0);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get buffer cache stats: {}", e.getMessage());
            }

            // Lock waits
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as lock_waits FROM sys.dm_tran_locks WHERE request_status = 'WAIT'");
                if (rs.next()) {
                    metrics.put("row_lock_waits", rs.getLong("lock_waits"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get lock stats: {}", e.getMessage());
            }

            // Batch requests/sec (as proxy for slow queries tracking)
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT cntr_value as batch_requests
                    FROM sys.dm_os_performance_counters
                    WHERE counter_name = 'Batch Requests/sec'
                    """);
                if (rs.next()) {
                    metrics.put("batch_requests", rs.getLong("batch_requests"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get batch request stats: {}", e.getMessage());
            }

            // Transactions
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT cntr_value as transactions
                    FROM sys.dm_os_performance_counters
                    WHERE counter_name = 'Transactions/sec'
                    AND instance_name = '_Total'
                    """);
                if (rs.next()) {
                    metrics.put("transactions", rs.getLong("transactions"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get transaction stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // CPU usage from sys.dm_os_ring_buffers
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT TOP 1
                        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as system_idle,
                        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as sql_cpu
                    FROM (
                        SELECT CONVERT(xml, record) as record
                        FROM sys.dm_os_ring_buffers
                        WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                        AND record LIKE '%<SystemHealth>%'
                    ) as x
                    ORDER BY record DESC
                    """);
                if (rs.next()) {
                    int sqlCpu = rs.getInt("sql_cpu");
                    int systemIdle = rs.getInt("system_idle");
                    int totalCpu = 100 - systemIdle;
                    metrics.put("cpuUsage", (double) totalCpu);
                    metrics.put("sqlServerCpuUsage", (double) sqlCpu);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get CPU stats from ring buffers: {}", e.getMessage());
                // Fallback: estimate from active requests
                try {
                    ResultSet rs = stmt.executeQuery("""
                        SELECT
                            COUNT(*) as active_requests,
                            (SELECT cpu_count FROM sys.dm_os_sys_info) as cpu_count
                        FROM sys.dm_exec_requests
                        WHERE session_id > 50
                        """);
                    if (rs.next()) {
                        int activeRequests = rs.getInt("active_requests");
                        int cpuCount = rs.getInt("cpu_count");
                        double cpuEstimate = Math.min((activeRequests * 100.0) / Math.max(cpuCount, 1), 100);
                        metrics.put("cpuUsage", Math.round(cpuEstimate * 100.0) / 100.0);
                    }
                    rs.close();
                } catch (SQLException e2) {
                    log.debug("Could not estimate CPU: {}", e2.getMessage());
                }
            }

            // Memory usage
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        physical_memory_kb / 1024 as physical_memory_mb,
                        committed_kb / 1024 as committed_mb,
                        committed_target_kb / 1024 as target_mb
                    FROM sys.dm_os_sys_info
                    """);
                if (rs.next()) {
                    long physicalMemoryMb = rs.getLong("physical_memory_mb");
                    long committedMb = rs.getLong("committed_mb");
                    long targetMb = rs.getLong("target_mb");

                    metrics.put("memoryTotal", physicalMemoryMb);
                    metrics.put("memoryUsed", committedMb);
                    metrics.put("memoryTarget", targetMb);

                    if (targetMb > 0) {
                        double memUsage = (committedMb * 100.0) / targetMb;
                        metrics.put("memoryUsage", Math.round(memUsage * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get memory stats: {}", e.getMessage());
            }

            // Disk I/O from sys.dm_io_virtual_file_stats
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        SUM(num_of_bytes_read) as total_bytes_read,
                        SUM(num_of_bytes_written) as total_bytes_written,
                        SUM(io_stall_read_ms) as read_stall_ms,
                        SUM(io_stall_write_ms) as write_stall_ms
                    FROM sys.dm_io_virtual_file_stats(NULL, NULL)
                    """);
                if (rs.next()) {
                    metrics.put("diskReadBytes", rs.getLong("total_bytes_read"));
                    metrics.put("diskWriteBytes", rs.getLong("total_bytes_written"));
                    metrics.put("diskReadTimeMs", rs.getLong("read_stall_ms"));
                    metrics.put("diskWriteTimeMs", rs.getLong("write_stall_ms"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get disk I/O stats: {}", e.getMessage());
            }

            // Page life expectancy (memory pressure indicator)
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT cntr_value as page_life_expectancy
                    FROM sys.dm_os_performance_counters
                    WHERE counter_name = 'Page life expectancy'
                    AND object_name LIKE '%Buffer Manager%'
                    """);
                if (rs.next()) {
                    metrics.put("pageLifeExpectancy", rs.getLong("page_life_expectancy"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get page life expectancy: {}", e.getMessage());
            }

            // Active queries
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT
                        COUNT(*) as active_queries,
                        SUM(CASE WHEN wait_type IS NOT NULL THEN 1 ELSE 0 END) as waiting_queries
                    FROM sys.dm_exec_requests
                    WHERE session_id > 50
                    """);
                if (rs.next()) {
                    metrics.put("activeQueries", rs.getLong("active_queries"));
                    metrics.put("waitingQueries", rs.getLong("waiting_queries"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get active query stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }
    @Override protected String[] getCreateTableStatements() {
        return new String[]{
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='warehouse') CREATE TABLE warehouse (w_id INT NOT NULL, w_name VARCHAR(10), w_street_1 VARCHAR(20), w_street_2 VARCHAR(20), w_city VARCHAR(20), w_state CHAR(2), w_zip CHAR(9), w_tax DECIMAL(4,4), w_ytd DECIMAL(12,2), PRIMARY KEY (w_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='district') CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_name VARCHAR(10), d_street_1 VARCHAR(20), d_street_2 VARCHAR(20), d_city VARCHAR(20), d_state CHAR(2), d_zip CHAR(9), d_tax DECIMAL(4,4), d_ytd DECIMAL(12,2), d_next_o_id INT, PRIMARY KEY (d_w_id, d_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customer') CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, c_first VARCHAR(16), c_middle CHAR(2), c_last VARCHAR(16), c_street_1 VARCHAR(20), c_street_2 VARCHAR(20), c_city VARCHAR(20), c_state CHAR(2), c_zip CHAR(9), c_phone CHAR(16), c_since DATETIME, c_credit CHAR(2), c_credit_lim DECIMAL(12,2), c_discount DECIMAL(4,4), c_balance DECIMAL(12,2), c_ytd_payment DECIMAL(12,2), c_payment_cnt INT, c_delivery_cnt INT, c_data VARCHAR(500), PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='item') CREATE TABLE item (i_id INT NOT NULL, i_im_id INT, i_name VARCHAR(24), i_price DECIMAL(5,2), i_data VARCHAR(50), PRIMARY KEY (i_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stock') CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT, s_dist_01 CHAR(24), s_dist_02 CHAR(24), s_dist_03 CHAR(24), s_dist_04 CHAR(24), s_dist_05 CHAR(24), s_dist_06 CHAR(24), s_dist_07 CHAR(24), s_dist_08 CHAR(24), s_dist_09 CHAR(24), s_dist_10 CHAR(24), s_ytd INT, s_order_cnt INT, s_remote_cnt INT, s_data VARCHAR(50), PRIMARY KEY (s_w_id, s_i_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='history') CREATE TABLE history (h_c_id INT, h_c_d_id INT, h_c_w_id INT, h_d_id INT, h_w_id INT, h_date DATETIME, h_amount DECIMAL(6,2), h_data VARCHAR(24))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='oorder') CREATE TABLE oorder (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, PRIMARY KEY (o_w_id, o_d_id, o_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='new_order') CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='order_line') CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT, ol_supply_w_id INT, ol_delivery_d DATETIME, ol_quantity INT, ol_amount DECIMAL(6,2), ol_dist_info CHAR(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
        };
    }
}
