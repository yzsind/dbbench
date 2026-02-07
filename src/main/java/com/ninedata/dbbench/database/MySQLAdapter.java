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
public class MySQLAdapter extends AbstractDatabaseAdapter {

    public MySQLAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "MySQL";
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // InnoDB metrics
            ResultSet rs = stmt.executeQuery("SHOW GLOBAL STATUS LIKE 'Innodb%'");
            while (rs.next()) {
                String name = rs.getString(1);
                String value = rs.getString(2);
                switch (name) {
                    case "Innodb_buffer_pool_read_requests" -> metrics.put("buffer_pool_reads", Long.parseLong(value));
                    case "Innodb_buffer_pool_reads" -> metrics.put("buffer_pool_read_misses", Long.parseLong(value));
                    case "Innodb_row_lock_waits" -> metrics.put("row_lock_waits", Long.parseLong(value));
                    case "Innodb_row_lock_time" -> metrics.put("row_lock_time_ms", Long.parseLong(value));
                    case "Innodb_rows_read" -> metrics.put("rows_read", Long.parseLong(value));
                    case "Innodb_rows_inserted" -> metrics.put("rows_inserted", Long.parseLong(value));
                    case "Innodb_rows_updated" -> metrics.put("rows_updated", Long.parseLong(value));
                    case "Innodb_rows_deleted" -> metrics.put("rows_deleted", Long.parseLong(value));
                }
            }
            rs.close();

            // Connection and query metrics
            rs = stmt.executeQuery("SHOW GLOBAL STATUS WHERE Variable_name IN ('Connections', 'Threads_connected', 'Threads_running', 'Queries', 'Slow_queries', 'Questions')");
            while (rs.next()) {
                String name = rs.getString(1);
                String value = rs.getString(2);
                switch (name) {
                    case "Connections" -> metrics.put("total_connections", Long.parseLong(value));
                    case "Threads_connected" -> metrics.put("active_connections", Long.parseLong(value));
                    case "Threads_running" -> metrics.put("running_threads", Long.parseLong(value));
                    case "Queries" -> metrics.put("total_queries", Long.parseLong(value));
                    case "Slow_queries" -> metrics.put("slow_queries", Long.parseLong(value));
                }
            }
            rs.close();

            // Calculate buffer pool hit ratio
            long reads = (Long) metrics.getOrDefault("buffer_pool_reads", 0L);
            long misses = (Long) metrics.getOrDefault("buffer_pool_read_misses", 0L);
            if (reads > 0) {
                double hitRatio = ((double) (reads - misses) / reads) * 100;
                metrics.put("buffer_pool_hit_ratio", Math.round(hitRatio * 100.0) / 100.0);
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // MySQL doesn't have direct OS metrics, but we can get some I/O stats
            // from performance_schema if available
            try {
                // File I/O statistics
                ResultSet rs = stmt.executeQuery("""
                    SELECT SUM(SUM_NUMBER_OF_BYTES_READ) as bytes_read,
                           SUM(SUM_NUMBER_OF_BYTES_WRITE) as bytes_write
                    FROM performance_schema.file_summary_by_event_name
                    WHERE EVENT_NAME LIKE 'wait/io/file/%'
                """);
                if (rs.next()) {
                    metrics.put("diskReadBytes", rs.getLong("bytes_read"));
                    metrics.put("diskWriteBytes", rs.getLong("bytes_write"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get file I/O stats: {}", e.getMessage());
            }

            // Network I/O from performance_schema (if available)
            try {
                ResultSet rs = stmt.executeQuery("""
                    SELECT SUM(SUM_NUMBER_OF_BYTES_READ) as bytes_recv,
                           SUM(SUM_NUMBER_OF_BYTES_WRITE) as bytes_sent
                    FROM performance_schema.socket_summary_by_event_name
                """);
                if (rs.next()) {
                    metrics.put("networkRecvBytes", rs.getLong("bytes_recv"));
                    metrics.put("networkSentBytes", rs.getLong("bytes_sent"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get network I/O stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    protected String[] getCreateTableStatements() {
        return new String[]{
            """
            CREATE TABLE IF NOT EXISTS warehouse (
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
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS district (
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
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS customer (
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
                c_since DATETIME,
                c_credit CHAR(2),
                c_credit_lim DECIMAL(12,2),
                c_discount DECIMAL(4,4),
                c_balance DECIMAL(12,2),
                c_ytd_payment DECIMAL(12,2),
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data VARCHAR(500),
                PRIMARY KEY (c_w_id, c_d_id, c_id),
                INDEX idx_customer_name (c_w_id, c_d_id, c_last, c_first)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS item (
                i_id INT NOT NULL,
                i_im_id INT,
                i_name VARCHAR(24),
                i_price DECIMAL(5,2),
                i_data VARCHAR(50),
                PRIMARY KEY (i_id)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS stock (
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
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS history (
                h_c_id INT,
                h_c_d_id INT,
                h_c_w_id INT,
                h_d_id INT,
                h_w_id INT,
                h_date DATETIME,
                h_amount DECIMAL(6,2),
                h_data VARCHAR(24)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS oorder (
                o_id INT NOT NULL,
                o_d_id INT NOT NULL,
                o_w_id INT NOT NULL,
                o_c_id INT,
                o_entry_d DATETIME,
                o_carrier_id INT,
                o_ol_cnt INT,
                o_all_local INT,
                PRIMARY KEY (o_w_id, o_d_id, o_id),
                INDEX idx_order_customer (o_w_id, o_d_id, o_c_id, o_id)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS new_order (
                no_o_id INT NOT NULL,
                no_d_id INT NOT NULL,
                no_w_id INT NOT NULL,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE IF NOT EXISTS order_line (
                ol_o_id INT NOT NULL,
                ol_d_id INT NOT NULL,
                ol_w_id INT NOT NULL,
                ol_number INT NOT NULL,
                ol_i_id INT,
                ol_supply_w_id INT,
                ol_delivery_d DATETIME,
                ol_quantity INT,
                ol_amount DECIMAL(6,2),
                ol_dist_info CHAR(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            ) ENGINE=InnoDB
            """
        };
    }
}
