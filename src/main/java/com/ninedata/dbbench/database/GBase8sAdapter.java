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
public class GBase8sAdapter extends AbstractDatabaseAdapter {

    public GBase8sAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "GBase8s";
    }

    @Override
    public boolean supportsLimitSyntax() {
        return false; // GBase 8s uses FIRST n / SKIP n syntax
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Active sessions from sysmaster
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as cnt FROM sysmaster:syssessions WHERE connected > 0"
                );
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("cnt"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get GBase8s session stats: {}", e.getMessage());
            }

            // Buffer pool read/write from sysmaster
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT SUM(bufreads) as buf_reads, SUM(bufwrites) as buf_writes, " +
                    "SUM(pagreads) as pag_reads, SUM(pagwrites) as pag_writes " +
                    "FROM sysmaster:syschkio"
                );
                if (rs.next()) {
                    long bufReads = rs.getLong("buf_reads");
                    long pagReads = rs.getLong("pag_reads");
                    metrics.put("buffer_reads", bufReads);
                    metrics.put("physical_reads", pagReads);
                    if (bufReads > 0) {
                        double hitRatio = ((double)(bufReads - pagReads) / bufReads) * 100;
                        metrics.put("buffer_pool_hit_ratio", Math.round(hitRatio * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get GBase8s I/O stats: {}", e.getMessage());
            }

            // Lock waits
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as lock_waits FROM sysmaster:syslocks WHERE waiter > 0"
                );
                if (rs.next()) {
                    metrics.put("lock_waits", rs.getLong("lock_waits"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get GBase8s lock stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Disk I/O from sysmaster
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT SUM(pagesread) as pages_read, SUM(pageswritten) as pages_written " +
                    "FROM sysmaster:syschkio"
                );
                if (rs.next()) {
                    long pageSize = 4096; // GBase 8s default page size
                    metrics.put("diskReadBytes", rs.getLong("pages_read") * pageSize);
                    metrics.put("diskWriteBytes", rs.getLong("pages_written") * pageSize);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get GBase8s disk I/O: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    protected String getDropTableStatement(String tableName) {
        return "DROP TABLE IF EXISTS " + tableName;
    }

    @Override
    protected String[] getCreateTableStatements() {
        return new String[]{
            "CREATE TABLE IF NOT EXISTS warehouse (w_id INT NOT NULL, w_name VARCHAR(10), w_street_1 VARCHAR(20), w_street_2 VARCHAR(20), w_city VARCHAR(20), w_state CHAR(2), w_zip CHAR(9), w_tax DECIMAL(4,4), w_ytd DECIMAL(12,2), PRIMARY KEY (w_id))",
            "CREATE TABLE IF NOT EXISTS district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_name VARCHAR(10), d_street_1 VARCHAR(20), d_street_2 VARCHAR(20), d_city VARCHAR(20), d_state CHAR(2), d_zip CHAR(9), d_tax DECIMAL(4,4), d_ytd DECIMAL(12,2), d_next_o_id INT, PRIMARY KEY (d_w_id, d_id))",
            "CREATE TABLE IF NOT EXISTS customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, c_first VARCHAR(16), c_middle CHAR(2), c_last VARCHAR(16), c_street_1 VARCHAR(20), c_street_2 VARCHAR(20), c_city VARCHAR(20), c_state CHAR(2), c_zip CHAR(9), c_phone CHAR(16), c_since DATETIME YEAR TO SECOND, c_credit CHAR(2), c_credit_lim DECIMAL(12,2), c_discount DECIMAL(4,4), c_balance DECIMAL(12,2), c_ytd_payment DECIMAL(12,2), c_payment_cnt INT, c_delivery_cnt INT, c_data VARCHAR(500), PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "CREATE TABLE IF NOT EXISTS item (i_id INT NOT NULL, i_im_id INT, i_name VARCHAR(24), i_price DECIMAL(5,2), i_data VARCHAR(50), PRIMARY KEY (i_id))",
            "CREATE TABLE IF NOT EXISTS stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT, s_dist_01 CHAR(24), s_dist_02 CHAR(24), s_dist_03 CHAR(24), s_dist_04 CHAR(24), s_dist_05 CHAR(24), s_dist_06 CHAR(24), s_dist_07 CHAR(24), s_dist_08 CHAR(24), s_dist_09 CHAR(24), s_dist_10 CHAR(24), s_ytd INT, s_order_cnt INT, s_remote_cnt INT, s_data VARCHAR(50), PRIMARY KEY (s_w_id, s_i_id))",
            "CREATE TABLE IF NOT EXISTS history (h_c_id INT, h_c_d_id INT, h_c_w_id INT, h_d_id INT, h_w_id INT, h_date DATETIME YEAR TO SECOND, h_amount DECIMAL(6,2), h_data VARCHAR(24))",
            "CREATE TABLE IF NOT EXISTS oorder (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT, o_entry_d DATETIME YEAR TO SECOND, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, PRIMARY KEY (o_w_id, o_d_id, o_id))",
            "CREATE TABLE IF NOT EXISTS new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
            "CREATE TABLE IF NOT EXISTS order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT, ol_supply_w_id INT, ol_delivery_d DATETIME YEAR TO SECOND, ol_quantity INT, ol_amount DECIMAL(6,2), ol_dist_info CHAR(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
        };
    }

    @Override
    protected String[] getCreateIndexStatements() {
        return new String[]{
            "CREATE INDEX IF NOT EXISTS idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)",
            "CREATE INDEX IF NOT EXISTS idx_order_customer ON oorder (o_w_id, o_d_id, o_c_id, o_id)"
        };
    }
}
