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
public class SybaseAdapter extends AbstractDatabaseAdapter {

    public SybaseAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "Sybase";
    }

    @Override
    public boolean supportsLimitSyntax() {
        return false; // Sybase ASE uses TOP n
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Active connections
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as cnt FROM master..sysprocesses WHERE status = 'runnable'"
                );
                if (rs.next()) {
                    metrics.put("active_connections", rs.getLong("cnt"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Sybase session stats: {}", e.getMessage());
            }

            // Total connections
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as cnt FROM master..sysprocesses"
                );
                if (rs.next()) {
                    metrics.put("total_connections", rs.getLong("cnt"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Sybase total connections: {}", e.getMessage());
            }

            // Cache hit ratio from sysmonitors
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT (1.0 - CONVERT(FLOAT, phys.value) / NULLIF(logi.value, 0)) * 100 as hit_ratio " +
                    "FROM master..sysmonitors phys, master..sysmonitors logi " +
                    "WHERE phys.field_name = 'bufs_pread' AND logi.field_name = 'bufs_lread'"
                );
                if (rs.next()) {
                    double hitRatio = rs.getDouble("hit_ratio");
                    if (!rs.wasNull() && hitRatio >= 0) {
                        metrics.put("buffer_pool_hit_ratio", Math.round(hitRatio * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Sybase cache stats: {}", e.getMessage());
            }

            // Lock waits
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) as lock_waits FROM master..syslocks WHERE type > 0"
                );
                if (rs.next()) {
                    metrics.put("lock_waits", rs.getLong("lock_waits"));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Sybase lock stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    public Map<String, Object> collectHostMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Engine CPU usage from sysmonitors
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT field_name, value FROM master..sysmonitors " +
                    "WHERE field_name IN ('cpu_busy', 'io_busy', 'cpu_idle')"
                );
                long cpuBusy = 0, cpuIdle = 0;
                while (rs.next()) {
                    String name = rs.getString("field_name");
                    long value = rs.getLong("value");
                    switch (name) {
                        case "cpu_busy" -> cpuBusy = value;
                        case "cpu_idle" -> cpuIdle = value;
                    }
                }
                if (cpuBusy + cpuIdle > 0) {
                    double cpuUsage = (cpuBusy * 100.0) / (cpuBusy + cpuIdle);
                    metrics.put("cpuUsage", Math.round(cpuUsage * 100.0) / 100.0);
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get Sybase CPU stats: {}", e.getMessage());
            }

            conn.commit();
        }
        return metrics;
    }

    @Override
    protected String[] getCreateTableStatements() {
        return new String[]{
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='warehouse') CREATE TABLE warehouse (w_id INT NOT NULL, w_name VARCHAR(10) NULL, w_street_1 VARCHAR(20) NULL, w_street_2 VARCHAR(20) NULL, w_city VARCHAR(20) NULL, w_state CHAR(2) NULL, w_zip CHAR(9) NULL, w_tax DECIMAL(4,4) NULL, w_ytd DECIMAL(12,2) NULL, PRIMARY KEY (w_id))",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='district') CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_name VARCHAR(10) NULL, d_street_1 VARCHAR(20) NULL, d_street_2 VARCHAR(20) NULL, d_city VARCHAR(20) NULL, d_state CHAR(2) NULL, d_zip CHAR(9) NULL, d_tax DECIMAL(4,4) NULL, d_ytd DECIMAL(12,2) NULL, d_next_o_id INT NULL, PRIMARY KEY (d_w_id, d_id))",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='customer') CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, c_first VARCHAR(16) NULL, c_middle CHAR(2) NULL, c_last VARCHAR(16) NULL, c_street_1 VARCHAR(20) NULL, c_street_2 VARCHAR(20) NULL, c_city VARCHAR(20) NULL, c_state CHAR(2) NULL, c_zip CHAR(9) NULL, c_phone CHAR(16) NULL, c_since DATETIME NULL, c_credit CHAR(2) NULL, c_credit_lim DECIMAL(12,2) NULL, c_discount DECIMAL(4,4) NULL, c_balance DECIMAL(12,2) NULL, c_ytd_payment DECIMAL(12,2) NULL, c_payment_cnt INT NULL, c_delivery_cnt INT NULL, c_data VARCHAR(500) NULL, PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='item') CREATE TABLE item (i_id INT NOT NULL, i_im_id INT NULL, i_name VARCHAR(24) NULL, i_price DECIMAL(5,2) NULL, i_data VARCHAR(50) NULL, PRIMARY KEY (i_id))",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='stock') CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT NULL, s_dist_01 CHAR(24) NULL, s_dist_02 CHAR(24) NULL, s_dist_03 CHAR(24) NULL, s_dist_04 CHAR(24) NULL, s_dist_05 CHAR(24) NULL, s_dist_06 CHAR(24) NULL, s_dist_07 CHAR(24) NULL, s_dist_08 CHAR(24) NULL, s_dist_09 CHAR(24) NULL, s_dist_10 CHAR(24) NULL, s_ytd INT NULL, s_order_cnt INT NULL, s_remote_cnt INT NULL, s_data VARCHAR(50) NULL, PRIMARY KEY (s_w_id, s_i_id))",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='history') CREATE TABLE history (h_c_id INT NULL, h_c_d_id INT NULL, h_c_w_id INT NULL, h_d_id INT NULL, h_w_id INT NULL, h_date DATETIME NULL, h_amount DECIMAL(6,2) NULL, h_data VARCHAR(24) NULL)",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='oorder') CREATE TABLE oorder (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT NULL, o_entry_d DATETIME NULL, o_carrier_id INT NULL, o_ol_cnt INT NULL, o_all_local INT NULL, PRIMARY KEY (o_w_id, o_d_id, o_id))",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='new_order') CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
            "IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='order_line') CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT NULL, ol_supply_w_id INT NULL, ol_delivery_d DATETIME NULL, ol_quantity INT NULL, ol_amount DECIMAL(6,2) NULL, ol_dist_info CHAR(24) NULL, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
        };
    }

    @Override
    protected String[] getCreateIndexStatements() {
        return new String[]{
            "IF NOT EXISTS (SELECT 1 FROM sysindexes WHERE name='idx_customer_name') CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)",
            "IF NOT EXISTS (SELECT 1 FROM sysindexes WHERE name='idx_order_customer') CREATE INDEX idx_order_customer ON oorder (o_w_id, o_d_id, o_c_id, o_id)"
        };
    }
}
