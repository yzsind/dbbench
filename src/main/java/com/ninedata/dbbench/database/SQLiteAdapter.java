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
public class SQLiteAdapter extends AbstractDatabaseAdapter {

    public SQLiteAdapter(DatabaseConfig config) {
        super(config);
    }

    @Override
    public String getDatabaseType() {
        return "SQLite";
    }

    @Override
    public boolean supportsForUpdate() {
        return false;
    }

    @Override
    public void initialize() throws SQLException {
        super.initialize();
        // Enable WAL mode for better concurrent read/write performance
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA busy_timeout=5000");
            //conn.commit();
        }
        log.info("SQLite WAL mode enabled");
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        Map<String, Object> metrics = new HashMap<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Cache hit ratio
            try {
                ResultSet rs = stmt.executeQuery("PRAGMA cache_stats");
                if (rs.next()) {
                    long hits = rs.getLong("hit");
                    long misses = rs.getLong("miss");
                    if (hits + misses > 0) {
                        double hitRatio = (hits * 100.0) / (hits + misses);
                        metrics.put("buffer_pool_hit_ratio", Math.round(hitRatio * 100.0) / 100.0);
                    }
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get SQLite cache stats: {}", e.getMessage());
            }

            // Database page stats
            try {
                ResultSet rs = stmt.executeQuery("PRAGMA page_count");
                if (rs.next()) {
                    metrics.put("page_count", rs.getLong(1));
                }
                rs.close();
                rs = stmt.executeQuery("PRAGMA page_size");
                if (rs.next()) {
                    metrics.put("page_size", rs.getLong(1));
                }
                rs.close();
                rs = stmt.executeQuery("PRAGMA freelist_count");
                if (rs.next()) {
                    metrics.put("freelist_count", rs.getLong(1));
                }
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not get SQLite page stats: {}", e.getMessage());
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
                w_id INTEGER NOT NULL, w_name TEXT, w_street_1 TEXT, w_street_2 TEXT,
                w_city TEXT, w_state TEXT, w_zip TEXT, w_tax REAL, w_ytd REAL,
                PRIMARY KEY (w_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS district (
                d_id INTEGER NOT NULL, d_w_id INTEGER NOT NULL, d_name TEXT,
                d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, d_state TEXT, d_zip TEXT,
                d_tax REAL, d_ytd REAL, d_next_o_id INTEGER,
                PRIMARY KEY (d_w_id, d_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS customer (
                c_id INTEGER NOT NULL, c_d_id INTEGER NOT NULL, c_w_id INTEGER NOT NULL,
                c_first TEXT, c_middle TEXT, c_last TEXT, c_street_1 TEXT, c_street_2 TEXT,
                c_city TEXT, c_state TEXT, c_zip TEXT, c_phone TEXT, c_since TEXT,
                c_credit TEXT, c_credit_lim REAL, c_discount REAL, c_balance REAL,
                c_ytd_payment REAL, c_payment_cnt INTEGER, c_delivery_cnt INTEGER, c_data TEXT,
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)",
            """
            CREATE TABLE IF NOT EXISTS item (
                i_id INTEGER NOT NULL, i_im_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT,
                PRIMARY KEY (i_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stock (
                s_i_id INTEGER NOT NULL, s_w_id INTEGER NOT NULL, s_quantity INTEGER,
                s_dist_01 TEXT, s_dist_02 TEXT, s_dist_03 TEXT, s_dist_04 TEXT, s_dist_05 TEXT,
                s_dist_06 TEXT, s_dist_07 TEXT, s_dist_08 TEXT, s_dist_09 TEXT, s_dist_10 TEXT,
                s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT,
                PRIMARY KEY (s_w_id, s_i_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS history (
                h_c_id INTEGER, h_c_d_id INTEGER, h_c_w_id INTEGER, h_d_id INTEGER,
                h_w_id INTEGER, h_date TEXT, h_amount REAL, h_data TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS oorder (
                o_id INTEGER NOT NULL, o_d_id INTEGER NOT NULL, o_w_id INTEGER NOT NULL,
                o_c_id INTEGER, o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER,
                o_all_local INTEGER, PRIMARY KEY (o_w_id, o_d_id, o_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_order_customer ON oorder (o_w_id, o_d_id, o_c_id, o_id)",
            """
            CREATE TABLE IF NOT EXISTS new_order (
                no_o_id INTEGER NOT NULL, no_d_id INTEGER NOT NULL, no_w_id INTEGER NOT NULL,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS order_line (
                ol_o_id INTEGER NOT NULL, ol_d_id INTEGER NOT NULL, ol_w_id INTEGER NOT NULL,
                ol_number INTEGER NOT NULL, ol_i_id INTEGER, ol_supply_w_id INTEGER,
                ol_delivery_d TEXT, ol_quantity INTEGER, ol_amount REAL, ol_dist_info TEXT,
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            )
            """
        };
    }
}
