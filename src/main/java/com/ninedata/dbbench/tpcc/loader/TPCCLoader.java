package com.ninedata.dbbench.tpcc.loader;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.tpcc.TPCCUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
public class TPCCLoader {
    private final DatabaseAdapter adapter;
    private final int warehouses;
    private final int concurrency;
    private Consumer<String> progressCallback;
    private final AtomicInteger completedWarehouses = new AtomicInteger(0);
    private final boolean isDB2;

    public TPCCLoader(DatabaseAdapter adapter, int warehouses) {
        this(adapter, warehouses, 4);
    }

    public TPCCLoader(DatabaseAdapter adapter, int warehouses, int concurrency) {
        this.adapter = adapter;
        this.warehouses = warehouses;
        this.concurrency = Math.max(1, Math.min(concurrency, warehouses));
        // Check if this is DB2 database (needs special batch handling)
        this.isDB2 = "DB2".equalsIgnoreCase(adapter.getDatabaseType());
    }

    public void setProgressCallback(Consumer<String> callback) {
        this.progressCallback = callback;
    }

    private void reportProgress(String message) {
        log.info(message);
        if (progressCallback != null) {
            progressCallback.accept(message);
        }
    }

    /**
     * Safely execute batch with proper error handling for DB2
     * DB2 JDBC driver throws BatchUpdateException even when batch partially succeeds
     */
    private void executeBatchSafely(PreparedStatement ps) throws SQLException {
        try {
            ps.executeBatch();
        } catch (BatchUpdateException e) {
            // For DB2, check if it's a partial success (ERRORCODE=-4229)
            if (isDB2) {
                // Log the warning but continue - DB2 may have committed some rows
                log.warn("DB2 batch warning (some rows may have succeeded): {}", e.getMessage());
                // Get the actual exception details
                SQLException nextEx = e.getNextException();
                while (nextEx != null) {
                    log.debug("  Batch detail: {}", nextEx.getMessage());
                    nextEx = nextEx.getNextException();
                }
            } else {
                throw e;
            }
        }
        ps.clearBatch();
    }

    public void load() throws SQLException {
        long start = System.currentTimeMillis();
        reportProgress(String.format("Starting TPC-C data load for %d warehouse(s) with %d concurrent threads...",
                warehouses, concurrency));

        // Load items first (shared across all warehouses)
        loadItems();

        // Load warehouses concurrently
        loadWarehousesConcurrently();

        long elapsed = (System.currentTimeMillis() - start) / 1000;
        reportProgress("Data load completed in " + elapsed + " seconds");
    }

    private void loadWarehousesConcurrently() throws SQLException {
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<Void>> futures = new ArrayList<>();
        completedWarehouses.set(0);

        reportProgress(String.format("Loading %d warehouses with %d parallel threads...", warehouses, concurrency));

        for (int w = 1; w <= warehouses; w++) {
            final int warehouseId = w;
            futures.add(executor.submit(() -> {
                try {
                    loadSingleWarehouse(warehouseId);
                    int completed = completedWarehouses.incrementAndGet();
                    reportProgress(String.format("Warehouse %d completed (%d/%d)", warehouseId, completed, warehouses));
                } catch (SQLException e) {
                    log.error("Failed to load warehouse {}: {}", warehouseId, e.getMessage());
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }

        // Wait for all warehouses to complete
        executor.shutdown();
        try {
            for (Future<Void> future : futures) {
                future.get();
            }
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error during concurrent warehouse loading: {}", e.getMessage());
            throw new SQLException("Failed to load warehouses concurrently", e);
        }
    }

    private void loadSingleWarehouse(int wId) throws SQLException {
        loadWarehouse(wId);
        loadDistricts(wId);
        loadCustomers(wId);
        loadStock(wId);
        loadOrders(wId);
    }

    private void loadItems() throws SQLException {
        reportProgress("Loading items...");
        int batchSize = isDB2 ? 1000 : 10000; // Smaller batch for DB2
        try (Connection conn = adapter.getConnection()) {
            String sql = "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 1; i <= TPCCUtil.ITEMS; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, TPCCUtil.randomInt(1, 10000));
                    ps.setString(3, TPCCUtil.randomString(14, 24));
                    ps.setDouble(4, TPCCUtil.randomDouble(1.00, 100.00));
                    String data = TPCCUtil.randomString(26, 50);
                    if (TPCCUtil.randomInt(1, 100) <= 10) {
                        int pos = TPCCUtil.randomInt(0, data.length() - 8);
                        data = data.substring(0, pos) + "ORIGINAL" + data.substring(pos + 8);
                    }
                    ps.setString(5, data);
                    ps.addBatch();
                    if (i % batchSize == 0) {
                        executeBatchSafely(ps);
                        reportProgress("  Loaded " + i + " items");
                    }
                }
                executeBatchSafely(ps);
            }
            conn.commit();
        }
        reportProgress("Items loaded: " + TPCCUtil.ITEMS);
    }

    private void loadWarehouse(int wId) throws SQLException {
        try (Connection conn = adapter.getConnection()) {
            String sql = "INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, wId);
                ps.setString(2, TPCCUtil.randomString(6, 10));
                ps.setString(3, TPCCUtil.randomString(10, 20));
                ps.setString(4, TPCCUtil.randomString(10, 20));
                ps.setString(5, TPCCUtil.randomString(10, 20));
                ps.setString(6, TPCCUtil.randomString(2, 2).toUpperCase());
                ps.setString(7, TPCCUtil.randomZip());
                ps.setDouble(8, TPCCUtil.randomDouble(0.0, 0.2));
                ps.setDouble(9, 300000.00);
                ps.executeUpdate();
            }
            conn.commit();
        }
    }

    private void loadDistricts(int wId) throws SQLException {
        try (Connection conn = adapter.getConnection()) {
            String sql = "INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
                    ps.setInt(1, d);
                    ps.setInt(2, wId);
                    ps.setString(3, TPCCUtil.randomString(6, 10));
                    ps.setString(4, TPCCUtil.randomString(10, 20));
                    ps.setString(5, TPCCUtil.randomString(10, 20));
                    ps.setString(6, TPCCUtil.randomString(10, 20));
                    ps.setString(7, TPCCUtil.randomString(2, 2).toUpperCase());
                    ps.setString(8, TPCCUtil.randomZip());
                    ps.setDouble(9, TPCCUtil.randomDouble(0.0, 0.2));
                    ps.setDouble(10, 30000.00);
                    ps.setInt(11, TPCCUtil.ORDERS_PER_DISTRICT + 1);
                    ps.addBatch();
                }
                executeBatchSafely(ps);
            }
            conn.commit();
        }
    }

    private void loadCustomers(int wId) throws SQLException {
        int batchSize = isDB2 ? 500 : 1000; // Smaller batch for DB2
        try (Connection conn = adapter.getConnection()) {
            String custSql = "INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String histSql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement custPs = conn.prepareStatement(custSql);
                 PreparedStatement histPs = conn.prepareStatement(histSql)) {

                for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
                    for (int c = 1; c <= TPCCUtil.CUSTOMERS_PER_DISTRICT; c++) {
                        String lastName = c <= 1000 ? TPCCUtil.generateLastName(c - 1) : TPCCUtil.generateLastName(TPCCUtil.NURand(255, 0, 999));

                        custPs.setInt(1, c);
                        custPs.setInt(2, d);
                        custPs.setInt(3, wId);
                        custPs.setString(4, TPCCUtil.randomString(8, 16));
                        custPs.setString(5, "OE");
                        custPs.setString(6, lastName);
                        custPs.setString(7, TPCCUtil.randomString(10, 20));
                        custPs.setString(8, TPCCUtil.randomString(10, 20));
                        custPs.setString(9, TPCCUtil.randomString(10, 20));
                        custPs.setString(10, TPCCUtil.randomString(2, 2).toUpperCase());
                        custPs.setString(11, TPCCUtil.randomZip());
                        custPs.setString(12, TPCCUtil.randomNumericString(16));
                        custPs.setTimestamp(13, new Timestamp(System.currentTimeMillis()));
                        custPs.setString(14, TPCCUtil.randomInt(1, 100) <= 10 ? "BC" : "GC");
                        custPs.setDouble(15, 50000.00);
                        custPs.setDouble(16, TPCCUtil.randomDouble(0.0, 0.5));
                        custPs.setDouble(17, -10.00);
                        custPs.setDouble(18, 10.00);
                        custPs.setInt(19, 1);
                        custPs.setInt(20, 0);
                        custPs.setString(21, TPCCUtil.randomString(300, 500));
                        custPs.addBatch();

                        histPs.setInt(1, c);
                        histPs.setInt(2, d);
                        histPs.setInt(3, wId);
                        histPs.setInt(4, d);
                        histPs.setInt(5, wId);
                        histPs.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
                        histPs.setDouble(7, 10.00);
                        histPs.setString(8, TPCCUtil.randomString(12, 24));
                        histPs.addBatch();

                        if (c % batchSize == 0) {
                            executeBatchSafely(custPs);
                            executeBatchSafely(histPs);
                        }
                    }
                    executeBatchSafely(custPs);
                    executeBatchSafely(histPs);
                }
            }
            conn.commit();
        }
    }

    private void loadStock(int wId) throws SQLException {
        int batchSize = isDB2 ? 1000 : 10000; // Smaller batch for DB2
        try (Connection conn = adapter.getConnection()) {
            String sql = "INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 1; i <= TPCCUtil.ITEMS; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, wId);
                    ps.setInt(3, TPCCUtil.randomInt(10, 100));
                    for (int j = 4; j <= 13; j++) {
                        ps.setString(j, TPCCUtil.randomString(24, 24));
                    }
                    ps.setInt(14, 0);
                    ps.setInt(15, 0);
                    ps.setInt(16, 0);
                    String data = TPCCUtil.randomString(26, 50);
                    if (TPCCUtil.randomInt(1, 100) <= 10) {
                        int pos = TPCCUtil.randomInt(0, data.length() - 8);
                        data = data.substring(0, pos) + "ORIGINAL" + data.substring(pos + 8);
                    }
                    ps.setString(17, data);
                    ps.addBatch();
                    if (i % batchSize == 0) {
                        executeBatchSafely(ps);
                    }
                }
                executeBatchSafely(ps);
            }
            conn.commit();
        }
    }

    private void loadOrders(int wId) throws SQLException {
        int batchSize = isDB2 ? 500 : 1000; // Smaller batch for DB2
        try (Connection conn = adapter.getConnection()) {
            String orderSql = "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            String newOrderSql = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)";
            String olSql = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            int[] customerIds = new int[TPCCUtil.CUSTOMERS_PER_DISTRICT];
            for (int i = 0; i < customerIds.length; i++) customerIds[i] = i + 1;

            try (PreparedStatement orderPs = conn.prepareStatement(orderSql);
                 PreparedStatement newOrderPs = conn.prepareStatement(newOrderSql);
                 PreparedStatement olPs = conn.prepareStatement(olSql)) {

                for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
                    // Shuffle customer IDs
                    for (int i = customerIds.length - 1; i > 0; i--) {
                        int j = TPCCUtil.randomInt(0, i);
                        int temp = customerIds[i];
                        customerIds[i] = customerIds[j];
                        customerIds[j] = temp;
                    }

                    for (int o = 1; o <= TPCCUtil.ORDERS_PER_DISTRICT; o++) {
                        int olCnt = TPCCUtil.randomInt(5, 15);

                        orderPs.setInt(1, o);
                        orderPs.setInt(2, d);
                        orderPs.setInt(3, wId);
                        orderPs.setInt(4, customerIds[o - 1]);
                        orderPs.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
                        if (o < 2101) {
                            orderPs.setInt(6, TPCCUtil.randomInt(1, 10));
                        } else {
                            orderPs.setNull(6, Types.INTEGER);
                        }
                        orderPs.setInt(7, olCnt);
                        orderPs.setInt(8, 1);
                        orderPs.addBatch();

                        if (o >= 2101) {
                            newOrderPs.setInt(1, o);
                            newOrderPs.setInt(2, d);
                            newOrderPs.setInt(3, wId);
                            newOrderPs.addBatch();
                        }

                        for (int ol = 1; ol <= olCnt; ol++) {
                            olPs.setInt(1, o);
                            olPs.setInt(2, d);
                            olPs.setInt(3, wId);
                            olPs.setInt(4, ol);
                            olPs.setInt(5, TPCCUtil.randomInt(1, TPCCUtil.ITEMS));
                            olPs.setInt(6, wId);
                            if (o < 2101) {
                                olPs.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
                            } else {
                                olPs.setNull(7, Types.TIMESTAMP);
                            }
                            olPs.setInt(8, 5);
                            olPs.setDouble(9, o < 2101 ? 0.00 : TPCCUtil.randomDouble(0.01, 9999.99));
                            olPs.setString(10, TPCCUtil.randomString(24, 24));
                            olPs.addBatch();
                        }

                        if (o % batchSize == 0) {
                            executeBatchSafely(orderPs);
                            executeBatchSafely(newOrderPs);
                            executeBatchSafely(olPs);
                        }
                    }
                    executeBatchSafely(orderPs);
                    executeBatchSafely(newOrderPs);
                    executeBatchSafely(olPs);
                }
            }
            conn.commit();
        }
    }
}
