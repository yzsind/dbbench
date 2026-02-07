package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.tpcc.TPCCUtil;

import java.sql.*;

public class NewOrderTransaction extends AbstractTransaction {

    public NewOrderTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        super(adapter, warehouseId, districtId);
    }

    @Override
    public String getName() {
        return "NEW_ORDER";
    }

    @Override
    protected boolean doExecute(Connection conn) throws SQLException {
        int customerId = TPCCUtil.NURand(1023, 1, TPCCUtil.CUSTOMERS_PER_DISTRICT);
        int orderLineCount = TPCCUtil.randomInt(5, 15);
        int[] itemIds = new int[orderLineCount];
        int[] supplyWIds = new int[orderLineCount];
        int[] quantities = new int[orderLineCount];

        for (int i = 0; i < orderLineCount; i++) {
            itemIds[i] = TPCCUtil.NURand(8191, 1, TPCCUtil.ITEMS);
            supplyWIds[i] = warehouseId;
            quantities[i] = TPCCUtil.randomInt(1, 10);
        }

        // 1% invalid item to test rollback
        if (TPCCUtil.randomInt(1, 100) == 1) {
            itemIds[orderLineCount - 1] = TPCCUtil.ITEMS + 1;
        }

        // Get warehouse tax
        double wTax;
        try (PreparedStatement ps = conn.prepareStatement("SELECT w_tax FROM warehouse WHERE w_id = ?")) {
            ps.setInt(1, warehouseId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            wTax = rs.getDouble(1);
        }

        // Get district info and update next order ID
        double dTax;
        int orderId;
        String districtSql = buildSelectForUpdateQuery("SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?");
        try (PreparedStatement ps = conn.prepareStatement(districtSql)) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            dTax = rs.getDouble(1);
            orderId = rs.getInt(2);
        }

        try (PreparedStatement ps = conn.prepareStatement("UPDATE district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?")) {
            ps.setInt(1, orderId + 1);
            ps.setInt(2, warehouseId);
            ps.setInt(3, districtId);
            ps.executeUpdate();
        }

        // Get customer discount
        double cDiscount;
        try (PreparedStatement ps = conn.prepareStatement("SELECT c_discount FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ps.setInt(3, customerId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            cDiscount = rs.getDouble(1);
        }

        // Insert order
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, NULL, ?, 1)")) {
            ps.setInt(1, orderId);
            ps.setInt(2, districtId);
            ps.setInt(3, warehouseId);
            ps.setInt(4, customerId);
            ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            ps.setInt(6, orderLineCount);
            ps.executeUpdate();
        }

        // Insert new_order
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)")) {
            ps.setInt(1, orderId);
            ps.setInt(2, districtId);
            ps.setInt(3, warehouseId);
            ps.executeUpdate();
        }

        // Process order lines
        for (int i = 0; i < orderLineCount; i++) {
            // Get item
            double iPrice;
            try (PreparedStatement ps = conn.prepareStatement("SELECT i_price, i_name, i_data FROM item WHERE i_id = ?")) {
                ps.setInt(1, itemIds[i]);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    conn.rollback();
                    return false; // Invalid item - rollback
                }
                iPrice = rs.getDouble(1);
            }

            // Get and update stock
            int sQuantity;
            String sDistInfo;
            String stockSql = buildSelectForUpdateQuery("SELECT s_quantity, s_dist_" + String.format("%02d", districtId) + ", s_data FROM stock WHERE s_w_id = ? AND s_i_id = ?");
            try (PreparedStatement ps = conn.prepareStatement(stockSql)) {
                ps.setInt(1, supplyWIds[i]);
                ps.setInt(2, itemIds[i]);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) return false;
                sQuantity = rs.getInt(1);
                sDistInfo = rs.getString(2);
            }

            int newQuantity = sQuantity - quantities[i];
            if (newQuantity < 10) newQuantity += 91;

            try (PreparedStatement ps = conn.prepareStatement("UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = ? AND s_i_id = ?")) {
                ps.setInt(1, newQuantity);
                ps.setInt(2, quantities[i]);
                ps.setInt(3, supplyWIds[i]);
                ps.setInt(4, itemIds[i]);
                ps.executeUpdate();
            }

            // Insert order line
            double olAmount = quantities[i] * iPrice;
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)")) {
                ps.setInt(1, orderId);
                ps.setInt(2, districtId);
                ps.setInt(3, warehouseId);
                ps.setInt(4, i + 1);
                ps.setInt(5, itemIds[i]);
                ps.setInt(6, supplyWIds[i]);
                ps.setInt(7, quantities[i]);
                ps.setDouble(8, olAmount);
                ps.setString(9, sDistInfo);
                ps.executeUpdate();
            }
        }

        return true;
    }
}
