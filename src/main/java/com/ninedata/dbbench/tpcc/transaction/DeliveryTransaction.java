package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.tpcc.TPCCUtil;

import java.sql.*;

public class DeliveryTransaction extends AbstractTransaction {

    public DeliveryTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        super(adapter, warehouseId, districtId);
    }

    @Override
    public String getName() {
        return "DELIVERY";
    }

    @Override
    protected boolean doExecute(Connection conn) throws SQLException {
        int carrierId = TPCCUtil.randomInt(1, 10);
        Timestamp deliveryDate = new Timestamp(System.currentTimeMillis());
        int delivered = 0;

        for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
            // Get oldest undelivered order
            int orderId;
            try (PreparedStatement ps = conn.prepareStatement("SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1 FOR UPDATE")) {
                ps.setInt(1, warehouseId);
                ps.setInt(2, d);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) continue;
                orderId = rs.getInt(1);
            }

            // Delete from new_order
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?")) {
                ps.setInt(1, warehouseId);
                ps.setInt(2, d);
                ps.setInt(3, orderId);
                ps.executeUpdate();
            }

            // Get customer ID
            int customerId;
            try (PreparedStatement ps = conn.prepareStatement("SELECT o_c_id FROM oorder WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")) {
                ps.setInt(1, warehouseId);
                ps.setInt(2, d);
                ps.setInt(3, orderId);
                ResultSet rs = ps.executeQuery();
                if (!rs.next()) continue;
                customerId = rs.getInt(1);
            }

            // Update order carrier
            try (PreparedStatement ps = conn.prepareStatement("UPDATE oorder SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")) {
                ps.setInt(1, carrierId);
                ps.setInt(2, warehouseId);
                ps.setInt(3, d);
                ps.setInt(4, orderId);
                ps.executeUpdate();
            }

            // Update order lines and get total amount
            double totalAmount = 0;
            try (PreparedStatement ps = conn.prepareStatement("SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")) {
                ps.setInt(1, warehouseId);
                ps.setInt(2, d);
                ps.setInt(3, orderId);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    totalAmount = rs.getDouble(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("UPDATE order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")) {
                ps.setTimestamp(1, deliveryDate);
                ps.setInt(2, warehouseId);
                ps.setInt(3, d);
                ps.setInt(4, orderId);
                ps.executeUpdate();
            }

            // Update customer balance
            try (PreparedStatement ps = conn.prepareStatement("UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")) {
                ps.setDouble(1, totalAmount);
                ps.setInt(2, warehouseId);
                ps.setInt(3, d);
                ps.setInt(4, customerId);
                ps.executeUpdate();
            }

            delivered++;
        }

        return delivered > 0;
    }
}
