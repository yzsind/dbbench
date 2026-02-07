package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.tpcc.TPCCUtil;

import java.sql.*;

public class OrderStatusTransaction extends AbstractTransaction {

    public OrderStatusTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        super(adapter, warehouseId, districtId);
    }

    @Override
    public String getName() {
        return "ORDER_STATUS";
    }

    @Override
    protected boolean doExecute(Connection conn) throws SQLException {
        int customerId;
        boolean byName = TPCCUtil.randomInt(1, 100) <= 60;

        if (byName) {
            String lastName = TPCCUtil.generateLastName(TPCCUtil.NURand(255, 0, 999));
            try (PreparedStatement ps = conn.prepareStatement("SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first")) {
                ps.setInt(1, warehouseId);
                ps.setInt(2, districtId);
                ps.setString(3, lastName);
                ResultSet rs = ps.executeQuery();
                int count = 0;
                int[] ids = new int[100];
                while (rs.next() && count < 100) {
                    ids[count++] = rs.getInt(1);
                }
                if (count == 0) return false;
                customerId = ids[(count + 1) / 2 - 1];
            }
        } else {
            customerId = TPCCUtil.NURand(1023, 1, TPCCUtil.CUSTOMERS_PER_DISTRICT);
        }

        // Get customer info
        try (PreparedStatement ps = conn.prepareStatement("SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ps.setInt(3, customerId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
        }

        // Get last order
        int orderId;
        try (PreparedStatement ps = conn.prepareStatement("SELECT o_id, o_entry_d, o_carrier_id FROM oorder WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1")) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ps.setInt(3, customerId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            orderId = rs.getInt(1);
        }

        // Get order lines
        try (PreparedStatement ps = conn.prepareStatement("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?")) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ps.setInt(3, orderId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                // Read order line data
            }
        }

        return true;
    }
}
