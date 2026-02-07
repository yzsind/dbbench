package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.tpcc.TPCCUtil;

import java.sql.*;

public class StockLevelTransaction extends AbstractTransaction {

    public StockLevelTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        super(adapter, warehouseId, districtId);
    }

    @Override
    public String getName() {
        return "STOCK_LEVEL";
    }

    @Override
    protected boolean doExecute(Connection conn) throws SQLException {
        int threshold = TPCCUtil.randomInt(10, 20);

        // Get next order ID
        int nextOrderId;
        try (PreparedStatement ps = conn.prepareStatement("SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?")) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            nextOrderId = rs.getInt(1);
        }

        // Count items below threshold
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT COUNT(DISTINCT s_i_id) FROM stock, order_line
            WHERE s_w_id = ? AND ol_w_id = ? AND ol_d_id = ?
            AND ol_o_id < ? AND ol_o_id >= ?
            AND s_i_id = ol_i_id AND s_quantity < ?
        """)) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, warehouseId);
            ps.setInt(3, districtId);
            ps.setInt(4, nextOrderId);
            ps.setInt(5, nextOrderId - 20);
            ps.setInt(6, threshold);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int lowStock = rs.getInt(1);
            }
        }

        return true;
    }
}
