package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.tpcc.TPCCUtil;

import java.sql.*;

public class PaymentTransaction extends AbstractTransaction {

    public PaymentTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        super(adapter, warehouseId, districtId);
    }

    @Override
    public String getName() {
        return "PAYMENT";
    }

    @Override
    protected boolean doExecute(Connection conn) throws SQLException {
        double amount = TPCCUtil.randomDouble(1.00, 5000.00);
        int customerId;
        String customerLastName = null;
        boolean byName = TPCCUtil.randomInt(1, 100) <= 60;

        if (byName) {
            customerLastName = TPCCUtil.generateLastName(TPCCUtil.NURand(255, 0, 999));
        } else {
            customerId = TPCCUtil.NURand(1023, 1, TPCCUtil.CUSTOMERS_PER_DISTRICT);
        }

        // Update warehouse YTD
        try (PreparedStatement ps = conn.prepareStatement("UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?")) {
            ps.setDouble(1, amount);
            ps.setInt(2, warehouseId);
            ps.executeUpdate();
        }

        // Get warehouse info
        String wName;
        try (PreparedStatement ps = conn.prepareStatement("SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE w_id = ?")) {
            ps.setInt(1, warehouseId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            wName = rs.getString(1);
        }

        // Update district YTD
        try (PreparedStatement ps = conn.prepareStatement("UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?")) {
            ps.setDouble(1, amount);
            ps.setInt(2, warehouseId);
            ps.setInt(3, districtId);
            ps.executeUpdate();
        }

        // Get district info
        String dName;
        try (PreparedStatement ps = conn.prepareStatement("SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE d_w_id = ? AND d_id = ?")) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            dName = rs.getString(1);
        }

        // Find customer
        int cId;
        String cCredit;
        if (byName) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first")) {
                ps.setInt(1, warehouseId);
                ps.setInt(2, districtId);
                ps.setString(3, customerLastName);
                ResultSet rs = ps.executeQuery();
                int count = 0;
                int[] ids = new int[100];
                while (rs.next() && count < 100) {
                    ids[count++] = rs.getInt(1);
                }
                if (count == 0) return false;
                cId = ids[(count + 1) / 2 - 1];
            }
        } else {
            cId = TPCCUtil.NURand(1023, 1, TPCCUtil.CUSTOMERS_PER_DISTRICT);
        }

        // Get customer info
        try (PreparedStatement ps = conn.prepareStatement("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")) {
            ps.setInt(1, warehouseId);
            ps.setInt(2, districtId);
            ps.setInt(3, cId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return false;
            cCredit = rs.getString(11);
        }

        // Update customer
        if ("BC".equals(cCredit)) {
            String cData;
            try (PreparedStatement ps = conn.prepareStatement("SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")) {
                ps.setInt(1, warehouseId);
                ps.setInt(2, districtId);
                ps.setInt(3, cId);
                ResultSet rs = ps.executeQuery();
                rs.next();
                cData = rs.getString(1);
            }

            String newData = String.format("%d %d %d %d %d %.2f | %s", cId, districtId, warehouseId, districtId, warehouseId, amount, cData);
            if (newData.length() > 500) newData = newData.substring(0, 500);

            try (PreparedStatement ps = conn.prepareStatement("UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")) {
                ps.setDouble(1, amount);
                ps.setDouble(2, amount);
                ps.setString(3, newData);
                ps.setInt(4, warehouseId);
                ps.setInt(5, districtId);
                ps.setInt(6, cId);
                ps.executeUpdate();
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement("UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?")) {
                ps.setDouble(1, amount);
                ps.setDouble(2, amount);
                ps.setInt(3, warehouseId);
                ps.setInt(4, districtId);
                ps.setInt(5, cId);
                ps.executeUpdate();
            }
        }

        // Insert history
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setInt(1, cId);
            ps.setInt(2, districtId);
            ps.setInt(3, warehouseId);
            ps.setInt(4, districtId);
            ps.setInt(5, warehouseId);
            ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            ps.setDouble(7, amount);
            ps.setString(8, wName + "    " + dName);
            ps.executeUpdate();
        }

        return true;
    }
}
