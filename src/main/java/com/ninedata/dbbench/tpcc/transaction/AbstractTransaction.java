package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;

@Getter
public abstract class AbstractTransaction {
    protected final DatabaseAdapter adapter;
    protected final int warehouseId;
    protected final int districtId;

    public AbstractTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        this.adapter = adapter;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
    }

    public abstract String getName();

    public boolean execute() {
        try (Connection conn = adapter.getConnection()) {
            boolean success = doExecute(conn);
            if (success) {
                conn.commit();
            } else {
                conn.rollback();
            }
            return success;
        } catch (SQLException e) {
            return false;
        }
    }

    protected abstract boolean doExecute(Connection conn) throws SQLException;
}
