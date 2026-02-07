package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
@Getter
public abstract class AbstractTransaction {
    protected final DatabaseAdapter adapter;
    protected final int warehouseId;
    protected final int districtId;
    protected final boolean useLimitSyntax;

    public AbstractTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        this.adapter = adapter;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.useLimitSyntax = adapter.supportsLimitSyntax();
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
            log.error(e.getMessage());
            return false;
        }
    }

    protected abstract boolean doExecute(Connection conn) throws SQLException;

    /**
     * Build a SELECT query with LIMIT 1 that works across databases.
     * For Oracle: uses ROWNUM
     * For DB2: uses FETCH FIRST 1 ROWS ONLY
     * For SQL Server: uses TOP 1
     * For MySQL/PostgreSQL/etc: uses LIMIT 1
     */
    protected String buildSelectFirstRowQuery(String baseQuery) {
        String dbType = adapter.getDatabaseType().toLowerCase();
        if (dbType.contains("sql server")) {
            // SQL Server: SELECT TOP 1 ... - need to insert TOP after SELECT
            return baseQuery.replaceFirst("(?i)SELECT\\s+", "SELECT TOP 1 ");
        } else if (useLimitSyntax) {
            return baseQuery + " LIMIT 1";
        } else {
            // DB2｜ORACLE: uses FETCH FIRST syntax
            return baseQuery + " FETCH FIRST 1 ROWS ONLY";
        }
    }

    /**
     * Build a SELECT FOR UPDATE query with LIMIT 1 that works across databases.
     */
    protected String buildSelectFirstRowForUpdateQuery(String baseQuery) {
        String dbType = adapter.getDatabaseType().toLowerCase();
        if (dbType.contains("sql server")) {
            // SQL Server: uses TOP 1 and WITH (UPDLOCK, ROWLOCK) instead of FOR UPDATE
            String query = baseQuery.replaceFirst("(?i)SELECT\\s+", "SELECT TOP 1 ");
            // Add WITH (UPDLOCK, ROWLOCK) hint after table name
            query = addSqlServerLockHint(query);
            return query;
        } else if (useLimitSyntax) {
            return baseQuery + " LIMIT 1 FOR UPDATE";
        } else {
            // DB2｜ORACLE: FOR UPDATE comes after FETCH FIRST
            return baseQuery + " FETCH FIRST 1 ROWS ONLY FOR UPDATE";
        }
    }

    /**
     * Build a SELECT FOR UPDATE query (without LIMIT) that works across databases.
     * For SQL Server: uses WITH (UPDLOCK, ROWLOCK) hint
     * For other databases: appends FOR UPDATE
     */
    protected String buildSelectForUpdateQuery(String baseQuery) {
        String dbType = adapter.getDatabaseType().toLowerCase();
        if (dbType.contains("sql server")) {
            // SQL Server: uses WITH (UPDLOCK, ROWLOCK) instead of FOR UPDATE
            return addSqlServerLockHint(baseQuery);
        } else {
            return baseQuery + " FOR UPDATE";
        }
    }

    /**
     * Add SQL Server lock hint WITH (UPDLOCK, ROWLOCK) after table name in FROM clause
     */
    private String addSqlServerLockHint(String query) {
        // Match FROM table_name and add hint after it
        // Handles: FROM table_name WHERE, FROM table_name ORDER BY, etc.
        return query.replaceFirst(
            "(?i)(FROM\\s+)(\\w+)(\\s+(?:WHERE|ORDER|GROUP|HAVING|$))",
            "$1$2 WITH (UPDLOCK, ROWLOCK)$3"
        );
    }
}
