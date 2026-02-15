package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.BiConsumer;

@Slf4j
@Getter
public abstract class AbstractTransaction {
    protected final DatabaseAdapter adapter;
    protected final int warehouseId;
    protected final int districtId;
    protected final boolean useLimitSyntax;
    protected final boolean useRowIdForLimitForUpdate;
    protected final boolean supportsForUpdate;

    @Setter
    private static BiConsumer<String, String> errorCallback;

    public AbstractTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        this.adapter = adapter;
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.useLimitSyntax = adapter.supportsLimitSyntax();
        this.useRowIdForLimitForUpdate = adapter.requiresRowIdForLimitForUpdate();
        this.supportsForUpdate = adapter.supportsForUpdate();
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
            String errorMsg = String.format("[%s] %s", getName(), e.getMessage());
            log.error(errorMsg);
            if (errorCallback != null) {
                errorCallback.accept("ERROR", errorMsg);
            }
            return false;
        }
    }

    protected abstract boolean doExecute(Connection conn) throws SQLException;

    /**
     * Build a SELECT query with LIMIT 1 that works across databases.
     * For Oracle: uses ROWNUM subquery
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
        } else if (dbType.contains("oracle")) {
            // Oracle: use ROWNUM subquery for compatibility with 11g and earlier
            return "SELECT * FROM (" + baseQuery + ") WHERE ROWNUM = 1";
        } else {
            // DB2: uses FETCH FIRST syntax
            return baseQuery + " FETCH FIRST 1 ROWS ONLY";
        }
    }

    /**
     * Build a SELECT FOR UPDATE query with LIMIT 1 that works across databases.
     * For Oracle 11g: uses ROWID-based subquery since FETCH FIRST is not supported
     * For DB2: uses FETCH FIRST 1 ROWS ONLY FOR UPDATE
     * For SQL Server: uses TOP 1 with lock hints
     * For MySQL/PostgreSQL/etc: uses LIMIT 1 FOR UPDATE
     */
    protected String buildSelectFirstRowForUpdateQuery(String baseQuery) {
        if (!supportsForUpdate) {
            // SQLite: no FOR UPDATE support, rely on file-level locking
            return buildSelectFirstRowQuery(baseQuery);
        }
        String dbType = adapter.getDatabaseType().toLowerCase();
        if (dbType.contains("sql server")) {
            // SQL Server: uses TOP 1 and WITH (UPDLOCK, ROWLOCK) instead of FOR UPDATE
            String query = baseQuery.replaceFirst("(?i)SELECT\\s+", "SELECT TOP 1 ");
            // Add WITH (UPDLOCK, ROWLOCK) hint after table name
            query = addSqlServerLockHint(query);
            return query;
        } else if (useLimitSyntax) {
            return baseQuery + " LIMIT 1 FOR UPDATE";
        } else if (useRowIdForLimitForUpdate) {
            // Oracle 11g: use ROWID-based subquery for SELECT ... FOR UPDATE with LIMIT
            return buildOracleRowIdForUpdateQuery(baseQuery);
        } else {
            // DB2: FOR UPDATE comes after FETCH FIRST
            return baseQuery + " FETCH FIRST 1 ROWS ONLY FOR UPDATE";
        }
    }

    /**
     * Build Oracle ROWID-based query for SELECT ... FOR UPDATE with LIMIT 1.
     * This is required for Oracle 11g and earlier which don't support FETCH FIRST syntax.
     *
     * Transforms: SELECT col FROM table WHERE cond ORDER BY col
     * Into: SELECT col FROM table WHERE ROWID = (SELECT ROWID FROM (SELECT ROWID FROM table WHERE cond ORDER BY col) WHERE ROWNUM = 1) FOR UPDATE
     */
    private String buildOracleRowIdForUpdateQuery(String baseQuery) {
        // Extract table name and WHERE/ORDER BY clauses from base query
        // Pattern: SELECT ... FROM table_name WHERE ... ORDER BY ...
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "(?i)SELECT\\s+.+?\\s+FROM\\s+(\\w+)\\s+(WHERE\\s+.+?)?(ORDER\\s+BY\\s+.+)?$"
        );
        java.util.regex.Matcher matcher = pattern.matcher(baseQuery.trim());

        if (matcher.find()) {
            String tableName = matcher.group(1);
            String whereClause = matcher.group(2) != null ? matcher.group(2).trim() : "";
            String orderByClause = matcher.group(3) != null ? matcher.group(3).trim() : "";

            // Build the ROWID subquery
            StringBuilder innerQuery = new StringBuilder("SELECT ROWID FROM ");
            innerQuery.append(tableName);
            if (!whereClause.isEmpty()) {
                innerQuery.append(" ").append(whereClause);
            }
            if (!orderByClause.isEmpty()) {
                innerQuery.append(" ").append(orderByClause);
            }

            // Extract the SELECT columns from original query
            java.util.regex.Pattern selectPattern = java.util.regex.Pattern.compile("(?i)SELECT\\s+(.+?)\\s+FROM");
            java.util.regex.Matcher selectMatcher = selectPattern.matcher(baseQuery);
            String selectColumns = selectMatcher.find() ? selectMatcher.group(1) : "*";

            // Build final query: SELECT cols FROM table WHERE ROWID = (SELECT ROWID FROM (...) WHERE ROWNUM = 1) FOR UPDATE
            StringBuilder result = new StringBuilder("SELECT ");
            result.append(selectColumns);
            result.append(" FROM ").append(tableName);
            result.append(" WHERE ROWID = (SELECT ROWID FROM (");
            result.append(innerQuery);
            result.append(") WHERE ROWNUM = 1) FOR UPDATE");

            return result.toString();
        }

        // Fallback: if pattern doesn't match, return original with FOR UPDATE (may fail but provides debug info)
        return baseQuery + " FOR UPDATE";
    }

    /**
     * Build a SELECT FOR UPDATE query (without LIMIT) that works across databases.
     * For SQL Server: uses WITH (UPDLOCK, ROWLOCK) hint
     * For other databases: appends FOR UPDATE
     */
    protected String buildSelectForUpdateQuery(String baseQuery) {
        if (!supportsForUpdate) {
            // SQLite: no FOR UPDATE support, rely on file-level locking
            return baseQuery;
        }
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
