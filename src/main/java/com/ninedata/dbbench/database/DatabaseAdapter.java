package com.ninedata.dbbench.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public interface DatabaseAdapter {
    void initialize() throws SQLException;
    Connection getConnection() throws SQLException;
    void close();
    void createSchema() throws SQLException;
    void dropSchema() throws SQLException;
    Map<String, Object> collectMetrics() throws SQLException;
    String getDatabaseType();

    /**
     * Check if database supports LIMIT syntax (MySQL, PostgreSQL, etc.)
     * Oracle and DB2 use different syntax (FETCH FIRST n ROWS ONLY or ROWNUM)
     */
    default boolean supportsLimitSyntax() {
        return true;
    }

    /**
     * Collect database host OS metrics (CPU, Memory, Disk I/O, Network I/O)
     * This is collected from the database server side if supported
     */
    default Map<String, Object> collectHostMetrics() throws SQLException {
        return new HashMap<>();
    }
}
