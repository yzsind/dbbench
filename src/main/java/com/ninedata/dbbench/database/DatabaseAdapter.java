package com.ninedata.dbbench.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface DatabaseAdapter {
    void initialize() throws SQLException;
    Connection getConnection() throws SQLException;
    void close();
    void createSchema() throws SQLException;
    void dropSchema() throws SQLException;
    Map<String, Object> collectMetrics() throws SQLException;
    String getDatabaseType();
}
