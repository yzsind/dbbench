package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public abstract class AbstractDatabaseAdapter implements DatabaseAdapter {
    protected final DatabaseConfig config;
    protected HikariDataSource dataSource;

    public AbstractDatabaseAdapter(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public void initialize() throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setMaximumPoolSize(config.getPool().getSize());
        hikariConfig.setMinimumIdle(config.getPool().getMinIdle());
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);
        hikariConfig.setAutoCommit(false);

        try {
            hikariConfig.setDriverClassName(config.getDriverClassName());
        } catch (Exception e) {
            log.warn("Could not set driver class: {}", e.getMessage());
        }

        this.dataSource = new HikariDataSource(hikariConfig);
        log.info("Database connection pool initialized for {}", getDatabaseType());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("Database connection pool closed");
        }
    }

    @Override
    public void createSchema() throws SQLException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            for (String sql : getCreateTableStatements()) {
                stmt.execute(sql);
            }
            conn.commit();
            log.info("TPC-C schema created successfully");
        }
    }

    @Override
    public void dropSchema() throws SQLException {
        String[] tables = {"order_line", "new_order", "oorder", "history", "stock", "item", "customer", "district", "warehouse"};
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            for (String table : tables) {
                try {
                    stmt.execute("DROP TABLE IF EXISTS " + table);
                } catch (SQLException e) {
                    log.debug("Table {} does not exist", table);
                }
            }
            conn.commit();
            log.info("TPC-C schema dropped");
        }
    }

    @Override
    public Map<String, Object> collectMetrics() throws SQLException {
        return new HashMap<>();
    }

    protected abstract String[] getCreateTableStatements();
}
