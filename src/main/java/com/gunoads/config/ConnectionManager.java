package com.gunoads.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Component
public class ConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Test database connectivity
     */
    public boolean testConnection() {
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            logger.info("Database connection test successful");
            return true;
        } catch (Exception e) {
            logger.error("Database connection test failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get connection from pool
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Get database metadata info
     */
    public DatabaseInfo getDatabaseInfo() {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT version() as version, current_database() as database, current_user as user",
                    (rs, rowNum) -> new DatabaseInfo(
                            rs.getString("version"),
                            rs.getString("database"),
                            rs.getString("user")
                    )
            );
        } catch (Exception e) {
            logger.error("Failed to get database info: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Check if schema exists and tables are accessible
     */
    public boolean validateSchema() {
        try {
            String[] requiredTables = {
                    "tbl_account", "tbl_campaign", "tbl_adset",
                    "tbl_advertisement", "tbl_placement",
                    "tbl_ads_reporting", "tbl_ads_processing_date"
            };

            for (String table : requiredTables) {
                Integer count = jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                        Integer.class, table
                );

                if (count == null || count == 0) {
                    logger.error("Required table '{}' not found", table);
                    return false;
                }
            }

            logger.info("Schema validation successful - all required tables found");
            return true;
        } catch (Exception e) {
            logger.error("Schema validation failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get connection pool stats
     */
    public PoolStats getPoolStats() {
        try {
            if (dataSource instanceof com.zaxxer.hikari.HikariDataSource) {
                com.zaxxer.hikari.HikariDataSource hikari = (com.zaxxer.hikari.HikariDataSource) dataSource;
                com.zaxxer.hikari.HikariPoolMXBean pool = hikari.getHikariPoolMXBean();

                return new PoolStats(
                        pool.getTotalConnections(),
                        pool.getActiveConnections(),
                        pool.getIdleConnections(),
                        pool.getThreadsAwaitingConnection()
                );
            }
        } catch (Exception e) {
            logger.warn("Could not retrieve pool stats: {}", e.getMessage());
        }
        return null;
    }

    // Inner classes for data transfer
    public static class DatabaseInfo {
        public final String version;
        public final String database;
        public final String user;

        public DatabaseInfo(String version, String database, String user) {
            this.version = version;
            this.database = database;
            this.user = user;
        }

        @Override
        public String toString() {
            return String.format("DatabaseInfo{version='%s', database='%s', user='%s'}",
                    version, database, user);
        }
    }

    public static class PoolStats {
        public final int totalConnections;
        public final int activeConnections;
        public final int idleConnections;
        public final int threadsAwaiting;

        public PoolStats(int total, int active, int idle, int waiting) {
            this.totalConnections = total;
            this.activeConnections = active;
            this.idleConnections = idle;
            this.threadsAwaiting = waiting;
        }

        @Override
        public String toString() {
            return String.format("PoolStats{total=%d, active=%d, idle=%d, waiting=%d}",
                    totalConnections, activeConnections, idleConnections, threadsAwaiting);
        }
    }
}