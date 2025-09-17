package com.gunoads.test.integration.dao;

import com.gunoads.config.DatabaseHealthChecker;
import com.gunoads.test.integration.BaseIntegrationTest;
import com.gunoads.util.ConnectionManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;

import static org.assertj.core.api.Assertions.*;

class DatabaseConnectionTest extends BaseIntegrationTest {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ConnectionManager connectionManager;

    @Autowired
    private DatabaseHealthChecker healthChecker;

    @Test
    void shouldConnectToDatabase() {
        // When & Then
        assertThatCode(() -> {
            try (Connection connection = dataSource.getConnection()) {
                assertThat(connection).isNotNull();
                assertThat(connection.isValid(5)).isTrue();
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void shouldExecuteSimpleQuery() {
        // When
        Integer result = jdbcTemplate.queryForObject("SELECT 1", Integer.class);

        // Then
        assertThat(result).isEqualTo(1);
    }

    @Test
    void shouldTestConnectionViaConnectionManager() {
        // When
        boolean isConnected = connectionManager.testConnection();

        // Then
        assertThat(isConnected).isTrue();
    }

    @Test
    void shouldValidateSchemaExists() {
        // When
        boolean schemaValid = connectionManager.validateSchema();

        // Then
        assertThat(schemaValid).isTrue();
    }

    @Test
    void shouldGetDatabaseInfo() {
        // When
        ConnectionManager.DatabaseInfo dbInfo = connectionManager.getDatabaseInfo();

        // Then
        assertThat(dbInfo).isNotNull();
        assertThat(dbInfo.database).isEqualTo("guno_db_test");
        assertThat(dbInfo.user).isEqualTo("guno_user");
        assertThat(dbInfo.version).contains("PostgreSQL");
    }

    @Test
    void shouldGetConnectionPoolStats() {
        // When
        ConnectionManager.PoolStats poolStats = connectionManager.getPoolStats();

        // Then
        assertThat(poolStats).isNotNull();
        assertThat(poolStats.totalConnections).isGreaterThanOrEqualTo(0);
        assertThat(poolStats.activeConnections).isGreaterThanOrEqualTo(0);
        assertThat(poolStats.idleConnections).isGreaterThanOrEqualTo(0);
        assertThat(poolStats.threadsAwaiting).isGreaterThanOrEqualTo(0);
    }

    @Test
    void shouldPassHealthCheck() {
        // When
        DatabaseHealthChecker.HealthStatus status = healthChecker.checkHealth();

        // Then
        assertThat(status.isHealthy).isTrue();
        assertThat(status.message).isEqualTo("Database is healthy");
        assertThat(status.details).isNotNull();
        assertThat(status.details).containsKey("database");
        assertThat(status.details).containsKey("user");
        assertThat(status.details).containsKeys("pool.total", "pool.active", "pool.idle");
    }

    @Test
    void shouldVerifyAllRequiredTablesExist() {
        // Given
        String[] requiredTables = {
                "tbl_account", "tbl_campaign", "tbl_adset",
                "tbl_advertisement", "tbl_placement",
                "tbl_ads_reporting", "tbl_ads_processing_date"
        };

        // When & Then
        for (String tableName : requiredTables) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                    Integer.class, tableName
            );
            assertThat(count).isEqualTo(1)
                    .withFailMessage("Required table '%s' not found", tableName);
        }
    }

    @Test
    void shouldCreateAndDropTestTable() {
        // Given
        String testTableName = "test_integration_" + System.currentTimeMillis();

        // When - Create table
        jdbcTemplate.execute(String.format(
                "CREATE TABLE %s (id VARCHAR(50) PRIMARY KEY, name VARCHAR(100))", testTableName
        ));

        // Then - Verify table exists
        Integer tableCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                Integer.class, testTableName
        );
        assertThat(tableCount).isEqualTo(1);

        // When - Insert test data
        jdbcTemplate.update(
                String.format("INSERT INTO %s (id, name) VALUES (?, ?)", testTableName),
                "test_id", "test_name"
        );

        // Then - Verify data
        String name = jdbcTemplate.queryForObject(
                String.format("SELECT name FROM %s WHERE id = ?", testTableName),
                String.class, "test_id"
        );
        assertThat(name).isEqualTo("test_name");

        // Cleanup
        jdbcTemplate.execute(String.format("DROP TABLE %s", testTableName));
    }

    @Test
    void shouldHandleConnectionPoolUnderLoad() throws InterruptedException {
        // Given
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        boolean[] results = new boolean[threadCount];

        // When - Simulate concurrent connections
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try (Connection connection = dataSource.getConnection()) {
                    Integer result = jdbcTemplate.queryForObject("SELECT " + (index + 1), Integer.class);
                    results[index] = (result == index + 1);
                } catch (Exception e) {
                    results[index] = false;
                }
            });
            threads[i].start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join(5000); // 5 second timeout
        }

        // Then - All connections should succeed
        for (int i = 0; i < threadCount; i++) {
            assertThat(results[i]).isTrue()
                    .withFailMessage("Connection %d failed", i);
        }
    }

    @Test
    void shouldMaintainTransactionIsolation() {
        // When - Start transaction and insert data
        String testId = "isolation_test_" + System.currentTimeMillis();

        // This should be rolled back due to @Transactional on BaseIntegrationTest
        jdbcTemplate.update(
                "INSERT INTO tbl_ads_processing_date (full_date, day_of_week, day_of_month, day_of_year, " +
                        "week_of_year, month_of_year, quarter, year, is_weekend, is_holiday, fiscal_year, fiscal_quarter) " +
                        "VALUES (?, 1, 1, 1, 1, 1, 1, 2999, false, false, 2999, 1)",
                testId
        );

        // Then - Data should exist within transaction
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM tbl_ads_processing_date WHERE full_date = ?",
                Integer.class, testId
        );
        assertThat(count).isEqualTo(1);
    }
}