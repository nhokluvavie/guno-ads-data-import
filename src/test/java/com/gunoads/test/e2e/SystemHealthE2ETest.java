package com.gunoads.test.e2e;

import com.gunoads.config.DatabaseHealthChecker;
import com.gunoads.util.ConnectionManager;
import com.gunoads.service.MetaAdsService;
import com.gunoads.dao.AccountDao;
import com.gunoads.dao.CampaignDao;
import com.gunoads.dao.AdsReportingDao;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.annotation.DirtiesContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E Test: System health monitoring and diagnostics
 * Tests: Database health, API connectivity, service status, resource monitoring
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SystemHealthE2ETest extends BaseE2ETest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private DatabaseHealthChecker databaseHealthChecker;

    @Autowired
    private ConnectionManager connectionManager;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private MetaAdsService metaAdsService;

    @Autowired
    private AccountDao accountDao;

    @Autowired
    private CampaignDao campaignDao;

    @Autowired
    private AdsReportingDao adsReportingDao;

    @BeforeEach
    void setUp() {
        logTestStart();
    }

    @Test
    @Order(1)
    @DisplayName("E2E: Database Health Check")
    void testDatabaseHealthCheck() {
        // When: Check database health
        DatabaseHealthChecker.HealthStatus status = databaseHealthChecker.checkHealth();

        // Then: Database should be healthy
        assertTrue(status.isHealthy, "Database should be healthy");
        assertEquals("Database is healthy", status.message);
        assertNotNull(status.details, "Health details should be provided");

        // Verify expected health details
        assertTrue(status.details.containsKey("database"), "Should contain database name");
        assertTrue(status.details.containsKey("user"), "Should contain user info");
        assertTrue(status.details.containsKey("pool.total"), "Should contain pool stats");
        assertTrue(status.details.containsKey("pool.active"), "Should contain active connections");
        assertTrue(status.details.containsKey("pool.idle"), "Should contain idle connections");

        System.out.println("✅ Database Health: " + status);
    }

    @Test
    @Order(2)
    @DisplayName("E2E: Connection Pool Health")
    void testConnectionPoolHealth() throws Exception {
        // When: Get pool statistics
        ConnectionManager.PoolStats poolStats = connectionManager.getPoolStats();

        // Then: Pool should be functioning
        assertNotNull(poolStats, "Pool stats should be available");
        assertTrue(poolStats.totalConnections >= 0, "Total connections should be non-negative");
        assertTrue(poolStats.activeConnections >= 0, "Active connections should be non-negative");
        assertTrue(poolStats.idleConnections >= 0, "Idle connections should be non-negative");

        // Test actual connection acquisition
        try (Connection connection = dataSource.getConnection()) {
            assertNotNull(connection, "Should be able to get connection from pool");
            assertTrue(connection.isValid(5), "Connection should be valid");
        }

        System.out.printf("✅ Pool Health: Total=%d, Active=%d, Idle=%d\n",
                poolStats.totalConnections, poolStats.activeConnections, poolStats.idleConnections);
    }

    @Test
    @Order(3)
    @DisplayName("E2E: Database Schema Validation")
    void testDatabaseSchemaValidation() {
        // When: Validate database schema
        boolean schemaValid = connectionManager.validateSchema();

        // Then: Schema should be valid
        assertTrue(schemaValid, "Database schema should be valid");

        // Verify database info
        ConnectionManager.DatabaseInfo dbInfo = connectionManager.getDatabaseInfo();
        assertNotNull(dbInfo, "Database info should be available");
        assertEquals("guno_db_test", dbInfo.database, "Should connect to test database");
        assertEquals("guno_user", dbInfo.user, "Should connect as correct user");
        assertTrue(dbInfo.version.contains("PostgreSQL"), "Should be PostgreSQL database");

        System.out.println("✅ Database Info: " + dbInfo.database + " (" + dbInfo.version + ")");
    }

    @Test
    @Order(4)
    @DisplayName("E2E: API Connectivity Health")
    void testApiConnectivityHealth() {
        // When: Test Meta API connectivity
        assertDoesNotThrow(() -> {
            metaAdsService.testConnectivity();
        }, "API connectivity test should not throw exceptions");

        // Get sync status to verify API health
        MetaAdsService.SyncStatus syncStatus = metaAdsService.getSyncStatus();
        assertNotNull(syncStatus, "Sync status should be available");

        System.out.println("✅ API Health: " + syncStatus);
    }

    @Test
    @Order(5)
    @DisplayName("E2E: System Status Endpoint")
    void testSystemStatusEndpoint() {
        // When: Check system status endpoint
        String statusUrl = baseUrl + "/api/scheduler/status";
        ResponseEntity<String> response = restTemplate.getForEntity(statusUrl, String.class);

        // Then: Status should be available
        assertEquals(HttpStatus.OK, response.getStatusCode(), "Status endpoint should be accessible");
        assertNotNull(response.getBody(), "Status response should contain data");

        String responseBody = response.getBody();
        assertTrue(responseBody.contains("isConnected"), "Should show connection status");
        assertTrue(responseBody.contains("accountCount"), "Should show account count");
        assertTrue(responseBody.contains("campaignCount"), "Should show campaign count");
        assertTrue(responseBody.contains("adSetCount"), "Should show adset count");
        assertTrue(responseBody.contains("adCount"), "Should show ad count");
        assertTrue(responseBody.contains("reportingCount"), "Should show reporting count");

        System.out.println("✅ System Status: " + responseBody);
    }

    @Test
    @Order(6)
    @DisplayName("E2E: Spring Actuator Health")
    void testSpringActuatorHealth() {
        // When: Check Spring actuator health
        String healthUrl = baseUrl + "/actuator/health";
        ResponseEntity<String> response = restTemplate.getForEntity(healthUrl, String.class);

        // Then: Health endpoint should be accessible
        assertEquals(HttpStatus.OK, response.getStatusCode(), "Actuator health should be accessible");
        assertNotNull(response.getBody(), "Health response should contain data");

        String responseBody = response.getBody();
        assertTrue(responseBody.contains("UP") || responseBody.contains("status"),
                "Should show health status");

        System.out.println("✅ Actuator Health: " + responseBody);
    }

    @Test
    @Order(7)
    @DisplayName("E2E: Data Integrity Health")
    void testDataIntegrityHealth() {
        // When: Check data counts across tables
        long accountCount = accountDao.count();
        long campaignCount = campaignDao.count();
        long reportingCount = adsReportingDao.count();

        // Then: Counts should be non-negative
        assertTrue(accountCount >= 0, "Account count should be non-negative");
        assertTrue(campaignCount >= 0, "Campaign count should be non-negative");
        assertTrue(reportingCount >= 0, "Reporting count should be non-negative");

        // Test basic database operations
        assertTrue(connectionManager.testConnection(), "Database connection should be working");

        System.out.printf("✅ Data Health: Accounts=%d, Campaigns=%d, Reports=%d\n",
                accountCount, campaignCount, reportingCount);
    }

    @Test
    @Order(8)
    @DisplayName("E2E: System Resource Monitoring")
    void testSystemResourceMonitoring() {
        // When: Monitor system resources
        Runtime runtime = Runtime.getRuntime();

        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        // Then: System should have reasonable resource usage
        assertTrue(totalMemory > 0, "Total memory should be positive");
        assertTrue(maxMemory > totalMemory, "Max memory should be greater than total");
        assertTrue(usedMemory < maxMemory, "Used memory should be less than max");

        // Memory usage should be reasonable (less than 80% of max)
        double memoryUsageRatio = (double) usedMemory / maxMemory;
        assertTrue(memoryUsageRatio < 0.8, "Memory usage should be under 80%");

        System.out.printf("✅ Memory Health: Used=%dMB, Total=%dMB, Max=%dMB (%.1f%%)\n",
                usedMemory / 1024 / 1024, totalMemory / 1024 / 1024,
                maxMemory / 1024 / 1024, memoryUsageRatio * 100);
    }

    @Test
    @Order(9)
    @DisplayName("E2E: Error Recovery Health")
    void testErrorRecoveryHealth() {
        // Given: System under stress
        String statusUrl = baseUrl + "/api/scheduler/status";

        // When: Make multiple rapid requests to test resilience
        for (int i = 0; i < 5; i++) {
            ResponseEntity<String> response = restTemplate.getForEntity(statusUrl, String.class);
            assertEquals(HttpStatus.OK, response.getStatusCode(),
                    "System should remain stable under load");
        }

        // Test database connection recovery
        assertTrue(connectionManager.testConnection(),
                "Database should recover from potential connection issues");

        System.out.println("✅ Error Recovery: System remains stable under stress");
    }

    @Test
    @Order(10)
    @DisplayName("E2E: Overall System Health Score")
    void testOverallSystemHealthScore() {
        // When: Evaluate overall system health
        boolean databaseHealthy = databaseHealthChecker.checkHealth().isHealthy;
        boolean connectionHealthy = connectionManager.testConnection();
        boolean schemaValid = connectionManager.validateSchema();

        // Check API health
        boolean apiHealthy = true;
        try {
            metaAdsService.testConnectivity();
        } catch (Exception e) {
            apiHealthy = false;
        }

        // Check endpoint health
        String statusUrl = baseUrl + "/api/scheduler/status";
        boolean endpointHealthy = restTemplate.getForEntity(statusUrl, String.class)
                .getStatusCode() == HttpStatus.OK;

        // Then: Calculate health score
        int healthScore = 0;
        if (databaseHealthy) healthScore += 20;
        if (connectionHealthy) healthScore += 20;
        if (schemaValid) healthScore += 20;
        if (apiHealthy) healthScore += 20;
        if (endpointHealthy) healthScore += 20;

        assertTrue(healthScore >= 80, "Overall system health should be at least 80%");

        System.out.printf("✅ Overall Health Score: %d%% (DB:%s, Conn:%s, Schema:%s, API:%s, Endpoints:%s)\n",
                healthScore, databaseHealthy, connectionHealthy, schemaValid, apiHealthy, endpointHealthy);
    }
}