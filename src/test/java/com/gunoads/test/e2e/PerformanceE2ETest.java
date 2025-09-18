package com.gunoads.test.e2e;

import com.gunoads.service.MetaAdsService;
import com.gunoads.dao.*;
import com.gunoads.util.ConnectionManager;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E Test: Performance testing with large datasets and load scenarios
 * Tests: Throughput, response times, memory usage, concurrent operations
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PerformanceE2ETest extends BaseE2ETest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MetaAdsService metaAdsService;

    @Autowired
    private AccountDao accountDao;

    @Autowired
    private CampaignDao campaignDao;

    @Autowired
    private AdSetDao adSetDao;

    @Autowired
    private AdvertisementDao advertisementDao;

    @Autowired
    private AdsReportingDao adsReportingDao;

    @Autowired
    private ConnectionManager connectionManager;

    // Performance thresholds
    private static final long SYNC_TIMEOUT_SECONDS = 60;
    private static final long MAX_RESPONSE_TIME_MS = 5000;
    private static final int CONCURRENT_REQUESTS = 10;
    private static final double MAX_MEMORY_USAGE_RATIO = 0.8;

    @BeforeEach
    void setUp() {
        logTestStart();
        // Force GC before performance tests
        System.gc();
        waitForProcessing(1000);
    }

    @Test
    @Order(1)
    @DisplayName("E2E: Sync Performance with Timeout")
    void testSyncPerformanceWithTimeout() {
        // Given: Performance measurement setup
        long startTime = System.currentTimeMillis();
        long initialMemory = getUsedMemory();

        // When: Execute hierarchy sync with timeout
        assertTimeoutPreemptively(Duration.ofSeconds(SYNC_TIMEOUT_SECONDS), () -> {
            metaAdsService.syncAccountHierarchy();
        }, "Hierarchy sync should complete within " + SYNC_TIMEOUT_SECONDS + " seconds");

        // Then: Measure performance metrics
        long duration = System.currentTimeMillis() - startTime;
        long finalMemory = getUsedMemory();
        long memoryIncrease = finalMemory - initialMemory;

        // Verify performance criteria
        assertTrue(duration < SYNC_TIMEOUT_SECONDS * 1000, "Sync should be fast");
        assertTrue(memoryIncrease < 500_000_000, "Memory usage should be reasonable"); // 500MB limit

        // Get data counts for throughput calculation
        long accountCount = accountDao.count();
        long campaignCount = campaignDao.count();
        long totalRecords = accountCount + campaignCount;

        double recordsPerSecond = totalRecords > 0 ? (double) totalRecords / (duration / 1000.0) : 0;

        System.out.printf("✅ Sync Performance: %d ms, %d records (%.1f records/sec), Memory: +%d MB\n",
                duration, totalRecords, recordsPerSecond, memoryIncrease / 1024 / 1024);
    }

    @Test
    @Order(2)
    @DisplayName("E2E: API Response Time Performance")
    void testApiResponseTimePerformance() {
        // Given: API endpoints to test
        String[] endpoints = {
                "/api/scheduler/status",
                "/actuator/health"
        };

        // When: Measure response times
        for (String endpoint : endpoints) {
            long startTime = System.currentTimeMillis();

            ResponseEntity<String> response = restTemplate.getForEntity(baseUrl + endpoint, String.class);

            long duration = System.currentTimeMillis() - startTime;

            // Then: Verify response time and status
            assertEquals(HttpStatus.OK, response.getStatusCode(),
                    "Endpoint " + endpoint + " should be accessible");
            assertTrue(duration < MAX_RESPONSE_TIME_MS,
                    "Response time for " + endpoint + " should be under " + MAX_RESPONSE_TIME_MS + "ms, was " + duration + "ms");

            System.out.printf("✅ API Performance [%s]: %d ms\n", endpoint, duration);
        }
    }

    @Test
    @Order(3)
    @DisplayName("E2E: Concurrent Request Handling")
    void testConcurrentRequestHandling() throws InterruptedException {
        // Given: Concurrent request setup
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);
        CountDownLatch latch = new CountDownLatch(CONCURRENT_REQUESTS);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicLong totalResponseTime = new AtomicLong(0);

        // When: Execute concurrent requests
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
            executor.submit(() -> {
                try {
                    long requestStart = System.currentTimeMillis();

                    ResponseEntity<String> response = restTemplate.getForEntity(
                            baseUrl + "/api/scheduler/status", String.class);

                    long requestDuration = System.currentTimeMillis() - requestStart;
                    totalResponseTime.addAndGet(requestDuration);

                    if (response.getStatusCode() == HttpStatus.OK) {
                        successCount.incrementAndGet();
                    }

                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all requests to complete
        assertTrue(latch.await(30, TimeUnit.SECONDS), "All concurrent requests should complete");

        long totalTime = System.currentTimeMillis() - startTime;
        double avgResponseTime = (double) totalResponseTime.get() / CONCURRENT_REQUESTS;
        double successRate = (double) successCount.get() / CONCURRENT_REQUESTS * 100;

        // Then: Verify concurrent performance
        assertTrue(successRate >= 95, "Success rate should be at least 95%");
        assertTrue(avgResponseTime < MAX_RESPONSE_TIME_MS, "Average response time should be reasonable");

        executor.shutdown();

        System.out.printf("✅ Concurrent Performance: %d requests, %.1f%% success, %.1f ms avg response\n",
                CONCURRENT_REQUESTS, successRate, avgResponseTime);
    }

    @Test
    @Order(4)
    @DisplayName("E2E: Memory Usage Under Load")
    void testMemoryUsageUnderLoad() throws Exception {
        // Given: Initial memory state
        System.gc();
        waitForProcessing(1000);
        long initialMemory = getUsedMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();

        // When: Execute memory-intensive operations
        for (int i = 0; i < 3; i++) {
            metaAdsService.testConnectivity();

            long currentMemory = getUsedMemory();
            double memoryRatio = (double) currentMemory / maxMemory;

            assertTrue(memoryRatio < MAX_MEMORY_USAGE_RATIO,
                    "Memory usage should stay under " + (MAX_MEMORY_USAGE_RATIO * 100) + "%");
        }

        // Then: Memory should be stable
        long finalMemory = getUsedMemory();
        long memoryIncrease = finalMemory - initialMemory;

        System.out.printf("✅ Memory Performance: Initial=%d MB, Final=%d MB, Increase=%d MB\n",
                initialMemory / 1024 / 1024, finalMemory / 1024 / 1024, memoryIncrease / 1024 / 1024);
    }

    @Test
    @Order(5)
    @DisplayName("E2E: Database Connection Pool Performance")
    void testDatabaseConnectionPoolPerformance() throws InterruptedException {
        // Given: Connection pool stress test
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch latch = new CountDownLatch(50);
        AtomicInteger successCount = new AtomicInteger(0);

        // When: Execute many database operations concurrently
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 50; i++) {
            executor.submit(() -> {
                try {
                    // Test connection acquisition and basic query
                    if (connectionManager.testConnection()) {
                        long count = accountDao.count();
                        if (count >= 0) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    // Connection pool stress may cause some failures
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All database operations should complete");

        long duration = System.currentTimeMillis() - startTime;
        double successRate = (double) successCount.get() / 50 * 100;

        // Then: Connection pool should handle load
        assertTrue(successRate >= 80, "Database connection success rate should be at least 80%");

        // Check pool health after stress
        ConnectionManager.PoolStats poolStats = connectionManager.getPoolStats();
        assertNotNull(poolStats, "Pool stats should be available");
        assertTrue(poolStats.totalConnections > 0, "Pool should have connections");

        executor.shutdown();

        System.out.printf("✅ DB Pool Performance: %d operations in %d ms, %.1f%% success\n",
                50, duration, successRate);
    }

    @Test
    @Order(6)
    @DisplayName("E2E: Large Dataset Processing Performance")
    void testLargeDatasetProcessingPerformance() {
        // Given: Large dataset scenario
        long startTime = System.currentTimeMillis();
        long initialCount = adsReportingDao.count();

        // When: Process performance data (potentially large)
        assertDoesNotThrow(() -> {
            metaAdsService.syncYesterdayPerformanceData();
        }, "Large dataset processing should not throw exceptions");

        long duration = System.currentTimeMillis() - startTime;
        long finalCount = adsReportingDao.count();
        long recordsProcessed = finalCount - initialCount;

        // Then: Processing should be efficient
        if (recordsProcessed > 0) {
            double recordsPerSecond = (double) recordsProcessed / (duration / 1000.0);
            assertTrue(recordsPerSecond >= 10, "Should process at least 10 records/second");

            System.out.printf("✅ Large Dataset Performance: %d records in %d ms (%.1f records/sec)\n",
                    recordsProcessed, duration, recordsPerSecond);
        } else {
            System.out.println("✅ Large Dataset Performance: No new records to process (normal for test data)");
        }
    }

    @Test
    @Order(7)
    @DisplayName("E2E: System Resource Monitoring")
    void testSystemResourceMonitoring() {
        // Given: Resource monitoring setup
        Runtime runtime = Runtime.getRuntime();

        // When: Monitor resources during operation
        long startTime = System.currentTimeMillis();
        long startMemory = getUsedMemory();

        // Execute some operations
        assertDoesNotThrow(() -> {
            metaAdsService.getSyncStatus();
            connectionManager.testConnection();
            accountDao.count();
        });

        long endTime = System.currentTimeMillis();
        long endMemory = getUsedMemory();

        // Then: Resources should be stable
        long duration = endTime - startTime;
        long memoryDelta = endMemory - startMemory;
        long maxMemory = runtime.maxMemory();
        double memoryUsage = (double) endMemory / maxMemory;

        assertTrue(duration < 5000, "Operations should complete quickly");
        assertTrue(memoryUsage < MAX_MEMORY_USAGE_RATIO, "Memory usage should be reasonable");
        assertTrue(Math.abs(memoryDelta) < 100_000_000, "Memory should not increase dramatically"); // 100MB

        System.out.printf("✅ Resource Monitoring: %d ms, Memory: %d MB (%.1f%%), Delta: %+d MB\n",
                duration, endMemory / 1024 / 1024, memoryUsage * 100, memoryDelta / 1024 / 1024);
    }

    @Test
    @Order(8)
    @DisplayName("E2E: Performance Under Error Conditions")
    void testPerformanceUnderErrorConditions() {
        // Given: Error condition simulation
        long startTime = System.currentTimeMillis();

        // When: Test system resilience
        for (int i = 0; i < 5; i++) {
            // Make requests that might fail
            try {
                ResponseEntity<String> response = restTemplate.getForEntity(
                        baseUrl + "/api/scheduler/status", String.class);
                assertEquals(HttpStatus.OK, response.getStatusCode());
            } catch (Exception e) {
                // System should handle errors gracefully
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        // Then: System should remain responsive
        assertTrue(duration < 15000, "Error handling should not significantly slow down system");
        assertTrue(connectionManager.testConnection(), "Database should remain accessible");

        System.out.printf("✅ Error Resilience Performance: %d ms for error scenario testing\n", duration);
    }

    @Test
    @Order(9)
    @DisplayName("E2E: Overall System Performance Score")
    void testOverallSystemPerformanceScore() throws Exception {
        // Given: Comprehensive performance evaluation
        long startTime = System.currentTimeMillis();

        // When: Execute various operations
        int performanceScore = 100; // Start with perfect score

        // Test sync performance (30% weight)
        long syncStart = System.currentTimeMillis();
        metaAdsService.syncAccountHierarchy();
        long syncDuration = System.currentTimeMillis() - syncStart;
        if (syncDuration > 30000) performanceScore -= 30; // Deduct if > 30s
        else if (syncDuration > 15000) performanceScore -= 15; // Deduct if > 15s

        // Test API response time (25% weight)
        long apiStart = System.currentTimeMillis();
        ResponseEntity<String> response = restTemplate.getForEntity(baseUrl + "/api/scheduler/status", String.class);
        long apiDuration = System.currentTimeMillis() - apiStart;
        if (apiDuration > 2000) performanceScore -= 25; // Deduct if > 2s
        else if (apiDuration > 1000) performanceScore -= 12; // Deduct if > 1s

        // Test memory usage (25% weight)
        long memory = getUsedMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        double memoryRatio = (double) memory / maxMemory;
        if (memoryRatio > 0.8) performanceScore -= 25; // Deduct if > 80%
        else if (memoryRatio > 0.6) performanceScore -= 12; // Deduct if > 60%

        // Test database performance (20% weight)
        long dbStart = System.currentTimeMillis();
        accountDao.count();
        long dbDuration = System.currentTimeMillis() - dbStart;
        if (dbDuration > 1000) performanceScore -= 20; // Deduct if > 1s
        else if (dbDuration > 500) performanceScore -= 10; // Deduct if > 0.5s

        long totalDuration = System.currentTimeMillis() - startTime;

        // Then: Performance should meet standards
        assertTrue(performanceScore >= 70, "Overall performance score should be at least 70%");

        System.out.printf("✅ Overall Performance Score: %d%% (Total time: %d ms)\n",
                performanceScore, totalDuration);
        System.out.printf("   - Sync: %d ms, API: %d ms, Memory: %.1f%%, DB: %d ms\n",
                syncDuration, apiDuration, memoryRatio * 100, dbDuration);
    }

    // Helper methods
    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    public void waitForProcessing(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}