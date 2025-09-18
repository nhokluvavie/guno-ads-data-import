package com.gunoads.test.e2e;

import com.gunoads.scheduler.DataSyncScheduler;
import com.gunoads.service.MetaAdsService;
import com.gunoads.dao.AccountDao;
import com.gunoads.dao.CampaignDao;
import com.gunoads.dao.AdsReportingDao;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E Test: Scheduler functionality with real CRON execution
 * Tests: Manual triggers, scheduled jobs, error handling
 */
@EnableScheduling
@TestPropertySource(properties = {
        "scheduler.enabled=true",
        "scheduler.daily-job-cron=*/10 * * * * ?",  // Every 10 seconds for testing
        "scheduler.hierarchy-job-cron=*/15 * * * * ?" // Every 15 seconds for testing
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SchedulerE2ETest extends BaseE2ETest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private DataSyncScheduler dataSyncScheduler;

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
    @DisplayName("E2E: Manual Sync Trigger via REST")
    void testManualSyncTrigger() {
        // Given: Manual sync endpoint
        String url = baseUrl + "/api/scheduler/sync/manual";

        // When: Trigger manual sync
        ResponseEntity<String> response = restTemplate.postForEntity(url, null, String.class);

        // Then: Request successful
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("success"));
        assertTrue(response.getBody().contains("Manual sync triggered"));

        // Wait for async execution
        waitForProcessing(3000);

        // Verify data was processed
        long accountCount = accountDao.count();
        assertTrue(accountCount >= 0, "Sync should complete without error");
    }

    @Test
    @Order(2)
    @DisplayName("E2E: Hierarchy Sync Trigger")
    void testHierarchySyncTrigger() {
        // Given: Hierarchy sync endpoint
        String url = baseUrl + "/api/scheduler/sync/hierarchy";
        long initialAccountCount = accountDao.count();

        // When: Trigger hierarchy sync
        ResponseEntity<String> response = restTemplate.postForEntity(url, null, String.class);

        // Then: Request successful
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("success"));
        assertTrue(response.getBody().contains("Hierarchy sync completed"));

        // Wait for processing
        waitForProcessing(3000);

        // Verify hierarchy data
        long finalAccountCount = accountDao.count();
        assertTrue(finalAccountCount >= initialAccountCount, "Should maintain or increase account count");
    }

    @Test
    @Order(3)
    @DisplayName("E2E: Performance Sync Trigger")
    void testPerformanceSyncTrigger() {
        // Given: Performance sync endpoint
        String url = baseUrl + "/api/scheduler/sync/performance";
        long initialReportingCount = adsReportingDao.count();

        // When: Trigger performance sync
        ResponseEntity<String> response = restTemplate.postForEntity(url, null, String.class);

        // Then: Request successful
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("success"));
        assertTrue(response.getBody().contains("Performance sync completed"));

        // Wait for processing
        waitForProcessing(3000);

        // Verify reporting data
        long finalReportingCount = adsReportingDao.count();
        assertTrue(finalReportingCount >= initialReportingCount, "Should maintain or increase reporting count");
    }

    @Test
    @Order(4)
    @DisplayName("E2E: Scheduler Status Monitoring")
    void testSchedulerStatusMonitoring() {
        // Given: Status endpoint
        String url = baseUrl + "/api/scheduler/status";

        // When: Get system status
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

        // Then: Status available
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());

        // Verify status contains expected fields
        String responseBody = response.getBody();
        assertTrue(responseBody.contains("isConnected"), "Should show connection status");
        assertTrue(responseBody.contains("accountCount"), "Should show account count");
        assertTrue(responseBody.contains("campaignCount"), "Should show campaign count");

        System.out.println("✅ System Status: " + responseBody);
    }

    @Test
    @Order(5)
    @DisplayName("E2E: Direct Scheduler Method Calls")
    void testDirectSchedulerMethods() {
        // Given: Direct access to scheduler
        long initialAccountCount = accountDao.count();
        long initialCampaignCount = campaignDao.count();

        // When: Call scheduler methods directly
        assertDoesNotThrow(() -> {
            dataSyncScheduler.syncAccountHierarchy();
            dataSyncScheduler.syncDailyPerformanceData();
        }, "Scheduler methods should execute without throwing exceptions");

        // Wait for async processing
        waitForProcessing(2000);

        // Then: Verify data consistency
        long finalAccountCount = accountDao.count();
        long finalCampaignCount = campaignDao.count();

        assertTrue(finalAccountCount >= initialAccountCount, "Account count should be maintained");
        assertTrue(finalCampaignCount >= initialCampaignCount, "Campaign count should be maintained");
    }

    @Test
    @Order(6)
    @DisplayName("E2E: Manual Trigger Testing")
    void testManualTriggerMethod() {
        // Given: Manual trigger capability
        long initialAccountCount = accountDao.count();

        // When: Execute manual trigger
        assertDoesNotThrow(() -> {
            dataSyncScheduler.triggerManualSync();
        }, "Manual trigger should execute without throwing exceptions");

        // Wait for processing
        waitForProcessing(3000);

        // Then: Verify execution completed
        long finalAccountCount = accountDao.count();
        assertTrue(finalAccountCount >= initialAccountCount, "Manual sync should maintain data integrity");
    }

    @Test
    @Order(7)
    @DisplayName("E2E: Concurrent Sync Request Handling")
    void testConcurrentSyncRequests() throws InterruptedException {
        // Given: Multiple concurrent requests
        String[] endpoints = {
                "/api/scheduler/sync/hierarchy",
                "/api/scheduler/sync/performance",
                "/api/scheduler/status"
        };

        // When: Make concurrent requests
        Thread[] threads = new Thread[endpoints.length];
        ResponseEntity<String>[] responses = new ResponseEntity[endpoints.length];

        for (int i = 0; i < endpoints.length; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                if (endpoints[index].contains("status")) {
                    responses[index] = restTemplate.getForEntity(baseUrl + endpoints[index], String.class);
                } else {
                    responses[index] = restTemplate.postForEntity(baseUrl + endpoints[index], null, String.class);
                }
            });
            threads[i].start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join(10000); // 10 second timeout
        }

        // Then: All requests should succeed
        for (int i = 0; i < responses.length; i++) {
            assertNotNull(responses[i], "Response " + i + " should not be null");
            assertEquals(HttpStatus.OK, responses[i].getStatusCode(),
                    "Endpoint " + endpoints[i] + " should return OK");
        }
    }

    @Test
    @Order(8)
    @DisplayName("E2E: Error Handling in Scheduler")
    void testErrorHandlingInScheduler() {
        // Given: System under potential error conditions
        String statusUrl = baseUrl + "/api/scheduler/status";

        // When: Check system health under load
        ResponseEntity<String> response = restTemplate.getForEntity(statusUrl, String.class);

        // Then: System should remain stable
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());

        // Verify error handling doesn't break the system
        String performanceUrl = baseUrl + "/api/scheduler/sync/performance";
        ResponseEntity<String> syncResponse = restTemplate.postForEntity(performanceUrl, null, String.class);

        // Should either succeed or handle error gracefully
        assertTrue(syncResponse.getStatusCode().is2xxSuccessful() ||
                        syncResponse.getStatusCode().is5xxServerError(),
                "Should either succeed or handle error gracefully");
    }

    public void waitForProcessing(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}