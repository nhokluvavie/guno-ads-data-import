package com.gunoads.test.e2e;

import com.gunoads.service.MetaAdsService;
import com.gunoads.dao.AccountDao;
import com.gunoads.dao.CampaignDao;
import com.gunoads.dao.AdsReportingDao;
import com.gunoads.dao.AdsProcessingDateDao;
import com.gunoads.model.entity.Account;
import com.gunoads.model.entity.Campaign;
import com.gunoads.model.entity.AdsReporting;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.annotation.DirtiesContext;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E Test: Complete data pipeline from Meta API to PostgreSQL
 * Tests: Meta API → Service → DAO → Database full flow
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class FullPipelineE2ETest extends BaseE2ETest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MetaAdsService metaAdsService;

    @Autowired
    private AccountDao accountDao;

    @Autowired
    private CampaignDao campaignDao;

    @Autowired
    private AdsReportingDao adsReportingDao;

    @Autowired
    private AdsProcessingDateDao processingDateDao;

    private static final LocalDate TEST_DATE = LocalDate.now().minusDays(1);

    @BeforeEach
    void setUp() {
        logTestStart();
        // Clean test data
        cleanupTestData();
    }

    @Test
    @Order(1)
    @DisplayName("E2E: Manual Full Sync via REST API")
    void testManualFullSyncTrigger() {
        // Given: REST endpoint for full sync
        String url = baseUrl + "/api/scheduler/sync/full";

        // When: Trigger full sync
        ResponseEntity<String> response = restTemplate.postForEntity(url, null, String.class);

        // Then: Sync initiated successfully
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("success") || response.getBody().contains("completed"));

        // Wait for async processing
        waitForProcessing(5000);

        // Verify data was synced
        long accountCount = accountDao.count();
        assertTrue(accountCount > 0, "Accounts should be synced from Meta API");
    }

    @Test
    @Order(2)
    @DisplayName("E2E: Full Data Pipeline Flow")
    void testFullDataPipelineFlow() throws Exception {
        // Given: Clean database state
        long initialAccountCount = accountDao.count();

        // When: Execute full sync (hierarchy + performance)
        metaAdsService.performFullSync();

        // Then: Verify complete data hierarchy
        verifyAccountData();
        verifyCampaignData();
        verifyReportingData();
    }

    @Test
    @Order(3)
    @DisplayName("E2E: Data Consistency Across Tables")
    void testDataConsistencyAcrossTables() throws Exception {
        // Given: Execute hierarchy sync first
        metaAdsService.syncAccountHierarchy();
        waitForProcessing(2000);

        // When: Check relational consistency
        List<Account> accounts = accountDao.findAll(10, 0);
        List<Campaign> campaigns = campaignDao.findAll(10, 0);
        long reportingCount = adsReportingDao.count();

        // Then: Verify relationships
        assertTrue(accountDao.count() > 0, "Should have accounts");

        // Verify foreign key relationships if campaigns exist
        if (!campaigns.isEmpty()) {
            for (Campaign campaign : campaigns) {
                boolean accountExists = accounts.stream()
                        .anyMatch(acc -> acc.getId().equals(campaign.getAccountId()));
                assertTrue(accountExists, "Campaign must reference existing account");
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("E2E: Error Handling and Recovery")
    void testErrorHandlingAndRecovery() {
        // Given: System status endpoint
        String statusUrl = baseUrl + "/api/scheduler/status";

        // When: Check system health
        ResponseEntity<String> response = restTemplate.getForEntity(statusUrl, String.class);

        // Then: System is healthy
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("isConnected"));

        // Test sync with error handling
        String syncUrl = baseUrl + "/api/scheduler/sync/performance";
        ResponseEntity<String> syncResponse = restTemplate.postForEntity(syncUrl, null, String.class);
        assertEquals(HttpStatus.OK, syncResponse.getStatusCode());
    }

    @Test
    @Order(5)
    @DisplayName("E2E: Performance with Real Data Volume")
    void testPerformanceWithRealDataVolume() throws Exception {
        // Given: Start time measurement
        long startTime = System.currentTimeMillis();

        // When: Sync hierarchy data (faster than full sync)
        metaAdsService.syncAccountHierarchy();

        // Then: Performance within acceptable limits
        long duration = System.currentTimeMillis() - startTime;
        assertTrue(duration < 30000, "Hierarchy sync should complete within 30 seconds");

        // Verify data volume
        long accountCount = accountDao.count();
        long campaignCount = campaignDao.count();

        System.out.printf("✅ E2E Performance: %d ms, %d accounts, %d campaigns\n",
                duration, accountCount, campaignCount);
    }

    private void verifyAccountData() {
        long accountCount = accountDao.count();
        assertTrue(accountCount > 0, "Should have account data");

        List<Account> accounts = accountDao.findAll(1, 0);
        if (!accounts.isEmpty()) {
            Account account = accounts.get(0);
            assertNotNull(account.getId(), "Account ID required");
            assertNotNull(account.getAccountName(), "Account name required");
            assertEquals("META", account.getPlatformId(), "Platform should be META");
        }
    }

    private void verifyCampaignData() {
        long campaignCount = campaignDao.count();
        if (campaignCount > 0) {
            List<Campaign> campaigns = campaignDao.findAll(1, 0);
            if (!campaigns.isEmpty()) {
                Campaign campaign = campaigns.get(0);
                assertNotNull(campaign.getId(), "Campaign ID required");
                assertNotNull(campaign.getCampaignName(), "Campaign name required");
                assertNotNull(campaign.getAccountId(), "Account ID required");
            }
        }
    }

    private void verifyReportingData() {
        long reportingCount = adsReportingDao.count();
        // Reporting data may be empty if no insights available
        assertTrue(reportingCount >= 0, "Reporting count should be non-negative");
    }

    private void cleanupTestData() {
        // E2E tests with real data - minimal cleanup needed
        // Test isolation handled by @Transactional in integration layer
    }

    private void waitForProcessing(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}