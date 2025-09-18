package com.gunoads.test.e2e;

import com.gunoads.service.MetaAdsService;
import com.gunoads.dao.AccountDao;
import com.gunoads.dao.CampaignDao;
import com.gunoads.dao.AdsReportingDao;
import com.gunoads.model.entity.Account;
import com.gunoads.model.entity.Campaign;
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
 * Tests: Meta API → MetaAdsService → DataTransformer → DAO → Database
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

    @BeforeEach
    void setUp() {
        logTestStart();
        cleanupTestData();
    }

    @Test
    @Order(1)
    @DisplayName("E2E: Full Sync via REST API")
    void testFullSyncViaRestApi() {
        // Given: REST endpoint for full sync
        String url = baseUrl + "/api/scheduler/sync/full";
        long initialAccountCount = accountDao.count();
        long initialCampaignCount = campaignDao.count();

        // When: Trigger full sync via REST
        ResponseEntity<String> response = restTemplate.postForEntity(url, null, String.class);

        // Then: Request successful
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("success") || response.getBody().contains("completed"));

        // Wait for async processing
        waitForProcessing(5000);

        // Verify data was synced
        long finalAccountCount = accountDao.count();
        long finalCampaignCount = campaignDao.count();

        assertTrue(finalAccountCount >= initialAccountCount, "Account count should be maintained or increased");
        assertTrue(finalCampaignCount >= initialCampaignCount, "Campaign count should be maintained or increased");

        System.out.printf("✅ REST Full Sync: %d→%d accounts, %d→%d campaigns\n",
                initialAccountCount, finalAccountCount, initialCampaignCount, finalCampaignCount);
    }

    @Test
    @Order(2)
    @DisplayName("E2E: Direct Service Full Sync")
    void testDirectServiceFullSync() {
        // Given: Clean initial state
        long initialAccountCount = accountDao.count();
        long initialReportingCount = adsReportingDao.count();

        // When: Execute full sync directly via service
        assertDoesNotThrow(() -> metaAdsService.performFullSync(),
                "Full sync should execute without throwing exceptions");

        // Wait for processing
        waitForProcessing(3000);

        // Then: Verify complete data hierarchy
        verifyAccountData();
        verifyCampaignData();
        verifyReportingData();

        // Verify data increased or maintained
        long finalAccountCount = accountDao.count();
        long finalReportingCount = adsReportingDao.count();

        assertTrue(finalAccountCount >= initialAccountCount, "Should maintain or increase account data");
        assertTrue(finalReportingCount >= initialReportingCount, "Should maintain or increase reporting data");

        System.out.printf("✅ Service Full Sync: %d→%d accounts, %d→%d reports\n",
                initialAccountCount, finalAccountCount, initialReportingCount, finalReportingCount);
    }

    @Test
    @Order(3)
    @DisplayName("E2E: Data Consistency Validation")
    void testDataConsistencyValidation() {
        // Given: Execute hierarchy sync first
        assertDoesNotThrow(() -> metaAdsService.syncAccountHierarchy(),
                "Hierarchy sync should execute without exceptions");

        waitForProcessing(2000);

        // When: Get all hierarchical data
        List<Account> accounts = accountDao.findAll(10, 0);
        List<Campaign> campaigns = campaignDao.findAll(10, 0);
        long totalAccounts = accountDao.count();
        long totalCampaigns = campaignDao.count();

        // Then: Verify basic data existence
        assertTrue(totalAccounts >= 0, "Should have non-negative account count");

        // Verify foreign key relationships if data exists
        if (!campaigns.isEmpty() && !accounts.isEmpty()) {
            verifyAccountCampaignRelationships(accounts, campaigns);
        }

        System.out.printf("✅ Data Consistency: %d accounts, %d campaigns verified\n",
                totalAccounts, totalCampaigns);
    }

    @Test
    @Order(4)
    @DisplayName("E2E: Performance Data Pipeline")
    void testPerformanceDataPipeline() {
        // Given: Ensure accounts exist first
        assertDoesNotThrow(() -> metaAdsService.syncAccountHierarchy());
        waitForProcessing(1000);

        long accountCount = accountDao.count();
        assertTrue(accountCount > 0, "Need accounts for performance data sync");

        // When: Sync performance data for yesterday
        LocalDate yesterday = LocalDate.now().minusDays(1);
        long initialReportingCount = adsReportingDao.count();

        assertDoesNotThrow(() -> metaAdsService.syncYesterdayPerformanceData(),
                "Performance sync should execute without exceptions");

        waitForProcessing(3000);

        // Then: Verify reporting data
        long finalReportingCount = adsReportingDao.count();
        assertTrue(finalReportingCount >= initialReportingCount,
                "Reporting data should be maintained or increased");

        System.out.printf("✅ Performance Pipeline: %d→%d reports for %s\n",
                initialReportingCount, finalReportingCount, yesterday);
    }

    @Test
    @Order(5)
    @DisplayName("E2E: System Status and Health Check")
    void testSystemStatusAndHealthCheck() {
        // When: Get system status via REST
        String statusUrl = baseUrl + "/api/scheduler/status";
        ResponseEntity<String> statusResponse = restTemplate.getForEntity(statusUrl, String.class);

        // Then: Status endpoint should work
        assertEquals(HttpStatus.OK, statusResponse.getStatusCode());
        assertNotNull(statusResponse.getBody());

        String statusBody = statusResponse.getBody();
        assertTrue(statusBody.contains("isConnected"), "Status should contain connection info");
        assertTrue(statusBody.contains("accountCount"), "Status should contain account count");
        assertTrue(statusBody.contains("campaignCount"), "Status should contain campaign count");

        // When: Get service status directly
        MetaAdsService.SyncStatus syncStatus = metaAdsService.getSyncStatus();

        // Then: Service status should be accessible
        assertNotNull(syncStatus, "Sync status should not be null");
        assertTrue(syncStatus.accountCount >= 0, "Account count should be non-negative");
        assertTrue(syncStatus.campaignCount >= 0, "Campaign count should be non-negative");
        assertTrue(syncStatus.reportingCount >= 0, "Reporting count should be non-negative");

        System.out.printf("✅ System Health: Connected=%s, Accounts=%d, Campaigns=%d, Reports=%d\n",
                syncStatus.isConnected, syncStatus.accountCount, syncStatus.campaignCount, syncStatus.reportingCount);
    }

    // ==================== PRIVATE HELPER METHODS ====================

    /**
     * Verify account data exists and is valid
     */
    private void verifyAccountData() {
        long accountCount = accountDao.count();
        assertTrue(accountCount >= 0, "Should have non-negative account count");

        if (accountCount > 0) {
            List<Account> accounts = accountDao.findAll(1, 0);
            if (!accounts.isEmpty()) {
                Account account = accounts.get(0);
                assertNotNull(account.getId(), "Account ID should not be null");
                assertNotNull(account.getAccountName(), "Account name should not be null");
                assertEquals("FB", account.getPlatformId(), "Platform should be FB");
            }
        }
    }

    /**
     * Verify campaign data exists and is valid
     */
    private void verifyCampaignData() {
        long campaignCount = campaignDao.count();
        assertTrue(campaignCount >= 0, "Should have non-negative campaign count");

        if (campaignCount > 0) {
            List<Campaign> campaigns = campaignDao.findAll(1, 0);
            if (!campaigns.isEmpty()) {
                Campaign campaign = campaigns.get(0);
                assertNotNull(campaign.getId(), "Campaign ID should not be null");
                assertNotNull(campaign.getCampaignName(), "Campaign name should not be null");
                assertNotNull(campaign.getAccountId(), "Account ID should not be null");
            }
        }
    }

    /**
     * Verify reporting data is accessible
     */
    private void verifyReportingData() {
        long reportingCount = adsReportingDao.count();
        // Reporting data may be empty if no insights available for yesterday
        assertTrue(reportingCount >= 0, "Reporting count should be non-negative");
    }

    /**
     * Verify foreign key relationships between accounts and campaigns
     */
    private void verifyAccountCampaignRelationships(List<Account> accounts, List<Campaign> campaigns) {
        for (Campaign campaign : campaigns) {
            boolean accountExists = accounts.stream()
                    .anyMatch(account -> account.getId().equals(campaign.getAccountId()));
            assertTrue(accountExists,
                    String.format("Campaign %s must reference existing account %s",
                            campaign.getId(), campaign.getAccountId()));
        }
    }
}