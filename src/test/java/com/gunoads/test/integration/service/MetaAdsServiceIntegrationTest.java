//package com.gunoads.test.integration.service;
//
//import com.gunoads.service.MetaAdsService;
//import com.gunoads.dao.*;
//import com.gunoads.test.integration.BaseIntegrationTest;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//
//import java.time.LocalDate;
//
//import static org.assertj.core.api.Assertions.*;
//import static org.junit.jupiter.api.Assumptions.*;
//
//class MetaAdsServiceIntegrationTest extends BaseIntegrationTest {
//
//    @Autowired
//    private MetaAdsService metaAdsService;
//
//    @Autowired
//    private AccountDao accountDao;
//
//    @Autowired
//    private CampaignDao campaignDao;
//
//    @Autowired
//    private AdSetDao adSetDao;
//
//    @Autowired
//    private AdvertisementDao advertisementDao;
//
//    @Autowired
//    private AdsReportingDao adsReportingDao;
//
//    @Test
//    void shouldTestConnectivity() {
//        // When
//        boolean isConnected = metaAdsService.testConnectivity();
//
//        // Then
//        assertThat(isConnected).isTrue();
//    }
//
//    @Test
//    void shouldGetSyncStatus() {
//        // When
//        MetaAdsService.SyncStatus status = metaAdsService.getSyncStatus();
//
//        // Then
//        assertThat(status).isNotNull();
//        assertThat(status.isConnected).isTrue();
//        assertThat(status.accountCount).isGreaterThanOrEqualTo(0);
//        assertThat(status.campaignCount).isGreaterThanOrEqualTo(0);
//        assertThat(status.adSetCount).isGreaterThanOrEqualTo(0);
//        assertThat(status.adCount).isGreaterThanOrEqualTo(0);
//        assertThat(status.reportingCount).isGreaterThanOrEqualTo(0);
//    }
//
//    @Test
//    void shouldSyncAccountHierarchy() {
//        // Given
//        long initialAccountCount = accountDao.count();
//        long initialCampaignCount = campaignDao.count();
//
//        // When
//        assertThatCode(() -> metaAdsService.syncAccountHierarchy())
//                .doesNotThrowAnyException();
//
//        // Then
//        long finalAccountCount = accountDao.count();
//        long finalCampaignCount = campaignDao.count();
//
//        assertThat(finalAccountCount).isGreaterThanOrEqualTo(initialAccountCount);
//        assertThat(finalCampaignCount).isGreaterThanOrEqualTo(initialCampaignCount);
//    }
//
//    @Test
//    void shouldSyncPerformanceDataForSpecificAccount() {
//        // Given - Get first available account
//        metaAdsService.syncAccountHierarchy();
//        waitFor(2000); // Wait for hierarchy sync
//
//        long accountCount = accountDao.count();
//        assumeTrue(accountCount > 0);
//
//        // Get first account
//        var accounts = accountDao.findAll(1, 0);
//        assumeTrue(!accounts.isEmpty());
//
//        String accountId = accounts.get(0).getId();
//        LocalDate yesterday = LocalDate.now().minusDays(1);
//        long initialReportingCount = adsReportingDao.count();
//
//        // When
//        assertThatCode(() -> metaAdsService.syncPerformanceData(accountId, yesterday, yesterday))
//                .doesNotThrowAnyException();
//
//        // Then
//        long finalReportingCount = adsReportingDao.count();
//        assertThat(finalReportingCount).isGreaterThanOrEqualTo(initialReportingCount);
//    }
//
//    @Test
//    void shouldSyncYesterdayPerformanceData() {
//        // Given
//        long initialReportingCount = adsReportingDao.count();
//
//        // When
//        assertThatCode(() -> metaAdsService.syncYesterdayPerformanceData())
//                .doesNotThrowAnyException();
//
//        // Then
//        long finalReportingCount = adsReportingDao.count();
//        assertThat(finalReportingCount).isGreaterThanOrEqualTo(initialReportingCount);
//    }
//
//    @Test
//    void shouldPerformFullSync() {
//        // Given
//        long initialAccountCount = accountDao.count();
//        long initialReportingCount = adsReportingDao.count();
//
//        // When
//        assertThatCode(() -> metaAdsService.performFullSync())
//                .doesNotThrowAnyException();
//
//        // Then
//        long finalAccountCount = accountDao.count();
//        long finalReportingCount = adsReportingDao.count();
//
//        assertThat(finalAccountCount).isGreaterThanOrEqualTo(initialAccountCount);
//        assertThat(finalReportingCount).isGreaterThanOrEqualTo(initialReportingCount);
//    }
//
//    @Test
//    void shouldSyncCampaignsForSpecificAccount() {
//        // Given - Ensure we have accounts
//        metaAdsService.syncAccountHierarchy();
//        waitFor(2000);
//
//        var accounts = accountDao.findAll(1, 0);
//        assumeTrue(!accounts.isEmpty());
//
//        String accountId = accounts.get(0).getId();
//        long initialCampaignCount = campaignDao.count();
//
//        // When
//        assertThatCode(() -> metaAdsService.syncCampaignsForAccount(accountId))
//                .doesNotThrowAnyException();
//
//        // Then
//        long finalCampaignCount = campaignDao.count();
//        assertThat(finalCampaignCount).isGreaterThanOrEqualTo(initialCampaignCount);
//    }
//
//    @Test
//    void shouldHandleEmptyAccountsGracefully() {
//        // Given - Mock scenario with no accounts (should not fail)
//
//        // When & Then - Should not throw exception even if no accounts
//        assertThatCode(() -> metaAdsService.syncYesterdayPerformanceData())
//                .doesNotThrowAnyException();
//    }
//
//    @Test
//    void shouldSyncWithinReasonableTime() {
//        // Given
//        long startTime = System.currentTimeMillis();
//
//        // When
//        metaAdsService.syncAccountHierarchy();
//        long hierarchyDuration = System.currentTimeMillis() - startTime;
//
//        // Then - Should complete within reasonable time
//        assertThat(hierarchyDuration).isLessThan(60000); // 60 seconds
//    }
//
//    @Test
//    void shouldMaintainDataConsistency() {
//        // Given
//        metaAdsService.syncAccountHierarchy();
//        waitFor(2000);
//
//        // When
//        long accountCount = accountDao.count();
//        long campaignCount = campaignDao.count();
//        long adSetCount = adSetDao.count();
//        long adCount = advertisementDao.count();
//
//        // Then - Data should be consistent
//        if (campaignCount > 0) {
//            assertThat(accountCount).isGreaterThan(0);
//        }
//        if (adSetCount > 0) {
//            assertThat(campaignCount).isGreaterThan(0);
//        }
//        if (adCount > 0) {
//            assertThat(adSetCount).isGreaterThan(0);
//        }
//    }
//
//    @Test
//    void shouldHandleNetworkErrorsGracefully() {
//        // When - This may fail due to network issues, should handle gracefully
//        assertThatCode(() -> {
//            // Attempt sync that might fail due to network
//            metaAdsService.testConnectivity();
//        }).doesNotThrowAnyException();
//    }
//
//    @Test
//    void shouldValidateServiceConfiguration() {
//        // When
//        MetaAdsService.SyncStatus status = metaAdsService.getSyncStatus();
//
//        // Then - Service should be properly configured
//        assertThat(status).isNotNull();
//        assertThat(status.toString()).contains("SyncStatus");
//        assertThat(status.toString()).contains("connected=");
//        assertThat(status.toString()).contains("accounts=");
//    }
//
//    @Test
//    void shouldSyncMultipleAccountsIfAvailable() {
//        // Given
//        metaAdsService.syncAccountHierarchy();
//        waitFor(2000);
//
//        var accounts = accountDao.findAll(3, 0); // Get up to 3 accounts
//
//        // When - Sync each account individually
//        for (var account : accounts) {
//            assertThatCode(() -> metaAdsService.syncCampaignsForAccount(account.getId()))
//                    .doesNotThrowAnyException();
//        }
//
//        // Then - Should complete successfully
//        assertThat(accounts).isNotNull();
//    }
//}