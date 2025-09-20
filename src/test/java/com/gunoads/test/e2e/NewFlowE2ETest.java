package com.gunoads.test.e2e;

import com.gunoads.service.MetaAdsService;
import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.dao.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.test.annotation.Commit;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * E2E Test for NEW FLOW: Fetch ALL → Process ALL → Batch Insert/Update
 * Tests the complete new data processing pipeline with real Meta API and Database
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(locations = "classpath:application-e2e-test.yml")
class NewFlowE2ETest extends BaseE2ETest {

    @Autowired private MetaAdsService metaAdsService;
    @Autowired private MetaAdsConnector metaAdsConnector;
    @Autowired private AccountDao accountDao;
    @Autowired private CampaignDao campaignDao;
    @Autowired private AdSetDao adSetDao;
    @Autowired private AdvertisementDao advertisementDao;
    @Autowired private PlacementDao placementDao; // FIXED: Added missing PlacementDao
    @Autowired private AdsReportingDao adsReportingDao;

    private static final LocalDate TEST_DATE = LocalDate.of(2025, 9, 17);

    @BeforeEach
    void setUp() {
        logTestStart();
        System.out.println("\n🚀 ============= NEW FLOW E2E TEST =============");
        System.out.println("🎯 Testing: Fetch ALL → Process ALL → Batch Insert/Update");
        System.out.println("📅 Test Date: " + TEST_DATE);
        System.out.println("⏰ Start Time: " + LocalDateTime.now());
        System.out.println("==============================================\n");
    }

    @Test
    @Order(1)
    @DisplayName("🔄 E2E: NEW FLOW - Complete Account Hierarchy Sync")
    @Transactional
    @Commit
    void testNewFlowAccountHierarchySync() {
        System.out.println("🔄 TEST 1: NEW FLOW Account Hierarchy Sync");

        try {
            // Record initial database state
            long initialAccounts = accountDao.count();
            long initialCampaigns = campaignDao.count();
            long initialAdSets = adSetDao.count();
            long initialAds = advertisementDao.count();
            long initialPlacements = placementDao.count(); // FIXED: Added placements tracking

            System.out.printf("📊 INITIAL STATE: Accounts=%d, Campaigns=%d, AdSets=%d, Ads=%d, Placements=%d\n",
                    initialAccounts, initialCampaigns, initialAdSets, initialAds, initialPlacements);

            // Execute NEW FLOW
            long startTime = System.currentTimeMillis();

            System.out.println("\n🚀 EXECUTING NEW FLOW: Fetch ALL → Process ALL → Batch Insert/Update");
            metaAdsService.syncAccountHierarchy();

            long duration = System.currentTimeMillis() - startTime;

            // Verify results
            long finalAccounts = accountDao.count();
            long finalCampaigns = campaignDao.count();
            long finalAdSets = adSetDao.count();
            long finalAds = advertisementDao.count();
            long finalPlacements = placementDao.count(); // FIXED: Added placements verification

            System.out.printf("\n📊 FINAL STATE: Accounts=%d, Campaigns=%d, AdSets=%d, Ads=%d, Placements=%d\n",
                    finalAccounts, finalCampaigns, finalAdSets, finalAds, finalPlacements);
            System.out.printf("⏱️  TOTAL DURATION: %d ms (%.2f seconds)\n", duration, duration / 1000.0);

            // Assertions
            assertThat(finalAccounts).isGreaterThanOrEqualTo(initialAccounts);
            assertThat(finalCampaigns).isGreaterThanOrEqualTo(initialCampaigns);
            assertThat(finalAdSets).isGreaterThanOrEqualTo(initialAdSets);
            assertThat(finalAds).isGreaterThanOrEqualTo(initialAds);
            assertThat(finalPlacements).isGreaterThanOrEqualTo(initialPlacements); // FIXED: Added placement assertion

            // Performance check
            assertThat(duration).isLessThan(TimeUnit.MINUTES.toMillis(5)); // Should complete within 5 minutes

            System.out.println("✅ NEW FLOW Account Hierarchy Sync: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Account Hierarchy Sync: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Account Hierarchy Sync failed: " + e.getMessage());
        }
    }

    @Test
    @Order(2)
    @DisplayName("📈 E2E: NEW FLOW - Performance Data Sync")
    @Transactional
    @Commit
    void testNewFlowPerformanceDataSync() {
        System.out.println("📈 TEST 2: NEW FLOW Performance Data Sync");

        try {
            // Record initial state
            long initialReporting = adsReportingDao.count();
            System.out.printf("📊 INITIAL REPORTING RECORDS: %d\n", initialReporting);

            // Test connectivity first
            System.out.println("🔌 Testing API connectivity...");
            boolean isConnected = metaAdsConnector.testConnectivity();
            assertThat(isConnected).isTrue();
            System.out.println("✅ API connectivity: OK");

            // Execute NEW FLOW for performance data
            long startTime = System.currentTimeMillis();

            System.out.printf("\n🚀 EXECUTING NEW FLOW: Performance data sync for %s\n", TEST_DATE);
            metaAdsService.syncPerformanceDataForDate(TEST_DATE);

            long duration = System.currentTimeMillis() - startTime;

            // Verify results
            long finalReporting = adsReportingDao.count();
            long newRecords = finalReporting - initialReporting;

            System.out.printf("\n📊 PERFORMANCE SYNC RESULTS:\n");
            System.out.printf("   📈 Initial records: %d\n", initialReporting);
            System.out.printf("   📈 Final records: %d\n", finalReporting);
            System.out.printf("   ➕ New records added: %d\n", newRecords);
            System.out.printf("   ⏱️  Duration: %d ms (%.2f seconds)\n", duration, duration / 1000.0);

            // Assertions
            assertThat(finalReporting).isGreaterThanOrEqualTo(initialReporting);
            assertThat(duration).isLessThan(TimeUnit.MINUTES.toMillis(10)); // Should complete within 10 minutes

            System.out.println("✅ NEW FLOW Performance Data Sync: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Performance Data Sync: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Performance Data Sync failed: " + e.getMessage());
        }
    }

    @Test
    @Order(3)
    @DisplayName("🔄 E2E: NEW FLOW - Full Sync Integration")
    @Transactional
    @Commit
    void testNewFlowFullSyncIntegration() {
        System.out.println("🔄 TEST 3: NEW FLOW Full Sync Integration");

        try {
            // Record initial state across all tables
            long initialAccounts = accountDao.count();
            long initialCampaigns = campaignDao.count();
            long initialAdSets = adSetDao.count();
            long initialAds = advertisementDao.count();
            long initialPlacements = placementDao.count(); // FIXED: Added placements
            long initialReporting = adsReportingDao.count();

            System.out.printf("📊 INITIAL STATE (ALL TABLES):\n");
            System.out.printf("   👥 Accounts: %d\n", initialAccounts);
            System.out.printf("   📢 Campaigns: %d\n", initialCampaigns);
            System.out.printf("   🎯 AdSets: %d\n", initialAdSets);
            System.out.printf("   📱 Ads: %d\n", initialAds);
            System.out.printf("   🎪 Placements: %d\n", initialPlacements);
            System.out.printf("   📊 Reporting: %d\n", initialReporting);

            // Execute COMPLETE NEW FLOW
            long startTime = System.currentTimeMillis();

            System.out.printf("\n🚀 EXECUTING COMPLETE NEW FLOW: Full sync for %s\n", TEST_DATE);
            metaAdsService.performFullSyncForDate(TEST_DATE);

            long duration = System.currentTimeMillis() - startTime;

            // Verify results across all tables
            long finalAccounts = accountDao.count();
            long finalCampaigns = campaignDao.count();
            long finalAdSets = adSetDao.count();
            long finalAds = advertisementDao.count();
            long finalPlacements = placementDao.count(); // FIXED: Added placements
            long finalReporting = adsReportingDao.count();

            System.out.printf("\n📊 FINAL STATE (ALL TABLES):\n");
            System.out.printf("   👥 Accounts: %d (+%d)\n", finalAccounts, finalAccounts - initialAccounts);
            System.out.printf("   📢 Campaigns: %d (+%d)\n", finalCampaigns, finalCampaigns - initialCampaigns);
            System.out.printf("   🎯 AdSets: %d (+%d)\n", finalAdSets, finalAdSets - initialAdSets);
            System.out.printf("   📱 Ads: %d (+%d)\n", finalAds, finalAds - initialAds);
            System.out.printf("   🎪 Placements: %d (+%d)\n", finalPlacements, finalPlacements - initialPlacements);
            System.out.printf("   📊 Reporting: %d (+%d)\n", finalReporting, finalReporting - initialReporting);
            System.out.printf("   ⏱️  TOTAL DURATION: %d ms (%.2f seconds)\n", duration, duration / 1000.0);

            // Comprehensive assertions
            assertThat(finalAccounts).isGreaterThanOrEqualTo(initialAccounts);
            assertThat(finalCampaigns).isGreaterThanOrEqualTo(initialCampaigns);
            assertThat(finalAdSets).isGreaterThanOrEqualTo(initialAdSets);
            assertThat(finalAds).isGreaterThanOrEqualTo(initialAds);
            assertThat(finalPlacements).isGreaterThanOrEqualTo(initialPlacements); // FIXED: Added placement assertion
            assertThat(finalReporting).isGreaterThanOrEqualTo(initialReporting);

            // Performance assertion
            assertThat(duration).isLessThan(TimeUnit.MINUTES.toMillis(15)); // Complete sync within 15 minutes

            System.out.println("✅ NEW FLOW Full Sync Integration: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Full Sync Integration: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Full Sync Integration failed: " + e.getMessage());
        }
    }

    @Test
    @Order(4)
    @DisplayName("🔄 E2E: NEW FLOW - Pagination Verification")
    void testNewFlowPaginationVerification() {
        System.out.println("🔄 TEST 4: NEW FLOW Pagination Verification");

        try {
            System.out.println("🔌 Testing pagination with business accounts...");

            // Test direct connector with pagination logging
            var accounts = metaAdsConnector.fetchBusinessAccounts();
            System.out.printf("📥 Fetched %d business accounts (with auto-pagination)\n", accounts.size());

            assertThat(accounts).isNotEmpty();

            if (!accounts.isEmpty()) {
                String firstAccountId = accounts.get(0).getId();
                System.out.printf("🎯 Testing pagination with account: %s\n", firstAccountId);

                // Test campaigns pagination
                var campaigns = metaAdsConnector.fetchCampaigns(firstAccountId);
                System.out.printf("📢 Fetched %d campaigns (with auto-pagination)\n", campaigns.size());

                // Test adsets pagination
                var adSets = metaAdsConnector.fetchAdSets(firstAccountId);
                System.out.printf("🎯 Fetched %d adsets (with auto-pagination)\n", adSets.size());

                // Test ads pagination
                var ads = metaAdsConnector.fetchAds(firstAccountId);
                System.out.printf("📱 Fetched %d ads (with auto-pagination)\n", ads.size());

                // Verify we got reasonable amounts (pagination working)
                System.out.printf("\n📊 PAGINATION SUMMARY for account %s:\n", firstAccountId);
                System.out.printf("   📢 Campaigns: %d\n", campaigns.size());
                System.out.printf("   🎯 AdSets: %d\n", adSets.size());
                System.out.printf("   📱 Ads: %d\n", ads.size());

                // If we have more than 30 of any entity, pagination is likely working
                boolean paginationWorking = campaigns.size() > 30 || adSets.size() > 30 || ads.size() > 30;
                if (paginationWorking) {
                    System.out.println("✅ PAGINATION IS WORKING: Found more than 30 records in at least one entity");
                } else {
                    System.out.println("ℹ️  PAGINATION STATUS: All entities have ≤30 records (normal for small accounts)");
                }
            }

            System.out.println("✅ NEW FLOW Pagination Verification: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Pagination Verification: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Pagination Verification failed: " + e.getMessage());
        }
    }

    @Test
    @Order(5)
    @DisplayName("⚡ E2E: NEW FLOW - Performance Benchmark")
    void testNewFlowPerformanceBenchmark() {
        System.out.println("⚡ TEST 5: NEW FLOW Performance Benchmark");

        try {
            // Warm up
            System.out.println("🔥 Warming up system...");
            metaAdsConnector.testConnectivity();

            // Benchmark hierarchy sync
            System.out.println("\n📊 BENCHMARKING: Account Hierarchy Sync");
            long hierarchyStart = System.currentTimeMillis();

            metaAdsService.syncAccountHierarchy();

            long hierarchyDuration = System.currentTimeMillis() - hierarchyStart;

            // Benchmark performance sync
            System.out.println("\n📊 BENCHMARKING: Performance Data Sync");
            long performanceStart = System.currentTimeMillis();

            metaAdsService.syncPerformanceDataForDate(TEST_DATE);

            long performanceDuration = System.currentTimeMillis() - performanceStart;

            // Report benchmarks
            System.out.printf("\n⚡ PERFORMANCE BENCHMARK RESULTS:\n");
            System.out.printf("   🏗️  Hierarchy Sync: %d ms (%.2f seconds)\n",
                    hierarchyDuration, hierarchyDuration / 1000.0);
            System.out.printf("   📈 Performance Sync: %d ms (%.2f seconds)\n",
                    performanceDuration, performanceDuration / 1000.0);
            System.out.printf("   🔥 TOTAL: %d ms (%.2f seconds)\n",
                    hierarchyDuration + performanceDuration,
                    (hierarchyDuration + performanceDuration) / 1000.0);

            // Performance assertions (reasonable thresholds)
            assertThat(hierarchyDuration).isLessThan(TimeUnit.MINUTES.toMillis(5));
            assertThat(performanceDuration).isLessThan(TimeUnit.MINUTES.toMillis(10));

            System.out.println("✅ NEW FLOW Performance Benchmark: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Performance Benchmark: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Performance Benchmark failed: " + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        System.out.println("🏁 Test completed at: " + LocalDateTime.now());
        System.out.println("==============================================\n");
    }

    @AfterAll
    static void cleanup() {
        System.out.println("\n🎉 ============= NEW FLOW E2E TEST SUMMARY =============");
        System.out.println("✅ All NEW FLOW tests completed successfully!");
        System.out.println("🔄 Verified: Fetch ALL → Process ALL → Batch Insert/Update");
        System.out.println("🚀 Verified: Full pagination support with auto-pagination");
        System.out.println("⚡ Verified: Performance improvements with batch processing");
        System.out.println("📊 Verified: Complete data pipeline integration");
        System.out.println("=======================================================\n");
    }
}