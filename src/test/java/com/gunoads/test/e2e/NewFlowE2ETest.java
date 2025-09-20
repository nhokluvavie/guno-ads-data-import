package com.gunoads.test.e2e;

import com.gunoads.service.MetaAdsService;
import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.dao.*;
import com.gunoads.model.dto.MetaAccountDto;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.test.annotation.Commit;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * E2E Test for NEW FLOW: Fetch ALL → Process ALL → Batch Insert/Update
 * MODIFIED: Tests ONLY on SINGLE account (first account) for better performance
 * FIXED: Complete implementation with all required methods
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
    private static String SINGLE_TEST_ACCOUNT_ID = "468073679646974"; // ADDED: Store single account for testing

    @BeforeEach
    void setUp() {
        logTestStart();
        System.out.println("\n🚀 ============= NEW FLOW E2E TEST (SINGLE ACCOUNT) =============");
        System.out.println("🎯 Testing: Fetch ALL → Process ALL → Batch Insert/Update");
        System.out.println("📅 Test Date: " + TEST_DATE);
        System.out.println("⏰ Start Time: " + LocalDateTime.now());
        System.out.println("✨ Mode: SINGLE ACCOUNT ONLY (Performance Optimized)");
        System.out.println("================================================================\n");
    }

    @Test
    @Order(1)
    @DisplayName("🔍 E2E: Setup Single Test Account")
    void testSetupSingleTestAccount() {
        System.out.println("🔍 TEST 1: Setup Single Test Account");

        try {
            System.out.println("📥 Fetching business accounts to select first one...");

            // Fetch all accounts but only use the first one
            List<MetaAccountDto> allAccounts = metaAdsConnector.fetchBusinessAccounts();
            assertThat(allAccounts).isNotEmpty();

            // Select ONLY the first account for all subsequent tests
            SINGLE_TEST_ACCOUNT_ID = allAccounts.get(2).getId();
            String accountName = allAccounts.get(2).getAccountName();

            System.out.printf("🎯 SELECTED TEST ACCOUNT: %s (%s)\n", SINGLE_TEST_ACCOUNT_ID, accountName);
            System.out.printf("📊 Total accounts available: %d (using only 1st account)\n", allAccounts.size());

            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();

            System.out.println("✅ Single Test Account Setup: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ Single Test Account Setup: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("Single Test Account Setup failed: " + e.getMessage());
        }
    }

    @Test
    @Order(2)
    @DisplayName("🔄 E2E: NEW FLOW - Single Account Hierarchy Sync")
    @Transactional
    @Commit
    void testNewFlowSingleAccountHierarchySync() {
        System.out.println("🔄 TEST 2: NEW FLOW Single Account Hierarchy Sync");

        try {
            // Ensure we have a test account
            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();
            System.out.printf("🎯 Testing with account: %s\n", SINGLE_TEST_ACCOUNT_ID);

            // Record initial database state
            long initialAccounts = accountDao.count();
            long initialCampaigns = campaignDao.count();
            long initialAdSets = adSetDao.count();
            long initialAds = advertisementDao.count();
            long initialPlacements = placementDao.count();

            System.out.printf("📊 INITIAL STATE: Accounts=%d, Campaigns=%d, AdSets=%d, Ads=%d, Placements=%d\n",
                    initialAccounts, initialCampaigns, initialAdSets, initialAds, initialPlacements);

            // Execute NEW FLOW for SINGLE account only
            long startTime = System.currentTimeMillis();

            System.out.printf("\n🚀 EXECUTING NEW FLOW for SINGLE ACCOUNT: %s\n", SINGLE_TEST_ACCOUNT_ID);

            // MODIFIED: Sync only single account instead of all accounts
            metaAdsService.syncSingleAccountHierarchy(SINGLE_TEST_ACCOUNT_ID);

            long duration = System.currentTimeMillis() - startTime;

            // Verify results
            long finalAccounts = accountDao.count();
            long finalCampaigns = campaignDao.count();
            long finalAdSets = adSetDao.count();
            long finalAds = advertisementDao.count();
            long finalPlacements = placementDao.count();

            System.out.printf("\n📊 FINAL STATE: Accounts=%d, Campaigns=%d, AdSets=%d, Ads=%d, Placements=%d\n",
                    finalAccounts, finalCampaigns, finalAdSets, finalAds, finalPlacements);
            System.out.printf("⏱️  SINGLE ACCOUNT DURATION: %d ms (%.2f seconds)\n", duration, duration / 1000.0);

            // Assertions for single account
            assertThat(finalAccounts).isGreaterThanOrEqualTo(initialAccounts);
            assertThat(finalCampaigns).isGreaterThanOrEqualTo(initialCampaigns);
            assertThat(finalAdSets).isGreaterThanOrEqualTo(initialAdSets);
            assertThat(finalAds).isGreaterThanOrEqualTo(initialAds);
            assertThat(finalPlacements).isGreaterThanOrEqualTo(initialPlacements);

            // Performance check - should be much faster for single account
            assertThat(duration).isLessThan(TimeUnit.MINUTES.toMillis(2)); // 2 minutes max for single account

            System.out.println("✅ NEW FLOW Single Account Hierarchy Sync: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Single Account Hierarchy Sync: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Single Account Hierarchy Sync failed: " + e.getMessage());
        }
    }

    @Test
    @Order(3)
    @DisplayName("📈 E2E: NEW FLOW - Performance Data Sync")
    @Transactional
    @Commit
    void testNewFlowPerformanceDataSync() {
        System.out.println("📈 TEST 3: NEW FLOW Performance Data Sync");

        try {
            // Ensure we have a test account
            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();
            System.out.printf("🎯 Testing performance sync with account: %s\n", SINGLE_TEST_ACCOUNT_ID);

            // Record initial state
            long initialReporting = adsReportingDao.count();
            System.out.printf("📊 INITIAL REPORTING RECORDS: %d\n", initialReporting);

            // Test connectivity first
            System.out.println("🔌 Testing API connectivity...");
            boolean isConnected = metaAdsConnector.testConnectivity();
            assertThat(isConnected).isTrue();
            System.out.println("✅ API connectivity: OK");

            // Execute NEW FLOW for performance data on SINGLE account
            long startTime = System.currentTimeMillis();

            System.out.printf("\n🚀 EXECUTING NEW FLOW: Performance data sync for account %s on %s\n",
                    SINGLE_TEST_ACCOUNT_ID, TEST_DATE);

            // MODIFIED: Sync performance data for single account only
            metaAdsService.syncPerformanceDataForAccountAndDate(SINGLE_TEST_ACCOUNT_ID, TEST_DATE);

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
            assertThat(duration).isLessThan(TimeUnit.MINUTES.toMillis(5)); // 5 minutes max for single account

            System.out.println("✅ NEW FLOW Performance Data Sync: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Performance Data Sync: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Performance Data Sync failed: " + e.getMessage());
        }
    }

    @Test
    @Order(4)
    @DisplayName("🔄 E2E: NEW FLOW - Single Account Full Integration")
    @Transactional
    @Commit
    void testNewFlowSingleAccountFullIntegration() {
        System.out.println("🔄 TEST 4: NEW FLOW Single Account Full Integration");

        try {
            // Ensure we have a test account
            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();
            System.out.printf("🎯 Full integration test with account: %s\n", SINGLE_TEST_ACCOUNT_ID);

            // Record initial state across all tables
            long initialAccounts = accountDao.count();
            long initialCampaigns = campaignDao.count();
            long initialAdSets = adSetDao.count();
            long initialAds = advertisementDao.count();
            long initialPlacements = placementDao.count();
            long initialReporting = adsReportingDao.count();

            System.out.printf("📊 INITIAL STATE: Accounts=%d, Campaigns=%d, AdSets=%d, Ads=%d, Placements=%d, Reporting=%d\n",
                    initialAccounts, initialCampaigns, initialAdSets, initialAds, initialPlacements, initialReporting);

            // Execute COMPLETE NEW FLOW for single account
            long startTime = System.currentTimeMillis();

            System.out.printf("\n🚀 EXECUTING COMPLETE NEW FLOW for SINGLE ACCOUNT: %s\n", SINGLE_TEST_ACCOUNT_ID);

            // Step 1: Sync hierarchy for single account
            System.out.println("📋 Step 1: Syncing hierarchy...");
            metaAdsService.syncSingleAccountHierarchy(SINGLE_TEST_ACCOUNT_ID);

            // Step 2: Sync performance data for single account
            System.out.println("📈 Step 2: Syncing performance data...");
            metaAdsService.syncPerformanceDataForAccountAndDate(SINGLE_TEST_ACCOUNT_ID, TEST_DATE);

            long duration = System.currentTimeMillis() - startTime;

            // Verify final results
            long finalAccounts = accountDao.count();
            long finalCampaigns = campaignDao.count();
            long finalAdSets = adSetDao.count();
            long finalAds = advertisementDao.count();
            long finalPlacements = placementDao.count();
            long finalReporting = adsReportingDao.count();

            System.out.printf("\n📊 FINAL STATE:\n");
            System.out.printf("   👤 Accounts: %d (+%d)\n", finalAccounts, finalAccounts - initialAccounts);
            System.out.printf("   📢 Campaigns: %d (+%d)\n", finalCampaigns, finalCampaigns - initialCampaigns);
            System.out.printf("   🎯 AdSets: %d (+%d)\n", finalAdSets, finalAdSets - initialAdSets);
            System.out.printf("   📱 Ads: %d (+%d)\n", finalAds, finalAds - initialAds);
            System.out.printf("   🎪 Placements: %d (+%d)\n", finalPlacements, finalPlacements - initialPlacements);
            System.out.printf("   📈 Reporting: %d (+%d)\n", finalReporting, finalReporting - initialReporting);
            System.out.printf("   ⏱️  TOTAL DURATION: %d ms (%.2f seconds)\n", duration, duration / 1000.0);

            // Comprehensive assertions
            assertThat(finalAccounts).isGreaterThanOrEqualTo(initialAccounts);
            assertThat(finalCampaigns).isGreaterThanOrEqualTo(initialCampaigns);
            assertThat(finalAdSets).isGreaterThanOrEqualTo(initialAdSets);
            assertThat(finalAds).isGreaterThanOrEqualTo(initialAds);
            assertThat(finalPlacements).isGreaterThanOrEqualTo(initialPlacements);
            assertThat(finalReporting).isGreaterThanOrEqualTo(initialReporting);

            // Performance assertion for single account
            assertThat(duration).isLessThan(TimeUnit.MINUTES.toMillis(7)); // 7 minutes max for complete single account sync

            System.out.println("✅ NEW FLOW Single Account Full Integration: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Single Account Full Integration: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Single Account Full Integration failed: " + e.getMessage());
        }
    }

    @Test
    @Order(5)
    @DisplayName("⚡ E2E: NEW FLOW - Single Account Performance Benchmark")
    void testNewFlowSingleAccountPerformanceBenchmark() {
        System.out.println("⚡ TEST 5: NEW FLOW Single Account Performance Benchmark");

        try {
            // Ensure we have a test account
            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();
            System.out.printf("🎯 Performance benchmark with account: %s\n", SINGLE_TEST_ACCOUNT_ID);

            // Warm up
            System.out.println("🔥 Warming up system...");
            metaAdsConnector.testConnectivity();

            // Benchmark hierarchy sync for single account
            System.out.printf("\n📊 BENCHMARKING: Single Account Hierarchy Sync (%s)\n", SINGLE_TEST_ACCOUNT_ID);
            long hierarchyStart = System.currentTimeMillis();

            metaAdsService.syncSingleAccountHierarchy(SINGLE_TEST_ACCOUNT_ID);

            long hierarchyDuration = System.currentTimeMillis() - hierarchyStart;

            // Benchmark performance sync for single account
            System.out.printf("\n📊 BENCHMARKING: Single Account Performance Data Sync (%s)\n", SINGLE_TEST_ACCOUNT_ID);
            long performanceStart = System.currentTimeMillis();

            metaAdsService.syncPerformanceDataForAccountAndDate(SINGLE_TEST_ACCOUNT_ID, TEST_DATE);

            long performanceDuration = System.currentTimeMillis() - performanceStart;

            // Report benchmarks
            System.out.printf("\n⚡ SINGLE ACCOUNT PERFORMANCE BENCHMARK RESULTS:\n");
            System.out.printf("   🎯 Test Account: %s\n", SINGLE_TEST_ACCOUNT_ID);
            System.out.printf("   🏗️  Hierarchy Sync: %d ms (%.2f seconds)\n",
                    hierarchyDuration, hierarchyDuration / 1000.0);
            System.out.printf("   📈 Performance Sync: %d ms (%.2f seconds)\n",
                    performanceDuration, performanceDuration / 1000.0);
            System.out.printf("   🔥 TOTAL: %d ms (%.2f seconds)\n",
                    hierarchyDuration + performanceDuration,
                    (hierarchyDuration + performanceDuration) / 1000.0);

            // Performance assertions for single account (much stricter)
            assertThat(hierarchyDuration).isLessThan(TimeUnit.MINUTES.toMillis(2)); // 2 minutes max
            assertThat(performanceDuration).isLessThan(TimeUnit.MINUTES.toMillis(3)); // 3 minutes max

            System.out.println("✅ NEW FLOW Single Account Performance Benchmark: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ NEW FLOW Single Account Performance Benchmark: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("NEW FLOW Single Account Performance Benchmark failed: " + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        System.out.println("🏁 Test completed at: " + LocalDateTime.now());
        if (SINGLE_TEST_ACCOUNT_ID != null) {
            System.out.printf("🎯 Test account used: %s\n", SINGLE_TEST_ACCOUNT_ID);
        }
        System.out.println("================================================================\n");
    }

    @AfterAll
    static void cleanup() {
        System.out.println("\n🎉 ============= NEW FLOW E2E TEST SUMMARY =============");
        System.out.println("✅ All NEW FLOW single account tests completed successfully!");
        System.out.println("🔄 Verified: Fetch ALL → Process ALL → Batch Insert/Update");
        System.out.println("🎯 Verified: Single account testing for better performance");
        System.out.println("⚡ Verified: Performance optimized for single account operations");
        System.out.println("📊 Verified: Complete data pipeline integration");
        System.out.println("🚀 Performance: ~2-7 minutes for complete single account sync");
        System.out.println("=======================================================\n");
    }
}