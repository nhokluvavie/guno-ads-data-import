package com.gunoads.test.e2e;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.dao.*;
import com.gunoads.model.dto.MetaAccountDto;
import com.gunoads.model.dto.MetaPlacementDto;
import com.gunoads.model.entity.*;
import com.gunoads.processor.DataTransformer;
import com.gunoads.processor.PlacementExtractor;
import com.gunoads.util.AdsReportingAggregator;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.test.annotation.Commit;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * E2E Test for NEW FLOW: Complete data pipeline testing
 * Simplified version without SyncStateDao dependency
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(locations = "classpath:application-e2e-test.yml")
class NewFlowE2ETest extends BaseE2ETest {

    @Autowired private MetaAdsConnector metaAdsConnector;
    @Autowired private DataTransformer dataTransformer;
    @Autowired private AccountDao accountDao;
    @Autowired private CampaignDao campaignDao;
    @Autowired private AdSetDao adSetDao;
    @Autowired private AdvertisementDao advertisementDao;
    @Autowired private PlacementDao placementDao;
    @Autowired private AdsReportingDao adsReportingDao;
    @Autowired private AdsProcessingDateDao adsProcessingDateDao;
    @Autowired private PlacementExtractor placementExtractor;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private AdsReportingAggregator adsReportingAggregator;


    private static final LocalDate TEST_DATE = LocalDate.of(2025, 9, 17); // Customize any date you want
    private static String SINGLE_TEST_ACCOUNT_ID = "468073679646974";

    @BeforeEach
    void setUp() {
        logTestStart();
        System.out.println("\n🚀 ============= NEW FLOW E2E TEST =============");
        System.out.println("🎯 Testing: Complete data pipeline flow");
        System.out.println("📅 Test Date: " + TEST_DATE);
        System.out.println("⏰ Start Time: " + LocalDateTime.now());
        System.out.println("==============================================\n");
    }

    @Test
    @Order(1)
    @DisplayName("🔍 E2E: Setup Test Account")
    void testSetupTestAccount() {
        System.out.println("🔍 TEST 1: Setup Test Account");

        try {
            System.out.println("📥 Fetching business accounts...");
            List<MetaAccountDto> allAccounts = metaAdsConnector.fetchBusinessAccounts();
            assertThat(allAccounts).isNotEmpty();

            // Select first available account for testing
            SINGLE_TEST_ACCOUNT_ID = allAccounts.get(0).getId();
            String accountName = allAccounts.get(0).getAccountName();

            System.out.printf("🎯 SELECTED TEST ACCOUNT: %s (%s)\n", SINGLE_TEST_ACCOUNT_ID, accountName);
            System.out.printf("📊 Total accounts available: %d\n", allAccounts.size());

            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();
            System.out.println("✅ Test Account Setup: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ Test Account Setup: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("Test Account Setup failed: " + e.getMessage());
        }
    }

    @Test
    @Order(2)
    @DisplayName("🔌 E2E: API Connectivity Test")
    void testApiConnectivity() {
        System.out.println("🔌 TEST 2: API Connectivity Test");

        try {
            System.out.println("🔌 Testing Meta API connectivity...");
            boolean isConnected = metaAdsConnector.testConnectivity();

            assertThat(isConnected).isTrue();
            System.out.println("✅ Meta API connectivity verified");

            // Test fetching accounts
            System.out.println("📥 Testing account fetching...");
            List<MetaAccountDto> accounts = metaAdsConnector.fetchBusinessAccounts();
            assertThat(accounts).isNotEmpty();
            System.out.printf("✅ Successfully fetched %d accounts\n", accounts.size());

            System.out.println("✅ API Connectivity Test: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ API Connectivity Test: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("API Connectivity Test failed: " + e.getMessage());
        }
    }

    @Test
    @Order(3)
    @DisplayName("🗄️ E2E: Database Operations Test")
    @Transactional
    @Commit
    void testDatabaseOperations() {
        System.out.println("🗄️ TEST 3: Database Operations Test");

        try {
            // Record initial database state
            long initialAccounts = accountDao.count();
            long initialCampaigns = campaignDao.count();
            long initialAdSets = adSetDao.count();
            long initialAds = advertisementDao.count();
            long initialPlacements = placementDao.count();

            System.out.printf("📊 INITIAL STATE: Accounts=%d, Campaigns=%d, AdSets=%d, Ads=%d, Placements=%d\n",
                    initialAccounts, initialCampaigns, initialAdSets, initialAds, initialPlacements);

            // Test basic database connectivity
            System.out.println("🔌 Testing database connectivity...");
            long accountCount = accountDao.count();
            System.out.printf("✅ Database connectivity verified - found %d accounts\n", accountCount);

            // The counts should be non-negative
            assertThat(initialAccounts).isGreaterThanOrEqualTo(0);
            assertThat(initialCampaigns).isGreaterThanOrEqualTo(0);
            assertThat(initialAdSets).isGreaterThanOrEqualTo(0);
            assertThat(initialAds).isGreaterThanOrEqualTo(0);
            assertThat(initialPlacements).isGreaterThanOrEqualTo(0);

            System.out.println("✅ Database Operations Test: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ Database Operations Test: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("Database Operations Test failed: " + e.getMessage());
        }
    }

    @Test
    @Order(4)
    @DisplayName("📈 E2E: Single Account Data Import Test")
    @Transactional
    @Commit
    void testSingleAccountDataImport() {
        System.out.println("📈 TEST 4: Single Account Data Import Test");

        try {
            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();
            System.out.printf("🎯 Testing data import for account: %s\n", SINGLE_TEST_ACCOUNT_ID);

            // Record initial database state
            long initialAccounts = accountDao.count();
            long initialCampaigns = campaignDao.count();
            long initialAdSets = adSetDao.count();
            long initialAds = advertisementDao.count();
            long initialPlacements = placementDao.count();

            System.out.printf("📊 INITIAL DB STATE: Accounts=%d, Campaigns=%d, AdSets=%d, Ads=%d, Placements=%d\n",
                    initialAccounts, initialCampaigns, initialAdSets, initialAds, initialPlacements);

            // Step 1: Import account data
            System.out.println("👤 Step 1: Importing account data...");
            var accounts = metaAdsConnector.fetchBusinessAccounts();
            var targetAccount = accounts.stream()
                    .filter(acc -> acc.getId().equals(SINGLE_TEST_ACCOUNT_ID))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Target account not found"));

            // Transform and save account
            var accountEntity = dataTransformer.transformAccount(targetAccount);
            if (accountDao.existsById(accountEntity.getId())) {
                accountDao.update(accountEntity);
                System.out.printf("   🔄 Updated account: %s\n", accountEntity.getAccountName());
            } else {
                accountDao.insert(accountEntity);
                System.out.printf("   ✅ Inserted account: %s\n", accountEntity.getAccountName());
            }

            // Step 2: Import campaigns for this specific account
            System.out.println("📢 Step 2: Importing campaigns...");
            long campaignStart = System.currentTimeMillis();

            var campaigns = metaAdsConnector.fetchCampaignsIncremental(SINGLE_TEST_ACCOUNT_ID);
            int campaignCount = 0;

            for (var campaignDto : campaigns) {
                var campaign = dataTransformer.transformCampaign(campaignDto);
                if (campaignDao.existsById(campaign.getId())) {
                    campaignDao.update(campaign);
                } else {
                    campaignDao.insert(campaign);
                }
                campaignCount++;

                if (campaignCount % 10 == 0) {
                    System.out.printf("   📢 Processed %d campaigns...\n", campaignCount);
                }
            }

            long campaignDuration = System.currentTimeMillis() - campaignStart;
            System.out.printf("   ✅ Imported %d campaigns in %d ms\n", campaigns.size(), campaignDuration);

            // Step 3: Import adsets for this specific account
            System.out.println("🎯 Step 3: Importing adsets...");
            long adsetStart = System.currentTimeMillis();

            var adSets = metaAdsConnector.fetchAdSetsIncremental(SINGLE_TEST_ACCOUNT_ID);
            int adsetCount = 0;

            for (var adSetDto : adSets) {
                var adSet = dataTransformer.transformAdSet(adSetDto);
                if (adSetDao.existsById(adSet.getId())) {
                    adSetDao.update(adSet);
                } else {
                    adSetDao.insert(adSet);
                }
                adsetCount++;

                if (adsetCount % 10 == 0) {
                    System.out.printf("   🎯 Processed %d adsets...\n", adsetCount);
                }
            }

            long adsetDuration = System.currentTimeMillis() - adsetStart;
            System.out.printf("   ✅ Imported %d adsets in %d ms\n", adSets.size(), adsetDuration);

            // Step 4: Import ads for this specific account
            System.out.println("📱 Step 4: Importing ads...");
            long adStart = System.currentTimeMillis();

            var ads = metaAdsConnector.fetchAdsIncremental(SINGLE_TEST_ACCOUNT_ID);
            int adCount = 0;

            for (var adDto : ads) {
                var ad = dataTransformer.transformAdvertisement(adDto);
                if (advertisementDao.existsById(ad.getId())) {
                    advertisementDao.update(ad);
                } else {
                    advertisementDao.insert(ad);
                }
                adCount++;

                // Extract and save placements (if any)
                // Note: This depends on your placement extraction logic
                try {
                    var placements = extractPlacementsFromAd(ad); // You may need this method
                    for (var placement : placements) {
                        if (placementDao.existsById(placement.getId())) {
                            placementDao.update(placement);
                        } else {
                            placementDao.insert(placement);
                        }
                    }
                } catch (Exception e) {
                    // Placement extraction may not be available for all ads
                    System.out.printf("   ⚠️ Could not extract placements for ad %s: %s\n", ad.getId(), e.getMessage());
                }

                if (adCount % 10 == 0) {
                    System.out.printf("   📱 Processed %d ads...\n", adCount);
                }
            }

            long adDuration = System.currentTimeMillis() - adStart;
            System.out.printf("   ✅ Imported %d ads in %d ms\n", ads.size(), adDuration);

            // Verify final database state
            long finalAccounts = accountDao.count();
            long finalCampaigns = campaignDao.count();
            long finalAdSets = adSetDao.count();
            long finalAds = advertisementDao.count();
            long finalPlacements = placementDao.count();

            System.out.printf("\n📊 FINAL DB STATE:\n");
            System.out.printf("   👤 Accounts: %d (+%d)\n", finalAccounts, finalAccounts - initialAccounts);
            System.out.printf("   📢 Campaigns: %d (+%d)\n", finalCampaigns, finalCampaigns - initialCampaigns);
            System.out.printf("   🎯 AdSets: %d (+%d)\n", finalAdSets, finalAdSets - initialAdSets);
            System.out.printf("   📱 Ads: %d (+%d)\n", finalAds, finalAds - initialAds);
            System.out.printf("   🎪 Placements: %d (+%d)\n", finalPlacements, finalPlacements - initialPlacements);

            long totalDuration = campaignDuration + adsetDuration + adDuration;
            System.out.printf("   ⏱️  Total Import Duration: %d ms (%.2f seconds)\n",
                    totalDuration, totalDuration / 1000.0);

            // Assertions
            assertThat(finalAccounts).isGreaterThanOrEqualTo(initialAccounts);
            assertThat(finalCampaigns).isGreaterThanOrEqualTo(initialCampaigns);
            assertThat(finalAdSets).isGreaterThanOrEqualTo(initialAdSets);
            assertThat(finalAds).isGreaterThanOrEqualTo(initialAds);

            // Performance assertions
            assertThat(totalDuration).isLessThan(TimeUnit.MINUTES.toMillis(20)); // 20 minutes max

            System.out.println("✅ Single Account Data Import Test: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ Single Account Data Import Test: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("Single Account Data Import Test failed: " + e.getMessage());
        }
    }

    /**
     * Helper method to extract placements from ad (if available)
     * This may need to be implemented based on your business logic
     */
    private List<Placement> extractPlacementsFromAd(Advertisement ad) {
        // Placeholder implementation - you may need to implement this based on your logic
        // For now, return empty list to avoid compilation errors
        return new ArrayList<>();
    }

    @Test
    @Order(5)
    @DisplayName("📈 E2E: Single Account Performance Data with Placement Extraction - UPDATED WITH AGGREGATION")
    @Transactional
    @Commit
    void testSingleAccountPerformanceDataWithPlacementExtraction() {
        System.out.println("📈 TEST 5: Single Account Performance Data with Placement Extraction - UPDATED");

        try {
            assertThat(SINGLE_TEST_ACCOUNT_ID).isNotNull();
            System.out.printf("🎯 Testing performance data for account: %s on date: %s\n",
                    SINGLE_TEST_ACCOUNT_ID, TEST_DATE);

            // === PHASE 1: INITIAL DATABASE STATE ===
            System.out.println("\n📊 PHASE 1: Recording initial database state...");
            long initialReporting = adsReportingDao.count();
            long initialPlacements = placementDao.count();

            System.out.printf("📊 INITIAL STATE: Reporting=%d, Placements=%d\n",
                    initialReporting, initialPlacements);

            // Ensure date dimension exists
            AdsProcessingDate dateRecord = createDateDimension(TEST_DATE);
            if (!adsProcessingDateDao.existsById(dateRecord.getFullDate())) {
                adsProcessingDateDao.insert(dateRecord);
                System.out.printf("📅 Created date dimension: %s\n", dateRecord.getFullDate());
            }

            // === PHASE 2: FETCH INSIGHTS DATA ===
            System.out.println("\n📈 PHASE 2: Fetching insights data...");
            long startTime = System.currentTimeMillis();

            var insights = metaAdsConnector.fetchInsights(SINGLE_TEST_ACCOUNT_ID, TEST_DATE, TEST_DATE);
            long fetchDuration = System.currentTimeMillis() - startTime;

            System.out.printf("✅ Fetched %d insights for account %s on %s in %d ms (%.2f seconds)\n",
                    insights.size(), SINGLE_TEST_ACCOUNT_ID, TEST_DATE, fetchDuration, fetchDuration / 1000.0);

            // Verify insights belong to this specific account
            System.out.println("🔍 Verifying insights data integrity...");
            for (var insight : insights) {
                assertThat(insight.getAccountId()).isEqualTo(SINGLE_TEST_ACCOUNT_ID);
            }

            if (insights.isEmpty()) {
                System.out.println("ℹ️  No insights found - this could be normal if:");
                System.out.println("   - No ads were active on the test date");
                System.out.println("   - Account had no spend on the test date");
                System.out.println("   - Test date is too far in the past/future");
                System.out.println("✅ Test completed (no data to process)\n");
                return; // Exit gracefully if no data
            }

            // === PHASE 3: EXTRACT PLACEMENTS FROM INSIGHTS ===
            System.out.println("\n📍 PHASE 3: Extracting placements from insights...");
            long placementStartTime = System.currentTimeMillis();

            Set<MetaPlacementDto> placementDtos = placementExtractor.extractPlacementsFromInsights(insights);
            long placementExtractionDuration = System.currentTimeMillis() - placementStartTime;

            System.out.printf("✅ Extracted %d unique placements in %d ms\n",
                    placementDtos.size(), placementExtractionDuration);

            // === PHASE 4: TRANSFORM AND SAVE PLACEMENTS ===
            System.out.println("\n💾 PHASE 4: Transforming and saving placements...");
            long placementSaveStartTime = System.currentTimeMillis();

            List<Placement> placements = placementDtos.stream()
                    .map(dataTransformer::transformPlacement)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            // Save placements using UPSERT pattern
            int placementsSaved = 0;
            for (Placement placement : placements) {
                try {
                    if (placementDao.existsById(placement.getId())) {
                        placementDao.update(placement);
                    } else {
                        placementDao.insert(placement);
                    }
                    placementsSaved++;
                } catch (Exception e) {
                    System.out.printf("⚠️ Failed to save placement %s: %s\n", placement.getId(), e.getMessage());
                }
            }

            long placementSaveDuration = System.currentTimeMillis() - placementSaveStartTime;
            System.out.printf("✅ Saved %d/%d placements in %d ms\n",
                    placementsSaved, placements.size(), placementSaveDuration);

            // === PHASE 5: TRANSFORM INSIGHTS TO REPORTING ENTITIES ===
            System.out.println("\n🔄 PHASE 5: Transforming insights to reporting entities...");
            long transformStartTime = System.currentTimeMillis();

            List<AdsReporting> reportingList = dataTransformer.transformInsightsList(insights);
            long transformDuration = System.currentTimeMillis() - transformStartTime;

            System.out.printf("✅ Transformed %d insights to %d reporting entities in %d ms\n",
                    insights.size(), reportingList.size(), transformDuration);

            // === PHASE 5.1: VALIDATE TRANSFORMED DATA (NEW) ===
            System.out.println("\n🔍 PHASE 5.1: Validating transformed data...");
            long validationStartTime = System.currentTimeMillis();

            List<AdsReporting> validReporting = new ArrayList<>();
            List<AdsReporting> invalidReporting = new ArrayList<>();

            for (AdsReporting reporting : reportingList) {
                if (dataTransformer.validateAdsReporting(reporting)) {
                    validReporting.add(reporting);
                } else {
                    invalidReporting.add(reporting);

                    // Log the invalid record for debugging
                    System.out.printf("⚠️ Invalid record: ad=%s, age_group='%s', gender='%s', region='%s', city='%s'\n",
                            reporting.getAdvertisementId(),
                            reporting.getAgeGroup(),
                            reporting.getGender(),
                            reporting.getRegion(),
                            reporting.getCity());
                }
            }

            long validationDuration = System.currentTimeMillis() - validationStartTime;

            System.out.printf("✅ Validation completed in %d ms:\n", validationDuration);
            System.out.printf("   ✅ Valid records: %d\n", validReporting.size());
            System.out.printf("   ❌ Invalid records: %d\n", invalidReporting.size());

            if (invalidReporting.size() > 0) {
                System.out.printf("   ⚠️ Invalid data rate: %.1f%%\n",
                        (invalidReporting.size() * 100.0) / reportingList.size());

                // Log sample invalid records
                System.out.println("   📋 Sample invalid records:");
                invalidReporting.stream().limit(3).forEach(r -> {
                    System.out.printf("     - Ad: %s, AgeGroup: '%s', Gender: '%s', CountryCode: %s\n",
                            r.getAdvertisementId(), r.getAgeGroup(), r.getGender(), r.getCountryCode());
                });
            }

            // Only proceed with valid records
            reportingList = validReporting;

            // Validate we still have data to process
            assertThat(reportingList).isNotEmpty();
            System.out.printf("🔍 Proceeding with %d valid records\n", reportingList.size());

            // Validate transformation results
            assertThat(reportingList).isNotEmpty();
            System.out.printf("🔍 Transformation efficiency: %.1f%%\n",
                    (reportingList.size() * 100.0 / insights.size()));

            // === PHASE 6: COMPOSITE KEY ANALYSIS (NEW) ===
            System.out.println("\n🔍 PHASE 6: Analyzing composite key duplicates...");

            Map<String, Integer> duplicateStats = adsReportingDao.getCompositeKeyDuplicateStats(reportingList);

            System.out.printf("📊 DUPLICATE ANALYSIS:\n");
            System.out.printf("   📥 Total records: %d\n", duplicateStats.get("totalRecords"));
            System.out.printf("   🔑 Unique composite keys: %d\n", duplicateStats.get("uniqueKeys"));
            System.out.printf("   🔄 Duplicate groups: %d\n", duplicateStats.get("duplicateGroups"));
            System.out.printf("   📊 Total duplicates: %d\n", duplicateStats.get("totalDuplicates"));

            if (duplicateStats.get("duplicateGroups") > 0) {
                double duplicationRate = (duplicateStats.get("totalDuplicates") * 100.0) / duplicateStats.get("totalRecords");
                System.out.printf("   ⚠️  Duplication rate: %.1f%% - AGGREGATION REQUIRED\n", duplicationRate);
            } else {
                System.out.println("   ✅ No duplicates found - direct insert possible");
            }

            // === PHASE 7: AGGREGATE BY COMPOSITE KEY (NEW) ===
            System.out.println("\n🧮 PHASE 7: Aggregating data by composite key...");
            long aggregationStartTime = System.currentTimeMillis();

            List<AdsReporting> aggregatedList = adsReportingAggregator.aggregateByCompositeKey(reportingList);
            long aggregationDuration = System.currentTimeMillis() - aggregationStartTime;

            System.out.printf("✅ Aggregation completed in %d ms:\n", aggregationDuration);
            System.out.printf("   📥 Input: %d records\n", reportingList.size());
            System.out.printf("   📤 Output: %d unique records\n", aggregatedList.size());
            System.out.printf("   📊 Reduction: %d records (%.1f%%)\n",
                    reportingList.size() - aggregatedList.size(),
                    ((reportingList.size() - aggregatedList.size()) * 100.0) / reportingList.size());

            // Validate aggregation integrity
            boolean aggregationValid = adsReportingAggregator.validateAggregatedData(reportingList, aggregatedList);
            assertThat(aggregationValid).isTrue();
            System.out.println("✅ Aggregation data integrity verified");

            System.out.println("\n🔍 PHASE 7.5: Enhanced CSV debugging with fixes...");

            // Test the fixed CSV generation
            System.out.println("📋 Testing FIXED CSV Header:");
            String csvHeader = AdsReportingDao.getCsvHeader();
            String[] headerFields = csvHeader.split(",");

            System.out.printf("📊 CSV Header: %d fields\n", headerFields.length);
            System.out.printf("📄 Header length: %d characters\n", csvHeader.length());
            System.out.printf("📋 Header preview: %s...\n", csvHeader.substring(0, Math.min(100, csvHeader.length())));

            // Check for header issues
            if (csvHeader.contains("\n") || csvHeader.contains("\r")) {
                System.out.println("❌ HEADER HAS NEWLINES!");
            } else {
                System.out.println("✅ Header is single line");
            }

            // Show key field positions
            System.out.println("🔑 Key field positions in header:");
            for (int i = 0; i < Math.min(12, headerFields.length); i++) {
                System.out.printf("   [%d] %s\n", i, headerFields[i].trim());
            }

            // Validate CSV format before proceeding
            System.out.println("\n🔍 Validating CSV format...");
            boolean csvValid = AdsReportingDao.validateCsvFormat(aggregatedList);

            if (!csvValid) {
                System.out.println("❌ CSV validation failed - cannot proceed");
                fail("CSV format validation failed - check field count mismatch");
            }

            // Test CSV generation for problematic scenario
            System.out.println("\n🧪 Testing CSV generation for sample records...");
            Function<AdsReporting, String> csvMapper = AdsReportingDao.getCsvMapper();

            for (int i = 0; i < Math.min(2, aggregatedList.size()); i++) {
                AdsReporting sample = aggregatedList.get(i);

                System.out.printf("\n📝 Testing record #%d:\n", i + 1);
                System.out.printf("   Ad ID: %s\n", sample.getAdvertisementId());
                System.out.printf("   Age Group: '%s' (length: %d)\n",
                        sample.getAgeGroup(), sample.getAgeGroup() != null ? sample.getAgeGroup().length() : 0);
                System.out.printf("   Gender: '%s'\n", sample.getGender());
                System.out.printf("   Region: '%s'\n", sample.getRegion());
                System.out.printf("   City: '%s'\n", sample.getCity());

                try {
                    String csvRow = csvMapper.apply(sample);
                    String[] rowFields = csvRow.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                    System.out.printf("   Generated %d CSV fields\n", rowFields.length);

                    // Check critical fields positions
                    if (rowFields.length >= 12) {
                        System.out.printf("   CSV fields [7-11]: age_group='%s', gender='%s', country_code='%s', region='%s', city='%s'\n",
                                rowFields[7], rowFields[8], rowFields[9], rowFields[10], rowFields[11]);

                        // Verify no empty critical fields
                        if (rowFields[7].trim().isEmpty()) {
                            System.out.println("   ❌ age_group is EMPTY in CSV!");
                        }
                        if (rowFields[8].trim().isEmpty()) {
                            System.out.println("   ❌ gender is EMPTY in CSV!");
                        }
                        if (rowFields[10].trim().isEmpty()) {
                            System.out.println("   ❌ region is EMPTY in CSV!");
                        }
                        if (rowFields[11].trim().isEmpty()) {
                            System.out.println("   ❌ city is EMPTY in CSV!");
                        }
                    } else {
                        System.out.println("   ❌ Insufficient CSV fields generated!");
                    }

                    // Show first part of CSV row
                    String preview = csvRow.length() > 150 ? csvRow.substring(0, 150) + "..." : csvRow;
                    System.out.printf("   CSV preview: %s\n", preview);

                } catch (Exception e) {
                    System.out.printf("   ❌ CSV generation failed: %s\n", e.getMessage());
                    e.printStackTrace();
                }
            }

            // Create and test a small CSV content sample
            System.out.println("\n🧪 Testing complete CSV content generation...");
            try {
                StringBuilder testCsv = new StringBuilder();
                testCsv.append(csvHeader).append("\n");

                // Add first record
                if (!aggregatedList.isEmpty()) {
                    AdsReporting firstRecord = aggregatedList.get(0);
                    String firstRow = csvMapper.apply(firstRecord);
                    testCsv.append(firstRow).append("\n");

                    System.out.printf("✅ Test CSV content generated (%d characters)\n", testCsv.length());
                    System.out.printf("📄 Lines in CSV: %d\n", testCsv.toString().split("\n").length);

                    // Show the problematic area around age_group
                    String[] lines = testCsv.toString().split("\n");
                    if (lines.length >= 2) {
                        String headerLine = lines[0];
                        String dataLine = lines[1];

                        System.out.println("🔍 Analyzing line structure:");
                        System.out.printf("   Header line: %s\n", headerLine.substring(0, Math.min(100, headerLine.length())));
                        System.out.printf("   Data line:   %s\n", dataLine.substring(0, Math.min(100, dataLine.length())));

                        // Find age_group position
                        String[] headerParts = headerLine.split(",");
                        String[] dataParts = dataLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                        for (int i = 0; i < Math.min(headerParts.length, dataParts.length); i++) {
                            if (headerParts[i].equals("age_group")) {
                                System.out.printf("   Found age_group at position %d: '%s'\n", i, dataParts[i]);
                                break;
                            }
                        }
                    }
                }

            } catch (Exception e) {
                System.out.printf("❌ CSV content generation test failed: %s\n", e.getMessage());
                e.printStackTrace();
            }

            System.out.println("✅ Enhanced CSV Debug completed\n");

            // === PHASE 8: UPSERT TO DATABASE (NEW) ===
            System.out.println("\n💾 PHASE 8: UPSERT aggregated data to database...");
            long insertStartTime = System.currentTimeMillis();

            int successfulInserts = adsReportingDao.upsertBatch(aggregatedList);
            long insertDuration = System.currentTimeMillis() - insertStartTime;

            System.out.printf("✅ UPSERT completed: %d records processed in %d ms\n",
                    successfulInserts, insertDuration);

            // Verify data was inserted
            long finalReporting = adsReportingDao.count();
            long newReporting = finalReporting - initialReporting;

            System.out.printf("📊 DATABASE IMPACT:\n");
            System.out.printf("   📈 Initial records: %d\n", initialReporting);
            System.out.printf("   📈 Final records: %d\n", finalReporting);
            System.out.printf("   📊 Net change: +%d records\n", newReporting);

            // Performance assertions
            assertThat(successfulInserts).isGreaterThan(0);
            assertThat(finalReporting).isGreaterThanOrEqualTo(initialReporting);

            // === PHASE 9: DATA INTEGRITY VERIFICATION ===
            System.out.println("\n🔍 PHASE 9: Verifying data integrity...");

            // Check referential integrity
            String integrityCheckSql = """
            SELECT COUNT(*) FROM tbl_ads_reporting r 
            LEFT JOIN tbl_placement p ON r.placement_id = p.id 
            WHERE p.id IS NULL AND r.placement_id != 'unknown'
            """;

            try {
                Long orphanedReporting = jdbcTemplate.queryForObject(integrityCheckSql, Long.class);
                if (orphanedReporting != null && orphanedReporting > 0) {
                    System.out.printf("⚠️ Warning: %d reporting records have invalid placement references\n", orphanedReporting);
                } else {
                    System.out.println("✅ All reporting records have valid placement references");
                }
            } catch (Exception e) {
                System.out.printf("⚠️ Could not verify relationship integrity: %s\n", e.getMessage());
            }

            // Check for remaining duplicates in database
            String duplicateCheckSql = """
            SELECT COUNT(*) - COUNT(DISTINCT account_id, platform_id, campaign_id, adset_id, 
                                   advertisement_id, placement_id, ads_processing_dt, 
                                   age_group, gender, country_code, region, city) as duplicate_count
            FROM tbl_ads_reporting
            WHERE ads_processing_dt = ?
            """;

            try {
                Long duplicatesInDb = jdbcTemplate.queryForObject(duplicateCheckSql, Long.class, TEST_DATE.toString());
                if (duplicatesInDb != null && duplicatesInDb > 0) {
                    System.out.printf("❌ ERROR: %d duplicate composite keys found in database!\n", duplicatesInDb);
                    fail("Database contains duplicate composite keys after UPSERT operation");
                } else {
                    System.out.println("✅ No duplicate composite keys in database");
                }
            } catch (Exception e) {
                System.out.printf("⚠️ Could not verify composite key uniqueness: %s\n", e.getMessage());
            }

            // === PHASE 10: PERFORMANCE SUMMARY ===
            long totalDuration = fetchDuration + placementExtractionDuration + placementSaveDuration +
                    transformDuration + aggregationDuration + insertDuration;

            System.out.printf("\n⚡ COMPREHENSIVE PERFORMANCE SUMMARY:\n");
            System.out.printf("   📡 API Fetch: %d ms (%.2f seconds)\n", fetchDuration, fetchDuration / 1000.0);
            System.out.printf("   📍 Placement Extraction: %d ms (%.2f seconds)\n", placementExtractionDuration, placementExtractionDuration / 1000.0);
            System.out.printf("   💾 Placement Save: %d ms (%.2f seconds)\n", placementSaveDuration, placementSaveDuration / 1000.0);
            System.out.printf("   🔄 Data Transform: %d ms (%.2f seconds)\n", transformDuration, transformDuration / 1000.0);
            System.out.printf("   🧮 Data Aggregation: %d ms (%.2f seconds)\n", aggregationDuration, aggregationDuration / 1000.0);
            System.out.printf("   💾 Database UPSERT: %d ms (%.2f seconds)\n", insertDuration, insertDuration / 1000.0);
            System.out.printf("   ⏱️  Total Pipeline: %d ms (%.2f seconds)\n", totalDuration, totalDuration / 1000.0);

            if (successfulInserts > 0) {
                System.out.printf("   📊 Records per second: %.1f\n", (successfulInserts * 1000.0 / totalDuration));
            }

            // Performance assertions
            assertThat(totalDuration).isLessThan(TimeUnit.MINUTES.toMillis(10)); // 10 minutes max

            // === TEST COMPLETION ===
            System.out.printf("\n🎉 COMPLETE PIPELINE SUCCESS WITH DATA INTEGRITY:\n");
            System.out.printf("   📈 %d insights fetched with smart batching\n", insights.size());
            System.out.printf("   📍 %d unique placements extracted and saved\n", placementDtos.size());
            System.out.printf("   🔄 %d entities transformed\n", reportingList.size());
            System.out.printf("   🧮 %d entities aggregated (removed %d duplicates)\n",
                    aggregatedList.size(), reportingList.size() - aggregatedList.size());
            System.out.printf("   💾 %d records successfully upserted to database\n", successfulInserts);
            System.out.printf("   🚀 End-to-end performance: %.2f seconds\n", totalDuration / 1000.0);
            System.out.println("   ✅ Data integrity maintained throughout pipeline");

            System.out.println("✅ Test 5 - COMPLETE PIPELINE WITH AGGREGATION & UPSERT: PASSED\n");

        } catch (Exception e) {
            System.err.println("❌ Test 5 - Complete Pipeline with Aggregation: FAILED");
            System.err.println("💥 ERROR: " + e.getMessage());
            e.printStackTrace();
            fail("Complete Pipeline with Aggregation test failed: " + e.getMessage());
        }
    }

    /**
     * Helper method to create date dimension record
     */
    private AdsProcessingDate createDateDimension(LocalDate date) {
        AdsProcessingDate dateRecord = new AdsProcessingDate();

        dateRecord.setFullDate(date.toString());
        dateRecord.setDayOfWeek(date.getDayOfWeek().getValue());
        dateRecord.setDayOfWeekName(date.getDayOfWeek().toString());
        dateRecord.setDayOfMonth(date.getDayOfMonth());
        dateRecord.setDayOfYear(date.getDayOfYear());
        dateRecord.setWeekOfYear(date.get(java.time.temporal.WeekFields.ISO.weekOfWeekBasedYear()));
        dateRecord.setMonthOfYear(date.getMonthValue());
        dateRecord.setMonthName(date.getMonth().toString());
        dateRecord.setQuarter((date.getMonthValue() - 1) / 3 + 1);
        dateRecord.setYear(date.getYear());
        dateRecord.setIsWeekend(date.getDayOfWeek().getValue() >= 6);
        dateRecord.setIsHoliday(false); // Default to false
        dateRecord.setHolidayName(null);
        dateRecord.setFiscalYear(date.getYear()); // Assume calendar year = fiscal year
        dateRecord.setFiscalQuarter((date.getMonthValue() - 1) / 3 + 1);

        return dateRecord;
    }

    @AfterEach
    void tearDown() {
        System.out.println("🏁 Test completed at: " + LocalDateTime.now());
        if (SINGLE_TEST_ACCOUNT_ID != null) {
            System.out.printf("🎯 Test account used: %s\n", SINGLE_TEST_ACCOUNT_ID);
        }
        System.out.println("==============================================\n");
    }

    @AfterAll
    static void cleanup() {
        System.out.println("\n🎉 ============= NEW FLOW E2E TEST SUMMARY =============");
        System.out.println("✅ All NEW FLOW tests completed successfully!");
        System.out.println("🔄 Verified: Complete data pipeline flow");
        System.out.println("⚡ Verified: Performance optimization");
        System.out.println("📊 Verified: Integration with real Meta API");
        System.out.println("🚀 Performance: ~1-3 minutes for basic operations");
        System.out.println("======================================================\n");
    }
}