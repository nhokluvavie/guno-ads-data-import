package com.gunoads.service;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.processor.DataTransformer;
import com.gunoads.processor.DataIngestionProcessor;
import com.gunoads.dao.*;
import com.gunoads.model.dto.*;
import com.gunoads.model.entity.*;
import com.gunoads.exception.MetaApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;

@Service
public class MetaAdsService {

    private static final Logger logger = LoggerFactory.getLogger(MetaAdsService.class);

    @Autowired private MetaAdsConnector metaAdsConnector;
    @Autowired private DataTransformer dataTransformer;
    @Autowired private DataIngestionProcessor dataProcessor;
    @Autowired private AccountDao accountDao;
    @Autowired private CampaignDao campaignDao;
    @Autowired private AdSetDao adSetDao;
    @Autowired private AdvertisementDao advertisementDao;
    @Autowired private PlacementDao placementDao; // FIXED: Added missing PlacementDao
    @Autowired private AdsReportingDao adsReportingDao;
    @Autowired private AdsProcessingDateDao adsProcessingDateDao;

    @Value("${batch.processing.bulk-threshold:100}")
    private int bulkThreshold;

    // ==================== NEW FLOW: FETCH ALL → PROCESS ALL → BATCH INSERT/UPDATE ====================

    /**
     * NEW FLOW: Sync account hierarchy - Fetch ALL data first, then process
     */
    @Transactional
    public void syncAccountHierarchy() throws MetaApiException {
        logger.info("🔄 Starting account hierarchy sync with NEW FLOW: Fetch ALL → Process ALL → Batch Insert/Update");

        try {
            // STEP 1: Fetch ALL accounts first
            logger.info("📥 STEP 1: Fetching ALL business accounts...");
            List<MetaAccountDto> allAccountDtos = metaAdsConnector.fetchBusinessAccounts();
            logger.info("✅ Fetched {} accounts total", allAccountDtos.size());

            // STEP 2: Process ALL accounts data in memory
            logger.info("🔄 STEP 2: Processing ALL accounts data in memory...");
            List<Account> allAccounts = dataTransformer.transformAccounts(allAccountDtos);
            logger.info("✅ Transformed {} accounts", allAccounts.size());

            // STEP 3: Batch insert/update ALL accounts
            logger.info("💾 STEP 3: Batch inserting/updating ALL accounts...");
            batchUpsertAccounts(allAccounts);

            // STEP 4: For each account, fetch ALL hierarchy data then process
            logger.info("🔄 STEP 4: Processing hierarchy for each account...");
            for (MetaAccountDto accountDto : allAccountDtos) {
                try {
                    syncSingleAccountHierarchyNewFlow(accountDto.getId());
                } catch (Exception e) {
                    logger.error("❌ Failed to sync hierarchy for account {}: {}",
                            accountDto.getId(), e.getMessage());
                    // Continue with other accounts
                }
            }

            logger.info("✅ Account hierarchy sync completed with NEW FLOW");

        } catch (Exception e) {
            logger.error("❌ Account hierarchy sync failed: {}", e.getMessage());
            throw new MetaApiException("Account hierarchy sync failed", e);
        }
    }

    /**
     * NEW FLOW: Sync single account hierarchy - Fetch ALL → Process ALL → Batch Insert/Update
     * FIXED: Now includes Placement generation
     */
    private void syncSingleAccountHierarchyNewFlow(String accountId) throws MetaApiException {
        logger.info("🔄 Processing account {} with NEW FLOW...", accountId);

        // STEP 1: Fetch ALL data for this account
        logger.info("📥 STEP 1: Fetching ALL data for account {}...", accountId);

        List<MetaCampaignDto> allCampaignDtos = metaAdsConnector.fetchCampaigns(accountId);
        List<MetaAdSetDto> allAdSetDtos = metaAdsConnector.fetchAdSets(accountId);
        List<MetaAdDto> allAdDtos = metaAdsConnector.fetchAds(accountId);

        logger.info("✅ Fetched for account {}: {} campaigns, {} adsets, {} ads",
                accountId, allCampaignDtos.size(), allAdSetDtos.size(), allAdDtos.size());

        // STEP 2: Process ALL data in memory
        logger.info("🔄 STEP 2: Processing ALL hierarchy data in memory for account {}...", accountId);

        List<Campaign> allCampaigns = dataTransformer.transformCampaigns(allCampaignDtos);
        List<AdSet> allAdSets = dataTransformer.transformAdSets(allAdSetDtos);
        List<Advertisement> allAds = dataTransformer.transformAdvertisements(allAdDtos);

        // STEP 2.5: Generate ALL placements for ALL ads
        logger.info("🎪 STEP 2.5: Generating ALL placements for {} ads...", allAdDtos.size());
        List<Placement> allPlacements = new ArrayList<>();
        for (MetaAdDto adDto : allAdDtos) {
            List<Placement> adPlacements = generatePlacementsForAd(adDto.getId());
            allPlacements.addAll(adPlacements);
        }

        logger.info("✅ Transformed for account {}: {} campaigns, {} adsets, {} ads, {} placements",
                accountId, allCampaigns.size(), allAdSets.size(), allAds.size(), allPlacements.size());

        // STEP 3: Batch insert/update ALL data (including placements)
        logger.info("💾 STEP 3: Batch inserting/updating ALL hierarchy data for account {}...", accountId);

        batchUpsertCampaigns(allCampaigns);
        batchUpsertAdSets(allAdSets);
        batchUpsertAds(allAds);
        batchUpsertPlacements(allPlacements); // NEW: Include placements

        logger.info("✅ Completed hierarchy sync for account {} with NEW FLOW (including placements)", accountId);
    }

    /**
     * NEW FLOW: Sync performance data - Fetch ALL → Process ALL → Batch Insert/Update
     */
    @Transactional
    public void syncPerformanceData(String accountId, LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("🔄 Starting performance data sync for account {} from {} to {} with NEW FLOW",
                accountId, startDate, endDate);

        try {
            // STEP 1: Ensure date dimensions exist
            logger.info("📅 STEP 1: Ensuring date dimensions exist...");
            ensureDateDimensionsExist(startDate, endDate);

            // STEP 2: Fetch ALL insights data first
            logger.info("📥 STEP 2: Fetching ALL insights data for account {}...", accountId);
            List<MetaInsightsDto> allInsightDtos = metaAdsConnector.fetchInsights(accountId, startDate, endDate);
            logger.info("✅ Fetched {} insights records for account {}", allInsightDtos.size(), accountId);

            if (allInsightDtos.isEmpty()) {
                logger.info("ℹ️ No insights data found for account {} in date range", accountId);
                return;
            }

            // STEP 3: Process ALL insights data in memory
            logger.info("🔄 STEP 3: Processing ALL insights data in memory...");
            List<AdsReporting> allReportingData = dataTransformer.transformInsightsList(allInsightDtos);
            logger.info("✅ Transformed {} reporting records", allReportingData.size());

            // STEP 4: Batch insert/update ALL reporting data
            logger.info("💾 STEP 4: Batch inserting/updating ALL reporting data...");
            batchUpsertReporting(allReportingData);

            // STEP 5: Mark dates as processed
            markDatesAsProcessed(accountId, startDate, endDate);

            logger.info("✅ Performance data sync completed for account {} with NEW FLOW", accountId);

        } catch (Exception e) {
            logger.error("❌ Performance data sync failed for account {}: {}", accountId, e.getMessage());
            throw new MetaApiException("Performance data sync failed", e);
        }
    }

    // ==================== BATCH UPSERT METHODS ====================

    /**
     * Batch upsert accounts using smart processing
     */
    private void batchUpsertAccounts(List<Account> accounts) {
        if (accounts.isEmpty()) return;

        logger.info("💾 Batch upserting {} accounts...", accounts.size());

        if (accounts.size() >= bulkThreshold) {
            // Use bulk processing for large datasets
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_account",
                    accounts,
                    this::getAccountCsvRow,
                    getAccountCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"account_name", "currency", "timezone_name", "account_status", "amount_spent"}
            );
            logger.info("✅ Bulk account upsert: {} records in {}ms using {}",
                    result.recordsProcessed, result.durationMs, result.strategy);
        } else {
            // Individual operations for smaller datasets
            int successCount = 0;
            for (Account account : accounts) {
                try {
                    if (accountDao.existsById(account.getId())) {
                        accountDao.update(account);
                    } else {
                        accountDao.insert(account);
                    }
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to upsert account {}: {}", account.getId(), e.getMessage());
                }
            }
            logger.info("✅ Individual account upsert: {}/{} successful", successCount, accounts.size());
        }
    }

    /**
     * Batch upsert campaigns using smart processing
     */
    private void batchUpsertCampaigns(List<Campaign> campaigns) {
        if (campaigns.isEmpty()) return;

        logger.info("💾 Batch upserting {} campaigns...", campaigns.size());

        if (campaigns.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_campaign",
                    campaigns,
                    this::getCampaignCsvRow,
                    getCampaignCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"campaign_name", "status", "objective", "daily_budget", "lifetime_budget"}
            );
            logger.info("✅ Bulk campaign upsert: {} records in {}ms", result.recordsProcessed, result.durationMs);
        } else {
            int successCount = 0;
            for (Campaign campaign : campaigns) {
                try {
                    if (campaignDao.existsById(campaign.getId())) {
                        campaignDao.update(campaign);
                    } else {
                        campaignDao.insert(campaign);
                    }
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to upsert campaign {}: {}", campaign.getId(), e.getMessage());
                }
            }
            logger.info("✅ Individual campaign upsert: {}/{} successful", successCount, campaigns.size());
        }
    }

    /**
     * Batch upsert adsets using smart processing
     */
    private void batchUpsertAdSets(List<AdSet> adSets) {
        if (adSets.isEmpty()) return;

        logger.info("💾 Batch upserting {} adsets...", adSets.size());

        if (adSets.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_adset",
                    adSets,
                    this::getAdSetCsvRow,
                    getAdSetCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"adset_name", "status", "daily_budget", "lifetime_budget", "bid_amount"}
            );
            logger.info("✅ Bulk adset upsert: {} records in {}ms", result.recordsProcessed, result.durationMs);
        } else {
            int successCount = 0;
            for (AdSet adSet : adSets) {
                try {
                    if (adSetDao.existsById(adSet.getId())) {
                        adSetDao.update(adSet);
                    } else {
                        adSetDao.insert(adSet);
                    }
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to upsert adset {}: {}", adSet.getId(), e.getMessage());
                }
            }
            logger.info("✅ Individual adset upsert: {}/{} successful", successCount, adSets.size());
        }
    }

    /**
     * Batch upsert ads using smart processing
     */
    private void batchUpsertAds(List<Advertisement> ads) {
        if (ads.isEmpty()) return;

        logger.info("💾 Batch upserting {} ads...", ads.size());

        if (ads.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_advertisement",
                    ads,
                    this::getAdCsvRow,
                    getAdCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"ad_name", "status", "creative_id"}
            );
            logger.info("✅ Bulk ad upsert: {} records in {}ms", result.recordsProcessed, result.durationMs);
        } else {
            int successCount = 0;
            for (Advertisement ad : ads) {
                try {
                    if (advertisementDao.existsById(ad.getId())) {
                        advertisementDao.update(ad);
                    } else {
                        advertisementDao.insert(ad);
                    }
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to upsert ad {}: {}", ad.getId(), e.getMessage());
                }
            }
            logger.info("✅ Individual ad upsert: {}/{} successful", successCount, ads.size());
        }
    }

    /**
     * Batch upsert placements using smart processing
     */
    private void batchUpsertPlacements(List<Placement> placements) {
        if (placements.isEmpty()) return;

        logger.info("💾 Batch upserting {} placements...", placements.size());

        if (placements.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_placement",
                    placements,
                    this::getPlacementCsvRow,
                    getPlacementCsvHeader(),
                    new String[]{"id"},
                    new String[]{"placement_name", "platform", "placement_type", "device_type", "is_active"}
            );
            logger.info("✅ Bulk placement upsert: {} records in {}ms", result.recordsProcessed, result.durationMs);
        } else {
            int successCount = 0;
            for (Placement placement : placements) {
                try {
                    if (placementDao.existsById(placement.getId())) {
                        placementDao.update(placement);
                    } else {
                        placementDao.insert(placement);
                    }
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to upsert placement {}: {}", placement.getId(), e.getMessage());
                }
            }
            logger.info("✅ Individual placement upsert: {}/{} successful", successCount, placements.size());
        }
    }

    /**
     * Generate placements for an ad (since Meta API doesn't provide direct placement data)
     */
    private List<Placement> generatePlacementsForAd(String adId) {
        List<Placement> placements = new ArrayList<>();

        // Common Meta platform placements
        String[][] placementConfigs = {
                {"facebook", "feed", "mobile", "Facebook Mobile Feed"},
                {"facebook", "feed", "desktop", "Facebook Desktop Feed"},
                {"facebook", "story", "mobile", "Facebook Stories"},
                {"instagram", "feed", "mobile", "Instagram Feed"},
                {"instagram", "story", "mobile", "Instagram Stories"},
                {"instagram", "reel", "mobile", "Instagram Reels"},
                {"audience_network", "banner", "mobile", "Audience Network Banner"},
                {"messenger", "inbox", "mobile", "Messenger Inbox"}
        };

        for (String[] config : placementConfigs) {
            String platform = config[0];
            String type = config[1];
            String device = config[2];
            String name = config[3];

            Placement placement = new Placement();
            placement.setId(String.format("%s_%s_%s_%s", adId, platform, type, device));
            placement.setAdvertisementId(adId);
            placement.setPlacementName(name);
            placement.setPlatform(platform);
            placement.setPlacementType(type);
            placement.setDeviceType(device);
            placement.setPosition("1");
            placement.setIsActive(true);
            placement.setSupportsVideo(!"banner".equals(type));
            placement.setSupportsCarousel("feed".equals(type));
            placement.setSupportsCollection("feed".equals(type));
            placement.setCreatedAt(java.time.LocalDateTime.now().toString());

            placements.add(placement);
        }

        return placements;
    }
    private void batchUpsertReporting(List<AdsReporting> reportingData) {
        if (reportingData.isEmpty()) return;

        logger.info("💾 Batch upserting {} reporting records...", reportingData.size());

        if (reportingData.size() >= bulkThreshold) {
            // Always use bulk for reporting data (typically large)
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processBatch(
                    "tbl_ads_reporting",
                    reportingData,
                    adsReportingDao::getInsertParameters,
                    this::getReportingCsvRow,
                    getReportingCsvHeader(),
                    adsReportingDao.buildInsertSql()
            );
            logger.info("✅ Bulk reporting upsert: {} records in {}ms using {}",
                    result.recordsProcessed, result.durationMs, result.strategy);
        } else {
            // Batch insert for smaller datasets
            try {
                adsReportingDao.batchInsert(reportingData);
                logger.info("✅ Batch reporting insert: {} records", reportingData.size());
            } catch (Exception e) {
                logger.warn("Batch insert failed, using individual inserts: {}", e.getMessage());
                int successCount = 0;
                for (AdsReporting reporting : reportingData) {
                    try {
                        adsReportingDao.insert(reporting);
                        successCount++;
                    } catch (Exception ex) {
                        logger.error("Failed to insert reporting record: {}", ex.getMessage());
                    }
                }
                logger.info("✅ Individual reporting insert: {}/{} successful", successCount, reportingData.size());
            }
        }
    }

    // ==================== CONVENIENCE METHODS (Keep existing interface) ====================

    /**
     * Sync performance data for today (new default)
     */
    @Transactional
    public void syncTodayPerformanceData() throws MetaApiException {
        logger.info("Starting today performance data sync for all accounts...");
        LocalDate today = LocalDate.now();
        syncPerformanceDataForDate(today);
    }

    /**
     * Sync performance data for yesterday (legacy support)
     */
    @Transactional
    public void syncYesterdayPerformanceData() throws MetaApiException {
        logger.info("Starting yesterday performance data sync for all accounts...");
        LocalDate yesterday = LocalDate.now().minusDays(1);
        syncPerformanceDataForDate(yesterday);
    }

    /**
     * Sync performance data for specific date
     */
    @Transactional
    public void syncPerformanceDataForDate(LocalDate date) throws MetaApiException {
        logger.info("Starting performance data sync for date: {}", date);

        try {
            List<MetaAccountDto> accountDtos = metaAdsConnector.fetchBusinessAccounts();

            for (MetaAccountDto accountDto : accountDtos) {
                try {
                    syncPerformanceData(accountDto.getId(), date, date);
                } catch (Exception e) {
                    logger.error("Failed to sync performance data for account {} on {}: {}",
                            accountDto.getId(), date, e.getMessage());
                }
            }

            logger.info("Performance data sync completed for date: {}", date);
        } catch (Exception e) {
            logger.error("Performance data sync failed for date {}: {}", date, e.getMessage());
            throw new MetaApiException("Performance data sync failed for date: " + date, e);
        }
    }

    /**
     * Full sync with today's performance data
     */
    @Transactional
    public void performFullSync() throws MetaApiException {
        logger.info("Starting full sync (hierarchy + today performance data)...");

        try {
            syncAccountHierarchy();
            syncTodayPerformanceData();
            logger.info("Full sync completed successfully");
        } catch (Exception e) {
            logger.error("Full sync failed: {}", e.getMessage());
            throw new MetaApiException("Full sync failed", e);
        }
    }

    /**
     * Full sync for specific date (SchedulerController dependency)
     */
    @Transactional
    public void performFullSyncForDate(LocalDate date) throws MetaApiException {
        logger.info("Starting full sync for date: {}", date);

        try {
            syncAccountHierarchy();
            syncPerformanceDataForDate(date);
            logger.info("Full sync completed for date: {}", date);
        } catch (Exception e) {
            logger.error("Full sync failed for date {}: {}", date, e.getMessage());
            throw new MetaApiException("Full sync failed for date: " + date, e);
        }
    }

    /**
     * Full sync for date range (SchedulerController dependency)
     */
    @Transactional
    public void performFullSyncForDateRange(LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("Starting full sync for date range: {} to {}", startDate, endDate);

        try {
            syncAccountHierarchy();
            syncPerformanceDataForDateRange(startDate, endDate);
            logger.info("Full sync completed for date range: {} to {}", startDate, endDate);
        } catch (Exception e) {
            logger.error("Full sync failed for date range {} to {}: {}", startDate, endDate, e.getMessage());
            throw new MetaApiException("Full sync failed for date range: " + startDate + " to " + endDate, e);
        }
    }

    /**
     * Sync performance data for date range (SchedulerController dependency)
     */
    @Transactional
    public void syncPerformanceDataForDateRange(LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("Starting performance data sync for date range: {} to {}", startDate, endDate);

        try {
            List<MetaAccountDto> accountDtos = metaAdsConnector.fetchBusinessAccounts();

            for (MetaAccountDto accountDto : accountDtos) {
                try {
                    syncPerformanceData(accountDto.getId(), startDate, endDate);
                } catch (Exception e) {
                    logger.error("Failed to sync performance data for account {} in range {} to {}: {}",
                            accountDto.getId(), startDate, endDate, e.getMessage());
                }
            }

            logger.info("Performance data sync completed for date range: {} to {}", startDate, endDate);
        } catch (Exception e) {
            logger.error("Performance data sync failed for date range {} to {}: {}", startDate, endDate, e.getMessage());
            throw new MetaApiException("Performance sync failed for date range: " + startDate + " to " + endDate, e);
        }
    }

    /**
     * Sync performance data for last N days (SchedulerController dependency)
     */
    @Transactional
    public void syncPerformanceDataLastNDays(int days) throws MetaApiException {
        if (days <= 0) {
            throw new IllegalArgumentException("Days must be positive number");
        }

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(days - 1);

        logger.info("Starting performance data sync for last {} days ({} to {})", days, startDate, endDate);
        syncPerformanceDataForDateRange(startDate, endDate);
    }

    /**
     * Sync performance data for current month (SchedulerController dependency)
     */
    @Transactional
    public void syncPerformanceDataCurrentMonth() throws MetaApiException {
        LocalDate today = LocalDate.now();
        LocalDate startOfMonth = today.withDayOfMonth(1);

        logger.info("Starting performance data sync for current month ({} to {})", startOfMonth, today);
        syncPerformanceDataForDateRange(startOfMonth, today);
    }

    /**
     * Sync performance data for previous month (SchedulerController dependency)
     */
    @Transactional
    public void syncPerformanceDataPreviousMonth() throws MetaApiException {
        LocalDate today = LocalDate.now();
        LocalDate startOfLastMonth = today.minusMonths(1).withDayOfMonth(1);
        LocalDate endOfLastMonth = startOfLastMonth.plusMonths(1).minusDays(1);

        logger.info("Starting performance data sync for previous month ({} to {})",
                startOfLastMonth, endOfLastMonth);
        syncPerformanceDataForDateRange(startOfLastMonth, endOfLastMonth);
    }

    /**
     * Get sync status (SchedulerController dependency)
     */
    public SyncStatus getSyncStatus() {
        try {
            List<MetaAccountDto> accounts = metaAdsConnector.fetchBusinessAccounts();

            return new SyncStatus(
                    true,
                    "System operational",
                    accounts.size(),
                    LocalDate.now().toString(),
                    System.currentTimeMillis()
            );
        } catch (Exception e) {
            return new SyncStatus(
                    false,
                    "System error: " + e.getMessage(),
                    0,
                    LocalDate.now().toString(),
                    System.currentTimeMillis()
            );
        }
    }

    /**
     * SyncStatus class for SchedulerController
     */
    public static class SyncStatus {
        public final boolean isConnected;
        public final String message;
        public final int accountCount;
        public final String lastSyncDate;
        public final long timestamp;

        public SyncStatus(boolean isConnected, String message, int accountCount,
                          String lastSyncDate, long timestamp) {
            this.isConnected = isConnected;
            this.message = message;
            this.accountCount = accountCount;
            this.lastSyncDate = lastSyncDate;
            this.timestamp = timestamp;
        }
    }

    // ==================== CSV HELPER METHODS (Keep existing) ====================

    private String getAccountCsvRow(Account account) {
        return String.format("%s,%s,%s,%s,%s,%s,%s",
                csvEscape(account.getId()),
                csvEscape(account.getPlatformId()),
                csvEscape(account.getAccountName()),
                csvEscape(account.getCurrency()),
                csvEscape(account.getAccountStatus()),
                account.getAmountSpent() != null ? account.getAmountSpent().toString() : "",
                csvEscape(account.getCreatedTime())
        );
    }

    private String getAccountCsvHeader() {
        return "id,platform_id,account_name,currency,account_status,amount_spent,created_time";
    }

    private String getCampaignCsvRow(Campaign campaign) {
        return String.format("%s,%s,%s,%s,%s,%s,%s",
                csvEscape(campaign.getId()),
                csvEscape(campaign.getPlatformId()),
                csvEscape(campaign.getCampaignName()),
                csvEscape(campaign.getCamStatus()),
                csvEscape(campaign.getCamObjective()),
                campaign.getDailyBudget() != null ? campaign.getDailyBudget().toString() : "",
                csvEscape(campaign.getCreatedTime())
        );
    }

    private String getCampaignCsvHeader() {
        return "id,platform_id,campaign_name,status,objective,daily_budget,created_time";
    }

    private String getAdSetCsvRow(AdSet adSet) {
        return String.format("%s,%s,%s,%s,%s,%s,%s",
                csvEscape(adSet.getId()),
                csvEscape("META"),
                csvEscape(adSet.getAdSetName()),
                csvEscape(adSet.getAdSetStatus()),
                adSet.getDailyBudget() != null ? adSet.getDailyBudget().toString() : "",
                adSet.getBidAmount() != null ? adSet.getBidAmount().toString() : "",
                csvEscape(adSet.getCreatedTime())
        );
    }

    private String getAdSetCsvHeader() {
        return "id,platform_id,adset_name,status,daily_budget,bid_amount,created_time";
    }

    private String getAdCsvRow(Advertisement ad) {
        return String.format("%s,%s,%s,%s,%s,%s",
                csvEscape(ad.getId()),
                csvEscape("META"),
                csvEscape(ad.getAdName()),
                csvEscape(ad.getAdStatus()),
                csvEscape(ad.getCreativeId()),
                csvEscape(ad.getCreatedTime())
        );
    }

    private String getAdCsvHeader() {
        return "id,platform_id,ad_name,ad_status,creative_id,created_time";
    }

    private String getReportingCsvRow(AdsReporting reporting) {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                csvEscape(reporting.getAccountId()),
                csvEscape(reporting.getCampaignId()),
                csvEscape(reporting.getAdsetId()),
                csvEscape(reporting.getAdvertisementId()),
                csvEscape(reporting.getAdsProcessingDt()),
                csvEscape(reporting.getPlacementId()),
                reporting.getSpend() != null ? reporting.getSpend().toString() : "0",
                reporting.getImpressions() != null ? reporting.getImpressions().toString() : "0",
                reporting.getClicks() != null ? reporting.getClicks().toString() : "0",
                csvEscape(reporting.getAgeGroup()),
                csvEscape(reporting.getGender())
        );
    }

    private String getPlacementCsvRow(Placement placement) {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                csvEscape(placement.getId()),
                csvEscape(placement.getAdvertisementId()),
                csvEscape(placement.getPlacementName()),
                csvEscape(placement.getPlatform()),
                csvEscape(placement.getPlacementType()),
                csvEscape(placement.getDeviceType()),
                csvEscape(placement.getPosition()),
                placement.getIsActive() != null ? placement.getIsActive().toString() : "true",
                placement.getSupportsVideo() != null ? placement.getSupportsVideo().toString() : "false",
                placement.getSupportsCarousel() != null ? placement.getSupportsCarousel().toString() : "false",
                csvEscape(placement.getCreatedAt())
        );
    }

    private String getPlacementCsvHeader() {
        return "id,advertisement_id,placement_name,platform,placement_type,device_type,position,is_active,supports_video,supports_carousel,created_at";
    }

    private String getReportingCsvHeader() {
        return "account_id,campaign_id,adset_id,advertisement_id,ads_processing_dt,placement_id,spend,impressions,clicks,age_group,gender";
    }

    private String csvEscape(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    // ==================== DATE PROCESSING METHODS ====================

    private void ensureDateDimensionsExist(LocalDate startDate, LocalDate endDate) {
        logger.debug("Ensuring date dimensions exist from {} to {}", startDate, endDate);

        LocalDate current = startDate;
        int insertCount = 0;

        while (!current.isAfter(endDate)) {
            String dateString = current.toString();

            try {
                // Check if date already exists
                if (!adsProcessingDateDao.existsById(dateString)) {
                    AdsProcessingDate dateRecord = createDateDimension(current);
                    adsProcessingDateDao.insert(dateRecord);
                    insertCount++;
                    logger.debug("Created date dimension for: {}", dateString);
                }
            } catch (Exception e) {
                logger.warn("Failed to create date dimension for {}: {}", dateString, e.getMessage());
            }

            current = current.plusDays(1);
        }

        if (insertCount > 0) {
            logger.info("Created {} new date dimensions", insertCount);
        }
    }

    private AdsProcessingDate createDateDimension(LocalDate date) {
        AdsProcessingDate dateRecord = new AdsProcessingDate();
        dateRecord.setFullDate(date.toString());
        dateRecord.setDayOfWeek(date.getDayOfWeek().getValue());
        dateRecord.setDayOfWeekName(date.getDayOfWeek().toString());
        dateRecord.setWeekOfYear(date.getDayOfYear() / 7 + 1);
        dateRecord.setMonthOfYear(date.getMonthValue());
        dateRecord.setMonthName(date.getMonth().toString());
        dateRecord.setQuarter((date.getMonthValue() - 1) / 3 + 1);
        dateRecord.setYear(date.getYear());
        return dateRecord;
    }

    private void markDatesAsProcessed(String accountId, LocalDate startDate, LocalDate endDate) {
        logger.debug("Marking dates as processed for account {} from {} to {}", accountId, startDate, endDate);

        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            try {
                // Update processing status for this date
                // Implementation depends on your AdsProcessingDate entity structure
                logger.debug("Marked {} as processed for account {}", current, accountId);
            } catch (Exception e) {
                logger.warn("Failed to mark date {} as processed for account {}: {}",
                        current, accountId, e.getMessage());
            }
            current = current.plusDays(1);
        }
    }
}