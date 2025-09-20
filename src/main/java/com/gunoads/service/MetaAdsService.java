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
    @Autowired private PlacementDao placementDao;
    @Autowired private AdsReportingDao adsReportingDao;
    @Autowired private AdsProcessingDateDao adsProcessingDateDao;

    @Value("${batch.processing.bulk-threshold:100}")
    private int bulkThreshold;

    // ==================== CORE SYNC METHODS ====================

    /**
     * Sync account hierarchy - Fetch ALL ACTIVE → Process ALL → Batch Insert/Update
     */
    @Transactional
    public void syncAccountHierarchy() throws MetaApiException {
        logger.info("🔄 Starting ACTIVE account hierarchy sync");

        try {
            // Step 1: Fetch ALL ACTIVE accounts
            List<MetaAccountDto> activeAccounts = metaAdsConnector.fetchBusinessAccounts();
            logger.info("✅ Fetched {} ACTIVE accounts", activeAccounts.size());

            // Step 2: Process accounts
            List<Account> accounts = dataTransformer.transformAccounts(activeAccounts);
            batchUpsertAccounts(accounts);

            // Step 3: Process hierarchy for each account
            for (MetaAccountDto accountDto : activeAccounts) {
                try {
                    syncSingleAccountHierarchy(accountDto.getId());
                } catch (Exception e) {
                    logger.error("❌ Failed to sync hierarchy for account {}: {}",
                            accountDto.getId(), e.getMessage());
                }
            }

            logger.info("✅ Account hierarchy sync completed");

        } catch (Exception e) {
            logger.error("❌ Account hierarchy sync failed: {}", e.getMessage());
            throw new MetaApiException("Account hierarchy sync failed", e);
        }
    }

    /**
     * Sync single account hierarchy - for E2E testing
     */
    @Transactional
    public void syncSingleAccountHierarchy(String accountId) throws MetaApiException {
        logger.info("🔄 Syncing ACTIVE hierarchy for account: {}", accountId);

        try {
            // Fetch ALL ACTIVE data for this account
            List<MetaCampaignDto> campaigns = metaAdsConnector.fetchCampaigns(accountId);
            List<MetaAdSetDto> adSets = metaAdsConnector.fetchAdSets(accountId);
            List<MetaAdDto> ads = metaAdsConnector.fetchAds(accountId);

            logger.info("✅ Fetched ACTIVE entities: {} campaigns, {} adsets, {} ads",
                    campaigns.size(), adSets.size(), ads.size());

            // Transform and batch upsert
            batchUpsertCampaigns(dataTransformer.transformCampaigns(campaigns));
            batchUpsertAdSets(dataTransformer.transformAdSets(adSets));
            batchUpsertAdvertisements(dataTransformer.transformAdvertisements(ads));

            // Generate and upsert placements
            List<Placement> placements = generatePlacementsForAds(ads);
            batchUpsertPlacements(placements);

            logger.info("✅ Single account hierarchy sync completed: {}", accountId);

        } catch (Exception e) {
            logger.error("❌ Single account hierarchy sync failed for {}: {}", accountId, e.getMessage());
            throw new MetaApiException("Single account hierarchy sync failed", e);
        }
    }

    /**
     * Sync performance data for specific date
     */
    @Transactional
    public void syncPerformanceDataForDate(LocalDate date) throws MetaApiException {
        logger.info("📈 Starting performance data sync for date: {}", date);

        try {
            List<MetaAccountDto> accounts = metaAdsConnector.fetchBusinessAccounts();

            for (MetaAccountDto account : accounts) {
                try {
                    syncPerformanceDataForAccountAndDate(account.getId(), date);
                } catch (Exception e) {
                    logger.error("Failed to sync performance data for account {} on {}: {}",
                            account.getId(), date, e.getMessage());
                }
            }

            logger.info("✅ Performance data sync completed for date: {}", date);

        } catch (Exception e) {
            logger.error("❌ Performance data sync failed for date {}: {}", date, e.getMessage());
            throw new MetaApiException("Performance data sync failed for date: " + date, e);
        }
    }

    /**
     * Sync performance data for single account and date - for E2E testing
     */
    @Transactional
    public void syncPerformanceDataForAccountAndDate(String accountId, LocalDate date) throws MetaApiException {
        logger.info("📊 Syncing performance data for account {} on {}", accountId, date);

        try {
            // Ensure date dimension exists
            ensureDateDimensionExists(date);

            // Fetch insights
            List<MetaInsightsDto> insights = metaAdsConnector.fetchInsights(accountId, date, date);
            logger.info("✅ Fetched {} insights for account {}", insights.size(), accountId);

            if (!insights.isEmpty()) {
                // Transform and batch upsert
                List<AdsReporting> reporting = dataTransformer.transformInsightsList(insights);
                batchUpsertReporting(reporting);
                logger.info("✅ Upserted {} reporting records", reporting.size());
            }

        } catch (Exception e) {
            logger.error("❌ Performance data sync failed for account {} on {}: {}",
                    accountId, date, e.getMessage());
            throw new MetaApiException("Performance data sync failed", e);
        }
    }

    // ==================== CONVENIENCE METHODS FOR SCHEDULER/CONTROLLER ====================

    /**
     * Sync today's performance data (default)
     */
    @Transactional
    public void syncTodayPerformanceData() throws MetaApiException {
        syncPerformanceDataForDate(LocalDate.now());
    }

    /**
     * Sync yesterday's performance data (legacy support)
     */
    @Transactional
    public void syncYesterdayPerformanceData() throws MetaApiException {
        syncPerformanceDataForDate(LocalDate.now().minusDays(1));
    }

    /**
     * Full sync (hierarchy + today's performance)
     */
    @Transactional
    public void performFullSync() throws MetaApiException {
        logger.info("🚀 Starting full sync (hierarchy + today performance)");
        syncAccountHierarchy();
        syncTodayPerformanceData();
        logger.info("✅ Full sync completed");
    }

    /**
     * Full sync for specific date
     */
    @Transactional
    public void performFullSyncForDate(LocalDate date) throws MetaApiException {
        logger.info("🚀 Starting full sync for date: {}", date);
        syncAccountHierarchy();
        syncPerformanceDataForDate(date);
        logger.info("✅ Full sync completed for date: {}", date);
    }

    /**
     * Full sync for date range
     */
    @Transactional
    public void performFullSyncForDateRange(LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("🚀 Starting full sync for range: {} to {}", startDate, endDate);
        syncAccountHierarchy();
        syncPerformanceDataForDateRange(startDate, endDate);
        logger.info("✅ Full sync completed for range: {} to {}", startDate, endDate);
    }

    /**
     * Sync performance data for date range
     */
    @Transactional
    public void syncPerformanceDataForDateRange(LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("📈 Starting performance sync for range: {} to {}", startDate, endDate);

        try {
            List<MetaAccountDto> accounts = metaAdsConnector.fetchBusinessAccounts();

            for (MetaAccountDto account : accounts) {
                try {
                    syncPerformanceDataForAccountAndDateRange(account.getId(), startDate, endDate);
                } catch (Exception e) {
                    logger.error("Failed to sync performance data for account {} in range {} to {}: {}",
                            account.getId(), startDate, endDate, e.getMessage());
                }
            }

            logger.info("✅ Performance data sync completed for range: {} to {}", startDate, endDate);

        } catch (Exception e) {
            logger.error("❌ Performance data sync failed for range {} to {}: {}",
                    startDate, endDate, e.getMessage());
            throw new MetaApiException("Performance sync failed for date range", e);
        }
    }

    /**
     * Sync performance data for single account and date range
     */
    @Transactional
    public void syncPerformanceDataForAccountAndDateRange(String accountId, LocalDate startDate, LocalDate endDate)
            throws MetaApiException {
        logger.info("📊 Syncing performance data for account {} from {} to {}",
                accountId, startDate, endDate);

        try {
            // Ensure date dimensions exist
            ensureDateDimensionsExist(startDate, endDate);

            // Fetch insights for range
            List<MetaInsightsDto> insights = metaAdsConnector.fetchInsights(accountId, startDate, endDate);
            logger.info("✅ Fetched {} insights for account {} (range)", insights.size(), accountId);

            if (!insights.isEmpty()) {
                List<AdsReporting> reporting = dataTransformer.transformInsightsList(insights);
                batchUpsertReporting(reporting);
                logger.info("✅ Upserted {} reporting records (range)", reporting.size());
            }

        } catch (Exception e) {
            logger.error("❌ Performance data sync failed for account {} from {} to {}: {}",
                    accountId, startDate, endDate, e.getMessage());
            throw new MetaApiException("Performance data sync failed for range", e);
        }
    }

    /**
     * Get sync status for controller
     */
    public SyncStatus getSyncStatus() {
        try {
            List<MetaAccountDto> accounts = metaAdsConnector.fetchBusinessAccounts();
            return new SyncStatus(true, "System operational", accounts.size(),
                    LocalDate.now().toString(), System.currentTimeMillis());
        } catch (Exception e) {
            return new SyncStatus(false, "System error: " + e.getMessage(), 0,
                    LocalDate.now().toString(), System.currentTimeMillis());
        }
    }

    // ==================== BATCH UPSERT METHODS ====================

    private void batchUpsertAccounts(List<Account> accounts) {
        if (accounts.isEmpty()) return;
        logger.info("💾 Batch upserting {} accounts", accounts.size());

        if (accounts.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_account", accounts, this::getAccountCsvRow, getAccountCsvHeader(),
                    new String[]{"id"}, new String[]{"account_name", "account_status"}
            );
            logger.info("✅ Bulk account upsert: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            batchUpsertIndividually(accounts, accountDao::existsById, accountDao::update, accountDao::insert);
        }
    }

    private void batchUpsertCampaigns(List<Campaign> campaigns) {
        if (campaigns.isEmpty()) return;
        logger.info("💾 Batch upserting {} campaigns", campaigns.size());

        if (campaigns.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_campaign", campaigns, this::getCampaignCsvRow, getCampaignCsvHeader(),
                    new String[]{"id"}, new String[]{"campaign_name", "cam_status"}
            );
            logger.info("✅ Bulk campaign upsert: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            batchUpsertIndividually(campaigns, campaignDao::existsById, campaignDao::update, campaignDao::insert);
        }
    }

    private void batchUpsertAdSets(List<AdSet> adSets) {
        if (adSets.isEmpty()) return;
        logger.info("💾 Batch upserting {} adsets", adSets.size());

        if (adSets.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_adset", adSets, this::getAdSetCsvRow, getAdSetCsvHeader(),
                    new String[]{"id"}, new String[]{"ad_set_name", "ad_set_status"}
            );
            logger.info("✅ Bulk adset upsert: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            batchUpsertIndividually(adSets, adSetDao::existsById, adSetDao::update, adSetDao::insert);
        }
    }

    private void batchUpsertAdvertisements(List<Advertisement> ads) {
        if (ads.isEmpty()) return;
        logger.info("💾 Batch upserting {} ads", ads.size());

        if (ads.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_advertisement", ads, this::getAdCsvRow, getAdCsvHeader(),
                    new String[]{"id"}, new String[]{"ad_name", "ad_status"}
            );
            logger.info("✅ Bulk ad upsert: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            batchUpsertIndividually(ads, advertisementDao::existsById,
                    advertisementDao::update, advertisementDao::insert);
        }
    }

    private void batchUpsertPlacements(List<Placement> placements) {
        if (placements.isEmpty()) return;
        logger.info("💾 Batch upserting {} placements", placements.size());

        if (placements.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_placement", placements, this::getPlacementCsvRow, getPlacementCsvHeader(),
                    new String[]{"id"}, new String[]{"placement_name", "platform"}
            );
            logger.info("✅ Bulk placement upsert: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            batchUpsertIndividually(placements, placementDao::existsById,
                    placementDao::update, placementDao::insert);
        }
    }

    private void batchUpsertReporting(List<AdsReporting> reportingData) {
        if (reportingData.isEmpty()) return;
        logger.info("💾 Batch upserting {} reporting records", reportingData.size());

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
            logger.info("✅ Individual reporting insert: {}/{} successful",
                    successCount, reportingData.size());
        }
    }

    // ==================== HELPER METHODS ====================

    private <T> void batchUpsertIndividually(List<T> entities,
                                             java.util.function.Predicate<String> existsChecker,
                                             java.util.function.Consumer<T> updater,
                                             java.util.function.Consumer<T> inserter) {
        int successCount = 0;
        for (T entity : entities) {
            try {
                String id = getEntityId(entity);
                if (existsChecker.test(id)) {
                    updater.accept(entity);
                } else {
                    inserter.accept(entity);
                }
                successCount++;
            } catch (Exception e) {
                logger.error("Failed to upsert entity: {}", e.getMessage());
            }
        }
        logger.info("✅ Individual upsert: {}/{} successful", successCount, entities.size());
    }

    private String getEntityId(Object entity) {
        if (entity instanceof Account) return ((Account) entity).getId();
        if (entity instanceof Campaign) return ((Campaign) entity).getId();
        if (entity instanceof AdSet) return ((AdSet) entity).getId();
        if (entity instanceof Advertisement) return ((Advertisement) entity).getId();
        if (entity instanceof Placement) return ((Placement) entity).getId();
        throw new IllegalArgumentException("Unknown entity type: " + entity.getClass());
    }

    private List<Placement> generatePlacementsForAds(List<MetaAdDto> ads) {
        List<Placement> placements = new ArrayList<>();
        for (MetaAdDto ad : ads) {
            placements.addAll(generatePlacementsForAd(ad.getId()));
        }
        return placements;
    }

    private List<Placement> generatePlacementsForAd(String adId) {
        List<Placement> placements = new ArrayList<>();
        String[] types = {"feed", "story", "reel", "banner"};
        String[] platforms = {"facebook", "instagram", "messenger", "audience_network"};

        for (String type : types) {
            for (String platform : platforms) {
                Placement placement = new Placement();
                placement.setId(adId + "_" + platform + "_" + type);
                placement.setAdvertisementId(adId);
                placement.setPlacementName(platform.substring(0, 1).toUpperCase() +
                        platform.substring(1) + " " + type.substring(0, 1).toUpperCase() + type.substring(1));
                placement.setPlatform(platform);
                placement.setPlacementType(type);
                placement.setDeviceType("mobile");
                placement.setPosition("feed");
                placement.setIsActive(true);
                placement.setSupportsVideo(!"banner".equals(type));
                placement.setSupportsCarousel("feed".equals(type));
                placement.setSupportsCollection("feed".equals(type));
                placement.setCreatedAt(java.time.LocalDateTime.now().toString());
                placements.add(placement);
            }
        }
        return placements;
    }

    private void ensureDateDimensionExists(LocalDate date) {
        try {
            String dateString = date.toString();
            if (!adsProcessingDateDao.existsById(dateString)) {
                AdsProcessingDate dateRecord = createDateRecord(date);
                adsProcessingDateDao.insert(dateRecord);
                logger.debug("Created date dimension for: {}", date);
            }
        } catch (Exception e) {
            logger.error("Failed to ensure date dimension for {}: {}", date, e.getMessage());
        }
    }

    private void ensureDateDimensionsExist(LocalDate startDate, LocalDate endDate) {
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            ensureDateDimensionExists(current);
            current = current.plusDays(1);
        }
    }

    private AdsProcessingDate createDateRecord(LocalDate date) {
        AdsProcessingDate dateRecord = new AdsProcessingDate();
        dateRecord.setFullDate(date.toString());
        dateRecord.setDayOfWeek(date.getDayOfWeek().getValue());
        dateRecord.setDayOfWeekName(date.getDayOfWeek().toString());
        dateRecord.setDayOfMonth(date.getDayOfMonth());
        dateRecord.setDayOfYear(date.getDayOfYear());
        dateRecord.setWeekOfYear(date.getDayOfYear() / 7 + 1);
        dateRecord.setMonthOfYear(date.getMonthValue());
        dateRecord.setMonthName(date.getMonth().toString());
        dateRecord.setQuarter((date.getMonthValue() - 1) / 3 + 1);
        dateRecord.setYear(date.getYear());
        dateRecord.setIsWeekend(date.getDayOfWeek().getValue() >= 6);
        dateRecord.setIsHoliday(false);
        dateRecord.setHolidayName(null);
        dateRecord.setFiscalYear(date.getYear());
        dateRecord.setFiscalQuarter((date.getMonthValue() - 1) / 3 + 1);
        return dateRecord;
    }

    // ==================== CSV HELPER METHODS ====================

    private String getAccountCsvRow(Account account) {
        return String.format("%s,%s,%s,%s,%s,%s,%s",
                csvEscape(account.getId()), csvEscape(account.getPlatformId()),
                csvEscape(account.getAccountName()), csvEscape(account.getCurrency()),
                csvEscape(account.getAccountStatus()),
                account.getAmountSpent() != null ? account.getAmountSpent().toString() : "",
                csvEscape(account.getCreatedTime()));
    }

    private String getAccountCsvHeader() {
        return "id,platform_id,account_name,currency,account_status,amount_spent,created_time";
    }

    private String getCampaignCsvRow(Campaign campaign) {
        return String.format("%s,%s,%s,%s,%s,%s,%s",
                csvEscape(campaign.getId()), csvEscape(campaign.getPlatformId()),
                csvEscape(campaign.getCampaignName()), csvEscape(campaign.getCamStatus()),
                csvEscape(campaign.getCamObjective()),
                campaign.getDailyBudget() != null ? campaign.getDailyBudget().toString() : "",
                csvEscape(campaign.getCreatedTime()));
    }

    private String getCampaignCsvHeader() {
        return "id,platform_id,campaign_name,cam_status,cam_objective,daily_budget,created_time";
    }

    private String getAdSetCsvRow(AdSet adSet) {
        return String.format("%s,%s,%s,%s,%s,%s",
                csvEscape(adSet.getId()), csvEscape(adSet.getCampaignId()),
                csvEscape(adSet.getAdSetName()), csvEscape(adSet.getAdSetStatus()),
                adSet.getDailyBudget() != null ? adSet.getDailyBudget().toString() : "",
                csvEscape(adSet.getCreatedTime()));
    }

    private String getAdSetCsvHeader() {
        return "id,campaign_id,ad_set_name,ad_set_status,daily_budget,created_time";
    }

    private String getAdCsvRow(Advertisement ad) {
        return String.format("%s,%s,%s,%s,%s,%s",
                csvEscape(ad.getId()), csvEscape(ad.getAdsetid()),
                csvEscape(ad.getAdName()), csvEscape(ad.getAdStatus()),
                csvEscape(ad.getCreativeId()), csvEscape(ad.getCreatedTime()));
    }

    private String getAdCsvHeader() {
        return "id,adsetid,ad_name,ad_status,creative_id,created_time";
    }

    private String getPlacementCsvRow(Placement placement) {
        return String.format("%s,%s,%s,%s,%s,%s",
                csvEscape(placement.getId()), csvEscape(placement.getAdvertisementId()),
                csvEscape(placement.getPlacementName()), csvEscape(placement.getPlatform()),
                csvEscape(placement.getPlacementType()), placement.getIsActive().toString());
    }

    private String getPlacementCsvHeader() {
        return "id,advertisement_id,placement_name,platform,placement_type,is_active";
    }

    private String csvEscape(String value) {
        if (value == null) return "";
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    // ==================== STATUS CLASS ====================

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
}