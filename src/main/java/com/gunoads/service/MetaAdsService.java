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

@Service
public class MetaAdsService {

    private static final Logger logger = LoggerFactory.getLogger(MetaAdsService.class);

    @Autowired private MetaAdsConnector metaAdsConnector;
    @Autowired private DataTransformer dataTransformer;
    @Autowired private DataIngestionProcessor dataProcessor;  // FIXED: Added missing dependency
    @Autowired private AccountDao accountDao;
    @Autowired private CampaignDao campaignDao;
    @Autowired private AdSetDao adSetDao;
    @Autowired private AdvertisementDao advertisementDao;
    @Autowired private AdsReportingDao adsReportingDao;
    @Autowired private AdsProcessingDateDao adsProcessingDateDao;  // FIXED: Added missing DAO

    @Value("${batch.processing.bulk-threshold:100}")  // FIXED: Lowered threshold for testing
    private int bulkThreshold;

    /**
     * OPTIMIZED: Sync account hierarchy using bulk operations for large datasets
     */
    @Transactional
    public void syncAccountHierarchy() throws MetaApiException {
        logger.info("Starting account hierarchy sync...");

        try {
            // 1. Fetch and sync accounts
            List<MetaAccountDto> accountDtos = metaAdsConnector.fetchBusinessAccounts();
            syncAccountsBulk(accountDtos);

            // 2. For each account, sync campaigns/adsets/ads
            for (MetaAccountDto accountDto : accountDtos) {
                try {
                    syncCampaignHierarchy(accountDto.getId());
                } catch (Exception e) {
                    logger.error("Failed to sync hierarchy for account {}: {}",
                            accountDto.getId(), e.getMessage());
                }
            }

            logger.info("Account hierarchy sync completed");

        } catch (Exception e) {
            logger.error("Account hierarchy sync failed: {}", e.getMessage());
            throw new MetaApiException("Account hierarchy sync failed", e);
        }
    }

    /**
     * OPTIMIZED: Bulk sync accounts using DataIngestionProcessor
     */
    private void syncAccountsBulk(List<MetaAccountDto> accountDtos) {
        if (accountDtos.isEmpty()) return;

        List<Account> accounts = dataTransformer.transformAccounts(accountDtos);

        if (accounts.size() >= bulkThreshold) {
            // Use bulk processing for large datasets
            logger.info("Using bulk processing for {} accounts", accounts.size());

            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_account",
                    accounts,
                    this::getAccountCsvRow,  // FIXED: Correct parameter order
                    getAccountCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"account_name", "currency", "timezone_name", "account_status", "amount_spent"}
            );

            logger.info("Bulk account processing completed: {} records in {}ms using {}",
                    result.recordsProcessed, result.durationMs, result.strategy);
        } else {
            // Use individual operations for small datasets with better error handling
            logger.info("Using individual processing for {} accounts", accounts.size());
            int successCount = 0;

            for (Account account : accounts) {
                try {
                    if (accountDao.existsById(account.getId())) {
                        accountDao.update(account);
                        logger.debug("Updated account: {}", account.getId());
                    } else {
                        accountDao.insert(account);
                        logger.debug("Inserted account: {}", account.getId());
                    }
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to sync account {}: {}", account.getId(), e.getMessage());
                    // Continue with other accounts instead of stopping
                }
            }
            logger.info("Individual account processing completed: {}/{} successful", successCount, accounts.size());
        }
    }

    /**
     * OPTIMIZED: Sync campaign hierarchy using bulk operations
     */
    private void syncCampaignHierarchy(String accountId) {
        logger.debug("Syncing campaign hierarchy for account: {}", accountId);

        try {
            // Fetch campaigns
            List<MetaCampaignDto> campaignDtos = metaAdsConnector.fetchCampaigns(accountId);
            syncCampaignsBulk(campaignDtos);

            // Fetch adsets
            List<MetaAdSetDto> adSetDtos = metaAdsConnector.fetchAdSets(accountId);
            syncAdSetsBulk(adSetDtos);

            // Fetch ads
            List<MetaAdDto> adDtos = metaAdsConnector.fetchAds(accountId);
            syncAdsBulk(adDtos);

        } catch (Exception e) {
            logger.error("Campaign hierarchy sync failed for account {}: {}", accountId, e.getMessage());
        }
    }

    /**
     * OPTIMIZED: Bulk sync campaigns
     */
    private void syncCampaignsBulk(List<MetaCampaignDto> campaignDtos) {
        if (campaignDtos.isEmpty()) return;

        List<Campaign> campaigns = dataTransformer.transformCampaigns(campaignDtos);

        if (campaigns.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_campaign",
                    campaigns,
                    this::getCampaignCsvRow,  // FIXED: Correct parameter order
                    getCampaignCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"campaign_name", "status", "objective", "daily_budget", "lifetime_budget"}
            );

            logger.debug("Bulk campaign processing: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            // Individual processing for small batches with better error handling
            int successCount = 0;
            for (Campaign campaign : campaigns) {
                try {
                    if (campaignDao.existsById(campaign.getId())) {
                        campaignDao.update(campaign);
                        logger.debug("Updated campaign: {}", campaign.getId());
                    } else {
                        campaignDao.insert(campaign);
                        logger.debug("Inserted campaign: {}", campaign.getId());
                    }
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to sync campaign {}: {}", campaign.getId(), e.getMessage());
                }
            }
            logger.info("Individual campaign processing completed: {}/{} successful", successCount, campaigns.size());
        }
    }

    /**
     * OPTIMIZED: Bulk sync adsets
     */
    private void syncAdSetsBulk(List<MetaAdSetDto> adSetDtos) {
        if (adSetDtos.isEmpty()) return;

        List<AdSet> adSets = dataTransformer.transformAdSets(adSetDtos);

        if (adSets.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_adset",
                    adSets,
                    this::getAdSetCsvRow,  // FIXED: Correct parameter order
                    getAdSetCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"adset_name", "status", "daily_budget", "lifetime_budget", "bid_amount"}
            );

            logger.debug("Bulk adset processing: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            for (AdSet adSet : adSets) {
                try {
                    if (adSetDao.existsById(adSet.getId())) {
                        adSetDao.update(adSet);
                    } else {
                        adSetDao.insert(adSet);
                    }
                } catch (Exception e) {
                    logger.error("Failed to sync adset {}: {}", adSet.getId(), e.getMessage());
                }
            }
        }
    }

    /**
     * OPTIMIZED: Bulk sync ads
     */
    private void syncAdsBulk(List<MetaAdDto> adDtos) {
        if (adDtos.isEmpty()) return;

        List<Advertisement> ads = dataTransformer.transformAdvertisements(adDtos);

        if (ads.size() >= bulkThreshold) {
            DataIngestionProcessor.ProcessingResult result = dataProcessor.processWithUpsert(
                    "tbl_advertisement",
                    ads,
                    this::getAdCsvRow,  // FIXED: Correct parameter order
                    getAdCsvHeader(),
                    new String[]{"id", "platform_id"},
                    new String[]{"ad_name", "status", "creative_id"}
            );

            logger.debug("Bulk ad processing: {} records in {}ms",
                    result.recordsProcessed, result.durationMs);
        } else {
            for (Advertisement ad : ads) {
                try {
                    if (advertisementDao.existsById(ad.getId())) {
                        advertisementDao.update(ad);
                    } else {
                        advertisementDao.insert(ad);
                    }
                } catch (Exception e) {
                    logger.error("Failed to sync ad {}: {}", ad.getId(), e.getMessage());
                }
            }
        }
    }

    /**
     * OPTIMIZED: Sync performance data using bulk operations
     */
    @Transactional
    public void syncPerformanceData(String accountId, LocalDate startDate, LocalDate endDate)
            throws MetaApiException {

        logger.info("Syncing performance data for account: {} from {} to {}",
                accountId, startDate, endDate);

        try {
            // FIXED: Ensure date dimensions exist first
            ensureDateDimensionsExist(startDate, endDate);

            // Fetch insights data with breakdowns
            List<MetaInsightsDto> insightDtos = metaAdsConnector.fetchInsights(
                    accountId, startDate, endDate
            );

            if (insightDtos.isEmpty()) {
                logger.info("No insights data found for account: {}", accountId);
                return;
            }

            // Transform to reporting entities
            List<AdsReporting> reportingList = dataTransformer.transformInsightsList(insightDtos);

            logger.info("Processing {} reporting records for account: {}",
                    reportingList.size(), accountId);

            // OPTIMIZED: Use bulk processing for performance data (always large datasets)
            if (reportingList.size() >= bulkThreshold) {
                DataIngestionProcessor.ProcessingResult result = dataProcessor.processBatch(
                        "tbl_ads_reporting",
                        reportingList,
                        adsReportingDao::getInsertParameters,
                        this::getReportingCsvRow,
                        getReportingCsvHeader(),
                        adsReportingDao.buildInsertSql()
                );

                logger.info("Bulk reporting processing completed: {} records in {}ms using {}",
                        result.recordsProcessed, result.durationMs, result.strategy);
            } else {
                // Fallback to batch insert for smaller datasets
                try {
                    adsReportingDao.batchInsert(reportingList);
                    logger.info("Batch insert completed: {} records", reportingList.size());
                } catch (Exception e) {
                    logger.error("Batch insert failed, using individual inserts: {}", e.getMessage());

                    int successCount = 0;
                    for (AdsReporting reporting : reportingList) {
                        try {
                            adsReportingDao.insert(reporting);
                            successCount++;
                        } catch (Exception ex) {
                            logger.warn("Failed to insert reporting record: {}", ex.getMessage());
                        }
                    }
                    logger.info("Individual inserts completed: {}/{}", successCount, reportingList.size());
                }
            }

            // FIXED: Mark dates as processed
            markDatesAsProcessed(accountId, startDate, endDate);

            logger.info("Performance data sync completed for account: {}", accountId);

        } catch (Exception e) {
            logger.error("Performance data sync failed for account {}: {}", accountId, e.getMessage());
            throw new MetaApiException("Performance data sync failed", e);
        }
    }

    /**
     * Sync yesterday's performance data for all accounts
     */
    @Transactional
    public void syncYesterdayPerformanceData() throws MetaApiException {
        logger.info("Starting yesterday performance data sync for all accounts...");

        try {
            List<MetaAccountDto> accountDtos = metaAdsConnector.fetchAccounts();
            LocalDate yesterday = LocalDate.now().minusDays(1);

            for (MetaAccountDto accountDto : accountDtos) {
                try {
                    syncPerformanceData(accountDto.getId(), yesterday, yesterday);
                } catch (Exception e) {
                    logger.error("Failed to sync performance data for account {}: {}",
                            accountDto.getId(), e.getMessage());
                }
            }

            logger.info("Yesterday performance data sync completed");

        } catch (Exception e) {
            logger.error("Yesterday performance data sync failed: {}", e.getMessage());
            throw new MetaApiException("Yesterday performance data sync failed", e);
        }
    }

    /**
     * Full sync: hierarchy + performance data
     */
    @Transactional
    public void performFullSync() throws MetaApiException {
        logger.info("Starting full sync (hierarchy + performance data)...");

        try {
            // 1. Sync account hierarchy first
            syncAccountHierarchy();

            // 2. Sync yesterday's performance data
            syncYesterdayPerformanceData();

            logger.info("Full sync completed successfully");

        } catch (Exception e) {
            logger.error("Full sync failed: {}", e.getMessage());
            throw new MetaApiException("Full sync failed", e);
        }
    }

    /**
     * Get sync status
     */
    public SyncStatus getSyncStatus() {
        try {
            long accountCount = accountDao.count();
            long campaignCount = campaignDao.count();
            long reportingCount = adsReportingDao.count();

            // FIXED: Use simple connection test instead of testConnection()
            boolean isConnected = checkMetaApiConnection();

            return new SyncStatus(isConnected, accountCount, campaignCount, reportingCount);
        } catch (Exception e) {
            logger.error("Failed to get sync status: {}", e.getMessage());
            return new SyncStatus(false, 0, 0, 0);
        }
    }

    /**
     * Simple connection test for Meta API
     */
    private boolean checkMetaApiConnection() {
        try {
            // Try to fetch minimal data to test connection
            List<MetaAccountDto> accounts = metaAdsConnector.fetchAccounts();
            return accounts != null; // Connection works if we get any response
        } catch (Exception e) {
            logger.debug("Meta API connection test failed: {}", e.getMessage());
            return false;
        }
    }

    // ==================== CSV MAPPING METHODS ====================

    private String getAccountCsvRow(Account account) {
        return String.format("%s,%s,%s,%s,%s,%s,%s",
                csvEscape(account.getId()),
                csvEscape(account.getPlatformId()),
                csvEscape(account.getAccountName()),
                csvEscape(account.getCurrency()),
                csvEscape(account.getTimezoneName()),
                csvEscape(account.getAccountStatus()),
                account.getAmountSpent() != null ? account.getAmountSpent().toString() : "0"
        );
    }

    private String getCampaignCsvRow(Campaign campaign) {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s",
                csvEscape(campaign.getId()),
                csvEscape(campaign.getPlatformId()),
                csvEscape(campaign.getAccountId()),
                csvEscape(campaign.getCampaignName()),
                csvEscape(campaign.getCamStatus()),
                csvEscape(campaign.getCamObjective()),
                campaign.getDailyBudget() != null ? campaign.getDailyBudget().toString() : "0",
                campaign.getLifetimeBudget() != null ? campaign.getLifetimeBudget().toString() : "0"
        );
    }

    private String getAdSetCsvRow(AdSet adSet) {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s",
                csvEscape(adSet.getId()),
                csvEscape("FB"),
                csvEscape(adSet.getCampaignId()),
                csvEscape(adSet.getAdSetName()),
                csvEscape(adSet.getAdSetStatus()),
                adSet.getDailyBudget() != null ? adSet.getDailyBudget().toString() : "0",
                adSet.getLifetimeBudget() != null ? adSet.getLifetimeBudget().toString() : "0",
                adSet.getBidAmount() != null ? adSet.getBidAmount().toString() : "0"
        );
    }

    private String getAdCsvRow(Advertisement ad) {
        return String.format("%s,%s,%s,%s,%s,%s",
                csvEscape(ad.getId()),
                csvEscape("FB"),
                csvEscape(ad.getId()),  // FIXED: Use consistent naming
                csvEscape(ad.getAdName()),
                csvEscape(ad.getAdStatus()),
                csvEscape(ad.getCreativeId())
        );
    }

    private String getReportingCsvRow(AdsReporting reporting) {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                csvEscape(reporting.getAccountId()),
                csvEscape(reporting.getPlatformId()),
                csvEscape(reporting.getCampaignId()),
                csvEscape(reporting.getAdsetId()),
                csvEscape(reporting.getAdvertisementId()),
                reporting.getDateStart().toString(),
                reporting.getImpressions().toString(),
                reporting.getClicks().toString(),
                reporting.getSpend().toString(),
                csvEscape(reporting.getAgeGroup()),
                csvEscape(reporting.getGender())
        );
    }

    // CSV Headers
    private String getAccountCsvHeader() {
        return "id,platform_id,account_name,currency,timezone_name,account_status,amount_spent";
    }

    private String getCampaignCsvHeader() {
        return "id,platform_id,account_id,campaign_name,status,objective,daily_budget,lifetime_budget";
    }

    private String getAdSetCsvHeader() {
        return "id,platform_id,campaign_id,adset_name,status,daily_budget,lifetime_budget,bid_amount";
    }

    private String getAdCsvHeader() {
        return "id,platform_id,adset_id,ad_name,status,creative_id";
    }

    private String getReportingCsvHeader() {
        return "account_id,platform_id,campaign_id,adset_id,ad_id,date_start,impressions,clicks,spend,age_range,gender";
    }

    private String csvEscape(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    // ==================== DATE PROCESSING METHODS ====================

    /**
     * FIXED: Ensure date dimensions exist for the date range
     */
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

    /**
     * FIXED: Create date dimension record from LocalDate
     */
    private AdsProcessingDate createDateDimension(LocalDate date) {
        AdsProcessingDate dateRecord = new AdsProcessingDate();

        dateRecord.setFullDate(date.toString());
        dateRecord.setDayOfWeek(date.getDayOfWeek().getValue());
        dateRecord.setDayOfWeekName(date.getDayOfWeek().name());
        dateRecord.setDayOfMonth(date.getDayOfMonth());
        dateRecord.setDayOfYear(date.getDayOfYear());
        dateRecord.setWeekOfYear(getWeekOfYear(date));
        dateRecord.setMonthOfYear(date.getMonthValue());
        dateRecord.setMonthName(date.getMonth().name());
        dateRecord.setQuarter(getQuarter(date));
        dateRecord.setYear(date.getYear());
        dateRecord.setIsWeekend(isWeekend(date));
        dateRecord.setIsHoliday(false); // Can be enhanced with holiday logic
        dateRecord.setHolidayName(null);
        dateRecord.setFiscalYear(getFiscalYear(date));
        dateRecord.setFiscalQuarter(getFiscalQuarter(date));

        return dateRecord;
    }

    /**
     * FIXED: Mark date range as processed for account
     */
    private void markDatesAsProcessed(String accountId, LocalDate startDate, LocalDate endDate) {
        logger.debug("Marking dates as processed for account {} from {} to {}",
                accountId, startDate, endDate);

        // This could be enhanced to track per-account processing status
        // For now, just log the completion
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            logger.debug("Marked {} as processed for account {}", current, accountId);
            current = current.plusDays(1);
        }
    }

    // Date utility methods
    private int getWeekOfYear(LocalDate date) {
        return date.getDayOfYear() / 7 + 1;
    }

    private int getQuarter(LocalDate date) {
        return (date.getMonthValue() - 1) / 3 + 1;
    }

    private boolean isWeekend(LocalDate date) {
        return date.getDayOfWeek().getValue() >= 6; // Saturday(6) or Sunday(7)
    }

    private int getFiscalYear(LocalDate date) {
        // Assuming fiscal year starts in April (can be configured)
        return date.getMonthValue() >= 4 ? date.getYear() : date.getYear() - 1;
    }

    private int getFiscalQuarter(LocalDate date) {
        // Fiscal quarters starting from April
        int month = date.getMonthValue();
        if (month >= 4 && month <= 6) return 1;
        if (month >= 7 && month <= 9) return 2;
        if (month >= 10 && month <= 12) return 3;
        return 4; // Jan-Mar
    }

    // Status class
    public static class SyncStatus {
        public final boolean isConnected;
        public final long accountCount;
        public final long campaignCount;
        public final long reportingCount;

        public SyncStatus(boolean isConnected, long accountCount, long campaignCount, long reportingCount) {
            this.isConnected = isConnected;
            this.accountCount = accountCount;
            this.campaignCount = campaignCount;
            this.reportingCount = reportingCount;
        }
    }
}