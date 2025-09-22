/**
 * MetaAdsService - Clean and Essential Version
 * Chỉ giữ lại essential functions, loại bỏ code thừa thãi
 */
package com.gunoads.service;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.processor.DataTransformer;
import com.gunoads.dao.*;
import com.gunoads.model.dto.*;
import com.gunoads.model.entity.*;
import com.gunoads.exception.MetaApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class MetaAdsService {
    private static final Logger logger = LoggerFactory.getLogger(MetaAdsService.class);

    @Autowired private MetaAdsConnector metaAdsConnector;
    @Autowired private DataTransformer dataTransformer;
    @Autowired private AccountDao accountDao;
    @Autowired private CampaignDao campaignDao;
    @Autowired private AdSetDao adSetDao;
    @Autowired private AdvertisementDao advertisementDao;
    @Autowired private PlacementDao placementDao;
    @Autowired private AdsReportingDao adsReportingDao;
    @Autowired private AdsProcessingDateDao adsProcessingDateDao;
    @Autowired private SyncStateDao syncStateDao;

    /**
     * Initialize sync tracking
     */
    @PostConstruct
    public void initializeSyncTracking() {
        syncStateDao.initializeSyncStateTable();
        logger.info("🔄 Sync tracking initialized");
    }

    // ==================== CORE SYNC METHODS ====================

    /**
     * MAIN: Sync account hierarchy với incremental logic
     */
    @Transactional
    public void syncAccountHierarchy() throws MetaApiException {
        logger.info("🏗️ Starting SMART account hierarchy sync");

        try {
            LocalDateTime syncStartTime = LocalDateTime.now();

            // Step 1: Accounts - always full sync
            logger.info("📋 Syncing accounts...");
            List<MetaAccountDto> accountDtos = metaAdsConnector.fetchBusinessAccounts();

            for (MetaAccountDto accountDto : accountDtos) {
                Account account = dataTransformer.transformAccount(accountDto);
                upsertAccount(account);
            }

            logger.info("✅ Accounts: {} processed", accountDtos.size());

            // Step 2: Business objects - incremental cho mỗi account
            for (MetaAccountDto accountDto : accountDtos) {
                String accountId = accountDto.getId();
                logger.info("🎯 Processing account: {}", accountId);

                try {
                    syncCampaignsIncremental(accountId, syncStartTime);
                    syncAdSetsIncremental(accountId, syncStartTime);
                    syncAdsIncremental(accountId, syncStartTime);

                } catch (Exception e) {
                    logger.error("❌ Failed to sync account {}: {}", accountId, e.getMessage());
                    syncStateDao.markSyncFailed("account_hierarchy", accountId, e.getMessage());
                }
            }

            logger.info("✅ SMART hierarchy sync completed");

        } catch (Exception e) {
            logger.error("❌ Account hierarchy sync failed: {}", e.getMessage());
            throw new MetaApiException("Account hierarchy sync failed", e);
        }
    }

    /**
     * MAIN: Sync performance data (full sync cho insights)
     */
    @Transactional
    public void syncPerformanceDataForDate(LocalDate date) throws MetaApiException {
        logger.info("📊 Starting performance data sync for date: {}", date);

        try {
            ensureDateDimensionExists(date);

            List<MetaAccountDto> accounts = metaAdsConnector.fetchBusinessAccounts();
            logger.info("📋 Processing {} accounts for insights", accounts.size());

            for (MetaAccountDto accountDto : accounts) {
                try {
                    List<MetaInsightsDto> insights = metaAdsConnector.fetchInsights(
                            accountDto.getId(), date, date);

                    if (!insights.isEmpty()) {
                        List<AdsReporting> reporting = dataTransformer.transformInsightsList(insights);
                        batchUpsertReporting(reporting);
                        logger.info("✅ Account {}: {} insights processed",
                                accountDto.getId(), insights.size());
                    }

                } catch (Exception e) {
                    logger.error("❌ Failed insights for account {}: {}",
                            accountDto.getId(), e.getMessage());
                }
            }

            logger.info("✅ Performance data sync completed for date: {}", date);

        } catch (Exception e) {
            logger.error("❌ Performance data sync failed for date {}: {}", date, e.getMessage());
            throw new MetaApiException("Performance data sync failed", e);
        }
    }

    // ==================== INCREMENTAL SYNC METHODS ====================

    /**
     * Incremental campaigns sync
     */
    private void syncCampaignsIncremental(String accountId, LocalDateTime syncTime) throws MetaApiException {
        try {
            boolean useIncremental = syncStateDao.canUseIncremental(SyncStateDao.ObjectType.CAMPAIGNS, accountId);

            List<MetaCampaignDto> campaigns;
            if (useIncremental) {
                logger.info("📈 Incremental campaigns for account: {}", accountId);
                campaigns = metaAdsConnector.fetchCampaignsIncremental(accountId);
            } else {
                logger.info("🔄 Full campaigns for account: {}", accountId);
                campaigns = metaAdsConnector.fetchCampaigns(accountId);
            }

            for (MetaCampaignDto campaignDto : campaigns) {
                Campaign campaign = dataTransformer.transformCampaign(campaignDto);
                upsertCampaign(campaign);
            }

            syncStateDao.updateSyncTime(SyncStateDao.ObjectType.CAMPAIGNS, accountId, syncTime);
            logger.info("✅ Campaigns: {} processed for account {}", campaigns.size(), accountId);

        } catch (Exception e) {
            syncStateDao.markSyncFailed(SyncStateDao.ObjectType.CAMPAIGNS, accountId, e.getMessage());
            throw e;
        }
    }

    /**
     * Incremental adsets sync
     */
    private void syncAdSetsIncremental(String accountId, LocalDateTime syncTime) throws MetaApiException {
        try {
            boolean useIncremental = syncStateDao.canUseIncremental(SyncStateDao.ObjectType.ADSETS, accountId);

            List<MetaAdSetDto> adSets;
            if (useIncremental) {
                logger.info("📈 Incremental adsets for account: {}", accountId);
                adSets = metaAdsConnector.fetchAdSetsIncremental(accountId);
            } else {
                logger.info("🔄 Full adsets for account: {}", accountId);
                adSets = metaAdsConnector.fetchAdSets(accountId);
            }

            for (MetaAdSetDto adSetDto : adSets) {
                AdSet adSet = dataTransformer.transformAdSet(adSetDto);
                upsertAdSet(adSet);
            }

            syncStateDao.updateSyncTime(SyncStateDao.ObjectType.ADSETS, accountId, syncTime);
            logger.info("✅ AdSets: {} processed for account {}", adSets.size(), accountId);

        } catch (Exception e) {
            syncStateDao.markSyncFailed(SyncStateDao.ObjectType.ADSETS, accountId, e.getMessage());
            throw e;
        }
    }

    /**
     * Incremental ads sync
     */
    private void syncAdsIncremental(String accountId, LocalDateTime syncTime) throws MetaApiException {
        try {
            boolean useIncremental = syncStateDao.canUseIncremental(SyncStateDao.ObjectType.ADS, accountId);

            List<MetaAdDto> ads;
            if (useIncremental) {
                logger.info("📈 Incremental ads for account: {}", accountId);
                ads = metaAdsConnector.fetchAdsIncremental(accountId);
            } else {
                logger.info("🔄 Full ads for account: {}", accountId);
                ads = metaAdsConnector.fetchAds(accountId);
            }

            for (MetaAdDto adDto : ads) {
                Advertisement ad = dataTransformer.transformAdvertisement(adDto);
                upsertAd(ad);

                // Extract placements
                List<Placement> placements = extractPlacements(ad);
                for (Placement placement : placements) {
                    upsertPlacement(placement);
                }
            }

            syncStateDao.updateSyncTime(SyncStateDao.ObjectType.ADS, accountId, syncTime);
            logger.info("✅ Ads: {} processed for account {}", ads.size(), accountId);

        } catch (Exception e) {
            syncStateDao.markSyncFailed(SyncStateDao.ObjectType.ADS, accountId, e.getMessage());
            throw e;
        }
    }

    // ==================== UPSERT METHODS ====================

    private void upsertAccount(Account account) {
        try {
            if (accountDao.existsById(account.getId())) {
                accountDao.update(account);
            } else {
                accountDao.insert(account);
            }
        } catch (Exception e) {
            logger.error("❌ Failed to upsert account {}: {}", account.getId(), e.getMessage());
        }
    }

    private void upsertCampaign(Campaign campaign) {
        try {
            if (campaignDao.existsById(campaign.getId())) {
                campaignDao.update(campaign);
            } else {
                campaignDao.insert(campaign);
            }
        } catch (Exception e) {
            logger.error("❌ Failed to upsert campaign {}: {}", campaign.getId(), e.getMessage());
        }
    }

    private void upsertAdSet(AdSet adSet) {
        try {
            if (adSetDao.existsById(adSet.getId())) {
                adSetDao.update(adSet);
            } else {
                adSetDao.insert(adSet);
            }
        } catch (Exception e) {
            logger.error("❌ Failed to upsert adset {}: {}", adSet.getId(), e.getMessage());
        }
    }

    private void upsertAd(Advertisement ad) {
        try {
            if (advertisementDao.existsById(ad.getId())) {
                advertisementDao.update(ad);
            } else {
                advertisementDao.insert(ad);
            }
        } catch (Exception e) {
            logger.error("❌ Failed to upsert ad {}: {}", ad.getId(), e.getMessage());
        }
    }

    private void upsertPlacement(Placement placement) {
        try {
            if (placementDao.existsById(placement.getId())) {
                placementDao.update(placement);
            } else {
                placementDao.insert(placement);
            }
        } catch (Exception e) {
            logger.error("❌ Failed to upsert placement {}: {}", placement.getId(), e.getMessage());
        }
    }

    // ==================== BATCH OPERATIONS ====================

    private void batchUpsertReporting(List<AdsReporting> reportingList) {
        try {
            if (reportingList.size() > 100) {
                // Use bulk operations for large datasets
                adsReportingDao.batchInsert(reportingList);
            } else {
                // Individual operations for small datasets
                for (AdsReporting reporting : reportingList) {
                    adsReportingDao.insert(reporting);
                }
            }
            logger.info("✅ Batch upsert: {} reporting records", reportingList.size());
        } catch (Exception e) {
            logger.error("❌ Batch upsert failed: {}", e.getMessage());
        }
    }

    // ==================== HELPER METHODS ====================

    private List<Placement> extractPlacements(Advertisement ad) {
        // Simple placement extraction logic
        return List.of(createPlacement(ad.getId(), "facebook_feed"));
    }

    private Placement createPlacement(String adId, String type) {
        Placement placement = new Placement();
        placement.setId(adId + "_" + type);
        placement.setAdvertisementId(adId);
        placement.setPlacementName(type);
        placement.setPlatform("facebook");
        placement.setPlacementType("feed");
        placement.setIsActive(true);
        placement.setSupportsVideo(true);
        placement.setSupportsCarousel(true);
        placement.setSupportsCollection(true);
        return placement;
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
        dateRecord.setFiscalYear(date.getYear());
        dateRecord.setFiscalQuarter((date.getMonthValue() - 1) / 3 + 1);
        return dateRecord;
    }

    // ==================== PUBLIC API METHODS ====================

    /**
     * Sync today's performance data
     */
    @Transactional
    public void syncTodayPerformanceData() throws MetaApiException {
        syncPerformanceDataForDate(LocalDate.now());
    }

    /**
     * Sync yesterday's performance data
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
        logger.info("🚀 Starting full sync");
        syncAccountHierarchy();
        syncTodayPerformanceData();
        logger.info("✅ Full sync completed");
    }

    /**
     * Force full sync cho specific account
     */
    @Transactional
    public void forceFullSync(String accountId) throws MetaApiException {
        logger.info("🔄 FORCING full sync for account: {}", accountId);

        // Clear sync state to force full sync
        syncStateDao.markSyncFailed(SyncStateDao.ObjectType.CAMPAIGNS, accountId, "Manual full sync");
        syncStateDao.markSyncFailed(SyncStateDao.ObjectType.ADSETS, accountId, "Manual full sync");
        syncStateDao.markSyncFailed(SyncStateDao.ObjectType.ADS, accountId, "Manual full sync");

        // Run sync
        LocalDateTime syncTime = LocalDateTime.now();
        syncCampaignsIncremental(accountId, syncTime);
        syncAdSetsIncremental(accountId, syncTime);
        syncAdsIncremental(accountId, syncTime);

        logger.info("✅ Force full sync completed for account: {}", accountId);
    }

    /**
     * Get sync status for monitoring
     */
    public SyncStateDao.SyncStats getSyncStats() {
        return syncStateDao.getSyncStats();
    }

    /**
     * Test connectivity
     */
    public boolean testConnectivity() {
        return metaAdsConnector.testConnectivity();
    }
}