package com.gunoads.service;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.dao.*;
import com.gunoads.model.dto.*;
import com.gunoads.model.entity.*;
import com.gunoads.processor.DataTransformer;
import com.gunoads.exception.MetaApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Service
public class MetaAdsService {

    private static final Logger logger = LoggerFactory.getLogger(MetaAdsService.class);

    @Autowired
    private MetaAdsConnector metaAdsConnector;

    @Autowired
    private DataTransformer dataTransformer;

    @Autowired
    private AccountDao accountDao;

    @Autowired
    private CampaignDao campaignDao;

    @Autowired
    private AdSetDao adSetDao;

    @Autowired
    private AdvertisementDao advertisementDao;

    @Autowired
    private PlacementDao placementDao;

    @Autowired
    private AdsReportingDao adsReportingDao;

    /**
     * Sync account hierarchy (accounts, campaigns, adsets, ads)
     */
    @Transactional
    public void syncAccountHierarchy() throws MetaApiException {
        logger.info("Starting account hierarchy sync...");

        try {
            // 1. Sync accounts
            List<MetaAccountDto> accountDtos = metaAdsConnector.fetchAccounts();
            List<Account> accounts = dataTransformer.transformAccounts(accountDtos);

            for (Account account : accounts) {
                try {
                    if (accountDao.existsById(account.getId())) {
                        accountDao.update(account);
                    } else {
                        accountDao.insert(account);
                    }
                    logger.debug("Synced account: {}", account.getId());
                } catch (Exception e) {
                    logger.error("Failed to sync account {}: {}", account.getId(), e.getMessage());
                }
            }

            // 2. Sync campaigns for each account
            for (Account account : accounts) {
                syncCampaignsForAccount(account.getId());
            }

            logger.info("Account hierarchy sync completed successfully");

        } catch (Exception e) {
            logger.error("Account hierarchy sync failed: {}", e.getMessage());
            throw new MetaApiException("Account hierarchy sync failed", e);
        }
    }

    /**
     * Sync campaigns for specific account
     */
    @Transactional
    public void syncCampaignsForAccount(String accountId) throws MetaApiException {
        logger.info("Syncing campaigns for account: {}", accountId);

        try {
            // 1. Fetch and sync campaigns
            List<MetaCampaignDto> campaignDtos = metaAdsConnector.fetchCampaigns(accountId);
            List<Campaign> campaigns = dataTransformer.transformCampaigns(campaignDtos);

            for (Campaign campaign : campaigns) {
                try {
                    if (campaignDao.existsById(campaign.getId())) {
                        campaignDao.update(campaign);
                    } else {
                        campaignDao.insert(campaign);
                    }
                    logger.debug("Synced campaign: {}", campaign.getId());
                } catch (Exception e) {
                    logger.error("Failed to sync campaign {}: {}", campaign.getId(), e.getMessage());
                }
            }

            // 2. Sync adsets for this account
            List<MetaAdSetDto> adSetDtos = metaAdsConnector.fetchAdSets(accountId);
            List<AdSet> adSets = dataTransformer.transformAdSets(adSetDtos);

            for (AdSet adSet : adSets) {
                try {
                    if (adSetDao.existsById(adSet.getId())) {
                        adSetDao.update(adSet);
                    } else {
                        adSetDao.insert(adSet);
                    }
                    logger.debug("Synced adset: {}", adSet.getId());
                } catch (Exception e) {
                    logger.error("Failed to sync adset {}: {}", adSet.getId(), e.getMessage());
                }
            }

            // 3. Sync ads for this account
            List<MetaAdDto> adDtos = metaAdsConnector.fetchAds(accountId);
            List<Advertisement> ads = dataTransformer.transformAdvertisements(adDtos);

            for (Advertisement ad : ads) {
                try {
                    if (advertisementDao.existsById(ad.getId())) {
                        advertisementDao.update(ad);
                    } else {
                        advertisementDao.insert(ad);
                    }
                    logger.debug("Synced ad: {}", ad.getId());
                } catch (Exception e) {
                    logger.error("Failed to sync ad {}: {}", ad.getId(), e.getMessage());
                }
            }

            logger.info("Campaigns sync completed for account: {}", accountId);

        } catch (Exception e) {
            logger.error("Campaigns sync failed for account {}: {}", accountId, e.getMessage());
            throw new MetaApiException("Campaigns sync failed", e);
        }
    }

    /**
     * Sync performance data (insights) for specific account and date range
     */
    @Transactional
    public void syncPerformanceData(String accountId, LocalDate startDate, LocalDate endDate)
            throws MetaApiException {

        logger.info("Syncing performance data for account: {} from {} to {}",
                accountId, startDate, endDate);

        try {
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

            // Use bulk insert for performance data
            if (!reportingList.isEmpty()) {
                try {
                    // For large datasets, use COPY FROM via DAO
                    adsReportingDao.batchInsert(reportingList);
                    logger.info("Successfully inserted {} performance records", reportingList.size());
                } catch (Exception e) {
                    logger.error("Bulk insert failed, trying individual inserts: {}", e.getMessage());

                    // Fallback to individual inserts
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
            // Get all accounts
            List<MetaAccountDto> accountDtos = metaAdsConnector.fetchAccounts();
            LocalDate yesterday = LocalDate.now().minusDays(1);

            for (MetaAccountDto accountDto : accountDtos) {
                try {
                    syncPerformanceData(accountDto.getId(), yesterday, yesterday);
                } catch (Exception e) {
                    logger.error("Failed to sync performance data for account {}: {}",
                            accountDto.getId(), e.getMessage());
                    // Continue with other accounts
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
     * Test connectivity with Meta API
     */
    public boolean testConnectivity() {
        try {
            return metaAdsConnector.testConnectivity();
        } catch (Exception e) {
            logger.error("Connectivity test failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get sync status summary
     */
    public SyncStatus getSyncStatus() {
        try {
            long accountCount = accountDao.count();
            long campaignCount = campaignDao.count();
            long adSetCount = adSetDao.count();
            long adCount = advertisementDao.count();
            long reportingCount = adsReportingDao.count();

            boolean isConnected = testConnectivity();

            return new SyncStatus(
                    isConnected,
                    accountCount,
                    campaignCount,
                    adSetCount,
                    adCount,
                    reportingCount
            );
        } catch (Exception e) {
            logger.error("Failed to get sync status: {}", e.getMessage());
            return new SyncStatus(false, 0, 0, 0, 0, 0);
        }
    }

    /**
     * Sync status summary class
     */
    public static class SyncStatus {
        public final boolean isConnected;
        public final long accountCount;
        public final long campaignCount;
        public final long adSetCount;
        public final long adCount;
        public final long reportingCount;

        public SyncStatus(boolean isConnected, long accountCount, long campaignCount,
                          long adSetCount, long adCount, long reportingCount) {
            this.isConnected = isConnected;
            this.accountCount = accountCount;
            this.campaignCount = campaignCount;
            this.adSetCount = adSetCount;
            this.adCount = adCount;
            this.reportingCount = reportingCount;
        }

        @Override
        public String toString() {
            return String.format("SyncStatus{connected=%s, accounts=%d, campaigns=%d, adsets=%d, ads=%d, reports=%d}",
                    isConnected, accountCount, campaignCount, adSetCount, adCount, reportingCount);
        }
    }
}