/**
 * MetaAdsConnector - Clean and Essential Version
 * Loại bỏ code thừa thãi, giữ lại functions cần thiết
 */
package com.gunoads.connector;

import com.gunoads.config.MetaAdsConfig;
import com.gunoads.config.MetaApiProperties;
import com.gunoads.dao.SyncStateDao;
import com.gunoads.exception.MetaApiException;
import com.gunoads.model.dto.*;
import com.gunoads.util.InsightsDataMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.facebook.ads.sdk.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class MetaAdsConnector {
    private static final Logger logger = LoggerFactory.getLogger(MetaAdsConnector.class);

    @Autowired private MetaAdsConfig metaAdsConfig;
    @Autowired private MetaApiProperties metaApiProperties;
    @Autowired private MetaApiClient metaApiClient;
    @Autowired private SyncStateDao syncStateDao;
    @Autowired private InsightsDataMerger insightsDataMerger;

    // ==================== CORE FETCH METHODS ====================

    /**
     * Fetch business accounts (active only)
     */
    public List<MetaAccountDto> fetchBusinessAccounts() throws MetaApiException {
        logger.info("Fetching ACTIVE business accounts");

        return metaApiClient.executeWithRetry(context -> {
            Business business = new Business(metaAdsConfig.getBusinessId(), context);

            APINodeList<AdAccount> accounts = business.getOwnedAdAccounts()
                    .requestFields(metaApiProperties.getFields().getAccount())
                    .setParam("limit", metaApiProperties.getPagination().getMaxLimit())
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaAccountDto> result = new ArrayList<>();
            int totalCount = 0;
            int activeCount = 0;

            for (AdAccount account : accounts) {
                MetaAccountDto dto = mapAccountToDto(account);
                totalCount++;

                if (isAccountActive(dto)) {
                    result.add(dto);
                    activeCount++;
                }

                if (totalCount % 50 == 0) {
                    logger.debug("Processed {} accounts ({} active)...", totalCount, activeCount);
                }
            }

            logger.info("✅ Fetched {} active accounts from {} total", activeCount, totalCount);
            return result;
        });
    }

    /**
     * Fetch campaigns (optimized with auto-pagination)
     */
    public List<MetaCampaignDto> fetchCampaigns(String accountId) throws MetaApiException {
        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            APINodeList<Campaign> campaigns = account.getCampaigns()
                    .requestFields(metaApiProperties.getFields().getCampaign())
                    .setParam("limit", metaApiProperties.getPagination().getMaxLimit())
                    .setParam("filtering", "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}]")
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaCampaignDto> result = new ArrayList<>();
            for (Campaign campaign : campaigns) {
                result.add(mapCampaignToDto(campaign));
            }

            logger.info("✅ Fetched {} campaigns for account: {}", result.size(), accountId);
            return result;
        });
    }

    /**
     * Fetch adsets (optimized with auto-pagination)
     */
    public List<MetaAdSetDto> fetchAdSets(String accountId) throws MetaApiException {
        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            APINodeList<AdSet> adSets = account.getAdSets()
                    .requestFields(metaApiProperties.getFields().getAdset())
                    .setParam("limit", metaApiProperties.getPagination().getMaxLimit())
                    .setParam("filtering", "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}]")
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaAdSetDto> result = new ArrayList<>();
            for (AdSet adSet : adSets) {
                result.add(mapAdSetToDto(adSet));
            }

            logger.info("✅ Fetched {} adsets for account: {}", result.size(), accountId);
            return result;
        });
    }

    /**
     * Fetch ads (optimized with auto-pagination)
     */
    public List<MetaAdDto> fetchAds(String accountId) throws MetaApiException {
        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            APINodeList<Ad> ads = account.getAds()
                    .requestFields(metaApiProperties.getFields().getAd())
                    .setParam("limit", metaApiProperties.getPagination().getMaxLimit())
                    .setParam("filtering", "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}]")
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaAdDto> result = new ArrayList<>();
            for (Ad ad : ads) {
                result.add(mapAdToDto(ad));
            }

            logger.info("✅ Fetched {} ads for account: {}", result.size(), accountId);
            return result;
        });
    }

    /**
     * Fetch insights (batched breakdowns + auto-pagination)
     */
    public List<MetaInsightsDto> fetchInsights(String accountId, LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("🔄 Fetching SMART BATCHED insights for account {} from {} to {}", accountId, startDate, endDate);

        long overallStartTime = System.currentTimeMillis();

        try {
            // Step 1: Fetch 3 breakdown batches concurrently
            List<MetaInsightsDto> batch1Results = fetchInsightsBatch(accountId, startDate, endDate, 0); // demographic
            List<MetaInsightsDto> batch2Results = fetchInsightsBatch(accountId, startDate, endDate, 1); // geographic
            List<MetaInsightsDto> batch3Results = fetchInsightsBatch(accountId, startDate, endDate, 2); // placement

            // Step 2: Validate batch consistency
            if (!insightsDataMerger.validateBatchConsistency(batch1Results, batch2Results, batch3Results)) {
                logger.warn("⚠️  Batch consistency issues detected - proceeding with available data");
            }

            // Step 3: Merge all batches into complete data
            List<MetaInsightsDto> mergedResults = insightsDataMerger.mergeAllBatches(
                    batch1Results, batch2Results, batch3Results);

            long totalDuration = System.currentTimeMillis() - overallStartTime;

            logger.info("✅ SMART BATCHED insights completed: {} final records in {}ms ({:.2f}s)",
                    mergedResults.size(), totalDuration, totalDuration / 1000.0);

            return mergedResults;

        } catch (Exception e) {
            logger.error("❌ Smart batch insights fetch failed: {}", e.getMessage());

            // Fallback: Try with minimal breakdown to get some data
            logger.warn("🔄 Attempting fallback with demographic data only...");
            return fetchInsightsBatch(accountId, startDate, endDate, 0); // demographic only
        }
    }

    /**
     * NEW: Fetch insights for specific breakdown batch
     *
     * @param accountId Account ID
     * @param startDate Start date
     * @param endDate End date
     * @param batchIndex Batch index (0=demographic, 1=geographic, 2=placement)
     * @return List of insights with specific breakdown data
     */
    private List<MetaInsightsDto> fetchInsightsBatch(String accountId, LocalDate startDate, LocalDate endDate, int batchIndex) throws MetaApiException {
        List<String> breakdowns = metaApiProperties.getBreakdownBatch(batchIndex);
        String batchName = metaApiProperties.getBatchName(batchIndex);

        logger.debug("📊 Fetching {} batch insights ({}): {}", batchName, batchIndex, breakdowns);

        long batchStartTime = System.currentTimeMillis();

        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            // Build insights request with specific breakdown batch
            AdAccount.APIRequestGetInsights insightsRequest = account.getInsights()
                    .requestFields(metaApiProperties.getFields().getInsights())
                    .setBreakdowns(String.join(",", breakdowns)) // Use specific batch breakdowns
                    .setLevel("ad")
                    .setTimeRange("{\"since\":\"" + startDate.format(DateTimeFormatter.ISO_LOCAL_DATE) +
                            "\",\"until\":\"" + endDate.format(DateTimeFormatter.ISO_LOCAL_DATE) + "\"}")
                    .setLimit(metaApiProperties.getPagination().getMaxLimit());

            APINodeList<AdsInsights> insights = insightsRequest
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaInsightsDto> result = new ArrayList<>();
            int count = 0;

            for (AdsInsights insight : insights) {
                result.add(mapInsightsToDto(insight));
                count++;

                if (count % 250 == 0) { // More frequent logging for batch progress
                    logger.debug("   📈 {} batch: {} insights processed...", batchName, count);
                }
            }

            long batchDuration = System.currentTimeMillis() - batchStartTime;

            logger.info("✅ {} batch completed: {} records in {}ms", batchName, result.size(), batchDuration);
            return result;
        });
    }

    // ==================== FALLBACK & ERROR HANDLING ====================

    /**
     * NEW: Fetch insights with fallback strategy if smart batching fails
     */
    public List<MetaInsightsDto> fetchInsightsWithFallback(String accountId, LocalDate startDate, LocalDate endDate) throws MetaApiException {
        try {
            // Primary: Smart 3-batch strategy
            return fetchInsights(accountId, startDate, endDate);

        } catch (MetaApiException e) {
            logger.error("❌ Smart batch strategy failed: {}", e.getMessage());

            // Fallback 1: Try demographic data only
            try {
                logger.warn("🔄 Fallback 1: Demographic data only...");
                return fetchInsightsBatch(accountId, startDate, endDate, 0);

            } catch (Exception e2) {
                logger.error("❌ Demographic fallback failed: {}", e2.getMessage());

                // Fallback 2: No breakdowns at all
                logger.warn("🔄 Fallback 2: No breakdowns...");
                return fetchInsightsNoBreakdowns(accountId, startDate, endDate);
            }
        }
    }

    /**
     * NEW: Emergency fallback - fetch insights without any breakdowns
     */
    private List<MetaInsightsDto> fetchInsightsNoBreakdowns(String accountId, LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.warn("⚠️  Emergency fallback: Fetching insights without breakdowns");

        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            AdAccount.APIRequestGetInsights insightsRequest = account.getInsights()
                    .requestFields(metaApiProperties.getFields().getInsights())
                    // NO BREAKDOWNS - just core metrics
                    .setLevel("ad")
                    .setTimeRange("{\"since\":\"" + startDate.format(DateTimeFormatter.ISO_LOCAL_DATE) +
                            "\",\"until\":\"" + endDate.format(DateTimeFormatter.ISO_LOCAL_DATE) + "\"}")
                    .setLimit(metaApiProperties.getPagination().getMaxLimit());

            APINodeList<AdsInsights> insights = insightsRequest
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaInsightsDto> result = new ArrayList<>();
            for (AdsInsights insight : insights) {
                MetaInsightsDto dto = mapInsightsToDto(insight);

                // Set default values for missing breakdown data
                if (dto.getAge() == null) dto.setAge("unknown");
                if (dto.getGender() == null) dto.setGender("unknown");
                if (dto.getCountry() == null) dto.setCountry("unknown");
                if (dto.getRegion() == null) dto.setRegion("unknown");

                result.add(dto);
            }

            logger.warn("⚠️  Emergency fallback completed: {} records (no breakdown data)", result.size());
            return result;
        });
    }

    // ==================== BACKWARD COMPATIBILITY ====================

    /**
     * @deprecated Use fetchInsights() which now uses smart batching
     * Kept for backward compatibility
     */
    @Deprecated
    public List<MetaInsightsDto> fetchInsightsOldMethod(String accountId, LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.warn("⚠️  Using deprecated fetchInsightsOldMethod - this may cause breakdown errors");

        // Delegate to new smart method
        return fetchInsights(accountId, startDate, endDate);
    }

    // ==================== INCREMENTAL FETCH METHODS ====================

    /**
     * Fetch campaigns incremental (with timestamp filtering)
     */
    public List<MetaCampaignDto> fetchCampaignsIncremental(String accountId) throws MetaApiException {
        LocalDateTime lastSync = syncStateDao.getLastSyncTime(SyncStateDao.ObjectType.CAMPAIGNS, accountId);

        if (lastSync == null) {
            logger.info("🆕 No previous sync - using full fetch for campaigns");
            return fetchCampaigns(accountId);
        }

        LocalDateTime since = lastSync.minusHours(1); // 1-hour overlap
        String sinceStr = since.format(DateTimeFormatter.ISO_INSTANT);

        logger.info("📈 Incremental campaigns fetch: since={}", since);

        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            // Combined filter: ACTIVE status AND updated_time
            String combinedFilter = String.format(
                    "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}," +
                            "{\"field\":\"updated_time\",\"operator\":\"GREATER_THAN\",\"value\":\"%s\"}]",
                    sinceStr
            );

            APINodeList<Campaign> campaigns = account.getCampaigns()
                    .requestFields(metaApiProperties.getFields().getCampaign())
                    .setParam("filtering", combinedFilter)
                    .setParam("limit", metaApiProperties.getPagination().getMaxLimit())
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaCampaignDto> result = new ArrayList<>();
            for (Campaign campaign : campaigns) {
                result.add(mapCampaignToDto(campaign));
            }

            logger.info("✅ Incremental campaigns: {} updated for account {}", result.size(), accountId);
            return result;
        });
    }

    /**
     * Fetch adsets incremental (with timestamp filtering)
     */
    public List<MetaAdSetDto> fetchAdSetsIncremental(String accountId) throws MetaApiException {
        LocalDateTime lastSync = syncStateDao.getLastSyncTime(SyncStateDao.ObjectType.ADSETS, accountId);

        if (lastSync == null) {
            logger.info("🆕 No previous sync - using full fetch for adsets");
            return fetchAdSets(accountId);
        }

        LocalDateTime since = lastSync.minusHours(1);
        String sinceStr = since.format(DateTimeFormatter.ISO_INSTANT);

        logger.info("📈 Incremental adsets fetch: since={}", since);

        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            // Combined filter: ACTIVE status AND updated_time
            String combinedFilter = String.format(
                    "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}," +
                            "{\"field\":\"updated_time\",\"operator\":\"GREATER_THAN\",\"value\":\"%s\"}]",
                    sinceStr
            );

            APINodeList<AdSet> adSets = account.getAdSets()
                    .requestFields(metaApiProperties.getFields().getAdset())
                    .setParam("filtering", combinedFilter)
                    .setParam("limit", metaApiProperties.getPagination().getMaxLimit())
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaAdSetDto> result = new ArrayList<>();
            for (AdSet adSet : adSets) {
                result.add(mapAdSetToDto(adSet));
            }

            logger.info("✅ Incremental adsets: {} updated for account {}", result.size(), accountId);
            return result;
        });
    }

    /**
     * Fetch ads incremental (with timestamp filtering)
     */
    public List<MetaAdDto> fetchAdsIncremental(String accountId) throws MetaApiException {
        LocalDateTime lastSync = syncStateDao.getLastSyncTime(SyncStateDao.ObjectType.ADS, accountId);

        if (lastSync == null) {
            logger.info("🆕 No previous sync - using full fetch for ads");
            return fetchAds(accountId);
        }

        LocalDateTime since = lastSync.minusHours(1);
        String sinceStr = since.format(DateTimeFormatter.ISO_INSTANT);

        logger.info("📈 Incremental ads fetch: since={}", since);

        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            // Combined filter: ACTIVE status AND updated_time
            String combinedFilter = String.format(
                    "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}," +
                            "{\"field\":\"updated_time\",\"operator\":\"GREATER_THAN\",\"value\":\"%s\"}]",
                    sinceStr
            );

            APINodeList<Ad> ads = account.getAds()
                    .requestFields(metaApiProperties.getFields().getAd())
                    .setParam("filtering", combinedFilter)
                    .setParam("limit", metaApiProperties.getPagination().getMaxLimit())
                    .execute()
                    .withAutoPaginationIterator(true);

            List<MetaAdDto> result = new ArrayList<>();
            for (Ad ad : ads) {
                result.add(mapAdToDto(ad));
            }

            logger.info("✅ Incremental ads: {} updated for account {}", result.size(), accountId);
            return result;
        });
    }

    // ==================== CONVENIENCE METHODS ====================

    /**
     * Fetch today's insights
     */
    public List<MetaInsightsDto> fetchTodayInsights(String accountId) throws MetaApiException {
        LocalDate today = LocalDate.now();
        return fetchInsights(accountId, today, today);
    }

    /**
     * Fetch yesterday's insights
     */
    public List<MetaInsightsDto> fetchYesterdayInsights(String accountId) throws MetaApiException {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        return fetchInsights(accountId, yesterday, yesterday);
    }

    /**
     * Test API connectivity
     */
    public boolean testConnectivity() {
        try {
            metaApiClient.executeWithRetry(context -> {
                Business business = new Business(metaAdsConfig.getBusinessId(), context);
                business.getOwnedAdAccounts()
                        .requestFields(List.of("id", "name"))
                        .setParam("limit", 1)
                        .execute();
                return true;
            });
            return true;
        } catch (Exception e) {
            logger.error("Connectivity test failed: {}", e.getMessage());
            return false;
        }
    }

    // ==================== MAPPING METHODS ====================

    private MetaAccountDto mapAccountToDto(AdAccount account) {
        MetaAccountDto dto = new MetaAccountDto();
        dto.setId(account.getFieldId());
        dto.setAccountName(account.getFieldName());
        dto.setAccountStatus(account.getFieldAccountStatus() != null ?
                account.getFieldAccountStatus().toString() : null);
        dto.setCurrency(account.getFieldCurrency());
        dto.setBusinessId(account.getId());
        dto.setTimezoneId(account.getFieldTimezoneId());
        dto.setTimezoneName(account.getFieldTimezoneName());
        dto.setAmountSpent(account.getFieldAmountSpent());
        dto.setBalance(account.getFieldBalance());
        dto.setSpendCap(account.getFieldSpendCap());
        return dto;
    }

    private MetaCampaignDto mapCampaignToDto(Campaign campaign) {
        MetaCampaignDto dto = new MetaCampaignDto();
        dto.setId(campaign.getFieldId());
        dto.setAccountId(campaign.getFieldAccountId());
        dto.setName(campaign.getFieldName());
        dto.setStatus(campaign.getFieldStatus() != null ?
                campaign.getFieldStatus().toString() : null);
        dto.setObjective(campaign.getFieldObjective() != null ?
                campaign.getFieldObjective().toString() : null);
        dto.setBuyingType(campaign.getFieldBuyingType() != null ?
                campaign.getFieldBuyingType().toString() : null);
        dto.setDailyBudget(campaign.getFieldDailyBudget());
        dto.setLifetimeBudget(campaign.getFieldLifetimeBudget());
        dto.setCreatedTime(campaign.getFieldCreatedTime());
        dto.setUpdatedTime(campaign.getFieldUpdatedTime());
        return dto;
    }

    private MetaAdSetDto mapAdSetToDto(AdSet adSet) {
        MetaAdSetDto dto = new MetaAdSetDto();
        dto.setId(adSet.getFieldId());
        dto.setCampaignId(adSet.getFieldCampaignId());
        dto.setName(adSet.getFieldName());
        dto.setStatus(adSet.getFieldStatus() != null ?
                adSet.getFieldStatus().toString() : null);
        dto.setDailyBudget(adSet.getFieldDailyBudget());
        dto.setLifetimeBudget(adSet.getFieldLifetimeBudget());
        dto.setBidAmount(adSet.getFieldBidAmount());
        dto.setCreatedTime(adSet.getFieldCreatedTime());
        dto.setUpdatedTime(adSet.getFieldUpdatedTime());
        return dto;
    }

    private MetaAdDto mapAdToDto(Ad ad) {
        MetaAdDto dto = new MetaAdDto();
        dto.setId(ad.getFieldId());
        dto.setAdsetId(ad.getFieldAdsetId());
        dto.setName(ad.getFieldName());
        dto.setStatus(ad.getFieldStatus() != null ?
                ad.getFieldStatus().toString() : null);
        dto.setConfiguredStatus(ad.getFieldConfiguredStatus() != null ?
                ad.getFieldConfiguredStatus().toString() : null);
        dto.setEffectiveStatus(ad.getFieldEffectiveStatus() != null ?
                ad.getFieldEffectiveStatus().toString() : null);
        dto.setCreatedTime(ad.getFieldCreatedTime());
        dto.setUpdatedTime(ad.getFieldUpdatedTime());

        // Handle creative
        AdCreative creative = ad.getFieldCreative();
        if (creative != null) {
            MetaAdDto.Creative creativeDto = new MetaAdDto.Creative();
            creativeDto.setId(creative.getFieldId());
            creativeDto.setName(creative.getFieldName());
            dto.setCreative(creativeDto);
        }

        return dto;
    }

    private MetaInsightsDto mapInsightsToDto(AdsInsights insight) {
        MetaInsightsDto dto = new MetaInsightsDto();

        // Basic identifiers
        dto.setAccountId(insight.getFieldAccountId());
        dto.setCampaignId(insight.getFieldCampaignId());
        dto.setAdsetId(insight.getFieldAdsetId());
        dto.setAdId(insight.getFieldAdId());
        dto.setDateStart(insight.getFieldDateStart());
        dto.setDateStop(insight.getFieldDateStop());

        // Core performance metrics
        dto.setImpressions(insight.getFieldImpressions());
        dto.setClicks(insight.getFieldClicks());
        dto.setSpend(insight.getFieldSpend());
        dto.setCtr(insight.getFieldCtr());
        dto.setCpc(insight.getFieldCpc());
        dto.setCpm(insight.getFieldCpm());
        dto.setCpp(insight.getFieldCpp());
        dto.setUniqueClicks(insight.getFieldUniqueClicks());
        dto.setReach(insight.getFieldReach());
        dto.setFrequency(insight.getFieldFrequency());

        // Breakdown fields
        dto.setAge(insight.getFieldAge());
        dto.setGender(insight.getFieldGender());
        dto.setCountry(insight.getFieldCountry());
        dto.setRegion(insight.getFieldRegion());

        // Device and placement
        String devicePlatform = insight.getFieldImpressionDevice();
        if (devicePlatform != null) {
            dto.setDevice_platform(devicePlatform);
        }

        String placement = insight.getFieldPublisherPlatform();
        if (placement != null) {
            dto.setPlacement(placement);
        }

        // Use region as city fallback
        String city = insight.getFieldRegion();
        if (city != null) {
            dto.setCity(city);
        }

        return dto;
    }

    // ==================== HELPER METHODS ====================

    private boolean isAccountActive(MetaAccountDto account) {
        return "1".equals(account.getAccountStatus()) || "ACTIVE".equals(account.getAccountStatus());
    }
}