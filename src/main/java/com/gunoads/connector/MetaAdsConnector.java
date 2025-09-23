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
        logger.info("🔄 Fetching BATCHED insights for account {} from {} to {}", accountId, startDate, endDate);

        return metaApiClient.executeWithRetry(context -> {
            AdAccount account = new AdAccount(accountId, context);

            AdAccount.APIRequestGetInsights insightsRequest = account.getInsights()
                    .requestFields(metaApiProperties.getFields().getInsights())
                    .setBreakdowns(metaApiProperties.getDefaultBreakdownsString())
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

                if (count % 500 == 0) {
                    logger.debug("   📈 Processed {} insights...", count);
                }
            }

            logger.info("✅ BATCHED insights fetch: {} records for account {}", result.size(), accountId);
            return result;
        });
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