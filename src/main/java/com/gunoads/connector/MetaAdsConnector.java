package com.gunoads.connector;

import com.gunoads.config.MetaAdsConfig;
import com.gunoads.config.MetaApiProperties;
import com.gunoads.exception.MetaApiException;
import com.gunoads.model.dto.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.facebook.ads.sdk.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class MetaAdsConnector {

    private static final Logger logger = LoggerFactory.getLogger(MetaAdsConnector.class);

    @Autowired
    private MetaAdsConfig metaAdsConfig;

    @Autowired
    private MetaApiProperties metaApiProperties;

    @Autowired
    private MetaApiClient metaApiClient;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // ==================== ACCOUNT METHODS ====================

    /**
     * Fetch ad accounts from business (recommended approach)
     */
    public List<MetaAccountDto> fetchBusinessAccounts() throws MetaApiException {
        logger.info("Fetching ad accounts from business: {}", metaAdsConfig.getBusinessId());

        return metaApiClient.executeWithRetry(context -> {
            try {
                Business business = new Business(metaAdsConfig.getBusinessId(), context);
                APINodeList<AdAccount> accounts = business
                        .getOwnedAdAccounts()
                        .requestFields(metaApiProperties.getFields().getAccount()) // Fix: Use List directly
                        .execute();

                List<MetaAccountDto> accountDtos = new ArrayList<>();
                for (AdAccount account : accounts) {
                    accountDtos.add(mapAccountToDto(account));
                }

                logger.info("Successfully fetched {} ad accounts from business", accountDtos.size());
                return accountDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch business accounts: {}", e.getMessage());
                throw new MetaApiException("Failed to fetch business ad accounts", e);
            }
        });
    }

    /**
     * Fetch all ad accounts accessible to the current user
     */
    public List<MetaAccountDto> fetchAccounts() throws MetaApiException {
        logger.info("Fetching Meta ad accounts...");

        return metaApiClient.executeWithRetry(context -> {
            try {
                APINodeList<AdAccount> accounts = new User("me", context)
                        .getAdAccounts()
                        .requestFields(metaApiProperties.getFields().getAccount()) // Fix: Use List directly
                        .execute();

                List<MetaAccountDto> accountDtos = new ArrayList<>();
                for (AdAccount account : accounts) {
                    accountDtos.add(mapAccountToDto(account));
                }

                logger.info("Successfully fetched {} ad accounts", accountDtos.size());
                return accountDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch accounts: {}", e.getMessage());
                throw new MetaApiException("Failed to fetch ad accounts", e);
            }
        });
    }

    // ==================== CAMPAIGN METHODS ====================

    /**
     * Fetch campaigns for specific account
     */
    public List<MetaCampaignDto> fetchCampaigns(String accountId) throws MetaApiException {
        logger.info("Fetching campaigns for account: {}", accountId);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);
                APINodeList<Campaign> campaigns = account
                        .getCampaigns()
                        .requestFields(metaApiProperties.getFields().getCampaign()) // Fix: Use List directly
                        .execute();

                List<MetaCampaignDto> campaignDtos = new ArrayList<>();
                for (Campaign campaign : campaigns) {
                    campaignDtos.add(mapCampaignToDto(campaign));
                }

                logger.info("Successfully fetched {} campaigns for account {}", campaignDtos.size(), accountId);
                return campaignDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch campaigns for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch campaigns", e);
            }
        });
    }

    // ==================== ADSET METHODS ====================

    /**
     * Fetch ad sets for specific account
     */
    public List<MetaAdSetDto> fetchAdSets(String accountId) throws MetaApiException {
        logger.info("Fetching ad sets for account: {}", accountId);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);
                APINodeList<AdSet> adSets = account
                        .getAdSets()
                        .requestFields(metaApiProperties.getFields().getAdset()) // Fix: Use List directly
                        .execute();

                List<MetaAdSetDto> adSetDtos = new ArrayList<>();
                for (AdSet adSet : adSets) {
                    adSetDtos.add(mapAdSetToDto(adSet));
                }

                logger.info("Successfully fetched {} ad sets for account {}", adSetDtos.size(), accountId);
                return adSetDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch ad sets for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch ad sets", e);
            }
        });
    }

    // ==================== AD METHODS ====================

    /**
     * Fetch ads for specific account
     */
    public List<MetaAdDto> fetchAds(String accountId) throws MetaApiException {
        logger.info("Fetching ads for account: {}", accountId);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);
                APINodeList<Ad> ads = account
                        .getAds()
                        .requestFields(metaApiProperties.getFields().getAd()) // Fix: Use List directly
                        .execute();

                List<MetaAdDto> adDtos = new ArrayList<>();
                for (Ad ad : ads) {
                    adDtos.add(mapAdToDto(ad));
                }

                logger.info("Successfully fetched {} ads for account {}", adDtos.size(), accountId);
                return adDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch ads for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch ads", e);
            }
        });
    }

    // ==================== INSIGHTS METHODS ====================

    /**
     * Fetch insights for account with date range
     */
    public List<MetaInsightsDto> fetchInsights(String accountId, LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("Fetching insights for account {} from {} to {}", accountId, startDate, endDate);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);

                // Fix: Use correct insights request
                AdAccount.APIRequestGetInsights insightsRequest = account.getInsights()
                        .requestFields(metaApiProperties.getFields().getInsights()) // Fix: Use List directly
                        .setBreakdowns(String.valueOf(metaApiProperties.getDefaultBreakdowns()))
                        .setTimeRange("{\"since\":\"" + startDate.format(DateTimeFormatter.ISO_LOCAL_DATE) +
                                "\",\"until\":\"" + endDate.format(DateTimeFormatter.ISO_LOCAL_DATE) + "\"}");

                APINodeList<AdsInsights> insights = insightsRequest.execute();

                List<MetaInsightsDto> insightDtos = new ArrayList<>();
                for (AdsInsights insight : insights) {
                    insightDtos.add(mapInsightsToDto(insight));
                }

                logger.info("Successfully fetched {} insights for account {}", insightDtos.size(), accountId);
                return insightDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch insights for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch insights", e);
            }
        });
    }

    /**
     * Fetch insights for yesterday (most common use case)
     */
    public List<MetaInsightsDto> fetchYesterdayInsights(String accountId) throws MetaApiException {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        return fetchInsights(accountId, yesterday, yesterday);
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Test API connectivity using business accounts
     */
    public boolean testConnectivity() {
        try {
            List<MetaAccountDto> accounts = fetchBusinessAccounts();
            return accounts != null && !accounts.isEmpty();
        } catch (Exception e) {
            logger.error("Connectivity test failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get connector status
     */
    public ConnectorStatus getStatus() {
        MetaApiClient.ClientStatus clientStatus = metaApiClient.getStatus();
        boolean isConnected = testConnectivity();
        return new ConnectorStatus(clientStatus, isConnected);
    }

    // ==================== MAPPING METHODS ====================

    /**
     * Map AdAccount to DTO
     */
    private MetaAccountDto mapAccountToDto(AdAccount account) {
        MetaAccountDto dto = new MetaAccountDto();

        dto.setId(safeGetString(account.getId()));
        dto.setAccountId(safeGetString(account.getFieldAccountId()));
        dto.setAccountName(safeGetString(account.getFieldName()));
        dto.setCurrency(safeGetString(account.getFieldCurrency()));
        dto.setTimezoneId(safeGetInteger(account.getFieldTimezoneId()));
        dto.setTimezoneName(safeGetString(account.getFieldTimezoneName()));
        dto.setAccountStatus(safeGetString(account.getFieldAccountStatus()));
        dto.setAmountSpent(safeGetString(account.getFieldAmountSpent()));
        dto.setBalance(safeGetString(account.getFieldBalance()));
        dto.setSpendCap(safeGetString(account.getFieldSpendCap()));

        Business business = account.getFieldBusiness();
        if (business != null) {
            dto.setBusinessId(safeGetString(business.getId()));
            dto.setBusinessName(safeGetString(business.getFieldName()));
        }

        return dto;
    }

    /**
     * Map Campaign to DTO
     */
    private MetaCampaignDto mapCampaignToDto(Campaign campaign) {
        MetaCampaignDto dto = new MetaCampaignDto();

        dto.setId(safeGetString(campaign.getId()));
        dto.setAccountId(safeGetString(campaign.getFieldAccountId()));
        dto.setName(safeGetString(campaign.getFieldName()));
        dto.setObjective(safeGetString(campaign.getFieldObjective()));
        dto.setStatus(safeGetString(campaign.getFieldStatus()));
        dto.setConfiguredStatus(safeGetString(campaign.getFieldConfiguredStatus()));
        dto.setEffectiveStatus(safeGetString(campaign.getFieldEffectiveStatus()));
        dto.setStartTime(safeGetString(campaign.getFieldStartTime()));
        dto.setStopTime(safeGetString(campaign.getFieldStopTime()));
        dto.setCreatedTime(safeGetString(campaign.getFieldCreatedTime()));
        dto.setUpdatedTime(safeGetString(campaign.getFieldUpdatedTime()));
        dto.setBuyingType(safeGetString(campaign.getFieldBuyingType()));
        dto.setDailyBudget(safeGetString(campaign.getFieldDailyBudget()));
        dto.setLifetimeBudget(safeGetString(campaign.getFieldLifetimeBudget()));
        dto.setBudgetRemaining(safeGetString(campaign.getFieldBudgetRemaining()));
        dto.setBidStrategy(safeGetString(campaign.getFieldBidStrategy()));
        dto.setSpendCap(safeGetString(campaign.getFieldSpendCap()));

        return dto;
    }

    /**
     * Map AdSet to DTO
     */
    private MetaAdSetDto mapAdSetToDto(AdSet adSet) {
        MetaAdSetDto dto = new MetaAdSetDto();

        dto.setId(safeGetString(adSet.getId()));
        dto.setCampaignId(safeGetString(adSet.getFieldCampaignId()));
        dto.setName(safeGetString(adSet.getFieldName()));
        dto.setStatus(safeGetString(adSet.getFieldStatus()));
        dto.setConfiguredStatus(safeGetString(adSet.getFieldConfiguredStatus()));
        dto.setEffectiveStatus(safeGetString(adSet.getFieldEffectiveStatus()));
        dto.setLifetimeImps(safeGetLong(adSet.getFieldLifetimeImps()));
        dto.setStartTime(safeGetString(adSet.getFieldStartTime()));
        dto.setEndTime(safeGetString(adSet.getFieldEndTime()));
        dto.setCreatedTime(safeGetString(adSet.getFieldCreatedTime()));
        dto.setUpdatedTime(safeGetString(adSet.getFieldUpdatedTime()));
        dto.setOptimizationGoal(safeGetString(adSet.getFieldOptimizationGoal()));
        dto.setBillingEvent(safeGetString(adSet.getFieldBillingEvent()));
        dto.setBidAmount(safeGetString(adSet.getFieldBidAmount()));
        dto.setDailyBudget(safeGetString(adSet.getFieldDailyBudget()));
        dto.setLifetimeBudget(safeGetString(adSet.getFieldLifetimeBudget()));
        dto.setBudgetRemaining(safeGetString(adSet.getFieldBudgetRemaining()));
        dto.setIsAutobid(false);

        // Handle targeting safely
        Targeting targeting = adSet.getFieldTargeting();
        if (targeting != null) {
            try {
                dto.setTargeting(objectMapper.writeValueAsString(targeting));
            } catch (Exception e) {
                logger.warn("Failed to serialize targeting for adset {}: {}", dto.getId(), e.getMessage());
                dto.setTargeting("{}");
            }
        }

        return dto;
    }

    /**
     * Map Ad to DTO
     */
    private MetaAdDto mapAdToDto(Ad ad) {
        MetaAdDto dto = new MetaAdDto();

        dto.setId(safeGetString(ad.getId()));
        dto.setAdsetId(safeGetString(ad.getFieldAdsetId()));
        dto.setName(safeGetString(ad.getFieldName()));
        dto.setStatus(safeGetString(ad.getFieldStatus()));
        dto.setConfiguredStatus(safeGetString(ad.getFieldConfiguredStatus()));
        dto.setEffectiveStatus(safeGetString(ad.getFieldEffectiveStatus()));
        dto.setCreatedTime(safeGetString(ad.getFieldCreatedTime()));
        dto.setUpdatedTime(safeGetString(ad.getFieldUpdatedTime()));

        // Creative information
        AdCreative creative = ad.getFieldCreative();
        if (creative != null) {
            MetaAdDto.Creative creativeDto = new MetaAdDto.Creative();
            creativeDto.setId(safeGetString(creative.getId()));
            creativeDto.setName(safeGetString(creative.getFieldName()));
            dto.setCreative(creativeDto);
        }

        return dto;
    }

    /**
     * Map AdsInsights to DTO - Fix: Use correct DTO fields only
     */
    private MetaInsightsDto mapInsightsToDto(AdsInsights insight) {
        MetaInsightsDto dto = new MetaInsightsDto();

        // Basic fields
        dto.setAccountId(safeGetString(insight.getFieldAccountId()));
        dto.setCampaignId(safeGetString(insight.getFieldCampaignId()));
        dto.setAdsetId(safeGetString(insight.getFieldAdsetId()));
        dto.setAdId(safeGetString(insight.getFieldAdId()));
        dto.setDateStart(safeGetString(insight.getFieldDateStart()));
        dto.setDateStop(safeGetString(insight.getFieldDateStop()));

        // Performance metrics - Fix: Use correct field names and types
        dto.setSpend(safeGetString(insight.getFieldSpend()));
        dto.setImpressions(safeGetString(insight.getFieldImpressions())); // Fix: String not Long
        dto.setClicks(safeGetString(insight.getFieldClicks())); // Fix: String not Long
        dto.setUniqueClicks(safeGetString(insight.getFieldUniqueClicks())); // Fix: String not Long
        dto.setReach(safeGetString(insight.getFieldReach())); // Fix: String not Long
        dto.setFrequency(safeGetString(insight.getFieldFrequency()));
        dto.setCpc(safeGetString(insight.getFieldCpc()));
        dto.setCpm(safeGetString(insight.getFieldCpm()));
        dto.setCpp(safeGetString(insight.getFieldCpp()));
        dto.setCtr(safeGetString(insight.getFieldCtr()));

        // Breakdowns - Fix: Use correct field names
        dto.setAge(safeGetString(insight.getFieldAge()));
        dto.setGender(safeGetString(insight.getFieldGender()));
        dto.setCountry(safeGetString(insight.getFieldCountry()));
        dto.setRegion(safeGetString(insight.getFieldRegion()));
        dto.setCity(safeGetString(insight.getFieldLocation()));
        dto.setPlacement(safeGetString(insight.getFieldPublisherPlatform()));
        dto.setDevice_platform(safeGetString(insight.getFieldPlatformPosition())); // Fix: Use existing DTO field
        dto.setCurrency(safeGetString(insight.getFieldAccountCurrency()));

        // Extract action metrics safely
        extractActionMetrics(insight, dto);

        return dto;
    }

    /**
     * Extract action metrics from insights - Fix: Use correct DTO setters
     */
    private void extractActionMetrics(AdsInsights insight, MetaInsightsDto dto) {
        try {
            List<AdsActionStats> actions = insight.getFieldActions();
            if (actions != null) {
                for (AdsActionStats action : actions) {
                    String actionType = action.getFieldActionType();
                    String value = safeGetString(action.getFieldValue()); // Fix: Get as String

                    if (actionType != null && value != null) {
                        // Only set fields that exist in DTO
                        switch (actionType) {
                            case "post_engagement":
                                // dto.setPostEngagement(value); // Fix: Only if exists in DTO
                                break;
                            case "page_engagement":
                                // dto.setPageEngagement(value); // Fix: Only if exists in DTO
                                break;
                            case "like":
                                // dto.setLikes(value); // Fix: Only if exists in DTO
                                break;
                            case "video_view":
                                // dto.setVideoViews(value); // Fix: Only if exists in DTO
                                break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to extract action metrics: {}", e.getMessage());
        }
    }

    // ==================== SAFE GETTER UTILITIES ====================

    private String safeGetString(Object value) {
        return value != null ? value.toString() : null;
    }

    private Integer safeGetInteger(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private Long safeGetLong(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return Long.parseLong(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private Boolean safeGetBoolean(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            return Boolean.parseBoolean(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    // ==================== INNER CLASSES ====================

    public static class ConnectorStatus {
        public final MetaApiClient.ClientStatus clientStatus;
        public final boolean isConnected;

        public ConnectorStatus(MetaApiClient.ClientStatus clientStatus, boolean isConnected) {
            this.clientStatus = clientStatus;
            this.isConnected = isConnected;
        }
    }
}