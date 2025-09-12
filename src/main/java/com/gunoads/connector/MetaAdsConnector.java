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

    /**
     * Fetch all ad accounts accessible to the current user
     */
    public List<MetaAccountDto> fetchAccounts() throws MetaApiException {
        logger.info("Fetching Meta ad accounts...");

        return metaApiClient.executeWithRetry(context -> {
            try {
                APINodeList<AdAccount> accounts = new User("me", context)
                        .getAdAccounts()
                        .requestAllFields()
                        .execute();

                List<MetaAccountDto> accountDtos = new ArrayList<>();

                for (AdAccount account : accounts) {
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
                    dto.setCreatedTime(safeGetString(account.getFieldCreatedTime()));

                    Business business = account.getFieldBusiness();
                    if (business != null) {
                        dto.setBusinessId(safeGetString(business.getId()));
                        dto.setBusinessName(safeGetString(business.getFieldName()));
                    }

                    accountDtos.add(dto);
                }

                logger.info("Successfully fetched {} ad accounts", accountDtos.size());
                return accountDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch accounts: {}", e.getMessage());
                throw new MetaApiException("Failed to fetch ad accounts", e);
            }
        });
    }

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
                        .requestAllFields()
                        .execute();

                List<MetaCampaignDto> campaignDtos = new ArrayList<>();

                for (Campaign campaign : campaigns) {
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

                    campaignDtos.add(dto);
                }

                logger.info("Successfully fetched {} campaigns for account {}", campaignDtos.size(), accountId);
                return campaignDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch campaigns for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch campaigns", e);
            }
        });
    }

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
                        .requestAllFields()
                        .execute();

                List<MetaAdSetDto> adSetDtos = new ArrayList<>();

                for (AdSet adSet : adSets) {
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

                    adSetDtos.add(dto);
                }

                logger.info("Successfully fetched {} ad sets for account {}", adSetDtos.size(), accountId);
                return adSetDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch ad sets for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch ad sets", e);
            }
        });
    }

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
                        .requestAllFields()
                        .execute();

                List<MetaAdDto> adDtos = new ArrayList<>();

                for (Ad ad : ads) {
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

                    adDtos.add(dto);
                }

                logger.info("Successfully fetched {} ads for account {}", adDtos.size(), accountId);
                return adDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch ads for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch ads", e);
            }
        });
    }

    /**
     * Fetch insights (performance data) for specific account and date range
     */
    public List<MetaInsightsDto> fetchInsights(String accountId, LocalDate startDate, LocalDate endDate)
            throws MetaApiException {

        logger.info("Fetching insights for account: {} from {} to {}", accountId, startDate, endDate);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);

                // Build insights request with time range as string
                String timeRangeJson = String.format(
                        "{\"since\":\"%s\",\"until\":\"%s\"}",
                        startDate.format(DateTimeFormatter.ISO_LOCAL_DATE),
                        endDate.format(DateTimeFormatter.ISO_LOCAL_DATE)
                );

                APINodeList<AdsInsights> insights = account.getInsights()
                        .requestAllFields()
                        .setLevel(AdsInsights.EnumLevel.VALUE_AD)
                        .setParam("time_range", timeRangeJson)
                        .execute();

                List<MetaInsightsDto> insightDtos = new ArrayList<>();

                for (AdsInsights insight : insights) {
                    MetaInsightsDto dto = new MetaInsightsDto();

                    // Basic metrics
                    dto.setAccountId(safeGetString(insight.getFieldAccountId()));
                    dto.setCampaignId(safeGetString(insight.getFieldCampaignId()));
                    dto.setAdsetId(safeGetString(insight.getFieldAdsetId()));
                    dto.setAdId(safeGetString(insight.getFieldAdId()));
                    dto.setSpend(safeGetString(insight.getFieldSpend()));
                    dto.setImpressions(safeGetString(insight.getFieldImpressions()));
                    dto.setClicks(safeGetString(insight.getFieldClicks()));
                    dto.setUniqueClicks(safeGetString(insight.getFieldUniqueClicks()));
                    dto.setReach(safeGetString(insight.getFieldReach()));
                    dto.setFrequency(safeGetString(insight.getFieldFrequency()));
                    dto.setCpc(safeGetString(insight.getFieldCpc()));
                    dto.setCpm(safeGetString(insight.getFieldCpm()));
                    dto.setCpp(safeGetString(insight.getFieldCpp()));
                    dto.setCtr(safeGetString(insight.getFieldCtr()));
                    dto.setUniqueCtr(safeGetString(insight.getFieldUniqueCtr()));
                    dto.setDateStart(safeGetString(insight.getFieldDateStart()));
                    dto.setDateStop(safeGetString(insight.getFieldDateStop()));

                    // Extract action metrics safely
                    extractActionMetrics(insight, dto);

                    insightDtos.add(dto);
                }

                logger.info("Successfully fetched {} insight records for account {}", insightDtos.size(), accountId);
                return insightDtos;

            } catch (Exception e) {
                logger.error("Failed to fetch insights for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch insights", e);
            }
        });
    }

    /**
     * Extract action metrics from insights safely
     */
    private void extractActionMetrics(AdsInsights insight, MetaInsightsDto dto) {
        try {
            List<AdsActionStats> actions = insight.getFieldActions();
            if (actions != null) {
                for (AdsActionStats action : actions) {
                    String actionType = safeGetString(action.getFieldActionType());
                    String value = safeGetString(action.getFieldValue());

                    if (actionType != null && value != null) {
                        switch (actionType) {
                            case "link_click":
                                dto.setLinkClicks(value);
                                break;
                            case "post_engagement":
                                dto.setPostEngagement(value);
                                break;
                            case "page_engagement":
                                dto.setPageEngagement(value);
                                break;
                            case "like":
                                dto.setLikes(value);
                                break;
                            case "comment":
                                dto.setComments(value);
                                break;
                            case "share":
                                dto.setShares(value);
                                break;
                            case "photo_view":
                                dto.setPhotoView(value);
                                break;
                            case "video_view":
                                dto.setVideoViews(value);
                                break;
                            case "purchase":
                                dto.setPurchases(value);
                                break;
                            case "lead":
                                dto.setLeads(value);
                                break;
                            case "mobile_app_install":
                                dto.setMobileAppInstall(value);
                                break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to extract action metrics: {}", e.getMessage());
        }
    }

    /**
     * Fetch insights for yesterday (most common use case)
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
            List<MetaAccountDto> accounts = fetchAccounts();
            return accounts != null && !accounts.isEmpty();
        } catch (Exception e) {
            logger.error("Connectivity test failed: {}", e.getMessage());
            return false;
        }
    }

    // Safe getter utility methods
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

    public static class ConnectorStatus {
        public final MetaApiClient.ClientStatus clientStatus;
        public final boolean isConnected;

        public ConnectorStatus(MetaApiClient.ClientStatus clientStatus, boolean isConnected) {
            this.clientStatus = clientStatus;
            this.isConnected = isConnected;
        }
    }
}