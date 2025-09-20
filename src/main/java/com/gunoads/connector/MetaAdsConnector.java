package com.gunoads.connector;

import com.gunoads.config.MetaAdsConfig;
import com.gunoads.config.MetaApiProperties;
import com.gunoads.exception.MetaApiException;
import com.gunoads.model.dto.*;
import com.gunoads.connector.MetaApiClient;
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
     * Fetch business accounts - UPDATED: Filter only ACTIVE accounts
     */
    public List<MetaAccountDto> fetchBusinessAccounts() throws MetaApiException {
        logger.info("Fetching ACTIVE ad accounts from business: {}", metaAdsConfig.getBusinessId());

        return metaApiClient.executeWithRetry(context -> {
            try {
                Business business = new Business(metaAdsConfig.getBusinessId(), context);

                // UPDATED: Fetch ALL accounts first (no API-level filtering due to SDK compatibility)
                APINodeList<AdAccount> accounts = business
                        .getOwnedAdAccounts()
                        .requestFields(metaApiProperties.getFields().getAccount())
                        .execute()
                        .withAutoPaginationIterator(true);

                List<MetaAccountDto> accountDtos = new ArrayList<>();
                int totalCount = 0;
                int activeCount = 0;

                // Iterator will automatically fetch all pages
                for (AdAccount account : accounts) {
                    MetaAccountDto dto = mapAccountToDto(account);
                    totalCount++;

                    // FIXED: Filter for ACTIVE accounts in Java code (account_status = 1)
                    if (isAccountActive(dto)) {
                        accountDtos.add(dto);
                        activeCount++;
                    }

                    // Log progress every 50 records
                    if (totalCount % 50 == 0) {
                        logger.debug("Processed {} accounts ({} active) from API...", totalCount, activeCount);
                    }
                }

                logger.info("✅ Successfully fetched {} ACTIVE ad accounts from business (filtered from {} total)",
                        activeCount, totalCount);

                // Log filtering summary
                logFilteringSummary("ACCOUNTS", "business", totalCount, activeCount);

                return accountDtos;

            } catch (Exception e) {
                logger.error("❌ Failed to fetch ACTIVE business accounts: {}", e.getMessage());
                throw new MetaApiException("Failed to fetch ACTIVE business ad accounts", e);
            }
        });
    }

// ==================== CAMPAIGN METHODS (UPDATED WITH ACTIVE FILTERING) ====================

    /**
     * Fetch campaigns - UPDATED: Filter only ACTIVE campaigns
     */
    public List<MetaCampaignDto> fetchCampaigns(String accountId) throws MetaApiException {
        logger.info("Fetching ACTIVE campaigns for account: {}", accountId);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);

                // UPDATED: Enable auto-pagination (filter ACTIVE campaigns in code due to SDK compatibility)
                APINodeList<Campaign> campaigns = account
                        .getCampaigns()
                        .requestFields(metaApiProperties.getFields().getCampaign())
                        .execute()
                        .withAutoPaginationIterator(true);

                List<MetaCampaignDto> campaignDtos = new ArrayList<>();
                int totalCount = 0;
                int activeCount = 0;

                // Iterator will automatically fetch all pages
                for (Campaign campaign : campaigns) {
                    MetaCampaignDto dto = mapCampaignToDto(campaign);

                    // Double-check status at DTO level (additional safety)
                    if ("ACTIVE".equals(dto.getStatus())) {
                        campaignDtos.add(dto);
                        activeCount++;
                    }
                    totalCount++;

                    // Log progress every 100 records
                    if (totalCount % 100 == 0) {
                        logger.debug("Processed {} campaigns ({} active) for account {}...",
                                totalCount, activeCount, accountId);
                    }
                }

                logger.info("✅ Successfully fetched {} ACTIVE campaigns for account {} (filtered from {} total)",
                        activeCount, accountId, totalCount);
                return campaignDtos;

            } catch (Exception e) {
                logger.error("❌ Failed to fetch ACTIVE campaigns for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch ACTIVE campaigns", e);
            }
        });
    }

    /**
     * Fetch ad sets - UPDATED: Filter only ACTIVE ad sets
     */
    public List<MetaAdSetDto> fetchAdSets(String accountId) throws MetaApiException {
        logger.info("Fetching ACTIVE ad sets for account: {}", accountId);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);

                // UPDATED: Enable auto-pagination (filter ACTIVE ad sets in code due to SDK compatibility)
                APINodeList<AdSet> adSets = account
                        .getAdSets()
                        .requestFields(metaApiProperties.getFields().getAdset())
                        .execute()
                        .withAutoPaginationIterator(true);

                List<MetaAdSetDto> adSetDtos = new ArrayList<>();
                int totalCount = 0;
                int activeCount = 0;

                // Iterator will automatically fetch all pages
                for (AdSet adSet : adSets) {
                    MetaAdSetDto dto = mapAdSetToDto(adSet);

                    // Double-check status at DTO level (additional safety)
                    if ("ACTIVE".equals(dto.getStatus())) {
                        adSetDtos.add(dto);
                        activeCount++;
                    }
                    totalCount++;

                    // Log progress every 100 records
                    if (totalCount % 100 == 0) {
                        logger.debug("Processed {} ad sets ({} active) for account {}...",
                                totalCount, activeCount, accountId);
                    }
                }

                logger.info("✅ Successfully fetched {} ACTIVE ad sets for account {} (filtered from {} total)",
                        activeCount, accountId, totalCount);
                return adSetDtos;

            } catch (Exception e) {
                logger.error("❌ Failed to fetch ACTIVE ad sets for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch ACTIVE ad sets", e);
            }
        });
    }

    /**
     * Fetch ads - UPDATED: Filter only ACTIVE ads
     */
    public List<MetaAdDto> fetchAds(String accountId) throws MetaApiException {
        logger.info("Fetching ACTIVE ads for account: {}", accountId);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);

                // UPDATED: Enable auto-pagination (filter ACTIVE ads in code due to SDK compatibility)
                APINodeList<Ad> ads = account
                        .getAds()
                        .requestFields(metaApiProperties.getFields().getAd())
                        .execute()
                        .withAutoPaginationIterator(true);

                List<MetaAdDto> adDtos = new ArrayList<>();
                int totalCount = 0;
                int activeCount = 0;

                // Iterator will automatically fetch all pages
                for (Ad ad : ads) {
                    MetaAdDto dto = mapAdToDto(ad);

                    // Double-check status at DTO level (additional safety)
                    if ("ACTIVE".equals(dto.getStatus())) {
                        adDtos.add(dto);
                        activeCount++;
                    }
                    totalCount++;

                    // Log progress every 100 records
                    if (totalCount % 100 == 0) {
                        logger.debug("Processed {} ads ({} active) for account {}...",
                                totalCount, activeCount, accountId);
                    }
                }

                logger.info("✅ Successfully fetched {} ACTIVE ads for account {} (filtered from {} total)",
                        activeCount, accountId, totalCount);
                return adDtos;

            } catch (Exception e) {
                logger.error("❌ Failed to fetch ACTIVE ads for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch ACTIVE ads", e);
            }
        });
    }

    /**
     * Fetch insights - FIXED: Auto-pagination enabled
     */
    public List<MetaInsightsDto> fetchInsights(String accountId, LocalDate startDate, LocalDate endDate) throws MetaApiException {
        logger.info("Fetching insights for account {} from {} to {}", accountId, startDate, endDate);

        return metaApiClient.executeWithRetry(context -> {
            try {
                AdAccount account = new AdAccount(accountId, context);

                // FIXED: Enable auto-pagination + use current format
                AdAccount.APIRequestGetInsights insightsRequest = account.getInsights()
                        .requestFields(metaApiProperties.getFields().getInsights())
                        .setBreakdowns(metaApiProperties.getDefaultBreakdownsString())
                        .setTimeRange("{\"since\":\"" + startDate.format(DateTimeFormatter.ISO_LOCAL_DATE) +
                                "\",\"until\":\"" + endDate.format(DateTimeFormatter.ISO_LOCAL_DATE) + "\"}");

                // FIXED: Enable auto-pagination to get ALL results
                APINodeList<AdsInsights> insights = insightsRequest
                        .execute()
                        .withAutoPaginationIterator(true);

                List<MetaInsightsDto> insightDtos = new ArrayList<>();
                int totalCount = 0;

                // FIXED: Iterator will automatically fetch all pages
                for (AdsInsights insight : insights) {
                    insightDtos.add(mapInsightsToDto(insight));
                    totalCount++;

                    // Log progress every 500 records (insights can be large)
                    if (totalCount % 500 == 0) {
                        logger.debug("Processed {} insights for account {}...", totalCount, accountId);
                    }
                }

                logger.info("✅ Successfully fetched {} insights for account {} from {} to {} (auto-pagination)",
                        insightDtos.size(), accountId, startDate, endDate);
                return insightDtos;

            } catch (Exception e) {
                logger.error("❌ Failed to fetch insights for account {}: {}", accountId, e.getMessage());
                throw new MetaApiException("Failed to fetch insights", e);
            }
        });
    }

    /**
     * NEW DEFAULT: Fetch insights for today (replaces yesterday as default)
     */
    public List<MetaInsightsDto> fetchTodayInsights(String accountId) throws MetaApiException {
        LocalDate today = LocalDate.now();
        return fetchInsights(accountId, today, today);
    }

    /**
     * KEEP EXISTING: Fetch yesterday's insights (legacy support)
     */
    public List<MetaInsightsDto> fetchYesterdayInsights(String accountId) throws MetaApiException {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        return fetchInsights(accountId, yesterday, yesterday);
    }

    // ==================== CONNECTIVITY & STATUS METHODS ====================

    /**
     * Test API connectivity by fetching business accounts
     */
    public boolean testConnectivity() {
        try {
            List<String> accountIds = metaApiClient.executeWithRetry(context -> {
                Business business = new Business(metaAdsConfig.getBusinessId(), context);
                APINodeList<AdAccount> result = business
                        .getOwnedAdAccounts()
                        .requestFields(List.of("id", "name")) // Minimal fields for connectivity test
                        .execute();

                // Convert to simple list for testing (just check if API works)
                List<String> accountLst = new ArrayList<>();
                for (AdAccount account : result) {
                    accountLst.add(account.getFieldId());
                    break; // Only need one to test connectivity
                }
                return accountLst;
            });

            // Consider successful if API call completed (even with empty results)
            return accountIds != null;

        } catch (Exception e) {
            logger.error("Connectivity test failed: {}", e.getMessage());
            return false;
        }
    }

    // ==================== MAPPING METHODS ====================
    // NOTE: These methods remain unchanged - only pagination was fixed above

    /**
     * Map AdAccount to MetaAccountDto
     */
    private MetaAccountDto mapAccountToDto(AdAccount account) {
        MetaAccountDto dto = new MetaAccountDto();
        dto.setId(account.getFieldId());
        dto.setAccountName(account.getFieldName());
        dto.setAccountStatus(account.getFieldAccountStatus() != null ?
                account.getFieldAccountStatus().toString() : null);
        dto.setCurrency(account.getFieldCurrency());
        dto.setBusinessId(account.getId());
        dto.setTimezoneId(account.getFieldTimezoneId() != null ?
                account.getFieldTimezoneId() : null);
        return dto;
    }

    /**
     * Map Campaign to MetaCampaignDto
     */
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
        dto.setCreatedTime(campaign.getFieldCreatedTime());
        dto.setUpdatedTime(campaign.getFieldUpdatedTime());
        return dto;
    }

    /**
     * Map AdSet to MetaAdSetDto
     */
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

    /**
     * Map Ad to MetaAdDto - FIXED: Use correct DTO structure
     */
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

        // FIXED: Creative information using correct DTO structure
        AdCreative creative = ad.getFieldCreative();
        if (creative != null) {
            MetaAdDto.Creative creativeDto = new MetaAdDto.Creative();
            creativeDto.setId(creative.getFieldId());
            creativeDto.setName(creative.getFieldName());
            // FIXED: Set other creative fields that exist in DTO
            dto.setCreative(creativeDto);
        }

        return dto;
    }

    /**
     * Map AdsInsights to MetaInsightsDto - FIXED: Use only existing DTO fields
     */
    private MetaInsightsDto mapInsightsToDto(AdsInsights insight) {
        MetaInsightsDto dto = new MetaInsightsDto();
        dto.setAccountId(insight.getFieldAccountId());
        dto.setCampaignId(insight.getFieldCampaignId());
        dto.setAdsetId(insight.getFieldAdsetId());
        dto.setAdId(insight.getFieldAdId());
        dto.setDateStart(insight.getFieldDateStart());
        dto.setDateStop(insight.getFieldDateStop());

        // Performance metrics
        dto.setImpressions(insight.getFieldImpressions());
        dto.setClicks(insight.getFieldClicks());
        dto.setSpend(insight.getFieldSpend());
        dto.setCtr(insight.getFieldCtr());
        dto.setCpc(insight.getFieldCpc());
        dto.setCpm(insight.getFieldCpm());
        dto.setCpp(insight.getFieldCpp());

        // FIXED: Only use breakdown fields that exist in DTO
        dto.setAge(insight.getFieldAge());
        dto.setGender(insight.getFieldGender());
        dto.setCountry(insight.getFieldCountry());
        dto.setRegion(insight.getFieldRegion());
        dto.setCity(insight.getFieldRegion()); // Use region as city fallback
        dto.setPlacement(insight.getFieldPublisherPlatform()); // Use publisher_platform as placement
        dto.setDevice_platform(insight.getFieldImpressionDevice()); // Use impression_device

        // Additional fields that exist in DTO
        dto.setUniqueClicks(insight.getFieldUniqueClicks());
        dto.setReach(insight.getFieldReach());
        dto.setFrequency(insight.getFieldFrequency());

        return dto;
    }

    /**
     * Check if account is active (account_status = 1)
     */
    private boolean isAccountActive(MetaAccountDto account) {
        // Meta API returns account_status as Integer: 1 = ACTIVE, 101 = RESTRICTED, etc.
        String status = account.getAccountStatus();
        if (status == null) return false;

        try {
            int statusCode = Integer.parseInt(status);
            boolean isActive = (statusCode == 1); // Only status 1 is considered ACTIVE

            if (!isActive) {
                logger.debug("Account {} filtered out: status={} ({})",
                        account.getId(), statusCode, getAccountStatusName(statusCode));
            }

            return isActive;
        } catch (NumberFormatException e) {
            logger.warn("Invalid account status format for account {}: {}", account.getId(), status);
            return false;
        }
    }

    /**
     * Get human-readable account status name
     */
    private String getAccountStatusName(int statusCode) {
        switch (statusCode) {
            case 1: return "ACTIVE";
            case 2: return "DISABLED";
            case 3: return "UNSETTLED";
            case 7: return "PENDING_RISK_REVIEW";
            case 8: return "PENDING_SETTLEMENT";
            case 9: return "IN_GRACE_PERIOD";
            case 100: return "PENDING_CLOSURE";
            case 101: return "CLOSED/RESTRICTED";
            case 201: return "ANY_ACTIVE";
            case 202: return "ANY_CLOSED";
            default: return "UNKNOWN_" + statusCode;
        }
    }

// ==================== LOGGING HELPER METHODS ====================

    /**
     * Log filtering summary for debugging
     */
    private void logFilteringSummary(String entityType, String accountId, int total, int active) {
        double activePercentage = total > 0 ? (active * 100.0 / total) : 0;
        logger.info("📊 FILTERING SUMMARY for {} in account {}: {} active out of {} total ({:.1f}%)",
                entityType, accountId, active, total, activePercentage);
    }
}