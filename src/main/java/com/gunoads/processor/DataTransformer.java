package com.gunoads.processor;

import com.gunoads.model.dto.*;
import com.gunoads.model.entity.*;
import com.gunoads.model.enums.PlatformType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class DataTransformer {

    private static final Logger logger = LoggerFactory.getLogger(DataTransformer.class);

    private static final String META_PLATFORM_ID = PlatformType.META.getCode();
    private static final String DEFAULT_TIMESTAMP = LocalDateTime.now().toString();

    /**
     * Transform Meta Account DTO to Account Entity
     */
    public Account transformAccount(MetaAccountDto dto) {
        if (dto == null) {
            logger.warn("Cannot transform null MetaAccountDto");
            return null;
        }

        try {
            Account account = new Account();

            // Required fields
            account.setId(safeGetString(dto.getId()));
            account.setPlatformId(META_PLATFORM_ID);

            // Basic info
            account.setAccountName(safeGetString(dto.getAccountName()));
            account.setCurrency(safeGetString(dto.getCurrency()));
            account.setTimezoneId(safeGetLong(dto.getTimezoneId(), 0L));
            account.setTimezoneName(safeGetString(dto.getTimezoneName()));
            account.setAccountStatus(safeGetString(dto.getAccountStatus()));
            account.setDisableReason(safeGetString(dto.getDisableReason()));

            // Business info - Fixed with correct types
            account.setBussinessId(safeGetString(dto.getBusinessId()));
            account.setBusinessName(safeGetString(dto.getBusinessName()));
            account.setBusinessCountryCode(safeGetInteger(dto.getBusinessCountryCode(), 0)); // Now Integer
            account.setBusinessCity(safeGetString(dto.getBusinessCity()));
            account.setBusinessState(safeGetString(dto.getBusinessState()));
            account.setBusinessStreet(safeGetString(dto.getBusinessStreet()));
            account.setBusinessZip(safeGetInteger(dto.getBusinessZip(), 0)); // Now Integer

            // Financial info
            account.setAmountSpent(parseBigDecimal(dto.getAmountSpent()));
            account.setBalance(parseBigDecimal(dto.getBalance()));
            account.setSpendCap(parseBigDecimal(dto.getSpendCap()));
            account.setFundingSource(safeGetString(dto.getFundingSource()));

            // Boolean fields with defaults
            account.setIsPersonal(safeGetBoolean(dto.getIsPersonal(), false));
            account.setIsPrepayAccount(safeGetBoolean(dto.getIsPrepayAccount(), false));
            account.setIsTaxIdRequired(safeGetBoolean(dto.getIsTaxIdRequired(), false));
            account.setHasPageAuthorizedAdAccount(safeGetBoolean(dto.getHasPageAuthorizedAdAccount(), false));
            account.setIsDirectDealsEnabled(safeGetBoolean(dto.getIsDirectDealsEnabled(), false));
            account.setIsNotificationsEnabled(safeGetBoolean(dto.getIsNotificationsEnabled(), false));

            // Timestamps
            account.setCreatedTime(safeGetString(dto.getCreatedTime()));
            account.setLastUpdated(DEFAULT_TIMESTAMP);

            // Required fields with defaults - Fixed
            account.setAccountAge(safeGetInteger(dto.getAccountAge(), 0));
            account.setCapabilities(safeGetString(dto.getCapabilities()));

            logger.debug("Successfully transformed Account: {}", account.getId());
            return account;

        } catch (Exception e) {
            logger.error("Failed to transform MetaAccountDto: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Transform Meta Campaign DTO to Campaign Entity
     */
    public Campaign transformCampaign(MetaCampaignDto dto) {
        if (dto == null) {
            logger.warn("Cannot transform null MetaCampaignDto");
            return null;
        }

        try {
            Campaign campaign = new Campaign();

            // Required fields
            campaign.setId(safeGetString(dto.getId()));
            campaign.setAccountId(safeGetString(dto.getAccountId()));
            campaign.setPlatformId(META_PLATFORM_ID);

            // Basic info
            campaign.setCampaignName(safeGetString(dto.getName()));
            campaign.setCamObjective(safeGetString(dto.getObjective()));
            campaign.setCamStatus(safeGetString(dto.getStatus()));
            campaign.setConfiguredStatus(safeGetString(dto.getConfiguredStatus()));
            campaign.setEffectiveStatus(safeGetString(dto.getEffectiveStatus()));

            // Timestamps
            campaign.setStartTime(safeGetString(dto.getStartTime()));
            campaign.setStopTime(safeGetString(dto.getStopTime()));
            campaign.setCreatedTime(safeGetString(dto.getCreatedTime()));
            campaign.setUpdatedTime(safeGetString(dto.getUpdatedTime()));
            campaign.setLastUpdated(DEFAULT_TIMESTAMP);

            // Budget and bidding
            campaign.setBuyingType(safeGetString(dto.getBuyingType()));
            campaign.setDailyBudget(parseBigDecimal(dto.getDailyBudget()));
            campaign.setLifetimeBudget(parseBigDecimal(dto.getLifetimeBudget()));
            campaign.setBudgetRemaining(parseBigDecimal(dto.getBudgetRemaining()));
            campaign.setBidStrategy(safeGetString(dto.getBidStrategy()));
            campaign.setSpendCap(parseBigDecimal(dto.getSpendCap()));

            // Additional fields - Fixed mapping
            campaign.setCamOptimizationType(safeGetString(dto.getOptimizationGoal()));
            campaign.setSpecialAdCategories(safeGetString(dto.getSpecialAdCategories()));
            campaign.setAttributionSpec(safeGetString(dto.getAttributionSpec()));

            logger.debug("Successfully transformed Campaign: {}", campaign.getId());
            return campaign;

        } catch (Exception e) {
            logger.error("Failed to transform MetaCampaignDto: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Transform Meta AdSet DTO to AdSet Entity
     */
    public AdSet transformAdSet(MetaAdSetDto dto) {
        if (dto == null) {
            logger.warn("Cannot transform null MetaAdSetDto");
            return null;
        }

        try {
            AdSet adSet = new AdSet();

            // Required fields
            adSet.setId(safeGetString(dto.getId()));
            adSet.setCampaignId(safeGetString(dto.getCampaignId()));

            // Basic info
            adSet.setAdSetName(safeGetString(dto.getName()));
            adSet.setAdSetStatus(safeGetString(dto.getStatus()));
            adSet.setConfiguredStatus(safeGetString(dto.getConfiguredStatus()));
            adSet.setEffectiveStatus(safeGetString(dto.getEffectiveStatus()));

            // Timestamps
            adSet.setStartTime(safeGetString(dto.getStartTime()));
            adSet.setEndTime(safeGetString(dto.getEndTime()));
            adSet.setCreatedTime(safeGetString(dto.getCreatedTime()));
            adSet.setUpdatedTime(safeGetString(dto.getUpdatedTime()));
            adSet.setLastUpdated(DEFAULT_TIMESTAMP);

            // Budget and bidding
            adSet.setDailyBudget(parseBigDecimal(dto.getDailyBudget()));
            adSet.setLifetimeBudget(parseBigDecimal(dto.getLifetimeBudget()));
            adSet.setBudgetRemaining(parseBigDecimal(dto.getBudgetRemaining()));
            adSet.setBidAmount(parseBigDecimal(safeGetLong(dto.getBidAmount(), 0L).toString()));
            adSet.setBidStrategy(safeGetString(dto.getBidStrategy()));

            // Optimization
            adSet.setOptimizationGoal(safeGetString(dto.getOptimizationGoal()));
            adSet.setBillingEvent(safeGetString(dto.getBillingEvent()));

            // Targeting
            adSet.setTargetingSpec(safeGetString(dto.getTargeting()));
            adSet.setGeoLocations(safeGetString(dto.getGeoLocations()));
            adSet.setAgeMin(safeGetInteger(dto.getAgeMin(), 18));
            adSet.setAgeMax(safeGetInteger(dto.getAgeMax(), 65));
            adSet.setGenders(safeGetString(dto.getGenders()));
            adSet.setInterests(safeGetString(dto.getInterests()));
            adSet.setBehaviors(safeGetString(dto.getBehaviors()));
            adSet.setCustomAudiences(safeGetString(dto.getCustomAudiences()));
            adSet.setLookalikAudiences(safeGetString(dto.getLookalikAudiences()));
            adSet.setExcludedCustomAudiences(safeGetString(dto.getExcludedCustomAudiences()));
            adSet.setAdsetSchedule(safeGetString(dto.getAdsetSchedule()));

            // Fixed: Now using correct DTO fields
            adSet.setLifetimeImps(safeGetLong(dto.getLifetimeImps(), 0L));
            adSet.setIsAutobid(safeGetBoolean(dto.getIsAutobid(), true));
            adSet.setAttributionSpec(safeGetString(dto.getAttributionSpec()));

            logger.debug("Successfully transformed AdSet: {}", adSet.getId());
            return adSet;

        } catch (Exception e) {
            logger.error("Failed to transform MetaAdSetDto: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Transform Meta Ad DTO to Advertisement Entity
     */
    public Advertisement transformAdvertisement(MetaAdDto dto) {
        if (dto == null) {
            logger.warn("Cannot transform null MetaAdDto");
            return null;
        }

        try {
            Advertisement ad = new Advertisement();

            // Required fields
            ad.setId(safeGetString(dto.getId()));
            ad.setAdsetid(safeGetString(dto.getAdsetId()));

            // Basic info
            ad.setAdName(safeGetString(dto.getName()));
            ad.setAdStatus(safeGetString(dto.getStatus()));
            ad.setConfiguredStatus(safeGetString(dto.getConfiguredStatus()));
            ad.setEffectiveStatus(safeGetString(dto.getEffectiveStatus()));

            // Timestamps
            ad.setCreatedTime(safeGetString(dto.getCreatedTime()));
            ad.setUpdatedTime(safeGetString(dto.getUpdatedTime()));
            ad.setLastUpdated(DEFAULT_TIMESTAMP);

            // Creative info
            if (dto.getCreative() != null) {
                MetaAdDto.Creative creative = dto.getCreative();
                ad.setCreativeId(safeGetString(creative.getId()));
                ad.setCreativeName(safeGetString(creative.getName()));
                ad.setCreativeType(safeGetString(creative.getType()));
                ad.setCallToActionType(safeGetString(creative.getCallToActionType()));

                // Creative boolean flags with defaults
                ad.setIsAppInstallAd(safeGetBoolean(creative.getIsAppInstallAd(), false));
                ad.setIsDynamicAd(safeGetBoolean(creative.getIsDynamicAd(), false));
                ad.setIsVideoAd(safeGetBoolean(creative.getIsVideoAd(), false));
                ad.setIsCarouselAd(safeGetBoolean(creative.getIsCarouselAd(), false));
            } else {
                // Default values for required boolean fields
                ad.setIsAppInstallAd(false);
                ad.setIsDynamicAd(false);
                ad.setIsVideoAd(false);
                ad.setIsCarouselAd(false);
            }

            logger.debug("Successfully transformed Advertisement: {}", ad.getId());
            return ad;

        } catch (Exception e) {
            logger.error("Failed to transform MetaAdDto: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Transform Meta Insights DTO to AdsReporting Entity - COMPLETELY UPDATED
     */
    public AdsReporting transformInsights(MetaInsightsDto dto) {
        if (dto == null) {
            logger.warn("Cannot transform null MetaInsightsDto");
            return null;
        }

        try {
            AdsReporting reporting = new AdsReporting();

            // Required IDs
            reporting.setAccountId(safeGetString(dto.getAccountId()));
            reporting.setPlatformId(META_PLATFORM_ID);
            reporting.setCampaignId(safeGetString(dto.getCampaignId()));
            reporting.setAdsetId(safeGetString(dto.getAdsetId()));
            reporting.setAdvertisementId(safeGetString(dto.getAdId()));

            // CRITICAL: Extract placement ID from breakdown data
            String placementId = MetaPlacementDto.extractPlacementId(
                    dto.getPlacement(),
                    dto.getDevice_platform()
            );
            reporting.setPlacementId(placementId);

            // Date and demographics - NOW USING CORRECT BREAKDOWN FIELDS
            reporting.setAdsProcessingDt(safeGetString(dto.getDateStart()));
            reporting.setAgeGroup(safeGetString(dto.getAge(), "unknown"));
            reporting.setGender(safeGetString(dto.getGender(), "unknown"));
            reporting.setCountryCode(parseCountryCode(dto.getCountry()));
            reporting.setRegion(safeGetString(dto.getRegion(), "unknown"));
            reporting.setCity(safeGetString(dto.getCity(), "unknown"));
            reporting.setCountryName(safeGetString(dto.getCountry()));

            // Core metrics
            reporting.setSpend(parseDouble(dto.getSpend(), 0.0));
            reporting.setRevenue(parseDouble(dto.getPurchaseValue(), 0.0));
            reporting.setPurchaseRoas(parseBigDecimal(dto.getPurchaseRoas()));
            reporting.setImpressions(parseLong(dto.getImpressions(), 0L));
            reporting.setClicks(parseLong(dto.getClicks(), 0L));
            reporting.setUniqueClicks(parseLong(dto.getUniqueClicks(), 0L));
            reporting.setLinkClicks(parseLong(dto.getLinkClicks(), 0L));
            reporting.setUniqueLinkClicks(parseLong(dto.getUniqueLinkClicks(), 0L));
            reporting.setReach(parseLong(dto.getReach(), 0L));

            // Cost metrics - NOW USING CORRECT FIELDS
            reporting.setCostPerUniqueClick(parseBigDecimal(dto.getCostPerUniqueClick()));
            reporting.setFrequency(parseBigDecimal(dto.getFrequency()));
            reporting.setCpc(parseBigDecimal(dto.getCpc()));
            reporting.setCpm(parseBigDecimal(dto.getCpm()));
            reporting.setCpp(parseBigDecimal(dto.getCpp()));
            reporting.setCtr(parseBigDecimal(dto.getCtr()));
            reporting.setUniqueCtr(parseBigDecimal(dto.getUniqueCtr()));

            // Engagement metrics
            reporting.setPostEngagement(parseLong(dto.getPostEngagement(), 0L));
            reporting.setPageEngagement(parseLong(dto.getPageEngagement(), 0L));
            reporting.setLikes(parseLong(dto.getLikes(), 0L));
            reporting.setComments(parseLong(dto.getComments(), 0L));
            reporting.setShares(parseLong(dto.getShares(), 0L));
            reporting.setPhotoView(parseLong(dto.getPhotoView(), 0L));

            // Video metrics - NOW USING ALL FIELDS
            reporting.setVideoViews(parseLong(dto.getVideoViews(), 0L));
            reporting.setVideoP25WatchedActions(parseLong(dto.getVideoP25WatchedActions(), 0L));
            reporting.setVideoP50WatchedActions(parseLong(dto.getVideoP50WatchedActions(), 0L));
            reporting.setVideoP75WatchedActions(parseLong(dto.getVideoP75WatchedActions(), 0L));
            reporting.setVideoP95WatchedActions(parseLong(dto.getVideoP95WatchedActions(), 0L));
            reporting.setVideoP100WatchedActions(parseLong(dto.getVideoP100WatchedActions(), 0L));
            reporting.setVideoAvgPercentWatched(parseBigDecimal(dto.getVideoAvgPercentWatched()));

            // Conversion metrics - USING CORRECT COST FIELDS
            reporting.setPurchases(parseLong(dto.getPurchases(), 0L));
            reporting.setPurchaseValue(parseBigDecimal(dto.getPurchaseValue()));
            reporting.setLeads(parseLong(dto.getLeads(), 0L));
            reporting.setCostPerLead(parseBigDecimal(dto.getCostPerLead()));
            reporting.setMobileAppInstall(parseLong(dto.getMobileAppInstall(), 0L));
            reporting.setCostPerAppInstall(parseBigDecimal(dto.getCostPerAppInstall()));

            // Additional metrics
            reporting.setSocialSpend(parseBigDecimal(dto.getSocialSpend()));
            reporting.setInlineLinkClicks(parseLong(dto.getInlineLinkClicks(), 0L));
            reporting.setInlinePostEngagement(parseLong(dto.getInlinePostEngagement(), 0L));
            reporting.setCostPerInlineLinkClick(parseBigDecimal(dto.getCostPerInlineLinkClick()));
            reporting.setCostPerInlinePostEngagement(parseBigDecimal(dto.getCostPerInlinePostEngagement()));

            // Meta info
            reporting.setCurrency(safeGetString(dto.getCurrency(), "USD"));
            reporting.setAttributionSetting(safeGetString(dto.getAttributionSetting()));
            reporting.setDateStart(safeGetString(dto.getDateStart()));
            reporting.setDateStop(safeGetString(dto.getDateStop()));
            reporting.setCreatedAt(DEFAULT_TIMESTAMP);
            reporting.setUpdatedAt(DEFAULT_TIMESTAMP);

            logger.debug("Successfully transformed AdsReporting for ad: {}", reporting.getAdvertisementId());
            return reporting;

        } catch (Exception e) {
            logger.error("Failed to transform MetaInsightsDto: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Transform Meta Placement DTO to Placement Entity - NEW METHOD
     */
    public Placement transformPlacement(MetaPlacementDto dto) {
        if (dto == null) {
            logger.warn("Cannot transform null MetaPlacementDto");
            return null;
        }

        try {
            Placement placement = new Placement();

            placement.setId(safeGetString(dto.getId()));
            placement.setAdvertisementId(safeGetString(dto.getAdvertisementId()));
            placement.setPlacementName(safeGetString(dto.getPlacementName()));
            placement.setPlatform(safeGetString(dto.getPlatform()));
            placement.setPlacementType(safeGetString(dto.getPlacementType()));
            placement.setDeviceType(safeGetString(dto.getDeviceType()));
            placement.setPosition(safeGetString(dto.getPosition()));
            placement.setIsActive(safeGetBoolean(dto.getIsActive(), true));
            placement.setSupportsVideo(safeGetBoolean(dto.getSupportsVideo(), false));
            placement.setSupportsCarousel(safeGetBoolean(dto.getSupportsCarousel(), false));
            placement.setSupportsCollection(safeGetBoolean(dto.getSupportsCollection(), false));
            placement.setCreatedAt(safeGetString(dto.getCreatedAt(), DEFAULT_TIMESTAMP));

            logger.debug("Successfully transformed Placement: {}", placement.getId());
            return placement;

        } catch (Exception e) {
            logger.error("Failed to transform MetaPlacementDto: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Batch transform methods - UPDATED
     */
    public List<Account> transformAccounts(List<MetaAccountDto> dtos) {
        if (dtos == null) return new ArrayList<>();
        return dtos.stream()
                .map(this::transformAccount)
                .filter(account -> account != null)
                .collect(Collectors.toList());
    }

    public List<Campaign> transformCampaigns(List<MetaCampaignDto> dtos) {
        if (dtos == null) return new ArrayList<>();
        return dtos.stream()
                .map(this::transformCampaign)
                .filter(campaign -> campaign != null)
                .collect(Collectors.toList());
    }

    public List<AdSet> transformAdSets(List<MetaAdSetDto> dtos) {
        if (dtos == null) return new ArrayList<>();
        return dtos.stream()
                .map(this::transformAdSet)
                .filter(adSet -> adSet != null)
                .collect(Collectors.toList());
    }

    public List<Advertisement> transformAdvertisements(List<MetaAdDto> dtos) {
        if (dtos == null) return new ArrayList<>();
        return dtos.stream()
                .map(this::transformAdvertisement)
                .filter(ad -> ad != null)
                .collect(Collectors.toList());
    }

    public List<AdsReporting> transformInsightsList(List<MetaInsightsDto> dtos) {
        if (dtos == null) return new ArrayList<>();
        return dtos.stream()
                .map(this::transformInsights)
                .filter(reporting -> reporting != null)
                .collect(Collectors.toList());
    }

    public List<Placement> transformPlacements(List<MetaPlacementDto> dtos) {
        if (dtos == null) return new ArrayList<>();
        return dtos.stream()
                .map(this::transformPlacement)
                .filter(placement -> placement != null)
                .collect(Collectors.toList());
    }

    // Safe conversion utility methods - UNCHANGED
    private String safeGetString(String value) {
        return value != null && !value.trim().isEmpty() && value.length() < 255 ? value.trim() : "";
    }

    private String safeGetString(String value, String defaultValue) {
        return value != null && !value.trim().isEmpty() ? value.trim() : defaultValue;
    }

    private Integer safeGetInteger(Integer value, Integer defaultValue) {
        return value != null ? value : defaultValue;
    }

    private Long safeGetLong(Long value, Long defaultValue) {
        return value != null ? value : defaultValue;
    }

    private Boolean safeGetBoolean(Boolean value, Boolean defaultValue) {
        return value != null ? value : defaultValue;
    }

    private BigDecimal parseBigDecimal(String value) {
        if (value == null || value.trim().isEmpty()) return new BigDecimal("0");
        try {
            return new BigDecimal(value.trim());
        } catch (Exception e) {
            logger.warn("Failed to parse BigDecimal: {}", value);
            return new BigDecimal("0");
        }
    }

    private Double parseDouble(String value, Double defaultValue) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception e) {
            logger.warn("Failed to parse Double: {}", value);
            return defaultValue;
        }
    }

    private Long parseLong(String value, Long defaultValue) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        try {
            return Long.parseLong(value.trim());
        } catch (Exception e) {
            logger.warn("Failed to parse Long: {}", value);
            return defaultValue;
        }
    }

    private Integer parseInteger(String value, Integer defaultValue) {
        if (value == null || value.trim().isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception e) {
            logger.warn("Failed to parse Integer: {}", value);
            return defaultValue;
        }
    }

    private Integer parseCountryCode(String country) {
        if (country == null) return 0;

        switch (country.toUpperCase().trim()) {
            case "US":
            case "USA":
            case "UNITED STATES":
                return 1;
            case "VN":
            case "VIETNAM":
                return 84;
            case "UK":
            case "GB":
            case "UNITED KINGDOM":
                return 44;
            case "CA":
            case "CANADA":
                return 124;
            case "AU":
            case "AUSTRALIA":
                return 61;
            default:
                return 0;
        }
    }
}