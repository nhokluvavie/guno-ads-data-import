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
            logger.warn("Cannot transform null MetaInsightsDto - creating placeholder");
            return createPlaceholderReporting();
        }

        try {
            AdsReporting reporting = new AdsReporting();

            // Required IDs - RELAXED: Always provide values, never null
            reporting.setAccountId(safeGetStringRelaxed(dto.getAccountId(), "unknown_account"));
            reporting.setPlatformId(META_PLATFORM_ID);
            reporting.setCampaignId(safeGetStringRelaxed(dto.getCampaignId(), "unknown_campaign"));
            reporting.setAdsetId(safeGetStringRelaxed(dto.getAdsetId(), "unknown_adset"));
            reporting.setAdvertisementId(safeGetStringRelaxed(dto.getAdId(), "unknown_ad"));

            // CRITICAL: Extract placement ID from breakdown data
            String placementId = MetaPlacementDto.extractPlacementId(
                    dto.getPlacement(),
                    dto.getDevice_platform()
            );
            reporting.setPlacementId(placementId != null ? placementId : "unknown");

            // Date and demographics - RELAXED: Always provide values
            reporting.setAdsProcessingDt(safeGetStringRelaxed(dto.getDateStart(), "2025-01-01"));
            reporting.setAgeGroup(safeGetStringRelaxed(dto.getAge(), "unknown"));
            reporting.setGender(safeGetStringRelaxed(dto.getGender(), "unknown"));
            reporting.setCountryCode(parseCountryCodeRelaxed(dto.getCountry()));
            reporting.setRegion(safeGetStringRelaxed(dto.getRegion(), "unknown"));
            reporting.setCity(safeGetStringRelaxed(dto.getCity(), "unknown"));
            reporting.setCountryName(safeGetStringRelaxed(dto.getCountry(), "unknown"));

            // Core metrics - RELAXED: Accept all values, even zeros
            reporting.setSpend(parseDoubleRelaxed(dto.getSpend()));
            reporting.setRevenue(parseDoubleRelaxed(dto.getPurchaseValue()));
            reporting.setPurchaseRoas(parseBigDecimalRelaxed(dto.getPurchaseRoas()));
            reporting.setImpressions(parseLongRelaxed(dto.getImpressions()));
            reporting.setClicks(parseLongRelaxed(dto.getClicks()));
            reporting.setUniqueClicks(parseLongRelaxed(dto.getUniqueClicks()));
            reporting.setLinkClicks(parseLongRelaxed(dto.getLinkClicks()));
            reporting.setUniqueLinkClicks(parseLongRelaxed(dto.getUniqueLinkClicks()));
            reporting.setReach(parseLongRelaxed(dto.getReach()));

            // Cost metrics - RELAXED
            reporting.setCostPerUniqueClick(parseBigDecimalRelaxed(dto.getCostPerUniqueClick()));
            reporting.setFrequency(parseBigDecimalRelaxed(dto.getFrequency()));
            reporting.setCpc(parseBigDecimalRelaxed(dto.getCpc()));
            reporting.setCpm(parseBigDecimalRelaxed(dto.getCpm()));
            reporting.setCpp(parseBigDecimalRelaxed(dto.getCpp()));
            reporting.setCtr(parseBigDecimalRelaxed(dto.getCtr()));
            reporting.setUniqueCtr(parseBigDecimalRelaxed(dto.getUniqueCtr()));

            // Engagement metrics - RELAXED
            reporting.setPostEngagement(parseLongRelaxed(dto.getPostEngagement()));
            reporting.setPageEngagement(parseLongRelaxed(dto.getPageEngagement()));
            reporting.setLikes(parseLongRelaxed(dto.getLikes()));
            reporting.setComments(parseLongRelaxed(dto.getComments()));
            reporting.setShares(parseLongRelaxed(dto.getShares()));
            reporting.setPhotoView(parseLongRelaxed(dto.getPhotoView()));
            reporting.setVideoViews(parseLongRelaxed(dto.getVideoViews()));

            // Video metrics - RELAXED
            reporting.setVideoP25WatchedActions(parseLongRelaxed(dto.getVideoP25WatchedActions()));
            reporting.setVideoP50WatchedActions(parseLongRelaxed(dto.getVideoP50WatchedActions()));
            reporting.setVideoP75WatchedActions(parseLongRelaxed(dto.getVideoP75WatchedActions()));
            reporting.setVideoP95WatchedActions(parseLongRelaxed(dto.getVideoP95WatchedActions()));
            reporting.setVideoP100WatchedActions(parseLongRelaxed(dto.getVideoP100WatchedActions()));
            reporting.setVideoAvgPercentWatched(parseBigDecimalRelaxed(dto.getVideoAvgPercentWatched()));

            // Conversion metrics - RELAXED
            reporting.setPurchases(parseLongRelaxed(dto.getPurchases()));
            reporting.setPurchaseValue(parseBigDecimalRelaxed(dto.getPurchaseValue()));
            reporting.setLeads(parseLongRelaxed(dto.getLeads()));
            reporting.setCostPerLead(parseBigDecimalRelaxed(dto.getCostPerLead()));
            reporting.setMobileAppInstall(parseLongRelaxed(dto.getMobileAppInstall()));
            reporting.setCostPerAppInstall(parseBigDecimalRelaxed(dto.getCostPerAppInstall()));

            // Additional metrics - RELAXED
            reporting.setSocialSpend(parseBigDecimalRelaxed(dto.getSocialSpend()));
            reporting.setInlineLinkClicks(parseLongRelaxed(dto.getInlineLinkClicks()));
            reporting.setInlinePostEngagement(parseLongRelaxed(dto.getInlinePostEngagement()));
            reporting.setCostPerInlineLinkClick(parseBigDecimalRelaxed(dto.getCostPerInlineLinkClick()));
            reporting.setCostPerInlinePostEngagement(parseBigDecimalRelaxed(dto.getCostPerInlinePostEngagement()));

            // Meta info - RELAXED
            reporting.setCurrency(safeGetStringRelaxed(dto.getCurrency(), "USD"));
            reporting.setAttributionSetting(safeGetStringRelaxed(dto.getAttributionSetting(), "unknown"));
            reporting.setDateStart(safeGetStringRelaxed(dto.getDateStart(), "2025-01-01"));
            reporting.setDateStop(safeGetStringRelaxed(dto.getDateStop(), "2025-01-01"));
            reporting.setCreatedAt(DEFAULT_TIMESTAMP);
            reporting.setUpdatedAt(DEFAULT_TIMESTAMP);

            logger.debug("Successfully transformed AdsReporting for ad: {}", reporting.getAdvertisementId());
            return reporting;

        } catch (Exception e) {
            logger.warn("Failed to transform MetaInsightsDto, creating recovery entity: {}", e.getMessage());
            return createRecoveryReporting(dto, e);
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

        logger.info("🔄 Starting batch transformation: {} insights", dtos.size());
        long startTime = System.currentTimeMillis();

        List<AdsReporting> results = new ArrayList<>();
        int successCount = 0;
        int failCount = 0;
        List<String> failureReasons = new ArrayList<>();

        for (int i = 0; i < dtos.size(); i++) {
            MetaInsightsDto dto = dtos.get(i);
            try {
                AdsReporting reporting = transformInsights(dto);
                if (reporting != null) {
                    results.add(reporting);
                    successCount++;
                } else {
                    failCount++;
                    String reason = String.format("Record %d: Null result (adId=%s, accountId=%s)",
                            i, dto.getAdId(), dto.getAccountId());
                    failureReasons.add(reason);

                    if (failCount <= 5) { // Log first 5 failures in detail
                        logger.warn("❌ Transformation failed: {}", reason);
                        logger.debug("   Failed DTO: adId={}, campaignId={}, placement={}, device={}",
                                dto.getAdId(), dto.getCampaignId(), dto.getPlacement(), dto.getDevice_platform());
                    }
                }
            } catch (Exception e) {
                failCount++;
                String reason = String.format("Record %d: Exception - %s", i, e.getMessage());
                failureReasons.add(reason);

                if (failCount <= 5) {
                    logger.error("❌ Transformation exception: {}", reason, e);
                }
            }

            // Progress logging for large batches
            if ((i + 1) % 100 == 0 || i == dtos.size() - 1) {
                logger.debug("📊 Progress: {}/{} processed ({} success, {} failed)",
                        i + 1, dtos.size(), successCount, failCount);
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        // Summary logging
        logger.info("✅ Batch transformation completed in {}ms:", duration);
        logger.info("   📥 Input: {} insights", dtos.size());
        logger.info("   📤 Output: {} reporting entities", results.size());
        logger.info("   ✅ Success: {} ({:.1f}%)", successCount, (successCount * 100.0 / dtos.size()));
        logger.info("   ❌ Failed: {} ({:.1f}%)", failCount, (failCount * 100.0 / dtos.size()));

        if (failCount > 0) {
            logger.warn("⚠️  {} transformation failures detected", failCount);

            // Log common failure patterns
            if (failCount > 5) {
                logger.warn("   First 5 failures logged above, {} more failures suppressed", failCount - 5);
            }

            // Analyze failure patterns
            analyzeFailurePatterns(failureReasons, dtos, failCount);
        }

        return results;
    }

    private AdsReporting createPlaceholderReporting() {
        AdsReporting reporting = new AdsReporting();

        // Required IDs
        reporting.setAccountId("placeholder_account");
        reporting.setPlatformId(META_PLATFORM_ID);
        reporting.setCampaignId("placeholder_campaign");
        reporting.setAdsetId("placeholder_adset");
        reporting.setAdvertisementId("placeholder_ad");
        reporting.setPlacementId("unknown");

        // Required fields
        reporting.setAdsProcessingDt("2025-01-01");
        reporting.setAgeGroup("unknown");
        reporting.setGender("unknown");
        reporting.setCountryCode(0);
        reporting.setRegion("unknown");
        reporting.setCity("unknown");

        // Zero metrics
        setZeroMetrics(reporting);

        reporting.setCurrency("USD");
        reporting.setDateStart("2025-01-01");
        reporting.setDateStop("2025-01-01");
        reporting.setCreatedAt(DEFAULT_TIMESTAMP);
        reporting.setUpdatedAt(DEFAULT_TIMESTAMP);

        return reporting;
    }

    private AdsReporting createRecoveryReporting(MetaInsightsDto dto, Exception error) {
        AdsReporting reporting = new AdsReporting();

        try {
            // Try to extract what we can
            reporting.setAccountId(dto.getAccountId() != null ? dto.getAccountId() : "recovery_account");
            reporting.setPlatformId(META_PLATFORM_ID);
            reporting.setCampaignId(dto.getCampaignId() != null ? dto.getCampaignId() : "recovery_campaign");
            reporting.setAdsetId(dto.getAdsetId() != null ? dto.getAdsetId() : "recovery_adset");
            reporting.setAdvertisementId(dto.getAdId() != null ? dto.getAdId() : "recovery_ad");
            reporting.setPlacementId("unknown");

            // Use safe values for required fields
            reporting.setAdsProcessingDt(dto.getDateStart() != null ? dto.getDateStart() : "2025-01-01");
            reporting.setAgeGroup("recovery");
            reporting.setGender("recovery");
            reporting.setCountryCode(-1); // Special code for recovery records
            reporting.setRegion("recovery");
            reporting.setCity("recovery");

            // Zero metrics
            setZeroMetrics(reporting);

            reporting.setCurrency("USD");
            reporting.setDateStart(dto.getDateStart() != null ? dto.getDateStart() : "2025-01-01");
            reporting.setDateStop(dto.getDateStop() != null ? dto.getDateStop() : "2025-01-01");
            reporting.setCreatedAt(DEFAULT_TIMESTAMP);
            reporting.setUpdatedAt(DEFAULT_TIMESTAMP);

            logger.info("Created recovery reporting entity for failed transformation");
            return reporting;

        } catch (Exception e2) {
            logger.error("Failed to create recovery entity, using placeholder");
            return createPlaceholderReporting();
        }
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

    private String safeGetStringRelaxed(String value, String defaultValue) {
        if (value == null) return defaultValue;
        String trimmed = value.trim();
        if (trimmed.isEmpty()) return defaultValue;
        // Remove length restriction for relaxed mode
        return trimmed.length() > 255 ? trimmed.substring(0, 255) : trimmed;
    }

    private Double parseDoubleRelaxed(String value) {
        if (value == null || value.trim().isEmpty()) return 0.0;
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception e) {
            logger.debug("Failed to parse Double '{}', using 0.0", value);
            return 0.0;
        }
    }

    private Long parseLongRelaxed(String value) {
        if (value == null || value.trim().isEmpty()) return 0L;
        try {
            // Handle decimal strings (e.g., "123.0" -> 123)
            double doubleVal = Double.parseDouble(value.trim());
            return (long) doubleVal;
        } catch (Exception e) {
            logger.debug("Failed to parse Long '{}', using 0", value);
            return 0L;
        }
    }

    private BigDecimal parseBigDecimalRelaxed(String value) {
        if (value == null || value.trim().isEmpty()) return new BigDecimal("0");
        try {
            return new BigDecimal(value.trim());
        } catch (Exception e) {
            logger.debug("Failed to parse BigDecimal '{}', using 0", value);
            return new BigDecimal("0");
        }
    }

    private Integer parseCountryCodeRelaxed(String country) {
        try {
            return parseCountryCode(country);
        } catch (Exception e) {
            return 0;
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

    private void analyzeFailurePatterns(List<String> failureReasons, List<MetaInsightsDto> dtos, int failCount) {
        try {
            // Count insights without required fields
            long missingAdId = dtos.stream().mapToLong(dto ->
                    (dto.getAdId() == null || dto.getAdId().trim().isEmpty()) ? 1 : 0).sum();
            long missingAccountId = dtos.stream().mapToLong(dto ->
                    (dto.getAccountId() == null || dto.getAccountId().trim().isEmpty()) ? 1 : 0).sum();
            long missingDateStart = dtos.stream().mapToLong(dto ->
                    (dto.getDateStart() == null || dto.getDateStart().trim().isEmpty()) ? 1 : 0).sum();

            if (missingAdId > 0 || missingAccountId > 0 || missingDateStart > 0) {
                logger.warn("📊 Missing required fields analysis:");
                if (missingAdId > 0) logger.warn("   🚫 {} insights missing adId", missingAdId);
                if (missingAccountId > 0) logger.warn("   🚫 {} insights missing accountId", missingAccountId);
                if (missingDateStart > 0) logger.warn("   🚫 {} insights missing dateStart", missingDateStart);
            }

            // Count insights with all zero metrics
            long allZeroMetrics = dtos.stream().mapToLong(dto -> {
                boolean hasNonZeroMetrics =
                        !isNullOrZero(dto.getImpressions()) ||
                                !isNullOrZero(dto.getClicks()) ||
                                !isNullOrZero(dto.getSpend()) ||
                                !isNullOrZero(dto.getReach());
                return hasNonZeroMetrics ? 0 : 1;
            }).sum();

            if (allZeroMetrics > 0) {
                logger.warn("📊 {} insights have all zero metrics (may be filtered)", allZeroMetrics);
            }

        } catch (Exception e) {
            logger.debug("Failed to analyze failure patterns: {}", e.getMessage());
        }
    }

    private boolean isValidInsightDto(MetaInsightsDto dto) {
        // Check required IDs
        if (isNullOrEmpty(dto.getAccountId())) {
            logger.debug("Invalid insight: missing accountId");
            return false;
        }
        if (isNullOrEmpty(dto.getAdId())) {
            logger.debug("Invalid insight: missing adId");
            return false;
        }
        if (isNullOrEmpty(dto.getDateStart())) {
            logger.debug("Invalid insight: missing dateStart");
            return false;
        }

        // Allow insights with zero metrics (they're still valid data points)
        return true;
    }

    private boolean isNullOrEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    private boolean isNullOrZero(String value) {
        return value == null || value.equals("0") || value.equals("") || value.equals("0.0");
    }

    private void setZeroMetrics(AdsReporting reporting) {
        reporting.setSpend(0.0);
        reporting.setRevenue(0.0);
        reporting.setPurchaseRoas(new BigDecimal("0"));
        reporting.setImpressions(0L);
        reporting.setClicks(0L);
        reporting.setUniqueClicks(0L);
        reporting.setLinkClicks(0L);
        reporting.setUniqueLinkClicks(0L);
        reporting.setReach(0L);

        reporting.setCostPerUniqueClick(new BigDecimal("0"));
        reporting.setFrequency(new BigDecimal("0"));
        reporting.setCpc(new BigDecimal("0"));
        reporting.setCpm(new BigDecimal("0"));
        reporting.setCpp(new BigDecimal("0"));
        reporting.setCtr(new BigDecimal("0"));
        reporting.setUniqueCtr(new BigDecimal("0"));

        reporting.setPostEngagement(0L);
        reporting.setPageEngagement(0L);
        reporting.setLikes(0L);
        reporting.setComments(0L);
        reporting.setShares(0L);
        reporting.setPhotoView(0L);
        reporting.setVideoViews(0L);

        reporting.setVideoP25WatchedActions(0L);
        reporting.setVideoP50WatchedActions(0L);
        reporting.setVideoP75WatchedActions(0L);
        reporting.setVideoP95WatchedActions(0L);
        reporting.setVideoP100WatchedActions(0L);
        reporting.setVideoAvgPercentWatched(new BigDecimal("0"));

        reporting.setPurchases(0L);
        reporting.setPurchaseValue(new BigDecimal("0"));
        reporting.setLeads(0L);
        reporting.setCostPerLead(new BigDecimal("0"));
        reporting.setMobileAppInstall(0L);
        reporting.setCostPerAppInstall(new BigDecimal("0"));

        reporting.setSocialSpend(new BigDecimal("0"));
        reporting.setInlineLinkClicks(0L);
        reporting.setInlinePostEngagement(0L);
        reporting.setCostPerInlineLinkClick(new BigDecimal("0"));
        reporting.setCostPerInlinePostEngagement(new BigDecimal("0"));
    }
}