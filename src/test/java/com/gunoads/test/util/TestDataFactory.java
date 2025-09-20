package com.gunoads.test.util;

import com.gunoads.model.dto.*;
import com.gunoads.model.entity.*;
import com.gunoads.model.enums.PlatformType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Factory for creating test data objects
 */
public class TestDataFactory {

    private static final String META_PLATFORM = PlatformType.META.getCode();
    private static final String TIMESTAMP = LocalDateTime.now().toString();

    // ==================== DTO Factories ====================

    public static MetaAccountDto createMetaAccountDto() {
        MetaAccountDto dto = new MetaAccountDto();
        dto.setId("act_123456789");
        dto.setAccountId("123456789");
        dto.setAccountName("Test Account");
        dto.setCurrency("USD");
        dto.setTimezoneId(1L);
        dto.setTimezoneName("America/New_York");
        dto.setAccountStatus("ACTIVE");
        dto.setBusinessId("business_123");
        dto.setBusinessName("Test Business");
        dto.setBusinessCountryCode(1);
        dto.setBusinessZip(10001);
        dto.setAmountSpent("1000.50");
        dto.setBalance("500.25");
        dto.setIsPersonal(false);
        dto.setIsPrepayAccount(false);
        dto.setIsTaxIdRequired(false);
        dto.setAccountAge(365);
        dto.setHasPageAuthorizedAdAccount(true);
        dto.setIsDirectDealsEnabled(false);
        dto.setIsNotificationsEnabled(true);
        dto.setCreatedTime(TIMESTAMP);
        return dto;
    }

    public static MetaCampaignDto createMetaCampaignDto() {
        MetaCampaignDto dto = new MetaCampaignDto();
        dto.setId("campaign_123");
        dto.setAccountId("act_123456789");
        dto.setName("Test Campaign");
        dto.setObjective("CONVERSIONS");
        dto.setStatus("ACTIVE");
        dto.setConfiguredStatus("ACTIVE");
        dto.setEffectiveStatus("ACTIVE");
        dto.setBuyingType("AUCTION");
        dto.setDailyBudget("100.00");
        dto.setCreatedTime(TIMESTAMP);
        return dto;
    }

    public static MetaAdSetDto createMetaAdSetDto() {
        MetaAdSetDto dto = new MetaAdSetDto();
        dto.setId("adset_123");
        dto.setCampaignId("campaign_123");
        dto.setName("Test AdSet");
        dto.setStatus("ACTIVE");
        dto.setLifetimeImps(1000L);
        dto.setOptimizationGoal("CONVERSIONS");
        dto.setBillingEvent("IMPRESSIONS");
        dto.setDailyBudget("50.00");
        dto.setAgeMin(18);
        dto.setAgeMax(65);
        dto.setIsAutobid(true);
        dto.setCreatedTime(TIMESTAMP);
        return dto;
    }

    public static MetaAdDto createMetaAdDto() {
        MetaAdDto dto = new MetaAdDto();
        dto.setId("ad_123");
        dto.setAdsetId("adset_123");
        dto.setName("Test Ad");
        dto.setStatus("ACTIVE");
        dto.setConfiguredStatus("ACTIVE");
        dto.setEffectiveStatus("ACTIVE");
        dto.setCreatedTime(TIMESTAMP);

        MetaAdDto.Creative creative = new MetaAdDto.Creative();
        creative.setId("creative_123");
        creative.setName("Test Creative");
        creative.setType("SINGLE_IMAGE");
        creative.setIsAppInstallAd(false);
        creative.setIsDynamicAd(false);
        creative.setIsVideoAd(false);
        creative.setIsCarouselAd(false);
        dto.setCreative(creative);

        return dto;
    }

    public static MetaInsightsDto createMetaInsightsDto() {
        MetaInsightsDto dto = new MetaInsightsDto();
        dto.setAccountId("act_123456789");
        dto.setCampaignId("campaign_123");
        dto.setAdsetId("adset_123");
        dto.setAdId("ad_123");
        dto.setAge("25-34");
        dto.setGender("male");
        dto.setCountry("US");
        dto.setRegion("California");
        dto.setCity("San Francisco");
        dto.setPlacement("feed");
        dto.setDevice_platform("mobile");
        dto.setSpend("100.50");
        dto.setImpressions("10000");
        dto.setClicks("500");
        dto.setUniqueClicks("450");
        dto.setReach("5000");
        dto.setFrequency("2.0");
        dto.setCpc("0.20");
        dto.setCpm("10.05");
        dto.setCtr("5.0");
        dto.setDateStart("2024-01-01");
        dto.setDateStop("2024-01-01");
        dto.setCurrency("USD");
        return dto;
    }

    // ==================== Entity Factories ====================

    public static Account createAccount() {
        Account account = new Account();
        account.setId("act_123456789");
        account.setPlatformId(META_PLATFORM);
        account.setAccountName("Test Account");
        account.setCurrency("USD");
        account.setTimezoneId(1L);
        account.setTimezoneName("America/New_York");
        account.setAccountStatus("ACTIVE");
        account.setBusinessCountryCode(1);
        account.setBusinessZip(10001);
        account.setIsPersonal(false);
        account.setIsPrepayAccount(false);
        account.setIsTaxIdRequired(false);
        account.setAccountAge(365);
        account.setHasPageAuthorizedAdAccount(true);
        account.setIsDirectDealsEnabled(false);
        account.setIsNotificationsEnabled(true);
        account.setLastUpdated(TIMESTAMP);
        return account;
    }

    public static Campaign createCampaign() {
        Campaign campaign = new Campaign();
        campaign.setId("campaign_123");
        campaign.setAccountId("act_123456789");
        campaign.setPlatformId(META_PLATFORM);
        campaign.setCampaignName("Test Campaign");
        campaign.setCamObjective("CONVERSIONS");
        campaign.setCamStatus("ACTIVE");
        campaign.setConfiguredStatus("ACTIVE");
        campaign.setEffectiveStatus("ACTIVE");
        campaign.setBuyingType("AUCTION");
        campaign.setDailyBudget(new BigDecimal("100.00"));
        campaign.setLastUpdated(TIMESTAMP);
        return campaign;
    }

    public static AdSet createAdSet() {
        AdSet adSet = new AdSet();
        adSet.setId("adset_123");
        adSet.setCampaignId("campaign_123");
        adSet.setAdSetName("Test AdSet");
        adSet.setAdSetStatus("ACTIVE");
        adSet.setLifetimeImps(1000L);
        adSet.setOptimizationGoal("CONVERSIONS");
        adSet.setBillingEvent("IMPRESSIONS");
        adSet.setDailyBudget(new BigDecimal("50.00"));
        adSet.setAgeMin(18);
        adSet.setAgeMax(65);
        adSet.setIsAutobid(true);
        adSet.setLastUpdated(TIMESTAMP);
        return adSet;
    }

    public static Advertisement createAdvertisement() {
        Advertisement ad = new Advertisement();
        ad.setId("ad_123");
        ad.setAdsetid("adset_123");
        ad.setAdName("Test Ad");
        ad.setAdStatus("ACTIVE");
        ad.setConfiguredStatus("ACTIVE");
        ad.setEffectiveStatus("ACTIVE");
        ad.setCreativeId("creative_123");
        ad.setCreativeName("Test Creative");
        ad.setCreativeType("SINGLE_IMAGE");
        ad.setIsAppInstallAd(false);
        ad.setIsDynamicAd(false);
        ad.setIsVideoAd(false);
        ad.setIsCarouselAd(false);
        ad.setLastUpdated(TIMESTAMP);
        return ad;
    }

    public static AdsReporting createAdsReporting() {
        AdsReporting reporting = new AdsReporting();
        reporting.setAccountId("act_123456789");
        reporting.setPlatformId(META_PLATFORM);
        reporting.setCampaignId("campaign_123");
        reporting.setAdsetId("adset_123");
        reporting.setAdvertisementId("ad_123");
        reporting.setPlacementId("feed_mobile");
        reporting.setAdsProcessingDt("2024-01-01");
        reporting.setAgeGroup("25-34");
        reporting.setGender("male");
        reporting.setCountryCode(1);
        reporting.setRegion("California");
        reporting.setCity("San Francisco");
        reporting.setSpend(100.50);
        reporting.setRevenue(200.00);
        reporting.setImpressions(10000L);
        reporting.setClicks(500L);
        reporting.setUniqueClicks(450L);
        reporting.setLinkClicks(400L);
        reporting.setUniqueLinkClicks(380L);
        reporting.setReach(5000L);
        reporting.setPostEngagement(300L);
        reporting.setPageEngagement(250L);
        reporting.setLikes(100L);
        reporting.setComments(20L);
        reporting.setShares(10L);
        reporting.setPhotoView(50L);
        reporting.setVideoViews(200L);
        reporting.setVideoP25WatchedActions(180L);
        reporting.setVideoP50WatchedActions(150L);
        reporting.setVideoP75WatchedActions(100L);
        reporting.setVideoP95WatchedActions(80L);
        reporting.setVideoP100WatchedActions(70L);
        reporting.setPurchases(5L);
        reporting.setLeads(10L);
        reporting.setMobileAppInstall(3L);
        reporting.setInlineLinkClicks(350L);
        reporting.setInlinePostEngagement(280L);
        reporting.setCurrency("USD");
        reporting.setDateStart("2024-01-01");
        reporting.setDateStop("2024-01-01");
        reporting.setCreatedAt(TIMESTAMP);
        reporting.setUpdatedAt(TIMESTAMP);
        reporting.setCountryName("United States");
        return reporting;
    }

    public static Placement createPlacement() {
        Placement placement = new Placement();
        placement.setId("placement_123");
        placement.setAdvertisementId("ad_123");
        placement.setPlacementName("Facebook Feed");
        placement.setPlatform("facebook");
        placement.setPlacementType("feed");
        placement.setDeviceType("mobile");
        placement.setPosition("1");
        placement.setIsActive(true);
        placement.setSupportsVideo(true);
        placement.setSupportsCarousel(true);
        placement.setSupportsCollection(false);
        placement.setCreatedAt(TIMESTAMP);
        return placement;
    }

    public static AdsProcessingDate createAdsProcessingDate() {
        AdsProcessingDate date = new AdsProcessingDate();
        date.setFullDate("2024-01-01");
        date.setDayOfWeek(1);
        date.setDayOfWeekName("Monday");
        date.setDayOfMonth(1);
        date.setDayOfYear(1);
        date.setWeekOfYear(1);
        date.setMonthOfYear(1);
        date.setMonthName("January");
        date.setQuarter(1);
        date.setYear(2024);
        date.setIsWeekend(false);
        date.setIsHoliday(false);
        date.setHolidayName(null);
        date.setFiscalYear(2024);
        date.setFiscalQuarter(1);
        return date;
    }

    // ==================== List Factories ====================

    public static List<MetaAccountDto> createMetaAccountDtoList(int count) {
        return List.of(createMetaAccountDto());
    }

    public static List<MetaCampaignDto> createMetaCampaignDtoList(int count) {
        return List.of(createMetaCampaignDto());
    }

    public static List<MetaInsightsDto> createMetaInsightsDtoList(int count) {
        return List.of(createMetaInsightsDto());
    }
}