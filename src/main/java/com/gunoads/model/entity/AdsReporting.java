package com.gunoads.model.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdsReporting {

    @NotBlank
    private String accountId;

    @NotBlank
    private String platformId;

    @NotBlank
    private String campaignId;

    @NotBlank
    private String adsetId;

    @NotBlank
    private String advertisementId;

    @NotBlank
    private String placementId;

    @NotBlank
    private String adsProcessingDt;

    @NotBlank
    private String ageGroup;

    @NotBlank
    private String gender;

    @NotNull
    private Integer countryCode;

    @NotBlank
    private String region;

    @NotBlank
    private String city;

    @NotNull
    private Double spend;

    @NotNull
    private Double revenue;

    private BigDecimal purchaseRoas;

    @NotNull
    private Long impressions;

    @NotNull
    private Long clicks;

    @NotNull
    private Long uniqueClicks;

    private BigDecimal costPerUniqueClick;

    @NotNull
    private Long linkClicks;

    @NotNull
    private Long uniqueLinkClicks;

    @NotNull
    private Long reach;

    private BigDecimal frequency;
    private BigDecimal cpc;
    private BigDecimal cpm;
    private BigDecimal cpp;
    private BigDecimal ctr;
    private BigDecimal uniqueCtr;

    @NotNull
    private Long postEngagement;

    @NotNull
    private Long pageEngagement;

    @NotNull
    private Long likes;

    @NotNull
    private Long comments;

    @NotNull
    private Long shares;

    @NotNull
    private Long photoView;

    @NotNull
    private Long videoViews;

    @NotNull
    private Long videoP25WatchedActions;

    @NotNull
    private Long videoP50WatchedActions;

    @NotNull
    private Long videoP75WatchedActions;

    @NotNull
    private Long videoP95WatchedActions;

    @NotNull
    private Long videoP100WatchedActions;

    private BigDecimal videoAvgPercentWatched;

    @NotNull
    private Long purchases;

    private BigDecimal purchaseValue;

    @NotNull
    private Long leads;

    private BigDecimal costPerLead;

    @NotNull
    private Long mobileAppInstall;

    private BigDecimal costPerAppInstall;
    private BigDecimal socialSpend;

    @NotNull
    private Long inlineLinkClicks;

    @NotNull
    private Long inlinePostEngagement;

    private BigDecimal costPerInlineLinkClick;
    private BigDecimal costPerInlinePostEngagement;
    private String currency;
    private String attributionSetting;
    private String dateStart;
    private String dateStop;
    private String createdAt;
    private String updatedAt;
    private String countryName;
}