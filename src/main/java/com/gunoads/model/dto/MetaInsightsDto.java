package com.gunoads.model.dto;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class MetaInsightsDto {

    // Basic identifiers
    @JsonProperty("campaign_id")
    private String campaignId;

    @JsonProperty("adset_id")
    private String adsetId;

    @JsonProperty("ad_id")
    private String adId;

    @JsonProperty("account_id")
    private String accountId;

    // BREAKDOWN DIMENSIONS (CRITICAL - These were missing!)
    private String age;
    private String gender;
    private String country;
    private String region;
    private String city;  // Added missing breakdown
    private String placement;
    private String device_platform;  // Added device breakdown

    // Core spend & performance metrics
    private String spend;
    private String revenue;  // May need custom mapping
    private String impressions;
    private String clicks;

    @JsonProperty("unique_clicks")
    private String uniqueClicks;

    @JsonProperty("link_clicks")
    private String linkClicks;

    @JsonProperty("unique_link_clicks")
    private String uniqueLinkClicks;

    private String reach;
    private String frequency;

    // Cost metrics
    private String cpc;
    private String cpm;
    private String cpp;
    private String ctr;

    @JsonProperty("unique_ctr")
    private String uniqueCtr;

    @JsonProperty("cost_per_unique_click")
    private String costPerUniqueClick;  // Added missing cost metric

    // Engagement metrics
    @JsonProperty("post_engagement")
    private String postEngagement;

    @JsonProperty("page_engagement")
    private String pageEngagement;

    private String likes;
    private String comments;
    private String shares;

    @JsonProperty("photo_view")
    private String photoView;

    @JsonProperty("video_views")
    private String videoViews;

    // Video completion metrics
    @JsonProperty("video_p25_watched_actions")
    private String videoP25WatchedActions;

    @JsonProperty("video_p50_watched_actions")
    private String videoP50WatchedActions;

    @JsonProperty("video_p75_watched_actions")
    private String videoP75WatchedActions;

    @JsonProperty("video_p95_watched_actions")
    private String videoP95WatchedActions;

    @JsonProperty("video_p100_watched_actions")
    private String videoP100WatchedActions;

    @JsonProperty("video_avg_percent_watched")
    private String videoAvgPercentWatched;  // Added missing video metric

    // Conversion metrics
    private String purchases;

    @JsonProperty("purchase_value")
    private String purchaseValue;

    @JsonProperty("purchase_roas")
    private String purchaseRoas;

    private String leads;

    @JsonProperty("cost_per_lead")
    private String costPerLead;

    @JsonProperty("mobile_app_install")
    private String mobileAppInstall;

    @JsonProperty("cost_per_app_install")
    private String costPerAppInstall;

    // Additional metrics
    @JsonProperty("social_spend")
    private String socialSpend;

    @JsonProperty("inline_link_clicks")
    private String inlineLinkClicks;

    @JsonProperty("inline_post_engagement")
    private String inlinePostEngagement;

    @JsonProperty("cost_per_inline_link_click")
    private String costPerInlineLinkClick;

    @JsonProperty("cost_per_inline_post_engagement")
    private String costPerInlinePostEngagement;

    // Meta information
    @JsonProperty("date_start")
    private String dateStart;

    @JsonProperty("date_stop")
    private String dateStop;

    private String currency;

    @JsonProperty("attribution_setting")
    private String attributionSetting;
}