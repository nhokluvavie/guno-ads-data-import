package com.gunoads.model.dto;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.HashMap;

/**
 * MetaInsightsBatchDto - Container for holding insights data from 3 breakdown batches
 *
 * Purpose: Merge data from 3 separate API calls with different breakdowns:
 * - Batch 1: Demographic data (age, gender)
 * - Batch 2: Geographic data (country, region)
 * - Batch 3: Placement data (publisher_platform, impression_device)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetaInsightsBatchDto {

    // ==================== UNIQUE IDENTIFIER (MERGE KEY) ====================

    /** Unique key for merging: accountId + adId + date */
    private String mergeKey;

    // Basic identifiers (same across all batches)
    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("campaign_id")
    private String campaignId;

    @JsonProperty("adset_id")
    private String adsetId;

    @JsonProperty("ad_id")
    private String adId;

    @JsonProperty("date_start")
    private String dateStart;

    @JsonProperty("date_stop")
    private String dateStop;

    // ==================== CORE METRICS (SAME ACROSS BATCHES) ====================

    // Performance metrics (will be same across all batches for same ad+date)
    private String spend;
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
    private String costPerUniqueClick;

    // ==================== BATCH 1: DEMOGRAPHIC DATA ====================

    /** From breakdown batch 1: ["age", "gender"] */
    private String age;
    private String gender;

    /** Flag to indicate if batch 1 data is present */
    private boolean hasDemographicData = false;

    // ==================== BATCH 2: GEOGRAPHIC DATA ====================

    /** From breakdown batch 2: ["country", "region"] */
    private String country;
    private String region;

    /** Flag to indicate if batch 2 data is present */
    private boolean hasGeographicData = false;

    // ==================== BATCH 3: PLACEMENT DATA ====================

    /** From breakdown batch 3: ["publisher_platform", "impression_device"] */
    @JsonProperty("publisher_platform")
    private String publisherPlatform;

    @JsonProperty("impression_device")
    private String impressionDevice;

    /** Flag to indicate if batch 3 data is present */
    private boolean hasPlacementData = false;

    // ==================== CONVERSION & ENGAGEMENT METRICS ====================

    // Conversion metrics (consistent across batches)
    private String purchases;

    @JsonProperty("purchase_value")
    private String purchaseValue;

    @JsonProperty("purchase_roas")
    private String purchaseRoas;

    private String leads;

    @JsonProperty("cost_per_lead")
    private String costPerLead;

    // Engagement metrics
    @JsonProperty("post_engagement")
    private String postEngagement;

    @JsonProperty("page_engagement")
    private String pageEngagement;

    private String likes;
    private String comments;
    private String shares;

    @JsonProperty("video_views")
    private String videoViews;

    // ==================== METADATA & TRACKING ====================

    /** Timestamp when this batch was created */
    private LocalDateTime createdAt;

    /** Number of batches merged (1-3) */
    private int batchesMerged = 0;

    /** Source batch information for debugging */
    private Map<String, Object> batchSources = new HashMap<>();

    /** Any merge warnings or issues */
    private String mergeWarnings;

    // ==================== UTILITY METHODS ====================

    /**
     * Generate merge key from core identifiers
     */
    public static String generateMergeKey(String accountId, String adId, String dateStart) {
        return String.format("%s_%s_%s", accountId, adId, dateStart);
    }

    /**
     * Auto-generate merge key from current fields
     */
    public void generateMergeKey() {
        this.mergeKey = generateMergeKey(this.accountId, this.adId, this.dateStart);
    }

    /**
     * Check if this batch has complete data from all 3 sources
     */
    public boolean isComplete() {
        return hasDemographicData && hasGeographicData && hasPlacementData;
    }

    /**
     * Get completion percentage (0-100)
     */
    public int getCompletionPercentage() {
        int completed = 0;
        if (hasDemographicData) completed++;
        if (hasGeographicData) completed++;
        if (hasPlacementData) completed++;
        return (completed * 100) / 3;
    }

    /**
     * Mark demographic data as present
     */
    public void markDemographicDataPresent() {
        this.hasDemographicData = true;
        this.batchesMerged++;
        this.batchSources.put("demographic", "batch1");
    }

    /**
     * Mark geographic data as present
     */
    public void markGeographicDataPresent() {
        this.hasGeographicData = true;
        this.batchesMerged++;
        this.batchSources.put("geographic", "batch2");
    }

    /**
     * Mark placement data as present
     */
    public void markPlacementDataPresent() {
        this.hasPlacementData = true;
        this.batchesMerged++;
        this.batchSources.put("placement", "batch3");
    }

    /**
     * Get missing batch names for logging
     */
    public String getMissingBatches() {
        StringBuilder missing = new StringBuilder();
        if (!hasDemographicData) missing.append("demographic,");
        if (!hasGeographicData) missing.append("geographic,");
        if (!hasPlacementData) missing.append("placement,");

        String result = missing.toString();
        return result.endsWith(",") ? result.substring(0, result.length() - 1) : result;
    }

    /**
     * Convert to standard MetaInsightsDto for backward compatibility
     */
    public MetaInsightsDto toStandardDto() {
        MetaInsightsDto dto = new MetaInsightsDto();

        // Basic identifiers
        dto.setAccountId(this.accountId);
        dto.setCampaignId(this.campaignId);
        dto.setAdsetId(this.adsetId);
        dto.setAdId(this.adId);
        dto.setDateStart(this.dateStart);
        dto.setDateStop(this.dateStop);

        // Core metrics
        dto.setSpend(this.spend);
        dto.setImpressions(this.impressions);
        dto.setClicks(this.clicks);
        dto.setUniqueClicks(this.uniqueClicks);
        dto.setLinkClicks(this.linkClicks);
        dto.setUniqueLinkClicks(this.uniqueLinkClicks);
        dto.setReach(this.reach);
        dto.setFrequency(this.frequency);
        dto.setCpc(this.cpc);
        dto.setCpm(this.cpm);
        dto.setCpp(this.cpp);
        dto.setCtr(this.ctr);
        dto.setUniqueCtr(this.uniqueCtr);
        dto.setCostPerUniqueClick(this.costPerUniqueClick);

        // Breakdown data (from 3 batches)
        dto.setAge(this.age);
        dto.setGender(this.gender);
        dto.setCountry(this.country);
        dto.setRegion(this.region);
        dto.setPlacement(this.publisherPlatform); // Map publisher_platform to placement
        dto.setDevice_platform(this.impressionDevice); // Map impression_device to device_platform

        // Conversion metrics
        dto.setPurchases(this.purchases);
        dto.setPurchaseValue(this.purchaseValue);
        dto.setPurchaseRoas(this.purchaseRoas);
        dto.setLeads(this.leads);
        dto.setCostPerLead(this.costPerLead);

        // Engagement metrics
        dto.setPostEngagement(this.postEngagement);
        dto.setPageEngagement(this.pageEngagement);
        dto.setLikes(this.likes);
        dto.setComments(this.comments);
        dto.setShares(this.shares);
        dto.setVideoViews(this.videoViews);

        return dto;
    }

    @Override
    public String toString() {
        return String.format("MetaInsightsBatchDto{mergeKey='%s', completion=%d%%, batches=%d, missing=%s}",
                mergeKey, getCompletionPercentage(), batchesMerged, getMissingBatches());
    }
}