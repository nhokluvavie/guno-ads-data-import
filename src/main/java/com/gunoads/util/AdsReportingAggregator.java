package com.gunoads.util;

import com.gunoads.model.entity.AdsReporting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

/**
 * AdsReportingAggregator - Handles composite key aggregation for AdsReporting data
 *
 * Purpose: Aggregate multiple AdsReporting records with same composite key into single record
 * to prevent duplicate key violations and ensure data integrity.
 */
@Component
public class AdsReportingAggregator {

    private static final Logger logger = LoggerFactory.getLogger(AdsReportingAggregator.class);

    /**
     * Aggregate AdsReporting list by composite primary key
     *
     * Composite Key (12 fields):
     * account_id, platform_id, campaign_id, adset_id, advertisement_id, placement_id,
     * ads_processing_dt, age_group, gender, country_code, region, city
     */
    public List<AdsReporting> aggregateByCompositeKey(List<AdsReporting> reportingList) {
        if (reportingList == null || reportingList.isEmpty()) {
            return new ArrayList<>();
        }

        long startTime = System.currentTimeMillis();
        logger.info("🔄 Starting composite key aggregation: {} input records", reportingList.size());

        // Group by composite key
        Map<String, List<AdsReporting>> groupedData = reportingList.stream()
                .collect(Collectors.groupingBy(this::generateCompositeKey));

        List<AdsReporting> aggregatedResults = new ArrayList<>();
        int duplicateGroups = 0;
        int totalDuplicates = 0;

        for (Map.Entry<String, List<AdsReporting>> entry : groupedData.entrySet()) {
            String compositeKey = entry.getKey();
            List<AdsReporting> duplicates = entry.getValue();

            if (duplicates.size() > 1) {
                duplicateGroups++;
                totalDuplicates += duplicates.size() - 1; // -1 because one record is kept
                logger.debug("🔄 Aggregating {} records for key: {}", duplicates.size(), compositeKey);
            }

            // Aggregate the group into single record
            AdsReporting aggregated = aggregateGroup(duplicates);
            aggregatedResults.add(aggregated);
        }

        long duration = System.currentTimeMillis() - startTime;

        // Log aggregation summary
        logger.info("✅ Composite key aggregation completed in {}ms:", duration);
        logger.info("   📥 Input: {} records", reportingList.size());
        logger.info("   📤 Output: {} unique records", aggregatedResults.size());
        logger.info("   🔄 Duplicate groups: {}", duplicateGroups);
        logger.info("   📊 Total duplicates removed: {}", totalDuplicates);

        if (duplicateGroups > 0) {
            double deduplicationRate = (totalDuplicates * 100.0) / reportingList.size();
            logger.info("   💾 Deduplication rate: {:.1f}%", deduplicationRate);
        }

        return aggregatedResults;
    }

    /**
     * Generate composite key string from AdsReporting record
     */
    private String generateCompositeKey(AdsReporting reporting) {
        return String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%s|%s",
                safeString(reporting.getAccountId()),
                safeString(reporting.getPlatformId()),
                safeString(reporting.getCampaignId()),
                safeString(reporting.getAdsetId()),
                safeString(reporting.getAdvertisementId()),
                safeString(reporting.getPlacementId()),
                safeString(reporting.getAdsProcessingDt()),
                safeString(reporting.getAgeGroup()),
                safeString(reporting.getGender()),
                reporting.getCountryCode() != null ? reporting.getCountryCode() : 0,
                safeString(reporting.getRegion()),
                safeString(reporting.getCity())
        );
    }

    /**
     * Aggregate a group of AdsReporting records with same composite key
     */
    private AdsReporting aggregateGroup(List<AdsReporting> group) {
        if (group.size() == 1) {
            return group.get(0); // No aggregation needed
        }

        // Use first record as base, aggregate metrics from all records
        AdsReporting base = copyRecord(group.get(0));

        // Aggregate numeric metrics
        double totalSpend = group.stream().mapToDouble(r -> r.getSpend() != null ? r.getSpend() : 0.0).sum();
        double totalRevenue = group.stream().mapToDouble(r -> r.getRevenue() != null ? r.getRevenue() : 0.0).sum();
        long totalImpressions = group.stream().mapToLong(r -> r.getImpressions() != null ? r.getImpressions() : 0L).sum();
        long totalClicks = group.stream().mapToLong(r -> r.getClicks() != null ? r.getClicks() : 0L).sum();
        long totalUniqueClicks = group.stream().mapToLong(r -> r.getUniqueClicks() != null ? r.getUniqueClicks() : 0L).sum();
        long totalLinkClicks = group.stream().mapToLong(r -> r.getLinkClicks() != null ? r.getLinkClicks() : 0L).sum();
        long totalUniqueLinkClicks = group.stream().mapToLong(r -> r.getUniqueLinkClicks() != null ? r.getUniqueLinkClicks() : 0L).sum();
        long totalReach = group.stream().mapToLong(r -> r.getReach() != null ? r.getReach() : 0L).sum();

        // Set aggregated core metrics
        base.setSpend(totalSpend);
        base.setRevenue(totalRevenue);
        base.setImpressions(totalImpressions);
        base.setClicks(totalClicks);
        base.setUniqueClicks(totalUniqueClicks);
        base.setLinkClicks(totalLinkClicks);
        base.setUniqueLinkClicks(totalUniqueLinkClicks);
        base.setReach(totalReach);

        // Aggregate engagement metrics
        long totalPostEngagement = group.stream().mapToLong(r -> r.getPostEngagement() != null ? r.getPostEngagement() : 0L).sum();
        long totalPageEngagement = group.stream().mapToLong(r -> r.getPageEngagement() != null ? r.getPageEngagement() : 0L).sum();
        long totalLikes = group.stream().mapToLong(r -> r.getLikes() != null ? r.getLikes() : 0L).sum();
        long totalComments = group.stream().mapToLong(r -> r.getComments() != null ? r.getComments() : 0L).sum();
        long totalShares = group.stream().mapToLong(r -> r.getShares() != null ? r.getShares() : 0L).sum();
        long totalPhotoView = group.stream().mapToLong(r -> r.getPhotoView() != null ? r.getPhotoView() : 0L).sum();
        long totalVideoViews = group.stream().mapToLong(r -> r.getVideoViews() != null ? r.getVideoViews() : 0L).sum();

        base.setPostEngagement(totalPostEngagement);
        base.setPageEngagement(totalPageEngagement);
        base.setLikes(totalLikes);
        base.setComments(totalComments);
        base.setShares(totalShares);
        base.setPhotoView(totalPhotoView);
        base.setVideoViews(totalVideoViews);

        // Aggregate video metrics
        long totalVideoP25 = group.stream().mapToLong(r -> r.getVideoP25WatchedActions() != null ? r.getVideoP25WatchedActions() : 0L).sum();
        long totalVideoP50 = group.stream().mapToLong(r -> r.getVideoP50WatchedActions() != null ? r.getVideoP50WatchedActions() : 0L).sum();
        long totalVideoP75 = group.stream().mapToLong(r -> r.getVideoP75WatchedActions() != null ? r.getVideoP75WatchedActions() : 0L).sum();
        long totalVideoP95 = group.stream().mapToLong(r -> r.getVideoP95WatchedActions() != null ? r.getVideoP95WatchedActions() : 0L).sum();
        long totalVideoP100 = group.stream().mapToLong(r -> r.getVideoP100WatchedActions() != null ? r.getVideoP100WatchedActions() : 0L).sum();

        base.setVideoP25WatchedActions(totalVideoP25);
        base.setVideoP50WatchedActions(totalVideoP50);
        base.setVideoP75WatchedActions(totalVideoP75);
        base.setVideoP95WatchedActions(totalVideoP95);
        base.setVideoP100WatchedActions(totalVideoP100);

        // Aggregate conversion metrics
        long totalPurchases = group.stream().mapToLong(r -> r.getPurchases() != null ? r.getPurchases() : 0L).sum();
        long totalLeads = group.stream().mapToLong(r -> r.getLeads() != null ? r.getLeads() : 0L).sum();
        long totalMobileAppInstall = group.stream().mapToLong(r -> r.getMobileAppInstall() != null ? r.getMobileAppInstall() : 0L).sum();
        long totalInlineLinkClicks = group.stream().mapToLong(r -> r.getInlineLinkClicks() != null ? r.getInlineLinkClicks() : 0L).sum();
        long totalInlinePostEngagement = group.stream().mapToLong(r -> r.getInlinePostEngagement() != null ? r.getInlinePostEngagement() : 0L).sum();

        base.setPurchases(totalPurchases);
        base.setLeads(totalLeads);
        base.setMobileAppInstall(totalMobileAppInstall);
        base.setInlineLinkClicks(totalInlineLinkClicks);
        base.setInlinePostEngagement(totalInlinePostEngagement);

        // Recalculate derived metrics after aggregation
        recalculateDerivedMetrics(base);

        // Update timestamps to latest
        String latestUpdated = group.stream()
                .map(AdsReporting::getUpdatedAt)
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .orElse(base.getUpdatedAt());
        base.setUpdatedAt(latestUpdated);

        return base;
    }

    /**
     * Recalculate derived metrics after aggregation
     */
    private void recalculateDerivedMetrics(AdsReporting reporting) {
        // Recalculate CPC
        if (reporting.getClicks() != null && reporting.getClicks() > 0 && reporting.getSpend() != null) {
            BigDecimal cpc = BigDecimal.valueOf(reporting.getSpend())
                    .divide(BigDecimal.valueOf(reporting.getClicks()), 4, RoundingMode.HALF_UP);
            reporting.setCpc(cpc);
        }

        // Recalculate CPM
        if (reporting.getImpressions() != null && reporting.getImpressions() > 0 && reporting.getSpend() != null) {
            BigDecimal cpm = BigDecimal.valueOf(reporting.getSpend() * 1000)
                    .divide(BigDecimal.valueOf(reporting.getImpressions()), 4, RoundingMode.HALF_UP);
            reporting.setCpm(cpm);
        }

        // Recalculate CTR
        if (reporting.getImpressions() != null && reporting.getImpressions() > 0 && reporting.getClicks() != null) {
            BigDecimal ctr = BigDecimal.valueOf(reporting.getClicks() * 100.0)
                    .divide(BigDecimal.valueOf(reporting.getImpressions()), 4, RoundingMode.HALF_UP);
            reporting.setCtr(ctr);
        }

        // Recalculate unique CTR
        if (reporting.getReach() != null && reporting.getReach() > 0 && reporting.getUniqueClicks() != null) {
            BigDecimal uniqueCtr = BigDecimal.valueOf(reporting.getUniqueClicks() * 100.0)
                    .divide(BigDecimal.valueOf(reporting.getReach()), 4, RoundingMode.HALF_UP);
            reporting.setUniqueCtr(uniqueCtr);
        }

        // Recalculate frequency
        if (reporting.getReach() != null && reporting.getReach() > 0 && reporting.getImpressions() != null) {
            BigDecimal frequency = BigDecimal.valueOf(reporting.getImpressions())
                    .divide(BigDecimal.valueOf(reporting.getReach()), 4, RoundingMode.HALF_UP);
            reporting.setFrequency(frequency);
        }

        // Recalculate cost per unique click
        if (reporting.getUniqueClicks() != null && reporting.getUniqueClicks() > 0 && reporting.getSpend() != null) {
            BigDecimal costPerUniqueClick = BigDecimal.valueOf(reporting.getSpend())
                    .divide(BigDecimal.valueOf(reporting.getUniqueClicks()), 4, RoundingMode.HALF_UP);
            reporting.setCostPerUniqueClick(costPerUniqueClick);
        }

        // Recalculate ROAS if we have purchase value
        if (reporting.getPurchaseValue() != null && reporting.getSpend() != null && reporting.getSpend() > 0) {
            BigDecimal roas = reporting.getPurchaseValue()
                    .divide(BigDecimal.valueOf(reporting.getSpend()), 4, RoundingMode.HALF_UP);
            reporting.setPurchaseRoas(roas);
        }
    }

    /**
     * Create a deep copy of AdsReporting record
     */
    private AdsReporting copyRecord(AdsReporting original) {
        AdsReporting copy = new AdsReporting();

        // Copy all fields
        copy.setAccountId(original.getAccountId());
        copy.setPlatformId(original.getPlatformId());
        copy.setCampaignId(original.getCampaignId());
        copy.setAdsetId(original.getAdsetId());
        copy.setAdvertisementId(original.getAdvertisementId());
        copy.setPlacementId(original.getPlacementId());
        copy.setAdsProcessingDt(original.getAdsProcessingDt());
        copy.setAgeGroup(original.getAgeGroup());
        copy.setGender(original.getGender());
        copy.setCountryCode(original.getCountryCode());
        copy.setRegion(original.getRegion());
        copy.setCity(original.getCity());

        // Copy metrics (will be overwritten by aggregation)
        copy.setSpend(original.getSpend());
        copy.setRevenue(original.getRevenue());
        copy.setPurchaseRoas(original.getPurchaseRoas());
        copy.setImpressions(original.getImpressions());
        copy.setClicks(original.getClicks());
        copy.setUniqueClicks(original.getUniqueClicks());
        copy.setCostPerUniqueClick(original.getCostPerUniqueClick());
        copy.setLinkClicks(original.getLinkClicks());
        copy.setUniqueLinkClicks(original.getUniqueLinkClicks());
        copy.setReach(original.getReach());
        copy.setFrequency(original.getFrequency());
        copy.setCpc(original.getCpc());
        copy.setCpm(original.getCpm());
        copy.setCpp(original.getCpp());
        copy.setCtr(original.getCtr());
        copy.setUniqueCtr(original.getUniqueCtr());

        // Copy engagement metrics
        copy.setPostEngagement(original.getPostEngagement());
        copy.setPageEngagement(original.getPageEngagement());
        copy.setLikes(original.getLikes());
        copy.setComments(original.getComments());
        copy.setShares(original.getShares());
        copy.setPhotoView(original.getPhotoView());
        copy.setVideoViews(original.getVideoViews());
        copy.setVideoP25WatchedActions(original.getVideoP25WatchedActions());
        copy.setVideoP50WatchedActions(original.getVideoP50WatchedActions());
        copy.setVideoP75WatchedActions(original.getVideoP75WatchedActions());
        copy.setVideoP95WatchedActions(original.getVideoP95WatchedActions());
        copy.setVideoP100WatchedActions(original.getVideoP100WatchedActions());
        copy.setVideoAvgPercentWatched(original.getVideoAvgPercentWatched());

        // Copy conversion metrics
        copy.setPurchases(original.getPurchases());
        copy.setPurchaseValue(original.getPurchaseValue());
        copy.setLeads(original.getLeads());
        copy.setCostPerLead(original.getCostPerLead());
        copy.setMobileAppInstall(original.getMobileAppInstall());
        copy.setCostPerAppInstall(original.getCostPerAppInstall());
        copy.setSocialSpend(original.getSocialSpend());
        copy.setInlineLinkClicks(original.getInlineLinkClicks());
        copy.setInlinePostEngagement(original.getInlinePostEngagement());
        copy.setCostPerInlineLinkClick(original.getCostPerInlineLinkClick());
        copy.setCostPerInlinePostEngagement(original.getCostPerInlinePostEngagement());

        // Copy metadata
        copy.setCurrency(original.getCurrency());
        copy.setAttributionSetting(original.getAttributionSetting());
        copy.setDateStart(original.getDateStart());
        copy.setDateStop(original.getDateStop());
        copy.setCreatedAt(original.getCreatedAt());
        copy.setUpdatedAt(original.getUpdatedAt());
        copy.setCountryName(original.getCountryName());

        return copy;
    }

    /**
     * Safe string helper to handle null values
     */
    private String safeString(String value) {
        return value != null ? value : "unknown";
    }

    /**
     * Validate aggregated data integrity
     */
    public boolean validateAggregatedData(List<AdsReporting> originalData, List<AdsReporting> aggregatedData) {
        if (originalData == null || aggregatedData == null) {
            return false;
        }

        // Check if total spend is preserved (within rounding tolerance)
        double originalTotalSpend = originalData.stream()
                .mapToDouble(r -> r.getSpend() != null ? r.getSpend() : 0.0).sum();
        double aggregatedTotalSpend = aggregatedData.stream()
                .mapToDouble(r -> r.getSpend() != null ? r.getSpend() : 0.0).sum();

        double spendDifference = Math.abs(originalTotalSpend - aggregatedTotalSpend);
        boolean spendPreserved = spendDifference < 0.01; // 1 cent tolerance

        logger.info("📊 Aggregation validation:");
        logger.info("   💰 Original total spend: ${:.2f}", originalTotalSpend);
        logger.info("   💰 Aggregated total spend: ${:.2f}", aggregatedTotalSpend);
        logger.info("   📈 Spend preservation: {}", spendPreserved ? "✅ PASS" : "❌ FAIL");

        return spendPreserved;
    }
}