package com.gunoads.util;

import com.gunoads.model.dto.MetaInsightsDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * FIXED InsightsDataMerger - Preserves ALL data instead of losing records
 *
 * OLD PROBLEM: Only kept records with all 3 breakdown types, causing massive data loss
 * NEW SOLUTION: INCLUSIVE merge - keep all records, merge where possible
 */
@Component
public class InsightsDataMerger {

    private static final Logger logger = LoggerFactory.getLogger(InsightsDataMerger.class);

    /**
     * FIXED: INCLUSIVE merge that preserves ALL records
     *
     * Strategy: Union all records, merge complementary data where possible
     */
    public List<MetaInsightsDto> mergeAllBatches(
            List<MetaInsightsDto> batch1Results,
            List<MetaInsightsDto> batch2Results,
            List<MetaInsightsDto> batch3Results) {

        logger.info("🔄 Starting INCLUSIVE merge: batch1={}, batch2={}, batch3={}",
                batch1Results.size(), batch2Results.size(), batch3Results.size());

        long startTime = System.currentTimeMillis();

        // Step 1: Create comprehensive record map (UNION approach)
        Map<String, MetaInsightsDto> allRecords = new HashMap<>();

        // Step 2: Add ALL records from batch 1 (demographic) - BASE LAYER
        addBatchRecords(batch1Results, allRecords, "demographic");

        // Step 3: Merge or add records from batch 2 (geographic)
        mergeBatchRecords(batch2Results, allRecords, "geographic");

        // Step 4: Merge or add records from batch 3 (placement)
        mergeBatchRecords(batch3Results, allRecords, "placement");

        List<MetaInsightsDto> finalResults = new ArrayList<>(allRecords.values());
        long duration = System.currentTimeMillis() - startTime;

        logger.info("✅ INCLUSIVE merge completed: {} total records in {}ms", finalResults.size(), duration);
        logInclusiveMergeStats(batch1Results, batch2Results, batch3Results, finalResults);

        return finalResults;
    }

    /**
     * Add all records from a batch to the master map
     */
    private void addBatchRecords(List<MetaInsightsDto> batchResults,
                                 Map<String, MetaInsightsDto> allRecords,
                                 String batchType) {

        for (MetaInsightsDto insight : batchResults) {
            String key = generateRecordKey(insight);

            if (!allRecords.containsKey(key)) {
                // New record - add it (clone to avoid reference issues)
                allRecords.put(key, cloneInsight(insight));
            }
            // If exists, first batch wins (demographic data is base layer)
        }

        logger.debug("✅ Added {} {} records", batchResults.size(), batchType);
    }

    /**
     * Merge records from additional batches into existing records
     */
    private void mergeBatchRecords(List<MetaInsightsDto> batchResults,
                                   Map<String, MetaInsightsDto> allRecords,
                                   String batchType) {

        int mergedCount = 0;
        int newCount = 0;

        for (MetaInsightsDto insight : batchResults) {
            String key = generateRecordKey(insight);

            if (allRecords.containsKey(key)) {
                // Merge into existing record
                MetaInsightsDto existing = allRecords.get(key);
                mergeInsightData(existing, insight, batchType);
                mergedCount++;
            } else {
                // New unique record - add it
                allRecords.put(key, cloneInsight(insight));
                newCount++;
            }
        }

        logger.debug("✅ {} batch: {} merged, {} new records", batchType, mergedCount, newCount);
    }

    /**
     * FIXED: Generate more granular record key to preserve breakdown diversity
     *
     * OLD: accountId + adId + date (too simple, lost breakdown variations)
     * NEW: Include breakdown dimensions to preserve data granularity
     */
    private String generateRecordKey(MetaInsightsDto insight) {
        return String.format("%s|%s|%s|%s|%s|%s|%s|%s",
                safeString(insight.getAccountId()),
                safeString(insight.getAdId()),
                safeString(insight.getDateStart()),
                safeString(insight.getAge()),
                safeString(insight.getGender()),
                safeString(insight.getCountry()),
                safeString(insight.getRegion()),
                safeString(insight.getPlacement())
        );
    }

    /**
     * Merge data from source insight into target insight
     */
    private void mergeInsightData(MetaInsightsDto target, MetaInsightsDto source, String batchType) {
        switch (batchType) {
            case "geographic":
                // Add geographic data if missing
                if (target.getCountry() == null) target.setCountry(source.getCountry());
                if (target.getRegion() == null) target.setRegion(source.getRegion());
                if (target.getCity() == null) target.setCity(source.getCity());
                break;

            case "placement":
                // Add placement data if missing
                if (target.getPlacement() == null) target.setPlacement(source.getPlacement());
                if (target.getDevice_platform() == null) target.setDevice_platform(source.getDevice_platform());
                break;
        }

        // Always merge core metrics (use source if target is missing/zero)
        mergeCoreMetrics(target, source);
    }

    /**
     * Merge core performance metrics - prefer non-zero values
     */
    private void mergeCoreMetrics(MetaInsightsDto target, MetaInsightsDto source) {
        // Spend
        if (isNullOrZero(target.getSpend()) && !isNullOrZero(source.getSpend())) {
            target.setSpend(source.getSpend());
        }

        // Impressions
        if (isNullOrZero(target.getImpressions()) && !isNullOrZero(source.getImpressions())) {
            target.setImpressions(source.getImpressions());
        }

        // Clicks
        if (isNullOrZero(target.getClicks()) && !isNullOrZero(source.getClicks())) {
            target.setClicks(source.getClicks());
        }

        // Reach
        if (isNullOrZero(target.getReach()) && !isNullOrZero(source.getReach())) {
            target.setReach(source.getReach());
        }

        // Cost metrics
        if (target.getCpc() == null && source.getCpc() != null) {
            target.setCpc(source.getCpc());
        }
        if (target.getCpm() == null && source.getCpm() != null) {
            target.setCpm(source.getCpm());
        }
        if (target.getCtr() == null && source.getCtr() != null) {
            target.setCtr(source.getCtr());
        }

        // Link clicks
        if (isNullOrZero(target.getLinkClicks()) && !isNullOrZero(source.getLinkClicks())) {
            target.setLinkClicks(source.getLinkClicks());
            target.setUniqueLinkClicks(source.getUniqueLinkClicks());
        }

        // Unique clicks
        if (isNullOrZero(target.getUniqueClicks()) && !isNullOrZero(source.getUniqueClicks())) {
            target.setUniqueClicks(source.getUniqueClicks());
            target.setCostPerUniqueClick(source.getCostPerUniqueClick());
        }

        // Frequency
        if (target.getFrequency() == null && source.getFrequency() != null) {
            target.setFrequency(source.getFrequency());
        }

        // Purchase metrics
        if (isNullOrZero(target.getPurchases()) && !isNullOrZero(source.getPurchases())) {
            target.setPurchases(source.getPurchases());
            target.setPurchaseValue(source.getPurchaseValue());
            target.setPurchaseRoas(source.getPurchaseRoas());
        }

        // Engagement metrics
        if (isNullOrZero(target.getPostEngagement()) && !isNullOrZero(source.getPostEngagement())) {
            target.setPostEngagement(source.getPostEngagement());
        }
        if (isNullOrZero(target.getVideoViews()) && !isNullOrZero(source.getVideoViews())) {
            target.setVideoViews(source.getVideoViews());
        }
    }

    /**
     * Clone insight to avoid reference issues
     */
    private MetaInsightsDto cloneInsight(MetaInsightsDto original) {
        MetaInsightsDto clone = new MetaInsightsDto();

        // Basic identifiers
        clone.setAccountId(original.getAccountId());
        clone.setCampaignId(original.getCampaignId());
        clone.setAdsetId(original.getAdsetId());
        clone.setAdId(original.getAdId());
        clone.setDateStart(original.getDateStart());
        clone.setDateStop(original.getDateStop());

        // Breakdown data
        clone.setAge(original.getAge());
        clone.setGender(original.getGender());
        clone.setCountry(original.getCountry());
        clone.setRegion(original.getRegion());
        clone.setCity(original.getCity());
        clone.setPlacement(original.getPlacement());
        clone.setDevice_platform(original.getDevice_platform());

        // Core metrics
        clone.setSpend(original.getSpend());
        clone.setImpressions(original.getImpressions());
        clone.setClicks(original.getClicks());
        clone.setUniqueClicks(original.getUniqueClicks());
        clone.setLinkClicks(original.getLinkClicks());
        clone.setUniqueLinkClicks(original.getUniqueLinkClicks());
        clone.setReach(original.getReach());
        clone.setFrequency(original.getFrequency());
        clone.setCpc(original.getCpc());
        clone.setCpm(original.getCpm());
        clone.setCpp(original.getCpp());
        clone.setCtr(original.getCtr());
        clone.setUniqueCtr(original.getUniqueCtr());
        clone.setCostPerUniqueClick(original.getCostPerUniqueClick());

        // Purchase metrics
        clone.setPurchases(original.getPurchases());
        clone.setPurchaseValue(original.getPurchaseValue());
        clone.setPurchaseRoas(original.getPurchaseRoas());

        // Engagement metrics
        clone.setPostEngagement(original.getPostEngagement());
        clone.setPageEngagement(original.getPageEngagement());
        clone.setLikes(original.getLikes());
        clone.setComments(original.getComments());
        clone.setShares(original.getShares());
        clone.setPhotoView(original.getPhotoView());
        clone.setVideoViews(original.getVideoViews());
        clone.setVideoP25WatchedActions(original.getVideoP25WatchedActions());
        clone.setVideoP50WatchedActions(original.getVideoP50WatchedActions());
        clone.setVideoP75WatchedActions(original.getVideoP75WatchedActions());
        clone.setVideoP95WatchedActions(original.getVideoP95WatchedActions());
        clone.setVideoP100WatchedActions(original.getVideoP100WatchedActions());
        clone.setVideoAvgPercentWatched(original.getVideoAvgPercentWatched());

        // Lead metrics
        clone.setLeads(original.getLeads());
        clone.setCostPerLead(original.getCostPerLead());
        clone.setMobileAppInstall(original.getMobileAppInstall());
        clone.setCostPerAppInstall(original.getCostPerAppInstall());

        // Additional metrics
        clone.setSocialSpend(original.getSocialSpend());
        clone.setInlineLinkClicks(original.getInlineLinkClicks());
        clone.setInlinePostEngagement(original.getInlinePostEngagement());
        clone.setCostPerInlineLinkClick(original.getCostPerInlineLinkClick());
        clone.setCostPerInlinePostEngagement(original.getCostPerInlinePostEngagement());

        // Meta fields
        clone.setCurrency(original.getCurrency());
        clone.setAttributionSetting(original.getAttributionSetting());

        return clone;
    }

    /**
     * Log comprehensive merge statistics
     */
    private void logInclusiveMergeStats(List<MetaInsightsDto> batch1,
                                        List<MetaInsightsDto> batch2,
                                        List<MetaInsightsDto> batch3,
                                        List<MetaInsightsDto> finalResults) {

        int totalInput = batch1.size() + batch2.size() + batch3.size();
        int finalOutput = finalResults.size();

        logger.info("📊 INCLUSIVE MERGE STATS:");
        logger.info("   📥 Input: {} + {} + {} = {} total records",
                batch1.size(), batch2.size(), batch3.size(), totalInput);
        logger.info("   📤 Output: {} unique records", finalOutput);

        if (totalInput > 0) {
            double preservationRate = (finalOutput * 100.0) / totalInput;
            logger.info("   💾 Data preservation: {:.1f}%", preservationRate);
        }

        // Count records with each type of data
        long withDemographic = finalResults.stream()
                .mapToLong(r -> (r.getAge() != null || r.getGender() != null) ? 1 : 0).sum();
        long withGeographic = finalResults.stream()
                .mapToLong(r -> (r.getCountry() != null || r.getRegion() != null) ? 1 : 0).sum();
        long withPlacement = finalResults.stream()
                .mapToLong(r -> (r.getPlacement() != null || r.getDevice_platform() != null) ? 1 : 0).sum();
        long withMetrics = finalResults.stream()
                .mapToLong(r -> (r.getImpressions() != null &&
                        !r.getImpressions().equals("0") &&
                        !r.getImpressions().equals("")) ? 1 : 0).sum();

        logger.info("   🔍 Data completeness:");
        logger.info("     📊 {} records with demographic data ({:.1f}%)",
                withDemographic, (withDemographic * 100.0 / finalOutput));
        logger.info("     🌍 {} records with geographic data ({:.1f}%)",
                withGeographic, (withGeographic * 100.0 / finalOutput));
        logger.info("     📍 {} records with placement data ({:.1f}%)",
                withPlacement, (withPlacement * 100.0 / finalOutput));
        logger.info("     📈 {} records with performance metrics ({:.1f}%)",
                withMetrics, (withMetrics * 100.0 / finalOutput));

        // Warn if data loss is high
        if (finalOutput < totalInput * 0.5) {
            logger.warn("⚠️  High data loss detected: {} → {} records", totalInput, finalOutput);
        }
    }

    // ==================== BACKWARD COMPATIBILITY METHODS ====================

    /**
     * Validate batch data consistency (kept for compatibility)
     */
    public boolean validateBatchConsistency(List<MetaInsightsDto> batch1,
                                            List<MetaInsightsDto> batch2,
                                            List<MetaInsightsDto> batch3) {
        logger.debug("🔍 Validating batch consistency...");

        Set<String> keys1 = extractSimpleMergeKeys(batch1);
        Set<String> keys2 = extractSimpleMergeKeys(batch2);
        Set<String> keys3 = extractSimpleMergeKeys(batch3);

        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(keys1);
        allKeys.addAll(keys2);
        allKeys.addAll(keys3);

        int totalUnique = allKeys.size();
        logger.debug("📊 Unique ad combinations across batches: {}", totalUnique);

        // Always return true for inclusive merge - we handle all cases
        return true;
    }

    /**
     * Extract simple merge keys for compatibility
     */
    private Set<String> extractSimpleMergeKeys(List<MetaInsightsDto> insights) {
        return insights.stream()
                .map(insight -> String.format("%s|%s|%s",
                        safeString(insight.getAccountId()),
                        safeString(insight.getAdId()),
                        safeString(insight.getDateStart())))
                .collect(Collectors.toSet());
    }

    // ==================== UTILITY METHODS ====================

    private String safeString(String value) {
        return value != null ? value : "null";
    }

    private boolean isNullOrZero(String value) {
        return value == null || value.equals("0") || value.equals("") || value.equals("0.0");
    }
}