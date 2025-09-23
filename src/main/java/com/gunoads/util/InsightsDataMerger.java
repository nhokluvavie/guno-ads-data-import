package com.gunoads.util;

import com.gunoads.model.dto.MetaInsightsDto;
import com.gunoads.model.dto.MetaInsightsBatchDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * InsightsDataMerger - Smart utility for merging 3 breakdown batches into complete insights
 *
 * Purpose:
 * - Merge demographic (age, gender) + geographic (country, region) + placement (platform, device) data
 * - Handle partial data gracefully
 * - Maintain data integrity and provide debugging info
 * - Convert merged data back to standard MetaInsightsDto format
 */
@Component
public class InsightsDataMerger {

    private static final Logger logger = LoggerFactory.getLogger(InsightsDataMerger.class);

    // ==================== MAIN MERGE METHODS ====================

    /**
     * MAIN METHOD: Merge 3 batches of insights data into complete MetaInsightsDto list
     *
     * @param batch1Results Demographic data (age, gender)
     * @param batch2Results Geographic data (country, region)
     * @param batch3Results Placement data (publisher_platform, impression_device)
     * @return List of complete MetaInsightsDto with all breakdown data
     */
    public List<MetaInsightsDto> mergeAllBatches(
            List<MetaInsightsDto> batch1Results,
            List<MetaInsightsDto> batch2Results,
            List<MetaInsightsDto> batch3Results) {

        logger.info("🔄 Starting insights batch merge: batch1={}, batch2={}, batch3={}",
                batch1Results.size(), batch2Results.size(), batch3Results.size());

        long startTime = System.currentTimeMillis();

        // Step 1: Create batch containers with merge keys
        Map<String, MetaInsightsBatchDto> mergedData = new HashMap<>();

        // Step 2: Process each batch
        processBatch1(batch1Results, mergedData);
        processBatch2(batch2Results, mergedData);
        processBatch3(batch3Results, mergedData);

        // Step 3: Convert back to standard DTOs
        List<MetaInsightsDto> finalResults = convertToStandardDtos(mergedData.values());

        long duration = System.currentTimeMillis() - startTime;

        // Step 4: Log merge statistics
        logMergeStatistics(mergedData.values(), duration);

        logger.info("✅ Batch merge completed: {} final insights in {}ms", finalResults.size(), duration);
        return finalResults;
    }

    // ==================== BATCH PROCESSING METHODS ====================

    /**
     * Process Batch 1: Demographic data (age, gender)
     */
    private void processBatch1(List<MetaInsightsDto> batch1Results, Map<String, MetaInsightsBatchDto> mergedData) {
        logger.debug("📊 Processing Batch 1 (Demographic): {} records", batch1Results.size());

        for (MetaInsightsDto insight : batch1Results) {
            String mergeKey = MetaInsightsBatchDto.generateMergeKey(
                    insight.getAccountId(), insight.getAdId(), insight.getDateStart());

            MetaInsightsBatchDto batchDto = mergedData.computeIfAbsent(mergeKey, k -> {
                MetaInsightsBatchDto newDto = createBaseBatchDto(insight);
                newDto.setMergeKey(k);
                return newDto;
            });

            // Set demographic data
            batchDto.setAge(insight.getAge());
            batchDto.setGender(insight.getGender());
            batchDto.markDemographicDataPresent();

            // Set core metrics (will be consistent across batches)
            setCoreMetrics(batchDto, insight);
        }

        logger.debug("✅ Batch 1 processed: {} unique merge keys",
                mergedData.values().stream().mapToInt(dto -> dto.isHasDemographicData() ? 1 : 0).sum());
    }

    /**
     * Process Batch 2: Geographic data (country, region)
     */
    private void processBatch2(List<MetaInsightsDto> batch2Results, Map<String, MetaInsightsBatchDto> mergedData) {
        logger.debug("🌍 Processing Batch 2 (Geographic): {} records", batch2Results.size());

        for (MetaInsightsDto insight : batch2Results) {
            String mergeKey = MetaInsightsBatchDto.generateMergeKey(
                    insight.getAccountId(), insight.getAdId(), insight.getDateStart());

            MetaInsightsBatchDto batchDto = mergedData.computeIfAbsent(mergeKey, k -> {
                MetaInsightsBatchDto newDto = createBaseBatchDto(insight);
                newDto.setMergeKey(k);
                return newDto;
            });

            // Set geographic data
            batchDto.setCountry(insight.getCountry());
            batchDto.setRegion(insight.getRegion());
            batchDto.markGeographicDataPresent();

            // Set core metrics
            setCoreMetrics(batchDto, insight);
        }

        logger.debug("✅ Batch 2 processed: {} unique merge keys",
                mergedData.values().stream().mapToInt(dto -> dto.isHasGeographicData() ? 1 : 0).sum());
    }

    /**
     * Process Batch 3: Placement data (publisher_platform, impression_device)
     */
    private void processBatch3(List<MetaInsightsDto> batch3Results, Map<String, MetaInsightsBatchDto> mergedData) {
        logger.debug("📱 Processing Batch 3 (Placement): {} records", batch3Results.size());

        for (MetaInsightsDto insight : batch3Results) {
            String mergeKey = MetaInsightsBatchDto.generateMergeKey(
                    insight.getAccountId(), insight.getAdId(), insight.getDateStart());

            MetaInsightsBatchDto batchDto = mergedData.computeIfAbsent(mergeKey, k -> {
                MetaInsightsBatchDto newDto = createBaseBatchDto(insight);
                newDto.setMergeKey(k);
                return newDto;
            });

            // Set placement data
            batchDto.setPublisherPlatform(insight.getPlacement());
            batchDto.setImpressionDevice(insight.getDevice_platform());
            batchDto.markPlacementDataPresent();

            // Set core metrics
            setCoreMetrics(batchDto, insight);
        }

        logger.debug("✅ Batch 3 processed: {} unique merge keys",
                mergedData.values().stream().mapToInt(dto -> dto.isHasPlacementData() ? 1 : 0).sum());
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Create base batch DTO from insight data
     */
    private MetaInsightsBatchDto createBaseBatchDto(MetaInsightsDto insight) {
        MetaInsightsBatchDto batchDto = new MetaInsightsBatchDto();

        // Set basic identifiers
        batchDto.setAccountId(insight.getAccountId());
        batchDto.setCampaignId(insight.getCampaignId());
        batchDto.setAdsetId(insight.getAdsetId());
        batchDto.setAdId(insight.getAdId());
        batchDto.setDateStart(insight.getDateStart());
        batchDto.setDateStop(insight.getDateStop());
        batchDto.setCreatedAt(LocalDateTime.now());

        return batchDto;
    }

    /**
     * Set core metrics that should be consistent across batches
     */
    private void setCoreMetrics(MetaInsightsBatchDto batchDto, MetaInsightsDto insight) {
        // Only set if not already set (first batch wins for core metrics)
        if (batchDto.getSpend() == null) {
            batchDto.setSpend(insight.getSpend());
            batchDto.setImpressions(insight.getImpressions());
            batchDto.setClicks(insight.getClicks());
            batchDto.setUniqueClicks(insight.getUniqueClicks());
            batchDto.setLinkClicks(insight.getLinkClicks());
            batchDto.setUniqueLinkClicks(insight.getUniqueLinkClicks());
            batchDto.setReach(insight.getReach());
            batchDto.setFrequency(insight.getFrequency());
            batchDto.setCpc(insight.getCpc());
            batchDto.setCpm(insight.getCpm());
            batchDto.setCpp(insight.getCpp());
            batchDto.setCtr(insight.getCtr());
            batchDto.setUniqueCtr(insight.getUniqueCtr());
            batchDto.setCostPerUniqueClick(insight.getCostPerUniqueClick());

            // Conversion metrics
            batchDto.setPurchases(insight.getPurchases());
            batchDto.setPurchaseValue(insight.getPurchaseValue());
            batchDto.setPurchaseRoas(insight.getPurchaseRoas());
            batchDto.setLeads(insight.getLeads());
            batchDto.setCostPerLead(insight.getCostPerLead());

            // Engagement metrics
            batchDto.setPostEngagement(insight.getPostEngagement());
            batchDto.setPageEngagement(insight.getPageEngagement());
            batchDto.setLikes(insight.getLikes());
            batchDto.setComments(insight.getComments());
            batchDto.setShares(insight.getShares());
            batchDto.setVideoViews(insight.getVideoViews());
        }
    }

    /**
     * Convert merged batch DTOs back to standard MetaInsightsDto list
     */
    private List<MetaInsightsDto> convertToStandardDtos(Collection<MetaInsightsBatchDto> batchDtos) {
        logger.debug("🔄 Converting {} merged batches to standard DTOs", batchDtos.size());

        List<MetaInsightsDto> standardDtos = batchDtos.stream()
                .map(MetaInsightsBatchDto::toStandardDto)
                .collect(Collectors.toList());

        logger.debug("✅ Converted {} batch DTOs to standard format", standardDtos.size());
        return standardDtos;
    }

    /**
     * Log detailed merge statistics for debugging
     */
    private void logMergeStatistics(Collection<MetaInsightsBatchDto> mergedData, long durationMs) {
        int totalRecords = mergedData.size();
        int completeRecords = (int) mergedData.stream().mapToLong(dto -> dto.isComplete() ? 1 : 0).sum();
        int partialRecords = totalRecords - completeRecords;

        long demographicCount = mergedData.stream().mapToLong(dto -> dto.isHasDemographicData() ? 1 : 0).sum();
        long geographicCount = mergedData.stream().mapToLong(dto -> dto.isHasGeographicData() ? 1 : 0).sum();
        long placementCount = mergedData.stream().mapToLong(dto -> dto.isHasPlacementData() ? 1 : 0).sum();

        logger.info("📊 MERGE STATISTICS:");
        logger.info("   📈 Total records: {}", totalRecords);
        logger.info("   ✅ Complete records (3 batches): {} ({:.1f}%)",
                completeRecords, (completeRecords * 100.0 / totalRecords));
        logger.info("   ⚠️  Partial records: {} ({:.1f}%)",
                partialRecords, (partialRecords * 100.0 / totalRecords));
        logger.info("   👥 Demographic data: {} ({:.1f}%)",
                demographicCount, (demographicCount * 100.0 / totalRecords));
        logger.info("   🌍 Geographic data: {} ({:.1f}%)",
                geographicCount, (geographicCount * 100.0 / totalRecords));
        logger.info("   📱 Placement data: {} ({:.1f}%)",
                placementCount, (placementCount * 100.0 / totalRecords));
        logger.info("   ⏱️  Merge duration: {}ms ({:.2f}s)", durationMs, durationMs / 1000.0);

        // Log warnings for partial records
        if (partialRecords > 0) {
            logger.warn("⚠️  {} records have incomplete data - check API response consistency", partialRecords);

            // Sample some partial records for debugging
            mergedData.stream()
                    .filter(dto -> !dto.isComplete())
                    .limit(3)
                    .forEach(dto -> logger.warn("   Partial: {} (missing: {})",
                            dto.getMergeKey(), dto.getMissingBatches()));
        }
    }

    // ==================== VALIDATION METHODS ====================

    /**
     * Validate batch data consistency
     */
    public boolean validateBatchConsistency(List<MetaInsightsDto> batch1, List<MetaInsightsDto> batch2, List<MetaInsightsDto> batch3) {
        logger.debug("🔍 Validating batch consistency...");

        Set<String> keys1 = extractMergeKeys(batch1);
        Set<String> keys2 = extractMergeKeys(batch2);
        Set<String> keys3 = extractMergeKeys(batch3);

        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(keys1);
        allKeys.addAll(keys2);
        allKeys.addAll(keys3);

        int totalUnique = allKeys.size();
        int intersectionSize = keys1.size() + keys2.size() + keys3.size() - totalUnique;

        logger.debug("📊 Batch overlap: {} total unique, {} common records", totalUnique, intersectionSize);

        if (intersectionSize == 0) {
            logger.warn("⚠️  No common records between batches - may indicate API data issues");
            return false;
        }

        return true;
    }

    /**
     * Extract merge keys from insights list
     */
    private Set<String> extractMergeKeys(List<MetaInsightsDto> insights) {
        return insights.stream()
                .map(insight -> MetaInsightsBatchDto.generateMergeKey(
                        insight.getAccountId(), insight.getAdId(), insight.getDateStart()))
                .collect(Collectors.toSet());
    }

    // ==================== ERROR HANDLING ====================

    /**
     * Handle merge errors gracefully
     */
    public List<MetaInsightsDto> mergeWithFallback(
            List<MetaInsightsDto> batch1Results,
            List<MetaInsightsDto> batch2Results,
            List<MetaInsightsDto> batch3Results) {

        try {
            return mergeAllBatches(batch1Results, batch2Results, batch3Results);
        } catch (Exception e) {
            logger.error("❌ Batch merge failed, using fallback strategy: {}", e.getMessage());

            // Fallback: Return largest batch with warning
            List<MetaInsightsDto> largest = getLargestBatch(batch1Results, batch2Results, batch3Results);
            logger.warn("⚠️  Using fallback: returning {} records from largest batch", largest.size());
            return largest;
        }
    }

    /**
     * Get largest batch as fallback
     */
    private List<MetaInsightsDto> getLargestBatch(List<MetaInsightsDto>... batches) {
        return Arrays.stream(batches)
                .max(Comparator.comparing(List::size))
                .orElse(new ArrayList<>());
    }
}