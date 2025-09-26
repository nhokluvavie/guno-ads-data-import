package com.gunoads.processor;

import com.gunoads.model.dto.MetaInsightsDto;
import com.gunoads.model.dto.MetaPlacementDto;
import com.gunoads.model.entity.Placement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * PlacementExtractor - Extract placement data from Meta Insights breakdown
 *
 * Purpose:
 * - Extract unique placement combinations from insights data
 * - Create placement entities from insights breakdown (placement + device_platform)
 * - Handle deduplication and data validation
 * - Support bulk placement processing
 */
@Component
public class PlacementExtractor {

    private static final Logger logger = LoggerFactory.getLogger(PlacementExtractor.class);
    private static final String DEFAULT_TIMESTAMP = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

    /**
     * MAIN METHOD: Extract unique placements from insights data
     *
     * @param insights List of insights containing placement breakdown
     * @return Set of unique placement DTOs
     */
    public Set<MetaPlacementDto> extractPlacementsFromInsights(List<MetaInsightsDto> insights) {
        if (insights == null || insights.isEmpty()) {
            logger.warn("No insights data provided for placement extraction");
            return new HashSet<>();
        }

        logger.info("🔄 Extracting placements from {} insights records", insights.size());
        long startTime = System.currentTimeMillis();

        Set<MetaPlacementDto> uniquePlacements = new HashSet<>();
        Map<String, Integer> placementCounts = new HashMap<>();

        // FIXED: Always ensure "unknown" placement exists
        boolean hasUnknownPlacement = false;

        for (MetaInsightsDto insight : insights) {
            try {
                MetaPlacementDto placement = createPlacementFromInsights(insight);
                if (placement != null) {
                    uniquePlacements.add(placement);
                    placementCounts.merge(placement.getId(), 1, Integer::sum);

                    if ("unknown".equals(placement.getId())) {
                        hasUnknownPlacement = true;
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to extract placement from insight: {}", e.getMessage());
            }
        }

        // CRITICAL FIX: Ensure "unknown" placement always exists
        if (!hasUnknownPlacement) {
            MetaPlacementDto unknownPlacement = createUnknownPlacement();
            uniquePlacements.add(unknownPlacement);
            placementCounts.put("unknown", 0);
            logger.info("✅ Added default 'unknown' placement for orphaned records");
        }

        long duration = System.currentTimeMillis() - startTime;
        logger.info("✅ Extracted {} unique placements from {} insights in {}ms",
                uniquePlacements.size(), insights.size(), duration);

        logPlacementStatistics(placementCounts);
        return uniquePlacements;
    }

    /**
     * Create placement DTO from insights breakdown data
     *
     * @param insights Single insights record with placement data
     * @return MetaPlacementDto or null if invalid data
     */
    public MetaPlacementDto createPlacementFromInsights(MetaInsightsDto insights) {
        if (insights == null) return null;

        String placement = insights.getPlacement();
        String devicePlatform = insights.getDevice_platform();
        String adId = insights.getAdId();

        try {
            MetaPlacementDto dto = new MetaPlacementDto();

            // FIXED: Generate placement ID - always create, even for unknown
            String placementId = generatePlacementId(placement, devicePlatform);
            dto.setId(placementId);

            // Set advertisement relationship (null for global placements)
            dto.setAdvertisementId(null); // Global placement approach

            // FIXED: Handle unknown placements gracefully
            if (placement == null || placement.trim().isEmpty()) {
                // Create default "unknown" placement
                dto.setPlacementName("Unknown Placement");
                dto.setPlatform("unknown");
                dto.setPlacementType("unknown");
                dto.setDeviceType(extractDeviceType(devicePlatform)); // Still try to get device
                dto.setPosition("unknown");

                // Default capabilities for unknown placements
                dto.setSupportsVideo(false);
                dto.setSupportsCarousel(false);
                dto.setSupportsCollection(false);
            } else {
                // Normal placement processing
                dto.setPlacementName(extractPlacementName(placement, devicePlatform));
                dto.setPlatform(extractPlatform(placement));
                dto.setPlacementType(extractPlacementType(placement));
                dto.setDeviceType(extractDeviceType(devicePlatform));
                dto.setPosition(extractPosition(placement));

                // Set capabilities based on placement type
                setPlacementCapabilities(dto, placement, devicePlatform);
            }

            // Meta info
            dto.setIsActive(true);
            dto.setCreatedAt(DEFAULT_TIMESTAMP);

            return dto;

        } catch (Exception e) {
            logger.warn("Failed to create placement from insights: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Generate unique placement ID from placement and device combination
     *
     * @param placement Publisher platform (facebook, instagram, etc.)
     * @param devicePlatform Device type (mobile_app, desktop, etc.)
     * @return Unique placement ID
     */
    public String generatePlacementId(String placement, String devicePlatform) {
        // FIXED: Handle null/empty placement
        if (placement == null || placement.trim().isEmpty()) {
            String devicePart = (devicePlatform != null && !devicePlatform.trim().isEmpty())
                    ? devicePlatform.toLowerCase().trim() : "unknown";
            return devicePart.equals("unknown") ? "unknown" : "unknown_" + devicePart;
        }

        String placementPart = placement.toLowerCase().trim();
        String devicePart = (devicePlatform != null && !devicePlatform.trim().isEmpty())
                ? devicePlatform.toLowerCase().trim() : "unknown";

        // Create unique combination
        String combinedId = String.format("%s_%s", placementPart, devicePart);

        // Clean and normalize
        return combinedId.replaceAll("[^a-zA-Z0-9_]", "_")
                .replaceAll("_{2,}", "_")  // Replace multiple underscores
                .toLowerCase();
    }

    private MetaPlacementDto createUnknownPlacement() {
        MetaPlacementDto dto = new MetaPlacementDto();

        dto.setId("unknown");
        dto.setAdvertisementId(null); // Global placement
        dto.setPlacementName("Unknown Placement");
        dto.setPlatform("unknown");
        dto.setPlacementType("unknown");
        dto.setDeviceType("unknown");
        dto.setPosition("unknown");
        dto.setIsActive(true);
        dto.setSupportsVideo(false);
        dto.setSupportsCarousel(false);
        dto.setSupportsCollection(false);
        dto.setCreatedAt(DEFAULT_TIMESTAMP);

        logger.debug("Created default 'unknown' placement");
        return dto;
    }

    /**
     * Deduplicate placements by ID, keeping the most complete record
     *
     * @param placements Set of placement DTOs
     * @return Deduplicated set
     */
    public Set<MetaPlacementDto> deduplicatePlacements(Set<MetaPlacementDto> placements) {
        if (placements == null || placements.size() <= 1) {
            return placements;
        }

        Map<String, MetaPlacementDto> uniquePlacements = new HashMap<>();

        for (MetaPlacementDto placement : placements) {
            String id = placement.getId();

            if (!uniquePlacements.containsKey(id)) {
                uniquePlacements.put(id, placement);
            } else {
                // Keep the more complete record (more non-null fields)
                MetaPlacementDto existing = uniquePlacements.get(id);
                if (isMoreComplete(placement, existing)) {
                    uniquePlacements.put(id, placement);
                }
            }
        }

        logger.info("Deduplication: {} → {} placements", placements.size(), uniquePlacements.size());
        return new HashSet<>(uniquePlacements.values());
    }

    // ==================== PRIVATE HELPER METHODS ====================

    private String extractPlacementName(String placement, String devicePlatform) {
        if (placement == null && devicePlatform == null) return "Unknown Placement";

        String platformName = (placement != null) ? formatPlacementName(placement) : "Unknown";
        String deviceName = (devicePlatform != null) ? formatDeviceName(devicePlatform) : "Unknown";

        return String.format("%s - %s", platformName, deviceName);
    }

    private String extractPlatform(String placement) {
        if (placement == null) return "unknown";

        String lower = placement.toLowerCase();
        if (lower.contains("facebook")) return "facebook";
        if (lower.contains("instagram")) return "instagram";
        if (lower.contains("messenger")) return "messenger";
        if (lower.contains("audience")) return "audience_network";

        return placement.toLowerCase();
    }

    private String extractPlacementType(String placement) {
        if (placement == null) return "unknown";

        String lower = placement.toLowerCase();
        if (lower.contains("feed")) return "feed";
        if (lower.contains("story")) return "story";
        if (lower.contains("reel")) return "reel";
        if (lower.contains("banner")) return "banner";
        if (lower.contains("video")) return "video";

        return "standard";
    }

    private String extractDeviceType(String devicePlatform) {
        if (devicePlatform == null || devicePlatform.trim().isEmpty()) {
            return "unknown";
        }

        String lower = devicePlatform.toLowerCase();
        if (lower.contains("mobile")) return "mobile";
        if (lower.contains("desktop")) return "desktop";
        if (lower.contains("tablet")) return "tablet";

        return devicePlatform.toLowerCase();
    }

    private String extractPosition(String placement) {
        if (placement == null) return "unknown";

        String lower = placement.toLowerCase();
        if (lower.contains("top")) return "top";
        if (lower.contains("bottom")) return "bottom";
        if (lower.contains("side")) return "sidebar";
        if (lower.contains("full")) return "fullscreen";

        return "standard";
    }

    private void setPlacementCapabilities(MetaPlacementDto dto, String placement, String devicePlatform) {
        String placementLower = (placement != null) ? placement.toLowerCase() : "";
        String deviceLower = (devicePlatform != null) ? devicePlatform.toLowerCase() : "";

        // Video support
        boolean supportsVideo = placementLower.contains("video") ||
                placementLower.contains("story") ||
                placementLower.contains("reel");
        dto.setSupportsVideo(supportsVideo);

        // Carousel support (mainly for feed placements)
        boolean supportsCarousel = placementLower.contains("feed") ||
                placementLower.contains("timeline");
        dto.setSupportsCarousel(supportsCarousel);

        // Collection support (mainly for mobile)
        boolean supportsCollection = deviceLower.contains("mobile") &&
                (placementLower.contains("feed") || placementLower.contains("instagram"));
        dto.setSupportsCollection(supportsCollection);
    }

    private String formatPlacementName(String placement) {
        return Arrays.stream(placement.split("_"))
                .map(word -> word.substring(0, 1).toUpperCase() + word.substring(1).toLowerCase())
                .collect(Collectors.joining(" "));
    }

    private String formatDeviceName(String device) {
        return Arrays.stream(device.split("_"))
                .map(word -> word.substring(0, 1).toUpperCase() + word.substring(1).toLowerCase())
                .collect(Collectors.joining(" "));
    }

    private boolean isMoreComplete(MetaPlacementDto placement1, MetaPlacementDto placement2) {
        int score1 = getCompletenessScore(placement1);
        int score2 = getCompletenessScore(placement2);
        return score1 > score2;
    }

    private int getCompletenessScore(MetaPlacementDto placement) {
        int score = 0;
        if (placement.getPlacementName() != null && !placement.getPlacementName().contains("Unknown")) score++;
        if (placement.getPlatform() != null && !placement.getPlatform().equals("unknown")) score++;
        if (placement.getPlacementType() != null && !placement.getPlacementType().equals("unknown")) score++;
        if (placement.getDeviceType() != null && !placement.getDeviceType().equals("unknown")) score++;
        if (placement.getPosition() != null && !placement.getPosition().equals("unknown")) score++;
        return score;
    }

    private void logPlacementStatistics(Map<String, Integer> placementCounts) {
        logger.info("📊 Placement extraction statistics:");
        placementCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry -> logger.info("   {} → {} insights", entry.getKey(), entry.getValue()));
    }
}