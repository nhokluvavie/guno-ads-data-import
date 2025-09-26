package com.gunoads.model.dto;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class MetaPlacementDto {

    private String id;

    @JsonProperty("advertisement_id")
    private String advertisementId;

    @JsonProperty("placement_name")
    private String placementName;

    private String platform;

    @JsonProperty("placement_type")
    private String placementType;

    @JsonProperty("device_type")
    private String deviceType;

    private String position;

    @JsonProperty("is_active")
    private Boolean isActive;

    @JsonProperty("supports_video")
    private Boolean supportsVideo;

    @JsonProperty("supports_carousel")
    private Boolean supportsCarousel;

    @JsonProperty("supports_collection")
    private Boolean supportsCollection;

    @JsonProperty("created_at")
    private String createdAt;

    // Helper method to extract placement from insights
    public static String extractPlacementId(String placement, String devicePlatform) {
        if (placement == null) return "unknown";

        // Create unique placement ID from placement + device
        String device = devicePlatform != null ? devicePlatform : "unknown";
        return String.format("%s_%s_%s", placement.toLowerCase(), device.toLowerCase())
                .replaceAll("[^a-zA-Z0-9_]", "_");
    }
}