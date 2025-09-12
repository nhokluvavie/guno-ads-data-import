package com.gunoads.model.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Placement {

    @NotBlank
    private String id;

    @NotBlank
    private String advertisementId;

    private String placementName;
    private String platform;
    private String placementType;
    private String deviceType;
    private String position;

    @NotNull
    private Boolean isActive;

    @NotNull
    private Boolean supportsVideo;

    @NotNull
    private Boolean supportsCarousel;

    @NotNull
    private Boolean supportsCollection;

    private String createdAt;

    public Placement(String id, String advertisementId) {
        this.id = id;
        this.advertisementId = advertisementId;
    }
}