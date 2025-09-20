package com.gunoads.model.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Advertisement {

    @NotBlank
    private String id;

    @NotBlank
    private String adsetid;

    private String adName;
    private String adStatus;
    private String configuredStatus;
    private String effectiveStatus;
    private String creativeId = "unknown";
    private String creativeName = "unknown";
    private String creativeType = "unknown";
    private String callToActionType;

    @NotNull
    private Boolean isAppInstallAd;

    @NotNull
    private Boolean isDynamicAd;

    @NotNull
    private Boolean isVideoAd;

    @NotNull
    private Boolean isCarouselAd;

    private String createdTime;
    private String updatedTime;
    private String lastUpdated;

    public Advertisement(String id, String adsetid) {
        this.id = id;
        this.adsetid = adsetid;
    }
}