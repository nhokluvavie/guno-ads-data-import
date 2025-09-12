package com.gunoads.model.dto;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class    MetaAdDto {

    private String id;

    @JsonProperty("adset_id")
    private String adsetId;

    private String name;

    private String status;

    @JsonProperty("configured_status")
    private String configuredStatus;

    @JsonProperty("effective_status")
    private String effectiveStatus;

    private Creative creative;

    @JsonProperty("created_time")
    private String createdTime;

    @JsonProperty("updated_time")
    private String updatedTime;

    @Data
    public static class Creative {
        private String id;
        private String name;
        private String type;

        @JsonProperty("call_to_action_type")
        private String callToActionType;

        @JsonProperty("is_app_install_ad")
        private Boolean isAppInstallAd;

        @JsonProperty("is_dynamic_ad")
        private Boolean isDynamicAd;

        @JsonProperty("is_video_ad")
        private Boolean isVideoAd;

        @JsonProperty("is_carousel_ad")
        private Boolean isCarouselAd;
    }
}