package com.gunoads.model.dto;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class MetaAdSetDto {

    private String id;

    @JsonProperty("campaign_id")
    private String campaignId;

    private String name;

    private String status;

    @JsonProperty("configured_status")
    private String configuredStatus;

    @JsonProperty("effective_status")
    private String effectiveStatus;

    @JsonProperty("lifetime_imps")
    private Long lifetimeImps;  // Added missing field

    @JsonProperty("start_time")
    private String startTime;

    @JsonProperty("end_time")
    private String endTime;

    @JsonProperty("created_time")
    private String createdTime;

    @JsonProperty("updated_time")
    private String updatedTime;

    @JsonProperty("optimization_goal")
    private String optimizationGoal;

    @JsonProperty("billing_event")
    private String billingEvent;

    @JsonProperty("bid_amount")
    private Long bidAmount;

    @JsonProperty("bid_strategy")
    private String bidStrategy;

    @JsonProperty("is_autobid")
    private Boolean isAutobid;  // Added missing field

    @JsonProperty("daily_budget")
    private String dailyBudget;

    @JsonProperty("lifetime_budget")
    private String lifetimeBudget;

    @JsonProperty("budget_remaining")
    private String budgetRemaining;

    private String targeting;

    @JsonProperty("geo_locations")
    private String geoLocations;

    @JsonProperty("age_min")
    private Integer ageMin;

    @JsonProperty("age_max")
    private Integer ageMax;

    private String genders;
    private String interests;
    private String behaviors;

    @JsonProperty("custom_audiences")
    private String customAudiences;

    @JsonProperty("lookalike_audiences")
    private String lookalikAudiences;

    @JsonProperty("excluded_custom_audiences")
    private String excludedCustomAudiences;

    @JsonProperty("adset_schedule")
    private String adsetSchedule;

    @JsonProperty("attribution_spec")
    private String attributionSpec;
}