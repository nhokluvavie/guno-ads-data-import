package com.gunoads.model.dto;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class MetaCampaignDto {

    private String id;

    @JsonProperty("account_id")
    private String accountId;

    private String name;

    private String objective;

    private String status;

    @JsonProperty("configured_status")
    private String configuredStatus;

    @JsonProperty("effective_status")
    private String effectiveStatus;

    @JsonProperty("start_time")
    private String startTime;

    @JsonProperty("stop_time")
    private String stopTime;

    @JsonProperty("created_time")
    private String createdTime;

    @JsonProperty("updated_time")
    private String updatedTime;

    @JsonProperty("buying_type")
    private String buyingType;

    @JsonProperty("daily_budget")
    private String dailyBudget;

    @JsonProperty("lifetime_budget")
    private String lifetimeBudget;

    @JsonProperty("budget_remaining")
    private String budgetRemaining;

    @JsonProperty("bid_strategy")
    private String bidStrategy;

    @JsonProperty("optimization_goal")
    private String optimizationGoal;

    @JsonProperty("special_ad_categories")
    private String specialAdCategories;

    @JsonProperty("attribution_spec")
    private String attributionSpec;

    @JsonProperty("spend_cap")
    private String spendCap;
}