package com.gunoads.model.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdSet {

    @NotBlank
    private String id;

    @NotBlank
    private String campaignId;

    private String adSetName;
    private String adSetStatus;

    @NotNull
    private Long lifetimeImps;

    private String startTime;
    private String endTime;
    private String createdTime;
    private String updatedTime;
    private String lastUpdated;
    private String configuredStatus;
    private String effectiveStatus;
    private String optimizationGoal;
    private String billingEvent;
    private BigDecimal bidAmount;
    private String bidStrategy;

    @NotNull
    private Boolean isAutobid;

    private BigDecimal dailyBudget;
    private BigDecimal lifetimeBudget;
    private BigDecimal budgetRemaining;
    private String targetingSpec;
    private String geoLocations;

    @NotNull
    private Integer ageMin;

    @NotNull
    private Integer ageMax;

    private String genders;
    private String interests;
    private String behaviors;
    private String customAudiences;
    private String lookalikAudiences;
    private String excludedCustomAudiences;
    private String adsetSchedule;
    private String attributionSpec;

    public AdSet(String id, String campaignId) {
        this.id = id;
        this.campaignId = campaignId;
    }
}