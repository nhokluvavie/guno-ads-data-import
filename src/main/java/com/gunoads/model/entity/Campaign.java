package com.gunoads.model.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Campaign {

    @NotBlank
    private String id;

    private String accountId;
    private String platformId;
    private String campaignName;
    private String camObjective;
    private String startTime;
    private String stopTime;
    private String createdTime;
    private String updatedTime;
    private String lastUpdated;
    private String camStatus;
    private String configuredStatus;
    private String effectiveStatus;
    private String buyingType;
    private BigDecimal dailyBudget;
    private BigDecimal lifetimeBudget;
    private BigDecimal budgetRemaining;
    private String bidStrategy;
    private String camOptimizationType;
    private String specialAdCategories;
    private String attributionSpec;
    private BigDecimal spendCap;

    public Campaign(String id) {
        this.id = id;
    }
}