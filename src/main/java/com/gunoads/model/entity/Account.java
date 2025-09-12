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
public class Account {

    @NotBlank
    private String id;

    @NotBlank
    private String platformId;

    private String accountName;
    private String currency;

    @NotNull
    private Integer timezoneId;

    private String timezoneName;
    private String accountStatus;
    private String disableReason;
    private String bussinessId;
    private String businessName;

    @NotNull
    private Integer businessCountryCode;

    private String businessCity;
    private String businessState;
    private String businessStreet;

    @NotNull
    private Integer businessZip;

    @NotNull
    private Boolean isPersonal;

    @NotNull
    private Boolean isPrepayAccount;

    @NotNull
    private Boolean isTaxIdRequired;

    private String capabilities;
    private BigDecimal amountSpent;
    private BigDecimal balance;
    private BigDecimal spendCap;
    private String fundingSource;

    @NotNull
    private Integer accountAge;

    @NotNull
    private Boolean hasPageAuthorizedAdAccount;

    @NotNull
    private Boolean isDirectDealsEnabled;

    @NotNull
    private Boolean isNotificationsEnabled;

    private String createdTime;
    private String lastUpdated;

    public Account(String id, String platformId) {
        this.id = id;
        this.platformId = platformId;
    }
}