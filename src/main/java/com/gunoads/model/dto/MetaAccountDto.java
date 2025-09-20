package com.gunoads.model.dto;

import lombok.Data;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

@Data
public class MetaAccountDto {

    private String id;

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("name")
    private String accountName;

    private String currency;

    @JsonProperty("timezone_id")
    private Long timezoneId;

    @JsonProperty("timezone_name")
    private String timezoneName;

    @JsonProperty("account_status")
    private String accountStatus;

    @JsonProperty("disable_reason")
    private String disableReason;

    @JsonProperty("business_id")
    private String businessId;

    @JsonProperty("business_name")
    private String businessName;

    @JsonProperty("business_country_code")
    private Integer businessCountryCode;  // Fixed: Integer not String

    @JsonProperty("business_city")
    private String businessCity;

    @JsonProperty("business_state")
    private String businessState;

    @JsonProperty("business_street")
    private String businessStreet;

    @JsonProperty("business_zip")
    private Integer businessZip;  // Fixed: Integer not String

    @JsonProperty("is_personal")
    private Boolean isPersonal;

    @JsonProperty("is_prepay_account")
    private Boolean isPrepayAccount;

    @JsonProperty("is_tax_id_required")
    private Boolean isTaxIdRequired;

    private String capabilities;

    @JsonProperty("amount_spent")
    private String amountSpent;

    private String balance;

    @JsonProperty("spend_cap")
    private String spendCap;

    @JsonProperty("funding_source")
    private String fundingSource;

    @JsonProperty("account_age")
    private Integer accountAge;  // Added missing field

    @JsonProperty("has_page_authorized_adaccount")
    private Boolean hasPageAuthorizedAdAccount;

    @JsonProperty("is_direct_deals_enabled")
    private Boolean isDirectDealsEnabled;

    @JsonProperty("is_notifications_enabled")
    private Boolean isNotificationsEnabled;

    @JsonProperty("created_time")
    private String createdTime;

    @JsonProperty("updated_time")
    private String updatedTime;
}