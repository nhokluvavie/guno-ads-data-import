package com.gunoads.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Min;
import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "meta.ads")
@Validated
public class MetaAdsConfig {

    @NotBlank
    private String appId;

    @NotBlank
    private String appSecret;

    @NotBlank
    private String accessToken;

    private String apiVersion = "v19.0";

    private String baseUrl = "https://graph.facebook.com";

    private RateLimit rateLimit = new RateLimit();

    @Data
    public static class RateLimit {
        @Min(1)
        private int requestsPerHour = 200;

        @Min(1)
        private int retryAttempts = 3;

        @Min(1000)
        private long retryDelayMs = 5000;

        @Min(1)
        private int maxConcurrentRequests = 5;

        @Min(1000)
        private long requestTimeoutMs = 30000;
    }

    // Helper methods
    public String getGraphUrl() {
        return baseUrl + "/" + apiVersion;
    }

    public String getAccountsEndpoint() {
        return getGraphUrl() + "/me/adaccounts";
    }

    public String getCampaignsEndpoint(String accountId) {
        return getGraphUrl() + "/act_" + accountId + "/campaigns";
    }

    public String getAdSetsEndpoint(String accountId) {
        return getGraphUrl() + "/act_" + accountId + "/adsets";
    }

    public String getAdsEndpoint(String accountId) {
        return getGraphUrl() + "/act_" + accountId + "/ads";
    }

    public String getInsightsEndpoint(String accountId) {
        return getGraphUrl() + "/act_" + accountId + "/insights";
    }

    public boolean isValidConfiguration() {
        return appId != null && !appId.trim().isEmpty() &&
                appSecret != null && !appSecret.trim().isEmpty() &&
                accessToken != null && !accessToken.trim().isEmpty();
    }
}