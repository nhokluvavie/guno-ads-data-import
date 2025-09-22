package com.gunoads.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "meta.api")
public class MetaApiProperties {

    // API Field configurations
    private Fields fields = new Fields();

    // UPDATED: Breakdowns for insights (v23.0 compatible)
    private List<String> defaultBreakdowns = List.of(
            "age", "gender", "country", "region",
            "device_platform", "impression_device", "publisher_platform"
    );

    // Default date preset for insights
    private String defaultDatePreset = "yesterday";

    // Pagination settings (enhanced for auto-pagination)
    private Pagination pagination = new Pagination();

    @Data
    public static class Fields {
        // SAFE: Account fields for v23.0 (only verified working fields)
        private List<String> account = List.of(
                "id", "account_id", "name", "currency", "timezone_id", "timezone_name",
                "account_status", "business", "amount_spent", "balance", "spend_cap"
                // REMOVED: All potentially problematic fields for v23.0 compatibility
        );

        // SAFE: Campaign fields for v23.0 (only verified working fields)
        private List<String> campaign = List.of(
                "id", "account_id", "name", "objective", "status", "configured_status",
                "effective_status", "start_time", "stop_time", "created_time",
                "updated_time", "buying_type", "daily_budget", "lifetime_budget",
                "budget_remaining", "bid_strategy"
                // REMOVED: potentially problematic fields for stability
        );

        // SAFE: AdSet fields for v23.0 (only verified working fields)
        private List<String> adset = List.of(
                "id", "campaign_id", "name", "status", "configured_status",
                "effective_status", "start_time", "end_time", "created_time",
                "updated_time", "optimization_goal", "billing_event",
                "bid_amount", "bid_strategy", "daily_budget", "lifetime_budget",
                "budget_remaining", "targeting"
                // REMOVED: potentially problematic fields for stability
        );

        // SAFE: Ad fields for v23.0 (only verified working fields)
        private List<String> ad = List.of(
                "id", "adset_id", "campaign_id", "name", "status", "configured_status",
                "effective_status", "creative", "created_time", "updated_time"
                // REMOVED: potentially problematic fields for stability
        );

        // SAFE: Insights fields for v23.0 (core metrics only)
        private List<String> insights = List.of(
                // Basic identifiers
                "campaign_id", "adset_id", "ad_id", "account_id",

                // Core performance metrics
                "spend", "impressions", "clicks", "unique_clicks", "link_clicks",
                "unique_link_clicks", "reach", "frequency",

                // Cost metrics
                "cpc", "cpm", "cpp", "ctr", "unique_ctr",

                // Basic engagement metrics
                "actions", "action_values", "conversions", "conversion_values"

                // REMOVED: All potentially problematic v23.0 fields for stability
        );
    }

    @Data
    public static class Pagination {
        private int defaultLimit = 100;      // Increased for better throughput
        private long maxLimit = 500;          // SDK v23.0 supports up to 500
        private boolean useAfterCursor = true;
        private boolean autoFetchAll = true;  // NEW: Enable auto-pagination by default
        private int maxPages = 100;          // NEW: Safety limit for auto-pagination
        private long pageDelayMs = 100;      // NEW: Delay between pages to respect rate limits
    }

    // UPDATED: Helper methods for v23.0
    public String getAccountFieldsString() {
        return String.join(",", fields.account);
    }

    public String getCampaignFieldsString() {
        return String.join(",", fields.campaign);
    }

    public String getAdSetFieldsString() {
        return String.join(",", fields.adset);
    }

    public String getAdFieldsString() {
        return String.join(",", fields.ad);
    }

    public String getInsightsFieldsString() {
        return String.join(",", fields.insights);
    }

    // NEW: Return breakdowns as List<String> for v23.0 SDK (PRIORITY METHOD)
    public List<String> getDefaultBreakdowns() {
        return defaultBreakdowns;
    }

    // KEPT: String version for backward compatibility
    public String getDefaultBreakdownsString() {
        return String.join(",", defaultBreakdowns);
    }

    // NEW: Enhanced breakdown configurations for different use cases
    public List<String> getDemographicBreakdowns() {
        return List.of("age", "gender");
    }

    public List<String> getGeographicBreakdowns() {
        return List.of("country", "region", "dma");
    }

    public List<String> getPlacementBreakdowns() {
        return List.of("placement", "publisher_platform", "platform_position", "impression_device");
    }

    public List<String> getTimeBreakdowns() {
        return List.of("hourly_stats_aggregated_by_advertiser_time_zone", "hourly_stats_aggregated_by_audience_time_zone");
    }

    // NEW: Validation methods
    public boolean isValidFieldConfiguration() {
        return fields.account != null && !fields.account.isEmpty() &&
                fields.campaign != null && !fields.campaign.isEmpty() &&
                fields.adset != null && !fields.adset.isEmpty() &&
                fields.ad != null && !fields.ad.isEmpty() &&
                fields.insights != null && !fields.insights.isEmpty();
    }

    public boolean isValidBreakdownConfiguration() {
        return defaultBreakdowns != null && !defaultBreakdowns.isEmpty();
    }
}