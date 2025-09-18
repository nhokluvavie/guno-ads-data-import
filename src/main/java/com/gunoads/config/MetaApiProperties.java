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

    // Breakdowns for insights
    private List<String> defaultBreakdowns = List.of(
            "age", "gender", "country", "region", "placement", "device_platform"
    );

    // Default date preset for insights
    private String defaultDatePreset = "yesterday";

    // Pagination settings
    private Pagination pagination = new Pagination();

    @Data
    public static class Fields {
        // Account fields - UPDATED for v23.0 (removed deprecated fields)
        private List<String> account = List.of(
                "id", "account_id", "name", "currency", "timezone_id", "timezone_name",
                "account_status", "business", "amount_spent", "balance", "spend_cap"
                // REMOVED: "funding_source", "liable_address", "disable_reason" - deprecated in v23.0
        );

        // Campaign fields - UPDATED for v23.0
        private List<String> campaign = List.of(
                "id", "account_id", "name", "objective", "status", "configured_status",
                "effective_status", "start_time", "stop_time", "created_time",
                "updated_time", "buying_type", "daily_budget", "lifetime_budget",
                "budget_remaining", "bid_strategy", "special_ad_categories"
                // REMOVED: Some deprecated fields in v23.0
        );

        // AdSet fields - UPDATED for v23.0
        private List<String> adset = List.of(
                "id", "campaign_id", "name", "status", "configured_status",
                "effective_status", "lifetime_imps", "start_time", "end_time",
                "created_time", "updated_time", "optimization_goal", "billing_event",
                "bid_amount", "bid_strategy", "daily_budget",
                "lifetime_budget", "budget_remaining", "targeting", "attribution_spec"
                // REMOVED: Some targeting fields deprecated
        );

        // Ad fields - UPDATED for v23.0
        private List<String> ad = List.of(
                "id", "adset_id", "name", "status", "configured_status",
                "effective_status", "creative", "created_time", "updated_time",
                "tracking_specs", "conversion_specs"
                // ADDED: New tracking fields in v23.0
        );

        // Insights fields - UPDATED for v23.0
        private List<String> insights = List.of(
                "campaign_id", "adset_id", "ad_id", "account_id", "spend",
                "impressions", "clicks", "unique_clicks", "link_clicks",
                "unique_link_clicks", "reach", "frequency", "cpc", "cpm",
                "cpp", "ctr", "unique_ctr", "actions", "action_values",
                "conversions", "conversion_values", "cost_per_action_type",
                "video_30_sec_watched_actions", "video_p25_watched_actions",
                "video_p50_watched_actions", "video_p75_watched_actions",
                "video_p95_watched_actions", "video_p100_watched_actions",
                "video_avg_percent_watched_actions", "quality_ranking",
                "engagement_rate_ranking", "conversion_rate_ranking"
                // ADDED: New quality metrics in v23.0
                // REMOVED: Some deprecated video metrics
        );
    }

    @Data
    public static class Pagination {
        private int defaultLimit = 100;
        private int maxLimit = 500;
        private boolean useAfterCursor = true;
    }

    // Helper methods
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

    public String getDefaultBreakdownsString() {
        return String.join(",", defaultBreakdowns);
    }
}