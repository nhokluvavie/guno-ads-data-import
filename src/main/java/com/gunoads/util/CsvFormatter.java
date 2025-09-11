package com.gunoads.util;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

public class CsvFormatter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Escape CSV value
     */
    public static String escapeCsv(Object value) {
        if (value == null) {
            return "";
        }

        String str = value.toString();

        // If contains comma, quote, or newline - wrap in quotes and escape quotes
        if (str.contains(",") || str.contains("\"") || str.contains("\n") || str.contains("\r")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }

        return str;
    }

    /**
     * Format string value for CSV
     */
    public static String formatString(String value) {
        return escapeCsv(value);
    }

    /**
     * Format number value for CSV
     */
    public static String formatNumber(Number value) {
        if (value == null) {
            return "";
        }

        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }

        return value.toString();
    }

    /**
     * Format boolean value for CSV
     */
    public static String formatBoolean(Boolean value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }

    /**
     * Format date value for CSV
     */
    public static String formatDate(LocalDateTime value) {
        if (value == null) {
            return "";
        }
        return value.format(DATE_FORMATTER);
    }

    /**
     * Format date string for CSV
     */
    public static String formatDateString(String value) {
        return escapeCsv(value);
    }

    /**
     * Join CSV values
     */
    public static String joinCsv(Object... values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(escapeCsv(values[i]));
        }
        return sb.toString();
    }

    /**
     * Create CSV mapper for AdsReporting
     */
    public static class AdsReportingCsvMapper {

        public static final String CSV_HEADER =
                "account_id,platform_id,campaign_id,adset_id,advertisement_id,placement_id," +
                        "ads_processing_dt,age_group,gender,country_code,region,city," +
                        "spend,revenue,purchase_roas,impressions,clicks,unique_clicks,cost_per_unique_click," +
                        "link_clicks,unique_link_clicks,reach,frequency,cpc,cpm,cpp,ctr,unique_ctr," +
                        "post_engagement,page_engagement,likes,comments,shares,photo_view,video_views," +
                        "video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions," +
                        "video_p95_watched_actions,video_p100_watched_actions,video_avg_percent_watched," +
                        "purchases,purchase_value,leads,cost_per_lead,mobile_app_install,cost_per_app_install," +
                        "social_spend,inline_link_clicks,inline_post_engagement,cost_per_inline_link_click," +
                        "cost_per_inline_post_engagement,currency,attribution_setting,date_start,date_stop," +
                        "created_at,updated_at,country_name";

        // This will be implemented when we have AdsReporting entity
        // public static Function<AdsReporting, String> toCsvRow() { ... }
    }

    /**
     * Create CSV mapper for Account
     */
    public static class AccountCsvMapper {

        public static final String CSV_HEADER =
                "id,platform_id,account_name,currency,timezone_id,timezone_name,account_status," +
                        "disable_reason,bussiness_id,business_name,business_country_code,business_city," +
                        "business_state,business_street,business_zip,is_personal,is_prepay_account," +
                        "is_tax_id_required,capabilities,amount_spent,balance,spend_cap,funding_source," +
                        "account_age,has_page_authorized_ad_account,is_direct_deals_enabled," +
                        "is_notifications_enabled,created_time,last_updated";

        // This will be implemented when we have Account entity
        // public static Function<Account, String> toCsvRow() { ... }
    }

    /**
     * Builder pattern for complex CSV formatting
     */
    public static class CsvRowBuilder {
        private final StringBuilder sb = new StringBuilder();
        private boolean first = true;

        public CsvRowBuilder add(Object value) {
            if (!first) {
                sb.append(",");
            }
            sb.append(escapeCsv(value));
            first = false;
            return this;
        }

        public CsvRowBuilder addString(String value) {
            return add(formatString(value));
        }

        public CsvRowBuilder addNumber(Number value) {
            return add(formatNumber(value));
        }

        public CsvRowBuilder addBoolean(Boolean value) {
            return add(formatBoolean(value));
        }

        public CsvRowBuilder addDate(LocalDateTime value) {
            return add(formatDate(value));
        }

        public CsvRowBuilder addDateString(String value) {
            return add(formatDateString(value));
        }

        public String build() {
            return sb.toString();
        }
    }
}