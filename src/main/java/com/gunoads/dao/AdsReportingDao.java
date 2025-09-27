package com.gunoads.dao;

import com.gunoads.model.entity.AdsReporting;
import com.gunoads.processor.DataIngestionProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * AdsReportingDao - Data Access Object for tbl_ads_reporting
 *
 * Handles CRUD operations with support for:
 * - Composite Primary Key (12 fields)
 * - UPSERT operations using PostgreSQL ON CONFLICT
 * - Bulk insert/update with COPY FROM
 * - Data integrity validation
 */
@Repository
public class AdsReportingDao extends StandardDao<AdsReporting, String> {

    private static final Logger logger = LoggerFactory.getLogger(AdsReportingDao.class);

    @Autowired
    private DataIngestionProcessor dataIngestionProcessor;

    /**
     * Composite Primary Key columns for tbl_ads_reporting (12 fields)
     * Based on schema: PRIMARY KEY (account_id, platform_id, campaign_id, adset_id,
     *                              advertisement_id, placement_id, ads_processing_dt,
     *                              age_group, gender, country_code, region, city)
     */
    public static final String[] PRIMARY_KEY_COLUMNS = {
            "account_id", "platform_id", "campaign_id", "adset_id",
            "advertisement_id", "placement_id", "ads_processing_dt",
            "age_group", "gender", "country_code", "region", "city"
    };

    /**
     * Columns that should be updated during UPSERT (all metrics + metadata)
     */
    public static final String[] UPDATE_COLUMNS = {
            "spend", "revenue", "purchase_roas", "impressions", "clicks",
            "unique_clicks", "cost_per_unique_click", "link_clicks", "unique_link_clicks",
            "reach", "frequency", "cpc", "cpm", "cpp", "ctr", "unique_ctr",
            "post_engagement", "page_engagement", "likes", "comments", "shares",
            "photo_view", "video_views", "video_p25_watched_actions", "video_p50_watched_actions",
            "video_p75_watched_actions", "video_p95_watched_actions", "video_p100_watched_actions",
            "video_avg_percent_watched", "purchases", "purchase_value", "leads", "cost_per_lead",
            "mobile_app_install", "cost_per_app_install", "social_spend", "inline_link_clicks",
            "inline_post_engagement", "cost_per_inline_link_click", "cost_per_inline_post_engagement",
            "currency", "attribution_setting", "date_start", "date_stop", "updated_at", "country_name"
    };

    // ==================== STANDARD DAO OVERRIDES ====================

    @Override
    public String getTableName() {
        return "tbl_ads_reporting";
    }

    @Override
    public String getIdColumnName() {
        return "account_id"; // Note: This is composite key, using first column for compatibility
    }

    @Override
    public RowMapper<AdsReporting> getRowMapper() {
        return new AdsReportingRowMapper();
    }

    @Override
    public String buildInsertSql() {
        return """
            INSERT INTO tbl_ads_reporting (
                account_id, platform_id, campaign_id, adset_id, advertisement_id, placement_id,
                ads_processing_dt, age_group, gender, country_code, region, city,
                spend, revenue, purchase_roas, impressions, clicks, unique_clicks, cost_per_unique_click,
                link_clicks, unique_link_clicks, reach, frequency, cpc, cpm, cpp, ctr, unique_ctr,
                post_engagement, page_engagement, likes, comments, shares, photo_view, video_views,
                video_p25_watched_actions, video_p50_watched_actions, video_p75_watched_actions,
                video_p95_watched_actions, video_p100_watched_actions, video_avg_percent_watched,
                purchases, purchase_value, leads, cost_per_lead, mobile_app_install, cost_per_app_install,
                social_spend, inline_link_clicks, inline_post_engagement, cost_per_inline_link_click,
                cost_per_inline_post_engagement, currency, attribution_setting, date_start, date_stop,
                created_at, updated_at, country_name
            ) VALUES (
                :accountId, :platformId, :campaignId, :adsetId, :advertisementId, :placementId,
                :adsProcessingDt, :ageGroup, :gender, :countryCode, :region, :city,
                :spend, :revenue, :purchaseRoas, :impressions, :clicks, :uniqueClicks, :costPerUniqueClick,
                :linkClicks, :uniqueLinkClicks, :reach, :frequency, :cpc, :cpm, :cpp, :ctr, :uniqueCtr,
                :postEngagement, :pageEngagement, :likes, :comments, :shares, :photoView, :videoViews,
                :videoP25WatchedActions, :videoP50WatchedActions, :videoP75WatchedActions,
                :videoP95WatchedActions, :videoP100WatchedActions, :videoAvgPercentWatched,
                :purchases, :purchaseValue, :leads, :costPerLead, :mobileAppInstall, :costPerAppInstall,
                :socialSpend, :inlineLinkClicks, :inlinePostEngagement, :costPerInlineLinkClick,
                :costPerInlinePostEngagement, :currency, :attributionSetting, :dateStart, :dateStop,
                :createdAt, :updatedAt, :countryName
            )
            """;
    }

    @Override
    protected String buildUpdateSql() {
        return """
            UPDATE tbl_ads_reporting SET
                spend = :spend, revenue = :revenue, purchase_roas = :purchaseRoas, 
                impressions = :impressions, clicks = :clicks, unique_clicks = :uniqueClicks,
                cost_per_unique_click = :costPerUniqueClick, link_clicks = :linkClicks, 
                unique_link_clicks = :uniqueLinkClicks, reach = :reach, frequency = :frequency,
                cpc = :cpc, cpm = :cpm, cpp = :cpp, ctr = :ctr, unique_ctr = :uniqueCtr,
                post_engagement = :postEngagement, page_engagement = :pageEngagement,
                likes = :likes, comments = :comments, shares = :shares, photo_view = :photoView,
                video_views = :videoViews, video_p25_watched_actions = :videoP25WatchedActions,
                video_p50_watched_actions = :videoP50WatchedActions, video_p75_watched_actions = :videoP75WatchedActions,
                video_p95_watched_actions = :videoP95WatchedActions, video_p100_watched_actions = :videoP100WatchedActions,
                video_avg_percent_watched = :videoAvgPercentWatched, purchases = :purchases,
                purchase_value = :purchaseValue, leads = :leads, cost_per_lead = :costPerLead,
                mobile_app_install = :mobileAppInstall, cost_per_app_install = :costPerAppInstall,
                social_spend = :socialSpend, inline_link_clicks = :inlineLinkClicks,
                inline_post_engagement = :inlinePostEngagement, cost_per_inline_link_click = :costPerInlineLinkClick,
                cost_per_inline_post_engagement = :costPerInlinePostEngagement, currency = :currency,
                attribution_setting = :attributionSetting, date_start = :dateStart, date_stop = :dateStop,
                updated_at = :updatedAt, country_name = :countryName
            WHERE account_id = :accountId AND platform_id = :platformId AND campaign_id = :campaignId 
            AND adset_id = :adsetId AND advertisement_id = :advertisementId AND placement_id = :placementId
            AND ads_processing_dt = :adsProcessingDt AND age_group = :ageGroup AND gender = :gender
            AND country_code = :countryCode AND region = :region AND city = :city
            """;
    }

    @Override
    public SqlParameterSource getInsertParameters(AdsReporting reporting) {
        return createParameterSource(reporting);
    }

    @Override
    protected SqlParameterSource getUpdateParameters(AdsReporting reporting) {
        return createParameterSource(reporting);
    }

    // ==================== COMPOSITE KEY METHODS ====================

    /**
     * Check if record exists by composite key
     * This is more accurate than the inherited existsById method
     */
    public boolean existsByCompositeKey(AdsReporting reporting) {
        String sql = """
            SELECT COUNT(*) FROM tbl_ads_reporting 
            WHERE account_id = ? AND platform_id = ? AND campaign_id = ? 
            AND adset_id = ? AND advertisement_id = ? AND placement_id = ?
            AND ads_processing_dt = ? AND age_group = ? AND gender = ?
            AND country_code = ? AND region = ? AND city = ?
            """;

        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class,
                    reporting.getAccountId(),
                    reporting.getPlatformId(),
                    reporting.getCampaignId(),
                    reporting.getAdsetId(),
                    reporting.getAdvertisementId(),
                    reporting.getPlacementId(),
                    reporting.getAdsProcessingDt(),
                    reporting.getAgeGroup(),
                    reporting.getGender(),
                    reporting.getCountryCode(),
                    reporting.getRegion(),
                    reporting.getCity()
            );

            return count != null && count > 0;

        } catch (Exception e) {
            logger.error("Error checking composite key existence: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Generate composite key string for debugging/logging
     */
    public String generateCompositeKeyString(AdsReporting reporting) {
        return String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%s|%s",
                safeString(reporting.getAccountId()),
                safeString(reporting.getPlatformId()),
                safeString(reporting.getCampaignId()),
                safeString(reporting.getAdsetId()),
                safeString(reporting.getAdvertisementId()),
                safeString(reporting.getPlacementId()),
                safeString(reporting.getAdsProcessingDt()),
                safeString(reporting.getAgeGroup()),
                safeString(reporting.getGender()),
                reporting.getCountryCode() != null ? reporting.getCountryCode() : 0,
                safeString(reporting.getRegion()),
                safeString(reporting.getCity())
        );
    }

    // ==================== UPSERT METHODS ====================

    /**
     * UPSERT batch of AdsReporting records using PostgreSQL ON CONFLICT
     * This handles the composite primary key properly and ensures data integrity
     */
    @Transactional
    public int upsertBatch(List<AdsReporting> reportingList) {
        if (reportingList == null || reportingList.isEmpty()) {
            logger.debug("Empty reporting list provided for upsert");
            return 0;
        }

        logger.info("🔄 Starting UPSERT operation for {} AdsReporting records", reportingList.size());

        try {
            // Use the data ingestion processor with UPSERT capability
            DataIngestionProcessor.ProcessingResult result = dataIngestionProcessor.processWithUpsert(
                    getTableName(),
                    reportingList,
                    AdsReportingDao.getCsvMapper(),
                    AdsReportingDao.getCsvHeader(),
                    PRIMARY_KEY_COLUMNS,
                    UPDATE_COLUMNS
            );

            logger.info("✅ UPSERT completed: {} records processed in {}ms",
                    result.recordsProcessed, result.durationMs);

            return (int) result.recordsProcessed;

        } catch (Exception e) {
            logger.error("❌ UPSERT operation failed: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to upsert AdsReporting batch", e);
        }
    }

//    /**
//     * Manual UPSERT for single record (fallback method)
//     */
//    @Transactional
//    public boolean upsert(AdsReporting reporting) {
//        try {
//            if (existsByCompositeKey(reporting)) {
//                return update(reporting) > 0;
//            } else {
//                return insert(reporting) > 0;
//            }
//        } catch (Exception e) {
//            logger.error("Failed to upsert single AdsReporting record: {}", e.getMessage());
//            return false;
//        }
//    }

    // ==================== DUPLICATE ANALYSIS METHODS ====================

    /**
     * Get count of records by composite key for debugging
     */
    public Map<String, Integer> getCompositeKeyDuplicateStats(List<AdsReporting> reportingList) {
        Map<String, Integer> stats = new HashMap<>();
        Map<String, Integer> keyCount = new HashMap<>();

        for (AdsReporting reporting : reportingList) {
            String compositeKey = generateCompositeKeyString(reporting);
            keyCount.merge(compositeKey, 1, Integer::sum);
        }

        int duplicateGroups = 0;
        int totalDuplicates = 0;

        for (Map.Entry<String, Integer> entry : keyCount.entrySet()) {
            if (entry.getValue() > 1) {
                duplicateGroups++;
                totalDuplicates += entry.getValue() - 1;
            }
        }

        stats.put("totalRecords", reportingList.size());
        stats.put("uniqueKeys", keyCount.size());
        stats.put("duplicateGroups", duplicateGroups);
        stats.put("totalDuplicates", totalDuplicates);

        return stats;
    }

    /**
     * Find records with duplicate composite keys
     */
    public List<AdsReporting> findDuplicatesByCompositeKey(String accountId, String processingDate) {
        String sql = """
            SELECT * FROM tbl_ads_reporting r1
            WHERE r1.account_id = ? AND r1.ads_processing_dt = ?
            AND EXISTS (
                SELECT 1 FROM tbl_ads_reporting r2 
                WHERE r2.account_id = r1.account_id 
                AND r2.platform_id = r1.platform_id
                AND r2.campaign_id = r1.campaign_id
                AND r2.adset_id = r1.adset_id
                AND r2.advertisement_id = r1.advertisement_id
                AND r2.placement_id = r1.placement_id
                AND r2.ads_processing_dt = r1.ads_processing_dt
                AND r2.age_group = r1.age_group
                AND r2.gender = r1.gender
                AND r2.country_code = r1.country_code
                AND r2.region = r1.region
                AND r2.city = r1.city
                AND r2.created_at != r1.created_at
            )
            ORDER BY r1.account_id, r1.campaign_id, r1.created_at
            """;

        return jdbcTemplate.query(sql, getRowMapper(), accountId, processingDate);
    }

    // ==================== CSV OPERATIONS FOR BULK INSERT ====================

    /**
     * Get CSV header for COPY operations
     */
    public static String getCsvHeader() {
        // Ensure header is one continuous line without breaks
        StringBuilder header = new StringBuilder();

        // Composite key fields (12 fields)
        header.append("account_id,platform_id,campaign_id,adset_id,advertisement_id,placement_id,");
        header.append("ads_processing_dt,age_group,gender,country_code,region,city,");

        // Core metrics (17 fields)
        header.append("spend,revenue,purchase_roas,impressions,clicks,unique_clicks,cost_per_unique_click,");
        header.append("link_clicks,unique_link_clicks,reach,frequency,cpc,cpm,cpp,ctr,unique_ctr,");

        // Engagement metrics (13 fields)
        header.append("post_engagement,page_engagement,likes,comments,shares,photo_view,video_views,");
        header.append("video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,");
        header.append("video_p95_watched_actions,video_p100_watched_actions,video_avg_percent_watched,");

        // Conversion metrics (8 fields)
        header.append("purchases,purchase_value,leads,cost_per_lead,mobile_app_install,cost_per_app_install,");
        header.append("social_spend,inline_link_clicks,inline_post_engagement,cost_per_inline_link_click,");
        header.append("cost_per_inline_post_engagement,");

        // Metadata (6 fields)
        header.append("currency,attribution_setting,date_start,date_stop,created_at,updated_at,country_name");

        return header.toString();
    }

    /**
     * Get CSV mapper function for COPY operations
     */
    public static Function<AdsReporting, String> getCsvMapper() {
        return reporting -> {
            StringBuilder csv = new StringBuilder();

            // Composite key fields (12 fields) - CRITICAL: Must match order exactly
            csv.append(csvEscape(reporting.getAccountId())).append(",");
            csv.append(csvEscape(reporting.getPlatformId())).append(",");
            csv.append(csvEscape(reporting.getCampaignId())).append(",");
            csv.append(csvEscape(reporting.getAdsetId())).append(",");
            csv.append(csvEscape(reporting.getAdvertisementId())).append(",");
            csv.append(csvEscape(reporting.getPlacementId())).append(",");
            csv.append(csvEscape(reporting.getAdsProcessingDt())).append(",");
            csv.append(csvEscape(reporting.getAgeGroup())).append(",");
            csv.append(csvEscape(reporting.getGender())).append(",");
            csv.append(reporting.getCountryCode() != null ? reporting.getCountryCode() : 0).append(",");
            csv.append(csvEscape(reporting.getRegion())).append(",");
            csv.append(csvEscape(reporting.getCity())).append(",");

            // Core metrics (17 fields)
            csv.append(reporting.getSpend() != null ? reporting.getSpend() : 0.0).append(",");
            csv.append(reporting.getRevenue() != null ? reporting.getRevenue() : 0.0).append(",");
            csv.append(csvEscape(reporting.getPurchaseRoas() != null ? reporting.getPurchaseRoas().toString() : "")).append(",");
            csv.append(reporting.getImpressions() != null ? reporting.getImpressions() : 0L).append(",");
            csv.append(reporting.getClicks() != null ? reporting.getClicks() : 0L).append(",");
            csv.append(reporting.getUniqueClicks() != null ? reporting.getUniqueClicks() : 0L).append(",");
            csv.append(csvEscape(reporting.getCostPerUniqueClick() != null ? reporting.getCostPerUniqueClick().toString() : "")).append(",");
            csv.append(reporting.getLinkClicks() != null ? reporting.getLinkClicks() : 0L).append(",");
            csv.append(reporting.getUniqueLinkClicks() != null ? reporting.getUniqueLinkClicks() : 0L).append(",");
            csv.append(reporting.getReach() != null ? reporting.getReach() : 0L).append(",");
            csv.append(csvEscape(reporting.getFrequency() != null ? reporting.getFrequency().toString() : "")).append(",");
            csv.append(csvEscape(reporting.getCpc() != null ? reporting.getCpc().toString() : "")).append(",");
            csv.append(csvEscape(reporting.getCpm() != null ? reporting.getCpm().toString() : "")).append(",");
            csv.append(csvEscape(reporting.getCpp() != null ? reporting.getCpp().toString() : "")).append(",");
            csv.append(csvEscape(reporting.getCtr() != null ? reporting.getCtr().toString() : "")).append(",");
            csv.append(csvEscape(reporting.getUniqueCtr() != null ? reporting.getUniqueCtr().toString() : "")).append(",");

            // Engagement metrics (13 fields)
            csv.append(reporting.getPostEngagement() != null ? reporting.getPostEngagement() : 0L).append(",");
            csv.append(reporting.getPageEngagement() != null ? reporting.getPageEngagement() : 0L).append(",");
            csv.append(reporting.getLikes() != null ? reporting.getLikes() : 0L).append(",");
            csv.append(reporting.getComments() != null ? reporting.getComments() : 0L).append(",");
            csv.append(reporting.getShares() != null ? reporting.getShares() : 0L).append(",");
            csv.append(reporting.getPhotoView() != null ? reporting.getPhotoView() : 0L).append(",");
            csv.append(reporting.getVideoViews() != null ? reporting.getVideoViews() : 0L).append(",");
            csv.append(reporting.getVideoP25WatchedActions() != null ? reporting.getVideoP25WatchedActions() : 0L).append(",");
            csv.append(reporting.getVideoP50WatchedActions() != null ? reporting.getVideoP50WatchedActions() : 0L).append(",");
            csv.append(reporting.getVideoP75WatchedActions() != null ? reporting.getVideoP75WatchedActions() : 0L).append(",");
            csv.append(reporting.getVideoP95WatchedActions() != null ? reporting.getVideoP95WatchedActions() : 0L).append(",");
            csv.append(reporting.getVideoP100WatchedActions() != null ? reporting.getVideoP100WatchedActions() : 0L).append(",");
            csv.append(csvEscape(reporting.getVideoAvgPercentWatched() != null ? reporting.getVideoAvgPercentWatched().toString() : "")).append(",");

            // Conversion metrics (8 fields)
            csv.append(reporting.getPurchases() != null ? reporting.getPurchases() : 0L).append(",");
            csv.append(csvEscape(reporting.getPurchaseValue() != null ? reporting.getPurchaseValue().toString() : "")).append(",");
            csv.append(reporting.getLeads() != null ? reporting.getLeads() : 0L).append(",");
            csv.append(csvEscape(reporting.getCostPerLead() != null ? reporting.getCostPerLead().toString() : "")).append(",");
            csv.append(reporting.getMobileAppInstall() != null ? reporting.getMobileAppInstall() : 0L).append(",");
            csv.append(csvEscape(reporting.getCostPerAppInstall() != null ? reporting.getCostPerAppInstall().toString() : "")).append(",");
            csv.append(csvEscape(reporting.getSocialSpend() != null ? reporting.getSocialSpend().toString() : "")).append(",");
            csv.append(reporting.getInlineLinkClicks() != null ? reporting.getInlineLinkClicks() : 0L).append(",");
            csv.append(reporting.getInlinePostEngagement() != null ? reporting.getInlinePostEngagement() : 0L).append(",");
            csv.append(csvEscape(reporting.getCostPerInlineLinkClick() != null ? reporting.getCostPerInlineLinkClick().toString() : "")).append(",");
            csv.append(csvEscape(reporting.getCostPerInlinePostEngagement() != null ? reporting.getCostPerInlinePostEngagement().toString() : "")).append(",");

            // Metadata (6 fields) - LAST FIELD NO COMMA
            csv.append(csvEscape(reporting.getCurrency())).append(",");
            csv.append(csvEscape(reporting.getAttributionSetting())).append(",");
            csv.append(csvEscape(reporting.getDateStart())).append(",");
            csv.append(csvEscape(reporting.getDateStop())).append(",");
            csv.append(csvEscape(reporting.getCreatedAt())).append(",");
            csv.append(csvEscape(reporting.getUpdatedAt())).append(",");
            csv.append(csvEscape(reporting.getCountryName())); // NO COMMA for last field

            return csv.toString();
        };
    }

    // ==================== UTILITY METHODS ====================

    /**
     * Create parameter source for both insert and update operations
     */
    private SqlParameterSource createParameterSource(AdsReporting reporting) {
        return new MapSqlParameterSource()
                .addValue("accountId", reporting.getAccountId())
                .addValue("platformId", reporting.getPlatformId())
                .addValue("campaignId", reporting.getCampaignId())
                .addValue("adsetId", reporting.getAdsetId())
                .addValue("advertisementId", reporting.getAdvertisementId())
                .addValue("placementId", reporting.getPlacementId())
                .addValue("adsProcessingDt", reporting.getAdsProcessingDt())
                .addValue("ageGroup", reporting.getAgeGroup())
                .addValue("gender", reporting.getGender())
                .addValue("countryCode", reporting.getCountryCode())
                .addValue("region", reporting.getRegion())
                .addValue("city", reporting.getCity())
                .addValue("spend", reporting.getSpend())
                .addValue("revenue", reporting.getRevenue())
                .addValue("purchaseRoas", reporting.getPurchaseRoas())
                .addValue("impressions", reporting.getImpressions())
                .addValue("clicks", reporting.getClicks())
                .addValue("uniqueClicks", reporting.getUniqueClicks())
                .addValue("costPerUniqueClick", reporting.getCostPerUniqueClick())
                .addValue("linkClicks", reporting.getLinkClicks())
                .addValue("uniqueLinkClicks", reporting.getUniqueLinkClicks())
                .addValue("reach", reporting.getReach())
                .addValue("frequency", reporting.getFrequency())
                .addValue("cpc", reporting.getCpc())
                .addValue("cpm", reporting.getCpm())
                .addValue("cpp", reporting.getCpp())
                .addValue("ctr", reporting.getCtr())
                .addValue("uniqueCtr", reporting.getUniqueCtr())
                .addValue("postEngagement", reporting.getPostEngagement())
                .addValue("pageEngagement", reporting.getPageEngagement())
                .addValue("likes", reporting.getLikes())
                .addValue("comments", reporting.getComments())
                .addValue("shares", reporting.getShares())
                .addValue("photoView", reporting.getPhotoView())
                .addValue("videoViews", reporting.getVideoViews())
                .addValue("videoP25WatchedActions", reporting.getVideoP25WatchedActions())
                .addValue("videoP50WatchedActions", reporting.getVideoP50WatchedActions())
                .addValue("videoP75WatchedActions", reporting.getVideoP75WatchedActions())
                .addValue("videoP95WatchedActions", reporting.getVideoP95WatchedActions())
                .addValue("videoP100WatchedActions", reporting.getVideoP100WatchedActions())
                .addValue("videoAvgPercentWatched", reporting.getVideoAvgPercentWatched())
                .addValue("purchases", reporting.getPurchases())
                .addValue("purchaseValue", reporting.getPurchaseValue())
                .addValue("leads", reporting.getLeads())
                .addValue("costPerLead", reporting.getCostPerLead())
                .addValue("mobileAppInstall", reporting.getMobileAppInstall())
                .addValue("costPerAppInstall", reporting.getCostPerAppInstall())
                .addValue("socialSpend", reporting.getSocialSpend())
                .addValue("inlineLinkClicks", reporting.getInlineLinkClicks())
                .addValue("inlinePostEngagement", reporting.getInlinePostEngagement())
                .addValue("costPerInlineLinkClick", reporting.getCostPerInlineLinkClick())
                .addValue("costPerInlinePostEngagement", reporting.getCostPerInlinePostEngagement())
                .addValue("currency", reporting.getCurrency())
                .addValue("attributionSetting", reporting.getAttributionSetting())
                .addValue("dateStart", reporting.getDateStart())
                .addValue("dateStop", reporting.getDateStop())
                .addValue("createdAt", reporting.getCreatedAt())
                .addValue("updatedAt", reporting.getUpdatedAt())
                .addValue("countryName", reporting.getCountryName());
    }

    /**
     * Helper method to escape CSV values
     */
    /**
     * Enhanced CSV escape method - handles all edge cases
     */
    private static String csvEscape(String value) {
        if (value == null) return "";

        // Remove any newlines that could break CSV
        String cleaned = value.replace("\n", " ").replace("\r", " ");

        // If contains comma, quote, or was cleaned - wrap in quotes
        if (cleaned.contains(",") || cleaned.contains("\"") || !cleaned.equals(value)) {
            return "\"" + cleaned.replace("\"", "\"\"") + "\"";
        }

        return cleaned;
    }

    /**
     * Safe string helper to handle null values
     */
    private String safeString(String value) {
        return value != null ? value : "unknown";
    }

    // ==================== ROW MAPPER ====================

    /**
     * RowMapper implementation for AdsReporting
     */
    public static class AdsReportingRowMapper implements RowMapper<AdsReporting> {
        @Override
        public AdsReporting mapRow(ResultSet rs, int rowNum) throws SQLException {
            AdsReporting reporting = new AdsReporting();

            // Composite key fields
            reporting.setAccountId(rs.getString("account_id"));
            reporting.setPlatformId(rs.getString("platform_id"));
            reporting.setCampaignId(rs.getString("campaign_id"));
            reporting.setAdsetId(rs.getString("adset_id"));
            reporting.setAdvertisementId(rs.getString("advertisement_id"));
            reporting.setPlacementId(rs.getString("placement_id"));
            reporting.setAdsProcessingDt(rs.getString("ads_processing_dt"));
            reporting.setAgeGroup(rs.getString("age_group"));
            reporting.setGender(rs.getString("gender"));
            reporting.setCountryCode(rs.getInt("country_code"));
            reporting.setRegion(rs.getString("region"));
            reporting.setCity(rs.getString("city"));

            // Core metrics
            reporting.setSpend(rs.getDouble("spend"));
            reporting.setRevenue(rs.getDouble("revenue"));
            reporting.setPurchaseRoas(rs.getBigDecimal("purchase_roas"));
            reporting.setImpressions(rs.getLong("impressions"));
            reporting.setClicks(rs.getLong("clicks"));
            reporting.setUniqueClicks(rs.getLong("unique_clicks"));
            reporting.setCostPerUniqueClick(rs.getBigDecimal("cost_per_unique_click"));
            reporting.setLinkClicks(rs.getLong("link_clicks"));
            reporting.setUniqueLinkClicks(rs.getLong("unique_link_clicks"));
            reporting.setReach(rs.getLong("reach"));
            reporting.setFrequency(rs.getBigDecimal("frequency"));
            reporting.setCpc(rs.getBigDecimal("cpc"));
            reporting.setCpm(rs.getBigDecimal("cpm"));
            reporting.setCpp(rs.getBigDecimal("cpp"));
            reporting.setCtr(rs.getBigDecimal("ctr"));
            reporting.setUniqueCtr(rs.getBigDecimal("unique_ctr"));

            // Engagement metrics
            reporting.setPostEngagement(rs.getLong("post_engagement"));
            reporting.setPageEngagement(rs.getLong("page_engagement"));
            reporting.setLikes(rs.getLong("likes"));
            reporting.setComments(rs.getLong("comments"));
            reporting.setShares(rs.getLong("shares"));
            reporting.setPhotoView(rs.getLong("photo_view"));
            reporting.setVideoViews(rs.getLong("video_views"));
            reporting.setVideoP25WatchedActions(rs.getLong("video_p25_watched_actions"));
            reporting.setVideoP50WatchedActions(rs.getLong("video_p50_watched_actions"));
            reporting.setVideoP75WatchedActions(rs.getLong("video_p75_watched_actions"));
            reporting.setVideoP95WatchedActions(rs.getLong("video_p95_watched_actions"));
            reporting.setVideoP100WatchedActions(rs.getLong("video_p100_watched_actions"));
            reporting.setVideoAvgPercentWatched(rs.getBigDecimal("video_avg_percent_watched"));

            // Conversion metrics
            reporting.setPurchases(rs.getLong("purchases"));
            reporting.setPurchaseValue(rs.getBigDecimal("purchase_value"));
            reporting.setLeads(rs.getLong("leads"));
            reporting.setCostPerLead(rs.getBigDecimal("cost_per_lead"));
            reporting.setMobileAppInstall(rs.getLong("mobile_app_install"));
            reporting.setCostPerAppInstall(rs.getBigDecimal("cost_per_app_install"));
            reporting.setSocialSpend(rs.getBigDecimal("social_spend"));
            reporting.setInlineLinkClicks(rs.getLong("inline_link_clicks"));
            reporting.setInlinePostEngagement(rs.getLong("inline_post_engagement"));
            reporting.setCostPerInlineLinkClick(rs.getBigDecimal("cost_per_inline_link_click"));
            reporting.setCostPerInlinePostEngagement(rs.getBigDecimal("cost_per_inline_post_engagement"));

            // Metadata
            reporting.setCurrency(rs.getString("currency"));
            reporting.setAttributionSetting(rs.getString("attribution_setting"));
            reporting.setDateStart(rs.getString("date_start"));
            reporting.setDateStop(rs.getString("date_stop"));
            reporting.setCreatedAt(rs.getString("created_at"));
            reporting.setUpdatedAt(rs.getString("updated_at"));
            reporting.setCountryName(rs.getString("country_name"));

            return reporting;
        }
    }

    // ==================== VALIDATION & MONITORING METHODS ====================

    /**
     * Validate data integrity after operations
     */
    public boolean validateDataIntegrity(String accountId, String processingDate) {
        try {
            // Check for null composite key fields
            String nullCheckSql = """
                SELECT COUNT(*) FROM tbl_ads_reporting 
                WHERE account_id = ? AND ads_processing_dt = ?
                AND (platform_id IS NULL OR campaign_id IS NULL OR adset_id IS NULL 
                     OR advertisement_id IS NULL OR placement_id IS NULL 
                     OR age_group IS NULL OR gender IS NULL OR region IS NULL OR city IS NULL)
                """;

            Integer nullCount = jdbcTemplate.queryForObject(nullCheckSql, Integer.class, accountId, processingDate);
            if (nullCount != null && nullCount > 0) {
                logger.warn("Found {} records with null composite key fields", nullCount);
                return false;
            }

            // Check for duplicate composite keys
            String duplicateCheckSql = """
                SELECT COUNT(*) - COUNT(DISTINCT account_id, platform_id, campaign_id, adset_id, 
                                       advertisement_id, placement_id, ads_processing_dt, 
                                       age_group, gender, country_code, region, city) as duplicate_count
                FROM tbl_ads_reporting
                WHERE account_id = ? AND ads_processing_dt = ?
                """;

            Integer duplicateCount = jdbcTemplate.queryForObject(duplicateCheckSql, Integer.class, accountId, processingDate);
            if (duplicateCount != null && duplicateCount > 0) {
                logger.warn("Found {} duplicate composite keys", duplicateCount);
                return false;
            }

            logger.info("✅ Data integrity validation passed for account {} on {}", accountId, processingDate);
            return true;

        } catch (Exception e) {
            logger.error("Data integrity validation failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get statistics for a specific account and date
     */
    public Map<String, Object> getAccountDateStats(String accountId, String processingDate) {
        Map<String, Object> stats = new HashMap<>();

        try {
            String statsSql = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT campaign_id) as unique_campaigns,
                    COUNT(DISTINCT adset_id) as unique_adsets,
                    COUNT(DISTINCT advertisement_id) as unique_ads,
                    COUNT(DISTINCT placement_id) as unique_placements,
                    SUM(spend) as total_spend,
                    SUM(impressions) as total_impressions,
                    SUM(clicks) as total_clicks
                FROM tbl_ads_reporting 
                WHERE account_id = ? AND ads_processing_dt = ?
                """;

            jdbcTemplate.queryForObject(statsSql, (rs, rowNum) -> {
                stats.put("totalRecords", rs.getInt("total_records"));
                stats.put("uniqueCampaigns", rs.getInt("unique_campaigns"));
                stats.put("uniqueAdsets", rs.getInt("unique_adsets"));
                stats.put("uniqueAds", rs.getInt("unique_ads"));
                stats.put("uniquePlacements", rs.getInt("unique_placements"));
                stats.put("totalSpend", rs.getDouble("total_spend"));
                stats.put("totalImpressions", rs.getLong("total_impressions"));
                stats.put("totalClicks", rs.getLong("total_clicks"));
                return null;
            }, accountId, processingDate);

        } catch (Exception e) {
            logger.error("Failed to get account date stats: {}", e.getMessage());
            stats.put("error", e.getMessage());
        }

        return stats;
    }

    /**
     * Clean up duplicate records (emergency method)
     */
    @Transactional
    public int cleanupDuplicates(String accountId, String processingDate) {
        logger.warn("🧹 Starting cleanup of duplicate records for account {} on {}", accountId, processingDate);

        String cleanupSql = """
            DELETE FROM tbl_ads_reporting r1
            WHERE r1.account_id = ? AND r1.ads_processing_dt = ?
            AND r1.created_at < (
                SELECT MAX(r2.created_at) FROM tbl_ads_reporting r2
                WHERE r2.account_id = r1.account_id 
                AND r2.platform_id = r1.platform_id
                AND r2.campaign_id = r1.campaign_id
                AND r2.adset_id = r1.adset_id
                AND r2.advertisement_id = r1.advertisement_id
                AND r2.placement_id = r1.placement_id
                AND r2.ads_processing_dt = r1.ads_processing_dt
                AND r2.age_group = r1.age_group
                AND r2.gender = r1.gender
                AND r2.country_code = r1.country_code
                AND r2.region = r1.region
                AND r2.city = r1.city
            )
            """;

        try {
            int deletedRows = jdbcTemplate.update(cleanupSql, accountId, processingDate);
            logger.info("🧹 Cleanup completed: {} duplicate records removed", deletedRows);
            return deletedRows;
        } catch (Exception e) {
            logger.error("Cleanup failed: {}", e.getMessage());
            throw new RuntimeException("Failed to cleanup duplicates", e);
        }
    }

    /**
     * Performance monitoring for large operations
     */
    public void logPerformanceMetrics(String operation, long startTime, int recordCount) {
        long duration = System.currentTimeMillis() - startTime;
        double recordsPerSecond = recordCount > 0 ? (recordCount * 1000.0) / duration : 0;

        logger.info("📊 Performance metrics for {}:", operation);
        logger.info("   📝 Records processed: {}", recordCount);
        logger.info("   ⏱️  Duration: {}ms ({:.2f}s)", duration, duration / 1000.0);
        logger.info("   🚀 Throughput: {:.1f} records/second", recordsPerSecond);

        // Log performance warnings
        if (recordsPerSecond < 100 && recordCount > 1000) {
            logger.warn("⚠️  Low throughput detected: {:.1f} records/second", recordsPerSecond);
        }

        if (duration > 60000) { // > 1 minute
            logger.warn("⚠️  Long operation detected: {:.2f} minutes", duration / 60000.0);
        }
    }

    // ==================== BACKUP & RECOVERY METHODS ====================

    /**
     * Create backup before major operations
     */
    @Transactional
    public boolean createBackup(String accountId, String processingDate, String backupSuffix) {
        String backupTableName = "tbl_ads_reporting_backup_" + backupSuffix;

        try {
            // Create backup table
            String createBackupSql = String.format(
                    "CREATE TABLE %s AS SELECT * FROM tbl_ads_reporting WHERE account_id = ? AND ads_processing_dt = ?",
                    backupTableName
            );

            jdbcTemplate.update(createBackupSql, accountId, processingDate);

            // Get record count
            String countSql = String.format("SELECT COUNT(*) FROM %s", backupTableName);
            Integer backupCount = jdbcTemplate.queryForObject(countSql, Integer.class);

            logger.info("✅ Backup created: {} with {} records", backupTableName, backupCount);
            return true;

        } catch (Exception e) {
            logger.error("❌ Failed to create backup: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Drop backup table
     */
    @Transactional
    public boolean dropBackup(String backupSuffix) {
        String backupTableName = "tbl_ads_reporting_backup_" + backupSuffix;

        try {
            String dropSql = String.format("DROP TABLE IF EXISTS %s", backupTableName);
            jdbcTemplate.execute(dropSql);

            logger.info("🗑️  Backup table dropped: {}", backupTableName);
            return true;

        } catch (Exception e) {
            logger.error("❌ Failed to drop backup table: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Validate CSV format before sending to database
     */
    public static boolean validateCsvFormat(List<AdsReporting> reportingList) {
        if (reportingList == null || reportingList.isEmpty()) {
            return true;
        }

        String header = getCsvHeader();
        String[] headerFields = header.split(",");
        System.out.printf("🔍 CSV Header validation: %d fields expected\n", headerFields.length);

        Function<AdsReporting, String> mapper = getCsvMapper();

        // Test first few records
        for (int i = 0; i < Math.min(3, reportingList.size()); i++) {
            AdsReporting reporting = reportingList.get(i);
            String csvRow = mapper.apply(reporting);
            String[] rowFields = csvRow.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            System.out.printf("🔍 Record #%d: %d fields generated\n", i + 1, rowFields.length);

            if (rowFields.length != headerFields.length) {
                System.err.printf("❌ CSV Field count mismatch: header=%d, row=%d\n",
                        headerFields.length, rowFields.length);
                System.err.printf("❌ Problematic row: %s\n", csvRow.substring(0, Math.min(200, csvRow.length())));
                return false;
            }

            // Check critical fields
            if (i == 0) {
                System.out.printf("🔍 Sample fields: age_group='%s', gender='%s', region='%s'\n",
                        rowFields[7], rowFields[8], rowFields[10]);
            }
        }

        System.out.println("✅ CSV format validation passed");
        return true;
    }
}