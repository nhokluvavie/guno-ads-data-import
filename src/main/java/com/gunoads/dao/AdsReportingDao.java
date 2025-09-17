package com.gunoads.dao;

import com.gunoads.model.entity.AdsReporting;
import com.gunoads.util.CsvFormatter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

@Repository
public class AdsReportingDao extends StandardDao<AdsReporting, String> {

    @Override
    public String getTableName() {
        return "tbl_ads_reporting";
    }

    @Override
    public String getIdColumnName() {
        return "account_id"; // Composite primary key, using first column
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
    public String buildUpdateSql() {
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

    @Override
    protected SqlParameterSource getUpdateParameters(AdsReporting reporting) {
        return getInsertParameters(reporting);
    }

    /**
     * CSV Mapper for COPY FROM operations
     */
    public static Function<AdsReporting, String> getCsvMapper() {
        return reporting -> new CsvFormatter.CsvRowBuilder()
                .add(reporting.getAccountId())
                .add(reporting.getPlatformId())
                .add(reporting.getCampaignId())
                .add(reporting.getAdsetId())
                .add(reporting.getAdvertisementId())
                .add(reporting.getPlacementId())
                .add(reporting.getAdsProcessingDt())
                .add(reporting.getAgeGroup())
                .add(reporting.getGender())
                .add(reporting.getCountryCode())
                .add(reporting.getRegion())
                .add(reporting.getCity())
                .add(reporting.getSpend())
                .add(reporting.getRevenue())
                .add(reporting.getPurchaseRoas())
                .add(reporting.getImpressions())
                .add(reporting.getClicks())
                .add(reporting.getUniqueClicks())
                .add(reporting.getCostPerUniqueClick())
                .add(reporting.getLinkClicks())
                .add(reporting.getUniqueLinkClicks())
                .add(reporting.getReach())
                .add(reporting.getFrequency())
                .add(reporting.getCpc())
                .add(reporting.getCpm())
                .add(reporting.getCpp())
                .add(reporting.getCtr())
                .add(reporting.getUniqueCtr())
                .add(reporting.getPostEngagement())
                .add(reporting.getPageEngagement())
                .add(reporting.getLikes())
                .add(reporting.getComments())
                .add(reporting.getShares())
                .add(reporting.getPhotoView())
                .add(reporting.getVideoViews())
                .add(reporting.getVideoP25WatchedActions())
                .add(reporting.getVideoP50WatchedActions())
                .add(reporting.getVideoP75WatchedActions())
                .add(reporting.getVideoP95WatchedActions())
                .add(reporting.getVideoP100WatchedActions())
                .add(reporting.getVideoAvgPercentWatched())
                .add(reporting.getPurchases())
                .add(reporting.getPurchaseValue())
                .add(reporting.getLeads())
                .add(reporting.getCostPerLead())
                .add(reporting.getMobileAppInstall())
                .add(reporting.getCostPerAppInstall())
                .add(reporting.getSocialSpend())
                .add(reporting.getInlineLinkClicks())
                .add(reporting.getInlinePostEngagement())
                .add(reporting.getCostPerInlineLinkClick())
                .add(reporting.getCostPerInlinePostEngagement())
                .add(reporting.getCurrency())
                .add(reporting.getAttributionSetting())
                .add(reporting.getDateStart())
                .add(reporting.getDateStop())
                .add(reporting.getCreatedAt())
                .add(reporting.getUpdatedAt())
                .add(reporting.getCountryName())
                .build();
    }

    public static final String CSV_HEADER = CsvFormatter.AdsReportingCsvMapper.CSV_HEADER;

    private static class AdsReportingRowMapper implements RowMapper<AdsReporting> {
        @Override
        public AdsReporting mapRow(ResultSet rs, int rowNum) throws SQLException {
            AdsReporting reporting = new AdsReporting();
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
}