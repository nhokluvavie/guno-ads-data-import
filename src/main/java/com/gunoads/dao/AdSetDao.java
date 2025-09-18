package com.gunoads.dao;

import com.gunoads.model.entity.AdSet;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class AdSetDao extends StandardDao<AdSet, String> {

    @Override
    public String getTableName() {
        return "tbl_adset";
    }

    @Override
    public String getIdColumnName() {
        return "id";
    }

    @Override
    public RowMapper<AdSet> getRowMapper() {
        return new AdSetRowMapper();
    }

    @Override
    public String buildInsertSql() {
        return """
            INSERT INTO tbl_adset (
                id, campaign_id, ad_set_name, ad_set_status, lifetime_imps, start_time,
                end_time, created_time, updated_time, last_updated, configured_status,
                effective_status, optimization_goal, billing_event, bid_amount, bid_strategy,
                is_autobid, daily_budget, lifetime_budget, budget_remaining, targeting_spec,
                geo_locations, age_min, age_max, genders, interests, behaviors,
                custom_audiences, lookalike_audiences, excluded_custom_audiences,
                adset_schedule, attribution_spec
            ) VALUES (
                :id, :campaignId, :adSetName, :adSetStatus, :lifetimeImps, :startTime,
                :endTime, :createdTime, :updatedTime, :lastUpdated, :configuredStatus,
                :effectiveStatus, :optimizationGoal, :billingEvent, :bidAmount, :bidStrategy,
                :isAutobid, :dailyBudget, :lifetimeBudget, :budgetRemaining, :targetingSpec,
                :geoLocations, :ageMin, :ageMax, :genders, :interests, :behaviors,
                :customAudiences, :lookalikAudiences, :excludedCustomAudiences,
                :adsetSchedule, :attributionSpec
            )
            """;
    }

    @Override
    public String buildUpdateSql() {
        return """
            UPDATE tbl_adset SET 
                ad_set_name = :adSetName, ad_set_status = :adSetStatus, lifetime_imps = :lifetimeImps,
                start_time = :startTime, end_time = :endTime, updated_time = :updatedTime,
                last_updated = :lastUpdated, configured_status = :configuredStatus,
                effective_status = :effectiveStatus, optimization_goal = :optimizationGoal,
                billing_event = :billingEvent, bid_amount = :bidAmount, bid_strategy = :bidStrategy,
                is_autobid = :isAutobid, daily_budget = :dailyBudget, lifetime_budget = :lifetimeBudget,
                budget_remaining = :budgetRemaining, targeting_spec = :targetingSpec,
                geo_locations = :geoLocations, age_min = :ageMin, age_max = :ageMax,
                genders = :genders, interests = :interests, behaviors = :behaviors,
                custom_audiences = :customAudiences, lookalike_audiences = :lookalikAudiences,
                excluded_custom_audiences = :excludedCustomAudiences, adset_schedule = :adsetSchedule,
                attribution_spec = :attributionSpec
            WHERE id = :id
            """;
    }

    @Override
    public SqlParameterSource getInsertParameters(AdSet adSet) {
        return new MapSqlParameterSource()
                .addValue("id", adSet.getId())
                .addValue("campaignId", adSet.getCampaignId())
                .addValue("adSetName", adSet.getAdSetName())
                .addValue("adSetStatus", adSet.getAdSetStatus())
                .addValue("lifetimeImps", adSet.getLifetimeImps())
                .addValue("startTime", adSet.getStartTime())
                .addValue("endTime", adSet.getEndTime())
                .addValue("createdTime", adSet.getCreatedTime())
                .addValue("updatedTime", adSet.getUpdatedTime())
                .addValue("lastUpdated", adSet.getLastUpdated())
                .addValue("configuredStatus", adSet.getConfiguredStatus())
                .addValue("effectiveStatus", adSet.getEffectiveStatus())
                .addValue("optimizationGoal", adSet.getOptimizationGoal())
                .addValue("billingEvent", adSet.getBillingEvent())
                .addValue("bidAmount", adSet.getBidAmount())
                .addValue("bidStrategy", adSet.getBidStrategy())
                .addValue("isAutobid", adSet.getIsAutobid())
                .addValue("dailyBudget", adSet.getDailyBudget())
                .addValue("lifetimeBudget", adSet.getLifetimeBudget())
                .addValue("budgetRemaining", adSet.getBudgetRemaining())
                .addValue("targetingSpec", adSet.getTargetingSpec())
                .addValue("geoLocations", adSet.getGeoLocations())
                .addValue("ageMin", adSet.getAgeMin())
                .addValue("ageMax", adSet.getAgeMax())
                .addValue("genders", adSet.getGenders())
                .addValue("interests", adSet.getInterests())
                .addValue("behaviors", adSet.getBehaviors())
                .addValue("customAudiences", adSet.getCustomAudiences())
                .addValue("lookalikAudiences", adSet.getLookalikAudiences())
                .addValue("excludedCustomAudiences", adSet.getExcludedCustomAudiences())
                .addValue("adsetSchedule", adSet.getAdsetSchedule())
                .addValue("attributionSpec", adSet.getAttributionSpec());
    }

    @Override
    protected SqlParameterSource getUpdateParameters(AdSet adSet) {
        return getInsertParameters(adSet);
    }

    public List<AdSet> findByCampaign(String campaignId) {
        String sql = "SELECT * FROM tbl_adset WHERE campaign_id = :campaignId";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("campaignId", campaignId), getRowMapper());
    }

    private static class AdSetRowMapper implements RowMapper<AdSet> {
        @Override
        public AdSet mapRow(ResultSet rs, int rowNum) throws SQLException {
            AdSet adSet = new AdSet();
            adSet.setId(rs.getString("id"));
            adSet.setCampaignId(rs.getString("campaign_id"));
            adSet.setAdSetName(rs.getString("ad_set_name"));
            adSet.setAdSetStatus(rs.getString("ad_set_status"));
            adSet.setLifetimeImps(rs.getLong("lifetime_imps"));
            adSet.setStartTime(rs.getString("start_time"));
            adSet.setEndTime(rs.getString("end_time"));
            adSet.setCreatedTime(rs.getString("created_time"));
            adSet.setUpdatedTime(rs.getString("updated_time"));
            adSet.setLastUpdated(rs.getString("last_updated"));
            adSet.setConfiguredStatus(rs.getString("configured_status"));
            adSet.setEffectiveStatus(rs.getString("effective_status"));
            adSet.setOptimizationGoal(rs.getString("optimization_goal"));
            adSet.setBillingEvent(rs.getString("billing_event"));
            adSet.setBidAmount(rs.getBigDecimal("bid_amount"));
            adSet.setBidStrategy(rs.getString("bid_strategy"));
            adSet.setIsAutobid(rs.getBoolean("is_autobid"));
            adSet.setDailyBudget(rs.getBigDecimal("daily_budget"));
            adSet.setLifetimeBudget(rs.getBigDecimal("lifetime_budget"));
            adSet.setBudgetRemaining(rs.getBigDecimal("budget_remaining"));
            adSet.setTargetingSpec(rs.getString("targeting_spec"));
            adSet.setGeoLocations(rs.getString("geo_locations"));
            adSet.setAgeMin(rs.getInt("age_min"));
            adSet.setAgeMax(rs.getInt("age_max"));
            adSet.setGenders(rs.getString("genders"));
            adSet.setInterests(rs.getString("interests"));
            adSet.setBehaviors(rs.getString("behaviors"));
            adSet.setCustomAudiences(rs.getString("custom_audiences"));
            adSet.setLookalikAudiences(rs.getString("lookalike_audiences"));
            adSet.setExcludedCustomAudiences(rs.getString("excluded_custom_audiences"));
            adSet.setAdsetSchedule(rs.getString("adset_schedule"));
            adSet.setAttributionSpec(rs.getString("attribution_spec"));
            return adSet;
        }
    }
}