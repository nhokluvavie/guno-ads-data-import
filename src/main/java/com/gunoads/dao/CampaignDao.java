package com.gunoads.dao;

import com.gunoads.model.entity.Campaign;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class CampaignDao extends StandardDao<Campaign, String> {

    @Override
    public String getTableName() {
        return "tbl_campaign";
    }

    @Override
    public String getIdColumnName() {
        return "id";
    }

    @Override
    public RowMapper<Campaign> getRowMapper() {
        return new CampaignRowMapper();
    }

    @Override
    public String buildInsertSql() {
        return """
            INSERT INTO tbl_campaign (
                id, account_id, platform_id, campaign_name, cam_objective, start_time,
                stop_time, created_time, updated_time, last_updated, cam_status,
                configured_status, effective_status, buying_type, daily_budget,
                lifetime_budget, budget_remaining, bid_strategy, cam_optimization_type,
                special_ad_categories, attribution_spec, spend_cap
            ) VALUES (
                :id, :accountId, :platformId, :campaignName, :camObjective, :startTime,
                :stopTime, :createdTime, :updatedTime, :lastUpdated, :camStatus,
                :configuredStatus, :effectiveStatus, :buyingType, :dailyBudget,
                :lifetimeBudget, :budgetRemaining, :bidStrategy, :camOptimizationType,
                :specialAdCategories, :attributionSpec, :spendCap
            )
            """;
    }

    @Override
    public String buildUpdateSql() {
        return """
            UPDATE tbl_campaign SET 
                campaign_name = :campaignName, cam_objective = :camObjective,
                start_time = :startTime, stop_time = :stopTime, updated_time = :updatedTime,
                last_updated = :lastUpdated, cam_status = :camStatus,
                configured_status = :configuredStatus, effective_status = :effectiveStatus,
                buying_type = :buyingType, daily_budget = :dailyBudget,
                lifetime_budget = :lifetimeBudget, budget_remaining = :budgetRemaining,
                bid_strategy = :bidStrategy, cam_optimization_type = :camOptimizationType,
                special_ad_categories = :specialAdCategories, attribution_spec = :attributionSpec,
                spend_cap = :spendCap
            WHERE id = :id
            """;
    }

    @Override
    public SqlParameterSource getInsertParameters(Campaign campaign) {
        return new MapSqlParameterSource()
                .addValue("id", campaign.getId())
                .addValue("accountId", campaign.getAccountId())
                .addValue("platformId", campaign.getPlatformId())
                .addValue("campaignName", campaign.getCampaignName())
                .addValue("camObjective", campaign.getCamObjective())
                .addValue("startTime", campaign.getStartTime())
                .addValue("stopTime", campaign.getStopTime())
                .addValue("createdTime", campaign.getCreatedTime())
                .addValue("updatedTime", campaign.getUpdatedTime())
                .addValue("lastUpdated", campaign.getLastUpdated())
                .addValue("camStatus", campaign.getCamStatus())
                .addValue("configuredStatus", campaign.getConfiguredStatus())
                .addValue("effectiveStatus", campaign.getEffectiveStatus())
                .addValue("buyingType", campaign.getBuyingType())
                .addValue("dailyBudget", campaign.getDailyBudget())
                .addValue("lifetimeBudget", campaign.getLifetimeBudget())
                .addValue("budgetRemaining", campaign.getBudgetRemaining())
                .addValue("bidStrategy", campaign.getBidStrategy())
                .addValue("camOptimizationType", campaign.getCamOptimizationType())
                .addValue("specialAdCategories", campaign.getSpecialAdCategories())
                .addValue("attributionSpec", campaign.getAttributionSpec())
                .addValue("spendCap", campaign.getSpendCap());
    }

    @Override
    protected SqlParameterSource getUpdateParameters(Campaign campaign) {
        return getInsertParameters(campaign);
    }

    public List<Campaign> findByAccount(String accountId) {
        String sql = "SELECT * FROM tbl_campaign WHERE account_id = :accountId";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("accountId", accountId), getRowMapper());
    }

    private static class CampaignRowMapper implements RowMapper<Campaign> {
        @Override
        public Campaign mapRow(ResultSet rs, int rowNum) throws SQLException {
            Campaign campaign = new Campaign();
            campaign.setId(rs.getString("id"));
            campaign.setAccountId(rs.getString("account_id"));
            campaign.setPlatformId(rs.getString("platform_id"));
            campaign.setCampaignName(rs.getString("campaign_name"));
            campaign.setCamObjective(rs.getString("cam_objective"));
            campaign.setStartTime(rs.getString("start_time"));
            campaign.setStopTime(rs.getString("stop_time"));
            campaign.setCreatedTime(rs.getString("created_time"));
            campaign.setUpdatedTime(rs.getString("updated_time"));
            campaign.setLastUpdated(rs.getString("last_updated"));
            campaign.setCamStatus(rs.getString("cam_status"));
            campaign.setConfiguredStatus(rs.getString("configured_status"));
            campaign.setEffectiveStatus(rs.getString("effective_status"));
            campaign.setBuyingType(rs.getString("buying_type"));
            campaign.setDailyBudget(rs.getBigDecimal("daily_budget"));
            campaign.setLifetimeBudget(rs.getBigDecimal("lifetime_budget"));
            campaign.setBudgetRemaining(rs.getBigDecimal("budget_remaining"));
            campaign.setBidStrategy(rs.getString("bid_strategy"));
            campaign.setCamOptimizationType(rs.getString("cam_optimization_type"));
            campaign.setSpecialAdCategories(rs.getString("special_ad_categories"));
            campaign.setAttributionSpec(rs.getString("attribution_spec"));
            campaign.setSpendCap(rs.getBigDecimal("spend_cap"));
            return campaign;
        }
    }
}