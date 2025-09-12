package com.gunoads.dao;

import com.gunoads.model.entity.Account;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class AccountDao extends StandardDao<Account, String> {

    @Override
    protected String getTableName() {
        return "tbl_account";
    }

    @Override
    protected String getIdColumnName() {
        return "id";
    }

    @Override
    protected RowMapper<Account> getRowMapper() {
        return new AccountRowMapper();
    }

    @Override
    protected String buildInsertSql() {
        return """
            INSERT INTO tbl_account (
                id, platform_id, account_name, currency, timezone_id, timezone_name, 
                account_status, disable_reason, bussiness_id, business_name, 
                business_country_code, business_city, business_state, business_street, 
                business_zip, is_personal, is_prepay_account, is_tax_id_required, 
                capabilities, amount_spent, balance, spend_cap, funding_source, 
                account_age, has_page_authorized_ad_account, is_direct_deals_enabled, 
                is_notifications_enabled, created_time, last_updated
            ) VALUES (
                :id, :platformId, :accountName, :currency, :timezoneId, :timezoneName,
                :accountStatus, :disableReason, :bussinessId, :businessName,
                :businessCountryCode, :businessCity, :businessState, :businessStreet,
                :businessZip, :isPersonal, :isPrepayAccount, :isTaxIdRequired,
                :capabilities, :amountSpent, :balance, :spendCap, :fundingSource,
                :accountAge, :hasPageAuthorizedAdAccount, :isDirectDealsEnabled,
                :isNotificationsEnabled, :createdTime, :lastUpdated
            )
            """;
    }

    @Override
    protected String buildUpdateSql() {
        return """
            UPDATE tbl_account SET 
                account_name = :accountName, currency = :currency, timezone_id = :timezoneId,
                timezone_name = :timezoneName, account_status = :accountStatus,
                disable_reason = :disableReason, bussiness_id = :bussinessId,
                business_name = :businessName, business_country_code = :businessCountryCode,
                business_city = :businessCity, business_state = :businessState,
                business_street = :businessStreet, business_zip = :businessZip,
                is_personal = :isPersonal, is_prepay_account = :isPrepayAccount,
                is_tax_id_required = :isTaxIdRequired, capabilities = :capabilities,
                amount_spent = :amountSpent, balance = :balance, spend_cap = :spendCap,
                funding_source = :fundingSource, account_age = :accountAge,
                has_page_authorized_ad_account = :hasPageAuthorizedAdAccount,
                is_direct_deals_enabled = :isDirectDealsEnabled,
                is_notifications_enabled = :isNotificationsEnabled,
                last_updated = :lastUpdated
            WHERE id = :id AND platform_id = :platformId
            """;
    }

    @Override
    protected SqlParameterSource getInsertParameters(Account account) {
        return new MapSqlParameterSource()
                .addValue("id", account.getId())
                .addValue("platformId", account.getPlatformId())
                .addValue("accountName", account.getAccountName())
                .addValue("currency", account.getCurrency())
                .addValue("timezoneId", account.getTimezoneId())
                .addValue("timezoneName", account.getTimezoneName())
                .addValue("accountStatus", account.getAccountStatus())
                .addValue("disableReason", account.getDisableReason())
                .addValue("bussinessId", account.getBussinessId())
                .addValue("businessName", account.getBusinessName())
                .addValue("businessCountryCode", account.getBusinessCountryCode())
                .addValue("businessCity", account.getBusinessCity())
                .addValue("businessState", account.getBusinessState())
                .addValue("businessStreet", account.getBusinessStreet())
                .addValue("businessZip", account.getBusinessZip())
                .addValue("isPersonal", account.getIsPersonal())
                .addValue("isPrepayAccount", account.getIsPrepayAccount())
                .addValue("isTaxIdRequired", account.getIsTaxIdRequired())
                .addValue("capabilities", account.getCapabilities())
                .addValue("amountSpent", account.getAmountSpent())
                .addValue("balance", account.getBalance())
                .addValue("spendCap", account.getSpendCap())
                .addValue("fundingSource", account.getFundingSource())
                .addValue("accountAge", account.getAccountAge())
                .addValue("hasPageAuthorizedAdAccount", account.getHasPageAuthorizedAdAccount())
                .addValue("isDirectDealsEnabled", account.getIsDirectDealsEnabled())
                .addValue("isNotificationsEnabled", account.getIsNotificationsEnabled())
                .addValue("createdTime", account.getCreatedTime())
                .addValue("lastUpdated", account.getLastUpdated());
    }

    @Override
    protected SqlParameterSource getUpdateParameters(Account account) {
        return getInsertParameters(account);
    }

    public List<Account> findByPlatform(String platformId) {
        String sql = "SELECT * FROM tbl_account WHERE platform_id = :platformId";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("platformId", platformId), getRowMapper());
    }

    private static class AccountRowMapper implements RowMapper<Account> {
        @Override
        public Account mapRow(ResultSet rs, int rowNum) throws SQLException {
            Account account = new Account();
            account.setId(rs.getString("id"));
            account.setPlatformId(rs.getString("platform_id"));
            account.setAccountName(rs.getString("account_name"));
            account.setCurrency(rs.getString("currency"));
            account.setTimezoneId(rs.getInt("timezone_id"));
            account.setTimezoneName(rs.getString("timezone_name"));
            account.setAccountStatus(rs.getString("account_status"));
            account.setDisableReason(rs.getString("disable_reason"));
            account.setBussinessId(rs.getString("bussiness_id"));
            account.setBusinessName(rs.getString("business_name"));
            account.setBusinessCountryCode(rs.getInt("business_country_code"));
            account.setBusinessCity(rs.getString("business_city"));
            account.setBusinessState(rs.getString("business_state"));
            account.setBusinessStreet(rs.getString("business_street"));
            account.setBusinessZip(rs.getInt("business_zip"));
            account.setIsPersonal(rs.getBoolean("is_personal"));
            account.setIsPrepayAccount(rs.getBoolean("is_prepay_account"));
            account.setIsTaxIdRequired(rs.getBoolean("is_tax_id_required"));
            account.setCapabilities(rs.getString("capabilities"));
            account.setAmountSpent(rs.getBigDecimal("amount_spent"));
            account.setBalance(rs.getBigDecimal("balance"));
            account.setSpendCap(rs.getBigDecimal("spend_cap"));
            account.setFundingSource(rs.getString("funding_source"));
            account.setAccountAge(rs.getInt("account_age"));
            account.setHasPageAuthorizedAdAccount(rs.getBoolean("has_page_authorized_ad_account"));
            account.setIsDirectDealsEnabled(rs.getBoolean("is_direct_deals_enabled"));
            account.setIsNotificationsEnabled(rs.getBoolean("is_notifications_enabled"));
            account.setCreatedTime(rs.getString("created_time"));
            account.setLastUpdated(rs.getString("last_updated"));
            return account;
        }
    }
}