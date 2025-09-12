package com.gunoads.dao;

import com.gunoads.model.entity.Advertisement;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class AdvertisementDao extends StandardDao<Advertisement, String> {

    @Override
    protected String getTableName() {
        return "tbl_advertisement";
    }

    @Override
    protected String getIdColumnName() {
        return "id";
    }

    @Override
    protected RowMapper<Advertisement> getRowMapper() {
        return new AdvertisementRowMapper();
    }

    @Override
    protected String buildInsertSql() {
        return """
            INSERT INTO tbl_advertisement (
                id, adsetid, ad_name, ad_status, configured_status, effective_status,
                creative_id, creative_name, creative_type, call_to_action_type,
                is_app_install_ad, is_dynamic_ad, is_video_ad, is_carousel_ad,
                created_time, updated_time, last_updated
            ) VALUES (
                :id, :adsetid, :adName, :adStatus, :configuredStatus, :effectiveStatus,
                :creativeId, :creativeName, :creativeType, :callToActionType,
                :isAppInstallAd, :isDynamicAd, :isVideoAd, :isCarouselAd,
                :createdTime, :updatedTime, :lastUpdated
            )
            """;
    }

    @Override
    protected String buildUpdateSql() {
        return """
            UPDATE tbl_advertisement SET 
                ad_name = :adName, ad_status = :adStatus, configured_status = :configuredStatus,
                effective_status = :effectiveStatus, creative_id = :creativeId,
                creative_name = :creativeName, creative_type = :creativeType,
                call_to_action_type = :callToActionType, is_app_install_ad = :isAppInstallAd,
                is_dynamic_ad = :isDynamicAd, is_video_ad = :isVideoAd,
                is_carousel_ad = :isCarouselAd, updated_time = :updatedTime,
                last_updated = :lastUpdated
            WHERE id = :id
            """;
    }

    @Override
    protected SqlParameterSource getInsertParameters(Advertisement ad) {
        return new MapSqlParameterSource()
                .addValue("id", ad.getId())
                .addValue("adsetid", ad.getAdsetid())
                .addValue("adName", ad.getAdName())
                .addValue("adStatus", ad.getAdStatus())
                .addValue("configuredStatus", ad.getConfiguredStatus())
                .addValue("effectiveStatus", ad.getEffectiveStatus())
                .addValue("creativeId", ad.getCreativeId())
                .addValue("creativeName", ad.getCreativeName())
                .addValue("creativeType", ad.getCreativeType())
                .addValue("callToActionType", ad.getCallToActionType())
                .addValue("isAppInstallAd", ad.getIsAppInstallAd())
                .addValue("isDynamicAd", ad.getIsDynamicAd())
                .addValue("isVideoAd", ad.getIsVideoAd())
                .addValue("isCarouselAd", ad.getIsCarouselAd())
                .addValue("createdTime", ad.getCreatedTime())
                .addValue("updatedTime", ad.getUpdatedTime())
                .addValue("lastUpdated", ad.getLastUpdated());
    }

    @Override
    protected SqlParameterSource getUpdateParameters(Advertisement ad) {
        return getInsertParameters(ad);
    }

    public List<Advertisement> findByAdSet(String adsetId) {
        String sql = "SELECT * FROM tbl_advertisement WHERE adsetid = :adsetId";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("adsetId", adsetId), getRowMapper());
    }

    private static class AdvertisementRowMapper implements RowMapper<Advertisement> {
        @Override
        public Advertisement mapRow(ResultSet rs, int rowNum) throws SQLException {
            Advertisement ad = new Advertisement();
            ad.setId(rs.getString("id"));
            ad.setAdsetid(rs.getString("adsetid"));
            ad.setAdName(rs.getString("ad_name"));
            ad.setAdStatus(rs.getString("ad_status"));
            ad.setConfiguredStatus(rs.getString("configured_status"));
            ad.setEffectiveStatus(rs.getString("effective_status"));
            ad.setCreativeId(rs.getString("creative_id"));
            ad.setCreativeName(rs.getString("creative_name"));
            ad.setCreativeType(rs.getString("creative_type"));
            ad.setCallToActionType(rs.getString("call_to_action_type"));
            ad.setIsAppInstallAd(rs.getBoolean("is_app_install_ad"));
            ad.setIsDynamicAd(rs.getBoolean("is_dynamic_ad"));
            ad.setIsVideoAd(rs.getBoolean("is_video_ad"));
            ad.setIsCarouselAd(rs.getBoolean("is_carousel_ad"));
            ad.setCreatedTime(rs.getString("created_time"));
            ad.setUpdatedTime(rs.getString("updated_time"));
            ad.setLastUpdated(rs.getString("last_updated"));
            return ad;
        }
    }
}