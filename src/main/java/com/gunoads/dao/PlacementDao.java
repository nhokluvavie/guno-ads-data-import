package com.gunoads.dao;

import com.gunoads.model.entity.Placement;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class PlacementDao extends StandardDao<Placement, String> {

    @Override
    public String getTableName() {
        return "tbl_placement";
    }

    @Override
    public String getIdColumnName() {
        return "id";
    }

    @Override
    public RowMapper<Placement> getRowMapper() {
        return new PlacementRowMapper();
    }

    @Override
    public String buildInsertSql() {
        return """
            INSERT INTO tbl_placement (
                id, advertisement_id, placement_name, platform, placement_type,
                device_type, position, is_active, supports_video, supports_carousel,
                supports_collection, created_at
            ) VALUES (
                :id, :advertisementId, :placementName, :platform, :placementType,
                :deviceType, :position, :isActive, :supportsVideo, :supportsCarousel,
                :supportsCollection, :createdAt
            )
            """;
    }

    @Override
    public String buildUpdateSql() {
        return """
            UPDATE tbl_placement SET 
                placement_name = :placementName, platform = :platform,
                placement_type = :placementType, device_type = :deviceType,
                position = :position, is_active = :isActive, supports_video = :supportsVideo,
                supports_carousel = :supportsCarousel, supports_collection = :supportsCollection
            WHERE id = :id
            """;
    }

    @Override
    public SqlParameterSource getInsertParameters(Placement placement) {
        return new MapSqlParameterSource()
                .addValue("id", placement.getId())
                .addValue("advertisementId", placement.getAdvertisementId())
                .addValue("placementName", placement.getPlacementName())
                .addValue("platform", placement.getPlatform())
                .addValue("placementType", placement.getPlacementType())
                .addValue("deviceType", placement.getDeviceType())
                .addValue("position", placement.getPosition())
                .addValue("isActive", placement.getIsActive())
                .addValue("supportsVideo", placement.getSupportsVideo())
                .addValue("supportsCarousel", placement.getSupportsCarousel())
                .addValue("supportsCollection", placement.getSupportsCollection())
                .addValue("createdAt", placement.getCreatedAt());
    }

    @Override
    protected SqlParameterSource getUpdateParameters(Placement placement) {
        return getInsertParameters(placement);
    }

    public List<Placement> findByAdvertisement(String advertisementId) {
        String sql = "SELECT * FROM tbl_placement WHERE advertisement_id = :advertisementId";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("advertisementId", advertisementId), getRowMapper());
    }

    private static class PlacementRowMapper implements RowMapper<Placement> {
        @Override
        public Placement mapRow(ResultSet rs, int rowNum) throws SQLException {
            Placement placement = new Placement();
            placement.setId(rs.getString("id"));
            placement.setAdvertisementId(rs.getString("advertisement_id"));
            placement.setPlacementName(rs.getString("placement_name"));
            placement.setPlatform(rs.getString("platform"));
            placement.setPlacementType(rs.getString("placement_type"));
            placement.setDeviceType(rs.getString("device_type"));
            placement.setPosition(rs.getString("position"));
            placement.setIsActive(rs.getBoolean("is_active"));
            placement.setSupportsVideo(rs.getBoolean("supports_video"));
            placement.setSupportsCarousel(rs.getBoolean("supports_carousel"));
            placement.setSupportsCollection(rs.getBoolean("supports_collection"));
            placement.setCreatedAt(rs.getString("created_at"));
            return placement;
        }
    }
}