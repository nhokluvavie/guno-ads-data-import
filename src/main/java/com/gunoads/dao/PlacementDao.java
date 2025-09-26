package com.gunoads.dao;

import com.gunoads.model.entity.Placement;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.stream.Collectors;

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

    // ==================== EXISTING METHODS ====================

    public List<Placement> findByAdvertisement(String advertisementId) {
        String sql = "SELECT * FROM tbl_placement WHERE advertisement_id = :advertisementId";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("advertisementId", advertisementId), getRowMapper());
    }

    // ==================== NEW BATCH OPERATIONS ====================

    /**
     * Batch upsert placements - insert new, update existing
     *
     * @param placements List of placements to upsert
     */
    public void batchUpsert(List<Placement> placements) {
        if (placements == null || placements.isEmpty()) {
            return;
        }

        logger.info("🔄 Starting batch upsert for {} placements", placements.size());
        long startTime = System.currentTimeMillis();

        try {
            // Separate into existing and new placements
            Set<String> existingIds = findExistingIds(placements.stream()
                    .map(Placement::getId)
                    .collect(Collectors.toSet()));

            List<Placement> toInsert = new ArrayList<>();
            List<Placement> toUpdate = new ArrayList<>();

            for (Placement placement : placements) {
                if (existingIds.contains(placement.getId())) {
                    toUpdate.add(placement);
                } else {
                    toInsert.add(placement);
                }
            }

            // Perform batch operations
            if (!toInsert.isEmpty()) {
                batchInsert(toInsert);
                logger.debug("📝 Inserted {} new placements", toInsert.size());
            }

            if (!toUpdate.isEmpty()) {
                batchUpdate(toUpdate);
                logger.debug("🔄 Updated {} existing placements", toUpdate.size());
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("✅ Batch upsert completed: {}/{} processed in {}ms",
                    toInsert.size(), toUpdate.size(), duration);

        } catch (Exception e) {
            logger.error("❌ Batch upsert failed: {}", e.getMessage());
            throw new RuntimeException("Batch upsert failed", e);
        }
    }

    /**
     * Batch update existing placements
     *
     * @param placements List of placements to update
     */
    public void batchUpdate(List<Placement> placements) {
        if (placements == null || placements.isEmpty()) {
            return;
        }

        String sql = buildUpdateSql();

        try {
            SqlParameterSource[] batchParams = placements.stream()
                    .map(this::getUpdateParameters)
                    .toArray(SqlParameterSource[]::new);

            int[] rowsAffected = namedParameterJdbcTemplate.batchUpdate(sql, batchParams);

            logger.info("Batch updated {} placements", rowsAffected.length);
        } catch (Exception e) {
            logger.error("Error in batch update: {}", e.getMessage());
            throw new RuntimeException("Database error while batch updating placements", e);
        }
    }

    /**
     * Find existing placement IDs from a set of IDs
     *
     * @param placementIds Set of placement IDs to check
     * @return Set of IDs that exist in database
     */
    public Set<String> findExistingIds(Set<String> placementIds) {
        if (placementIds == null || placementIds.isEmpty()) {
            return new HashSet<>();
        }

        // Convert set to comma-separated string for IN clause
        String idsString = placementIds.stream()
                .map(id -> "'" + id + "'")
                .collect(Collectors.joining(","));

        String sql = "SELECT id FROM tbl_placement WHERE id IN (" + idsString + ")";

        try {
            return new HashSet<>(jdbcTemplate.queryForList(sql, String.class));
        } catch (Exception e) {
            logger.error("Error finding existing placement IDs: {}", e.getMessage());
            return new HashSet<>();
        }
    }

    /**
     * Find missing placement IDs that don't exist in database
     *
     * @param placementIds Set of placement IDs to check
     * @return Set of IDs that DON'T exist in database
     */
    public Set<String> findMissingPlacements(Set<String> placementIds) {
        Set<String> existingIds = findExistingIds(placementIds);
        Set<String> missingIds = new HashSet<>(placementIds);
        missingIds.removeAll(existingIds);

        logger.debug("Missing placements: {}/{} IDs not found", missingIds.size(), placementIds.size());
        return missingIds;
    }

    /**
     * Check if specific placement exists by placement key (combination)
     *
     * @param placementId Placement ID to check
     * @return true if placement exists
     */
    public boolean existsByPlacementKey(String placementId) {
        return existsById(placementId);
    }

    /**
     * Find placements by platform
     *
     * @param platform Platform name (facebook, instagram, etc.)
     * @return List of placements for the platform
     */
    public List<Placement> findByPlatform(String platform) {
        String sql = "SELECT * FROM tbl_placement WHERE platform = :platform ORDER BY placement_name";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("platform", platform), getRowMapper());
    }

    /**
     * Find active placements only
     *
     * @return List of active placements
     */
    public List<Placement> findActive() {
        String sql = "SELECT * FROM tbl_placement WHERE is_active = true ORDER BY platform, placement_name";
        return jdbcTemplate.query(sql, getRowMapper());
    }

    /**
     * Count placements by platform
     *
     * @return Count of placements grouped by platform
     */
    public List<PlacementPlatformCount> countByPlatform() {
        String sql = """
            SELECT platform, COUNT(*) as count 
            FROM tbl_placement 
            WHERE is_active = true
            GROUP BY platform 
            ORDER BY count DESC
            """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> new PlacementPlatformCount(
                rs.getString("platform"),
                rs.getLong("count")
        ));
    }

    /**
     * Cleanup orphaned placements (no corresponding advertisement)
     *
     * @return Number of orphaned placements removed
     */
    public int cleanupOrphanedPlacements() {
        String sql = """
            DELETE FROM tbl_placement 
            WHERE advertisement_id NOT IN (SELECT id FROM tbl_advertisement)
            """;

        try {
            int deletedCount = jdbcTemplate.update(sql);
            if (deletedCount > 0) {
                logger.info("🧹 Cleaned up {} orphaned placements", deletedCount);
            }
            return deletedCount;
        } catch (Exception e) {
            logger.error("Error cleaning orphaned placements: {}", e.getMessage());
            return 0;
        }
    }

    // ==================== HELPER CLASSES ====================

    /**
     * Helper class for platform count results
     */
    public static class PlacementPlatformCount {
        private final String platform;
        private final Long count;

        public PlacementPlatformCount(String platform, Long count) {
            this.platform = platform;
            this.count = count;
        }

        public String getPlatform() { return platform; }
        public Long getCount() { return count; }

        @Override
        public String toString() {
            return String.format("%s: %d placements", platform, count);
        }
    }

    // ==================== ROW MAPPER ====================

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