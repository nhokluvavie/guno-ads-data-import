/**
 * PHASE 2C: SyncStateDao - Track incremental sync timestamps
 * NEW CLASS để quản lý sync state cho business objects
 */
package com.gunoads.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Repository
public class SyncStateDao {
    private static final Logger logger = LoggerFactory.getLogger(SyncStateDao.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Table để track sync state (tạo tự động nếu chưa có)
    private static final String SYNC_STATE_TABLE = "tbl_sync_state";

    /**
     * Initialize sync state table nếu chưa tồn tại
     */
    public void initializeSyncStateTable() {
        try {
            String createTableSql = "CREATE TABLE IF NOT EXISTS tbl_sync_state (" +
                    "object_type VARCHAR(50) NOT NULL," +
                    "account_id VARCHAR(255)," +
                    "last_sync_time TIMESTAMP NOT NULL," +
                    "sync_status VARCHAR(20) DEFAULT 'SUCCESS'," +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    "PRIMARY KEY (object_type, account_id)" +
                    ")";

            jdbcTemplate.execute(createTableSql);
            logger.info("✅ Sync state table initialized");

        } catch (Exception e) {
            logger.error("❌ Failed to initialize sync state table: {}", e.getMessage());
        }
    }

    /**
     * GET last sync time cho object type + account
     */
    public LocalDateTime getLastSyncTime(String objectType, String accountId) {
        try {
            String sql = "SELECT last_sync_time FROM " + SYNC_STATE_TABLE +
                    " WHERE object_type = ? AND account_id = ?";

            String timestampStr = jdbcTemplate.queryForObject(sql, String.class, objectType, accountId);

            if (timestampStr != null) {
                LocalDateTime lastSync = LocalDateTime.parse(timestampStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                logger.debug("📅 Last sync for {}:{} was {}", objectType, accountId, lastSync);
                return lastSync;
            }

        } catch (Exception e) {
            logger.debug("🆕 No previous sync found for {}:{} - will do full sync", objectType, accountId);
        }

        return null; // First time sync
    }

    /**
     * UPDATE sync time sau khi sync thành công
     */
    public void updateSyncTime(String objectType, String accountId, LocalDateTime syncTime) {
        try {
            String upsertSql = "INSERT INTO " + SYNC_STATE_TABLE +
                    " (object_type, account_id, last_sync_time, updated_at)" +
                    " VALUES (?, ?, ?, CURRENT_TIMESTAMP)" +
                    " ON CONFLICT (object_type, account_id)" +
                    " DO UPDATE SET" +
                    " last_sync_time = EXCLUDED.last_sync_time," +
                    " sync_status = 'SUCCESS'," +
                    " updated_at = CURRENT_TIMESTAMP";

            int rows = jdbcTemplate.update(upsertSql,
                    objectType,
                    accountId,
                    syncTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            );

            logger.debug("✅ Updated sync time for {}:{} to {}", objectType, accountId, syncTime);

        } catch (Exception e) {
            logger.error("❌ Failed to update sync time for {}:{}: {}", objectType, accountId, e.getMessage());
        }
    }

    /**
     * CHECK if incremental sync is possible
     */
    public boolean canUseIncremental(String objectType, String accountId) {
        LocalDateTime lastSync = getLastSyncTime(objectType, accountId);

        if (lastSync == null) {
            logger.info("🆕 First sync for {}: {} - using full sync", objectType, accountId);
            return false;
        }

        // Check if last sync was within reasonable time (24 hours)
        LocalDateTime now = LocalDateTime.now();
        long hoursSinceLastSync = java.time.Duration.between(lastSync, now).toHours();

        if (hoursSinceLastSync > 24) {
            logger.warn("⚠️ Last sync for {}:{} was {} hours ago - using full sync for safety",
                    objectType, accountId, hoursSinceLastSync);
            return false;
        }

        logger.info("✅ Using incremental sync for {}:{} (last sync: {} hours ago)",
                objectType, accountId, hoursSinceLastSync);
        return true;
    }

    /**
     * MARK sync as failed
     */
    public void markSyncFailed(String objectType, String accountId, String errorMessage) {
        try {
            String updateSql = "INSERT INTO " + SYNC_STATE_TABLE +
                    " (object_type, account_id, last_sync_time, sync_status, updated_at)" +
                    " VALUES (?, ?, CURRENT_TIMESTAMP, 'FAILED', CURRENT_TIMESTAMP)" +
                    " ON CONFLICT (object_type, account_id)" +
                    " DO UPDATE SET" +
                    " sync_status = 'FAILED'," +
                    " updated_at = CURRENT_TIMESTAMP";

            jdbcTemplate.update(updateSql, objectType, accountId);
            logger.error("❌ Marked sync failed for {}:{} - {}", objectType, accountId, errorMessage);

        } catch (Exception e) {
            logger.error("❌ Failed to mark sync as failed: {}", e.getMessage());
        }
    }

    /**
     * GET sync statistics for monitoring
     */
    public SyncStats getSyncStats() {
        try {
            String sql = "SELECT " +
                    "COUNT(*) as total_syncs," +
                    "COUNT(CASE WHEN sync_status = 'SUCCESS' THEN 1 END) as successful_syncs," +
                    "COUNT(CASE WHEN sync_status = 'FAILED' THEN 1 END) as failed_syncs," +
                    "MAX(updated_at) as last_activity " +
                    "FROM " + SYNC_STATE_TABLE;

            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> new SyncStats(
                    rs.getInt("total_syncs"),
                    rs.getInt("successful_syncs"),
                    rs.getInt("failed_syncs"),
                    rs.getString("last_activity")
            ));

        } catch (Exception e) {
            logger.error("❌ Failed to get sync stats: {}", e.getMessage());
            return new SyncStats(0, 0, 0, "unknown");
        }
    }

    /**
     * CLEAR old sync states (cleanup)
     */
    public void cleanupOldSyncStates(int daysToKeep) {
        try {
            String deleteSql = "DELETE FROM " + SYNC_STATE_TABLE +
                    " WHERE updated_at < CURRENT_TIMESTAMP - INTERVAL '" + daysToKeep + " days'";

            int deleted = jdbcTemplate.update(deleteSql);
            if (deleted > 0) {
                logger.info("🧹 Cleaned up {} old sync state records", deleted);
            }

        } catch (Exception e) {
            logger.error("❌ Failed to cleanup old sync states: {}", e.getMessage());
        }
    }

    /**
     * Sync statistics model
     */
    public static class SyncStats {
        public final int totalSyncs;
        public final int successfulSyncs;
        public final int failedSyncs;
        public final String lastActivity;

        public SyncStats(int totalSyncs, int successfulSyncs, int failedSyncs, String lastActivity) {
            this.totalSyncs = totalSyncs;
            this.successfulSyncs = successfulSyncs;
            this.failedSyncs = failedSyncs;
            this.lastActivity = lastActivity;
        }

        @Override
        public String toString() {
            return String.format("SyncStats{total=%d, success=%d, failed=%d, lastActivity=%s}",
                    totalSyncs, successfulSyncs, failedSyncs, lastActivity);
        }
    }

    /**
     * Constants cho object types
     */
    public static class ObjectType {
        public static final String CAMPAIGNS = "campaigns";
        public static final String ADSETS = "adsets";
        public static final String ADS = "ads";
        public static final String ACCOUNTS = "accounts"; // Ít khi dùng incremental
    }
}