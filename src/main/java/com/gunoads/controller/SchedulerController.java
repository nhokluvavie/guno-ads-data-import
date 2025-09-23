package com.gunoads.controller;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.scheduler.DataSyncScheduler;
import com.gunoads.service.MetaAdsService;
import com.gunoads.dao.SyncStateDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;

@RestController
@RequestMapping("/api/scheduler")
public class SchedulerController {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerController.class);

    @Autowired private DataSyncScheduler dataSyncScheduler;
    @Autowired private MetaAdsService metaAdsService;
    @Autowired private MetaAdsConnector metaAdsConnector;
    @Autowired private SyncStateDao syncStateDao;

    // ==================== MANUAL SYNC TRIGGERS ====================

    /**
     * Trigger manual full sync (hierarchy + today's performance)
     */
    @PostMapping("/sync/manual")
    public ResponseEntity<Map<String, Object>> triggerManualSync() {
        try {
            logger.info("Manual trigger: Full sync");
            dataSyncScheduler.triggerManualSync();

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Manual full sync completed",
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Manual sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }

    /**
     * Trigger hierarchy sync only
     */
    @PostMapping("/sync/hierarchy")
    public ResponseEntity<Map<String, Object>> triggerHierarchySync() {
        try {
            logger.info("Manual trigger: Hierarchy sync");
            metaAdsService.syncAccountHierarchy();

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Hierarchy sync completed",
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Hierarchy sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }

    /**
     * Trigger performance data sync for today
     */
    @PostMapping("/sync/performance")
    public ResponseEntity<Map<String, Object>> triggerPerformanceSync() {
        try {
            logger.info("Manual trigger: Today's performance sync");
            metaAdsService.syncTodayPerformanceData();

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Today's performance sync completed",
                    "date", LocalDate.now().toString(),
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Performance sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "date", LocalDate.now().toString(),
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }

    /**
     * Trigger performance data sync for yesterday
     */
    @PostMapping("/sync/performance/yesterday")
    public ResponseEntity<Map<String, Object>> triggerYesterdayPerformanceSync() {
        try {
            logger.info("Manual trigger: Yesterday's performance sync");
            metaAdsService.syncYesterdayPerformanceData();

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Yesterday's performance sync completed",
                    "date", LocalDate.now().minusDays(1).toString(),
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Yesterday performance sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "date", LocalDate.now().minusDays(1).toString(),
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }

    /**
     * Trigger performance data sync for specific date
     */
    @PostMapping("/sync/performance/date/{date}")
    public ResponseEntity<Map<String, Object>> triggerPerformanceSyncForDate(@PathVariable String date) {
        try {
            LocalDate targetDate = LocalDate.parse(date);
            logger.info("Manual trigger: Performance sync for date {}", targetDate);

            metaAdsService.syncPerformanceDataForDate(targetDate);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Performance sync completed for date: " + date,
                    "date", date,
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (DateTimeParseException e) {
            return ResponseEntity.badRequest()
                    .body(Map.of(
                            "status", "error",
                            "message", "Invalid date format. Use YYYY-MM-DD format",
                            "example", "2025-01-15",
                            "timestamp", LocalDateTime.now().toString()
                    ));
        } catch (Exception e) {
            logger.error("Performance sync failed for date {}: {}", date, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "date", date,
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }

    /**
     * Force full sync for specific account
     */
    @PostMapping("/sync/force/{accountId}")
    public ResponseEntity<Map<String, Object>> forceFullSyncForAccount(@PathVariable String accountId) {
        try {
            logger.info("Manual trigger: Force full sync for account {}", accountId);
            metaAdsService.forceFullSync(accountId);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Force full sync completed for account: " + accountId,
                    "accountId", accountId,
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Force full sync failed for account {}: {}", accountId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "accountId", accountId,
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }

    // ==================== STATUS & MONITORING ====================

//    /**
//     * Get system status and sync statistics
//     */
//    @GetMapping("/status")
//    public ResponseEntity<Map<String, Object>> getStatus() {
//        try {
//            // Test connectivity
//            boolean isConnected = metaAdsConnector.testConnectivity();
//
//            // Get sync statistics
//            SyncStateDao.SyncStats syncStats = metaAdsService.getSyncStats();
//
//            return ResponseEntity.ok(Map.of(
//                    "status", "success",
//                    "apiConnected", isConnected,
//                    "syncStats", Map.of(
//                            "totalAccounts", syncStats.getTotalAccounts(),
//                            "successfulSyncs", syncStats.getSuccessfulSyncs(),
//                            "failedSyncs", syncStats.getFailedSyncs(),
//                            "lastSyncTime", syncStats.getLastSyncTime() != null ?
//                                    syncStats.getLastSyncTime().toString() : "Never"
//                    ),
//                    "timestamp", LocalDateTime.now().toString()
//            ));
//        } catch (Exception e) {
//            logger.error("Failed to get status: {}", e.getMessage());
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                    .body(Map.of(
//                            "status", "error",
//                            "message", e.getMessage(),
//                            "timestamp", LocalDateTime.now().toString()
//                    ));
//        }
//    }

    /**
     * Get connectivity status
     */
    @GetMapping("/connectivity")
    public ResponseEntity<Map<String, Object>> getConnectivity() {
        try {
            boolean isConnected = metaAdsConnector.testConnectivity();

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "connected", isConnected,
                    "message", isConnected ? "Meta API connectivity verified" : "Meta API connection failed",
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Connectivity test failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "connected", false,
                            "message", e.getMessage(),
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }

//    /**
//     * Get detailed sync statistics
//     */
//    @GetMapping("/stats")
//    public ResponseEntity<Map<String, Object>> getSyncStats() {
//        try {
//            SyncStateDao.SyncStats syncStats = metaAdsService.getSyncStats();
//
//            return ResponseEntity.ok(Map.of(
//                    "status", "success",
//                    "statistics", Map.of(
//                            "totalAccounts", syncStats.getTotalAccounts(),
//                            "successfulSyncs", syncStats.getSuccessfulSyncs(),
//                            "failedSyncs", syncStats.getFailedSyncs(),
//                            "successRate", syncStats.getTotalAccounts() > 0 ?
//                                    (double) syncStats.getSuccessfulSyncs() / syncStats.getTotalAccounts() * 100 : 0,
//                            "lastSyncTime", syncStats.getLastSyncTime() != null ?
//                                    syncStats.getLastSyncTime().toString() : "Never",
//                            "lastUpdateTime", syncStats.getLastUpdateTime() != null ?
//                                    syncStats.getLastUpdateTime().toString() : "Never"
//                    ),
//                    "timestamp", LocalDateTime.now().toString()
//            ));
//        } catch (Exception e) {
//            logger.error("Failed to get sync statistics: {}", e.getMessage());
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                    .body(Map.of(
//                            "status", "error",
//                            "message", e.getMessage(),
//                            "timestamp", LocalDateTime.now().toString()
//                    ));
//        }
//    }

    // ==================== HEALTH CHECKS ====================

    /**
     * Simple health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            return ResponseEntity.ok(Map.of(
                    "status", "UP",
                    "service", "SchedulerController",
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of(
                            "status", "DOWN",
                            "service", "SchedulerController",
                            "error", e.getMessage(),
                            "timestamp", LocalDateTime.now().toString()
                    ));
        }
    }
}