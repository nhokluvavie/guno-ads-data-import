package com.gunoads.controller;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.scheduler.DataSyncScheduler;
import com.gunoads.service.MetaAdsService;
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

    @Autowired
    private DataSyncScheduler dataSyncScheduler;

    @Autowired
    private MetaAdsService metaAdsService;

    /**
     * Trigger manual sync
     */
    @PostMapping("/sync/manual")
    public ResponseEntity<Map<String, String>> triggerManualSync() {
        try {
            dataSyncScheduler.triggerManualSync();
            return ResponseEntity.ok(Map.of("status", "success", "message", "Manual sync triggered"));
        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /**
     * NEW DEFAULT: Sync today's performance data (replaces yesterday as default)
     */
    @PostMapping("/sync/performance/today")
    public ResponseEntity<Map<String, Object>> syncTodayPerformance() {
        try {
            logger.info("Manual trigger: Today performance sync");
            metaAdsService.syncTodayPerformanceData();

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Today's performance sync completed",
                    "date", LocalDate.now().toString(),
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Today performance sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "date", LocalDate.now().toString()
                    ));
        }
    }

    /**
     * KEEP EXISTING: Sync yesterday's performance data (legacy support)
     */
    @PostMapping("/sync/performance/yesterday")
    public ResponseEntity<Map<String, Object>> syncYesterdayPerformance() {
        try {
            logger.info("Manual trigger: Yesterday performance sync");
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
                            "date", LocalDate.now().minusDays(1).toString()
                    ));
        }
    }

    /**
     * NEW: Sync performance data for specific date
     */
    @PostMapping("/sync/performance/date/{date}")
    public ResponseEntity<Map<String, Object>> syncPerformanceForDate(@PathVariable String date) {
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
                            "example", "2025-01-15"
                    ));
        } catch (Exception e) {
            logger.error("Performance sync failed for date {}: {}", date, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "date", date
                    ));
        }
    }

    /**
     * NEW: Sync performance data for date range
     */
    @PostMapping("/sync/performance/range")
    public ResponseEntity<Map<String, Object>> syncPerformanceForRange(
            @RequestParam String startDate,
            @RequestParam String endDate) {
        try {
            LocalDate start = LocalDate.parse(startDate);
            LocalDate end = LocalDate.parse(endDate);

            // Validate date range
            if (start.isAfter(end)) {
                return ResponseEntity.badRequest()
                        .body(Map.of(
                                "status", "error",
                                "message", "Start date must be before or equal to end date",
                                "startDate", startDate,
                                "endDate", endDate
                        ));
            }

            logger.info("Manual trigger: Performance sync for range {} to {}", start, end);
            metaAdsService.syncPerformanceDataForDateRange(start, end);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Performance sync completed for range: " + startDate + " to " + endDate,
                    "startDate", startDate,
                    "endDate", endDate,
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (DateTimeParseException e) {
            return ResponseEntity.badRequest()
                    .body(Map.of(
                            "status", "error",
                            "message", "Invalid date format. Use YYYY-MM-DD format",
                            "example", "startDate=2025-01-01&endDate=2025-01-15"
                    ));
        } catch (Exception e) {
            logger.error("Performance sync failed for range {} to {}: {}", startDate, endDate, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "startDate", startDate,
                            "endDate", endDate
                    ));
        }
    }

    /**
     * NEW: Sync performance data for last N days
     */
    @PostMapping("/sync/performance/last-days/{days}")
    public ResponseEntity<Map<String, Object>> syncPerformanceLastNDays(@PathVariable int days) {
        try {
            if (days <= 0 || days > 90) { // Limit to 90 days for safety
                return ResponseEntity.badRequest()
                        .body(Map.of(
                                "status", "error",
                                "message", "Days must be between 1 and 90",
                                "days", days
                        ));
            }

            logger.info("Manual trigger: Performance sync for last {} days", days);
            metaAdsService.syncPerformanceDataLastNDays(days);

            LocalDate endDate = LocalDate.now();
            LocalDate startDate = endDate.minusDays(days - 1);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Performance sync completed for last " + days + " days",
                    "startDate", startDate.toString(),
                    "endDate", endDate.toString(),
                    "days", days,
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Performance sync failed for last {} days: {}", days, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "days", days
                    ));
        }
    }

    /**
     * NEW: Sync performance data for current month
     */
    @PostMapping("/sync/performance/current-month")
    public ResponseEntity<Map<String, Object>> syncPerformanceCurrentMonth() {
        try {
            logger.info("Manual trigger: Performance sync for current month");
            metaAdsService.syncPerformanceDataCurrentMonth();

            LocalDate today = LocalDate.now();
            LocalDate startOfMonth = today.withDayOfMonth(1);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Performance sync completed for current month",
                    "startDate", startOfMonth.toString(),
                    "endDate", today.toString(),
                    "month", today.getMonth().toString(),
                    "year", today.getYear(),
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Current month performance sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /**
     * NEW: Sync performance data for previous month
     */
    @PostMapping("/sync/performance/previous-month")
    public ResponseEntity<Map<String, Object>> syncPerformancePreviousMonth() {
        try {
            logger.info("Manual trigger: Performance sync for previous month");
            metaAdsService.syncPerformanceDataPreviousMonth();

            LocalDate today = LocalDate.now();
            LocalDate startOfLastMonth = today.minusMonths(1).withDayOfMonth(1);
            LocalDate endOfLastMonth = startOfLastMonth.plusMonths(1).minusDays(1);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Performance sync completed for previous month",
                    "startDate", startOfLastMonth.toString(),
                    "endDate", endOfLastMonth.toString(),
                    "month", startOfLastMonth.getMonth().toString(),
                    "year", startOfLastMonth.getYear(),
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Previous month performance sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /**
     * UPDATED: Enhanced full sync (now defaults to today)
     */
    @PostMapping("/sync/full")
    public ResponseEntity<Map<String, Object>> triggerFullSync() {
        try {
            logger.info("Manual trigger: Full sync (hierarchy + today performance)");
            metaAdsService.performFullSync();

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Full sync completed (hierarchy + today performance)",
                    "date", LocalDate.now().toString(),
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (Exception e) {
            logger.error("Full sync failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /**
     * NEW: Full sync for specific date
     */
    @PostMapping("/sync/full/date/{date}")
    public ResponseEntity<Map<String, Object>> triggerFullSyncForDate(@PathVariable String date) {
        try {
            LocalDate targetDate = LocalDate.parse(date);
            logger.info("Manual trigger: Full sync for date {}", targetDate);

            metaAdsService.performFullSyncForDate(targetDate);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Full sync completed for date: " + date,
                    "date", date,
                    "timestamp", LocalDateTime.now().toString()
            ));
        } catch (DateTimeParseException e) {
            return ResponseEntity.badRequest()
                    .body(Map.of(
                            "status", "error",
                            "message", "Invalid date format. Use YYYY-MM-DD format",
                            "example", "2025-01-15"
                    ));
        } catch (Exception e) {
            logger.error("Full sync failed for date {}: {}", date, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of(
                            "status", "error",
                            "message", e.getMessage(),
                            "date", date
                    ));
        }
    }

    /**
     * Trigger hierarchy sync only
     */
    @PostMapping("/sync/hierarchy")
    public ResponseEntity<Map<String, String>> triggerHierarchySync() {
        try {
            dataSyncScheduler.syncAccountHierarchy();
            return ResponseEntity.ok(Map.of("status", "success", "message", "Hierarchy sync completed"));
        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    /**
     * Trigger performance data sync only
     */
    @PostMapping("/sync/performance")
    public ResponseEntity<Map<String, String>> triggerPerformanceSync() {
        try {
            dataSyncScheduler.syncDailyPerformanceData();
            return ResponseEntity.ok(Map.of("status", "success", "message", "Performance sync completed"));
        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .body(Map.of("status", "error", "message", e.getMessage()));
        }
    }



    /**
     * Get system status
     */
    @GetMapping("/status")
    public ResponseEntity<MetaAdsService.SyncStatus> getStatus() {
        try {
            MetaAdsService.SyncStatus status = metaAdsService.getSyncStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.status(500).build();
        }
    }
}