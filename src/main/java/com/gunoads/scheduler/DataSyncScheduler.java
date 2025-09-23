package com.gunoads.scheduler;

import com.gunoads.service.MetaAdsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Service
@ConditionalOnProperty(name = "scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class DataSyncScheduler {

    private static final Logger logger = LoggerFactory.getLogger(DataSyncScheduler.class);

    @Autowired
    private MetaAdsService metaAdsService;

    /**
     * UPDATED: Daily performance data sync at 2:00 AM (now defaults to TODAY)
     */
    @Scheduled(cron = "${scheduler.daily-job-cron:0 0 2 * * ?}")
    public void syncDailyPerformanceData() {
        logger.info("Starting daily performance data sync at {}", LocalDateTime.now());

        try {
            // CHANGED: Use TODAY instead of YESTERDAY as default
            metaAdsService.syncTodayPerformanceData();
            logger.info("Daily performance data sync completed successfully");
        } catch (Exception e) {
            logger.error("Daily performance data sync failed: {}", e.getMessage(), e);
        }
    }

    /**
     * NEW: Alternative daily sync for yesterday (if needed)
     */
    @Scheduled(cron = "${scheduler.yesterday-job-cron:-}") // Disabled by default
    public void syncYesterdayPerformanceData() {
        logger.info("Starting yesterday performance data sync at {}", LocalDateTime.now());

        try {
            metaAdsService.syncYesterdayPerformanceData();
            logger.info("Yesterday performance data sync completed successfully");
        } catch (Exception e) {
            logger.error("Yesterday performance data sync failed: {}", e.getMessage(), e);
        }
    }

    /**
     * KEEP EXISTING: Weekly hierarchy sync on Sunday at 1:00 AM
     */
    @Scheduled(cron = "${scheduler.hierarchy-job-cron:0 0 1 ? * SUN}")
    public void syncAccountHierarchy() {
        logger.info("Starting weekly account hierarchy sync at {}", LocalDateTime.now());

        try {
            metaAdsService.syncAccountHierarchy();
            logger.info("Weekly account hierarchy sync completed successfully");
        } catch (Exception e) {
            logger.error("Weekly account hierarchy sync failed: {}", e.getMessage(), e);
        }
    }

    /**
     * UPDATED: Manual sync trigger (now uses TODAY as default)
     */
    public void triggerManualSync() {
        logger.info("Manual sync triggered at {}", LocalDateTime.now());

        try {
            // CHANGED: performFullSync() now defaults to TODAY
            metaAdsService.performFullSync();
            logger.info("Manual sync completed successfully");
        } catch (Exception e) {
            logger.error("Manual sync failed: {}", e.getMessage(), e);
        }
    }

    /**
     * NEW: Manual sync for specific date
     */
//    public void triggerManualSyncForDate(LocalDate date) {
//        logger.info("Manual sync triggered for date {} at {}", date, LocalDateTime.now());
//
//        try {
//            metaAdsService.performFullSyncForDate(date);
//            logger.info("Manual sync completed successfully for date: {}", date);
//        } catch (Exception e) {
//            logger.error("Manual sync failed for date {}: {}", date, e.getMessage(), e);
//        }
//    }

    /**
     * NEW: Manual sync for date range
     */
//    public void triggerManualSyncForDateRange(LocalDate startDate, LocalDate endDate) {
//        logger.info("Manual sync triggered for range {} to {} at {}", startDate, endDate, LocalDateTime.now());
//
//        try {
//            metaAdsService.performFullSyncForDateRange(startDate, endDate);
//            logger.info("Manual sync completed successfully for range: {} to {}", startDate, endDate);
//        } catch (Exception e) {
//            logger.error("Manual sync failed for range {} to {}: {}", startDate, endDate, e.getMessage(), e);
//        }
//    }
}