package com.gunoads.scheduler;

import com.gunoads.service.MetaAdsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@ConditionalOnProperty(name = "scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class DataSyncScheduler {

    private static final Logger logger = LoggerFactory.getLogger(DataSyncScheduler.class);

    @Autowired
    private MetaAdsService metaAdsService;

    /**
     * Daily performance data sync at 2:00 AM
     */
    @Scheduled(cron = "${scheduler.daily-job-cron:0 0 2 * * ?}")
    public void syncDailyPerformanceData() {
        logger.info("Starting daily performance data sync at {}", LocalDateTime.now());

        try {
            metaAdsService.syncYesterdayPerformanceData();
            logger.info("Daily performance data sync completed successfully");
        } catch (Exception e) {
            logger.error("Daily performance data sync failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Weekly hierarchy sync on Sunday at 1:00 AM
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
     * Manual sync trigger for testing
     */
    public void triggerManualSync() {
        logger.info("Manual sync triggered at {}", LocalDateTime.now());

        try {
            metaAdsService.performFullSync();
            logger.info("Manual sync completed successfully");
        } catch (Exception e) {
            logger.error("Manual sync failed: {}", e.getMessage(), e);
        }
    }
}