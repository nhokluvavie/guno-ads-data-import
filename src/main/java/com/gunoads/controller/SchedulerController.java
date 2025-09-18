package com.gunoads.controller;

import com.gunoads.scheduler.DataSyncScheduler;
import com.gunoads.service.MetaAdsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/scheduler")
public class SchedulerController {

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
     * Trigger full sync (hierarchy + performance) - MISSING ENDPOINT ADDED
     */
    @PostMapping("/sync/full")
    public ResponseEntity<Map<String, String>> triggerFullSync() {
        try {
            metaAdsService.performFullSync();
            return ResponseEntity.ok(Map.of("status", "success", "message", "Full sync completed"));
        } catch (Exception e) {
            return ResponseEntity.status(500)
                    .body(Map.of("status", "error", "message", e.getMessage()));
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