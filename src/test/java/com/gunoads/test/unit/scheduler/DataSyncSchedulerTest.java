package com.gunoads.test.unit.scheduler;

import com.gunoads.scheduler.DataSyncScheduler;
import com.gunoads.service.MetaAdsService;
import com.gunoads.test.unit.BaseUnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.mockito.Mockito.*;

class DataSyncSchedulerTest extends BaseUnitTest {

    @Mock
    private MetaAdsService metaAdsService;

    @InjectMocks
    private DataSyncScheduler scheduler;

    @BeforeEach
    void setUp() {
        logTestStart();
    }

    @Test
    void shouldSyncDailyPerformanceData() {
        // When
        scheduler.syncDailyPerformanceData();

        // Then
        verify(metaAdsService).syncYesterdayPerformanceData();
    }

    @Test
    void shouldHandlePerformanceDataSyncFailure() {
        // Given
        doThrow(new RuntimeException("Sync failed")).when(metaAdsService).syncYesterdayPerformanceData();

        // When & Then - should not throw exception
        scheduler.syncDailyPerformanceData();

        verify(metaAdsService).syncYesterdayPerformanceData();
    }

    @Test
    void shouldSyncAccountHierarchy() {
        // When
        scheduler.syncAccountHierarchy();

        // Then
        verify(metaAdsService).syncAccountHierarchy();
    }

    @Test
    void shouldHandleHierarchySyncFailure() {
        // Given
        doThrow(new RuntimeException("Hierarchy sync failed")).when(metaAdsService).syncAccountHierarchy();

        // When & Then - should not throw exception
        scheduler.syncAccountHierarchy();

        verify(metaAdsService).syncAccountHierarchy();
    }

    @Test
    void shouldTriggerManualSync() {
        // When
        scheduler.triggerManualSync();

        // Then
        verify(metaAdsService).performFullSync();
    }

    @Test
    void shouldHandleManualSyncFailure() {
        // Given
        doThrow(new RuntimeException("Manual sync failed")).when(metaAdsService).performFullSync();

        // When & Then - should not throw exception
        scheduler.triggerManualSync();

        verify(metaAdsService).performFullSync();
    }
}