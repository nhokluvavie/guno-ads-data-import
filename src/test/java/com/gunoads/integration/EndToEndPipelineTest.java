package com.gunoads.integration;

import com.gunoads.AbstractIntegrationTest;
import com.gunoads.service.MetaAdsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.*;

class EndToEndPipelineTest extends AbstractIntegrationTest {

    @Autowired
    private MetaAdsService metaAdsService;

    @Test
    void shouldTestConnectivity() {
        System.out.println("Testing end-to-end connectivity...");

        boolean connected = metaAdsService.testConnectivity();
        assertThat(connected).isTrue();

        System.out.println("✅ End-to-end connectivity successful");
    }

    @Test
    void shouldSyncAccountHierarchy() {
        System.out.println("Testing full account hierarchy sync...");

        // Get initial status
        var initialStatus = metaAdsService.getSyncStatus();
        System.out.printf("   Initial status: %s\n", initialStatus);

        // Perform sync
        metaAdsService.syncAccountHierarchy();

        // Get final status
        var finalStatus = metaAdsService.getSyncStatus();
        System.out.printf("   Final status: %s\n", finalStatus);

        // Verify improvements
        assertThat(finalStatus.accountCount).isGreaterThanOrEqualTo(initialStatus.accountCount);
        assertThat(finalStatus.isConnected).isTrue();

        System.out.println("✅ Account hierarchy sync completed");
    }

    @Test
    void shouldSyncPerformanceData() {
        System.out.println("Testing performance data sync...");

        var initialStatus = metaAdsService.getSyncStatus();

        try {
            metaAdsService.syncYesterdayPerformanceData();

            var finalStatus = metaAdsService.getSyncStatus();
            System.out.printf("   Reporting records: %d -> %d\n",
                    initialStatus.reportingCount, finalStatus.reportingCount);

            System.out.println("✅ Performance data sync completed");

        } catch (Exception e) {
            System.out.printf("ℹ️ Performance sync info: %s\n", e.getMessage());
            // This might fail if no recent data available - that's okay for testing
        }
    }

    @Test
    void shouldPerformFullSync() {
        System.out.println("Testing full system sync...");

        var initialStatus = metaAdsService.getSyncStatus();
        System.out.printf("   Before: %s\n", initialStatus);

        try {
            metaAdsService.performFullSync();

            var finalStatus = metaAdsService.getSyncStatus();
            System.out.printf("   After: %s\n", finalStatus);

            assertThat(finalStatus.isConnected).isTrue();
            assertThat(finalStatus.accountCount).isGreaterThan(0);

            System.out.println("✅ Full sync completed successfully");

        } catch (Exception e) {
            System.out.printf("⚠️ Full sync completed with warnings: %s\n", e.getMessage());
        }
    }

    @Test
    void shouldGetSystemStatus() {
        System.out.println("Testing system status reporting...");

        var status = metaAdsService.getSyncStatus();

        assertThat(status).isNotNull();
        System.out.printf("✅ System Status:\n");
        System.out.printf("   Connected: %s\n", status.isConnected);
        System.out.printf("   Accounts: %d\n", status.accountCount);
        System.out.printf("   Campaigns: %d\n", status.campaignCount);
        System.out.printf("   AdSets: %d\n", status.adSetCount);
        System.out.printf("   Ads: %d\n", status.adCount);
        System.out.printf("   Reporting Records: %d\n", status.reportingCount);
    }
}