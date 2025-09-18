package com.gunoads.test.e2e;

import com.gunoads.Application;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

/**
 * Base class for end-to-end tests - full system tests with real API calls
 */
@SpringBootTest(
        classes = Application.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@ActiveProfiles("e2e-test")  // Fix: Use e2e-test profile
public abstract class BaseE2ETest {

    @LocalServerPort
    protected int port;

    protected String baseUrl;

    @BeforeEach
    void setUpE2ETest() {
        baseUrl = "http://localhost:" + port;
        logTestStart();
    }

    protected void logTestStart() {
        System.out.printf("🚀 E2E TEST: %s (port: %d)\n", getClass().getSimpleName(), port);
    }

    /**
     * Wait for async processing to complete
     */
    protected void waitForProcessing(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted", e);
        }
    }

    /**
     * Clean test data - minimal for E2E tests
     */
    protected void cleanupTestData() {
        // E2E tests with real data - minimal cleanup needed
        // Test isolation handled by @Transactional in integration layer
        System.out.println("🧹 E2E cleanup: Using real data, minimal cleanup");
    }
}