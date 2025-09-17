package com.gunoads.test.integration;

import com.gunoads.Application;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

/**
 * Base class for integration tests - real Spring context + database + API
 */
@SpringBootTest(classes = Application.class)
@ActiveProfiles("integration-test")
@TestPropertySource(locations = "classpath:application-integration-test.yml")
@Transactional
public abstract class BaseIntegrationTest {

    @BeforeEach
    void setUpIntegrationTest() {
        // Common setup for integration tests
        logTestStart();
    }

    protected void logTestStart() {
        System.out.printf("⚙️ INTEGRATION TEST: %s\n", getClass().getSimpleName());
    }

    /**
     * Helper method to wait for async operations
     */
    protected void waitFor(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Verify database is accessible
     */
    protected void verifyDatabaseConnection() {
        // Will be implemented in DatabaseConnectionTest
    }

    /**
     * Verify Meta API is accessible
     */
    protected void verifyMetaApiConnection() {
        // Will be implemented in MetaApiConnectionTest
    }
}