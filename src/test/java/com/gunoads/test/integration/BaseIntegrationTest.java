package com.gunoads.test.integration;

import com.gunoads.Application;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

/**
 * Base class for integration tests - tests with Spring context and database
 */
@SpringBootTest(classes = Application.class)
@ActiveProfiles("test")
@Transactional
public abstract class BaseIntegrationTest {

    @BeforeEach
    void setUpIntegrationTest() {
        // Common setup for integration tests
    }

    protected void logTestStart() {
        System.out.printf("⚙️ INTEGRATION TEST: %s\n", getClass().getSimpleName());
    }
}