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
@ActiveProfiles("test")
public abstract class BaseE2ETest {

    @LocalServerPort
    protected int port;

    protected String baseUrl;

    @BeforeEach
    void setUpE2ETest() {
        baseUrl = "http://localhost:" + port;
    }

    protected void logTestStart() {
        System.out.printf("🚀 E2E TEST: %s (port: %d)\n", getClass().getSimpleName(), port);
    }
}