package com.gunoads;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Application.class)
@ActiveProfiles("integration-test")
@Transactional
public abstract class AbstractIntegrationTest {

    @BeforeEach
    void setUpIntegrationTest() {
        // Common setup for all integration tests
        System.out.println("=== Starting Integration Test: " + getClass().getSimpleName() + " ===");
    }
}