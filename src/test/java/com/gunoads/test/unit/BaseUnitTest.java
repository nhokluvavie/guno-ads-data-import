package com.gunoads.test.unit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

/**
 * Base class for unit tests - fast, isolated tests with mocks
 */
@ExtendWith(MockitoExtension.class)
@ActiveProfiles("test")
public abstract class BaseUnitTest {

    @BeforeEach
    void setUpUnitTest() {
        // Common setup for unit tests
    }

    protected void logTestStart() {
        System.out.printf("🧪 UNIT TEST: %s\n", getClass().getSimpleName());
    }
}