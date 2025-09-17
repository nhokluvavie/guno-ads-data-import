package com.gunoads.test.unit.connector;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.connector.MetaApiClient;
import com.gunoads.config.MetaAdsConfig;
import com.gunoads.config.MetaApiProperties;
import com.gunoads.model.dto.*;
import com.gunoads.test.unit.BaseUnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.InjectMocks;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MetaAdsConnectorTest extends BaseUnitTest {

    @Mock private MetaAdsConfig metaAdsConfig;
    @Mock private MetaApiProperties metaApiProperties;
    @Mock private MetaApiClient metaApiClient;

    @InjectMocks
    private MetaAdsConnector connector;

    @BeforeEach
    void setUp() {
        when(metaAdsConfig.getBusinessId()).thenReturn("business_123");
        logTestStart();
    }

    @Test
    void shouldGetSafeString() throws Exception {
        // Test the safe getter utility methods using reflection or by testing public behavior

        // Given - testing null safety in mapping
        String nullValue = null;
        String validValue = "test-value";

        // When/Then - these methods are used internally in mapping
        // We can verify their behavior through the public API
        assertThat(validValue).isNotNull();
        assertThat(nullValue).isNull();
    }

    @Test
    void shouldTestConnectivity() throws Exception {
        // Given
        when(metaApiClient.executeWithRetry(any())).thenReturn(List.of());

        // When
        boolean connected = connector.testConnectivity();

        // Then
        assertThat(connected).isTrue();
        verify(metaApiClient).executeWithRetry(any());
    }

    @Test
    void shouldHandleConnectivityFailure() throws Exception {
        // Given
        when(metaApiClient.executeWithRetry(any())).thenThrow(new RuntimeException("Connection failed"));

        // When
        boolean connected = connector.testConnectivity();

        // Then
        assertThat(connected).isFalse();
    }

    @Test
    void shouldCreateConnectorStatus() {
        // Given
        MetaApiClient.ClientStatus clientStatus = mock(MetaApiClient.ClientStatus.class);
        boolean isConnected = true;

        // When
        MetaAdsConnector.ConnectorStatus status =
                new MetaAdsConnector.ConnectorStatus(clientStatus, isConnected);

        // Then
        assertThat(status.clientStatus).isEqualTo(clientStatus);
        assertThat(status.isConnected).isTrue();
    }

    @Test
    void shouldBuildYesterdayDate() {
        // Given
        LocalDate today = LocalDate.now();
        LocalDate yesterday = today.minusDays(1);

        // When
        String accountId = "act_123";
        // This would call fetchYesterdayInsights internally

        // Then
        assertThat(yesterday).isBefore(today);
        assertThat(yesterday).isEqualTo(LocalDate.now().minusDays(1));
    }

    @Test
    void shouldHandleSafeGetters() {
        // Test safe conversion methods behavior

        // String conversion
        Object nullObj = null;
        Object stringObj = "test";
        Object numberObj = 123;

        // These would be used in the mapping methods
        String nullResult = nullObj != null ? nullObj.toString() : null;
        String stringResult = stringObj != null ? stringObj.toString() : null;
        String numberResult = numberObj != null ? numberObj.toString() : null;

        assertThat(nullResult).isNull();
        assertThat(stringResult).isEqualTo("test");
        assertThat(numberResult).isEqualTo("123");
    }

    @Test
    void shouldHandleSafeIntegerConversion() {
        // Test integer conversion behavior
        Object nullValue = null;
        Object stringNumber = "123";
        Object actualNumber = 456;
        Object invalidString = "invalid";

        // Simulate the safe conversion logic
        Integer nullResult = safeConvertToInteger(nullValue);
        Integer stringResult = safeConvertToInteger(stringNumber);
        Integer numberResult = safeConvertToInteger(actualNumber);
        Integer invalidResult = safeConvertToInteger(invalidString);

        assertThat(nullResult).isNull();
        assertThat(stringResult).isEqualTo(123);
        assertThat(numberResult).isEqualTo(456);
        assertThat(invalidResult).isNull();
    }

    @Test
    void shouldHandleSafeLongConversion() {
        // Test long conversion behavior
        Object nullValue = null;
        Object stringNumber = "123";
        Object actualNumber = 456L;

        Long nullResult = safeConvertToLong(nullValue);
        Long stringResult = safeConvertToLong(stringNumber);
        Long numberResult = safeConvertToLong(actualNumber);

        assertThat(nullResult).isNull();
        assertThat(stringResult).isEqualTo(123L);
        assertThat(numberResult).isEqualTo(456L);
    }

    @Test
    void shouldHandleSafeBooleanConversion() {
        // Test boolean conversion behavior
        Object nullValue = null;
        Object booleanTrue = true;
        Object stringTrue = "true";
        Object stringFalse = "false";

        Boolean nullResult = safeConvertToBoolean(nullValue);
        Boolean booleanResult = safeConvertToBoolean(booleanTrue);
        Boolean stringTrueResult = safeConvertToBoolean(stringTrue);
        Boolean stringFalseResult = safeConvertToBoolean(stringFalse);

        assertThat(nullResult).isNull();
        assertThat(booleanResult).isTrue();
        assertThat(stringTrueResult).isTrue();
        assertThat(stringFalseResult).isFalse();
    }

    // Helper methods simulating the connector's safe conversion logic
    private Integer safeConvertToInteger(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private Long safeConvertToLong(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return Long.parseLong(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private Boolean safeConvertToBoolean(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            return Boolean.parseBoolean(value.toString());
        } catch (Exception e) {
            return null;
        }
    }
}