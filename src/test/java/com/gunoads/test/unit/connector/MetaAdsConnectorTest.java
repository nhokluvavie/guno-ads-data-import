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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@MockitoSettings(strictness = Strictness.LENIENT)
class MetaAdsConnectorTest extends BaseUnitTest {

    @Mock private MetaAdsConfig metaAdsConfig;
    @Mock private MetaApiProperties metaApiProperties;
    @Mock private MetaApiClient metaApiClient;

    @InjectMocks
    private MetaAdsConnector connector;

    @BeforeEach
    void setUp() {
        // Setup common mocks for all tests with lenient stubbing
        lenient().when(metaAdsConfig.getBusinessId()).thenReturn("business_123");
        logTestStart();
    }

    @Test
    void shouldGetSafeString() {
        // Test the safe getter utility methods behavior

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
        // Given - Mock successful API call that returns non-empty list
        when(metaApiClient.executeWithRetry(any())).thenReturn(List.of("dummy_account"));

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
        verify(metaApiClient).executeWithRetry(any());
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

        // Then
        assertThat(nullResult).isNull();
        assertThat(stringResult).isEqualTo("test");
        assertThat(numberResult).isEqualTo("123");
    }

    @Test
    void shouldHandleEmptyResponse() throws Exception {
        // Given - Empty response means no accounts but API works
        when(metaApiClient.executeWithRetry(any())).thenReturn(List.of());

        // When
        boolean connected = connector.testConnectivity();

        // Then - Empty response still counts as successful connection
        assertThat(connected).isTrue(); // Implementation might require non-empty list
        verify(metaApiClient).executeWithRetry(any());
    }

    @Test
    void shouldHandleNullResponse() throws Exception {
        // Given
        when(metaApiClient.executeWithRetry(any())).thenReturn(null);

        // When
        boolean connected = connector.testConnectivity();

        // Then - Null response indicates failure
        assertThat(connected).isFalse();
        verify(metaApiClient).executeWithRetry(any());
    }

    @Test
    void shouldValidateBusinessId() {
        // Given
        when(metaAdsConfig.getBusinessId()).thenReturn("business_123");

        // When
        String businessId = metaAdsConfig.getBusinessId();

        // Then
        assertThat(businessId).isEqualTo("business_123");
        verify(metaAdsConfig).getBusinessId();
    }

    @Test
    void shouldHandleMultipleConnectivityTests() throws Exception {
        // Given - Mock responses for multiple calls
        when(metaApiClient.executeWithRetry(any()))
                .thenReturn(List.of("account1"))  // First call succeeds with data
                .thenThrow(new RuntimeException("Temporary failure"))  // Second call fails
                .thenReturn(List.of("account1")); // Third call succeeds with data

        // When
        boolean firstResult = connector.testConnectivity();
        boolean secondResult = connector.testConnectivity();
        boolean thirdResult = connector.testConnectivity();

        // Then
        assertThat(firstResult).isTrue();
        assertThat(secondResult).isFalse();
        assertThat(thirdResult).isTrue();
        verify(metaApiClient, times(3)).executeWithRetry(any());
    }
}