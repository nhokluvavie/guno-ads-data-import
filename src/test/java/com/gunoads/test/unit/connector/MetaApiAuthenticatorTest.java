package com.gunoads.test.unit.connector;

import com.gunoads.connector.MetaApiAuthenticator;
import com.gunoads.config.MetaAdsConfig;
import com.gunoads.exception.MetaApiException;
import com.gunoads.test.unit.BaseUnitTest;
import com.facebook.ads.sdk.APIContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class MetaApiAuthenticatorTest extends BaseUnitTest {

    @Mock
    private MetaAdsConfig metaAdsConfig;

    @InjectMocks
    private MetaApiAuthenticator authenticator;

    @BeforeEach
    void setUp() {
        // Setup valid configuration
        when(metaAdsConfig.getAppId()).thenReturn("test_app_id");
        when(metaAdsConfig.getAppSecret()).thenReturn("test_app_secret");
        when(metaAdsConfig.getAccessToken()).thenReturn("test_access_token");
        when(metaAdsConfig.isValidConfiguration()).thenReturn(true);
        logTestStart();
    }

    @Test
    void shouldCreateApiContext() {
        // When
        APIContext context = authenticator.getApiContext();

        // Then
        assertThat(context).isNotNull();
        verify(metaAdsConfig).isValidConfiguration();
        verify(metaAdsConfig).getAccessToken();
        verify(metaAdsConfig).getAppSecret();
    }

    @Test
    void shouldReuseExistingApiContext() {
        // Given
        APIContext firstContext = authenticator.getApiContext();

        // When
        APIContext secondContext = authenticator.getApiContext();

        // Then
        assertThat(secondContext).isSameAs(firstContext);
        verify(metaAdsConfig, times(1)).isValidConfiguration(); // Only called once
    }

    @Test
    void shouldThrowExceptionForInvalidConfiguration() {
        // Given
        when(metaAdsConfig.isValidConfiguration()).thenReturn(false);

        // When & Then
        assertThatThrownBy(() -> authenticator.getApiContext())
                .isInstanceOf(MetaApiException.class)
                .hasMessageContaining("Invalid Meta API configuration");
    }

    @Test
    void shouldRefreshAuthenticationWhenExpired() throws Exception {
        // Given
        APIContext oldContext = authenticator.getApiContext();

        // Simulate time passing (force revalidation by calling refresh)
        authenticator.refreshAuthentication();

        // When
        APIContext newContext = authenticator.getApiContext();

        // Then
        assertThat(newContext).isNotNull();
        // Should create new context after refresh
        verify(metaAdsConfig, atLeast(2)).isValidConfiguration();
    }

    @Test
    void shouldRefreshAuthenticationManually() {
        // Given
        authenticator.getApiContext(); // Initial authentication

        // When
        authenticator.refreshAuthentication();

        // Then
        // Should create new context on next call
        APIContext newContext = authenticator.getApiContext();
        assertThat(newContext).isNotNull();
        verify(metaAdsConfig, atLeast(2)).isValidConfiguration();
    }

    @Test
    void shouldGetAuthenticationStatusWhenAuthenticated() {
        // Given
        authenticator.getApiContext(); // Trigger authentication

        // When
        MetaApiAuthenticator.AuthenticationStatus status = authenticator.getAuthenticationStatus();

        // Then
        assertThat(status.isAuthenticated).isTrue();
        assertThat(status.hasValidToken).isTrue();
        assertThat(status.message).isEqualTo("Authenticated successfully");
        assertThat(status.lastValidated).isNotNull();
    }

    @Test
    void shouldGetAuthenticationStatusWhenNotAuthenticated() {
        // Given
        when(metaAdsConfig.isValidConfiguration()).thenReturn(false);

        // When
        MetaApiAuthenticator.AuthenticationStatus status = authenticator.getAuthenticationStatus();

        // Then
        assertThat(status.isAuthenticated).isFalse();
        assertThat(status.hasValidToken).isFalse();
        assertThat(status.message).contains("Invalid Meta API configuration");
        assertThat(status.lastValidated).isNull();
    }

    @Test
    void shouldHandleAuthenticationException() {
        // Given
        when(metaAdsConfig.isValidConfiguration()).thenReturn(true);
        when(metaAdsConfig.getAccessToken()).thenReturn(null); // Invalid token

        // When & Then
        assertThatThrownBy(() -> authenticator.getApiContext())
                .isInstanceOf(MetaApiException.class)
                .hasMessageContaining("Authentication failed");
    }

    @Test
    void shouldCreateAuthenticationStatusCorrectly() {
        // Given
        boolean isAuthenticated = true;
        String message = "Test message";
        boolean hasValidToken = true;
        LocalDateTime lastValidated = LocalDateTime.now();

        // When
        MetaApiAuthenticator.AuthenticationStatus status =
                new MetaApiAuthenticator.AuthenticationStatus(isAuthenticated, message, hasValidToken, lastValidated);

        // Then
        assertThat(status.isAuthenticated).isTrue();
        assertThat(status.message).isEqualTo("Test message");
        assertThat(status.hasValidToken).isTrue();
        assertThat(status.lastValidated).isEqualTo(lastValidated);
    }

    @Test
    void shouldToStringAuthenticationStatus() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        MetaApiAuthenticator.AuthenticationStatus status =
                new MetaApiAuthenticator.AuthenticationStatus(true, "Test", true, now);

        // When
        String result = status.toString();

        // Then
        assertThat(result).contains("authenticated=true");
        assertThat(result).contains("token=true");
        assertThat(result).contains("validated=" + now);
    }

    @Test
    void shouldHandleConfigurationChanges() {
        // Given
        authenticator.getApiContext(); // Initial authentication

        // When configuration changes
        when(metaAdsConfig.getAccessToken()).thenReturn("new_access_token");
        authenticator.refreshAuthentication();

        // When
        APIContext newContext = authenticator.getApiContext();

        // Then
        assertThat(newContext).isNotNull();
        verify(metaAdsConfig, atLeast(2)).getAccessToken();
    }

    @Test
    void shouldValidateConfigurationFields() {
        // Given
        when(metaAdsConfig.getAppId()).thenReturn("");
        when(metaAdsConfig.isValidConfiguration()).thenReturn(false);

        // When & Then
        assertThatThrownBy(() -> authenticator.getApiContext())
                .isInstanceOf(MetaApiException.class)
                .hasMessageContaining("Invalid Meta API configuration");
    }
}