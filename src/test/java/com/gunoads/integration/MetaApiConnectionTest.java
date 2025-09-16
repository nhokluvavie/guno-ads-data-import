package com.gunoads.integration;

import com.gunoads.AbstractIntegrationTest;
import com.gunoads.config.MetaAdsConfig;
import com.gunoads.connector.MetaApiAuthenticator;
import com.gunoads.connector.MetaApiClient;
import com.gunoads.connector.MetaAdsConnector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.*;

class MetaApiConnectionTest extends AbstractIntegrationTest {

    @Autowired
    private MetaAdsConfig metaAdsConfig;

    @Autowired
    private MetaApiAuthenticator authenticator;

    @Autowired
    private MetaApiClient apiClient;

    @Autowired
    private MetaAdsConnector connector;

    @Test
    void shouldHaveValidConfiguration() {
        System.out.println("Testing Meta API configuration...");

        assertThat(metaAdsConfig.getAppId()).isNotEmpty();
        assertThat(metaAdsConfig.getAppSecret()).isNotEmpty();
        assertThat(metaAdsConfig.getAccessToken()).isNotEmpty();
        assertThat(metaAdsConfig.getBusinessId()).isNotEmpty();
        assertThat(metaAdsConfig.isValidConfiguration()).isTrue();

        System.out.println("✅ Configuration is valid");
        System.out.printf("   App ID: %s\n", metaAdsConfig.getAppId());
        System.out.printf("   Business ID: %s\n", metaAdsConfig.getBusinessId());
        System.out.printf("   API Version: %s\n", metaAdsConfig.getApiVersion());
    }

    @Test
    void shouldAuthenticateSuccessfully() {
        System.out.println("Testing Meta API authentication...");

        var authStatus = authenticator.getAuthenticationStatus();

        assertThat(authStatus.isAuthenticated).isTrue();
        assertThat(authStatus.hasValidToken).isTrue();
        assertThat(authStatus.message).contains("success");

        System.out.println("✅ Authentication successful");
        System.out.printf("   Status: %s\n", authStatus);
    }

    @Test
    void shouldGetClientStatus() {
        System.out.println("Testing API client status...");

        var clientStatus = apiClient.getStatus();

        assertThat(clientStatus).isNotNull();
        assertThat(clientStatus.authStatus.isAuthenticated).isTrue();
        assertThat(clientStatus.availablePermits).isGreaterThan(0);

        System.out.println("✅ Client status is healthy");
        System.out.printf("   Available permits: %d\n", clientStatus.availablePermits);
        System.out.printf("   Request count: %d\n", clientStatus.requestCount);
    }

    @Test
    void shouldTestConnectivity() {
        System.out.println("Testing Meta API connectivity...");

        boolean isConnected = connector.testConnectivity();

        assertThat(isConnected).isTrue();
        System.out.println("✅ Meta API connectivity successful");
    }

    @Test
    void shouldHandleRateLimit() {
        System.out.println("Testing rate limit configuration...");

        var rateLimit = metaAdsConfig.getRateLimit();

        assertThat(rateLimit.getRequestsPerHour()).isGreaterThan(0);
        assertThat(rateLimit.getRetryAttempts()).isGreaterThan(0);
        assertThat(rateLimit.getRetryDelayMs()).isGreaterThan(0);

        System.out.println("✅ Rate limit configuration valid");
        System.out.printf("   Requests/hour: %d\n", rateLimit.getRequestsPerHour());
        System.out.printf("   Retry attempts: %d\n", rateLimit.getRetryAttempts());
    }
}