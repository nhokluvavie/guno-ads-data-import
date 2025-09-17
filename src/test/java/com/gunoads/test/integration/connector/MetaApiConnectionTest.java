package com.gunoads.test.integration.connector;

import com.gunoads.connector.MetaApiAuthenticator;
import com.gunoads.connector.MetaApiClient;
import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.config.MetaAdsConfig;
import com.gunoads.test.integration.BaseIntegrationTest;
import com.facebook.ads.sdk.APIContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.*;

class MetaApiConnectionTest extends BaseIntegrationTest {

    @Autowired
    private MetaAdsConfig metaAdsConfig;

    @Autowired
    private MetaApiAuthenticator authenticator;

    @Autowired
    private MetaApiClient apiClient;

    @Autowired
    private MetaAdsConnector connector;

    @Test
    void shouldHaveValidMetaConfiguration() {
        // Then
        assertThat(metaAdsConfig.getAppId()).isNotEmpty();
        assertThat(metaAdsConfig.getAppSecret()).isNotEmpty();
        assertThat(metaAdsConfig.getAccessToken()).isNotEmpty();
        assertThat(metaAdsConfig.getBusinessId()).isNotEmpty();
        assertThat(metaAdsConfig.isValidConfiguration()).isTrue();
    }

    @Test
    void shouldAuthenticateWithMetaApi() {
        // When
        APIContext context = authenticator.getApiContext();

        // Then
        assertThat(context).isNotNull();
        assertThat(context.getAccessToken()).isNotEmpty();
    }

    @Test
    void shouldGetAuthenticationStatus() {
        // When
        MetaApiAuthenticator.AuthenticationStatus status = authenticator.getAuthenticationStatus();

        // Then
        assertThat(status.isAuthenticated).isTrue();
        assertThat(status.hasValidToken).isTrue();
        assertThat(status.message).isEqualTo("Authenticated successfully");
        assertThat(status.lastValidated).isNotNull();
    }

    @Test
    void shouldRefreshAuthentication() {
        // Given
        authenticator.getApiContext(); // Initial authentication

        // When
        authenticator.refreshAuthentication();
        MetaApiAuthenticator.AuthenticationStatus status = authenticator.getAuthenticationStatus();

        // Then
        assertThat(status.isAuthenticated).isTrue();
        assertThat(status.hasValidToken).isTrue();
    }

    @Test
    void shouldGetApiClientStatus() {
        // When
        MetaApiClient.ClientStatus status = apiClient.getStatus();

        // Then
        assertThat(status).isNotNull();
        assertThat(status.authStatus).isNotNull();
        assertThat(status.authStatus.isAuthenticated).isTrue();
        assertThat(status.availablePermits).isGreaterThan(0);
    }

    @Test
    void shouldTestConnectorConnectivity() {
        // When
        boolean isConnected = connector.testConnectivity();

        // Then
        assertThat(isConnected).isTrue();
    }

    @Test
    void shouldGetConnectorStatus() {
        // When
        MetaApiClient.ClientStatus clientStatus = apiClient.getStatus();
        boolean isConnected = connector.testConnectivity();
        MetaAdsConnector.ConnectorStatus status = new MetaAdsConnector.ConnectorStatus(clientStatus, isConnected);

        // Then
        assertThat(status.clientStatus).isNotNull();
        assertThat(status.isConnected).isTrue();
    }

    @Test
    void shouldHandleRateLimiting() throws InterruptedException {
        // Given
        int requestCount = 5;
        boolean[] results = new boolean[requestCount];

        // When - Make multiple API calls
        for (int i = 0; i < requestCount; i++) {
            try {
                results[i] = connector.testConnectivity();
                Thread.sleep(100); // Small delay between requests
            } catch (Exception e) {
                results[i] = false;
            }
        }

        // Then - All requests should succeed (rate limit should handle gracefully)
        for (int i = 0; i < requestCount; i++) {
            assertThat(results[i]).isTrue()
                    .withFailMessage("Request %d failed", i);
        }
    }

    @Test
    void shouldMaintainConnectionAcrossMultipleCalls() {
        // When
        boolean firstCall = connector.testConnectivity();
        waitFor(1000); // Wait 1 second
        boolean secondCall = connector.testConnectivity();
        waitFor(1000); // Wait 1 second
        boolean thirdCall = connector.testConnectivity();

        // Then
        assertThat(firstCall).isTrue();
        assertThat(secondCall).isTrue();
        assertThat(thirdCall).isTrue();
    }

    @Test
    void shouldValidateApiEndpoints() {
        // When
        String graphUrl = metaAdsConfig.getGraphUrl();
        String accountsEndpoint = metaAdsConfig.getAccountsEndpoint();
        String businessAccountsEndpoint = metaAdsConfig.getBusinessAccountsEndpoint();

        // Then
        assertThat(graphUrl).contains("https://graph.facebook.com");
        assertThat(graphUrl).contains("v23.0");
        assertThat(accountsEndpoint).contains("/me/adaccounts");
        assertThat(businessAccountsEndpoint).contains(metaAdsConfig.getBusinessId());
        assertThat(businessAccountsEndpoint).contains("/adaccounts");
    }

    @Test
    void shouldHandleAuthenticationWithRetry() {
        // Given - Force refresh to test retry mechanism
        authenticator.refreshAuthentication();

        // When
        boolean connectivityAfterRefresh = connector.testConnectivity();

        // Then
        assertThat(connectivityAfterRefresh).isTrue();
    }

    @Test
    void shouldValidateConfigurationEndpoints() {
        // Given
        String testAccountId = "act_123456789";

        // When
        String campaignsEndpoint = metaAdsConfig.getCampaignsEndpoint(testAccountId);
        String adSetsEndpoint = metaAdsConfig.getAdSetsEndpoint(testAccountId);
        String adsEndpoint = metaAdsConfig.getAdsEndpoint(testAccountId);
        String insightsEndpoint = metaAdsConfig.getInsightsEndpoint(testAccountId);

        // Then
        assertThat(campaignsEndpoint).contains("/act_123456789/campaigns");
        assertThat(adSetsEndpoint).contains("/act_123456789/adsets");
        assertThat(adsEndpoint).contains("/act_123456789/ads");
        assertThat(insightsEndpoint).contains("/act_123456789/insights");
    }
}