package com.gunoads.connector;

import com.gunoads.config.MetaAdsConfig;
import com.gunoads.exception.MetaApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.facebook.ads.sdk.APIContext;

import java.time.LocalDateTime;

@Component
public class MetaApiAuthenticator {

    private static final Logger logger = LoggerFactory.getLogger(MetaApiAuthenticator.class);

    @Autowired
    private MetaAdsConfig metaAdsConfig;

    private APIContext apiContext;
    private LocalDateTime lastValidated;
    private static final int VALIDATION_INTERVAL_MINUTES = 30;

    /**
     * Get authenticated API context
     */
    public APIContext getApiContext() {
        if (apiContext == null || needsRevalidation()) {
            authenticate();
        }
        return apiContext;
    }

    /**
     * Authenticate and create API context
     */
    private void authenticate() {
        try {
            logger.info("Authenticating Meta API with app ID: {}", metaAdsConfig.getAppId());

            // Validate configuration
            if (!metaAdsConfig.isValidConfiguration()) {
                throw new MetaApiException("Invalid Meta API configuration - missing required credentials");
            }

            // Create API context - standard constructor
            apiContext = new APIContext(
                    metaAdsConfig.getAccessToken(),
                    metaAdsConfig.getAppSecret()
            );

            // Enable debug mode for testing
            apiContext.enableDebug(true);

            // Test authentication
            validateAuthentication();

            lastValidated = LocalDateTime.now();
            logger.info("Meta API authentication successful");

        } catch (Exception e) {
            logger.error("Meta API authentication failed: {}", e.getMessage());
            throw new MetaApiException("Authentication failed", e);
        }
    }

    /**
     * Validate authentication by making a test call
     */
    private void validateAuthentication() {
        try {
            // Simple validation call - get user info
            // This will throw exception if authentication fails
            logger.debug("Validating Meta API authentication...");

            // Note: Actual validation call will be implemented in next sub-phase
            // For now, just validate that we have required credentials

        } catch (Exception e) {
            throw new MetaApiException("Authentication validation failed", e);
        }
    }

    /**
     * Check if re-authentication is needed
     */
    private boolean needsRevalidation() {
        return lastValidated == null ||
                lastValidated.isBefore(LocalDateTime.now().minusMinutes(VALIDATION_INTERVAL_MINUTES));
    }

    /**
     * Force re-authentication
     */
    public void refreshAuthentication() {
        logger.info("Refreshing Meta API authentication");
        apiContext = null;
        authenticate();
    }

    /**
     * Get current authentication status
     */
    public AuthenticationStatus getAuthenticationStatus() {
        try {
            APIContext context = getApiContext();
            return new AuthenticationStatus(
                    true,
                    "Authenticated successfully",
                    context.getAccessToken() != null,
                    lastValidated
            );
        } catch (Exception e) {
            return new AuthenticationStatus(
                    false,
                    e.getMessage(),
                    false,
                    null
            );
        }
    }

    public static class AuthenticationStatus {
        public final boolean isAuthenticated;
        public final String message;
        public final boolean hasValidToken;
        public final LocalDateTime lastValidated;

        public AuthenticationStatus(boolean isAuthenticated, String message,
                                    boolean hasValidToken, LocalDateTime lastValidated) {
            this.isAuthenticated = isAuthenticated;
            this.message = message;
            this.hasValidToken = hasValidToken;
            this.lastValidated = lastValidated;
        }

        @Override
        public String toString() {
            return String.format("AuthStatus{authenticated=%s, token=%s, validated=%s}",
                    isAuthenticated, hasValidToken, lastValidated);
        }
    }
}