package com.gunoads.connector;

import com.gunoads.config.MetaAdsConfig;
import com.gunoads.config.MetaApiProperties;
import com.gunoads.exception.MetaApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.facebook.ads.sdk.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Component
public class MetaApiClient {

    private static final Logger logger = LoggerFactory.getLogger(MetaApiClient.class);

    @Autowired
    private MetaAdsConfig metaAdsConfig;

    @Autowired
    private MetaApiProperties metaApiProperties;

    @Autowired
    private MetaApiAuthenticator authenticator;

    // Rate limiting
    private final Semaphore rateLimitSemaphore;
    private LocalDateTime lastRequestTime = LocalDateTime.now();
    private int requestCount = 0;
    private LocalDateTime hourStart = LocalDateTime.now();

    public MetaApiClient() {
        // Initialize rate limiting semaphore - will be updated after config injection
        this.rateLimitSemaphore = new Semaphore(5); // Default, will be updated
    }

    /**
     * Execute API request with rate limiting and error handling
     */
    public <T> T executeRequest(ApiRequest<T> request) throws MetaApiException {
        try {
            // Rate limiting
            acquireRateLimit();

            // Get authenticated context
            APIContext context = authenticator.getApiContext();

            // Execute request
            T result = request.execute(context);

            logger.debug("API request executed successfully");
            return result;

        } catch (Exception e) {
            logger.error("API request failed: {}", e.getMessage());
            throw handleApiException(e);
        } finally {
            releaseRateLimit();
        }
    }

    /**
     * Rate limiting - acquire permission to make request
     */
    private void acquireRateLimit() throws InterruptedException {
        // Check hourly rate limit
        LocalDateTime now = LocalDateTime.now();
        if (ChronoUnit.HOURS.between(hourStart, now) >= 1) {
            // Reset hourly counter
            requestCount = 0;
            hourStart = now;
        }

        if (requestCount >= metaAdsConfig.getRateLimit().getRequestsPerHour()) {
            long waitTime = 3600 - ChronoUnit.SECONDS.between(hourStart, now);
            logger.warn("Hourly rate limit reached, waiting {} seconds", waitTime);
            Thread.sleep(waitTime * 1000);

            // Reset after wait
            requestCount = 0;
            hourStart = LocalDateTime.now();
        }

        // Acquire concurrent request semaphore
        boolean acquired = rateLimitSemaphore.tryAcquire(
                metaAdsConfig.getRateLimit().getRequestTimeoutMs(),
                TimeUnit.MILLISECONDS
        );

        if (!acquired) {
            throw new MetaApiException("Rate limit timeout - too many concurrent requests");
        }

        requestCount++;
        lastRequestTime = LocalDateTime.now();
    }

    /**
     * Release rate limiting semaphore
     */
    private void releaseRateLimit() {
        rateLimitSemaphore.release();
    }

    /**
     * Handle API exceptions and convert to MetaApiException
     */
    private MetaApiException handleApiException(Exception e) {
        if (e instanceof APIException) {
            APIException apiEx = (APIException) e;

            // APIException doesn't have these methods in standard way
            // Use basic information available
            String message = apiEx.getMessage();
            String header = apiEx.getHeader();

            return new MetaApiException(
                    message != null ? message : "API request failed",
                    0, // Error code not directly available
                    "API_ERROR",
                    header // Use header as trace ID
            );
        }

        if (e instanceof MetaApiException) {
            return (MetaApiException) e;
        }

        return new MetaApiException("Unexpected API error", e);
    }

    /**
     * Retry logic for failed requests
     */
    public <T> T executeWithRetry(ApiRequest<T> request) throws MetaApiException {
        int maxRetries = metaAdsConfig.getRateLimit().getRetryAttempts();
        long retryDelay = metaAdsConfig.getRateLimit().getRetryDelayMs();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return executeRequest(request);
            } catch (MetaApiException e) {
                if (attempt == maxRetries || !shouldRetry(e)) {
                    throw e;
                }

                logger.warn("API request failed (attempt {}/{}), retrying in {}ms: {}",
                        attempt, maxRetries, retryDelay, e.getMessage());

                try {
                    Thread.sleep(retryDelay);
                    retryDelay *= 2; // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MetaApiException("Request interrupted during retry", ie);
                }
            }
        }

        throw new MetaApiException("Max retries exceeded");
    }

    /**
     * Determine if exception is retryable
     */
    private boolean shouldRetry(MetaApiException e) {
        return e.isRateLimitError() ||
                (e.getErrorCode() >= 500 && e.getErrorCode() < 600); // Server errors
    }

    /**
     * Get API client status
     */
    public ClientStatus getStatus() {
        return new ClientStatus(
                authenticator.getAuthenticationStatus(),
                requestCount,
                hourStart,
                rateLimitSemaphore.availablePermits(),
                lastRequestTime
        );
    }

    /**
     * Functional interface for API requests
     */
    @FunctionalInterface
    public interface ApiRequest<T> {
        T execute(APIContext context) throws Exception;
    }

    public static class ClientStatus {
        public final MetaApiAuthenticator.AuthenticationStatus authStatus;
        public final int requestCount;
        public final LocalDateTime hourStart;
        public final int availablePermits;
        public final LocalDateTime lastRequestTime;

        public ClientStatus(MetaApiAuthenticator.AuthenticationStatus authStatus,
                            int requestCount, LocalDateTime hourStart,
                            int availablePermits, LocalDateTime lastRequestTime) {
            this.authStatus = authStatus;
            this.requestCount = requestCount;
            this.hourStart = hourStart;
            this.availablePermits = availablePermits;
            this.lastRequestTime = lastRequestTime;
        }
    }
}