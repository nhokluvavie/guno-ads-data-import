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

/**
 * PHASE 1: Critical Rate Limit Fix - Updated MetaApiClient
 * Changes:
 * 1. Rate limit từ 200 → 80 requests/hour
 * 2. Minimum delay 10s between requests
 * 3. Circuit breaker cho code 17 errors
 * 4. Timeout tăng từ 30s → 60s
 */
@Component
public class MetaApiClient {
    private static final Logger logger = LoggerFactory.getLogger(MetaApiClient.class);

    @Autowired
    private MetaAdsConfig metaAdsConfig;
    @Autowired
    private MetaApiAuthenticator authenticator;

    // Rate limiting với circuit breaker
    private final Semaphore rateLimitSemaphore;
    private volatile LocalDateTime lastRequestTime = LocalDateTime.now();
    private volatile int requestCount = 0;
    private volatile LocalDateTime hourStart = LocalDateTime.now();

    // Circuit breaker cho rate limit errors
    private volatile boolean circuitBreakerOpen = false;
    private volatile LocalDateTime circuitBreakerResetTime;

    public MetaApiClient() {
        this.rateLimitSemaphore = new Semaphore(3); // Giảm từ 5 → 3
    }

    /**
     * MAIN EXECUTION với rate limit fix
     */
    public <T> T executeRequest(ApiRequest<T> request) throws MetaApiException {
        try {
            acquireRateLimitSafe();
            APIContext context = authenticator.getApiContext();
            T result = request.execute(context);

            // Success - reset circuit breaker
            if (circuitBreakerOpen) {
                circuitBreakerOpen = false;
                logger.info("✅ Circuit breaker reset - API calls resumed");
            }

            return result;
        } catch (Exception e) {
            throw handleApiExceptionWithCircuitBreaker(e);
        } finally {
            rateLimitSemaphore.release();
        }
    }

    /**
     * SAFE RATE LIMITING với circuit breaker
     */
    private void acquireRateLimitSafe() throws InterruptedException, MetaApiException {
        // Check circuit breaker first
        if (circuitBreakerOpen) {
            LocalDateTime now = LocalDateTime.now();
            if (now.isBefore(circuitBreakerResetTime)) {
                long waitSeconds = ChronoUnit.SECONDS.between(now, circuitBreakerResetTime);
                throw new MetaApiException(String.format(
                        "Circuit breaker active - rate limit cooldown for %d seconds", waitSeconds));
            } else {
                circuitBreakerOpen = false;
                logger.info("🔄 Circuit breaker timeout expired - ready to retry");
            }
        }

        LocalDateTime now = LocalDateTime.now();

        // Reset hourly counter
        if (ChronoUnit.HOURS.between(hourStart, now) >= 1) {
            requestCount = 0;
            hourStart = now;
            logger.info("🔄 Hourly rate limit counter reset");
        }

        // Use 80% of configured limit for safety buffer
        int safeLimit = (int) (metaAdsConfig.getRateLimit().getRequestsPerHour() * 0.8);
        if (requestCount >= safeLimit) {
            long waitTime = Math.min(3600 - ChronoUnit.SECONDS.between(hourStart, now), 1800);
            logger.warn("⚠️ Rate limit safety buffer reached, waiting {} seconds", waitTime);
            Thread.sleep(waitTime * 1000);
            requestCount = 0;
            hourStart = LocalDateTime.now();
        }

        // Acquire semaphore với timeout tăng lên
        boolean acquired = rateLimitSemaphore.tryAcquire(60, TimeUnit.SECONDS);
        if (!acquired) {
            throw new MetaApiException("Rate limit semaphore timeout after 60 seconds");
        }

        // Minimum 10-second delay between requests
        long timeSinceLastRequest = ChronoUnit.MILLIS.between(lastRequestTime, now);
        if (timeSinceLastRequest < 10000) {
            long sleepTime = 10000 - timeSinceLastRequest;
            logger.debug("⏱️ Minimum interval wait: {}ms", sleepTime);
            Thread.sleep(sleepTime);
        }

        requestCount++;
        lastRequestTime = LocalDateTime.now();
        logger.debug("📊 Request {}/{} in current hour", requestCount, safeLimit);
    }

    /**
     * ENHANCED EXCEPTION HANDLING với circuit breaker
     */
    private MetaApiException handleApiExceptionWithCircuitBreaker(Exception e) {
        MetaApiException metaEx = convertToMetaApiException(e);

        // Detect rate limit errors (code 17, subcode 2446079)
        if (isRateLimitError(metaEx)) {
            activateCircuitBreaker(metaEx);
        }

        return metaEx;
    }

    /**
     * CIRCUIT BREAKER ACTIVATION
     */
    private void activateCircuitBreaker(MetaApiException e) {
        circuitBreakerOpen = true;

        // Set cooldown period based on error
        if (e.getErrorCode() == 17) { // Account rate limit
            circuitBreakerResetTime = LocalDateTime.now().plusMinutes(15);
            logger.error("🚨 CIRCUIT BREAKER: Account rate limit (code 17) - 15 min cooldown");
        } else { // Other rate limits
            circuitBreakerResetTime = LocalDateTime.now().plusMinutes(5);
            logger.error("⚠️ CIRCUIT BREAKER: Rate limit detected - 5 min cooldown");
        }

        // Reset request counter immediately
        requestCount = 0;
        hourStart = LocalDateTime.now();
    }

    /**
     * RATE LIMIT ERROR DETECTION
     */
    private boolean isRateLimitError(MetaApiException e) {
        return e.getErrorCode() == 17 ||  // User request limit
                e.getErrorCode() == 4 ||   // Application request limit
                e.getErrorCode() == 613 || // Temporary request limit
                (e.getMessage() != null && (
                        e.getMessage().contains("Too Many API Calls") ||
                                e.getMessage().contains("rate limit") ||
                                e.getMessage().contains("User request limit")
                ));
    }

    /**
     * CONVERT EXCEPTION với enhanced error code extraction
     */
    private MetaApiException convertToMetaApiException(Exception e) {
        if (e instanceof APIException) {
            APIException apiEx = (APIException) e;

            // Extract error code from message if available
            String message = apiEx.getMessage();
            int errorCode = extractErrorCodeFromMessage(message);

            return new MetaApiException(
                    message != null ? message : "API request failed",
                    errorCode,
                    "API_ERROR",
                    apiEx.getHeader()
            );
        }

        if (e instanceof MetaApiException) {
            return (MetaApiException) e;
        }

        return new MetaApiException("Unexpected API error: " + e.getMessage(), e);
    }

    /**
     * EXTRACT ERROR CODE từ message text
     */
    private int extractErrorCodeFromMessage(String message) {
        if (message == null) return 0;

        // Look for "code": 17 pattern
        if (message.contains("\"code\": 17") || message.contains("code 17")) {
            return 17;
        }
        if (message.contains("\"code\": 4") || message.contains("code 4")) {
            return 4;
        }
        if (message.contains("\"code\": 613") || message.contains("code 613")) {
            return 613;
        }

        return 0;
    }

    /**
     * ENHANCED RETRY với circuit breaker awareness
     */
    public <T> T executeWithRetry(ApiRequest<T> request) throws MetaApiException {
        int maxRetries = metaAdsConfig.getRateLimit().getRetryAttempts();
        long baseDelay = metaAdsConfig.getRateLimit().getRetryDelayMs();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return executeRequest(request);
            } catch (MetaApiException e) {
                // Don't retry if circuit breaker is open
                if (circuitBreakerOpen) {
                    throw e;
                }

                if (attempt == maxRetries || !shouldRetry(e)) {
                    throw e;
                }

                long delay = calculateDelay(baseDelay, attempt, e);
                logger.warn("🔄 Retry {}/{} in {}ms: {}", attempt, maxRetries, delay, e.getMessage());

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MetaApiException("Request interrupted during retry", ie);
                }
            }
        }

        throw new MetaApiException("Max retries exceeded");
    }

    /**
     * SMART DELAY CALCULATION
     */
    private long calculateDelay(long baseDelay, int attempt, MetaApiException e) {
        if (isRateLimitError(e)) {
            // Longer delays for rate limit errors
            return Math.min(baseDelay * (long) Math.pow(3, attempt), 300000); // Max 5 minutes
        } else {
            // Standard exponential backoff
            return Math.min(baseDelay * (long) Math.pow(2, attempt), 60000); // Max 1 minute
        }
    }

    /**
     * RETRY DECISION LOGIC
     */
    private boolean shouldRetry(MetaApiException e) {
        // Don't retry rate limit errors - let circuit breaker handle
        if (isRateLimitError(e)) {
            return false;
        }

        // Retry server errors and auth issues
        return (e.getErrorCode() >= 500 && e.getErrorCode() < 600) ||
                e.getErrorCode() == 1 || // Temporary API error
                e.getErrorCode() == 2;   // Temporary API error
    }

    /**
     * STATUS MONITORING
     */
    public EnhancedClientStatus getStatus() {
        return new EnhancedClientStatus(
                authenticator.getAuthenticationStatus(),
                requestCount,
                hourStart,
                rateLimitSemaphore.availablePermits(),
                lastRequestTime,
                circuitBreakerOpen,
                circuitBreakerResetTime
        );
    }

    @FunctionalInterface
    public interface ApiRequest<T> {
        T execute(APIContext context) throws Exception;
    }

    public static class EnhancedClientStatus {
        public final MetaApiAuthenticator.AuthenticationStatus authStatus;
        public final int requestCount;
        public final LocalDateTime hourStart;
        public final int availablePermits;
        public final LocalDateTime lastRequestTime;
        public final boolean circuitBreakerOpen;
        public final LocalDateTime circuitBreakerResetTime;

        public EnhancedClientStatus(MetaApiAuthenticator.AuthenticationStatus authStatus,
                                    int requestCount, LocalDateTime hourStart, int availablePermits,
                                    LocalDateTime lastRequestTime, boolean circuitBreakerOpen,
                                    LocalDateTime circuitBreakerResetTime) {
            this.authStatus = authStatus;
            this.requestCount = requestCount;
            this.hourStart = hourStart;
            this.availablePermits = availablePermits;
            this.lastRequestTime = lastRequestTime;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.circuitBreakerResetTime = circuitBreakerResetTime;
        }
    }
}