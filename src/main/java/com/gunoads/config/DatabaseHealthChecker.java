package com.gunoads.config;

import com.gunoads.util.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class DatabaseHealthChecker {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseHealthChecker.class);

    @Autowired
    private ConnectionManager connectionManager;

    public HealthStatus checkHealth() {
        try {
            if (!connectionManager.testConnection()) {
                return new HealthStatus(false, "Database connection failed", null);
            }

            if (!connectionManager.validateSchema()) {
                return new HealthStatus(false, "Required tables not found", null);
            }

            ConnectionManager.DatabaseInfo dbInfo = connectionManager.getDatabaseInfo();
            ConnectionManager.PoolStats poolStats = connectionManager.getPoolStats();

            Map<String, Object> details = new HashMap<>();
            details.put("database", dbInfo != null ? dbInfo.database : "unknown");
            details.put("user", dbInfo != null ? dbInfo.user : "unknown");

            if (poolStats != null) {
                details.put("pool.total", poolStats.totalConnections);
                details.put("pool.active", poolStats.activeConnections);
                details.put("pool.idle", poolStats.idleConnections);
                details.put("pool.waiting", poolStats.threadsAwaiting);
            }

            return new HealthStatus(true, "Database is healthy", details);

        } catch (Exception e) {
            logger.error("Health check failed: {}", e.getMessage());
            return new HealthStatus(false, e.getMessage(), null);
        }
    }

    public static class HealthStatus {
        public final boolean isHealthy;
        public final String message;
        public final Map<String, Object> details;

        public HealthStatus(boolean isHealthy, String message, Map<String, Object> details) {
            this.isHealthy = isHealthy;
            this.message = message;
            this.details = details;
        }

        @Override
        public String toString() {
            return String.format("HealthStatus{healthy=%s, message='%s', details=%s}",
                    isHealthy, message, details);
        }
    }
}