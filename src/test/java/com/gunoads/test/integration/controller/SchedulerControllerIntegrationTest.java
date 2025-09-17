package com.gunoads.test.integration.controller;

import com.gunoads.service.MetaAdsService;
import com.gunoads.test.integration.BaseIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebMvc
class SchedulerControllerIntegrationTest extends BaseIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private String getBaseUrl() {
        return "http://localhost:" + port + "/api/scheduler";
    }

    @Test
    void shouldTriggerManualSync() {
        // When
        ResponseEntity<String> response = restTemplate.postForEntity(
                getBaseUrl() + "/sync/manual", null, String.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("status");
        assertThat(response.getBody()).contains("success");
        assertThat(response.getBody()).contains("Manual sync triggered");
    }

    @Test
    void shouldTriggerHierarchySync() {
        // When
        ResponseEntity<String> response = restTemplate.postForEntity(
                getBaseUrl() + "/sync/hierarchy", null, String.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("status");
        assertThat(response.getBody()).contains("success");
        assertThat(response.getBody()).contains("Hierarchy sync completed");
    }

    @Test
    void shouldTriggerPerformanceSync() {
        // When
        ResponseEntity<String> response = restTemplate.postForEntity(
                getBaseUrl() + "/sync/performance", null, String.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("status");
        assertThat(response.getBody()).contains("success");
        assertThat(response.getBody()).contains("Performance sync completed");
    }

    @Test
    void shouldGetSystemStatus() {
        // When
        ResponseEntity<String> response = restTemplate.getForEntity(
                getBaseUrl() + "/status", String.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("isConnected");
        assertThat(response.getBody()).contains("accountCount");
        assertThat(response.getBody()).contains("campaignCount");
    }

    @Test
    void shouldParseStatusResponse() throws Exception {
        // When
        ResponseEntity<String> response = restTemplate.getForEntity(
                getBaseUrl() + "/status", String.class
        );

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        Map<String, Object> statusMap = objectMapper.readValue(response.getBody(), Map.class);
        assertThat(statusMap).containsKey("isConnected");
        assertThat(statusMap).containsKey("accountCount");
        assertThat(statusMap).containsKey("campaignCount");
        assertThat(statusMap).containsKey("adSetCount");
        assertThat(statusMap).containsKey("adCount");
        assertThat(statusMap).containsKey("reportingCount");

        assertThat(statusMap.get("isConnected")).isInstanceOf(Boolean.class);
        assertThat(statusMap.get("accountCount")).isInstanceOf(Number.class);
    }

    @Test
    void shouldHandleMultipleConcurrentRequests() throws InterruptedException {
        // Given
        int requestCount = 3;
        Thread[] threads = new Thread[requestCount];
        ResponseEntity<String>[] responses = new ResponseEntity[requestCount];

        // When - Make concurrent requests
        for (int i = 0; i < requestCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                responses[index] = restTemplate.getForEntity(
                        getBaseUrl() + "/status", String.class
                );
            });
            threads[i].start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join(10000); // 10 second timeout
        }

        // Then - All requests should succeed
        for (int i = 0; i < requestCount; i++) {
            assertThat(responses[i]).isNotNull();
            assertThat(responses[i].getStatusCode()).isEqualTo(HttpStatus.OK);
        }
    }

    @Test
    void shouldReturnConsistentResponseFormat() {
        // When
        ResponseEntity<String> manualResponse = restTemplate.postForEntity(
                getBaseUrl() + "/sync/manual", null, String.class
        );
        ResponseEntity<String> hierarchyResponse = restTemplate.postForEntity(
                getBaseUrl() + "/sync/hierarchy", null, String.class
        );
        ResponseEntity<String> performanceResponse = restTemplate.postForEntity(
                getBaseUrl() + "/sync/performance", null, String.class
        );

        // Then - All should have consistent format
        assertThat(manualResponse.getBody()).contains("\"status\":");
        assertThat(manualResponse.getBody()).contains("\"message\":");

        assertThat(hierarchyResponse.getBody()).contains("\"status\":");
        assertThat(hierarchyResponse.getBody()).contains("\"message\":");

        assertThat(performanceResponse.getBody()).contains("\"status\":");
        assertThat(performanceResponse.getBody()).contains("\"message\":");
    }

    @Test
    void shouldHaveWorkingHealthEndpoints() {
        // When
        ResponseEntity<String> healthResponse = restTemplate.getForEntity(
                "http://localhost:" + port + "/actuator/health", String.class
        );

        // Then
        assertThat(healthResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(healthResponse.getBody()).contains("status");
    }

    @Test
    void shouldHandleSyncTimeout() {
        // Given - Set reasonable timeout expectation
        long startTime = System.currentTimeMillis();

        // When
        ResponseEntity<String> response = restTemplate.postForEntity(
                getBaseUrl() + "/sync/hierarchy", null, String.class
        );
        long duration = System.currentTimeMillis() - startTime;

        // Then - Should complete within reasonable time
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(duration).isLessThan(120000); // 2 minutes max
    }

    @Test
    void shouldValidateAllEndpointsExist() {
        // Test all endpoints are accessible

        // Manual sync
        ResponseEntity<String> manual = restTemplate.postForEntity(
                getBaseUrl() + "/sync/manual", null, String.class
        );
        assertThat(manual.getStatusCode()).isIn(HttpStatus.OK, HttpStatus.INTERNAL_SERVER_ERROR);

        // Hierarchy sync
        ResponseEntity<String> hierarchy = restTemplate.postForEntity(
                getBaseUrl() + "/sync/hierarchy", null, String.class
        );
        assertThat(hierarchy.getStatusCode()).isIn(HttpStatus.OK, HttpStatus.INTERNAL_SERVER_ERROR);

        // Performance sync
        ResponseEntity<String> performance = restTemplate.postForEntity(
                getBaseUrl() + "/sync/performance", null, String.class
        );
        assertThat(performance.getStatusCode()).isIn(HttpStatus.OK, HttpStatus.INTERNAL_SERVER_ERROR);

        // Status
        ResponseEntity<String> status = restTemplate.getForEntity(
                getBaseUrl() + "/status", String.class
        );
        assertThat(status.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void shouldReturnValidJsonResponses() throws Exception {
        // When
        ResponseEntity<String> statusResponse = restTemplate.getForEntity(
                getBaseUrl() + "/status", String.class
        );
        ResponseEntity<String> syncResponse = restTemplate.postForEntity(
                getBaseUrl() + "/sync/manual", null, String.class
        );

        // Then - Should be valid JSON
        assertThatCode(() -> objectMapper.readValue(statusResponse.getBody(), Map.class))
                .doesNotThrowAnyException();
        assertThatCode(() -> objectMapper.readValue(syncResponse.getBody(), Map.class))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldHandleSequentialSyncRequests() {
        // When - Make sequential requests
        ResponseEntity<String> first = restTemplate.postForEntity(
                getBaseUrl() + "/sync/hierarchy", null, String.class
        );
        waitFor(1000); // Small delay

        ResponseEntity<String> second = restTemplate.postForEntity(
                getBaseUrl() + "/sync/performance", null, String.class
        );
        waitFor(1000); // Small delay

        ResponseEntity<String> third = restTemplate.getForEntity(
                getBaseUrl() + "/status", String.class
        );

        // Then - All should succeed
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(third.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}