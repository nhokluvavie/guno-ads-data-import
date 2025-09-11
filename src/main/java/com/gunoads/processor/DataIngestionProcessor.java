package com.gunoads.processor;

import com.gunoads.dao.BulkInsertDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.function.Function;

@Component
public class DataIngestionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionProcessor.class);

    @Value("${batch.processing.copy-from-threshold:5000}")
    private int copyFromThreshold;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    private BulkInsertDao bulkInsertDao;

    /**
     * Smart batch processing - chooses best strategy based on data size
     */
    @Transactional
    public <T> ProcessingResult processBatch(String tableName,
                                             List<T> data,
                                             Function<T, SqlParameterSource> parameterMapper,
                                             Function<T, String> csvMapper,
                                             String csvHeader,
                                             String sql) {

        if (data == null || data.isEmpty()) {
            logger.info("No data to process for table: {}", tableName);
            return new ProcessingResult(0, 0, ProcessingStrategy.NONE);
        }

        ProcessingStrategy strategy = chooseStrategy(data.size());
        logger.info("Processing {} records for table {} using strategy: {}",
                data.size(), tableName, strategy);

        long startTime = System.currentTimeMillis();

        try {
            switch (strategy) {
                case COPY_FROM:
                    return processByCopyFrom(tableName, data, csvMapper, csvHeader, startTime);

                case BATCH_JDBC:
                    return processByBatchJdbc(data, parameterMapper, sql, startTime);

                default:
                    return new ProcessingResult(0, 0, ProcessingStrategy.NONE);
            }
        } catch (Exception e) {
            logger.error("Batch processing failed for table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Batch processing failed", e);
        }
    }

    /**
     * COPY FROM with UPSERT capability
     */
    @Transactional
    public <T> ProcessingResult processWithUpsert(String tableName,
                                                  List<T> data,
                                                  Function<T, String> csvMapper,
                                                  String csvHeader,
                                                  String[] keyColumns,
                                                  String[] updateColumns) {

        if (data == null || data.isEmpty()) {
            return new ProcessingResult(0, 0, ProcessingStrategy.NONE);
        }

        long startTime = System.currentTimeMillis();
        logger.info("Processing {} records with UPSERT for table: {}", data.size(), tableName);

        try {
            String tempTableName = tableName + "_temp_" + System.currentTimeMillis();

            BulkInsertDao.CopyResult result = bulkInsertDao.copyFromWithUpsert(
                    tableName, tempTableName, data, csvMapper, csvHeader, keyColumns, updateColumns
            );

            long duration = System.currentTimeMillis() - startTime;
            return new ProcessingResult(result.rowsProcessed, duration, ProcessingStrategy.COPY_FROM_UPSERT);

        } catch (Exception e) {
            logger.error("UPSERT processing failed for table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("UPSERT processing failed", e);
        }
    }

    /**
     * Choose optimal processing strategy based on data size
     */
    private ProcessingStrategy chooseStrategy(int dataSize) {
        if (dataSize >= copyFromThreshold) {
            return ProcessingStrategy.COPY_FROM;
        } else {
            return ProcessingStrategy.BATCH_JDBC;
        }
    }

    /**
     * Process using COPY FROM
     */
    private <T> ProcessingResult processByCopyFrom(String tableName,
                                                   List<T> data,
                                                   Function<T, String> csvMapper,
                                                   String csvHeader,
                                                   long startTime) {

        BulkInsertDao.CopyResult result = bulkInsertDao.copyFromCsv(
                tableName, data, csvMapper, csvHeader
        );

        long duration = System.currentTimeMillis() - startTime;
        return new ProcessingResult(result.rowsProcessed, duration, ProcessingStrategy.COPY_FROM);
    }

    /**
     * Process using Batch JDBC
     */
    private <T> ProcessingResult processByBatchJdbc(List<T> data,
                                                    Function<T, SqlParameterSource> parameterMapper,
                                                    String sql,
                                                    long startTime) {

        SqlParameterSource[] batchParams = data.stream()
                .map(parameterMapper)
                .toArray(SqlParameterSource[]::new);

        int[] results = namedParameterJdbcTemplate.batchUpdate(sql, batchParams);

        long successCount = 0;
        for (int result : results) {
            if (result > 0) successCount++;
        }

        long duration = System.currentTimeMillis() - startTime;
        return new ProcessingResult(successCount, duration, ProcessingStrategy.BATCH_JDBC);
    }

    /**
     * Processing strategies
     */
    public enum ProcessingStrategy {
        NONE,
        BATCH_JDBC,
        COPY_FROM,
        COPY_FROM_UPSERT
    }

    /**
     * Processing result
     */
    public static class ProcessingResult {
        public final long recordsProcessed;
        public final long durationMs;
        public final ProcessingStrategy strategy;
        public final long recordsPerSecond;

        public ProcessingResult(long recordsProcessed, long durationMs, ProcessingStrategy strategy) {
            this.recordsProcessed = recordsProcessed;
            this.durationMs = durationMs;
            this.strategy = strategy;
            this.recordsPerSecond = durationMs > 0 ? (recordsProcessed * 1000) / durationMs : 0;
        }

        @Override
        public String toString() {
            return String.format("ProcessingResult{records=%d, duration=%dms, strategy=%s, rate=%d/sec}",
                    recordsProcessed, durationMs, strategy, recordsPerSecond);
        }
    }
}