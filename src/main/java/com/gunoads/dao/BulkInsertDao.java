package com.gunoads.dao;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

@Repository
public class BulkInsertDao {

    private static final Logger logger = LoggerFactory.getLogger(BulkInsertDao.class);

    @Autowired
    private DataSource dataSource;

    /**
     * COPY FROM using CSV data stream
     */
    @Transactional
    public <T> CopyResult copyFromCsv(String tableName,
                                      List<T> data,
                                      Function<T, String> csvMapper,
                                      String csvHeader) {

        if (data == null || data.isEmpty()) {
            logger.info("No data to copy for table: {}", tableName);
            return new CopyResult(0, 0);
        }

        long startTime = System.currentTimeMillis();
        logger.info("Starting COPY FROM for table: {} with {} records", tableName, data.size());

        try (Connection connection = dataSource.getConnection()) {

            // Cast to PostgreSQL connection
            BaseConnection pgConnection = connection.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);

            // Build COPY command
            String copyCommand = String.format(
                    "COPY %s FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',')",
                    tableName
            );

            // Create CSV content
            StringBuilder csvContent = new StringBuilder();
            csvContent.append(csvHeader).append("\n");

            for (T item : data) {
                csvContent.append(csvMapper.apply(item)).append("\n");
            }

            // Execute COPY FROM
            try (Reader reader = new StringReader(csvContent.toString())) {
                long rowsCopied = copyManager.copyIn(copyCommand, reader);

                long duration = System.currentTimeMillis() - startTime;
                logger.info("COPY FROM completed: {} rows in {}ms ({} rows/sec)",
                        rowsCopied, duration, (rowsCopied * 1000) / Math.max(duration, 1));

                return new CopyResult(rowsCopied, duration);
            }

        } catch (SQLException | IOException e) {
            logger.error("COPY FROM failed for table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("COPY FROM operation failed", e);
        }
    }

    /**
     * COPY FROM with UPSERT (INSERT ... ON CONFLICT)
     */
    @Transactional
    public <T> CopyResult copyFromWithUpsert(String tableName,
                                             String tempTableName,
                                             List<T> data,
                                             Function<T, String> csvMapper,
                                             String csvHeader,
                                             String[] keyColumns,
                                             String[] updateColumns) {

        if (data == null || data.isEmpty()) {
            return new CopyResult(0, 0);
        }

        long startTime = System.currentTimeMillis();
        logger.info("Starting COPY FROM with UPSERT for table: {} with {} records", tableName, data.size());

        try (Connection connection = dataSource.getConnection()) {

            BaseConnection pgConnection = connection.unwrap(BaseConnection.class);
            CopyManager copyManager = new CopyManager(pgConnection);

            // 1. Create temporary table
            String createTempTable = String.format("CREATE TEMP TABLE %s (LIKE %s)", tempTableName, tableName);
            connection.createStatement().execute(createTempTable);
            logger.debug("Created temporary table: {}", tempTableName);

            // 2. COPY data to temp table
            String copyCommand = String.format(
                    "COPY %s FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',')",
                    tempTableName
            );

            StringBuilder csvContent = new StringBuilder();
            csvContent.append(csvHeader).append("\n");
            for (T item : data) {
                csvContent.append(csvMapper.apply(item)).append("\n");
            }

            try (Reader reader = new StringReader(csvContent.toString())) {
                long tempRows = copyManager.copyIn(copyCommand, reader);
                logger.debug("Copied {} rows to temp table", tempRows);
            }

            // 3. UPSERT from temp table to main table
            String upsertSql = buildUpsertSql(tableName, tempTableName, keyColumns, updateColumns);
            int upsertedRows = connection.createStatement().executeUpdate(upsertSql);

            // 4. Drop temp table (auto-dropped at transaction end)
            connection.createStatement().execute("DROP TABLE " + tempTableName);

            long duration = System.currentTimeMillis() - startTime;
            logger.info("COPY FROM with UPSERT completed: {} rows in {}ms", upsertedRows, duration);

            return new CopyResult(upsertedRows, duration);

        } catch (SQLException | IOException e) {
            logger.error("COPY FROM with UPSERT failed: {}", e.getMessage());
            throw new RuntimeException("COPY FROM UPSERT operation failed", e);
        }
    }

    /**
     * Build UPSERT SQL using INSERT ... ON CONFLICT
     */
    private String buildUpsertSql(String mainTable, String tempTable, String[] keyColumns, String[] updateColumns) {
        StringBuilder sql = new StringBuilder();

        sql.append("INSERT INTO ").append(mainTable);
        sql.append(" SELECT * FROM ").append(tempTable);
        sql.append(" ON CONFLICT (").append(String.join(", ", keyColumns)).append(")");

        if (updateColumns.length > 0) {
            sql.append(" DO UPDATE SET ");
            for (int i = 0; i < updateColumns.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(updateColumns[i]).append(" = EXCLUDED.").append(updateColumns[i]);
            }
        } else {
            sql.append(" DO NOTHING");
        }

        return sql.toString();
    }

    /**
     * COPY result summary
     */
    public static class CopyResult {
        public final long rowsProcessed;
        public final long durationMs;
        public final long rowsPerSecond;

        public CopyResult(long rowsProcessed, long durationMs) {
            this.rowsProcessed = rowsProcessed;
            this.durationMs = durationMs;
            this.rowsPerSecond = durationMs > 0 ? (rowsProcessed * 1000) / durationMs : 0;
        }

        @Override
        public String toString() {
            return String.format("CopyResult{rows=%d, duration=%dms, rate=%d rows/sec}",
                    rowsProcessed, durationMs, rowsPerSecond);
        }
    }
}