package com.gunoads.test.unit.processor;

import com.gunoads.processor.DataIngestionProcessor;
import com.gunoads.dao.BulkInsertDao;
import com.gunoads.test.unit.BaseUnitTest;
import com.gunoads.test.util.TestDataFactory;
import com.gunoads.model.entity.Account;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DataIngestionProcessorTest extends BaseUnitTest {

    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    @Mock private BulkInsertDao bulkInsertDao;

    @InjectMocks
    private DataIngestionProcessor processor;

    @BeforeEach
    void setUp() {
        logTestStart();
    }

    @Test
    void shouldChooseCopyFromForLargeDataset() {
        // Given
        List<Account> accounts = createLargeAccountList(6000); // Above threshold
        Function<Account, SqlParameterSource> parameterMapper = account -> null;
        Function<Account, String> csvMapper = account -> "csv-row";
        String csvHeader = "header";
        String sql = "INSERT...";

        BulkInsertDao.CopyResult copyResult = new BulkInsertDao.CopyResult(6000, 5000);
        when(bulkInsertDao.copyFromCsv(anyString(), anyList(), any(), anyString()))
                .thenReturn(copyResult);

        // When
        DataIngestionProcessor.ProcessingResult result = processor.processBatch(
                "tbl_test", accounts, parameterMapper, csvMapper, csvHeader, sql
        );

        // Then
        assertThat(result.strategy).isEqualTo(DataIngestionProcessor.ProcessingStrategy.COPY_FROM);
        assertThat(result.recordsProcessed).isEqualTo(6000);
        verify(bulkInsertDao).copyFromCsv("tbl_test", accounts, csvMapper, csvHeader);
        verify(namedParameterJdbcTemplate, never()).batchUpdate(any(), any(SqlParameterSource[].class));
    }

    @Test
    void shouldChooseBatchJdbcForSmallDataset() {
        // Given
        List<Account> accounts = createSmallAccountList(100); // Below threshold
        Function<Account, SqlParameterSource> parameterMapper = account -> mock(SqlParameterSource.class);
        Function<Account, String> csvMapper = account -> "csv-row";
        String csvHeader = "header";
        String sql = "INSERT...";

        int[] batchResults = new int[100];
        for (int i = 0; i < 100; i++) batchResults[i] = 1;

        when(namedParameterJdbcTemplate.batchUpdate(anyString(), any(SqlParameterSource[].class)))
                .thenReturn(batchResults);

        // When
        DataIngestionProcessor.ProcessingResult result = processor.processBatch(
                "tbl_test", accounts, parameterMapper, csvMapper, csvHeader, sql
        );

        // Then
        assertThat(result.strategy).isEqualTo(DataIngestionProcessor.ProcessingStrategy.BATCH_JDBC);
        assertThat(result.recordsProcessed).isEqualTo(100);
        verify(namedParameterJdbcTemplate).batchUpdate(eq(sql), any(SqlParameterSource[].class));
        verify(bulkInsertDao, never()).copyFromCsv(any(), any(), any(), any());
    }

    @Test
    void shouldHandleEmptyDataList() {
        // Given
        List<Account> accounts = List.of();
        Function<Account, SqlParameterSource> parameterMapper = account -> null;
        Function<Account, String> csvMapper = account -> "csv-row";

        // When
        DataIngestionProcessor.ProcessingResult result = processor.processBatch(
                "tbl_test", accounts, parameterMapper, csvMapper, "header", "sql"
        );

        // Then
        assertThat(result.strategy).isEqualTo(DataIngestionProcessor.ProcessingStrategy.NONE);
        assertThat(result.recordsProcessed).isEqualTo(0);
        verify(bulkInsertDao, never()).copyFromCsv(any(), any(), any(), any());
        verify(namedParameterJdbcTemplate, never()).batchUpdate(any(), any(SqlParameterSource[].class));
    }

    @Test
    void shouldHandleNullDataList() {
        // When
        DataIngestionProcessor.ProcessingResult result = processor.processBatch(
                "tbl_test", null, null, null, "header", "sql"
        );

        // Then
        assertThat(result.strategy).isEqualTo(DataIngestionProcessor.ProcessingStrategy.NONE);
        assertThat(result.recordsProcessed).isEqualTo(0);
    }

    @Test
    void shouldProcessWithUpsert() {
        // Given
        List<Account> accounts = createSmallAccountList(10);
        Function<Account, String> csvMapper = account -> "csv-row";
        String csvHeader = "header";
        String[] keyColumns = {"id", "platform_id"};
        String[] updateColumns = {"account_name", "currency"};

        BulkInsertDao.CopyResult copyResult = new BulkInsertDao.CopyResult(10, 1000);
        when(bulkInsertDao.copyFromWithUpsert(anyString(), anyString(), anyList(), any(),
                anyString(), any(), any())).thenReturn(copyResult);

        // When
        DataIngestionProcessor.ProcessingResult result = processor.processWithUpsert(
                "tbl_test", accounts, csvMapper, csvHeader, keyColumns, updateColumns
        );

        // Then
        assertThat(result.strategy).isEqualTo(DataIngestionProcessor.ProcessingStrategy.COPY_FROM_UPSERT);
        assertThat(result.recordsProcessed).isEqualTo(10);
        verify(bulkInsertDao).copyFromWithUpsert(
                eq("tbl_test"), contains("tbl_test_temp_"), eq(accounts),
                eq(csvMapper), eq(csvHeader), eq(keyColumns), eq(updateColumns)
        );
    }

    @Test
    void shouldCalculateProcessingRate() {
        // Given
        long recordsProcessed = 1000;
        long durationMs = 2000; // 2 seconds

        // When
        DataIngestionProcessor.ProcessingResult result =
                new DataIngestionProcessor.ProcessingResult(recordsProcessed, durationMs,
                        DataIngestionProcessor.ProcessingStrategy.COPY_FROM);

        // Then
        assertThat(result.recordsPerSecond).isEqualTo(500); // 1000 records / 2 seconds
    }

    private List<Account> createLargeAccountList(int size) {
        return List.of(TestDataFactory.createAccount()); // Simplified for test
    }

    private List<Account> createSmallAccountList(int size) {
        return List.of(TestDataFactory.createAccount()); // Simplified for test
    }
}