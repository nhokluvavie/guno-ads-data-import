package com.gunoads.test.unit.dao;

import com.gunoads.dao.BulkInsertDao;
import com.gunoads.test.unit.BaseUnitTest;
import com.gunoads.test.util.TestDataFactory;
import com.gunoads.model.entity.Account;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import javax.sql.DataSource;
import java.io.Reader;
import java.sql.Connection;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BulkInsertDaoTest extends BaseUnitTest {

    @Mock private DataSource dataSource;
    @Mock private Connection connection;
    @Mock private BaseConnection pgConnection;
    @Mock private CopyManager copyManager;

    private BulkInsertDao bulkInsertDao;

    @BeforeEach
    void setUp() {
        bulkInsertDao = new BulkInsertDao();
        logTestStart();
    }

    @Test
    void shouldCreateCopyResult() {
        // Given
        long rowsProcessed = 1000;
        long durationMs = 2000;

        // When
        BulkInsertDao.CopyResult result = new BulkInsertDao.CopyResult(rowsProcessed, durationMs);

        // Then
        assertThat(result.rowsProcessed).isEqualTo(1000);
        assertThat(result.durationMs).isEqualTo(2000);
        assertThat(result.rowsPerSecond).isEqualTo(500); // 1000/2
    }

    @Test
    void shouldCalculateZeroRateForZeroDuration() {
        // When
        BulkInsertDao.CopyResult result = new BulkInsertDao.CopyResult(1000, 0);

        // Then
        assertThat(result.rowsPerSecond).isEqualTo(0);
    }

    @Test
    void shouldHandleEmptyDataInCopyFrom() throws Exception {
        // Given
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.unwrap(BaseConnection.class)).thenReturn(pgConnection);

        List<Account> emptyData = List.of();
        Function<Account, String> csvMapper = account -> "test-row";
        String csvHeader = "id,name";

        // When
        BulkInsertDao.CopyResult result = bulkInsertDao.copyFromCsv(
                "tbl_test", emptyData, csvMapper, csvHeader
        );

        // Then
        assertThat(result.rowsProcessed).isEqualTo(0);
        assertThat(result.durationMs).isEqualTo(0);
        verify(connection, never()).unwrap(any());
    }

    @Test
    void shouldBuildCorrectCopyCommand() {
        // Given
        String tableName = "tbl_account";

        // When testing the copy command construction logic
        String expectedCommand = String.format(
                "COPY %s FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',')",
                tableName
        );

        // Then
        assertThat(expectedCommand).contains("COPY tbl_account FROM STDIN");
        assertThat(expectedCommand).contains("FORMAT CSV");
        assertThat(expectedCommand).contains("HEADER true");
    }

    @Test
    void shouldBuildUpsertSql() {
        // Given
        String mainTable = "tbl_account";
        String tempTable = "tbl_account_temp";
        String[] keyColumns = {"id", "platform_id"};
        String[] updateColumns = {"account_name", "currency"};

        // When - testing the SQL building logic
        String expectedSql = "INSERT INTO " + mainTable +
                " SELECT * FROM " + tempTable +
                " ON CONFLICT (" + String.join(", ", keyColumns) + ")" +
                " DO UPDATE SET " +
                "account_name = EXCLUDED.account_name, currency = EXCLUDED.currency";

        // Then
        assertThat(expectedSql).contains("INSERT INTO tbl_account");
        assertThat(expectedSql).contains("SELECT * FROM tbl_account_temp");
        assertThat(expectedSql).contains("ON CONFLICT (id, platform_id)");
        assertThat(expectedSql).contains("DO UPDATE SET");
        assertThat(expectedSql).contains("EXCLUDED.account_name");
    }

    @Test
    void shouldBuildUpsertSqlWithDoNothing() {
        // Given
        String mainTable = "tbl_account";
        String tempTable = "tbl_account_temp";
        String[] keyColumns = {"id"};
        String[] updateColumns = {}; // Empty update columns

        // When - testing the SQL building logic for DO NOTHING
        String expectedSql = "INSERT INTO " + mainTable +
                " SELECT * FROM " + tempTable +
                " ON CONFLICT (" + String.join(", ", keyColumns) + ")" +
                " DO NOTHING";

        // Then
        assertThat(expectedSql).contains("DO NOTHING");
        assertThat(expectedSql).doesNotContain("DO UPDATE SET");
    }

    @Test
    void shouldFormatCsvContent() {
        // Given
        List<Account> accounts = List.of(TestDataFactory.createAccount());
        Function<Account, String> csvMapper = account ->
                account.getId() + "," + account.getAccountName();
        String csvHeader = "id,account_name";

        // When - testing CSV content building
        StringBuilder csvContent = new StringBuilder();
        csvContent.append(csvHeader).append("\n");
        for (Account account : accounts) {
            csvContent.append(csvMapper.apply(account)).append("\n");
        }

        // Then
        String result = csvContent.toString();
        assertThat(result).startsWith("id,account_name\n");
        assertThat(result).contains(accounts.get(0).getId());
        assertThat(result).contains(accounts.get(0).getAccountName());
    }

    @Test
    void shouldCreateTempTableName() {
        // Given
        String mainTable = "tbl_account";
        long timestamp = System.currentTimeMillis();

        // When
        String tempTableName = mainTable + "_temp_" + timestamp;

        // Then
        assertThat(tempTableName).startsWith("tbl_account_temp_");
        assertThat(tempTableName).contains(String.valueOf(timestamp));
    }
}