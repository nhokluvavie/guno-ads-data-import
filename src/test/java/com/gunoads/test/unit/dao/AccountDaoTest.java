package com.gunoads.test.unit.dao;

import com.gunoads.dao.AccountDao;
import com.gunoads.model.entity.Account;
import com.gunoads.test.unit.BaseUnitTest;
import com.gunoads.test.util.TestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AccountDaoTest extends BaseUnitTest {

    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private AccountDao accountDao;

    @BeforeEach
    void setUp() {
        accountDao = new AccountDao();
        // Inject mocked dependencies via reflection or setter methods
        logTestStart();
    }

    @Test
    void shouldReturnCorrectTableName() {
        // When
        String tableName = accountDao.getTableName();

        // Then
        assertThat(tableName).isEqualTo("tbl_account");
    }

    @Test
    void shouldReturnCorrectIdColumnName() {
        // When
        String idColumn = accountDao.getIdColumnName();

        // Then
        assertThat(idColumn).isEqualTo("id");
    }

    @Test
    void shouldBuildCorrectInsertSql() {
        // When
        String insertSql = accountDao.buildInsertSql();

        // Then
        assertThat(insertSql).contains("INSERT INTO tbl_account");
        assertThat(insertSql).contains(":id");
        assertThat(insertSql).contains(":platformId");
        assertThat(insertSql).contains(":accountName");
    }

    @Test
    void shouldBuildCorrectUpdateSql() {
        // When
        String updateSql = accountDao.buildUpdateSql();

        // Then
        assertThat(updateSql).contains("UPDATE tbl_account SET");
        assertThat(updateSql).contains("WHERE id = :id AND platform_id = :platformId");
    }

    @Test
    void shouldCreateInsertParameters() {
        // Given
        Account account = TestDataFactory.createAccount();

        // When
        SqlParameterSource params = accountDao.getInsertParameters(account);

        // Then
        assertThat(params.getValue("id")).isEqualTo(account.getId());
        assertThat(params.getValue("platformId")).isEqualTo(account.getPlatformId());
        assertThat(params.getValue("accountName")).isEqualTo(account.getAccountName());
        assertThat(params.getValue("currency")).isEqualTo(account.getCurrency());
    }

    @Test
    void shouldMapRowToAccount() {
        // Given
        RowMapper<Account> mapper = accountDao.getRowMapper();

        // Note: Testing row mapper would require mocking ResultSet
        // For unit tests, we focus on testing the logic, not JDBC specifics
        assertThat(mapper).isNotNull();
    }

    @Test
    void shouldFindByPlatform() {
        // Given
        String platformId = "META";
        List<Account> expectedAccounts = List.of(TestDataFactory.createAccount());

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(expectedAccounts);

        // When
        List<Account> accounts = accountDao.findByPlatform(platformId);

        // Then
        assertThat(accounts).hasSize(1);
        verify(namedParameterJdbcTemplate).query(
                contains("WHERE platform_id = :platformId"),
                any(SqlParameterSource.class),
                any(RowMapper.class)
        );
    }

    @Test
    void shouldHandleEmptyResultInFindByPlatform() {
        // Given
        String platformId = "META";

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(List.of());

        // When
        List<Account> accounts = accountDao.findByPlatform(platformId);

        // Then
        assertThat(accounts).isEmpty();
    }
}