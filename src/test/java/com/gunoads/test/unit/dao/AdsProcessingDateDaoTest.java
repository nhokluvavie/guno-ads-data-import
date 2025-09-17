package com.gunoads.test.unit.dao;

import com.gunoads.dao.AdsProcessingDateDao;
import com.gunoads.model.entity.AdsProcessingDate;
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AdsProcessingDateDaoTest extends BaseUnitTest {

    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private AdsProcessingDateDao dateDao;

    @BeforeEach
    void setUp() {
        dateDao = new AdsProcessingDateDao();
        logTestStart();
    }

    @Test
    void shouldReturnCorrectTableName() {
        // When
        String tableName = dateDao.getTableName();

        // Then
        assertThat(tableName).isEqualTo("tbl_ads_processing_date");
    }

    @Test
    void shouldReturnCorrectIdColumnName() {
        // When
        String idColumn = dateDao.getIdColumnName();

        // Then
        assertThat(idColumn).isEqualTo("full_date");
    }

    @Test
    void shouldBuildCorrectInsertSql() {
        // When
        String insertSql = dateDao.buildInsertSql();

        // Then
        assertThat(insertSql).contains("INSERT INTO tbl_ads_processing_date");
        assertThat(insertSql).contains(":fullDate");
        assertThat(insertSql).contains(":dayOfWeek");
        assertThat(insertSql).contains(":dayOfWeekName");
        assertThat(insertSql).contains(":dayOfMonth");
        assertThat(insertSql).contains(":dayOfYear");
        assertThat(insertSql).contains(":weekOfYear");
        assertThat(insertSql).contains(":monthOfYear");
        assertThat(insertSql).contains(":monthName");
        assertThat(insertSql).contains(":quarter");
        assertThat(insertSql).contains(":year");
        assertThat(insertSql).contains(":isWeekend");
        assertThat(insertSql).contains(":isHoliday");
        assertThat(insertSql).contains(":fiscalYear");
        assertThat(insertSql).contains(":fiscalQuarter");
    }

    @Test
    void shouldBuildCorrectUpdateSql() {
        // When
        String updateSql = dateDao.buildUpdateSql();

        // Then
        assertThat(updateSql).contains("UPDATE tbl_ads_processing_date SET");
        assertThat(updateSql).contains("WHERE full_date = :fullDate");
        assertThat(updateSql).contains("day_of_week = :dayOfWeek");
        assertThat(updateSql).contains("is_weekend = :isWeekend");
    }

    @Test
    void shouldCreateInsertParameters() {
        // Given
        AdsProcessingDate date = TestDataFactory.createAdsProcessingDate();

        // When
        SqlParameterSource params = dateDao.getInsertParameters(date);

        // Then
        assertThat(params.getValue("fullDate")).isEqualTo(date.getFullDate());
        assertThat(params.getValue("dayOfWeek")).isEqualTo(date.getDayOfWeek());
        assertThat(params.getValue("dayOfWeekName")).isEqualTo(date.getDayOfWeekName());
        assertThat(params.getValue("dayOfMonth")).isEqualTo(date.getDayOfMonth());
        assertThat(params.getValue("dayOfYear")).isEqualTo(date.getDayOfYear());
        assertThat(params.getValue("weekOfYear")).isEqualTo(date.getWeekOfYear());
        assertThat(params.getValue("monthOfYear")).isEqualTo(date.getMonthOfYear());
        assertThat(params.getValue("monthName")).isEqualTo(date.getMonthName());
        assertThat(params.getValue("quarter")).isEqualTo(date.getQuarter());
        assertThat(params.getValue("year")).isEqualTo(date.getYear());
        assertThat(params.getValue("isWeekend")).isEqualTo(date.getIsWeekend());
        assertThat(params.getValue("isHoliday")).isEqualTo(date.getIsHoliday());
        assertThat(params.getValue("holidayName")).isEqualTo(date.getHolidayName());
        assertThat(params.getValue("fiscalYear")).isEqualTo(date.getFiscalYear());
        assertThat(params.getValue("fiscalQuarter")).isEqualTo(date.getFiscalQuarter());
    }

    @Test
    void shouldFindByYear() {
        // Given
        int year = 2024;
        List<AdsProcessingDate> expectedDates = List.of(TestDataFactory.createAdsProcessingDate());

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(expectedDates);

        // When
        List<AdsProcessingDate> dates = dateDao.findByYear(year);

        // Then
        assertThat(dates).hasSize(1);
        verify(namedParameterJdbcTemplate).query(
                contains("WHERE year = :year ORDER BY full_date"),
                any(SqlParameterSource.class),
                any(RowMapper.class)
        );
    }

    @Test
    void shouldFindByDateRange() {
        // Given
        String startDate = "2024-01-01";
        String endDate = "2024-01-31";
        List<AdsProcessingDate> expectedDates = List.of(TestDataFactory.createAdsProcessingDate());

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(expectedDates);

        // When
        List<AdsProcessingDate> dates = dateDao.findByDateRange(startDate, endDate);

        // Then
        assertThat(dates).hasSize(1);
        verify(namedParameterJdbcTemplate).query(
                contains("WHERE full_date BETWEEN :startDate AND :endDate ORDER BY full_date"),
                any(SqlParameterSource.class),
                any(RowMapper.class)
        );
    }

    @Test
    void shouldMapRowToAdsProcessingDate() {
        // Given
        RowMapper<AdsProcessingDate> mapper = dateDao.getRowMapper();

        // Then
        assertThat(mapper).isNotNull();
    }

    @Test
    void shouldHandleEmptyResultInFindByYear() {
        // Given
        int year = 2024;

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(List.of());

        // When
        List<AdsProcessingDate> dates = dateDao.findByYear(year);

        // Then
        assertThat(dates).isEmpty();
    }
}