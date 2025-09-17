package com.gunoads.dao;

import com.gunoads.model.entity.AdsProcessingDate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class AdsProcessingDateDao extends StandardDao<AdsProcessingDate, String> {

    @Override
    public String getTableName() {
        return "tbl_ads_processing_date";
    }

    @Override
    public String getIdColumnName() {
        return "full_date";
    }

    @Override
    public RowMapper<AdsProcessingDate> getRowMapper() {
        return new AdsProcessingDateRowMapper();
    }

    @Override
    public String buildInsertSql() {
        return """
            INSERT INTO tbl_ads_processing_date (
                full_date, day_of_week, day_of_week_name, day_of_month, day_of_year,
                week_of_year, month_of_year, month_name, quarter, year,
                is_weekend, is_holiday, holiday_name, fiscal_year, fiscal_quarter
            ) VALUES (
                :fullDate, :dayOfWeek, :dayOfWeekName, :dayOfMonth, :dayOfYear,
                :weekOfYear, :monthOfYear, :monthName, :quarter, :year,
                :isWeekend, :isHoliday, :holidayName, :fiscalYear, :fiscalQuarter
            )
            """;
    }

    @Override
    public String buildUpdateSql() {
        return """
            UPDATE tbl_ads_processing_date SET 
                day_of_week = :dayOfWeek, day_of_week_name = :dayOfWeekName,
                day_of_month = :dayOfMonth, day_of_year = :dayOfYear,
                week_of_year = :weekOfYear, month_of_year = :monthOfYear,
                month_name = :monthName, quarter = :quarter, year = :year,
                is_weekend = :isWeekend, is_holiday = :isHoliday,
                holiday_name = :holidayName, fiscal_year = :fiscalYear,
                fiscal_quarter = :fiscalQuarter
            WHERE full_date = :fullDate
            """;
    }

    @Override
    public SqlParameterSource getInsertParameters(AdsProcessingDate date) {
        return new MapSqlParameterSource()
                .addValue("fullDate", date.getFullDate())
                .addValue("dayOfWeek", date.getDayOfWeek())
                .addValue("dayOfWeekName", date.getDayOfWeekName())
                .addValue("dayOfMonth", date.getDayOfMonth())
                .addValue("dayOfYear", date.getDayOfYear())
                .addValue("weekOfYear", date.getWeekOfYear())
                .addValue("monthOfYear", date.getMonthOfYear())
                .addValue("monthName", date.getMonthName())
                .addValue("quarter", date.getQuarter())
                .addValue("year", date.getYear())
                .addValue("isWeekend", date.getIsWeekend())
                .addValue("isHoliday", date.getIsHoliday())
                .addValue("holidayName", date.getHolidayName())
                .addValue("fiscalYear", date.getFiscalYear())
                .addValue("fiscalQuarter", date.getFiscalQuarter());
    }

    @Override
    protected SqlParameterSource getUpdateParameters(AdsProcessingDate date) {
        return getInsertParameters(date);
    }

    public List<AdsProcessingDate> findByYear(int year) {
        String sql = "SELECT * FROM tbl_ads_processing_date WHERE year = :year ORDER BY full_date";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource("year", year), getRowMapper());
    }

    public List<AdsProcessingDate> findByDateRange(String startDate, String endDate) {
        String sql = "SELECT * FROM tbl_ads_processing_date WHERE full_date BETWEEN :startDate AND :endDate ORDER BY full_date";
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource()
                        .addValue("startDate", startDate)
                        .addValue("endDate", endDate),
                getRowMapper());
    }

    private static class AdsProcessingDateRowMapper implements RowMapper<AdsProcessingDate> {
        @Override
        public AdsProcessingDate mapRow(ResultSet rs, int rowNum) throws SQLException {
            AdsProcessingDate date = new AdsProcessingDate();
            date.setFullDate(rs.getString("full_date"));
            date.setDayOfWeek(rs.getInt("day_of_week"));
            date.setDayOfWeekName(rs.getString("day_of_week_name"));
            date.setDayOfMonth(rs.getInt("day_of_month"));
            date.setDayOfYear(rs.getInt("day_of_year"));
            date.setWeekOfYear(rs.getInt("week_of_year"));
            date.setMonthOfYear(rs.getInt("month_of_year"));
            date.setMonthName(rs.getString("month_name"));
            date.setQuarter(rs.getInt("quarter"));
            date.setYear(rs.getInt("year"));
            date.setIsWeekend(rs.getBoolean("is_weekend"));
            date.setIsHoliday(rs.getBoolean("is_holiday"));
            date.setHolidayName(rs.getString("holiday_name"));
            date.setFiscalYear(rs.getInt("fiscal_year"));
            date.setFiscalQuarter(rs.getInt("fiscal_quarter"));
            return date;
        }
    }
}