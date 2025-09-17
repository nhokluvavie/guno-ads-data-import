package com.gunoads.test.unit.dao;

import com.gunoads.dao.AdsReportingDao;
import com.gunoads.model.entity.AdsReporting;
import com.gunoads.test.unit.BaseUnitTest;
import com.gunoads.test.util.TestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AdsReportingDaoTest extends BaseUnitTest {

    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private AdsReportingDao adsReportingDao;

    @BeforeEach
    void setUp() {
        adsReportingDao = new AdsReportingDao();
        logTestStart();
    }

    @Test
    void shouldReturnCorrectTableName() {
        // When
        String tableName = adsReportingDao.getTableName();

        // Then
        assertThat(tableName).isEqualTo("tbl_ads_reporting");
    }

    @Test
    void shouldReturnCorrectIdColumnName() {
        // When
        String idColumn = adsReportingDao.getIdColumnName();

        // Then
        assertThat(idColumn).isEqualTo("account_id");
    }

    @Test
    void shouldBuildCorrectInsertSql() {
        // When
        String insertSql = adsReportingDao.buildInsertSql();

        // Then
        assertThat(insertSql).contains("INSERT INTO tbl_ads_reporting");
        assertThat(insertSql).contains(":accountId");
        assertThat(insertSql).contains(":campaignId");
        assertThat(insertSql).contains(":adsetId");
        assertThat(insertSql).contains(":advertisementId");
        assertThat(insertSql).contains(":placementId");
        assertThat(insertSql).contains(":ageGroup");
        assertThat(insertSql).contains(":gender");
        assertThat(insertSql).contains(":spend");
        assertThat(insertSql).contains(":impressions");
        assertThat(insertSql).contains(":clicks");
        assertThat(insertSql).contains(":videoViews");
        assertThat(insertSql).contains(":purchases");
    }

    @Test
    void shouldBuildCorrectUpdateSql() {
        // When
        String updateSql = adsReportingDao.buildUpdateSql();

        // Then
        assertThat(updateSql).contains("UPDATE tbl_ads_reporting SET");
        assertThat(updateSql).contains("WHERE account_id = :accountId");
        assertThat(updateSql).contains("AND platform_id = :platformId");
        assertThat(updateSql).contains("AND campaign_id = :campaignId");
        assertThat(updateSql).contains("AND ads_processing_dt = :adsProcessingDt");
        assertThat(updateSql).contains("AND age_group = :ageGroup");
        assertThat(updateSql).contains("AND gender = :gender");
    }

    @Test
    void shouldCreateInsertParameters() {
        // Given
        AdsReporting reporting = TestDataFactory.createAdsReporting();

        // When
        SqlParameterSource params = adsReportingDao.getInsertParameters(reporting);

        // Then
        assertThat(params.getValue("accountId")).isEqualTo(reporting.getAccountId());
        assertThat(params.getValue("platformId")).isEqualTo(reporting.getPlatformId());
        assertThat(params.getValue("campaignId")).isEqualTo(reporting.getCampaignId());
        assertThat(params.getValue("adsetId")).isEqualTo(reporting.getAdsetId());
        assertThat(params.getValue("advertisementId")).isEqualTo(reporting.getAdvertisementId());
        assertThat(params.getValue("placementId")).isEqualTo(reporting.getPlacementId());
        assertThat(params.getValue("ageGroup")).isEqualTo(reporting.getAgeGroup());
        assertThat(params.getValue("gender")).isEqualTo(reporting.getGender());
        assertThat(params.getValue("spend")).isEqualTo(reporting.getSpend());
        assertThat(params.getValue("impressions")).isEqualTo(reporting.getImpressions());
        assertThat(params.getValue("clicks")).isEqualTo(reporting.getClicks());
        assertThat(params.getValue("videoViews")).isEqualTo(reporting.getVideoViews());
        assertThat(params.getValue("purchases")).isEqualTo(reporting.getPurchases());
    }

    @Test
    void shouldHaveCsvMapper() {
        // When
        Function<AdsReporting, String> csvMapper = AdsReportingDao.getCsvMapper();

        // Then
        assertThat(csvMapper).isNotNull();
    }

    @Test
    void shouldGenerateCorrectCsvRow() {
        // Given
        AdsReporting reporting = TestDataFactory.createAdsReporting();
        Function<AdsReporting, String> csvMapper = AdsReportingDao.getCsvMapper();

        // When
        String csvRow = csvMapper.apply(reporting);

        // Then
        assertThat(csvRow).isNotNull();
        assertThat(csvRow).contains(reporting.getAccountId());
        assertThat(csvRow).contains(reporting.getCampaignId());
        assertThat(csvRow).contains(reporting.getAgeGroup());
        assertThat(csvRow).contains(reporting.getGender());
        assertThat(csvRow).contains(String.valueOf(reporting.getSpend()));
        assertThat(csvRow).contains(String.valueOf(reporting.getImpressions()));
    }

    @Test
    void shouldHaveCsvHeader() {
        // When
        String csvHeader = AdsReportingDao.CSV_HEADER;

        // Then
        assertThat(csvHeader).isNotNull();
        assertThat(csvHeader).contains("account_id");
        assertThat(csvHeader).contains("campaign_id");
        assertThat(csvHeader).contains("age_group");
        assertThat(csvHeader).contains("gender");
        assertThat(csvHeader).contains("spend");
        assertThat(csvHeader).contains("impressions");
        assertThat(csvHeader).contains("clicks");
        assertThat(csvHeader).contains("video_views");
        assertThat(csvHeader).contains("purchases");
    }

    @Test
    void shouldMapRowToAdsReporting() {
        // Given
        RowMapper<AdsReporting> mapper = adsReportingDao.getRowMapper();

        // Then
        assertThat(mapper).isNotNull();
    }
}