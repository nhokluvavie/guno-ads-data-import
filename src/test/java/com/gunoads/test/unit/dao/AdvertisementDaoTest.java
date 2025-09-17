package com.gunoads.test.unit.dao;

import com.gunoads.dao.AdvertisementDao;
import com.gunoads.model.entity.Advertisement;
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

class AdvertisementDaoTest extends BaseUnitTest {

    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private AdvertisementDao advertisementDao;

    @BeforeEach
    void setUp() {
        advertisementDao = new AdvertisementDao();
        logTestStart();
    }

    @Test
    void shouldReturnCorrectTableName() {
        // When
        String tableName = advertisementDao.getTableName();

        // Then
        assertThat(tableName).isEqualTo("tbl_advertisement");
    }

    @Test
    void shouldReturnCorrectIdColumnName() {
        // When
        String idColumn = advertisementDao.getIdColumnName();

        // Then
        assertThat(idColumn).isEqualTo("id");
    }

    @Test
    void shouldBuildCorrectInsertSql() {
        // When
        String insertSql = advertisementDao.buildInsertSql();

        // Then
        assertThat(insertSql).contains("INSERT INTO tbl_advertisement");
        assertThat(insertSql).contains(":id");
        assertThat(insertSql).contains(":adsetid");
        assertThat(insertSql).contains(":adName");
        assertThat(insertSql).contains(":creativeId");
        assertThat(insertSql).contains(":isAppInstallAd");
        assertThat(insertSql).contains(":isVideoAd");
        assertThat(insertSql).contains(":isCarouselAd");
    }

    @Test
    void shouldBuildCorrectUpdateSql() {
        // When
        String updateSql = advertisementDao.buildUpdateSql();

        // Then
        assertThat(updateSql).contains("UPDATE tbl_advertisement SET");
        assertThat(updateSql).contains("WHERE id = :id");
        assertThat(updateSql).contains("ad_name = :adName");
    }

    @Test
    void shouldCreateInsertParameters() {
        // Given
        Advertisement ad = TestDataFactory.createAdvertisement();

        // When
        SqlParameterSource params = advertisementDao.getInsertParameters(ad);

        // Then
        assertThat(params.getValue("id")).isEqualTo(ad.getId());
        assertThat(params.getValue("adsetid")).isEqualTo(ad.getAdsetid());
        assertThat(params.getValue("adName")).isEqualTo(ad.getAdName());
        assertThat(params.getValue("creativeId")).isEqualTo(ad.getCreativeId());
        assertThat(params.getValue("isAppInstallAd")).isEqualTo(ad.getIsAppInstallAd());
        assertThat(params.getValue("isVideoAd")).isEqualTo(ad.getIsVideoAd());
        assertThat(params.getValue("isCarouselAd")).isEqualTo(ad.getIsCarouselAd());
    }

    @Test
    void shouldFindByAdSet() {
        // Given
        String adsetId = "adset_123";
        List<Advertisement> expectedAds = List.of(TestDataFactory.createAdvertisement());

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(expectedAds);

        // When
        List<Advertisement> ads = advertisementDao.findByAdSet(adsetId);

        // Then
        assertThat(ads).hasSize(1);
        verify(namedParameterJdbcTemplate).query(
                contains("WHERE adsetid = :adsetId"),
                any(SqlParameterSource.class),
                any(RowMapper.class)
        );
    }

    @Test
    void shouldMapRowToAdvertisement() {
        // Given
        RowMapper<Advertisement> mapper = advertisementDao.getRowMapper();

        // Then
        assertThat(mapper).isNotNull();
    }
}