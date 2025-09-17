package com.gunoads.test.unit.dao;

import com.gunoads.dao.AdSetDao;
import com.gunoads.model.entity.AdSet;
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

class AdSetDaoTest extends BaseUnitTest {

    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private AdSetDao adSetDao;

    @BeforeEach
    void setUp() {
        adSetDao = new AdSetDao();
        logTestStart();
    }

    @Test
    void shouldReturnCorrectTableName() {
        // When
        String tableName = adSetDao.getTableName();

        // Then
        assertThat(tableName).isEqualTo("tbl_adset");
    }

    @Test
    void shouldReturnCorrectIdColumnName() {
        // When
        String idColumn = adSetDao.getIdColumnName();

        // Then
        assertThat(idColumn).isEqualTo("id");
    }

    @Test
    void shouldBuildCorrectInsertSql() {
        // When
        String insertSql = adSetDao.buildInsertSql();

        // Then
        assertThat(insertSql).contains("INSERT INTO tbl_adset");
        assertThat(insertSql).contains(":id");
        assertThat(insertSql).contains(":campaignId");
        assertThat(insertSql).contains(":adSetName");
        assertThat(insertSql).contains(":lifetimeImps");
        assertThat(insertSql).contains(":optimizationGoal");
        assertThat(insertSql).contains(":ageMin");
        assertThat(insertSql).contains(":ageMax");
        assertThat(insertSql).contains(":isAutobid");
    }

    @Test
    void shouldBuildCorrectUpdateSql() {
        // When
        String updateSql = adSetDao.buildUpdateSql();

        // Then
        assertThat(updateSql).contains("UPDATE tbl_adset SET");
        assertThat(updateSql).contains("WHERE id = :id");
        assertThat(updateSql).contains("ad_set_name = :adSetName");
    }

    @Test
    void shouldCreateInsertParameters() {
        // Given
        AdSet adSet = TestDataFactory.createAdSet();

        // When
        SqlParameterSource params = adSetDao.getInsertParameters(adSet);

        // Then
        assertThat(params.getValue("id")).isEqualTo(adSet.getId());
        assertThat(params.getValue("campaignId")).isEqualTo(adSet.getCampaignId());
        assertThat(params.getValue("adSetName")).isEqualTo(adSet.getAdSetName());
        assertThat(params.getValue("lifetimeImps")).isEqualTo(adSet.getLifetimeImps());
        assertThat(params.getValue("ageMin")).isEqualTo(adSet.getAgeMin());
        assertThat(params.getValue("ageMax")).isEqualTo(adSet.getAgeMax());
        assertThat(params.getValue("isAutobid")).isEqualTo(adSet.getIsAutobid());
    }

    @Test
    void shouldFindByCampaign() {
        // Given
        String campaignId = "campaign_123";
        List<AdSet> expectedAdSets = List.of(TestDataFactory.createAdSet());

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(expectedAdSets);

        // When
        List<AdSet> adSets = adSetDao.findByCampaign(campaignId);

        // Then
        assertThat(adSets).hasSize(1);
        verify(namedParameterJdbcTemplate).query(
                contains("WHERE campaign_id = :campaignId"),
                any(SqlParameterSource.class),
                any(RowMapper.class)
        );
    }

    @Test
    void shouldMapRowToAdSet() {
        // Given
        RowMapper<AdSet> mapper = adSetDao.getRowMapper();

        // Then
        assertThat(mapper).isNotNull();
    }
}