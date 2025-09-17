package com.gunoads.test.unit.dao;

import com.gunoads.dao.CampaignDao;
import com.gunoads.model.entity.Campaign;
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

class CampaignDaoTest extends BaseUnitTest {

    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private CampaignDao campaignDao;

    @BeforeEach
    void setUp() {
        campaignDao = new CampaignDao();
        logTestStart();
    }

    @Test
    void shouldReturnCorrectTableName() {
        // When
        String tableName = campaignDao.getTableName();

        // Then
        assertThat(tableName).isEqualTo("tbl_campaign");
    }

    @Test
    void shouldReturnCorrectIdColumnName() {
        // When
        String idColumn = campaignDao.getIdColumnName();

        // Then
        assertThat(idColumn).isEqualTo("id");
    }

    @Test
    void shouldBuildCorrectInsertSql() {
        // When
        String insertSql = campaignDao.buildInsertSql();

        // Then
        assertThat(insertSql).contains("INSERT INTO tbl_campaign");
        assertThat(insertSql).contains(":id");
        assertThat(insertSql).contains(":accountId");
        assertThat(insertSql).contains(":campaignName");
        assertThat(insertSql).contains(":camObjective");
        assertThat(insertSql).contains(":buyingType");
    }

    @Test
    void shouldBuildCorrectUpdateSql() {
        // When
        String updateSql = campaignDao.buildUpdateSql();

        // Then
        assertThat(updateSql).contains("UPDATE tbl_campaign SET");
        assertThat(updateSql).contains("WHERE id = :id");
        assertThat(updateSql).contains("campaign_name = :campaignName");
    }

    @Test
    void shouldCreateInsertParameters() {
        // Given
        Campaign campaign = TestDataFactory.createCampaign();

        // When
        SqlParameterSource params = campaignDao.getInsertParameters(campaign);

        // Then
        assertThat(params.getValue("id")).isEqualTo(campaign.getId());
        assertThat(params.getValue("accountId")).isEqualTo(campaign.getAccountId());
        assertThat(params.getValue("campaignName")).isEqualTo(campaign.getCampaignName());
        assertThat(params.getValue("camObjective")).isEqualTo(campaign.getCamObjective());
        assertThat(params.getValue("buyingType")).isEqualTo(campaign.getBuyingType());
    }

    @Test
    void shouldFindByAccount() {
        // Given
        String accountId = "act_123";
        List<Campaign> expectedCampaigns = List.of(TestDataFactory.createCampaign());

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(expectedCampaigns);

        // When
        List<Campaign> campaigns = campaignDao.findByAccount(accountId);

        // Then
        assertThat(campaigns).hasSize(1);
        verify(namedParameterJdbcTemplate).query(
                contains("WHERE account_id = :accountId"),
                any(SqlParameterSource.class),
                any(RowMapper.class)
        );
    }

    @Test
    void shouldMapRowToCampaign() {
        // Given
        RowMapper<Campaign> mapper = campaignDao.getRowMapper();

        // Then
        assertThat(mapper).isNotNull();
    }
}