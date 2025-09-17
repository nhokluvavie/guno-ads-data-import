package com.gunoads.test.unit.dao;

import com.gunoads.dao.PlacementDao;
import com.gunoads.model.entity.Placement;
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

class PlacementDaoTest extends BaseUnitTest {

    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private PlacementDao placementDao;

    @BeforeEach
    void setUp() {
        placementDao = new PlacementDao();
        logTestStart();
    }

    @Test
    void shouldReturnCorrectTableName() {
        // When
        String tableName = placementDao.getTableName();

        // Then
        assertThat(tableName).isEqualTo("tbl_placement");
    }

    @Test
    void shouldReturnCorrectIdColumnName() {
        // When
        String idColumn = placementDao.getIdColumnName();

        // Then
        assertThat(idColumn).isEqualTo("id");
    }

    @Test
    void shouldBuildCorrectInsertSql() {
        // When
        String insertSql = placementDao.buildInsertSql();

        // Then
        assertThat(insertSql).contains("INSERT INTO tbl_placement");
        assertThat(insertSql).contains(":id");
        assertThat(insertSql).contains(":advertisementId");
        assertThat(insertSql).contains(":placementName");
        assertThat(insertSql).contains(":platform");
        assertThat(insertSql).contains(":placementType");
        assertThat(insertSql).contains(":deviceType");
        assertThat(insertSql).contains(":isActive");
        assertThat(insertSql).contains(":supportsVideo");
        assertThat(insertSql).contains(":supportsCarousel");
        assertThat(insertSql).contains(":supportsCollection");
    }

    @Test
    void shouldBuildCorrectUpdateSql() {
        // When
        String updateSql = placementDao.buildUpdateSql();

        // Then
        assertThat(updateSql).contains("UPDATE tbl_placement SET");
        assertThat(updateSql).contains("WHERE id = :id");
        assertThat(updateSql).contains("placement_name = :placementName");
    }

    @Test
    void shouldCreateInsertParameters() {
        // Given
        Placement placement = TestDataFactory.createPlacement();

        // When
        SqlParameterSource params = placementDao.getInsertParameters(placement);

        // Then
        assertThat(params.getValue("id")).isEqualTo(placement.getId());
        assertThat(params.getValue("advertisementId")).isEqualTo(placement.getAdvertisementId());
        assertThat(params.getValue("placementName")).isEqualTo(placement.getPlacementName());
        assertThat(params.getValue("platform")).isEqualTo(placement.getPlatform());
        assertThat(params.getValue("placementType")).isEqualTo(placement.getPlacementType());
        assertThat(params.getValue("deviceType")).isEqualTo(placement.getDeviceType());
        assertThat(params.getValue("position")).isEqualTo(placement.getPosition());
        assertThat(params.getValue("isActive")).isEqualTo(placement.getIsActive());
        assertThat(params.getValue("supportsVideo")).isEqualTo(placement.getSupportsVideo());
        assertThat(params.getValue("supportsCarousel")).isEqualTo(placement.getSupportsCarousel());
        assertThat(params.getValue("supportsCollection")).isEqualTo(placement.getSupportsCollection());
        assertThat(params.getValue("createdAt")).isEqualTo(placement.getCreatedAt());
    }

    @Test
    void shouldFindByAdvertisement() {
        // Given
        String advertisementId = "ad_123";
        List<Placement> expectedPlacements = List.of(TestDataFactory.createPlacement());

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(expectedPlacements);

        // When
        List<Placement> placements = placementDao.findByAdvertisement(advertisementId);

        // Then
        assertThat(placements).hasSize(1);
        verify(namedParameterJdbcTemplate).query(
                contains("WHERE advertisement_id = :advertisementId"),
                any(SqlParameterSource.class),
                any(RowMapper.class)
        );
    }

    @Test
    void shouldMapRowToPlacement() {
        // Given
        RowMapper<Placement> mapper = placementDao.getRowMapper();

        // Then
        assertThat(mapper).isNotNull();
    }

    @Test
    void shouldHandleEmptyResultInFindByAdvertisement() {
        // Given
        String advertisementId = "ad_123";

        when(namedParameterJdbcTemplate.query(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(List.of());

        // When
        List<Placement> placements = placementDao.findByAdvertisement(advertisementId);

        // Then
        assertThat(placements).isEmpty();
    }
}