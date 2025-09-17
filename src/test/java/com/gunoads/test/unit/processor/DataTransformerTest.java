package com.gunoads.test.unit.processor;

import com.gunoads.processor.DataTransformer;
import com.gunoads.model.dto.*;
import com.gunoads.model.entity.*;
import com.gunoads.test.unit.BaseUnitTest;
import com.gunoads.test.util.TestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class DataTransformerTest extends BaseUnitTest {

    private DataTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new DataTransformer();
        logTestStart();
    }

    @Test
    void shouldTransformMetaAccountDto() {
        // Given
        MetaAccountDto dto = TestDataFactory.createMetaAccountDto();

        // When
        Account account = transformer.transformAccount(dto);

        // Then
        assertThat(account).isNotNull();
        assertThat(account.getId()).isEqualTo(dto.getId());
        assertThat(account.getAccountName()).isEqualTo(dto.getAccountName());
        assertThat(account.getCurrency()).isEqualTo(dto.getCurrency());
        assertThat(account.getPlatformId()).isEqualTo("META");
        assertThat(account.getTimezoneId()).isEqualTo(dto.getTimezoneId());
        assertThat(account.getBusinessCountryCode()).isEqualTo(dto.getBusinessCountryCode());
        assertThat(account.getIsPersonal()).isEqualTo(dto.getIsPersonal());
    }

    @Test
    void shouldHandleNullAccountDto() {
        // When
        Account account = transformer.transformAccount(null);

        // Then
        assertThat(account).isNull();
    }

    @Test
    void shouldTransformMetaCampaignDto() {
        // Given
        MetaCampaignDto dto = TestDataFactory.createMetaCampaignDto();

        // When
        Campaign campaign = transformer.transformCampaign(dto);

        // Then
        assertThat(campaign).isNotNull();
        assertThat(campaign.getId()).isEqualTo(dto.getId());
        assertThat(campaign.getCampaignName()).isEqualTo(dto.getName());
        assertThat(campaign.getCamObjective()).isEqualTo(dto.getObjective());
        assertThat(campaign.getPlatformId()).isEqualTo("META");
        assertThat(campaign.getBuyingType()).isEqualTo(dto.getBuyingType());
    }

    @Test
    void shouldTransformMetaAdSetDto() {
        // Given
        MetaAdSetDto dto = TestDataFactory.createMetaAdSetDto();

        // When
        AdSet adSet = transformer.transformAdSet(dto);

        // Then
        assertThat(adSet).isNotNull();
        assertThat(adSet.getId()).isEqualTo(dto.getId());
        assertThat(adSet.getCampaignId()).isEqualTo(dto.getCampaignId());
        assertThat(adSet.getAdSetName()).isEqualTo(dto.getName());
        assertThat(adSet.getLifetimeImps()).isEqualTo(dto.getLifetimeImps());
        assertThat(adSet.getAgeMin()).isEqualTo(dto.getAgeMin());
        assertThat(adSet.getAgeMax()).isEqualTo(dto.getAgeMax());
        assertThat(adSet.getIsAutobid()).isEqualTo(dto.getIsAutobid());
    }

    @Test
    void shouldTransformMetaAdDto() {
        // Given
        MetaAdDto dto = TestDataFactory.createMetaAdDto();

        // When
        Advertisement ad = transformer.transformAdvertisement(dto);

        // Then
        assertThat(ad).isNotNull();
        assertThat(ad.getId()).isEqualTo(dto.getId());
        assertThat(ad.getAdsetid()).isEqualTo(dto.getAdsetId());
        assertThat(ad.getAdName()).isEqualTo(dto.getName());
        assertThat(ad.getCreativeId()).isEqualTo(dto.getCreative().getId());
        assertThat(ad.getIsVideoAd()).isEqualTo(dto.getCreative().getIsVideoAd());
    }

    @Test
    void shouldTransformMetaInsightsDto() {
        // Given
        MetaInsightsDto dto = TestDataFactory.createMetaInsightsDto();

        // When
        AdsReporting reporting = transformer.transformInsights(dto);

        // Then
        assertThat(reporting).isNotNull();
        assertThat(reporting.getAccountId()).isEqualTo(dto.getAccountId());
        assertThat(reporting.getCampaignId()).isEqualTo(dto.getCampaignId());
        assertThat(reporting.getAdsetId()).isEqualTo(dto.getAdsetId());
        assertThat(reporting.getAdvertisementId()).isEqualTo(dto.getAdId());
        assertThat(reporting.getPlatformId()).isEqualTo("META");
        assertThat(reporting.getAgeGroup()).isEqualTo(dto.getAge());
        assertThat(reporting.getGender()).isEqualTo(dto.getGender());
        assertThat(reporting.getCountryName()).isEqualTo(dto.getCountry());
        assertThat(reporting.getSpend()).isEqualTo(Double.parseDouble(dto.getSpend()));
        assertThat(reporting.getImpressions()).isEqualTo(Long.parseLong(dto.getImpressions()));
        assertThat(reporting.getClicks()).isEqualTo(Long.parseLong(dto.getClicks()));
    }

    @Test
    void shouldTransformListOfAccounts() {
        // Given
        List<MetaAccountDto> dtos = TestDataFactory.createMetaAccountDtoList(3);

        // When
        List<Account> accounts = transformer.transformAccounts(dtos);

        // Then
        assertThat(accounts).hasSize(dtos.size());
        assertThat(accounts.get(0).getId()).isEqualTo(dtos.get(0).getId());
    }

    @Test
    void shouldHandleEmptyList() {
        // When
        List<Account> accounts = transformer.transformAccounts(List.of());

        // Then
        assertThat(accounts).isEmpty();
    }

    @Test
    void shouldHandleNullList() {
        // When
        List<Account> accounts = transformer.transformAccounts(null);

        // Then
        assertThat(accounts).isEmpty();
    }
}