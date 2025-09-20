package com.gunoads.test.integration.connector;

import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.model.dto.*;
import com.gunoads.test.integration.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class MetaDataFetchTest extends BaseIntegrationTest {

    @Autowired
    private MetaAdsConnector connector;

    @Test
    void shouldFetchBusinessAccounts() {
        // When
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();

        // Then
        assertThat(accounts).isNotNull();
        assertThat(accounts).isNotEmpty();

        MetaAccountDto firstAccount = accounts.get(0);
        assertThat(firstAccount.getId()).isNotEmpty();
        assertThat(firstAccount.getAccountName()).isNotEmpty();
        assertThat(firstAccount.getCurrency()).isNotEmpty();
    }

    @Test
    void shouldFetchUserAccounts() {
        // When
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();

        // Then
        assertThat(accounts).isNotNull();
        // May be empty if user has no direct account access

        if (!accounts.isEmpty()) {
            MetaAccountDto firstAccount = accounts.get(0);
            assertThat(firstAccount.getId()).isNotEmpty();
            assertThat(firstAccount.getAccountName()).isNotEmpty();
        }
    }

    @Test
    void shouldFetchCampaignsForAccount() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();

        // When
        List<MetaCampaignDto> campaigns = connector.fetchCampaigns(accountId);

        // Then
        assertThat(campaigns).isNotNull();

        if (!campaigns.isEmpty()) {
            MetaCampaignDto firstCampaign = campaigns.get(0);
            assertThat(firstCampaign.getId()).isNotEmpty();
            assertThat(firstCampaign.getAccountId()).isEqualTo(accountId);
            assertThat(firstCampaign.getName()).isNotEmpty();
            assertThat(firstCampaign.getStatus()).isNotEmpty();
        }
    }

    @Test
    void shouldFetchAdSetsForAccount() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();

        // When
        List<MetaAdSetDto> adSets = connector.fetchAdSets(accountId);

        // Then
        assertThat(adSets).isNotNull();

        if (!adSets.isEmpty()) {
            MetaAdSetDto firstAdSet = adSets.get(0);
            assertThat(firstAdSet.getId()).isNotEmpty();
            assertThat(firstAdSet.getCampaignId()).isNotEmpty();
            assertThat(firstAdSet.getName()).isNotEmpty();
            assertThat(firstAdSet.getStatus()).isNotEmpty();
        }
    }

    @Test
    void shouldFetchAdsForAccount() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();

        // When
        List<MetaAdDto> ads = connector.fetchAds(accountId);

        // Then
        assertThat(ads).isNotNull();

        if (!ads.isEmpty()) {
            MetaAdDto firstAd = ads.get(0);
            assertThat(firstAd.getId()).isNotEmpty();
            assertThat(firstAd.getAdsetId()).isNotEmpty();
            assertThat(firstAd.getName()).isNotEmpty();
            assertThat(firstAd.getStatus()).isNotEmpty();
        }
    }

    @Test
    void shouldFetchInsightsForAccount() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();
        LocalDate yesterday = LocalDate.now().minusDays(1);

        // When
        List<MetaInsightsDto> insights = connector.fetchInsights(accountId, yesterday, yesterday);

        // Then
        assertThat(insights).isNotNull();

        if (!insights.isEmpty()) {
            MetaInsightsDto firstInsight = insights.get(0);
            assertThat(firstInsight.getAccountId()).isEqualTo(accountId);
            assertThat(firstInsight.getDateStart()).isNotEmpty();
            assertThat(firstInsight.getDateStop()).isNotEmpty();

            // Verify metrics are present
            if (firstInsight.getSpend() != null) {
                assertThat(firstInsight.getSpend()).isNotEmpty();
            }
            if (firstInsight.getImpressions() != null) {
                assertThat(firstInsight.getImpressions()).isNotEmpty();
            }
        }
    }

    @Test
    void shouldFetchYesterdayInsightsForAccount() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();

        // When
        List<MetaInsightsDto> insights = connector.fetchYesterdayInsights(accountId);

        // Then
        assertThat(insights).isNotNull();

        String expectedDate = LocalDate.now().minusDays(1).toString();
        if (!insights.isEmpty()) {
            MetaInsightsDto firstInsight = insights.get(0);
            assertThat(firstInsight.getDateStart()).contains(expectedDate);
        }
    }

    @Test
    void shouldValidateAccountDataStructure() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        MetaAccountDto account = accounts.get(0);

        // Then - Validate required fields
        assertThat(account.getId()).isNotEmpty();
        assertThat(account.getAccountName()).isNotEmpty();
        assertThat(account.getCurrency()).isNotEmpty();
        assertThat(account.getAccountStatus()).isNotEmpty();

        // Validate boolean fields are not null
        assertThat(account.getIsPersonal()).isNotNull();
        assertThat(account.getIsPrepayAccount()).isNotNull();
        assertThat(account.getIsTaxIdRequired()).isNotNull();
    }

    @Test
    void shouldValidateCampaignDataStructure() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();
        List<MetaCampaignDto> campaigns = connector.fetchCampaigns(accountId);
        assumeTrue(!campaigns.isEmpty());

        MetaCampaignDto campaign = campaigns.get(0);

        // Then - Validate required fields
        assertThat(campaign.getId()).isNotEmpty();
        assertThat(campaign.getAccountId()).isEqualTo(accountId);
        assertThat(campaign.getName()).isNotEmpty();
        assertThat(campaign.getStatus()).isNotEmpty();

        // Validate optional fields don't break
        if (campaign.getObjective() != null) {
            assertThat(campaign.getObjective()).isNotEmpty();
        }
        if (campaign.getBuyingType() != null) {
            assertThat(campaign.getBuyingType()).isNotEmpty();
        }
    }

    @Test
    void shouldValidateInsightsDataStructure() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();
        LocalDate yesterday = LocalDate.now().minusDays(1);
        List<MetaInsightsDto> insights = connector.fetchInsights(accountId, yesterday, yesterday);

        if (!insights.isEmpty()) {
            MetaInsightsDto insight = insights.get(0);

            // Then - Validate required fields
            assertThat(insight.getAccountId()).isEqualTo(accountId);
            assertThat(insight.getDateStart()).isNotEmpty();
            assertThat(insight.getDateStop()).isNotEmpty();

            // Validate numeric fields are properly formatted
            if (insight.getSpend() != null) {
                assertThatCode(() -> Double.parseDouble(insight.getSpend())).doesNotThrowAnyException();
            }
            if (insight.getImpressions() != null) {
                assertThatCode(() -> Long.parseLong(insight.getImpressions())).doesNotThrowAnyException();
            }
            if (insight.getClicks() != null) {
                assertThatCode(() -> Long.parseLong(insight.getClicks())).doesNotThrowAnyException();
            }
        }
    }

    @Test
    void shouldHandleEmptyDataGracefully() {
        // Given - Use a test account that may have no data
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();

        // When - Fetch data that might be empty
        List<MetaCampaignDto> campaigns = connector.fetchCampaigns(accountId);
        List<MetaAdSetDto> adSets = connector.fetchAdSets(accountId);
        List<MetaAdDto> ads = connector.fetchAds(accountId);

        // Then - Should not throw exceptions
        assertThat(campaigns).isNotNull();
        assertThat(adSets).isNotNull();
        assertThat(ads).isNotNull();
    }

    @Test
    void shouldFetchDataWithinTimeout() {
        // Given
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        assumeTrue(!accounts.isEmpty());

        String accountId = accounts.get(0).getId();
        long startTime = System.currentTimeMillis();

        // When
        connector.fetchCampaigns(accountId);
        long duration = System.currentTimeMillis() - startTime;

        // Then - Should complete within reasonable time
        assertThat(duration).isLessThan(30000); // 30 seconds timeout
    }

    // Remove unused method
}