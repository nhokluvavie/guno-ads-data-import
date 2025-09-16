package com.gunoads.integration;

import com.gunoads.AbstractIntegrationTest;
import com.gunoads.connector.MetaAdsConnector;
import com.gunoads.model.dto.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class MetaDataFetchTest extends AbstractIntegrationTest {

    @Autowired
    private MetaAdsConnector connector;

    @Test
    void shouldFetchAccounts() {
        System.out.println("Testing account fetching from Meta API...");

        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();

        assertThat(accounts).isNotNull();
        System.out.printf("✅ Fetched %d accounts\n", accounts.size());

        if (!accounts.isEmpty()) {
            MetaAccountDto firstAccount = accounts.get(0);
            assertThat(firstAccount.getId()).isNotEmpty();
            assertThat(firstAccount.getAccountName()).isNotEmpty();

            System.out.printf("   First account: %s (%s)\n",
                    firstAccount.getAccountName(), firstAccount.getId());
        }
    }

    @Test
    void shouldFetchCampaigns() {
        System.out.println("Testing campaign fetching from Meta API...");

        // First get accounts
        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        if (accounts.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping campaign test");
            return;
        }

        String accountId = accounts.get(0).getId();
        List<MetaCampaignDto> campaigns = connector.fetchCampaigns(accountId);

        assertThat(campaigns).isNotNull();
        System.out.printf("✅ Fetched %d campaigns for account %s\n", campaigns.size(), accountId);

        if (!campaigns.isEmpty()) {
            MetaCampaignDto firstCampaign = campaigns.get(0);
            assertThat(firstCampaign.getId()).isNotEmpty();

            System.out.printf("   First campaign: %s (%s)\n",
                    firstCampaign.getName(), firstCampaign.getId());
        }
    }

    @Test
    void shouldFetchAdSets() {
        System.out.println("Testing adset fetching from Meta API...");

        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        if (accounts.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping adsets test");
            return;
        }

        String accountId = accounts.get(0).getId();
        List<MetaAdSetDto> adSets = connector.fetchAdSets(accountId);

        assertThat(adSets).isNotNull();
        System.out.printf("✅ Fetched %d adsets for account %s\n", adSets.size(), accountId);

        if (!adSets.isEmpty()) {
            MetaAdSetDto firstAdSet = adSets.get(0);
            assertThat(firstAdSet.getId()).isNotEmpty();

            System.out.printf("   First adset: %s (%s)\n",
                    firstAdSet.getName(), firstAdSet.getId());
        }
    }

    @Test
    void shouldFetchAds() {
        System.out.println("Testing ads fetching from Meta API...");

        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        if (accounts.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping ads test");
            return;
        }

        String accountId = accounts.get(0).getId();
        List<MetaAdDto> ads = connector.fetchAds(accountId);

        assertThat(ads).isNotNull();
        System.out.printf("✅ Fetched %d ads for account %s\n", ads.size(), accountId);

        if (!ads.isEmpty()) {
            MetaAdDto firstAd = ads.get(0);
            assertThat(firstAd.getId()).isNotEmpty();

            System.out.printf("   First ad: %s (%s)\n",
                    firstAd.getName(), firstAd.getId());
        }
    }

    @Test
    void shouldFetchInsights() {
        System.out.println("Testing insights fetching from Meta API...");

        List<MetaAccountDto> accounts = connector.fetchBusinessAccounts();
        if (accounts.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping insights test");
            return;
        }

        String accountId = accounts.get(0).getId();
        LocalDate yesterday = LocalDate.now().minusDays(1);

        List<MetaInsightsDto> insights = connector.fetchInsights(accountId, yesterday, yesterday);

        assertThat(insights).isNotNull();
        System.out.printf("✅ Fetched %d insight records for account %s (date: %s)\n",
                insights.size(), accountId, yesterday);

        if (!insights.isEmpty()) {
            MetaInsightsDto firstInsight = insights.get(0);
            assertThat(firstInsight.getAccountId()).isNotEmpty();

            System.out.printf("   First insight: spend=%s, impressions=%s, clicks=%s\n",
                    firstInsight.getSpend(), firstInsight.getImpressions(), firstInsight.getClicks());
        }
    }
}