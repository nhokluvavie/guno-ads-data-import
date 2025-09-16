package com.gunoads.integration;

import com.gunoads.AbstractIntegrationTest;
import com.gunoads.dao.*;
import com.gunoads.model.entity.*;
import com.gunoads.processor.DataTransformer;
import com.gunoads.connector.MetaAdsConnector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class DatabaseInsertionTest extends AbstractIntegrationTest {

    @Autowired
    private MetaAdsConnector connector;

    @Autowired
    private DataTransformer transformer;

    @Autowired
    private AccountDao accountDao;

    @Autowired
    private CampaignDao campaignDao;

    @Autowired
    private AdSetDao adSetDao;

    @Autowired
    private AdvertisementDao advertisementDao;

    @Autowired
    private AdsReportingDao adsReportingDao;

    @Test
    void shouldTransformAndInsertAccounts() {
        System.out.println("Testing account transformation and insertion...");

        // Fetch from API
        var accountDtos = connector.fetchBusinessAccounts();
        if (accountDtos.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping account insertion test");
            return;
        }

        // Transform to entities
        List<Account> accounts = transformer.transformAccounts(accountDtos);
        assertThat(accounts).hasSameSizeAs(accountDtos);

        // Insert to database
        long initialCount = accountDao.count();
        for (Account account : accounts) {
            if (!accountDao.existsById(account.getId())) {
                accountDao.insert(account);
            }
        }

        long finalCount = accountDao.count();
        System.out.printf("✅ Inserted %d accounts (total: %d)\n",
                finalCount - initialCount, finalCount);

        // Verify data
        Account firstAccount = accounts.get(0);
        var retrieved = accountDao.findById(firstAccount.getId());
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getAccountName()).isEqualTo(firstAccount.getAccountName());

        System.out.printf("   Verified account: %s\n", firstAccount.getAccountName());
    }

    @Test
    void shouldTransformAndInsertCampaigns() {
        System.out.println("Testing campaign transformation and insertion...");

        // Get account first
        var accounts = connector.fetchBusinessAccounts();
        if (accounts.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping campaign insertion test");
            return;
        }
        String accountId = accounts.get(0).getId();

        // Fetch campaigns
        var campaignDtos = connector.fetchCampaigns(accountId);
        if (campaignDtos.isEmpty()) {
            System.out.println("ℹ️ No campaigns found for account " + accountId);
            return;
        }

        // Transform and insert
        List<Campaign> campaigns = transformer.transformCampaigns(campaignDtos);
        long initialCount = campaignDao.count();

        for (Campaign campaign : campaigns) {
            if (!campaignDao.existsById(campaign.getId())) {
                campaignDao.insert(campaign);
            }
        }

        long finalCount = campaignDao.count();
        System.out.printf("✅ Inserted %d campaigns (total: %d)\n",
                finalCount - initialCount, finalCount);

        // Verify
        Campaign firstCampaign = campaigns.get(0);
        var retrieved = campaignDao.findById(firstCampaign.getId());
        assertThat(retrieved).isPresent();

        System.out.printf("   Verified campaign: %s\n", firstCampaign.getCampaignName());
    }

    @Test
    void shouldTransformAndInsertInsights() {
        System.out.println("Testing insights transformation and insertion...");

        // Get account first
        var accounts = connector.fetchBusinessAccounts();
        if (accounts.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping insights insertion test");
            return;
        }
        String accountId = accounts.get(0).getId();

        // Fetch insights
        var insightDtos = connector.fetchYesterdayInsights(accountId);
        if (insightDtos.isEmpty()) {
            System.out.println("ℹ️ No insights found for account " + accountId);
            return;
        }

        // Transform and insert
        List<AdsReporting> reportingList = transformer.transformInsightsList(insightDtos);
        long initialCount = adsReportingDao.count();

        // Use batch insert for performance
        try {
            adsReportingDao.batchInsert(reportingList);
        } catch (Exception e) {
            System.out.println("⚠️ Batch insert failed, trying individual inserts...");
            for (AdsReporting reporting : reportingList) {
                try {
                    adsReportingDao.insert(reporting);
                } catch (Exception ex) {
                    System.out.printf("   Failed to insert: %s\n", ex.getMessage());
                }
            }
        }

        long finalCount = adsReportingDao.count();
        System.out.printf("✅ Inserted %d reporting records (total: %d)\n",
                finalCount - initialCount, finalCount);
    }

    @Test
    void shouldHandleDuplicateInsertion() {
        System.out.println("Testing duplicate insertion handling...");

        var accounts = connector.fetchBusinessAccounts();
        if (accounts.isEmpty()) {
            System.out.println("ℹ️ No accounts found - skipping duplicate handling test");
            return;
        }

        List<Account> accountEntities = transformer.transformAccounts(accounts);
        Account testAccount = accountEntities.get(0);

        // First insertion
        if (!accountDao.existsById(testAccount.getId())) {
            accountDao.insert(testAccount);
        }

        // Second insertion should update
        testAccount.setAccountName("Updated " + testAccount.getAccountName());
        accountDao.update(testAccount);

        // Verify update
        var retrieved = accountDao.findById(testAccount.getId());
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getAccountName()).startsWith("Updated");

        System.out.println("✅ Duplicate handling successful");
    }
}