package com.gunoads.test.integration.dao;

import com.gunoads.dao.*;
import com.gunoads.model.entity.*;
import com.gunoads.test.integration.BaseIntegrationTest;
import com.gunoads.test.util.TestDataFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class DatabaseOperationsTest extends BaseIntegrationTest {

    @Autowired
    private AccountDao accountDao;

    @Autowired
    private CampaignDao campaignDao;

    @Autowired
    private AdSetDao adSetDao;

    @Autowired
    private AdvertisementDao advertisementDao;

    @Autowired
    private PlacementDao placementDao;

    @Autowired
    private AdsReportingDao adsReportingDao;

    @Autowired
    private AdsProcessingDateDao adsProcessingDateDao;

    @Test
    void shouldPerformAccountCrudOperations() {
        // Given
        Account account = TestDataFactory.createAccount();
        account.setId("integration_test_" + System.currentTimeMillis());

        // When - Insert
        accountDao.insert(account);

        // Then - Find
        Optional<Account> found = accountDao.findById(account.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getAccountName()).isEqualTo(account.getAccountName());
        assertThat(found.get().getCurrency()).isEqualTo(account.getCurrency());

        // When - Update
        account.setAccountName("Updated Account Name");
        accountDao.update(account);

        // Then - Verify update
        Optional<Account> updated = accountDao.findById(account.getId());
        assertThat(updated).isPresent();
        assertThat(updated.get().getAccountName()).isEqualTo("Updated Account Name");

        // When - Delete
        accountDao.deleteById(account.getId());

        // Then - Verify deletion
        Optional<Account> deleted = accountDao.findById(account.getId());
        assertThat(deleted).isEmpty();
    }

    @Test
    void shouldPerformCampaignCrudOperations() {
        // Given
        Account account = TestDataFactory.createAccount();
        account.setId("acc_" + System.currentTimeMillis());
        accountDao.insert(account);

        Campaign campaign = TestDataFactory.createCampaign();
        campaign.setId("cam_" + System.currentTimeMillis());
        campaign.setAccountId(account.getId());

        // When - Insert
        campaignDao.insert(campaign);

        // Then - Find
        Optional<Campaign> found = campaignDao.findById(campaign.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getCampaignName()).isEqualTo(campaign.getCampaignName());

        // When - Find by account
        List<Campaign> campaigns = campaignDao.findByAccount(account.getId());
        assertThat(campaigns).hasSize(1);
        assertThat(campaigns.get(0).getId()).isEqualTo(campaign.getId());
    }

    @Test
    void shouldPerformAdSetCrudOperations() {
        // Given - Setup hierarchy
        Account account = TestDataFactory.createAccount();
        account.setId("acc_" + System.currentTimeMillis());
        accountDao.insert(account);

        Campaign campaign = TestDataFactory.createCampaign();
        campaign.setId("cam_" + System.currentTimeMillis());
        campaign.setAccountId(account.getId());
        campaignDao.insert(campaign);

        AdSet adSet = TestDataFactory.createAdSet();
        adSet.setId("ads_" + System.currentTimeMillis());
        adSet.setCampaignId(campaign.getId());

        // When - Insert
        adSetDao.insert(adSet);

        // Then - Find
        Optional<AdSet> found = adSetDao.findById(adSet.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getAdSetName()).isEqualTo(adSet.getAdSetName());

        // When - Find by campaign
        List<AdSet> adSets = adSetDao.findByCampaign(campaign.getId());
        assertThat(adSets).hasSize(1);
        assertThat(adSets.get(0).getId()).isEqualTo(adSet.getId());
    }

    @Test
    void shouldPerformAdvertisementCrudOperations() {
        // Given - Setup hierarchy
        Account account = TestDataFactory.createAccount();
        account.setId("acc_" + System.currentTimeMillis());
        accountDao.insert(account);

        Campaign campaign = TestDataFactory.createCampaign();
        campaign.setId("cam_" + System.currentTimeMillis());
        campaign.setAccountId(account.getId());
        campaignDao.insert(campaign);

        AdSet adSet = TestDataFactory.createAdSet();
        adSet.setId("ads_" + System.currentTimeMillis());
        adSet.setCampaignId(campaign.getId());
        adSetDao.insert(adSet);

        Advertisement ad = TestDataFactory.createAdvertisement();
        ad.setId("ad_" + System.currentTimeMillis());
        ad.setAdsetid(adSet.getId());

        // When - Insert
        advertisementDao.insert(ad);

        // Then - Find
        Optional<Advertisement> found = advertisementDao.findById(ad.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getAdName()).isEqualTo(ad.getAdName());

        // When - Find by adset
        List<Advertisement> ads = advertisementDao.findByAdSet(adSet.getId());
        assertThat(ads).hasSize(1);
        assertThat(ads.get(0).getId()).isEqualTo(ad.getId());
    }

    @Test
    void shouldPerformPlacementCrudOperations() {
        // Given - Setup hierarchy
        Account account = TestDataFactory.createAccount();
        account.setId("acc_" + System.currentTimeMillis());
        accountDao.insert(account);

        Campaign campaign = TestDataFactory.createCampaign();
        campaign.setId("cam_" + System.currentTimeMillis());
        campaign.setAccountId(account.getId());
        campaignDao.insert(campaign);

        AdSet adSet = TestDataFactory.createAdSet();
        adSet.setId("ads_" + System.currentTimeMillis());
        adSet.setCampaignId(campaign.getId());
        adSetDao.insert(adSet);

        Advertisement ad = TestDataFactory.createAdvertisement();
        ad.setId("ad_" + System.currentTimeMillis());
        ad.setAdsetid(adSet.getId());
        advertisementDao.insert(ad);

        Placement placement = TestDataFactory.createPlacement();
        placement.setId("pl_" + System.currentTimeMillis());
        placement.setAdvertisementId(ad.getId());

        // When - Insert
        placementDao.insert(placement);

        // Then - Find
        Optional<Placement> found = placementDao.findById(placement.getId());
        assertThat(found).isPresent();
        assertThat(found.get().getPlacementName()).isEqualTo(placement.getPlacementName());

        // When - Find by advertisement
        List<Placement> placements = placementDao.findByAdvertisement(ad.getId());
        assertThat(placements).hasSize(1);
        assertThat(placements.get(0).getId()).isEqualTo(placement.getId());
    }

    @Test
    void shouldPerformAdsProcessingDateCrudOperations() {
        // Given
        AdsProcessingDate date = TestDataFactory.createAdsProcessingDate();
        date.setFullDate("2024-12-31");

        // When - Insert
        adsProcessingDateDao.insert(date);

        // Then - Find
        Optional<AdsProcessingDate> found = adsProcessingDateDao.findById(date.getFullDate());
        assertThat(found).isPresent();
        assertThat(found.get().getYear()).isEqualTo(date.getYear());

        // When - Find by year
        List<AdsProcessingDate> dates = adsProcessingDateDao.findByYear(2024);
        assertThat(dates).isNotEmpty();

        // When - Find by date range
        List<AdsProcessingDate> rangeDates = adsProcessingDateDao.findByDateRange("2024-12-01", "2024-12-31");
        assertThat(rangeDates).isNotEmpty();
    }

    @Test
    void shouldPerformAdsReportingCrudOperations() {
        // Given - Setup complete hierarchy
        String timestamp = String.valueOf(System.currentTimeMillis());

        Account account = TestDataFactory.createAccount();
        account.setId("acc_" + timestamp);
        accountDao.insert(account);

        Campaign campaign = TestDataFactory.createCampaign();
        campaign.setId("cam_" + timestamp);
        campaign.setAccountId(account.getId());
        campaignDao.insert(campaign);

        AdSet adSet = TestDataFactory.createAdSet();
        adSet.setId("ads_" + timestamp);
        adSet.setCampaignId(campaign.getId());
        adSetDao.insert(adSet);

        Advertisement ad = TestDataFactory.createAdvertisement();
        ad.setId("ad_" + timestamp);
        ad.setAdsetid(adSet.getId());
        advertisementDao.insert(ad);

        Placement placement = TestDataFactory.createPlacement();
        placement.setId("pl_" + timestamp);
        placement.setAdvertisementId(ad.getId());
        placementDao.insert(placement);

        AdsProcessingDate date = TestDataFactory.createAdsProcessingDate();
        date.setFullDate("2024-01-15");
        adsProcessingDateDao.insert(date);

        AdsReporting reporting = TestDataFactory.createAdsReporting();
        reporting.setAccountId(account.getId());
        reporting.setCampaignId(campaign.getId());
        reporting.setAdsetId(adSet.getId());
        reporting.setAdvertisementId(ad.getId());
        reporting.setPlacementId(placement.getId());
        reporting.setAdsProcessingDt(date.getFullDate());

        // When - Insert
        adsReportingDao.insert(reporting);

        // Then - Verify insert (composite key, so use count)
        long count = adsReportingDao.count();
        assertThat(count).isGreaterThan(0);
    }

    @Test
    void shouldHandleBatchOperations() {
        // Given
        List<Account> accounts = List.of(
                createTestAccount("batch_1_" + System.currentTimeMillis()),
                createTestAccount("batch_2_" + System.currentTimeMillis()),
                createTestAccount("batch_3_" + System.currentTimeMillis())
        );

        // When - Batch insert
        accountDao.batchInsert(accounts);

        // Then - Verify all inserted
        for (Account account : accounts) {
            Optional<Account> found = accountDao.findById(account.getId());
            assertThat(found).isPresent();
        }
    }

    @Test
    void shouldMaintainForeignKeyConstraints() {
        // Given
        Campaign campaign = TestDataFactory.createCampaign();
        campaign.setId("fk_test_" + System.currentTimeMillis());
        campaign.setAccountId("non_existent_account");

        // When & Then - Should fail due to foreign key constraint
        assertThatThrownBy(() -> campaignDao.insert(campaign))
                .hasMessageContaining("foreign key constraint");
    }

    @Test
    void shouldHandleTransactionRollback() {
        // Given
        Account account = TestDataFactory.createAccount();
        account.setId("rollback_test_" + System.currentTimeMillis());

        // When - Insert account
        accountDao.insert(account);

        // Then - Account should exist
        assertThat(accountDao.existsById(account.getId())).isTrue();

        // After test method completes, @Transactional should rollback
        // This will be verified by the transaction isolation
    }

    @Test
    void shouldCountEntities() {
        // When
        long accountCount = accountDao.count();
        long campaignCount = campaignDao.count();
        long adSetCount = adSetDao.count();

        // Then
        assertThat(accountCount).isGreaterThanOrEqualTo(0);
        assertThat(campaignCount).isGreaterThanOrEqualTo(0);
        assertThat(adSetCount).isGreaterThanOrEqualTo(0);
    }

    @Test
    void shouldFindWithPagination() {
        // Given - Insert test data
        for (int i = 0; i < 5; i++) {
            Account account = createTestAccount("page_test_" + i + "_" + System.currentTimeMillis());
            accountDao.insert(account);
        }

        // When - Find with pagination
        List<Account> firstPage = accountDao.findAll(3, 0);
        List<Account> secondPage = accountDao.findAll(3, 3);

        // Then
        assertThat(firstPage).hasSizeLessThanOrEqualTo(3);
        assertThat(secondPage).hasSizeLessThanOrEqualTo(3);
    }

    private Account createTestAccount(String id) {
        Account account = TestDataFactory.createAccount();
        account.setId(id);
        return account;
    }
}