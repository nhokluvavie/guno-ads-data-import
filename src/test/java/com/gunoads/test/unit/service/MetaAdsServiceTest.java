//package com.gunoads.test.unit.service;
//
//import com.gunoads.service.MetaAdsService;
//import com.gunoads.connector.MetaAdsConnector;
//import com.gunoads.processor.DataTransformer;
//import com.gunoads.dao.*;
//import com.gunoads.model.dto.*;
//import com.gunoads.model.entity.*;
//import com.gunoads.test.unit.BaseUnitTest;
//import com.gunoads.test.util.TestDataFactory;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//
//import java.time.LocalDate;
//import java.util.List;
//
//import static org.assertj.core.api.Assertions.*;
//import static org.mockito.Mockito.*;
//
//class MetaAdsServiceTest extends BaseUnitTest {
//
//    @Mock private MetaAdsConnector metaAdsConnector;
//    @Mock private DataTransformer dataTransformer;
//    @Mock private AccountDao accountDao;
//    @Mock private CampaignDao campaignDao;
//    @Mock private AdSetDao adSetDao;
//    @Mock private AdvertisementDao advertisementDao;
//    @Mock private AdsReportingDao adsReportingDao;
//
//    @InjectMocks
//    private MetaAdsService metaAdsService;
//
//    @BeforeEach
//    void setUp() {
//        logTestStart();
//    }
//
//    @Test
//    void shouldSyncAccountHierarchy() {
//        // Given
//        List<MetaAccountDto> accountDtos = TestDataFactory.createMetaAccountDtoList(1);
//        List<Account> accounts = List.of(TestDataFactory.createAccount());
//        List<MetaCampaignDto> campaignDtos = TestDataFactory.createMetaCampaignDtoList(1);
//        List<Campaign> campaigns = List.of(TestDataFactory.createCampaign());
//
//        when(metaAdsConnector.fetchBusinessAccounts()).thenReturn(accountDtos);
//        when(dataTransformer.transformAccounts(accountDtos)).thenReturn(accounts);
//        when(metaAdsConnector.fetchCampaigns(any())).thenReturn(campaignDtos);
//        when(dataTransformer.transformCampaigns(campaignDtos)).thenReturn(campaigns);
//        when(metaAdsConnector.fetchAdSets(any())).thenReturn(List.of());
//        when(metaAdsConnector.fetchAds(any())).thenReturn(List.of());
//        when(accountDao.existsById(any())).thenReturn(false);
//
//        // When
//        metaAdsService.syncAccountHierarchy();
//
//        // Then
//        verify(metaAdsConnector).fetchBusinessAccounts();
//        verify(dataTransformer).transformAccounts(accountDtos);
//        verify(accountDao).insert(accounts.get(0));
//        verify(metaAdsConnector).fetchCampaigns(accounts.get(0).getId());
//    }
//
//    @Test
//    void shouldUpdateExistingAccount() {
//        // Given
//        List<MetaAccountDto> accountDtos = TestDataFactory.createMetaAccountDtoList(1);
//        List<Account> accounts = List.of(TestDataFactory.createAccount());
//
//        when(metaAdsConnector.fetchBusinessAccounts()).thenReturn(accountDtos);
//        when(dataTransformer.transformAccounts(accountDtos)).thenReturn(accounts);
//        when(accountDao.existsById(any())).thenReturn(true);
//        when(metaAdsConnector.fetchCampaigns(any())).thenReturn(List.of());
//        when(metaAdsConnector.fetchAdSets(any())).thenReturn(List.of());
//        when(metaAdsConnector.fetchAds(any())).thenReturn(List.of());
//
//        // When
//        metaAdsService.syncAccountHierarchy();
//
//        // Then
//        verify(accountDao).update(accounts.get(0));
//        verify(accountDao, never()).insert(any());
//    }
//
//    @Test
//    void shouldSyncPerformanceData() {
//        // Given
//        String accountId = "act_123";
//        LocalDate date = LocalDate.now().minusDays(1);
//        List<MetaInsightsDto> insightDtos = TestDataFactory.createMetaInsightsDtoList(1);
//        List<AdsReporting> reportingList = List.of(TestDataFactory.createAdsReporting());
//
//        when(metaAdsConnector.fetchInsights(accountId, date, date)).thenReturn(insightDtos);
//        when(dataTransformer.transformInsightsList(insightDtos)).thenReturn(reportingList);
//
//        // When
//        metaAdsService.syncPerformanceData(accountId, date, date);
//
//        // Then
//        verify(metaAdsConnector).fetchInsights(accountId, date, date);
//        verify(dataTransformer).transformInsightsList(insightDtos);
//        verify(adsReportingDao).batchInsert(reportingList);
//    }
//
//    @Test
//    void shouldHandleEmptyInsightsData() {
//        // Given
//        String accountId = "act_123";
//        LocalDate date = LocalDate.now().minusDays(1);
//
//        when(metaAdsConnector.fetchInsights(accountId, date, date)).thenReturn(List.of());
//
//        // When
//        metaAdsService.syncPerformanceData(accountId, date, date);
//
//        // Then
//        verify(metaAdsConnector).fetchInsights(accountId, date, date);
//        verify(dataTransformer, never()).transformInsightsList(any());
//        verify(adsReportingDao, never()).batchInsert(any());
//    }
//
//    @Test
//    void shouldTestConnectivity() {
//        // Given
//        when(metaAdsConnector.testConnectivity()).thenReturn(true);
//
//        // When
//        boolean result = metaAdsService.testConnectivity();
//
//        // Then
//        assertThat(result).isTrue();
//        verify(metaAdsConnector).testConnectivity();
//    }
//
//    @Test
//    void shouldGetSyncStatus() {
//        // Given
//        when(accountDao.count()).thenReturn(5L);
//        when(campaignDao.count()).thenReturn(20L);
//        when(adSetDao.count()).thenReturn(50L);
//        when(advertisementDao.count()).thenReturn(100L);
//        when(adsReportingDao.count()).thenReturn(1000L);
//        when(metaAdsConnector.testConnectivity()).thenReturn(true);
//
//        // When
//        MetaAdsService.SyncStatus status = metaAdsService.getSyncStatus();
//
//        // Then
//        assertThat(status.isConnected).isTrue();
//        assertThat(status.accountCount).isEqualTo(5L);
//        assertThat(status.campaignCount).isEqualTo(20L);
//        assertThat(status.adSetCount).isEqualTo(50L);
//        assertThat(status.adCount).isEqualTo(100L);
//        assertThat(status.reportingCount).isEqualTo(1000L);
//    }
//
//    @Test
//    void shouldHandleConnectivityFailure() {
//        // Given
//        when(metaAdsConnector.testConnectivity()).thenThrow(new RuntimeException("Connection failed"));
//
//        // When
//        boolean result = metaAdsService.testConnectivity();
//
//        // Then
//        assertThat(result).isFalse();
//    }
//}