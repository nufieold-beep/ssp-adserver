package http

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"ssp/internal/monitor"

	"github.com/gofiber/fiber/v2"
)

func almostEqualFloat(got, want float64) bool {
	return math.Abs(got-want) <= 1e-9
}

func TestAnalyticsOverviewUsesDerivedFillAndCPMFormulas(t *testing.T) {
	app := fiber.New()
	s := newStore()
	metrics := monitor.New()
	metrics.AdRequests.Add(10)
	metrics.AdOpps.Add(8)
	metrics.Impressions.Add(3)
	metrics.NoBids.Add(5)
	metrics.RecordWin(4.0)
	metrics.RecordWin(8.0)
	metrics.RecordSpend(3.0)
	metrics.RecordSpend(6.0)
	metrics.RecordGrossSpend(4.0)
	metrics.RecordGrossSpend(8.0)

	registerAnalyticsRoutes(app, s, metrics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/overview", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	defer resp.Body.Close()

	var body struct {
		FilledOpportunities int64   `json:"filled_opportunities"`
		AdRequestFillRate   float64 `json:"ad_request_fill_rate"`
		OpportunityFillRate float64 `json:"opportunity_fill_rate"`
		NoBidRate           float64 `json:"no_bid_rate"`
		ECPM                float64 `json:"ecpm"`
		GrossCPM            float64 `json:"gross_cpm"`
		TotalRevenue        float64 `json:"total_revenue"`
		TotalGrossRevenue   float64 `json:"total_gross_revenue"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode overview response: %v", err)
	}

	if body.FilledOpportunities != 2 {
		t.Fatalf("expected 2 filled opportunities, got %d", body.FilledOpportunities)
	}
	if !almostEqualFloat(body.AdRequestFillRate, 20) {
		t.Fatalf("expected ad request fill rate 20, got %.2f", body.AdRequestFillRate)
	}
	if !almostEqualFloat(body.OpportunityFillRate, 37.5) {
		t.Fatalf("expected opportunity fill rate 37.5, got %.2f", body.OpportunityFillRate)
	}
	if !almostEqualFloat(body.NoBidRate, 62.5) {
		t.Fatalf("expected no-bid rate 62.5, got %.2f", body.NoBidRate)
	}
	if !almostEqualFloat(body.ECPM, 4.5) {
		t.Fatalf("expected eCPM 4.5, got %.2f", body.ECPM)
	}
	if !almostEqualFloat(body.GrossCPM, 6.0) {
		t.Fatalf("expected gross CPM 6.0, got %.2f", body.GrossCPM)
	}
	if !almostEqualFloat(body.TotalRevenue, 0.009) {
		t.Fatalf("expected net revenue 0.009, got %.6f", body.TotalRevenue)
	}
	if !almostEqualFloat(body.TotalGrossRevenue, 0.012) {
		t.Fatalf("expected gross revenue 0.012, got %.6f", body.TotalGrossRevenue)
	}
}

func TestDemandSupplyAndBundleReportsUseClearingPriceAndTotals(t *testing.T) {
	app := fiber.New()
	s := newStore()
	metrics := monitor.New()

	s.supplyTags[11] = &SupplyTag{ID: 11, Name: "CTV App"}
	s.demandEndpoints[7] = &DemandEndpoint{ID: 7, Name: "DSP Alpha"}
	s.adDecisions = []AdDecision{
		{
			ADomain:    "ads.example",
			DemandEp:   "demand-ep-7",
			CreativeID: "creative-a",
			SupplyID:   11,
			AppBundle:  "com.example.app",
			BidPrice:   10.0,
			GrossPrice: 4.0,
			NetPrice:   3.0,
		},
		{
			ADomain:    "ads.example",
			DemandEp:   "demand-ep-7",
			CreativeID: "creative-b",
			SupplyID:   11,
			AppBundle:  "com.example.app",
			BidPrice:   12.0,
			GrossPrice: 8.0,
			NetPrice:   6.0,
		},
	}

	registerAnalyticsRoutes(app, s, metrics)

	// Demand report
	demandResp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/analytics/reports/demand", nil))
	if err != nil {
		t.Fatalf("unexpected demand report error: %v", err)
	}
	defer demandResp.Body.Close()
	var demandRows []demandReportRow
	if err := json.NewDecoder(demandResp.Body).Decode(&demandRows); err != nil {
		t.Fatalf("failed to decode demand report: %v", err)
	}
	if len(demandRows) != 2 {
		t.Fatalf("expected 2 demand rows, got %d", len(demandRows))
	}
	if !almostEqualFloat(demandRows[0].DemandRevenueTotal, 0.009) {
		t.Fatalf("expected demand revenue total 0.009, got %.6f", demandRows[0].DemandRevenueTotal)
	}
	if !almostEqualFloat(demandRows[0].DemandGrossRevenueTotal, 0.012) {
		t.Fatalf("expected demand gross revenue total 0.012, got %.6f", demandRows[0].DemandGrossRevenueTotal)
	}
	if !almostEqualFloat(demandRows[0].GrossCPM, 8.0) {
		t.Fatalf("expected first row gross CPM to use clearing price 8.0, got %.2f", demandRows[0].GrossCPM)
	}
	if demandRows[0].DemandName != "DSP Alpha" {
		t.Fatalf("expected resolved demand name DSP Alpha, got %q", demandRows[0].DemandName)
	}

	// Supply report
	supplyResp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/analytics/reports/supply", nil))
	if err != nil {
		t.Fatalf("unexpected supply report error: %v", err)
	}
	defer supplyResp.Body.Close()
	var supplyRows []supplyReportRow
	if err := json.NewDecoder(supplyResp.Body).Decode(&supplyRows); err != nil {
		t.Fatalf("failed to decode supply report: %v", err)
	}
	if len(supplyRows) != 1 {
		t.Fatalf("expected 1 supply row, got %d", len(supplyRows))
	}
	if !almostEqualFloat(supplyRows[0].Revenue, 0.009) {
		t.Fatalf("expected supply revenue 0.009, got %.6f", supplyRows[0].Revenue)
	}
	if !almostEqualFloat(supplyRows[0].GrossRevenue, 0.012) {
		t.Fatalf("expected supply gross revenue 0.012, got %.6f", supplyRows[0].GrossRevenue)
	}
	if supplyRows[0].SupplyName != "CTV App" {
		t.Fatalf("expected supply name CTV App, got %q", supplyRows[0].SupplyName)
	}

	// Bundle report
	bundleResp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/analytics/reports/bundles", nil))
	if err != nil {
		t.Fatalf("unexpected bundle report error: %v", err)
	}
	defer bundleResp.Body.Close()
	var bundleRows []bundleReportRow
	if err := json.NewDecoder(bundleResp.Body).Decode(&bundleRows); err != nil {
		t.Fatalf("failed to decode bundle report: %v", err)
	}
	if len(bundleRows) != 1 {
		t.Fatalf("expected 1 bundle row, got %d", len(bundleRows))
	}
	if bundleRows[0].AppBundle != "com.example.app" {
		t.Fatalf("expected canonical bundle com.example.app, got %q", bundleRows[0].AppBundle)
	}
	if !almostEqualFloat(bundleRows[0].GrossCPM, 6.0) {
		t.Fatalf("expected bundle gross CPM 6.0, got %.2f", bundleRows[0].GrossCPM)
	}
}

func TestPersistedAnalyticsTotalsSurviveRuntimeStateReload(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "runtime_state.json")

	s := newStore()
	s.metrics = monitor.New()
	s.statePath = statePath
	s.supplyTags[11] = &SupplyTag{ID: 11, Name: "CTV App"}
	s.demandEndpoints[7] = &DemandEndpoint{ID: 7, Name: "DSP Alpha"}
	s.metrics.LoadHourlyMetrics([]monitor.HourlyMetricBucket{{
		Hour:                time.Date(2026, time.March, 7, 9, 0, 0, 0, time.UTC),
		AdRequests:          5,
		AdOpportunities:     5,
		FilledOpportunities: 1,
		Impressions:         1,
		NoBids:              4,
		Revenue:             0.003,
		GrossRevenue:        0.004,
	}})

	decision := AdDecision{
		SupplyID:   11,
		DemandEp:   "demand-ep-7",
		AppBundle:  "com.example.app",
		NetPrice:   3.0,
		GrossPrice: 4.0,
	}

	s.mu.Lock()
	s.recordPersistedAnalyticsLocked(decision)
	s.loadMetricsExportBucketsLocked([]metricsExportBucket{{
		Hour:                time.Date(2026, time.March, 7, 9, 0, 0, 0, time.UTC),
		SourceID:            11,
		DemandEndpointID:    7,
		CountryCode:         "USA",
		BundleID:            "com.example.app",
		AdRequests:          1,
		AdOpportunities:     1,
		FilledOpportunities: 1,
		Impressions:         1,
		SourceIDRevenue:     0.003,
		SourceIDFloorCPMSum: 2.5,
		TotalRevenue:        0.004,
	}})
	write := s.prepareSupplyDemandStateWriteLocked()
	s.mu.Unlock()
	if err := write.Persist(); err != nil {
		t.Fatalf("failed to persist runtime state: %v", err)
	}

	reloaded := newStore()
	reloaded.metrics = monitor.New()
	if err := reloaded.loadSupplyDemandState(statePath); err != nil {
		t.Fatalf("failed to reload runtime state: %v", err)
	}
	reloaded.supplyTags[11] = &SupplyTag{ID: 11, Name: "CTV App"}
	reloaded.demandEndpoints[7] = &DemandEndpoint{ID: 7, Name: "DSP Alpha"}

	state := reloaded.snapshotAnalyticsState()
	if got := state.supplyTotals[11].Revenue; !almostEqualFloat(got, 0.003) {
		t.Fatalf("expected persisted supply revenue 0.003, got %.6f", got)
	}
	if got := state.demandTotals["demand-ep-7"].GrossRevenue; !almostEqualFloat(got, 0.004) {
		t.Fatalf("expected persisted demand gross revenue 0.004, got %.6f", got)
	}
	if got := state.bundleTotals["com.example.app"].FilledOpportunities; got != 1 {
		t.Fatalf("expected persisted bundle filled opportunities 1, got %d", got)
	}
	hourly := reloaded.metrics.SnapshotHourlyMetrics()
	if len(hourly) != 1 {
		t.Fatalf("expected 1 persisted hourly metrics bucket, got %d", len(hourly))
	}
	if got := hourly[0].Revenue; !almostEqualFloat(got, 0.003) {
		t.Fatalf("expected persisted hourly revenue 0.003, got %.6f", got)
	}
	exportBuckets := reloaded.snapshotMetricsExportBuckets()
	if len(exportBuckets) != 1 {
		t.Fatalf("expected 1 persisted export metrics bucket, got %d", len(exportBuckets))
	}
	if got := exportBuckets[0].SourceID; got != 11 {
		t.Fatalf("expected persisted export source id 11, got %d", got)
	}
	if got := exportBuckets[0].DemandEndpointID; got != 7 {
		t.Fatalf("expected persisted export demand endpoint id 7, got %d", got)
	}
	if got := exportBuckets[0].SourceIDFloorCPMSum; !almostEqualFloat(got, 2.5) {
		t.Fatalf("expected persisted source floor cpm sum 2.5, got %.2f", got)
	}

	app := fiber.New()
	registerAnalyticsRoutes(app, reloaded, reloaded.metrics)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/analytics/reports/demand-totals", nil))
	if err != nil {
		t.Fatalf("unexpected demand totals request error: %v", err)
	}
	defer resp.Body.Close()

	var rows []demandTotalsReportRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatalf("failed to decode demand totals response: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 persisted demand totals row, got %d", len(rows))
	}
	if rows[0].DemandName != "DSP Alpha" {
		t.Fatalf("expected reloaded demand name DSP Alpha, got %q", rows[0].DemandName)
	}
	if !almostEqualFloat(rows[0].Revenue, 0.003) {
		t.Fatalf("expected persisted demand revenue 0.003, got %.6f", rows[0].Revenue)
	}
}

func TestBuildMetricsExportRowsGroupsByDateAndHour(t *testing.T) {
	buckets := []metricsExportBucket{
		{
			Hour:                time.Date(2026, time.March, 6, 10, 0, 0, 0, time.UTC),
			SourceID:            11,
			DemandEndpointID:    77,
			CountryCode:         "USA",
			BundleID:            "com.example.app",
			AdRequests:          10,
			AdOpportunities:     10,
			FilledOpportunities: 4,
			Impressions:         3,
			SourceIDRevenue:     0.012,
			SourceIDFloorCPMSum: 8.0,
			TotalRevenue:        0.016,
		},
		{
			Hour:                time.Date(2026, time.March, 6, 11, 0, 0, 0, time.UTC),
			SourceID:            11,
			DemandEndpointID:    77,
			CountryCode:         "USA",
			BundleID:            "com.example.app",
			AdRequests:          5,
			AdOpportunities:     5,
			FilledOpportunities: 1,
			Impressions:         1,
			SourceIDRevenue:     0.003,
			SourceIDFloorCPMSum: 2.0,
			TotalRevenue:        0.004,
		},
		{
			Hour:                time.Date(2026, time.March, 7, 9, 0, 0, 0, time.UTC),
			SourceID:            15,
			DemandEndpointID:    88,
			CountryCode:         "CAN",
			BundleID:            "com.example.other",
			AdRequests:          20,
			AdOpportunities:     20,
			FilledOpportunities: 10,
			Impressions:         8,
			SourceIDRevenue:     0.030,
			SourceIDFloorCPMSum: 20.0,
			TotalRevenue:        0.040,
		},
	}

	dateQuery, err := resolveMetricsExportQuery("custom", "date", "2026-03-06", "2026-03-07", "0", "23", time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("unexpected date export query error: %v", err)
	}
	dateRows := buildMetricsExportRows(buckets, dateQuery)
	if len(dateRows) != 2 {
		t.Fatalf("expected 2 daily rows, got %d", len(dateRows))
	}
	if dateRows[0].Date != "2026-03-06" || dateRows[0].AdRequests != 15 || dateRows[0].Impressions != 4 {
		t.Fatalf("unexpected first daily row: %+v", dateRows[0])
	}
	if dateRows[0].SourceID != 11 || dateRows[0].DemandEndpointID != 77 || dateRows[0].CountryCode != "USA" || dateRows[0].BundleID != "com.example.app" {
		t.Fatalf("expected daily row dimensions to be preserved, got %+v", dateRows[0])
	}
	if !almostEqualFloat(dateRows[0].ECPM, 4.0) {
		t.Fatalf("expected first daily row total eCPM 4.0, got %.2f", dateRows[0].ECPM)
	}
	if !almostEqualFloat(dateRows[0].SourceIDRevenue, 0.015) {
		t.Fatalf("expected first daily row source revenue 0.015, got %.6f", dateRows[0].SourceIDRevenue)
	}
	if !almostEqualFloat(dateRows[0].SourceIDECPM, 2.0) {
		t.Fatalf("expected first daily row source eCPM 2.0 from floor price, got %.2f", dateRows[0].SourceIDECPM)
	}

	hourQuery, err := resolveMetricsExportQuery("custom", "hour", "2026-03-06", "2026-03-06", "10", "11", time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("unexpected hourly export query error: %v", err)
	}
	hourRows := buildMetricsExportRows(buckets, hourQuery)
	if len(hourRows) != 2 {
		t.Fatalf("expected 2 hourly rows, got %d", len(hourRows))
	}
	if hourRows[0].Hour != "10:00" || hourRows[1].Hour != "11:00" {
		t.Fatalf("expected hourly rows to preserve separate UTC hour values, got %+v", hourRows)
	}

	summaryQuery, err := resolveMetricsExportQuery("today", "summary", "", "", "", "", time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("unexpected summary export query error: %v", err)
	}
	summaryRows := buildMetricsExportRows(buckets, summaryQuery)
	if len(summaryRows) != 1 {
		t.Fatalf("expected 1 summary row for today, got %d", len(summaryRows))
	}
	if summaryRows[0].Date != "" || summaryRows[0].Hour != "" {
		t.Fatalf("expected summary rows without date/hour columns, got %+v", summaryRows[0])
	}
}

func TestMetricsExportRouteReturnsCSV(t *testing.T) {
	app := fiber.New()
	s := newStore()
	metrics := monitor.New()
	s.mu.Lock()
	s.loadMetricsExportBucketsLocked([]metricsExportBucket{{
		Hour:                time.Date(2026, time.March, 6, 10, 0, 0, 0, time.UTC),
		SourceID:            11,
		DemandEndpointID:    77,
		CountryCode:         "USA",
		BundleID:            "com.example.app",
		AdRequests:          10,
		AdOpportunities:     10,
		FilledOpportunities: 4,
		Impressions:         3,
		SourceIDRevenue:     0.012,
		SourceIDFloorCPMSum: 8.0,
		TotalRevenue:        0.016,
	}})
	s.mu.Unlock()

	registerAnalyticsRoutes(app, s, metrics)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analytics/reports/export-metrics?preset=custom&group_by=hour&start_date=2026-03-06&end_date=2026-03-06&start_hour=10&end_hour=10&format=csv", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected export route error: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read csv export body: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "Date,Hour,Source ID,Demand ORTB Endpoint ID,Country Code,Country,Bundle ID,Ad Requests,Ad Opportunities,Impressions,Source ID Revenue,Source ID Floor eCPM,Demand ORTB Endpoints Revenue,eCPM") {
		t.Fatalf("expected csv header, got %q", text)
	}
	if !strings.Contains(text, "2026-03-06,10:00,11,77,USA,United States,com.example.app,10,10,3,0.012000,2.00,0.016000,4.00") {
		t.Fatalf("expected csv row for exported hour, got %q", text)
	}
}

func TestBuildMetricsExportRowsFallsBackToSourceRevenueWhenFloorCPMIsMissing(t *testing.T) {
	buckets := []metricsExportBucket{{
		Hour:                time.Date(2026, time.March, 7, 9, 0, 0, 0, time.UTC),
		SourceID:            11,
		DemandEndpointID:    7,
		CountryCode:         "USA",
		BundleID:            "com.example.app",
		AdRequests:          1,
		AdOpportunities:     1,
		FilledOpportunities: 1,
		Impressions:         1,
		SourceIDRevenue:     0.003,
		TotalRevenue:        0.004,
	}}

	query, err := resolveMetricsExportQuery("today", "summary", "", "", "", "", time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("unexpected summary export query error: %v", err)
	}
	rows := buildMetricsExportRows(buckets, query)
	if len(rows) != 1 {
		t.Fatalf("expected 1 summary row, got %d", len(rows))
	}
	if !almostEqualFloat(rows[0].SourceIDECPM, 3.0) {
		t.Fatalf("expected legacy source eCPM fallback 3.0, got %.2f", rows[0].SourceIDECPM)
	}
}

func TestMetricsExportLoadMigratesLegacyMarginRevenueToSupplyRevenue(t *testing.T) {
	s := newStore()
	s.mu.Lock()
	s.loadMetricsExportBucketsLocked([]metricsExportBucket{{
		Hour:                      time.Date(2026, time.March, 7, 9, 0, 0, 0, time.UTC),
		SourceID:                  11,
		DemandEndpointID:          7,
		CountryCode:               "USA",
		BundleID:                  "com.example.app",
		AdRequests:                1,
		AdOpportunities:           1,
		FilledOpportunities:       1,
		Impressions:               1,
		LegacySourceMarginRevenue: 0.001,
		TotalRevenue:              0.004,
	}})
	s.mu.Unlock()

	buckets := s.snapshotMetricsExportBuckets()
	if len(buckets) != 1 {
		t.Fatalf("expected 1 migrated export bucket, got %d", len(buckets))
	}
	if !almostEqualFloat(buckets[0].SourceIDRevenue, 0.003) {
		t.Fatalf("expected migrated supply revenue 0.003, got %.6f", buckets[0].SourceIDRevenue)
	}
}

func TestMetricsExportLoadBackfillsGrossEqualSourceRevenueUsingEndpointMargin(t *testing.T) {
	s := newStore()
	s.demandEndpoints[7] = &DemandEndpoint{ID: 7, Margin: 25}
	s.mu.Lock()
	s.loadMetricsExportBucketsLocked([]metricsExportBucket{{
		Hour:                time.Date(2026, time.March, 7, 9, 0, 0, 0, time.UTC),
		SourceID:            11,
		DemandEndpointID:    7,
		CountryCode:         "USA",
		BundleID:            "com.example.app",
		AdRequests:          1,
		AdOpportunities:     1,
		FilledOpportunities: 1,
		Impressions:         1,
		SourceIDRevenue:     0.004,
		TotalRevenue:        0.004,
	}})
	s.mu.Unlock()

	buckets := s.snapshotMetricsExportBuckets()
	if len(buckets) != 1 {
		t.Fatalf("expected 1 normalized export bucket, got %d", len(buckets))
	}
	if !almostEqualFloat(buckets[0].SourceIDRevenue, 0.003) {
		t.Fatalf("expected backfilled supply revenue 0.003, got %.6f", buckets[0].SourceIDRevenue)
	}
}
