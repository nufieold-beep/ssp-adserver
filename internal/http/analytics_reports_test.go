package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"ssp/internal/monitor"

	"github.com/gofiber/fiber/v2"
)

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
	if body.AdRequestFillRate != 20 {
		t.Fatalf("expected ad request fill rate 20, got %.2f", body.AdRequestFillRate)
	}
	if body.OpportunityFillRate != 37.5 {
		t.Fatalf("expected opportunity fill rate 37.5, got %.2f", body.OpportunityFillRate)
	}
	if body.NoBidRate != 62.5 {
		t.Fatalf("expected no-bid rate 62.5, got %.2f", body.NoBidRate)
	}
	if body.ECPM != 4.5 {
		t.Fatalf("expected eCPM 4.5, got %.2f", body.ECPM)
	}
	if body.GrossCPM != 6.0 {
		t.Fatalf("expected gross CPM 6.0, got %.2f", body.GrossCPM)
	}
	if body.TotalRevenue != 0.009 {
		t.Fatalf("expected net revenue 0.009, got %.6f", body.TotalRevenue)
	}
	if body.TotalGrossRevenue != 0.012 {
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
	if demandRows[0].DemandRevenueTotal != 0.009 {
		t.Fatalf("expected demand revenue total 0.009, got %.6f", demandRows[0].DemandRevenueTotal)
	}
	if demandRows[0].DemandGrossRevenueTotal != 0.012 {
		t.Fatalf("expected demand gross revenue total 0.012, got %.6f", demandRows[0].DemandGrossRevenueTotal)
	}
	if demandRows[0].GrossCPM != 8.0 {
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
	if supplyRows[0].Revenue != 0.009 {
		t.Fatalf("expected supply revenue 0.009, got %.6f", supplyRows[0].Revenue)
	}
	if supplyRows[0].GrossRevenue != 0.012 {
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
	if bundleRows[0].GrossCPM != 6.0 {
		t.Fatalf("expected bundle gross CPM 6.0, got %.2f", bundleRows[0].GrossCPM)
	}
}

func TestPersistedAnalyticsTotalsSurviveRuntimeStateReload(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "runtime_state.json")

	s := newStore()
	s.statePath = statePath
	s.supplyTags[11] = &SupplyTag{ID: 11, Name: "CTV App"}
	s.demandEndpoints[7] = &DemandEndpoint{ID: 7, Name: "DSP Alpha"}

	decision := AdDecision{
		SupplyID:   11,
		DemandEp:   "demand-ep-7",
		AppBundle:  "com.example.app",
		NetPrice:   3.0,
		GrossPrice: 4.0,
	}

	s.mu.Lock()
	s.recordPersistedAnalyticsLocked(decision)
	write := s.prepareSupplyDemandStateWriteLocked()
	s.mu.Unlock()
	if err := write.Persist(); err != nil {
		t.Fatalf("failed to persist runtime state: %v", err)
	}

	reloaded := newStore()
	if err := reloaded.loadSupplyDemandState(statePath); err != nil {
		t.Fatalf("failed to reload runtime state: %v", err)
	}
	reloaded.supplyTags[11] = &SupplyTag{ID: 11, Name: "CTV App"}
	reloaded.demandEndpoints[7] = &DemandEndpoint{ID: 7, Name: "DSP Alpha"}

	state := reloaded.snapshotAnalyticsState()
	if got := state.supplyTotals[11].Revenue; got != 0.003 {
		t.Fatalf("expected persisted supply revenue 0.003, got %.6f", got)
	}
	if got := state.demandTotals["demand-ep-7"].GrossRevenue; got != 0.004 {
		t.Fatalf("expected persisted demand gross revenue 0.004, got %.6f", got)
	}
	if got := state.bundleTotals["com.example.app"].FilledOpportunities; got != 1 {
		t.Fatalf("expected persisted bundle filled opportunities 1, got %d", got)
	}

	app := fiber.New()
	registerAnalyticsRoutes(app, reloaded, monitor.New())
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
	if rows[0].Revenue != 0.003 {
		t.Fatalf("expected persisted demand revenue 0.003, got %.6f", rows[0].Revenue)
	}
}
