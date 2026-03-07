package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"ssp/internal/monitor"
	"ssp/internal/openrtb"
	"ssp/internal/pipeline"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestHandlePipelineServeResultDoesNotDoubleRecordPipelineErrors(t *testing.T) {
	app := fiber.New()
	metrics := monitor.New()
	metrics.RecordError()

	app.Get("/", func(c *fiber.Ctx) error {
		return handlePipelineServeResult(c, &pipeline.Pipeline{}, metrics, newStore(), &openrtb.BidRequest{ID: "req-1"}, &pipeline.Result{Error: errors.New("boom")}, nil, "pipeline")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, resp.StatusCode)
	}
	if got := metrics.Errors.Load(); got != 1 {
		t.Fatalf("expected pipeline error to be counted once, got %d", got)
	}
}

func TestDecisionAuditSourceUsesSupplyTagID(t *testing.T) {
	tag := &SupplyTag{ID: 42, SlotID: "slot-42", Name: "CTV Source"}

	if got := decisionAuditSource(nil, tag, "supply_tag"); got != "42" {
		t.Fatalf("expected supply tag ID to be used as decision source, got %q", got)
	}
}

func TestRecordAdDecisionStoresSupplySourceID(t *testing.T) {
	s := newStore()
	s.recordAdDecision(&openrtb.BidRequest{App: &openrtb.App{Bundle: "B072M565NX29an"}}, nil, 0, 42, "42", "", "", 0, "", "served")

	if len(s.adDecisions) != 1 {
		t.Fatalf("expected one decision record, got %d", len(s.adDecisions))
	}
	if got := s.adDecisions[0].SupplyID; got != 42 {
		t.Fatalf("expected supply source id 42, got %d", got)
	}
	if got := s.adDecisions[0].RawBundle; got != "B072M565NX29an" {
		t.Fatalf("expected raw bundle to be stored, got %q", got)
	}
}

func TestDecisionAuditBundlePrefersConfiguredBundleForNonCanonicalRequestBundle(t *testing.T) {
	req := &openrtb.BidRequest{App: &openrtb.App{Bundle: "B00V3UTTPSernsp", ID: "B00V3UTTPSernsp"}}
	tag := &SupplyTag{AppBundle: "com.amazon.firetv.ernsp"}

	if got := decisionAuditBundle(req, tag); got != "com.amazon.firetv.ernsp" {
		t.Fatalf("expected configured app bundle fallback, got %q", got)
	}
}

func TestDecisionAuditBundleNormalizesPublisherBundle(t *testing.T) {
	req := &openrtb.BidRequest{App: &openrtb.App{Bundle: " FireTV/iFood TV OPMP "}}

	if got := decisionAuditBundle(req, nil); got != "firetv.ifood.tv.opmp" {
		t.Fatalf("expected normalized bundle, got %q", got)
	}
}

func TestDecisionAuditBundleDerivesFromStoreURLWhenRequestBundleIsJunk(t *testing.T) {
	req := &openrtb.BidRequest{App: &openrtb.App{Bundle: "1022720soccha", StoreURL: "https://play.google.com/store/apps/details?id=com.social.chat.app"}}

	if got := decisionAuditBundle(req, nil); got != "com.social.chat.app" {
		t.Fatalf("expected canonical bundle from store URL, got %q", got)
	}
}

func TestDecisionAuditBundleRejectsEncodedURLBundleAndUsesStoreURL(t *testing.T) {
	req := &openrtb.BidRequest{App: &openrtb.App{
		Bundle:   "https3a2f2fwww.vizio.com2fen2fsmart.tv.apps3fappname3ddantdm",
		StoreURL: "https://www.vizio.com/en/smart-tv-apps?appName=dantdm",
	}}

	if got := decisionAuditBundle(req, nil); got != "vizio.dantdm" {
		t.Fatalf("expected vizio bundle from store URL fallback, got %q", got)
	}
}

func TestDecisionAuditBundleSuppressesJunkWithoutFallback(t *testing.T) {
	req := &openrtb.BidRequest{App: &openrtb.App{Bundle: "1022720soccha", ID: "1022720soccha"}}

	if got := decisionAuditBundle(req, nil); got != "" {
		t.Fatalf("expected junk bundle to be suppressed, got %q", got)
	}
}

func TestEventRoutesCleanBundleFromTrackingQuery(t *testing.T) {
	app := fiber.New()
	metrics := monitor.New()
	registerEventRoutes(app, newStore(), metrics)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/event/impression?rid=req-1&bndl=1022720soccha", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, resp.StatusCode)
	}

	events := metrics.GetTrafficEvents("vast_impression")
	if len(events) != 1 {
		t.Fatalf("expected one vast impression event, got %d", len(events))
	}
	if got := events[0].Bundle; got != "" {
		t.Fatalf("expected junk callback bundle to be suppressed, got %q", got)
	}
}

func TestDeliveryBlockedExportRowLeavesDemandEndpointUnattributed(t *testing.T) {
	app := fiber.New()
	metrics := monitor.New()
	s := newStore()
	s.campaigns[7] = &Campaign{
		ID:          7,
		Name:        "Budget Blocked",
		Status:      1,
		ADomain:     "ads.example",
		BudgetDaily: 0.001,
		SpentToday:  0.001,
	}
	s.rebuildCampaignIndexLocked()

	req := &openrtb.BidRequest{
		ID:  "req-blocked-export",
		App: &openrtb.App{Bundle: "com.example.app"},
		Device: &openrtb.Device{
			Geo: &openrtb.Geo{Country: "USA"},
		},
	}
	result := &pipeline.Result{
		Winner: &openrtb.Bid{
			ID:      "bid-1",
			CrID:    "creative-1",
			Seat:    "seat-1",
			Price:   4.0,
			ADomain: []string{"ads.example"},
		},
		WinPrice: 4.0,
		VAST:     "<VAST/>",
	}

	app.Get("/", func(c *fiber.Ctx) error {
		return handlePipelineServeResult(c, &pipeline.Pipeline{}, metrics, s, req, result, &SupplyTag{ID: 42}, "supply_tag")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected no-ad XML status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	buckets := s.snapshotMetricsExportBuckets()
	if len(buckets) != 1 {
		t.Fatalf("expected 1 export bucket, got %d", len(buckets))
	}
	if got := buckets[0].DemandEndpointID; got != 0 {
		t.Fatalf("expected delivery-blocked export row to leave demand endpoint unattributed, got endpoint id %d", got)
	}
	if got := buckets[0].SourceID; got != 42 {
		t.Fatalf("expected source id 42 in export bucket, got %d", got)
	}
	if got := buckets[0].AdRequests; got != 1 {
		t.Fatalf("expected one attributed ad request, got %d", got)
	}
	if got := buckets[0].Impressions; got != 0 {
		t.Fatalf("expected no impressions for blocked delivery, got %d", got)
	}
}

func TestServedExportRowUsesDemandORTBEndpointID(t *testing.T) {
	app := fiber.New()
	metrics := monitor.New()
	s := newStore()
	s.campaigns[7] = &Campaign{
		ID:          7,
		Name:        "Served Campaign",
		Status:      1,
		ADomain:     "ads.example",
		BudgetDaily: 10,
	}
	s.rebuildCampaignIndexLocked()

	req := &openrtb.BidRequest{
		ID:  "req-served-export",
		Imp: []openrtb.Imp{{BidFloor: 2.5}},
		App: &openrtb.App{Bundle: "com.example.app"},
		Device: &openrtb.Device{
			Geo: &openrtb.Geo{Country: "USA"},
		},
	}
	result := &pipeline.Result{
		Winner: &openrtb.Bid{
			ID:        "bid-1",
			CrID:      "creative-1",
			Seat:      "seat-1",
			Price:     4.0,
			Margin:    0.25,
			ADomain:   []string{"ads.example"},
			DemandSrc: "demand-ep-9",
		},
		WinPrice:  4.0,
		VAST:      "<VAST/>",
		AdapterID: "demand-ep-9",
	}

	app.Get("/", func(c *fiber.Ctx) error {
		return handlePipelineServeResult(c, &pipeline.Pipeline{}, metrics, s, req, result, &SupplyTag{ID: 42}, "supply_tag")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected served XML status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	buckets := s.snapshotMetricsExportBuckets()
	if len(buckets) != 1 {
		t.Fatalf("expected 1 export bucket, got %d", len(buckets))
	}
	if got := buckets[0].DemandEndpointID; got != 9 {
		t.Fatalf("expected demand ORTB endpoint id 9, got %d", got)
	}
	if got := buckets[0].SourceIDRevenue; got != 0.003 {
		t.Fatalf("expected source supply revenue 0.003, got %.6f", got)
	}
	if got := buckets[0].SourceIDFloorCPMSum; got != 2.5 {
		t.Fatalf("expected source floor cpm sum 2.5, got %.2f", got)
	}
	if got := buckets[0].TotalRevenue; got != 0.004 {
		t.Fatalf("expected total revenue 0.004, got %.6f", got)
	}
}
