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

func TestDecisionAuditBundleSuppressesJunkWithoutFallback(t *testing.T) {
	req := &openrtb.BidRequest{App: &openrtb.App{Bundle: "1022720soccha", ID: "1022720soccha"}}

	if got := decisionAuditBundle(req, nil); got != "" {
		t.Fatalf("expected junk bundle to be suppressed, got %q", got)
	}
}

func TestEventRoutesCleanBundleFromTrackingQuery(t *testing.T) {
	app := fiber.New()
	metrics := monitor.New()
	registerEventRoutes(app, metrics)

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
