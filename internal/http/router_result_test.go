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
		return handlePipelineServeResult(c, &pipeline.Pipeline{}, metrics, newStore(), &openrtb.BidRequest{ID: "req-1"}, &pipeline.Result{Error: errors.New("boom")}, "pipeline")
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
