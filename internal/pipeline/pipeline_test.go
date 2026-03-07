package pipeline_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ssp/internal/adapter"
	"ssp/internal/adquality"
	"ssp/internal/auction"
	"ssp/internal/floor"
	"ssp/internal/monitor"
	"ssp/internal/openrtb"
	"ssp/internal/pipeline"
)

type fakeAdapter struct {
	id   string
	bids []openrtb.Bid
}

type errorAdapter struct {
	id  string
	err error
}

type noBidAdapter struct {
	id string
}

func (f *fakeAdapter) ID() string {
	return f.id
}

func (f *fakeAdapter) Name() string {
	return "fake"
}

func (f *fakeAdapter) Type() adapter.AdapterType {
	return adapter.TypeORTB
}

func (f *fakeAdapter) Supports(_ *openrtb.BidRequest) bool {
	return true
}

func (f *fakeAdapter) RequestBids(ctx context.Context, _ *openrtb.BidRequest) (*adapter.BidResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	bids := make([]openrtb.Bid, len(f.bids))
	copy(bids, f.bids)
	return &adapter.BidResult{Bids: bids}, nil
}

func (e *errorAdapter) ID() string {
	return e.id
}

func (e *errorAdapter) Name() string {
	return "error"
}

func (e *errorAdapter) Type() adapter.AdapterType {
	return adapter.TypeORTB
}

func (e *errorAdapter) Supports(_ *openrtb.BidRequest) bool {
	return true
}

func (e *errorAdapter) RequestBids(ctx context.Context, _ *openrtb.BidRequest) (*adapter.BidResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return nil, e.err
}

func (n *noBidAdapter) ID() string {
	return n.id
}

func (n *noBidAdapter) Name() string {
	return "no-bid"
}

func (n *noBidAdapter) Type() adapter.AdapterType {
	return adapter.TypeORTB
}

func (n *noBidAdapter) Supports(_ *openrtb.BidRequest) bool {
	return true
}

func (n *noBidAdapter) RequestBids(ctx context.Context, _ *openrtb.BidRequest) (*adapter.BidResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return &adapter.BidResult{AdapterID: n.id, NoBid: true, NoBidReason: "http_204_no_content"}, nil
}

func TestExecuteDefersNotificationsUntilFinalizeDelivery(t *testing.T) {
	var winCount atomic.Int64
	var billCount atomic.Int64
	var lossCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/win":
			winCount.Add(1)
		case "/bill":
			billCount.Add(1)
		case "/loss":
			lossCount.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	reg := adapter.NewRegistry()
	reg.Register(&fakeAdapter{
		id: "fake-adapter",
		bids: []openrtb.Bid{
			{
				ID:    "bid-win",
				ImpID: "imp-1",
				Price: 5.0,
				Adm:   "https://cdn.example.com/video.mp4",
				NURL:  server.URL + "/win",
				BURL:  server.URL + "/bill",
			},
			{
				ID:    "bid-loss",
				ImpID: "imp-1",
				Price: 4.0,
				Adm:   "https://cdn.example.com/video.mp4",
				LURL:  server.URL + "/loss",
			},
		},
	}, &adapter.AdapterConfig{ID: "fake-adapter", Name: "Fake Adapter", Type: adapter.TypeORTB, Endpoint: "http://unused", Status: 1})

	p := &pipeline.Pipeline{
		Registry:    reg,
		FloorEngine: floor.NewEngine(),
		AQScanner:   adquality.NewScanner(),
		Metrics:     monitor.New(),
		AuctionType: "first_price",
		DefaultTMax: 100,
	}

	req := &openrtb.BidRequest{
		ID:  "req-1",
		Imp: []openrtb.Imp{{ID: "imp-1", BidFloor: 1.0}},
	}

	result := p.Execute(context.Background(), req, server.URL)
	if result.Error != nil {
		t.Fatalf("unexpected execute error: %v", result.Error)
	}
	if result.Winner == nil {
		t.Fatal("expected a winner")
	}
	if !result.NotificationsPending {
		t.Fatal("expected notifications to remain pending until delivery finalization")
	}

	time.Sleep(150 * time.Millisecond)
	if winCount.Load() != 0 || billCount.Load() != 0 || lossCount.Load() != 0 {
		t.Fatalf("expected no notices before FinalizeDelivery, got win=%d bill=%d loss=%d",
			winCount.Load(), billCount.Load(), lossCount.Load())
	}

	p.FinalizeDelivery(result)
	waitForCount(t, &winCount, 1, time.Second, "win notice")
	waitForCount(t, &lossCount, 1, time.Second, "loss notice")

	time.Sleep(150 * time.Millisecond)
	if billCount.Load() != 0 {
		t.Fatalf("expected billing notice to remain deferred until callback, got %d", billCount.Load())
	}
	if result.NotificationsPending {
		t.Fatal("expected notifications to be marked complete after finalization")
	}
	if got := p.Metrics.BidLosses.Load(); got != 1 {
		t.Fatalf("expected one recorded bid loss, got %d", got)
	}

	auction.FireBillingNoticeByBidID(result.Winner.ID)
	waitForCount(t, &billCount, 1, time.Second, "billing notice")

	auction.FireBillingNoticeByBidID(result.Winner.ID)
	time.Sleep(150 * time.Millisecond)
	if billCount.Load() != 1 {
		t.Fatalf("expected billing notice to fire once, got %d", billCount.Load())
	}
}

func TestExecuteTracksAdapterErrorsSeparatelyFromInternalErrors(t *testing.T) {
	reg := adapter.NewRegistry()
	reg.Register(&errorAdapter{id: "error-adapter", err: context.DeadlineExceeded}, &adapter.AdapterConfig{ID: "error-adapter", Name: "Error Adapter", Type: adapter.TypeORTB, Endpoint: "http://unused", Status: 1})

	metrics := monitor.New()
	p := &pipeline.Pipeline{
		Registry:    reg,
		FloorEngine: floor.NewEngine(),
		AQScanner:   adquality.NewScanner(),
		Metrics:     metrics,
		AuctionType: "first_price",
		DefaultTMax: 100,
	}

	req := &openrtb.BidRequest{
		ID:  "req-error",
		Imp: []openrtb.Imp{{ID: "imp-1", BidFloor: 1.0, TagID: "tag-1"}},
		App: &openrtb.App{Bundle: "bundle-1"},
	}

	result := p.Execute(context.Background(), req, "https://ads.example.com")
	if !result.NoBid {
		t.Fatal("expected no bid result when the only adapter errors")
	}
	if got := metrics.Errors.Load(); got != 0 {
		t.Fatalf("expected internal error counter to remain 0, got %d", got)
	}
	if got := metrics.AdapterErrors.Load(); got != 1 {
		t.Fatalf("expected adapter error counter to be 1, got %d", got)
	}
	if got := metrics.NoBids.Load(); got != 1 {
		t.Fatalf("expected no-bid counter to be 1, got %d", got)
	}

	adapterEvents := metrics.GetTrafficEvents("adapter_error")
	if len(adapterEvents) == 0 {
		t.Fatal("expected adapter error traffic event to be recorded")
	}
	if !strings.Contains(adapterEvents[len(adapterEvents)-1].Details, "adapter=error-adapter") {
		t.Fatalf("expected adapter error details to include adapter id, got %q", adapterEvents[len(adapterEvents)-1].Details)
	}

	reasons := metrics.AdapterErrorReasonCounts(10)
	if len(reasons) == 0 || reasons[0].Reason != "error-adapter:timeout" {
		t.Fatalf("expected timeout reason for adapter error, got %#v", reasons)
	}

	noBidReasons := metrics.NoBidReasonCounts(10)
	if len(noBidReasons) == 0 || noBidReasons[0].Reason != "all_adapters_timed_out" {
		t.Fatalf("expected all_adapters_timed_out no-bid reason, got %#v", noBidReasons)
	}
}

func TestExecuteIncludesNoBidAdapterDetails(t *testing.T) {
	reg := adapter.NewRegistry()
	reg.Register(&noBidAdapter{id: "no-bid-adapter"}, &adapter.AdapterConfig{ID: "no-bid-adapter", Name: "No Bid Adapter", Type: adapter.TypeORTB, Endpoint: "http://unused", Status: 1})

	metrics := monitor.New()
	p := &pipeline.Pipeline{
		Registry:    reg,
		FloorEngine: floor.NewEngine(),
		AQScanner:   adquality.NewScanner(),
		Metrics:     metrics,
		AuctionType: "first_price",
		DefaultTMax: 100,
	}

	req := &openrtb.BidRequest{
		ID:  "req-no-bid",
		Imp: []openrtb.Imp{{ID: "imp-1", BidFloor: 0.5, TagID: "tag-1"}},
		App: &openrtb.App{Bundle: "bundle-1"},
	}

	result := p.Execute(context.Background(), req, "https://ads.example.com")
	if !result.NoBid {
		t.Fatal("expected no bid result when the only adapter explicitly no-bids")
	}

	noBidEvents := metrics.GetTrafficEvents("no_bid")
	if len(noBidEvents) == 0 {
		t.Fatal("expected no_bid traffic event to be recorded")
	}
	details := noBidEvents[len(noBidEvents)-1].Details
	if !strings.Contains(details, "selected_adapters=no-bid-adapter") {
		t.Fatalf("expected no_bid details to include selected adapter, got %q", details)
	}
	if !strings.Contains(details, "no_bid_adapters=no-bid-adapter") {
		t.Fatalf("expected no_bid details to include explicit no-bid adapter, got %q", details)
	}
	if !strings.Contains(details, "no_bid_reasons=no-bid-adapter(http_204_no_content)") {
		t.Fatalf("expected no_bid details to include explicit no-bid reason, got %q", details)
	}

	noBidReasons := metrics.NoBidReasonCounts(10)
	if len(noBidReasons) == 0 || noBidReasons[0].Reason != "all_adapters_no_bid" {
		t.Fatalf("expected all_adapters_no_bid reason, got %#v", noBidReasons)
	}
}

func waitForCount(t *testing.T, counter *atomic.Int64, want int64, timeout time.Duration, label string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: got %d want %d", label, counter.Load(), want)
}
