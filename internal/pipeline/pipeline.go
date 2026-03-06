package pipeline

import (
	"context"
	"fmt"
	"ssp/internal/adapter"
	"ssp/internal/adquality"
	"ssp/internal/auction"
	"ssp/internal/eventbus"
	"ssp/internal/floor"
	"ssp/internal/monitor"
	"ssp/internal/openrtb"
	"ssp/internal/vast"
	"time"
)

// Pipeline implements the multi-stage ad serving flow used by enterprise SSPs.
// FreeWheel and Magnite both process requests through discrete stages:
//
//	Request → Validate → Enrich → Floor → Route → Bid → Quality → Auction → Serve
//
// Each stage is autonomous and publishes events to the bus.
type Pipeline struct {
	Registry    *adapter.Registry
	FloorEngine *floor.Engine
	AQScanner   *adquality.Scanner
	Metrics     *monitor.Metrics
	Bus         *eventbus.Bus
	AuctionType string
	DefaultTMax int
}

// Result holds the output of the full pipeline execution.
type Result struct {
	Winner       *openrtb.Bid
	WinPrice     float64
	Losers       []openrtb.Bid
	VAST         string
	RequestID    string
	AdapterID    string
	AuctionType  string
	BidLatency   time.Duration
	TotalLatency time.Duration
	NoBid        bool
	Error        error
	BaseURL      string // populated by caller for tracking URLs
}

// Execute runs the full ad serving pipeline for a single request.
// baseURL is the publicly-reachable server origin for VAST tracking URLs.
// adapterIDs optionally restricts which demand adapters receive bid requests.
// Pass nil to fan out to all active adapters (no mapping filter).
func (p *Pipeline) Execute(ctx context.Context, req *openrtb.BidRequest, baseURL string, adapterIDs ...[]string) *Result {
	start := time.Now()
	result := &Result{RequestID: req.ID, BaseURL: baseURL}

	// ── Stage 1: Record request ──
	p.Metrics.RecordAdRequest()
	p.Metrics.RecordAdOpp()

	bundle := ""
	if req.App != nil {
		bundle = req.App.Bundle
	}
	p.Bus.Publish(eventbus.Event{Type: eventbus.EvtAdRequest, Data: map[string]interface{}{
		"request_id": req.ID, "bundle": bundle,
	}})

	p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
		Type: "ortb_request", RequestID: req.ID, Env: detectEnv(req),
		Details: fmt.Sprintf("bundle=%s tag=%s", bundle, req.Imp[0].TagID),
	})

	// ── Stage 2: Dynamic floor optimization ──
	// GAM and Magnite both apply dynamic floors per-request based on
	// geo, device type, time of day, and historical data.
	effectiveFloor := p.FloorEngine.Calculate(req)
	if effectiveFloor > req.Imp[0].BidFloor {
		req.Imp[0].BidFloor = effectiveFloor
	}
	p.Bus.Publish(eventbus.Event{Type: eventbus.EvtFloorApplied, Data: map[string]interface{}{
		"request_id": req.ID, "floor": req.Imp[0].BidFloor,
	}})

	// ── Stage 3: Fan-out bid requests ──
	// Send bid requests to all eligible demand adapters in parallel.
	// Each adapter has its own timeout and QPS limit (GAM traffic shaping).
	defaultTMaxMs := p.DefaultTMax
	if defaultTMaxMs <= 0 {
		defaultTMaxMs = 500
	}
	tmax := time.Duration(defaultTMaxMs) * time.Millisecond
	if req.TMax > 0 {
		tmax = time.Duration(req.TMax) * time.Millisecond
	}

	bidStart := time.Now()
	var bidResults []*adapter.BidResult
	if len(adapterIDs) > 0 && adapterIDs[0] != nil {
		// Mapped mode: only send to selected demand sources
		bidResults = p.Registry.FanOutTo(ctx, req, tmax, adapterIDs[0])
	} else {
		// Unmapped mode: fan out to all active adapters
		bidResults = p.Registry.FanOut(ctx, req, tmax)
	}
	result.BidLatency = time.Since(bidStart)
	p.Metrics.RecordBidLatency(float64(result.BidLatency.Milliseconds()))

	// ── Stage 4: Collect and flatten bids ──
	var collectedBids []openrtb.Bid
	hadAdapterErrors := false
	for _, br := range bidResults {
		if br.Error != nil {
			hadAdapterErrors = true
			p.Bus.Publish(eventbus.Event{Type: eventbus.EvtError, Data: map[string]interface{}{
				"request_id": req.ID, "adapter": br.AdapterID, "error": br.Error.Error(),
			}})
			continue
		}
		if br.NoBid {
			p.Bus.Publish(eventbus.Event{Type: eventbus.EvtNoBid, Data: map[string]interface{}{
				"request_id": req.ID, "adapter": br.AdapterID,
			}})
			continue
		}
		for _, bid := range br.Bids {
			p.Bus.Publish(eventbus.Event{Type: eventbus.EvtBidResponse, Data: map[string]interface{}{
				"request_id": req.ID, "adapter": br.AdapterID,
				"bid_price": bid.Price, "bid_id": bid.ID, "latency_ms": br.Latency.Milliseconds(),
			}})
		}
		collectedBids = append(collectedBids, br.Bids...)
	}

	if len(collectedBids) == 0 {
		if hadAdapterErrors {
			p.Metrics.RecordError()
			p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
				Type: "adapter_error", RequestID: req.ID, Env: detectEnv(req),
				Details: "all eligible demand adapters failed",
			})
		}
		p.Metrics.RecordNoBid()
		p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type: "no_bid", RequestID: req.ID, Env: detectEnv(req),
			Details: "no bids received from demand adapters",
		})
		result.NoBid = true
		result.VAST = vast.BuildNoAd()
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 5: Ad quality / brand safety scan ──
	collectedBids = p.AQScanner.Filter(collectedBids, req)
	if len(collectedBids) == 0 {
		p.Metrics.RecordNoBid()
		result.NoBid = true
		result.VAST = vast.BuildNoAd()
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 6: Open-market auction ──
	auctionType := p.AuctionType
	auctionResult := auction.Run(collectedBids, req.Imp[0].BidFloor, auctionType)

	p.Bus.PublishSync(eventbus.Event{Type: eventbus.EvtAuctionEnd, Data: map[string]interface{}{
		"request_id": req.ID, "winner_id": safeWinnerID(auctionResult),
		"win_price": auctionResult.WinPrice, "auction_type": auctionType,
		"bid_count": len(collectedBids),
	}})

	if auctionResult.Winner == nil {
		p.Metrics.RecordNoBid()
		result.NoBid = true
		result.VAST = vast.BuildNoAd()
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 7: Win/Loss notification ──
	winner := auctionResult.Winner
	auction.FireWinNotice(winner)
	for i := range auctionResult.Losers {
		auction.FireLossNotice(&auctionResult.Losers[i])
		p.Metrics.RecordLoss()
	}

	// ── Stage 8: Build VAST response ──
	xml := vast.Build(winner, req, result.BaseURL)
	if xml == "" {
		p.Metrics.RecordError()
		result.Error = fmt.Errorf("VAST build failed for bid %s", winner.ID)
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 9: Billing & metrics (impression counted on client-side pixel fire) ──
	p.Metrics.RecordWin(auctionResult.WinPrice)
	p.Metrics.RecordSpend(winner.ReportingPrice(auctionResult.WinPrice))
	p.Metrics.RecordVastStart()

	p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
		Type: "ortb_response", RequestID: req.ID, Env: detectEnv(req),
		Details: fmt.Sprintf("winner=%s price=%.2f clear=%.2f type=%s nurl=%v burl=%v",
			winner.ID, winner.Price, auctionResult.WinPrice, auctionType,
			winner.NURL != "", winner.BURL != ""),
	})

	result.Winner = winner
	result.WinPrice = auctionResult.WinPrice
	result.Losers = auctionResult.Losers
	result.VAST = xml
	result.AuctionType = auctionType
	result.TotalLatency = time.Since(start)
	return result
}

func detectEnv(req *openrtb.BidRequest) string {
	switch req.Device.DeviceType {
	case 3:
		return "CTV"
	case 7:
		return "STB"
	case 4:
		return "Mobile"
	default:
		return "CTV"
	}
}

func safeWinnerID(r *auction.AuctionResult) string {
	if r.Winner != nil {
		return r.Winner.ID
	}
	return ""
}
