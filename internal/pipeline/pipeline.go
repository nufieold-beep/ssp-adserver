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
	"strings"
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
	Winner               *openrtb.Bid
	WinPrice             float64
	Losers               []openrtb.Bid
	VAST                 string
	RequestID            string
	AdapterID            string
	AuctionType          string
	BidLatency           time.Duration
	TotalLatency         time.Duration
	NoBid                bool
	Error                error
	BaseURL              string // populated by caller for tracking URLs
	NotificationsPending bool
}

// Execute runs the full ad serving pipeline for a single request.
// baseURL is the publicly-reachable server origin for VAST tracking URLs.
// adapterIDs optionally restricts which demand adapters receive bid requests.
// Pass nil to fan out to all active adapters (no mapping filter).
func (p *Pipeline) Execute(ctx context.Context, req *openrtb.BidRequest, baseURL string, adapterIDs ...[]string) *Result {
	start := time.Now()
	result := &Result{RequestID: req.ID, BaseURL: baseURL}

	hasAdRequestSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtAdRequest)
	hasFloorSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtFloorApplied)
	hasErrorSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtError)
	hasNoBidSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtNoBid)
	hasBidResponseSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtBidResponse)
	hasAuctionEndSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtAuctionEnd)

	// ── Stage 1: Record request ──
	p.Metrics.RecordAdRequest()
	p.Metrics.RecordAdOpp()

	bundle := ""
	if req.App != nil {
		bundle = openrtb.CleanBundleValue(req.App.Bundle, req.App.ID, req.App.StoreURL)
	}
	if hasAdRequestSubscriber {
		p.Bus.Publish(eventbus.Event{Type: eventbus.EvtAdRequest, Data: map[string]interface{}{
			"request_id": req.ID, "bundle": bundle,
		}})
	}

	p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
		Type: "ortb_request", RequestID: req.ID, Env: detectRequestEnvironment(req),
		Details: fmt.Sprintf("bundle=%s tag=%s floor=%.2f", bundle, req.Imp[0].TagID, req.Imp[0].BidFloor),
		Bundle:  bundle,
		Supply:  req.Imp[0].TagID,
	})

	// ── Stage 2: Dynamic floor optimization ──
	// GAM and Magnite both apply dynamic floors per-request based on
	// geo, device type, time of day, and historical data.
	effectiveFloor := p.FloorEngine.Calculate(req)
	if effectiveFloor > req.Imp[0].BidFloor {
		req.Imp[0].BidFloor = effectiveFloor
	}
	if hasFloorSubscriber {
		p.Bus.Publish(eventbus.Event{Type: eventbus.EvtFloorApplied, Data: map[string]interface{}{
			"request_id": req.ID, "floor": req.Imp[0].BidFloor,
		}})
	}

	// ── Stage 3: Fan-out bid requests ──
	// Send bid requests to all eligible demand adapters in parallel.
	// Each adapter has its own timeout and QPS limit (GAM traffic shaping).
	defaultTMaxMs := p.DefaultTMax
	if defaultTMaxMs <= 0 {
		defaultTMaxMs = 500
	}
	tmax := time.Duration(defaultTMaxMs) * time.Millisecond
	if req.TMax > 0 {
		tmax = time.Duration(int(req.TMax)) * time.Millisecond
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
	var candidateBids []openrtb.Bid
	adapterErrorCount := 0
	adapterTimeoutCount := 0
	adapterNoBidCount := 0
	attemptedAdapterIDs := make([]string, 0, len(bidResults))
	errorAdapterIDs := make([]string, 0, len(bidResults))
	timeoutAdapterIDs := make([]string, 0, len(bidResults))
	noBidAdapterIDs := make([]string, 0, len(bidResults))
	noBidAdapterReasons := make([]string, 0, len(bidResults))
	for _, br := range bidResults {
		if adapterID := strings.TrimSpace(br.AdapterID); adapterID != "" {
			attemptedAdapterIDs = append(attemptedAdapterIDs, adapterID)
		}
		if br.Error != nil {
			adapterErrorCount++
			errorAdapterIDs = append(errorAdapterIDs, br.AdapterID)
			if br.TimedOut {
				adapterTimeoutCount++
				timeoutAdapterIDs = append(timeoutAdapterIDs, br.AdapterID)
			}
			if p.Metrics != nil {
				p.Metrics.RecordAdapterErrorReason(classifyAdapterErrorReason(br.AdapterID, br.Error, br.TimedOut))
				p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
					Type:      "adapter_error",
					RequestID: req.ID,
					Env:       detectRequestEnvironment(req),
					Details:   formatAdapterErrorDetail(br.AdapterID, br.Error, br.TimedOut),
					Bundle:    bundle,
					Supply:    req.Imp[0].TagID,
				})
			}
			if hasErrorSubscriber {
				p.Bus.Publish(eventbus.Event{Type: eventbus.EvtError, Data: map[string]interface{}{
					"request_id": req.ID, "adapter": br.AdapterID, "error": br.Error.Error(),
				}})
			}
			continue
		}
		if br.NoBid {
			adapterNoBidCount++
			noBidAdapterIDs = append(noBidAdapterIDs, br.AdapterID)
			noBidAdapterReasons = append(noBidAdapterReasons, formatNoBidAdapterDetail(br.AdapterID, br.NoBidReason))
			if hasNoBidSubscriber {
				p.Bus.Publish(eventbus.Event{Type: eventbus.EvtNoBid, Data: map[string]interface{}{
					"request_id": req.ID, "adapter": br.AdapterID,
				}})
			}
			continue
		}
		if hasBidResponseSubscriber {
			for _, bid := range br.Bids {
				p.Bus.Publish(eventbus.Event{Type: eventbus.EvtBidResponse, Data: map[string]interface{}{
					"request_id": req.ID, "adapter": br.AdapterID,
					"bid_price": bid.Price, "bid_id": bid.ID, "latency_ms": br.Latency.Milliseconds(),
				}})
			}
		}
		for i := range br.Bids {
			br.Bids[i].DemandSrc = br.AdapterID
		}
		candidateBids = append(candidateBids, br.Bids...)
	}

	if len(candidateBids) == 0 {
		reasonDetails := buildNoBidReasonDetails(len(bidResults), adapterErrorCount, adapterTimeoutCount, adapterNoBidCount, attemptedAdapterIDs, errorAdapterIDs, timeoutAdapterIDs, noBidAdapterIDs, noBidAdapterReasons)
		if p.Metrics != nil {
			p.Metrics.RecordNoBidReason(buildNoBidReasonCode(len(bidResults), adapterErrorCount, adapterTimeoutCount, adapterNoBidCount))
		}
		p.Metrics.RecordNoBid()
		p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type: "no_bid", RequestID: req.ID, Env: detectRequestEnvironment(req),
			Details: fmt.Sprintf("%s floor=%.2f", reasonDetails, req.Imp[0].BidFloor),
			Bundle:  bundle,
			Supply:  req.Imp[0].TagID,
		})
		result.NoBid = true
		result.VAST = vast.BuildNoAd()
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 5: Ad quality / brand safety scan ──
	candidateBids = p.AQScanner.Filter(candidateBids, req)
	if len(candidateBids) == 0 {
		if p.Metrics != nil {
			p.Metrics.RecordNoBidReason("filtered_by_ad_quality")
		}
		p.Metrics.RecordNoBid()
		p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type:      "no_bid",
			RequestID: req.ID,
			Env:       detectRequestEnvironment(req),
			Details:   fmt.Sprintf("filtered_by_ad_quality floor=%.2f", req.Imp[0].BidFloor),
			Bundle:    bundle,
			Supply:    req.Imp[0].TagID,
		})
		result.NoBid = true
		result.VAST = vast.BuildNoAd()
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 6: Open-market auction ──
	auctionType := p.AuctionType
	auctionResult := auction.Run(candidateBids, req.Imp[0].BidFloor, auctionType)

	if hasAuctionEndSubscriber {
		p.Bus.PublishSync(eventbus.Event{Type: eventbus.EvtAuctionEnd, Data: map[string]interface{}{
			"request_id": req.ID, "winner_id": winnerIDOrEmpty(auctionResult),
			"win_price": auctionResult.WinPrice, "auction_type": auctionType,
			"bid_count": len(candidateBids),
		}})
	}

	if auctionResult.Winner == nil {
		if p.Metrics != nil {
			p.Metrics.RecordNoBidReason("no_auction_winner")
		}
		p.Metrics.RecordNoBid()
		p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type:      "no_bid",
			RequestID: req.ID,
			Env:       detectRequestEnvironment(req),
			Details:   fmt.Sprintf("no_auction_winner floor=%.2f", req.Imp[0].BidFloor),
			Bundle:    bundle,
			Supply:    req.Imp[0].TagID,
		})
		result.NoBid = true
		result.VAST = vast.BuildNoAd()
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 7: Build final VAST before any notices are fired ──
	winner := auctionResult.Winner
	xml := vast.Build(winner, req, result.BaseURL)
	if xml == "" {
		p.Metrics.RecordError()
		result.Error = fmt.Errorf("vast build failed for bid %s", winner.ID)
		result.TotalLatency = time.Since(start)
		return result
	}

	// ── Stage 8: Final notices happen only after delivery approval ──

	p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
		Type: "ortb_response", RequestID: req.ID, Env: detectRequestEnvironment(req),
		Details: fmt.Sprintf("winner=%s price=%.2f clear=%.2f type=%s nurl=%v burl=%v",
			winner.ID, winner.Price, auctionResult.WinPrice, auctionType,
			winner.NURL != "", winner.BURL != ""),
	})

	result.Winner = winner
	result.WinPrice = auctionResult.WinPrice
	result.Losers = auctionResult.Losers
	result.VAST = xml
	result.AuctionType = auctionType
	result.AdapterID = winner.DemandSrc
	if result.AdapterID == "" {
		result.AdapterID = winner.Seat
	}
	result.NotificationsPending = true
	result.TotalLatency = time.Since(start)
	return result
}

func (p *Pipeline) FinalizeDelivery(result *Result) {
	if p == nil || result == nil || !result.NotificationsPending || result.Winner == nil {
		return
	}

	auction.FireWinNotice(result.Winner)
	auction.RegisterBillableNotice(result.Winner)

	for i := range result.Losers {
		auction.FireLossNotice(&result.Losers[i])
		if p.Metrics != nil {
			p.Metrics.RecordLoss()
		}
	}

	result.NotificationsPending = false
}

func detectRequestEnvironment(req *openrtb.BidRequest) string {
	if req == nil || req.Device == nil {
		return "CTV"
	}
	switch int(req.Device.DeviceType) {
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

func winnerIDOrEmpty(r *auction.AuctionResult) string {
	if r.Winner != nil {
		return r.Winner.ID
	}
	return ""
}

func buildNoBidReasonDetails(totalAdapters, errorCount, timeoutCount, noBidCount int, attemptedAdapterIDs, errorAdapterIDs, timeoutAdapterIDs, noBidAdapterIDs, noBidAdapterReasons []string) string {
	if totalAdapters == 0 {
		return "no eligible demand adapters (check mapping/status/targeting/qps)"
	}
	parts := []string{fmt.Sprintf("no winning bids: adapters=%d errors=%d timeouts=%d explicit_no_bids=%d",
		totalAdapters, errorCount, timeoutCount, noBidCount)}
	if selected := summarizeAdapterIDs(attemptedAdapterIDs); selected != "" {
		parts = append(parts, fmt.Sprintf("selected_adapters=%s", selected))
	}
	if noBidAdapters := summarizeAdapterIDs(noBidAdapterIDs); noBidAdapters != "" {
		parts = append(parts, fmt.Sprintf("no_bid_adapters=%s", noBidAdapters))
	}
	if noBidReasons := summarizeAdapterIDs(noBidAdapterReasons); noBidReasons != "" {
		parts = append(parts, fmt.Sprintf("no_bid_reasons=%s", noBidReasons))
	}
	if timeoutAdapters := summarizeAdapterIDs(timeoutAdapterIDs); timeoutAdapters != "" {
		parts = append(parts, fmt.Sprintf("timeout_adapters=%s", timeoutAdapters))
	}
	if errorAdapters := summarizeAdapterIDs(errorAdapterIDs); errorAdapters != "" {
		parts = append(parts, fmt.Sprintf("error_adapters=%s", errorAdapters))
	}
	return strings.Join(parts, " ")
}

func formatNoBidAdapterDetail(adapterID, reason string) string {
	adapterID = strings.TrimSpace(adapterID)
	reason = strings.TrimSpace(reason)
	if adapterID == "" {
		return reason
	}
	if reason == "" {
		return adapterID
	}
	return fmt.Sprintf("%s(%s)", adapterID, reason)
}

func summarizeAdapterIDs(ids []string) string {
	if len(ids) == 0 {
		return ""
	}

	const maxList = 4
	seen := make(map[string]struct{}, len(ids))
	ordered := make([]string, 0, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ordered = append(ordered, id)
	}
	if len(ordered) == 0 {
		return ""
	}
	if len(ordered) <= maxList {
		return strings.Join(ordered, ",")
	}
	return strings.Join(ordered[:maxList], ",") + fmt.Sprintf(",+%d_more", len(ordered)-maxList)
}

func buildNoBidReasonCode(totalAdapters, errorCount, timeoutCount, noBidCount int) string {
	switch {
	case totalAdapters == 0:
		return "no_eligible_adapters"
	case errorCount == totalAdapters && timeoutCount == totalAdapters:
		return "all_adapters_timed_out"
	case errorCount == totalAdapters:
		return "all_adapters_errored"
	case noBidCount == totalAdapters:
		return "all_adapters_no_bid"
	default:
		return "mixed_adapter_outcomes_no_winner"
	}
}

func formatAdapterErrorDetail(adapterID string, err error, timedOut bool) string {
	parts := []string{fmt.Sprintf("adapter=%s", adapterID)}
	if timedOut {
		parts = append(parts, "timeout=true")
	}
	if err != nil {
		parts = append(parts, fmt.Sprintf("error=%s", strings.TrimSpace(err.Error())))
	}
	return strings.Join(parts, " ")
}

func classifyAdapterErrorReason(adapterID string, err error, timedOut bool) string {
	reason := "unknown"
	if timedOut {
		reason = "timeout"
	} else if err != nil {
		msg := strings.ToLower(err.Error())
		switch {
		case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "client.timeout"), strings.Contains(msg, "timeout"):
			reason = "timeout"
		case strings.Contains(msg, "http 400"):
			reason = "http_400"
		case strings.Contains(msg, "http 401"):
			reason = "http_401"
		case strings.Contains(msg, "http 403"):
			reason = "http_403"
		case strings.Contains(msg, "http 404"):
			reason = "http_404"
		case strings.Contains(msg, "http 429"):
			reason = "http_429"
		case strings.Contains(msg, "http 500"):
			reason = "http_500"
		case strings.Contains(msg, "http 502"):
			reason = "http_502"
		case strings.Contains(msg, "http 503"):
			reason = "http_503"
		case strings.Contains(msg, "http 504"):
			reason = "http_504"
		case strings.Contains(msg, "invalid xml"):
			reason = "invalid_xml"
		case strings.Contains(msg, "non-vast payload"):
			reason = "non_vast_payload"
		case strings.Contains(msg, "invalid character"), strings.Contains(msg, "cannot unmarshal"), strings.Contains(msg, "decode"):
			reason = "decode_error"
		default:
			reason = "request_error"
		}
	}
	if strings.TrimSpace(adapterID) == "" {
		return reason
	}
	return adapterID + ":" + reason
}
