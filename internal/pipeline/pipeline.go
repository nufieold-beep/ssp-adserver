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

type requestTelemetry struct {
	bundle        string
	env           string
	supply        string
	floorDecision floor.Decision
}

type adapterCollectionSummary struct {
	candidateBids       []openrtb.Bid
	errorCount          int
	timeoutCount        int
	noBidCount          int
	attemptedAdapterIDs []string
	errorAdapterIDs     []string
	timeoutAdapterIDs   []string
	noBidAdapterIDs     []string
	noBidReasons        []string
}

// Execute runs the full ad serving pipeline for a single request.
// baseURL is the publicly-reachable server origin for VAST tracking URLs.
// adapterIDs optionally restricts which demand adapters receive bid requests.
// Pass nil to fan out to all active adapters (no mapping filter).
func (p *Pipeline) Execute(ctx context.Context, req *openrtb.BidRequest, baseURL string, adapterIDs ...[]string) *Result {
	start := time.Now()
	result := &Result{RequestID: req.ID, BaseURL: baseURL}
	execCtx, cancel := p.resolveExecutionContext(ctx, req)
	defer cancel()
	telemetry := requestTelemetry{
		bundle: bundleForRequest(req),
		env:    detectRequestEnvironment(req),
		supply: requestTagID(req),
	}

	hasAdRequestSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtAdRequest)
	hasFloorSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtFloorApplied)
	hasErrorSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtError)
	hasNoBidSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtNoBid)
	hasBidResponseSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtBidResponse)
	hasAuctionEndSubscriber := p.Bus != nil && p.Bus.HasSubscribers(eventbus.EvtAuctionEnd)

	// ── Stage 1: Record request ──
	p.recordRequestStart(req, telemetry, hasAdRequestSubscriber)

	if hasAdRequestSubscriber {
		p.Bus.Publish(eventbus.Event{Type: eventbus.EvtAdRequest, Data: map[string]interface{}{
			"request_id": req.ID, "bundle": telemetry.bundle,
		}})
	}

	// ── Stage 2: Dynamic floor optimization ──
	// GAM and Magnite both apply dynamic floors per-request based on
	// geo, device type, time of day, and historical data.
	telemetry.floorDecision = p.applyDynamicFloor(req, telemetry, hasFloorSubscriber)
	if hasFloorSubscriber {
		p.Bus.Publish(eventbus.Event{Type: eventbus.EvtFloorApplied, Data: map[string]interface{}{
			"request_id": req.ID, "floor": requestFloor(req), "mode": telemetry.floorDecision.Mode,
		}})
	}

	// ── Stage 3: Fan-out bid requests ──
	// Send bid requests to all eligible demand adapters in parallel.
	// Each adapter has its own timeout and QPS limit (GAM traffic shaping).
	tmax := p.resolveBidTimeout(execCtx, req)

	bidStart := time.Now()
	var bidResults []*adapter.BidResult
	if len(adapterIDs) > 0 && adapterIDs[0] != nil {
		// Mapped mode: only send to selected demand sources
		bidResults = p.Registry.FanOutTo(execCtx, req, tmax, adapterIDs[0])
	} else {
		// Unmapped mode: fan out to all active adapters
		bidResults = p.Registry.FanOut(execCtx, req, tmax)
	}
	result.BidLatency = time.Since(bidStart)
	if p.Metrics != nil {
		p.Metrics.RecordBidLatency(float64(result.BidLatency.Milliseconds()))
	}

	// ── Stage 4: Collect and flatten bids ──
	summary := p.collectCandidateBids(req, telemetry, bidResults, hasErrorSubscriber, hasNoBidSubscriber, hasBidResponseSubscriber)

	if len(summary.candidateBids) == 0 {
		reasonCode := buildNoBidReasonCode(len(bidResults), summary.errorCount, summary.timeoutCount, summary.noBidCount)
		reasonDetails := buildNoBidReasonDetails(len(bidResults), summary.errorCount, summary.timeoutCount, summary.noBidCount, summary.attemptedAdapterIDs, summary.errorAdapterIDs, summary.timeoutAdapterIDs, summary.noBidAdapterIDs, summary.noBidReasons)
		return p.finishNoBidResult(result, start, req, telemetry, reasonCode, reasonDetails)
	}
	if err := execCtx.Err(); err != nil {
		return p.finishNoBidResult(result, start, req, telemetry, "pipeline_timeout", err.Error())
	}

	// ── Stage 5: Ad quality / brand safety scan ──
	candidateBids := summary.candidateBids
	if p.AQScanner != nil {
		candidateBids = p.AQScanner.Filter(candidateBids, req)
	}
	if len(candidateBids) == 0 {
		return p.finishNoBidResult(result, start, req, telemetry, "filtered_by_ad_quality", "filtered_by_ad_quality")
	}
	if err := execCtx.Err(); err != nil {
		return p.finishNoBidResult(result, start, req, telemetry, "pipeline_timeout", err.Error())
	}

	// ── Stage 6: Open-market auction ──
	auctionType := p.AuctionType
	auctionResult := auction.Run(candidateBids, requestFloor(req), auctionType)

	if hasAuctionEndSubscriber {
		p.Bus.PublishSync(eventbus.Event{Type: eventbus.EvtAuctionEnd, Data: map[string]interface{}{
			"request_id": req.ID, "winner_id": winnerIDOrEmpty(auctionResult),
			"win_price": auctionResult.WinPrice, "auction_type": auctionType,
			"bid_count": len(candidateBids),
		}})
	}

	if auctionResult.Winner == nil {
		return p.finishNoBidResult(result, start, req, telemetry, "no_auction_winner", "no_auction_winner")
	}
	if err := execCtx.Err(); err != nil {
		return p.finishNoBidResult(result, start, req, telemetry, "pipeline_timeout", err.Error())
	}

	// ── Stage 7: Build final VAST before any notices are fired ──
	winner := auctionResult.Winner
	xml := vast.Build(winner, req, result.BaseURL)
	if xml == "" {
		if p.Metrics != nil {
			p.Metrics.RecordError()
		}
		result.Error = fmt.Errorf("vast build failed for bid %s", winner.ID)
		result.TotalLatency = time.Since(start)
		return result
	}
	if p.FloorEngine != nil {
		p.FloorEngine.ObserveWinPrice(auctionResult.WinPrice)
	}

	// ── Stage 8: Final notices happen only after delivery approval ──

	if p.Metrics != nil {
		p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type:      "ortb_response",
			RequestID: req.ID,
			Env:       telemetry.env,
			Details: fmt.Sprintf("winner=%s price=%.2f clear=%.2f type=%s floor=%.2f mode=%s latency_ms=%d nurl=%v burl=%v",
				winner.ID, winner.Price, auctionResult.WinPrice, auctionType,
				telemetry.floorDecision.AppliedFloor, telemetry.floorDecision.Mode, time.Since(start).Milliseconds(),
				winner.NURL != "", winner.BURL != ""),
			Bundle: telemetry.bundle,
			Supply: telemetry.supply,
			Price:  fmt.Sprintf("%.2f", auctionResult.WinPrice),
		})
	}

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

func (p *Pipeline) recordRequestStart(req *openrtb.BidRequest, telemetry requestTelemetry, hasAdRequestSubscriber bool) {
	if p.Metrics == nil {
		return
	}
	p.Metrics.RecordAdRequest()
	p.Metrics.RecordAdOpp()
	p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
		Type:      "ortb_request",
		RequestID: req.ID,
		Env:       telemetry.env,
		Details:   fmt.Sprintf("bundle=%s tag=%s floor=%.2f", telemetry.bundle, telemetry.supply, requestFloor(req)),
		Bundle:    telemetry.bundle,
		Supply:    telemetry.supply,
	})
}

func (p *Pipeline) applyDynamicFloor(req *openrtb.BidRequest, telemetry requestTelemetry, hasFloorSubscriber bool) floor.Decision {
	decision := floor.Decision{BaseFloor: requestFloor(req), AppliedFloor: requestFloor(req), Mode: "none"}
	if decision.AppliedFloor > 0 {
		decision.Mode = "request"
	}
	if p.FloorEngine != nil {
		decision = p.FloorEngine.CalculateDecision(req)
	}
	if imp := primaryImp(req); imp != nil && decision.AppliedFloor > imp.BidFloor {
		imp.BidFloor = decision.AppliedFloor
	}
	if p.Metrics != nil {
		details := fmt.Sprintf("mode=%s base=%.2f applied=%.2f adaptive=%.2f", decision.Mode, decision.BaseFloor, decision.AppliedFloor, decision.AdaptiveFloor)
		if decision.MatchedRuleID != "" {
			details += fmt.Sprintf(" rule=%s", decision.MatchedRuleID)
		}
		p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type:      "floor_decision",
			RequestID: req.ID,
			Env:       telemetry.env,
			Details:   details,
			Bundle:    telemetry.bundle,
			Supply:    telemetry.supply,
			Price:     fmt.Sprintf("%.2f", decision.AppliedFloor),
		})
	}
	return decision
}

func (p *Pipeline) collectCandidateBids(req *openrtb.BidRequest, telemetry requestTelemetry, bidResults []*adapter.BidResult, hasErrorSubscriber, hasNoBidSubscriber, hasBidResponseSubscriber bool) adapterCollectionSummary {
	summary := adapterCollectionSummary{
		candidateBids:       make([]openrtb.Bid, 0, len(bidResults)),
		attemptedAdapterIDs: make([]string, 0, len(bidResults)),
		errorAdapterIDs:     make([]string, 0, len(bidResults)),
		timeoutAdapterIDs:   make([]string, 0, len(bidResults)),
		noBidAdapterIDs:     make([]string, 0, len(bidResults)),
		noBidReasons:        make([]string, 0, len(bidResults)),
	}
	for _, br := range bidResults {
		if adapterID := strings.TrimSpace(br.AdapterID); adapterID != "" {
			summary.attemptedAdapterIDs = append(summary.attemptedAdapterIDs, adapterID)
		}
		if br.Error != nil {
			timedOut := isTimeoutAdapterError(br.Error, br.TimedOut)
			summary.errorCount++
			summary.errorAdapterIDs = append(summary.errorAdapterIDs, br.AdapterID)
			if timedOut {
				summary.timeoutCount++
				summary.timeoutAdapterIDs = append(summary.timeoutAdapterIDs, br.AdapterID)
			}
			if p.Metrics != nil {
				p.Metrics.RecordAdapterErrorReason(classifyAdapterErrorReason(br.AdapterID, br.Error, timedOut))
				p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
					Type:      "adapter_error",
					RequestID: req.ID,
					Env:       telemetry.env,
					Details:   formatAdapterErrorDetail(br.AdapterID, br.Error, timedOut),
					Bundle:    telemetry.bundle,
					Supply:    telemetry.supply,
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
			summary.noBidCount++
			summary.noBidAdapterIDs = append(summary.noBidAdapterIDs, br.AdapterID)
			summary.noBidReasons = append(summary.noBidReasons, formatNoBidAdapterDetail(br.AdapterID, br.NoBidReason))
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
		summary.candidateBids = append(summary.candidateBids, br.Bids...)
	}
	return summary
}

func (p *Pipeline) finishNoBidResult(result *Result, start time.Time, req *openrtb.BidRequest, telemetry requestTelemetry, reasonCode, reasonDetails string) *Result {
	if p.Metrics != nil {
		p.Metrics.RecordNoBidReason(reasonCode)
		p.Metrics.RecordNoBid()
		p.Metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type:      "no_bid",
			RequestID: req.ID,
			Env:       telemetry.env,
			Details:   formatPipelineNoBidDetail(reasonDetails, telemetry.floorDecision, time.Since(start)),
			Bundle:    telemetry.bundle,
			Supply:    telemetry.supply,
			Price:     fmt.Sprintf("%.2f", telemetry.floorDecision.AppliedFloor),
		})
	}
	result.NoBid = true
	result.VAST = vast.BuildNoAdForRequest(req)
	result.TotalLatency = time.Since(start)
	return result
}

func (p *Pipeline) resolveExecutionContext(ctx context.Context, req *openrtb.BidRequest) (context.Context, context.CancelFunc) {
	timeout := p.defaultExecutionTimeout(req)
	if timeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (p *Pipeline) resolveBidTimeout(ctx context.Context, req *openrtb.BidRequest) time.Duration {
	timeout := p.defaultExecutionTimeout(req)
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && (timeout <= 0 || remaining < timeout) {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		return 500 * time.Millisecond
	}
	return timeout
}

func (p *Pipeline) defaultExecutionTimeout(req *openrtb.BidRequest) time.Duration {
	defaultTMaxMs := p.DefaultTMax
	if defaultTMaxMs <= 0 {
		defaultTMaxMs = 500
	}
	if req != nil && req.TMax > 0 {
		return time.Duration(int(req.TMax)) * time.Millisecond
	}
	return time.Duration(defaultTMaxMs) * time.Millisecond
}

func primaryImp(req *openrtb.BidRequest) *openrtb.Imp {
	if req == nil || len(req.Imp) == 0 {
		return nil
	}
	return &req.Imp[0]
}

func requestFloor(req *openrtb.BidRequest) float64 {
	if imp := primaryImp(req); imp != nil {
		return imp.BidFloor
	}
	return 0
}

func requestTagID(req *openrtb.BidRequest) string {
	if imp := primaryImp(req); imp != nil {
		return imp.TagID
	}
	return ""
}

func bundleForRequest(req *openrtb.BidRequest) string {
	if req == nil || req.App == nil {
		return ""
	}
	return openrtb.CleanBundleValue(req.App.Bundle, req.App.ID, req.App.StoreURL)
}

func formatPipelineNoBidDetail(reason string, decision floor.Decision, latency time.Duration) string {
	parts := []string{strings.TrimSpace(reason)}
	parts = append(parts, fmt.Sprintf("base_floor=%.2f", decision.BaseFloor))
	parts = append(parts, fmt.Sprintf("applied_floor=%.2f", decision.AppliedFloor))
	if decision.AdaptiveFloor > 0 {
		parts = append(parts, fmt.Sprintf("adaptive_floor=%.2f", decision.AdaptiveFloor))
	}
	if decision.MatchedRuleID != "" {
		parts = append(parts, fmt.Sprintf("floor_rule=%s", decision.MatchedRuleID))
	}
	if decision.Mode != "" {
		parts = append(parts, fmt.Sprintf("floor_mode=%s", decision.Mode))
	}
	parts = append(parts, fmt.Sprintf("latency_ms=%d", latency.Milliseconds()))
	return strings.Join(parts, " ")
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
	case timeoutCount == totalAdapters:
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
	if isTimeoutAdapterError(err, timedOut) {
		reason = "timeout"
	} else if err != nil {
		msg := strings.ToLower(err.Error())
		switch {
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

func isTimeoutAdapterError(err error, timedOut bool) bool {
	if timedOut {
		return true
	}
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "client.timeout") || strings.Contains(msg, "timeout")
}
