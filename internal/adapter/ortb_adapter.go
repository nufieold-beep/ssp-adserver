package adapter

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"strings"
	"time"

	openrtb2 "github.com/prebid/openrtb/v20/openrtb2"
)

// ORTBAdapter implements DemandAdapter for OpenRTB 2.5/2.6 DSP endpoints.
// This is how Magnite and Prebid Server connect to programmatic DSPs:
// standard JSON POST with the BidRequest, parse BidResponse.
type ORTBAdapter struct {
	id            string
	name          string
	endpoint      string
	ortbVersion   string
	client        *http.Client
	floor         float64
	margin        float64
	gzipSupport   bool
	removePChain  bool
	schainEnabled bool
	badv          []string
	bcat          []string
}

func NewORTBAdapter(cfg *AdapterConfig) *ORTBAdapter {
	t := resolveTimeout(cfg.TimeoutMs)
	return &ORTBAdapter{
		id: cfg.ID, name: cfg.Name, endpoint: cfg.Endpoint,
		ortbVersion: normalizeORTBVersion(cfg.ORTBVersion),
		floor:       cfg.Floor, margin: normalizeMargin(cfg.Margin),
		gzipSupport:   cfg.GZIPSupport,
		removePChain:  cfg.RemovePChain,
		schainEnabled: cfg.SChainEnabled,
		badv:          sanitizeStringList(cfg.BAdv),
		bcat:          sanitizeStringList(cfg.BCat),
		client:        httputil.NewClient(t),
	}
}

func (a *ORTBAdapter) ID() string                          { return a.id }
func (a *ORTBAdapter) Name() string                        { return a.name }
func (a *ORTBAdapter) Type() AdapterType                   { return TypeORTB }
func (a *ORTBAdapter) Supports(_ *openrtb.BidRequest) bool { return true }

func (a *ORTBAdapter) RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error) {
	// Clone and apply per-endpoint ORTB fields
	outReq := a.applyEndpointConfig(ctx, req)

	buf := httputil.GetBuffer()
	defer httputil.PutBuffer(buf)

	if a.gzipSupport {
		// GZIP compress the request body
		gz := gzip.NewWriter(buf)
		if err := json.NewEncoder(gz).Encode(outReq); err != nil {
			return nil, err
		}
		gz.Close()
	} else {
		if err := json.NewEncoder(buf).Encode(outReq); err != nil {
			return nil, err
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, buf)
	if err != nil {
		return nil, err
	}
	ua := ""
	ip := ""
	if outReq.Device != nil {
		ua = outReq.Device.UA
		ip = outReq.Device.IP
	}
	httputil.SetORTBHeaders(httpReq, outReq.ID, ua, ip, a.ortbVersion)
	if a.gzipSupport {
		httpReq.Header.Set("Content-Encoding", "gzip")
		httpReq.Header.Set("Accept-Encoding", "gzip")
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return &BidResult{AdapterID: a.id, NoBid: true, NoBidReason: "http_204_no_content"}, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := httputil.ReadResponseBody(resp)
		msg := strings.TrimSpace(string(body))
		if len(msg) > 240 {
			msg = msg[:240]
		}
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return nil, fmt.Errorf("ortb adapter %s returned HTTP %d: %s", a.id, resp.StatusCode, msg)
	}

	reader, closeFn, gzErr := httputil.ResponseBodyReader(resp)
	if gzErr != nil {
		return nil, gzErr
	}
	defer closeFn()

	var prebidResp openrtb2.BidResponse
	if err := json.NewDecoder(reader).Decode(&prebidResp); err != nil {
		return nil, err
	}

	validatedBids := openrtb.ValidateBidResponse(&prebidResp, outReq)
	if len(validatedBids) == 0 {
		return &BidResult{AdapterID: a.id, NoBid: true, NoBidReason: "no_valid_bids_after_validation"}, nil
	}

	// Margin is metadata for internal billing/reporting only.
	// Keep bid.Price gross so floor checks and auction ranking are unaffected.
	if a.margin > 0 {
		for i := range validatedBids {
			validatedBids[i].Margin = a.margin
		}
	}

	return &BidResult{AdapterID: a.id, Bids: validatedBids}, nil
}

// applyEndpointConfig merges per-endpoint ORTB settings into a copy of the bid request.
func (a *ORTBAdapter) applyEndpointConfig(ctx context.Context, req *openrtb.BidRequest) *openrtb.BidRequest {
	// Shallow copy the request
	clonedReq := *req

	if req.App != nil {
		appCopy := *req.App
		appCopy.StoreURL = openrtb.DecodeStoreURLValue(req.App.StoreURL)
		clonedReq.App = &appCopy
	}

	if len(req.Imp) > 0 {
		clonedReq.Imp = make([]openrtb.Imp, len(req.Imp))
		copy(clonedReq.Imp, req.Imp)

		for i := range clonedReq.Imp {
			floor := clonedReq.Imp[i].BidFloor
			if a.floor > floor {
				floor = a.floor
			}
			// Maintain supply net-floor when margin is configured.
			if a.margin > 0 && floor > 0 {
				floor = floor / (1 - a.margin)
			}
			clonedReq.Imp[i].BidFloor = floor

			if a.ortbVersion == "2.5" && clonedReq.Imp[i].Video != nil {
				videoCopy := *clonedReq.Imp[i].Video
				videoCopy.Plcmt = 0
				clonedReq.Imp[i].Video = &videoCopy
			}
		}
	}
	clonedReq.TMax = normalizeOutboundTMax(ctx, clonedReq.TMax, a.client.Timeout)

	// Merge BAdv: combine request-level + endpoint-level blocked advertisers
	if len(req.BAdv) > 0 || len(a.badv) > 0 {
		clonedReq.BAdv = mergeSanitizedLists(req.BAdv, a.badv)
	}

	// Merge BCat: combine request-level + endpoint-level blocked categories
	if len(req.BCat) > 0 || len(a.bcat) > 0 {
		clonedReq.BCat = mergeSanitizedLists(req.BCat, a.bcat)
	}

	clonedReq.Source = applySupplyChainPolicy(clonedReq.Source, a.schainEnabled, a.removePChain)

	if a.ortbVersion == "2.5" {
		downgradeRequestToORTB25(&clonedReq)
	}

	return &clonedReq
}

func downgradeRequestToORTB25(req *openrtb.BidRequest) {
	if req == nil {
		return
	}

	if req.Device != nil && req.Device.SUA != nil {
		deviceCopy := *req.Device
		deviceCopy.SUA = nil
		req.Device = &deviceCopy
	}

	if req.Regs != nil && len(req.Regs.GPPSID) > 0 {
		regsCopy := *req.Regs
		regsCopy.GPPSID = nil
		req.Regs = &regsCopy
	}
}

func sanitizeStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		v := normalizeListToken(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func mergeSanitizedLists(requestValues, staticValues []string) []string {
	if len(requestValues) == 0 {
		return staticValues
	}

	out := make([]string, 0, len(requestValues)+len(staticValues))
	seen := make(map[string]struct{}, len(requestValues)+len(staticValues))

	for _, raw := range requestValues {
		v := normalizeListToken(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	for _, v := range staticValues {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeOutboundTMax(ctx context.Context, current int64, clientTimeout time.Duration) int64 {
	budgets := make([]int64, 0, 3)
	if current > 0 {
		budgets = append(budgets, current)
	}
	if clientBudget := bufferedBudgetMs(clientTimeout, 50); clientBudget > 0 {
		budgets = append(budgets, clientBudget)
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining > 0 {
				if remainingMs := int64(remaining / time.Millisecond); remainingMs > 0 {
					budgets = append(budgets, remainingMs)
				}
			}
		}
	}
	if len(budgets) == 0 {
		return current
	}
	best := budgets[0]
	for _, budget := range budgets[1:] {
		if budget < best {
			best = budget
		}
	}
	return best
}

func bufferedBudgetMs(timeout time.Duration, safetyMarginMs int64) int64 {
	budgetMs := int64(timeout / time.Millisecond)
	if budgetMs <= 0 {
		return 0
	}
	if safetyMarginMs > 0 && budgetMs > safetyMarginMs {
		budgetMs -= safetyMarginMs
	}
	return budgetMs
}

func applySupplyChainPolicy(source *openrtb.Source, schainEnabled, removePChain bool) *openrtb.Source {
	if source == nil {
		return nil
	}
	sourceCopy := *source
	if source.SChain != nil {
		schainCopy := *source.SChain
		if len(source.SChain.Nodes) > 0 {
			schainCopy.Nodes = append([]openrtb.SChainNode(nil), source.SChain.Nodes...)
		}
		sourceCopy.SChain = &schainCopy
	}
	if !schainEnabled {
		sourceCopy.SChain = nil
	}
	if removePChain {
		sourceCopy.PChain = ""
	}
	if sourceCopy.SChain == nil && strings.TrimSpace(sourceCopy.PChain) == "" && strings.TrimSpace(sourceCopy.TID) == "" {
		return nil
	}
	return &sourceCopy
}

func normalizeListToken(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ReplaceAll(v, `\\"`, `"`)
	v = strings.ReplaceAll(v, `\"`, `"`)
	v = strings.ReplaceAll(v, `\'`, `'`)
	for {
		prev := v
		v = strings.TrimSpace(v)
		v = strings.TrimPrefix(v, `\"`)
		v = strings.TrimSuffix(v, `\"`)
		v = strings.TrimPrefix(v, `"`)
		v = strings.TrimSuffix(v, `"`)
		v = strings.Trim(v, `"'`)
		if v == prev {
			break
		}
	}
	v = strings.TrimSpace(v)
	return v
}
