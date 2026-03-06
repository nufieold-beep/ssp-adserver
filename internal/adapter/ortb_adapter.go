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
)

// ORTBAdapter implements DemandAdapter for OpenRTB 2.5/2.6 DSP endpoints.
// This is how Magnite and Prebid Server connect to programmatic DSPs:
// standard JSON POST with the BidRequest, parse BidResponse.
type ORTBAdapter struct {
	id            string
	name          string
	endpoint      string
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
		floor: cfg.Floor, margin: cfg.Margin,
		gzipSupport:   cfg.GZIPSupport,
		removePChain:  cfg.RemovePChain,
		schainEnabled: cfg.SChainEnabled,
		badv:          cfg.BAdv,
		bcat:          cfg.BCat,
		client:        httputil.NewClient(t),
	}
}

func (a *ORTBAdapter) ID() string                          { return a.id }
func (a *ORTBAdapter) Name() string                        { return a.name }
func (a *ORTBAdapter) Type() AdapterType                   { return TypeORTB }
func (a *ORTBAdapter) Supports(_ *openrtb.BidRequest) bool { return true }

func (a *ORTBAdapter) RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error) {
	// Clone and apply per-endpoint ORTB fields
	outReq := a.applyEndpointConfig(req)

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
	httputil.SetORTBHeaders(httpReq, outReq.ID, outReq.Device.UA, outReq.Device.IP)
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
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
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

	var bidResp openrtb.BidResponse
	if err := json.NewDecoder(reader).Decode(&bidResp); err != nil {
		return nil, err
	}

	validatedBids := bidResp.Validate(req)
	if len(validatedBids) == 0 {
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
	}

	// Apply margin
	if a.margin > 0 {
		for i := range validatedBids {
			validatedBids[i].Margin = a.margin
			validatedBids[i].Price *= (1 - a.margin)
		}
	}

	return &BidResult{AdapterID: a.id, Bids: validatedBids}, nil
}

// applyEndpointConfig merges per-endpoint ORTB settings into a copy of the bid request.
func (a *ORTBAdapter) applyEndpointConfig(req *openrtb.BidRequest) *openrtb.BidRequest {
	// Shallow copy the request
	clonedReq := *req

	// Merge BAdv: combine request-level + endpoint-level blocked advertisers
	if len(req.BAdv) > 0 || len(a.badv) > 0 {
		merged := make([]string, 0, len(req.BAdv)+len(a.badv))
		merged = append(merged, req.BAdv...)
		merged = append(merged, a.badv...)
		clonedReq.BAdv = sanitizeStringList(merged)
	}

	// Merge BCat: combine request-level + endpoint-level blocked categories
	if len(req.BCat) > 0 || len(a.bcat) > 0 {
		merged := make([]string, 0, len(req.BCat)+len(a.bcat))
		merged = append(merged, req.BCat...)
		merged = append(merged, a.bcat...)
		clonedReq.BCat = sanitizeStringList(merged)
	}

	// Supply chain: remove ext.schain if not enabled for this endpoint
	if !a.schainEnabled && clonedReq.Ext != nil && clonedReq.Ext.SChain != nil {
		clonedReq.Ext = nil
	}

	// Remove PChain: strip schain nodes (pchain removal)
	if a.removePChain && clonedReq.Ext != nil && clonedReq.Ext.SChain != nil {
		clonedReq.Ext = nil
	}

	return &clonedReq
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
