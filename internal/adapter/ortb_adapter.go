package adapter

import (
	"context"
	"encoding/json"
	"net/http"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"time"
)

// ORTBAdapter implements DemandAdapter for OpenRTB 2.5/2.6 DSP endpoints.
// This is how Magnite and Prebid Server connect to programmatic DSPs:
// standard JSON POST with the BidRequest, parse BidResponse.
type ORTBAdapter struct {
	id       string
	name     string
	endpoint string
	client   *http.Client
	floor    float64
	margin   float64
}

func NewORTBAdapter(cfg *AdapterConfig) *ORTBAdapter {
	t := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if t == 0 {
		t = 200 * time.Millisecond
	}
	return &ORTBAdapter{
		id: cfg.ID, name: cfg.Name, endpoint: cfg.Endpoint,
		floor: cfg.Floor, margin: cfg.Margin,
		client: httputil.NewClient(t),
	}
}

func (a *ORTBAdapter) ID() string                          { return a.id }
func (a *ORTBAdapter) Name() string                        { return a.name }
func (a *ORTBAdapter) Type() AdapterType                   { return TypeORTB }
func (a *ORTBAdapter) Supports(_ *openrtb.BidRequest) bool { return true }

func (a *ORTBAdapter) RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error) {
	buf := httputil.GetBuffer()
	defer httputil.PutBuffer(buf)
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, buf)
	if err != nil {
		return nil, err
	}
	httputil.SetORTBHeaders(httpReq, req.ID, req.Device.UA, req.Device.IP)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
	}
	if resp.StatusCode != 200 {
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
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

	valid := bidResp.Validate(req)
	if len(valid) == 0 {
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
	}

	// Apply margin
	if a.margin > 0 {
		for i := range valid {
			valid[i].Price *= (1 - a.margin)
		}
	}

	return &BidResult{AdapterID: a.id, Bids: valid}, nil
}
