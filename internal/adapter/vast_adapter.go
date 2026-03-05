package adapter

import (
	"context"
	"html"
	"net/http"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"time"
)

// VASTAdapter implements DemandAdapter for VAST tag demand sources.
// This handles ad networks that return VAST XML directly (no OpenRTB).
// Magnite and FreeWheel both support VAST tags as a demand type alongside
// programmatic ORTB, unified through a single adapter interface.
type VASTAdapter struct {
	id     string
	name   string
	tag    string
	client *http.Client
	cpm    float64
	margin float64
}

func NewVASTAdapter(cfg *AdapterConfig) *VASTAdapter {
	t := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if t == 0 {
		t = 200 * time.Millisecond
	}
	if cfg.Floor == 0 {
		cfg.Floor = 1.0
	}
	return &VASTAdapter{
		id: cfg.ID, name: cfg.Name, tag: cfg.Endpoint,
		cpm: cfg.Floor, margin: cfg.Margin,
		client: httputil.NewClient(t),
	}
}

func (a *VASTAdapter) ID() string                          { return a.id }
func (a *VASTAdapter) Name() string                        { return a.name }
func (a *VASTAdapter) Type() AdapterType                   { return TypeVAST }
func (a *VASTAdapter) Supports(_ *openrtb.BidRequest) bool { return true }

func (a *VASTAdapter) RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, a.tag, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := httputil.ReadResponseBody(resp)
	if err != nil {
		return nil, err
	}

	adm := html.UnescapeString(string(body))
	if adm == "" {
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
	}

	price := a.cpm
	if a.margin > 0 {
		price *= (1 - a.margin)
	}

	bid := openrtb.Bid{
		ID:    "vast-" + a.id,
		ImpID: "1",
		Price: price,
		Adm:   adm,
	}

	return &BidResult{AdapterID: a.id, Bids: []openrtb.Bid{bid}}, nil
}
