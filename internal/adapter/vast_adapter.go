package adapter

import (
	"context"
	"fmt"
	"net/http"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"ssp/internal/vast"
	"strings"
)

// VASTAdapter implements DemandAdapter for VAST tag demand sources.
// This handles ad networks that return VAST XML directly (no OpenRTB).
// Magnite and FreeWheel both support VAST tags as a demand type alongside
// programmatic ORTB, unified through a single adapter interface.
type VASTAdapter struct {
	id          string
	name        string
	tag         string
	client      *http.Client
	cpm         float64
	margin      float64
	gzipSupport bool
}

func NewVASTAdapter(cfg *AdapterConfig) *VASTAdapter {
	t := resolveTimeout(cfg.TimeoutMs)
	if cfg.Floor == 0 {
		cfg.Floor = 1.0
	}
	return &VASTAdapter{
		id: cfg.ID, name: cfg.Name, tag: cfg.Endpoint,
		cpm: cfg.Floor, margin: normalizeMargin(cfg.Margin),
		gzipSupport: cfg.GZIPSupport,
		client:      httputil.NewClient(t),
	}
}

func (a *VASTAdapter) ID() string                          { return a.id }
func (a *VASTAdapter) Name() string                        { return a.name }
func (a *VASTAdapter) Type() AdapterType                   { return TypeVAST }
func (a *VASTAdapter) Supports(_ *openrtb.BidRequest) bool { return true }

func (a *VASTAdapter) RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error) {
	tagURL := vast.EnrichTagURL(a.tag, req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, tagURL, nil)
	if err != nil {
		return nil, err
	}
	if a.gzipSupport {
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
		return nil, fmt.Errorf("vast adapter %s returned HTTP %d: %s", a.id, resp.StatusCode, msg)
	}

	body, err := httputil.ReadResponseBody(resp)
	if err != nil {
		return nil, err
	}

	adm := strings.TrimSpace(string(body))
	if adm == "" {
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
	}

	lowAdm := strings.ToLower(adm)
	if !strings.Contains(lowAdm, "<vast") && !strings.Contains(lowAdm, "<vmap") {
		return nil, fmt.Errorf("vast adapter %s returned non-VAST payload", a.id)
	}

	price := a.cpm

	// Match ImpID to actual request imp for bid validation
	impID := "1"
	if len(req.Imp) > 0 {
		impID = req.Imp[0].ID
	}

	bid := openrtb.Bid{
		ID:     "vast-" + a.id,
		ImpID:  impID,
		Price:  price,
		Margin: a.margin,
		Adm:    adm,
		MType:  2,
	}

	return &BidResult{AdapterID: a.id, Bids: []openrtb.Bid{bid}}, nil
}
