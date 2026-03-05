package adapter

import (
	"context"
	"ssp/internal/openrtb"
	"time"
)

// LegacyBidder is the interface that legacy bidders implement.
// This mirrors bidder.Bidder without importing the package (avoid circular deps).
type LegacyBidder interface {
	Name() string
	BidderType() string
	Request(req openrtb.BidRequest) ([]openrtb.Bid, error)
}

// LegacyAdapter wraps a legacy Bidder as a DemandAdapter so both legacy
// and enterprise demand sources compete in a single parallel FanOut.
// This is the standard adapter pattern used by Prebid Server to unify
// different demand partner integrations into one auction.
type LegacyAdapter struct {
	bidder LegacyBidder
	id     string
	atype  AdapterType
}

// NewLegacyAdapter wraps a legacy bidder as a DemandAdapter.
func NewLegacyAdapter(b LegacyBidder) *LegacyAdapter {
	at := TypeORTB
	if b.BidderType() == "vast" {
		at = TypeVAST
	}
	return &LegacyAdapter{
		bidder: b,
		id:     "legacy-" + b.Name(),
		atype:  at,
	}
}

func (a *LegacyAdapter) ID() string        { return a.id }
func (a *LegacyAdapter) Name() string       { return a.bidder.Name() }
func (a *LegacyAdapter) Type() AdapterType  { return a.atype }
func (a *LegacyAdapter) Supports(_ *openrtb.BidRequest) bool { return true }

func (a *LegacyAdapter) RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error) {
	type result struct {
		bids []openrtb.Bid
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		bids, err := a.bidder.Request(*req)
		select {
		case ch <- result{bids, err}:
		case <-ctx.Done():
			// Context cancelled — exit cleanly without blocking
		}
	}()

	start := time.Now()
	select {
	case <-ctx.Done():
		return &BidResult{AdapterID: a.id, Error: ctx.Err(), Latency: time.Since(start)}, nil
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		if len(r.bids) == 0 {
			return &BidResult{AdapterID: a.id, NoBid: true, Latency: time.Since(start)}, nil
		}
		return &BidResult{AdapterID: a.id, Bids: r.bids, Latency: time.Since(start)}, nil
	}
}
