package bidder

import (
	"ssp/internal/config"
	"ssp/internal/openrtb"
	"sync"
)

type Manager struct {
	bidders []Bidder
}

// NewManagerFromConfig creates a bidder manager from YAML config.
func NewManagerFromConfig(cfg *config.Config) *Manager {
	var bidders []Bidder
	for _, bc := range cfg.Bidders {
		if bc.Status == 0 {
			continue
		}
		switch bc.Type {
		case "ortb":
			bidders = append(bidders, NewORTBBidder(bc.Name, bc.Endpoint, bc.Timeout))
		case "vast":
			bidders = append(bidders, NewVASTBidder(bc.Name, bc.Endpoint, bc.Timeout, bc.Floor))
		}
	}
	return &Manager{bidders: bidders}
}

// NewManager creates a default manager (fallback).
func NewManager() *Manager {
	return &Manager{
		bidders: []Bidder{
			NewORTBBidder("dsp1", "https://example-dsp.com/openrtb", 120),
			NewVASTBidder("network1", "https://ads.network.com/vast", 120, 1.0),
		},
	}
}

func (m *Manager) CallAll(req openrtb.BidRequest) []openrtb.Bid {
	var wg sync.WaitGroup
	bidChan := make(chan openrtb.Bid, 20)

	for _, bidder := range m.bidders {
		wg.Add(1)
		go func(b Bidder) {
			defer wg.Done()
			bids, err := b.Request(req)
			if err != nil {
				return
			}
			for _, bid := range bids {
				bidChan <- bid
			}
		}(bidder)
	}

	wg.Wait()
	close(bidChan)

	var bids []openrtb.Bid
	for b := range bidChan {
		bids = append(bids, b)
	}
	return bids
}

func (m *Manager) Bidders() []Bidder { return m.bidders }
