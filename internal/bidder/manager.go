package bidder

import (
	"ssp/internal/config"
	"ssp/internal/openrtb"
	"strings"
	"sync"
)

type ManagedBidder struct {
	Bidder
	Config *config.AdapterConfig
	// Pre-compiled targeting sets for O(1) lookups (lazy init)
	geoSet  map[string]bool
	osSet   map[string]bool
	bcatSet map[string]bool
}

type Manager struct {
	bidders []ManagedBidder
}

// NewManagerFromConfig creates a bidder manager from YAML config.
// It will utilize advanced AdapterConfig to perform precision targeting filters before launching DSP threads.
func NewManagerFromConfig(cfg *config.Config) *Manager {
	var bidders []ManagedBidder

	if len(cfg.Adapters) > 0 {
		for _, ac := range cfg.Adapters {
			if ac.Status == 0 {
				continue
			}

			acCpy := ac
			var b Bidder
			switch ac.Type {
			case "ortb":
				b = NewORTBBidder(ac.Name, ac.Endpoint, ac.TimeoutMs)
			case "vast":
				b = NewVASTBidder(ac.Name, ac.Endpoint, ac.TimeoutMs, ac.Floor)
			}
			if b != nil {
				bidders = append(bidders, ManagedBidder{
					Bidder: b,
					Config: &acCpy,
				})
			}
		}
	} else {
		// Fallback for legacy configs
		for _, bc := range cfg.Bidders {
			if bc.Status == 0 {
				continue
			}
			var b Bidder
			switch bc.Type {
			case "ortb":
				b = NewORTBBidder(bc.Name, bc.Endpoint, bc.Timeout)
			case "vast":
				b = NewVASTBidder(bc.Name, bc.Endpoint, bc.Timeout, bc.Floor)
			}
			if b != nil {
				bidders = append(bidders, ManagedBidder{
					Bidder: b,
					Config: &config.AdapterConfig{Name: bc.Name},
				})
			}
		}
	}

	return &Manager{bidders: bidders}
}

// NewManager creates a default manager (fallback).
func NewManager() *Manager {
	return &Manager{
		bidders: []ManagedBidder{
			{
				Bidder: NewORTBBidder("dsp1", "https://example-dsp.com/openrtb", 120),
				Config: &config.AdapterConfig{Name: "dsp1"},
			},
		},
	}
}

// validateTargeting evaluates request against Adapter Config criteria. Returns false if DSP should be skipped.
func (mb *ManagedBidder) validateTargeting(req openrtb.BidRequest) bool {
	if mb.Config == nil {
		return true // pass-through
	}

	// Geolocation blocking / Allowing — O(1) map lookup
	if len(mb.Config.TargetGeos) > 0 && req.Device.Geo != nil && req.Device.Geo.Country != "" {
		if mb.geoSet == nil {
			mb.geoSet = buildStringSet(mb.Config.TargetGeos)
		}
		if !mb.geoSet[strings.ToUpper(req.Device.Geo.Country)] {
			return false
		}
	}

	// OS Targeting — O(1) map lookup
	if len(mb.Config.TargetOS) > 0 && req.Device.OS != "" {
		if mb.osSet == nil {
			mb.osSet = buildStringSet(mb.Config.TargetOS)
		}
		if !mb.osSet[strings.ToUpper(req.Device.OS)] {
			return false
		}
	}

	// Bcat Blocking (Blocked Categories)
	if len(mb.Config.BlockedBcat) > 0 && len(req.BCat) > 0 {
		if mb.bcatSet == nil {
			mb.bcatSet = buildStringSet(mb.Config.BlockedBcat)
		}
		for _, reqCat := range req.BCat {
			if mb.bcatSet[strings.ToUpper(reqCat)] {
				return false
			}
		}
	}

	return true
}

func buildStringSet(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, s := range items {
		m[strings.ToUpper(s)] = true
	}
	return m
}

func (m *Manager) CallAll(req openrtb.BidRequest) []openrtb.Bid {
	var wg sync.WaitGroup
	bidChan := make(chan openrtb.Bid, len(m.bidders)*2) // Buffered for worst case

	for _, bidder := range m.bidders {
		if !bidder.validateTargeting(req) {
			continue // Stop processing and avoid sending network traffic
		}

		wg.Add(1)
		go func(b ManagedBidder) {
			defer wg.Done()
			bids, err := b.Request(req)
			if err != nil {
				return
			}
			for _, bid := range bids {
				bid.DemandSrc = b.Name()
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

func (m *Manager) Bidders() []Bidder {
	var interfaceBidders []Bidder
	for _, b := range m.bidders {
		interfaceBidders = append(interfaceBidders, b.Bidder)
	}
	return interfaceBidders
}
