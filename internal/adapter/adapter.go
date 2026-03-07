package adapter

import (
	"context"
	"ssp/internal/openrtb"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DemandAdapter is the unified interface for all demand sources.
// Magnite, FreeWheel, and Prebid Server all use this pattern: every demand
// partner (DSP, ad network, VAST tag, header bidder) implements one interface
// so the auction engine treats them identically.
type DemandAdapter interface {
	// ID returns the unique adapter identifier.
	ID() string
	// Name returns the human-readable name.
	Name() string
	// Type returns "ortb", "vast", or "direct".
	Type() AdapterType
	// RequestBids sends a bid request and returns bids within the timeout.
	RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error)
	// Supports returns true if this adapter can handle the given request.
	Supports(req *openrtb.BidRequest) bool
}

type AdapterType string

const (
	TypeORTB   AdapterType = "ortb"
	TypeVAST   AdapterType = "vast"
	TypeDirect AdapterType = "direct"
)

func resolveTimeout(timeoutMs int) time.Duration {
	if timeoutMs <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(timeoutMs) * time.Millisecond
}

func normalizeORTBVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "2.5" {
		return "2.5"
	}
	return "2.6"
}

// normalizeMargin interprets margin as either ratio (0.2) or percent (20).
// Dashboard inputs are percentage-based, while config files may use ratios.
func normalizeMargin(margin float64) float64 {
	if margin <= 0 {
		return 0
	}
	if margin > 1 {
		margin = margin / 100.0
	}
	if margin >= 1 {
		return 0.999
	}
	return margin
}

// BidResult contains bids from a single adapter plus metadata.
type BidResult struct {
	AdapterID   string
	Bids        []openrtb.Bid
	Latency     time.Duration
	NoBid       bool
	NoBidReason string
	TimedOut    bool
	Error       error
}

// AdapterConfig holds adapter-specific settings loaded from config.
type AdapterConfig struct {
	ID            string      `yaml:"id" json:"id"`
	Name          string      `yaml:"name" json:"name"`
	Type          AdapterType `yaml:"type" json:"type"`
	ORTBVersion   string      `yaml:"ortb_version" json:"ortb_version"`
	Endpoint      string      `yaml:"endpoint" json:"endpoint"`
	TimeoutMs     int         `yaml:"timeout_ms" json:"timeout_ms"`
	Floor         float64     `yaml:"floor" json:"floor"`
	Margin        float64     `yaml:"margin" json:"margin"`
	QPSLimit      int         `yaml:"qps_limit" json:"qps_limit"` // 0 = unlimited
	AuctionType   string      `yaml:"auction_type" json:"auction_type"`
	Status        int         `yaml:"status" json:"status"` // 1=active
	TargetGeos    []string
	TargetOS      []string
	BlockedBcat   []string
	AllowedMime   []string
	GZIPSupport   bool
	RemovePChain  bool
	SChainEnabled bool
	BAdv          []string
	BCat          []string

	targetGeoSet   map[string]struct{} `yaml:"-" json:"-"`
	targetOSSet    map[string]struct{} `yaml:"-" json:"-"`
	blockedBCatSet map[string]struct{} `yaml:"-" json:"-"`
}

// Registry manages all demand adapters with hot-reload capability.
// This is the pattern Magnite uses: adapters can be added/removed/updated
// at runtime without restarting the server.
type Registry struct {
	mu       sync.RWMutex
	adapters map[string]DemandAdapter
	configs  map[string]*AdapterConfig
	qps      map[string]*qpsTracker
}

type qpsTracker struct {
	limit   int
	count   atomic.Int64
	resetAt atomic.Int64 // unix second when count resets
}

func (q *qpsTracker) Allow() bool {
	if q.limit <= 0 {
		return true
	}
	now := time.Now().Unix()
	if now > q.resetAt.Load() {
		q.count.Store(0)
		q.resetAt.Store(now)
	}
	return q.count.Add(1) <= int64(q.limit)
}

func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[string]DemandAdapter),
		configs:  make(map[string]*AdapterConfig),
		qps:      make(map[string]*qpsTracker),
	}
}

// Register adds a demand adapter to the registry.
func (r *Registry) Register(adapter DemandAdapter, cfg *AdapterConfig) {
	if adapter == nil || cfg == nil || cfg.ID == "" {
		return
	}
	r.mu.Lock()
	preprocessAdapterConfig(cfg)
	r.adapters[cfg.ID] = adapter
	r.configs[cfg.ID] = cfg
	if cfg.QPSLimit > 0 {
		r.qps[cfg.ID] = &qpsTracker{limit: cfg.QPSLimit}
	}
	r.mu.Unlock()
}

// Remove removes an adapter from the registry (hot-reload support).
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	delete(r.adapters, id)
	delete(r.configs, id)
	delete(r.qps, id)
	r.mu.Unlock()
}

// GetActive returns all active adapters that support the given request.
// Applies QPS throttling per adapter (GAM-style traffic shaping).
func (r *Registry) GetActive(req *openrtb.BidRequest) []DemandAdapter {
	r.mu.RLock()
	defer r.mu.RUnlock()

	active := make([]DemandAdapter, 0, len(r.adapters))
	for id, adapter := range r.adapters {
		cfg := r.configs[id]
		if cfg == nil {
			continue
		}
		if cfg.Status != 1 {
			continue
		}
		if !validateTargetingOptions(cfg, req) {
			continue
		}
		if !adapter.Supports(req) {
			continue
		}
		if tracker, ok := r.qps[id]; ok && !tracker.Allow() {
			continue
		}
		active = append(active, adapter)
	}
	return active
}

// FanOut sends bid requests to all eligible adapters in parallel with timeout.
func (r *Registry) FanOut(ctx context.Context, req *openrtb.BidRequest, tmax time.Duration) []*BidResult {
	return r.dispatchBids(ctx, req, tmax, r.GetActive(req))
}

// FanOutTo sends bid requests to a specific set of adapters (by ID) in parallel.
// Used when supply-demand mappings restrict which demand sources receive requests.
func (r *Registry) FanOutTo(ctx context.Context, req *openrtb.BidRequest, tmax time.Duration, adapterIDs []string) []*BidResult {
	r.mu.RLock()
	adapters := make([]DemandAdapter, 0, len(adapterIDs))
	for _, id := range adapterIDs {
		a, ok := r.adapters[id]
		if !ok {
			continue
		}
		cfg := r.configs[id]
		if cfg != nil && cfg.Status != 1 {
			continue
		}
		if !a.Supports(req) {
			continue
		}
		if tracker, ok := r.qps[id]; ok && !tracker.Allow() {
			continue
		}
		adapters = append(adapters, a)
	}
	r.mu.RUnlock()

	return r.dispatchBids(ctx, req, tmax, adapters)
}

// dispatchBids sends bid requests to adapters in parallel with timeout.
// Returns immediately when TMax expires with whatever bids have arrived.
func (r *Registry) dispatchBids(ctx context.Context, req *openrtb.BidRequest, tmax time.Duration, adapters []DemandAdapter) []*BidResult {
	if len(adapters) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, tmax)
	defer cancel()

	resultCh := make(chan *BidResult, len(adapters))
	pending := make(map[string]struct{}, len(adapters))
	for _, adapter := range adapters {
		pending[adapter.ID()] = struct{}{}
		go func(a DemandAdapter) {
			start := time.Now()
			result, err := a.RequestBids(ctx, req)
			if err != nil {
				resultCh <- &BidResult{AdapterID: a.ID(), Error: err, Latency: time.Since(start)}
				return
			}
			if result == nil {
				resultCh <- &BidResult{AdapterID: a.ID(), NoBid: true, Latency: time.Since(start)}
				return
			}
			result.Latency = time.Since(start)
			result.AdapterID = a.ID()
			resultCh <- result
		}(adapter)
	}

	var out []*BidResult
	remaining := len(adapters)
	for remaining > 0 {
		select {
		case br := <-resultCh:
			out = append(out, br)
			delete(pending, br.AdapterID)
			remaining--
		case <-ctx.Done():
			for adapterID := range pending {
				out = append(out, &BidResult{AdapterID: adapterID, Error: context.DeadlineExceeded, TimedOut: true, NoBid: true})
			}
			return out
		}
	}
	return out
}

// GetConfig returns the config for a specific adapter.
func (r *Registry) GetConfig(id string) *AdapterConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.configs[id]
}

// All returns all registered adapter IDs.
func (r *Registry) All() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.adapters))
	for id := range r.adapters {
		ids = append(ids, id)
	}
	return ids
}

// Count returns the number of registered adapters.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.adapters)
}

// AdapterInfo is a JSON-serialisable view of a registered adapter.
type AdapterInfo struct {
	ID       string      `json:"id"`
	Name     string      `json:"name"`
	Type     AdapterType `json:"type"`
	Endpoint string      `json:"endpoint"`
	Status   int         `json:"status"`
	QPSLimit int         `json:"qps_limit"`
}

// List returns info about all registered adapters.
func (r *Registry) List() []AdapterInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AdapterInfo, 0, len(r.adapters))
	for id, a := range r.adapters {
		cfg := r.configs[id]
		info := AdapterInfo{
			ID:   id,
			Name: a.Name(),
			Type: a.Type(),
		}
		if cfg != nil {
			info.Endpoint = cfg.Endpoint
			info.Status = cfg.Status
			info.QPSLimit = cfg.QPSLimit
		}
		out = append(out, info)
	}
	return out
}

// validateTargetingOptions ensures request matches specific target criteria (TargetGeos, TargetOS, BlockedBcat)
func validateTargetingOptions(cfg *AdapterConfig, req *openrtb.BidRequest) bool {
	if cfg == nil {
		return true
	}
	if req == nil || req.Device == nil {
		return true
	}

	// Geo targeting
	if len(cfg.targetGeoSet) > 0 {
		if req.Device.Geo == nil || req.Device.Geo.Country == "" {
			return false
		}
		_, match := cfg.targetGeoSet[strings.ToUpper(req.Device.Geo.Country)]
		if !match {
			return false
		}
	}

	// OS targeting
	if len(cfg.targetOSSet) > 0 {
		if req.Device.OS == "" {
			return false
		}
		_, match := cfg.targetOSSet[strings.ToUpper(req.Device.OS)]
		if !match {
			return false
		}
	}

	// Bcat blocking
	if len(cfg.blockedBCatSet) > 0 && len(req.BCat) > 0 {
		for _, bc := range req.BCat {
			if _, blocked := cfg.blockedBCatSet[strings.ToUpper(strings.TrimSpace(bc))]; blocked {
				return false
			}
		}
	}
	return true
}

func preprocessAdapterConfig(cfg *AdapterConfig) {
	if cfg == nil {
		return
	}
	cfg.targetGeoSet = normalizeUpperSet(cfg.TargetGeos)
	cfg.targetOSSet = normalizeUpperSet(cfg.TargetOS)
	cfg.blockedBCatSet = normalizeUpperSet(cfg.BlockedBcat)
}

func normalizeUpperSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, raw := range values {
		normalized := strings.ToUpper(strings.TrimSpace(raw))
		if normalized == "" {
			continue
		}
		set[normalized] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}
