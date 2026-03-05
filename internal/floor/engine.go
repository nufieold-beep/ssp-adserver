package floor

import (
	"ssp/internal/openrtb"
	"strings"
	"sync"
	"time"
)

// Rule defines a floor price rule. Rules are evaluated in priority order;
// the first matching rule wins. This mirrors how Magnite and Index Exchange
// implement multi-dimensional floor optimization.
type Rule struct {
	ID          string   `json:"id" yaml:"id"`
	Name        string   `json:"name" yaml:"name"`
	Priority    int      `json:"priority" yaml:"priority"` // Lower = higher priority
	FloorCPM    float64  `json:"floor_cpm" yaml:"floor_cpm"`
	Geos        []string `json:"geos" yaml:"geos"`                 // ISO country codes
	DeviceTypes []int    `json:"device_types" yaml:"device_types"` // 3=CTV, 7=STB
	AppBundles  []string `json:"app_bundles" yaml:"app_bundles"`
	Hours       []int    `json:"hours" yaml:"hours"`             // 0-23 UTC hours
	MediaTypes  []string `json:"media_types" yaml:"media_types"` // "video", "banner"
	Status      int      `json:"status" yaml:"status"`           // 1=active
}

// Engine computes floor prices using rule-based matching plus an adaptive
// component derived from recent win prices. Enterprise SSPs like Magnite
// use similar multi-factor floor optimization to maximize publisher revenue
// without suppressing fill rate.
type Engine struct {
	mu       sync.RWMutex
	rules    []*Rule
	avgPrice float64 // Rolling average win price for adaptive floors
}

func NewEngine() *Engine {
	return &Engine{rules: make([]*Rule, 0)}
}

// AddRule adds a floor rule and re-sorts by priority.
func (e *Engine) AddRule(r *Rule) {
	e.mu.Lock()
	e.rules = append(e.rules, r)
	sortRules(e.rules)
	e.mu.Unlock()
}

// RemoveRule removes a rule by ID.
func (e *Engine) RemoveRule(id string) {
	e.mu.Lock()
	for i, r := range e.rules {
		if r.ID == id {
			e.rules = append(e.rules[:i], e.rules[i+1:]...)
			break
		}
	}
	e.mu.Unlock()
}

// ListRules returns all floor rules.
func (e *Engine) ListRules() []*Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Rule, len(e.rules))
	copy(out, e.rules)
	return out
}

// UpdateAvgPrice sets the rolling average win price for the adaptive component.
func (e *Engine) UpdateAvgPrice(avg float64) {
	e.mu.Lock()
	e.avgPrice = avg
	e.mu.Unlock()
}

// Calculate returns the effective floor price for a request.
// Evaluation order:
//  1. Walk rules in priority order; use the first match
//  2. If no rule matches, use adaptive floor (70% of avg win price)
//  3. Return 0 if no rule matches and no avg price data
func (e *Engine) Calculate(req *openrtb.BidRequest) float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, r := range e.rules {
		if r.Status != 1 {
			continue
		}
		if matchesRule(r, req) {
			return r.FloorCPM
		}
	}

	// Adaptive floor: 70% of rolling average (same as existing DynamicFloor)
	if e.avgPrice > 0 {
		return e.avgPrice * 0.7
	}
	return 0
}

func matchesRule(r *Rule, req *openrtb.BidRequest) bool {
	if !matchRuleGeo(r, req) {
		return false
	}
	if !matchRuleDevice(r, req) {
		return false
	}
	if !matchRuleBundle(r, req) {
		return false
	}
	if !matchRuleHour(r) {
		return false
	}
	if !matchRuleMedia(r, req) {
		return false
	}
	return true
}

func matchRuleGeo(r *Rule, req *openrtb.BidRequest) bool {
	if len(r.Geos) == 0 {
		return true
	}
	if req.Device.Geo == nil || req.Device.Geo.Country == "" {
		return false
	}
	country := strings.ToUpper(req.Device.Geo.Country)
	for _, g := range r.Geos {
		if strings.ToUpper(g) == country {
			return true
		}
	}
	return false
}

func matchRuleDevice(r *Rule, req *openrtb.BidRequest) bool {
	if len(r.DeviceTypes) == 0 {
		return true
	}
	for _, dt := range r.DeviceTypes {
		if dt == req.Device.DeviceType {
			return true
		}
	}
	return false
}

func matchRuleBundle(r *Rule, req *openrtb.BidRequest) bool {
	if len(r.AppBundles) == 0 {
		return true
	}
	if req.App == nil || req.App.Bundle == "" {
		return false
	}
	for _, b := range r.AppBundles {
		if b == req.App.Bundle {
			return true
		}
	}
	return false
}

func matchRuleHour(r *Rule) bool {
	if len(r.Hours) == 0 {
		return true
	}
	h := time.Now().UTC().Hour()
	for _, rh := range r.Hours {
		if rh == h {
			return true
		}
	}
	return false
}

func matchRuleMedia(r *Rule, req *openrtb.BidRequest) bool {
	if len(r.MediaTypes) == 0 {
		return true
	}
	hasVideo := false
	if len(req.Imp) > 0 && req.Imp[0].Video != nil {
		hasVideo = true
	}
	for _, m := range r.MediaTypes {
		if strings.ToLower(m) == "video" && hasVideo {
			return true
		}
	}
	return false
}

func sortRules(rules []*Rule) {
	for i := 1; i < len(rules); i++ {
		key := rules[i]
		j := i - 1
		for j >= 0 && rules[j].Priority > key.Priority {
			rules[j+1] = rules[j]
			j--
		}
		rules[j+1] = key
	}
}
