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

	// Normalized lookup sets for fast request-path matching.
	geoSet       map[string]struct{} `json:"-" yaml:"-"`
	deviceSet    map[int]struct{}    `json:"-" yaml:"-"`
	bundleSet    map[string]struct{} `json:"-" yaml:"-"`
	hourSet      map[int]struct{}    `json:"-" yaml:"-"`
	mediaTypeSet map[string]struct{} `json:"-" yaml:"-"`
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
	normalizeRule(r)
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
	ctx := buildRuleMatchCtx(req)
	hourUTC := time.Now().UTC().Hour()

	for _, r := range e.rules {
		if r.Status != 1 {
			continue
		}
		if matchesRule(r, ctx, hourUTC) {
			return r.FloorCPM
		}
	}

	// Adaptive floor: 70% of rolling average (same as existing DynamicFloor)
	if e.avgPrice > 0 {
		return e.avgPrice * 0.7
	}
	return 0
}

type ruleMatchCtx struct {
	country    string
	deviceType int
	bundle     string
	hasVideo   bool
}

func buildRuleMatchCtx(req *openrtb.BidRequest) ruleMatchCtx {
	ctx := ruleMatchCtx{}
	if req == nil {
		return ctx
	}
	if req.Device != nil {
		ctx.deviceType = int(req.Device.DeviceType)
		if req.Device.Geo != nil {
			ctx.country = strings.ToUpper(strings.TrimSpace(req.Device.Geo.Country))
		}
	}
	if req.App != nil {
		ctx.bundle = strings.ToLower(strings.TrimSpace(req.App.Bundle))
	}
	ctx.hasVideo = len(req.Imp) > 0 && req.Imp[0].Video != nil
	return ctx
}

func normalizeRule(r *Rule) {
	r.geoSet = make(map[string]struct{}, len(r.Geos))
	for _, g := range r.Geos {
		n := strings.ToUpper(strings.TrimSpace(g))
		if n != "" {
			r.geoSet[n] = struct{}{}
		}
	}

	r.deviceSet = make(map[int]struct{}, len(r.DeviceTypes))
	for _, dt := range r.DeviceTypes {
		r.deviceSet[dt] = struct{}{}
	}

	r.bundleSet = make(map[string]struct{}, len(r.AppBundles))
	for _, b := range r.AppBundles {
		n := strings.ToLower(strings.TrimSpace(b))
		if n != "" {
			r.bundleSet[n] = struct{}{}
		}
	}

	r.hourSet = make(map[int]struct{}, len(r.Hours))
	for _, h := range r.Hours {
		r.hourSet[h] = struct{}{}
	}

	r.mediaTypeSet = make(map[string]struct{}, len(r.MediaTypes))
	for _, m := range r.MediaTypes {
		n := strings.ToLower(strings.TrimSpace(m))
		if n != "" {
			r.mediaTypeSet[n] = struct{}{}
		}
	}
}

func matchesRule(r *Rule, ctx ruleMatchCtx, hourUTC int) bool {
	if !matchRuleGeo(r, ctx) {
		return false
	}
	if !matchRuleDevice(r, ctx) {
		return false
	}
	if !matchRuleBundle(r, ctx) {
		return false
	}
	if !matchRuleHour(r, hourUTC) {
		return false
	}
	if !matchRuleMedia(r, ctx) {
		return false
	}
	return true
}

func matchRuleGeo(r *Rule, ctx ruleMatchCtx) bool {
	if len(r.geoSet) == 0 {
		return true
	}
	if ctx.country == "" {
		return false
	}
	_, ok := r.geoSet[ctx.country]
	return ok
}

func matchRuleDevice(r *Rule, ctx ruleMatchCtx) bool {
	if len(r.deviceSet) == 0 {
		return true
	}
	_, ok := r.deviceSet[ctx.deviceType]
	return ok
}

func matchRuleBundle(r *Rule, ctx ruleMatchCtx) bool {
	if len(r.bundleSet) == 0 {
		return true
	}
	if ctx.bundle == "" {
		return false
	}
	_, ok := r.bundleSet[ctx.bundle]
	return ok
}

func matchRuleHour(r *Rule, hourUTC int) bool {
	if len(r.hourSet) == 0 {
		return true
	}
	_, ok := r.hourSet[hourUTC]
	return ok
}

func matchRuleMedia(r *Rule, ctx ruleMatchCtx) bool {
	if len(r.mediaTypeSet) == 0 {
		return true
	}
	if ctx.hasVideo {
		if _, ok := r.mediaTypeSet["video"]; ok {
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
