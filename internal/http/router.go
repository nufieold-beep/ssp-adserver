package http

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ssp/internal/adapter"
	"ssp/internal/adquality"
	"ssp/internal/auction"
	"ssp/internal/config"
	"ssp/internal/floor"
	"ssp/internal/httputil"
	"ssp/internal/monitor"
	"ssp/internal/openrtb"
	"ssp/internal/pipeline"
	"ssp/internal/validate"
	"ssp/internal/vast"

	"github.com/gofiber/fiber/v2"
	"github.com/prebid/openrtb/v20/adcom1"
)

// ── In-memory stores ──

type Campaign struct {
	ID            int     `json:"id"`
	Name          string  `json:"name"`
	AdvertiserID  int     `json:"advertiser_id"`
	Status        int     `json:"status"` // 1=active, 0=paused
	Bid           float64 `json:"bid"`
	BidFloor      float64 `json:"bid_floor"`
	ADomain       string  `json:"adomain"`
	BudgetDaily   float64 `json:"budget_daily"`
	BudgetTotal   float64 `json:"budget_total"`
	SpentToday    float64 `json:"spent_today"`
	SpentTotal    float64 `json:"spent_total"`
	FrequencyCap  int     `json:"frequency_cap"`
	PacingEnabled bool    `json:"pacing_enabled"`
	Env           string  `json:"env"`
	Description   string  `json:"description"`
	IabCategories string  `json:"iab_categories"`
}

type Advertiser struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Company     string  `json:"company"`
	Email       string  `json:"email"`
	Balance     float64 `json:"balance"`
	DailyBudget float64 `json:"daily_budget"`
	Status      int     `json:"status"`
}

type SupplyTag struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	SlotID      string  `json:"slot_id"`
	Integration string  `json:"integration_type"` // tag, ortb, prebid
	Pricing     string  `json:"pricing_model"`
	Floor       float64 `json:"floor"`
	Margin      float64 `json:"margin"`
	Env         string  `json:"environment"` // CTV, STB, Mobile
	MinDur      int     `json:"min_duration"`
	MaxDur      int     `json:"max_duration"`
	Width       int     `json:"width"`
	Height      int     `json:"height"`
	Sensitive   bool    `json:"sensitive"`
	Status      int     `json:"status"`
	Channel     string  `json:"channel"`
	// CTV-specific fields for VAST tag generation
	CountryCode  string `json:"country_code,omitempty"`
	ContentGenre string `json:"content_genre,omitempty"` // comma-separated: game,entertainment,family
	ContentLang  string `json:"content_lang,omitempty"`  // en, es, etc.
	DeviceType   int    `json:"device_type,omitempty"`   // 3=CTV, 7=STB
	AppName      string `json:"app_name,omitempty"`      // Added for App/Site building
	AppBundle    string `json:"app_bundle,omitempty"`    // Added for App/Site building
	Domain       string `json:"domain,omitempty"`        // Added for App/Site building
	VastURL      string `json:"vast_url,omitempty"`      // Generated VAST tag URL (read-only)
}

type DemandEndpoint struct {
	ID           int      `json:"id"`
	Name         string   `json:"name"`
	URL          string   `json:"endpoint_url"`
	Integration  string   `json:"integration"`
	OrtbVersion  string   `json:"ortb_version"`
	AuctionType  string   `json:"auction_type"`
	Floor        float64  `json:"floor"`
	Timeout      int      `json:"timeout_ms"`
	QPS          int      `json:"qps_limit"`
	Sensitive    bool     `json:"sensitive"`
	Margin       float64  `json:"margin"`
	Status       int      `json:"status"`
	GZIPSupport  bool     `json:"gzip_support"`
	RemovePChain bool     `json:"remove_pchain"`
	BAdv         []string `json:"badv"`
	BCat         []string `json:"bcat"`
	SupplyChain  bool     `json:"schain_enabled"`
}

type DemandVastTag struct {
	ID     int     `json:"id"`
	Name   string  `json:"name"`
	URL    string  `json:"vast_url"`
	Floor  float64 `json:"floor"`
	Margin float64 `json:"margin"`
	CPM    float64 `json:"cpm"`
	Status int     `json:"status"`
}

type SDMapping struct {
	ID       int    `json:"id"`
	SupplyID int    `json:"supply_tag_id"`
	DemandID int    `json:"demand_source_id"`
	Type     string `json:"demand_type"` // ortb, vast
	Priority int    `json:"priority"`
	Weight   int    `json:"weight"`
	Status   int    `json:"status"`
}

type TargetingRule struct {
	ID         int             `json:"id"`
	CampaignID int             `json:"campaign_id"`
	Type       string          `json:"rule_type"`
	Include    bool            `json:"include"`
	Value      json.RawMessage `json:"rule_value"`
}

type AdDecision struct {
	Time       time.Time `json:"time"`
	CampaignID int       `json:"campaign_id,omitempty"`
	Campaign   string    `json:"campaign,omitempty"`
	CreativeID string    `json:"creative_id"`
	SupplyID   int       `json:"supply_source_id,omitempty"`
	Source     string    `json:"source"`
	ADomain    string    `json:"adomain"`
	ADSource   string    `json:"ad_source"`
	BidPrice   float64   `json:"bid_price"`
	GrossPrice float64   `json:"gross_price"`
	NetPrice   float64   `json:"net_price"`
	Seat       string    `json:"seat"`
	AdmType    string    `json:"adm_type"`
	DemandEp   string    `json:"demand_endpoint"`
	Delivery   string    `json:"delivery_status,omitempty"`
	AppBundle  string    `json:"app_bundle"`
	RawBundle  string    `json:"raw_bundle,omitempty"`
	Country    string    `json:"country"`
	DeviceType string    `json:"device_type"`
}

// store holds all in-memory state.
type store struct {
	mu sync.RWMutex

	deliveryMu sync.RWMutex
	decisionMu sync.RWMutex
	persistMu  sync.Mutex

	campaigns      map[int]*Campaign
	nextCampaignID int

	advertisers      map[int]*Advertiser
	nextAdvertiserID int

	supplyTags           map[int]*SupplyTag
	nextSupplyTagID      int
	activeSupplyTagCount int
	supplyBySlotID       map[string]*SupplyTag
	supplyBySlotIDNorm   map[string]*SupplyTag
	supplyByName         map[string]*SupplyTag
	supplyByNameNorm     map[string]*SupplyTag
	supplyByIDStr        map[string]*SupplyTag

	demandEndpoints      map[int]*DemandEndpoint
	nextDemandEndpointID int

	demandVastTags      map[int]*DemandVastTag
	nextDemandVastTagID int

	mappings               map[int]*SDMapping
	nextMappingID          int
	mappingsBySID          map[int][]*SDMapping
	mappingAdapterIDsBySID map[int][]string

	activeCampaignByDomain map[string]*Campaign
	activeCampaignFallback *Campaign

	targetingRules map[int]*TargetingRule
	nextRuleID     int

	adDecisions []AdDecision

	analyticsTotals       analyticsAccumulator
	analyticsDemandTotals map[string]analyticsAccumulator
	analyticsSupplyTotals map[int]analyticsAccumulator
	analyticsBundleTotals map[string]analyticsAccumulator

	budgetDayKey   string
	frequencyByKey map[string]int

	dashboardUser      string
	dashboardPass      string
	dashboardSessions  map[string]time.Time
	statePath          string
	stateGeneration    atomic.Uint64
	statePersistMu     sync.Mutex
	statePersistDirty  bool
	statePersistQueued bool
}

type supplyDemandState struct {
	Version              int                     `json:"version"`
	NextCampaignID       int                     `json:"next_campaign_id"`
	NextAdvertiserID     int                     `json:"next_advertiser_id"`
	NextRuleID           int                     `json:"next_rule_id"`
	NextSupplyTagID      int                     `json:"next_supply_tag_id"`
	NextDemandEndpointID int                     `json:"next_demand_endpoint_id"`
	NextDemandVastTagID  int                     `json:"next_demand_vast_tag_id"`
	NextMappingID        int                     `json:"next_mapping_id"`
	Campaigns            []Campaign              `json:"campaigns"`
	Advertisers          []Advertiser            `json:"advertisers"`
	TargetingRules       []TargetingRule         `json:"targeting_rules"`
	SupplyTags           []SupplyTag             `json:"supply_tags"`
	DemandEndpoints      []DemandEndpoint        `json:"demand_endpoints"`
	DemandVastTags       []DemandVastTag         `json:"demand_vast_tags"`
	Mappings             []SDMapping             `json:"mappings"`
	Analytics            persistedAnalyticsState `json:"analytics,omitempty"`
}

type pendingSupplyDemandStateWrite struct {
	store      *store
	statePath  string
	generation uint64
	snapshot   supplyDemandState
}

const runtimeStateFileName = "runtime_state.json"

func newStore() *store {
	return &store{
		campaigns:              make(map[int]*Campaign),
		nextCampaignID:         1,
		advertisers:            make(map[int]*Advertiser),
		nextAdvertiserID:       1,
		supplyTags:             make(map[int]*SupplyTag),
		nextSupplyTagID:        1,
		activeSupplyTagCount:   0,
		supplyBySlotID:         make(map[string]*SupplyTag),
		supplyBySlotIDNorm:     make(map[string]*SupplyTag),
		supplyByName:           make(map[string]*SupplyTag),
		supplyByNameNorm:       make(map[string]*SupplyTag),
		supplyByIDStr:          make(map[string]*SupplyTag),
		demandEndpoints:        make(map[int]*DemandEndpoint),
		nextDemandEndpointID:   1,
		demandVastTags:         make(map[int]*DemandVastTag),
		nextDemandVastTagID:    1,
		mappings:               make(map[int]*SDMapping),
		nextMappingID:          1,
		mappingsBySID:          make(map[int][]*SDMapping),
		mappingAdapterIDsBySID: make(map[int][]string),
		activeCampaignByDomain: make(map[string]*Campaign),
		targetingRules:         make(map[int]*TargetingRule),
		nextRuleID:             1,
		analyticsDemandTotals:  make(map[string]analyticsAccumulator),
		analyticsSupplyTotals:  make(map[int]analyticsAccumulator),
		analyticsBundleTotals:  make(map[string]analyticsAccumulator),
		budgetDayKey:           time.Now().UTC().Format("2006-01-02"),
		frequencyByKey:         make(map[string]int),
		dashboardUser:          "admin",
		dashboardPass:          "admin",
		dashboardSessions:      make(map[string]time.Time),
	}
}

func generateDashboardSessionToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (s *store) createDashboardSession() (string, error) {
	token, err := generateDashboardSessionToken()
	if err != nil {
		return "", err
	}

	expires := time.Now().UTC().Add(24 * time.Hour)
	s.mu.Lock()
	if s.dashboardSessions == nil {
		s.dashboardSessions = make(map[string]time.Time)
	}
	s.dashboardSessions[token] = expires
	s.mu.Unlock()

	return token, nil
}

func (s *store) validateDashboardSession(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}

	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()

	expires, ok := s.dashboardSessions[token]
	if !ok {
		return false
	}
	if now.After(expires) {
		delete(s.dashboardSessions, token)
		return false
	}
	return true
}

func (s *store) clearDashboardSession(token string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}
	s.mu.Lock()
	delete(s.dashboardSessions, token)
	s.mu.Unlock()
}

func resolveSupplyDemandStatePath(configPath string) string {
	if v := strings.TrimSpace(os.Getenv("SSP_STATE_PATH")); v != "" {
		return v
	}
	if configPath != "" && filepath.IsAbs(configPath) {
		return filepath.Join(filepath.Dir(configPath), runtimeStateFileName)
	}
	return filepath.Join("data", runtimeStateFileName)
}

func maxInt(values ...int) int {
	m := 0
	for _, v := range values {
		if v > m {
			m = v
		}
	}
	return m
}

func (s *store) loadSupplyDemandState(statePath string) error {
	s.statePath = strings.TrimSpace(statePath)
	if s.statePath == "" {
		return nil
	}

	raw, err := os.ReadFile(s.statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var snapshot supplyDemandState
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.campaigns = make(map[int]*Campaign, len(snapshot.Campaigns))
	maxCampaignID := 0
	for i := range snapshot.Campaigns {
		campaign := snapshot.Campaigns[i]
		if campaign.ID <= 0 {
			continue
		}
		c := campaign
		s.campaigns[c.ID] = &c
		if c.ID > maxCampaignID {
			maxCampaignID = c.ID
		}
	}

	s.advertisers = make(map[int]*Advertiser, len(snapshot.Advertisers))
	maxAdvertiserID := 0
	for i := range snapshot.Advertisers {
		advertiser := snapshot.Advertisers[i]
		if advertiser.ID <= 0 {
			continue
		}
		a := advertiser
		s.advertisers[a.ID] = &a
		if a.ID > maxAdvertiserID {
			maxAdvertiserID = a.ID
		}
	}

	s.targetingRules = make(map[int]*TargetingRule, len(snapshot.TargetingRules))
	maxRuleID := 0
	for i := range snapshot.TargetingRules {
		rule := snapshot.TargetingRules[i]
		if rule.ID <= 0 {
			continue
		}
		r := rule
		s.targetingRules[r.ID] = &r
		if r.ID > maxRuleID {
			maxRuleID = r.ID
		}
	}

	s.supplyTags = make(map[int]*SupplyTag, len(snapshot.SupplyTags))
	maxSupplyID := 0
	for i := range snapshot.SupplyTags {
		tag := snapshot.SupplyTags[i]
		if tag.ID <= 0 {
			continue
		}
		t := tag
		s.supplyTags[t.ID] = &t
		if t.ID > maxSupplyID {
			maxSupplyID = t.ID
		}
	}

	s.demandEndpoints = make(map[int]*DemandEndpoint, len(snapshot.DemandEndpoints))
	maxDemandEndpointID := 0
	for i := range snapshot.DemandEndpoints {
		ep := snapshot.DemandEndpoints[i]
		if ep.ID <= 0 {
			continue
		}
		e := ep
		s.demandEndpoints[e.ID] = &e
		if e.ID > maxDemandEndpointID {
			maxDemandEndpointID = e.ID
		}
	}

	s.demandVastTags = make(map[int]*DemandVastTag, len(snapshot.DemandVastTags))
	maxDemandVastTagID := 0
	for i := range snapshot.DemandVastTags {
		vt := snapshot.DemandVastTags[i]
		if vt.ID <= 0 {
			continue
		}
		t := vt
		s.demandVastTags[t.ID] = &t
		if t.ID > maxDemandVastTagID {
			maxDemandVastTagID = t.ID
		}
	}

	s.mappings = make(map[int]*SDMapping, len(snapshot.Mappings))
	maxMappingID := 0
	for i := range snapshot.Mappings {
		mm := snapshot.Mappings[i]
		if mm.ID <= 0 {
			continue
		}
		m := mm
		s.mappings[m.ID] = &m
		if m.ID > maxMappingID {
			maxMappingID = m.ID
		}
	}

	s.nextCampaignID = maxInt(1, snapshot.NextCampaignID, maxCampaignID+1)
	s.nextAdvertiserID = maxInt(1, snapshot.NextAdvertiserID, maxAdvertiserID+1)
	s.nextRuleID = maxInt(1, snapshot.NextRuleID, maxRuleID+1)
	s.nextSupplyTagID = maxInt(1, snapshot.NextSupplyTagID, maxSupplyID+1)
	s.nextDemandEndpointID = maxInt(1, snapshot.NextDemandEndpointID, maxDemandEndpointID+1)
	s.nextDemandVastTagID = maxInt(1, snapshot.NextDemandVastTagID, maxDemandVastTagID+1)
	s.nextMappingID = maxInt(1, snapshot.NextMappingID, maxMappingID+1)
	s.budgetDayKey = time.Now().UTC().Format("2006-01-02")
	s.frequencyByKey = make(map[string]int)
	s.loadPersistedAnalyticsLocked(snapshot.Analytics)

	s.rebuildSupplyIndexLocked()
	s.rebuildMappingIndexLocked()
	s.rebuildCampaignIndexLocked()
	return nil
}

// prepareSupplyDemandStateWriteLocked snapshots persisted runtime state.
// Caller must hold s.mu lock while mutating the in-memory model.
func (s *store) prepareSupplyDemandStateWriteLocked() *pendingSupplyDemandStateWrite {
	if s.statePath == "" {
		return nil
	}

	generation := s.stateGeneration.Add(1)
	snapshot := supplyDemandState{Version: 2}
	s.snapshotSupplyDemandStateLocked(&snapshot)

	return &pendingSupplyDemandStateWrite{
		store:      s,
		statePath:  s.statePath,
		generation: generation,
		snapshot:   snapshot,
	}
}

func (s *store) snapshotSupplyDemandState() *pendingSupplyDemandStateWrite {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.statePath == "" {
		return nil
	}

	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()

	snapshot := supplyDemandState{Version: 2}
	s.snapshotSupplyDemandStateLocked(&snapshot)

	return &pendingSupplyDemandStateWrite{
		store:      s,
		statePath:  s.statePath,
		generation: s.stateGeneration.Load(),
		snapshot:   snapshot,
	}
}

func (s *store) writeSupplyDemandState() error {
	write := s.snapshotSupplyDemandState()
	if write == nil {
		return nil
	}
	return write.Persist()
}

func (s *store) scheduleDeferredStatePersist() {
	if s == nil || strings.TrimSpace(s.statePath) == "" {
		return
	}

	s.statePersistMu.Lock()
	s.statePersistDirty = true
	if s.statePersistQueued {
		s.statePersistMu.Unlock()
		return
	}
	s.statePersistQueued = true
	s.statePersistMu.Unlock()

	go func() {
		time.Sleep(2 * time.Second)
		for {
			s.statePersistMu.Lock()
			s.statePersistDirty = false
			s.statePersistMu.Unlock()

			if err := s.writeSupplyDemandState(); err != nil {
				log.Printf("Warning: failed to persist runtime analytics state (%s): %v", s.statePath, err)
			}

			s.statePersistMu.Lock()
			if !s.statePersistDirty {
				s.statePersistQueued = false
				s.statePersistMu.Unlock()
				return
			}
			s.statePersistMu.Unlock()
		}
	}()
}

func (w *pendingSupplyDemandStateWrite) Persist() error {
	if w == nil || w.store == nil || w.statePath == "" {
		return nil
	}
	return w.store.persistSupplyDemandState(w)
}

func (s *store) persistSupplyDemandState(write *pendingSupplyDemandStateWrite) error {
	if write == nil || write.statePath == "" {
		return nil
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	if latest := s.stateGeneration.Load(); latest != write.generation {
		if refreshed := s.snapshotSupplyDemandState(); refreshed != nil {
			write = refreshed
		}
	}

	return writeSupplyDemandStateSnapshot(write.statePath, write.snapshot)
}

func writeSupplyDemandStateSnapshot(statePath string, snapshot supplyDemandState) error {
	if statePath == "" {
		return nil
	}

	stateDir := filepath.Dir(statePath)
	if err := os.MkdirAll(stateDir, 0750); err != nil {
		return err
	}

	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	tmpPath := statePath + ".tmp"
	if err := os.WriteFile(tmpPath, encoded, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, statePath); err != nil {
		_ = os.Remove(statePath)
		if err2 := os.Rename(tmpPath, statePath); err2 != nil {
			return err2
		}
	}
	return nil
}

// snapshotSupplyDemandStateLocked copies persisted entities from the store.
// Caller must hold s.mu, and callers using s.mu.RLock must also hold s.deliveryMu.
func (s *store) snapshotSupplyDemandStateLocked(dst *supplyDemandState) {
	dst.NextCampaignID = s.nextCampaignID
	dst.NextAdvertiserID = s.nextAdvertiserID
	dst.NextRuleID = s.nextRuleID
	dst.NextSupplyTagID = s.nextSupplyTagID
	dst.NextDemandEndpointID = s.nextDemandEndpointID
	dst.NextDemandVastTagID = s.nextDemandVastTagID
	dst.NextMappingID = s.nextMappingID

	dst.Campaigns = make([]Campaign, 0, len(s.campaigns))
	for _, campaign := range s.campaigns {
		dst.Campaigns = append(dst.Campaigns, *campaign)
	}
	sort.Slice(dst.Campaigns, func(i, j int) bool { return dst.Campaigns[i].ID < dst.Campaigns[j].ID })

	dst.Advertisers = make([]Advertiser, 0, len(s.advertisers))
	for _, advertiser := range s.advertisers {
		dst.Advertisers = append(dst.Advertisers, *advertiser)
	}
	sort.Slice(dst.Advertisers, func(i, j int) bool { return dst.Advertisers[i].ID < dst.Advertisers[j].ID })

	dst.TargetingRules = make([]TargetingRule, 0, len(s.targetingRules))
	for _, rule := range s.targetingRules {
		dst.TargetingRules = append(dst.TargetingRules, *rule)
	}
	sort.Slice(dst.TargetingRules, func(i, j int) bool { return dst.TargetingRules[i].ID < dst.TargetingRules[j].ID })

	dst.SupplyTags = make([]SupplyTag, 0, len(s.supplyTags))
	for _, tag := range s.supplyTags {
		dst.SupplyTags = append(dst.SupplyTags, *tag)
	}
	sort.Slice(dst.SupplyTags, func(i, j int) bool { return dst.SupplyTags[i].ID < dst.SupplyTags[j].ID })

	dst.DemandEndpoints = make([]DemandEndpoint, 0, len(s.demandEndpoints))
	for _, endpoint := range s.demandEndpoints {
		dst.DemandEndpoints = append(dst.DemandEndpoints, *endpoint)
	}
	sort.Slice(dst.DemandEndpoints, func(i, j int) bool { return dst.DemandEndpoints[i].ID < dst.DemandEndpoints[j].ID })

	dst.DemandVastTags = make([]DemandVastTag, 0, len(s.demandVastTags))
	for _, vastTag := range s.demandVastTags {
		dst.DemandVastTags = append(dst.DemandVastTags, *vastTag)
	}
	sort.Slice(dst.DemandVastTags, func(i, j int) bool { return dst.DemandVastTags[i].ID < dst.DemandVastTags[j].ID })

	dst.Mappings = make([]SDMapping, 0, len(s.mappings))
	for _, mapping := range s.mappings {
		dst.Mappings = append(dst.Mappings, *mapping)
	}
	sort.Slice(dst.Mappings, func(i, j int) bool { return dst.Mappings[i].ID < dst.Mappings[j].ID })

	dst.Analytics = s.snapshotPersistedAnalyticsLocked()
}

func (s *store) registerPersistedDemandAdapters(reg *adapter.Registry) {
	if reg == nil {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, endpoint := range s.demandEndpoints {
		if endpoint.Status != 1 || endpoint.URL == "" {
			continue
		}
		if err := httputil.ValidateDemandURL(endpoint.URL); err != nil {
			log.Printf("Skipping persisted demand endpoint %d (%s): %v", endpoint.ID, endpoint.Name, err)
			continue
		}

		adapterID := fmt.Sprintf("demand-ep-%d", endpoint.ID)
		acfg := buildDemandEndpointAdapterConfig(adapterID, endpoint)

		if !adapter.RegisterFromConfig(reg, acfg) {
			log.Printf("Skipping persisted demand endpoint %d (%s): unsupported integration %q", endpoint.ID, endpoint.Name, endpoint.Integration)
		}
	}

	for _, demandVastTag := range s.demandVastTags {
		if demandVastTag.Status != 1 || demandVastTag.URL == "" {
			continue
		}
		if err := httputil.ValidateDemandURL(demandVastTag.URL); err != nil {
			log.Printf("Skipping persisted demand VAST tag %d (%s): %v", demandVastTag.ID, demandVastTag.Name, err)
			continue
		}

		adapterID := fmt.Sprintf("demand-vast-%d", demandVastTag.ID)
		acfg := buildDemandVASTAdapterConfig(adapterID, demandVastTag)
		if !adapter.RegisterFromConfig(reg, acfg) {
			log.Printf("Skipping persisted demand VAST tag %d (%s): unsupported adapter type", demandVastTag.ID, demandVastTag.Name)
		}
	}
}

// rebuildSupplyIndexLocked rebuilds active supply-tag lookup indexes.
// Caller must hold s.mu Lock/RLock as appropriate.
func (s *store) rebuildSupplyIndexLocked() {
	s.supplyBySlotID = make(map[string]*SupplyTag, len(s.supplyTags))
	s.supplyBySlotIDNorm = make(map[string]*SupplyTag, len(s.supplyTags))
	s.supplyByName = make(map[string]*SupplyTag, len(s.supplyTags))
	s.supplyByNameNorm = make(map[string]*SupplyTag, len(s.supplyTags))
	s.supplyByIDStr = make(map[string]*SupplyTag, len(s.supplyTags))
	s.activeSupplyTagCount = 0
	for _, t := range s.supplyTags {
		if t.Status != 1 {
			continue
		}
		s.activeSupplyTagCount++
		if t.SlotID != "" {
			s.supplyBySlotID[t.SlotID] = t
			if norm := normalizeSupplyLookupKey(t.SlotID); norm != "" {
				s.supplyBySlotIDNorm[norm] = t
			}
		}
		if t.Name != "" {
			s.supplyByName[t.Name] = t
			if norm := normalizeSupplyLookupKey(t.Name); norm != "" {
				s.supplyByNameNorm[norm] = t
			}
		}
		s.supplyByIDStr[strconv.Itoa(t.ID)] = t
	}
}

// rebuildMappingIndexLocked rebuilds active mapping indexes by supply id.
// Caller must hold s.mu Lock/RLock as appropriate.
func (s *store) rebuildMappingIndexLocked() {
	s.mappingsBySID = make(map[int][]*SDMapping)
	s.mappingAdapterIDsBySID = make(map[int][]string)
	for _, m := range s.mappings {
		if m.Status != 1 {
			continue
		}
		s.mappingsBySID[m.SupplyID] = append(s.mappingsBySID[m.SupplyID], m)
		if adapterID, ok := mappingAdapterIDForTypeAndDemandID(m.Type, m.DemandID); ok {
			s.mappingAdapterIDsBySID[m.SupplyID] = append(s.mappingAdapterIDsBySID[m.SupplyID], adapterID)
		}
	}

	for sid, ids := range s.mappingAdapterIDsBySID {
		if len(ids) <= 1 {
			continue
		}
		seen := make(map[string]struct{}, len(ids))
		deduped := make([]string, 0, len(ids))
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			deduped = append(deduped, id)
		}
		s.mappingAdapterIDsBySID[sid] = deduped
	}
}

func (s *store) hasActiveSupplyTags() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeSupplyTagCount > 0
}

func (s *store) mappedAdapterIDsForSupplyID(sid int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.mappingAdapterIDsBySID[sid]
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, len(ids))
	copy(out, ids)
	return out
}

func mappingAdapterIDForTypeAndDemandID(mappingType string, demandID int) (string, bool) {
	if demandID <= 0 {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(mappingType)) {
	case "ortb":
		return "demand-ep-" + strconv.Itoa(demandID), true
	case "vast":
		return "demand-vast-" + strconv.Itoa(demandID), true
	default:
		return "", false
	}
}

func (s *store) rebuildCampaignIndexLocked() {
	s.activeCampaignByDomain = make(map[string]*Campaign)
	s.activeCampaignFallback = nil

	for _, campaign := range s.campaigns {
		if campaign.Status != 1 {
			continue
		}

		domains := splitDomains(campaign.ADomain)
		if len(domains) == 0 {
			if s.activeCampaignFallback == nil || campaign.ID < s.activeCampaignFallback.ID {
				s.activeCampaignFallback = campaign
			}
			continue
		}

		for _, domain := range domains {
			if existing, ok := s.activeCampaignByDomain[domain]; !ok || campaign.ID < existing.ID {
				s.activeCampaignByDomain[domain] = campaign
			}
		}
	}
}

// lookupActiveSupplyTagByKey checks whether a tag name/slot_id/id matches
// any active registered supply source.
func normalizeSupplyLookupKey(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func (s *store) lookupActiveSupplyTagByKey(tagKey string) *SupplyTag {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := strings.TrimSpace(tagKey)
	if key == "" {
		return nil
	}

	if t, ok := s.supplyBySlotID[key]; ok {
		return t
	}
	if t, ok := s.supplyByName[key]; ok {
		return t
	}
	if t, ok := s.supplyByIDStr[key]; ok {
		return t
	}

	norm := normalizeSupplyLookupKey(key)
	if t, ok := s.supplyBySlotIDNorm[norm]; ok {
		return t
	}
	if t, ok := s.supplyByNameNorm[norm]; ok {
		return t
	}
	if t, ok := s.supplyByIDStr[norm]; ok {
		return t
	}
	return nil
}

func isSyntheticBundleValue(bundle string) bool {
	b := strings.ToLower(strings.TrimSpace(bundle))
	if b == "" || b == "app.unknown" {
		return true
	}
	return strings.HasPrefix(b, "supply.")
}

// enrichFromSupplyTag overrides the BidRequest fields with the supply tag
// configuration set in the dashboard. The dashboard config is the source of
// truth — it always overrides the query-param defaults.
func enrichFromSupplyTag(req *openrtb.BidRequest, tag *SupplyTag) {
	if tag == nil || len(req.Imp) == 0 {
		return
	}
	if req.Device == nil {
		req.Device = &openrtb.Device{}
	}
	// Floor: use tag floor (override default $5)
	if tag.Floor > 0 {
		req.Imp[0].BidFloor = tag.Floor
	}
	// Dimensions
	if tag.Width > 0 && req.Imp[0].Video != nil {
		w := int64(tag.Width)
		req.Imp[0].Video.W = &w
	}
	if tag.Height > 0 && req.Imp[0].Video != nil {
		h := int64(tag.Height)
		req.Imp[0].Video.H = &h
	}
	// Duration
	if tag.MinDur > 0 && req.Imp[0].Video != nil {
		req.Imp[0].Video.MinDuration = int64(tag.MinDur)
	}
	if tag.MaxDur > 0 && req.Imp[0].Video != nil {
		req.Imp[0].Video.MaxDuration = int64(tag.MaxDur)
	}
	// Slot ID
	if tag.SlotID != "" {
		req.Imp[0].TagID = tag.SlotID
		if req.App != nil {
			if req.App.Publisher == nil {
				req.App.Publisher = &openrtb.Publisher{}
			}
			req.App.Publisher.ID = tag.SlotID
		}
	}
	// Device type from supply tag env
	if tag.DeviceType > 0 {
		req.Device.DeviceType = adcom1.DeviceType(tag.DeviceType)
	}
	// Country code (convert to alpha-3)
	if tag.CountryCode != "" {
		cc := tag.CountryCode
		if len(cc) == 2 {
			cc = openrtb.ToAlpha3(cc)
		}
		if req.Device.Geo != nil {
			req.Device.Geo.Country = cc
		} else {
			req.Device.Geo = &openrtb.Geo{Country: cc, Type: adcom1.LocationType(2)}
		}
	}
	// App fields: runtime values from the publisher's request take priority.
	// Supply tag config is used as a fallback when the publisher didn't send a
	// clean canonical app identifier.
	if req.App != nil {
		if tag.AppBundle != "" && openrtb.CleanBundleValue(req.App.Bundle, req.App.ID, req.App.StoreURL) == "" {
			req.App.Bundle = tag.AppBundle
			req.App.ID = tag.AppBundle
		}
		if tag.AppName != "" && req.App.Name == "" {
			req.App.Name = tag.AppName
		}
		if tag.Domain != "" && req.App.StoreURL == "" {
			req.App.StoreURL = tag.Domain
		}
		if tag.ContentGenre != "" {
			cats := strings.Split(tag.ContentGenre, ",")
			req.App.Cat = cats
			// Enrich Content object for better DSP targeting
			if req.App.Content == nil {
				req.App.Content = &openrtb.Content{}
			}
			req.App.Content.Genre = tag.ContentGenre
			req.App.Content.Cat = cats
		}
	}
	// Content language
	if tag.ContentLang != "" {
		req.Device.Language = tag.ContentLang
		if req.App != nil {
			if req.App.Content == nil {
				req.App.Content = &openrtb.Content{}
			}
			req.App.Content.Language = tag.ContentLang
		}
	}
}

func normalizeDomainValue(v string) string {
	v = strings.TrimSpace(strings.ToLower(v))
	v = strings.TrimPrefix(v, "http://")
	v = strings.TrimPrefix(v, "https://")
	v = strings.TrimPrefix(v, "www.")
	if i := strings.IndexByte(v, '/'); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v)
}

func splitDomains(raw string) []string {
	tokens := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '|' || r == ' ' || r == '\t' || r == '\n'
	})
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		n := normalizeDomainValue(token)
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

func domainMatchesCampaign(campaignDomainsRaw, winnerDomain string) bool {
	winnerDomain = normalizeDomainValue(winnerDomain)
	if winnerDomain == "" {
		return false
	}
	for _, campaignDomain := range splitDomains(campaignDomainsRaw) {
		if campaignDomain == winnerDomain {
			return true
		}
	}
	return false
}

func utcDayProgress() float64 {
	now := time.Now().UTC()
	seconds := now.Hour()*3600 + now.Minute()*60 + now.Second()
	return float64(seconds+1) / 86400.0
}

func buildDeliveryUserKey(req *openrtb.BidRequest) string {
	if req == nil {
		return "anonymous"
	}
	if req.User != nil && strings.TrimSpace(req.User.ID) != "" {
		return "uid:" + strings.TrimSpace(req.User.ID)
	}
	if req.Device != nil && strings.TrimSpace(req.Device.IFA) != "" {
		return "ifa:" + strings.TrimSpace(req.Device.IFA)
	}
	if req.Device != nil && strings.TrimSpace(req.Device.IP) != "" {
		bundle := ""
		if req.App != nil {
			bundle = strings.TrimSpace(req.App.Bundle)
		}
		if bundle != "" {
			return "ip:" + strings.TrimSpace(req.Device.IP) + "|bundle:" + bundle
		}
		return "ip:" + strings.TrimSpace(req.Device.IP)
	}
	return "anonymous"
}

func (s *store) resetDailyDeliveryStateLocked() {
	today := time.Now().UTC().Format("2006-01-02")
	if s.budgetDayKey == today {
		return
	}
	s.budgetDayKey = today
	s.frequencyByKey = make(map[string]int)
	for _, campaign := range s.campaigns {
		campaign.SpentToday = 0
	}
}

func (s *store) campaignFrequencyUsedLocked(campaignID int) int {
	if campaignID <= 0 {
		return 0
	}
	prefix := strconv.Itoa(campaignID) + "|"
	total := 0
	for key, count := range s.frequencyByKey {
		if strings.HasPrefix(key, prefix) {
			total += count
		}
	}
	return total
}

func (s *store) selectCampaignForWinnerLocked(winner *openrtb.Bid) *Campaign {
	winnerDomain := ""
	if winner != nil && len(winner.ADomain) > 0 {
		winnerDomain = normalizeDomainValue(winner.ADomain[0])
	}
	if winnerDomain != "" {
		if matched, ok := s.activeCampaignByDomain[winnerDomain]; ok {
			return matched
		}
	}
	return s.activeCampaignFallback
}

func (s *store) reserveCampaignDelivery(req *openrtb.BidRequest, winner *openrtb.Bid, winPrice float64) (int, string, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()

	s.resetDailyDeliveryStateLocked()

	campaign := s.selectCampaignForWinnerLocked(winner)
	if campaign == nil {
		return 0, "", "served_unmanaged", true
	}

	spendDelta := 0.0
	if winner != nil {
		spendDelta = winner.ReportingPrice(winPrice) / 1000.0
	}
	if spendDelta < 0 {
		spendDelta = 0
	}

	if campaign.BudgetDaily > 0 && campaign.SpentToday+spendDelta > campaign.BudgetDaily {
		return campaign.ID, campaign.Name, "blocked_budget_daily", false
	}
	if campaign.BudgetTotal > 0 && campaign.SpentTotal+spendDelta > campaign.BudgetTotal {
		return campaign.ID, campaign.Name, "blocked_budget_total", false
	}

	if campaign.PacingEnabled && campaign.BudgetDaily > 0 {
		allowedByNow := (campaign.BudgetDaily * utcDayProgress()) + (campaign.BudgetDaily * 0.10)
		if campaign.SpentToday+spendDelta > allowedByNow {
			return campaign.ID, campaign.Name, "blocked_pacing", false
		}
	}

	if campaign.FrequencyCap > 0 {
		userKey := buildDeliveryUserKey(req)
		freqKey := strconv.Itoa(campaign.ID) + "|" + userKey
		if s.frequencyByKey[freqKey] >= campaign.FrequencyCap {
			return campaign.ID, campaign.Name, "blocked_frequency_cap", false
		}
		s.frequencyByKey[freqKey]++
	}

	campaign.SpentToday += spendDelta
	campaign.SpentTotal += spendDelta

	return campaign.ID, campaign.Name, "served", true
}

func winnerPrimaryDomain(winner *openrtb.Bid) string {
	if winner == nil || len(winner.ADomain) == 0 {
		return ""
	}
	return winner.ADomain[0]
}

func requestEnvironment(req *openrtb.BidRequest) string {
	if req == nil || req.Device == nil {
		return "CTV"
	}
	switch int(req.Device.DeviceType) {
	case 1, 4, 5:
		return "Mobile"
	case 2:
		return "Desktop"
	case 7:
		return "STB"
	default:
		return "CTV"
	}
}

func decisionAuditSource(req *openrtb.BidRequest, tag *SupplyTag, fallback string) string {
	if tag != nil {
		if tag.ID > 0 {
			return strconv.Itoa(tag.ID)
		}
		if slotID := strings.TrimSpace(tag.SlotID); slotID != "" {
			return slotID
		}
		if name := strings.TrimSpace(tag.Name); name != "" {
			return name
		}
	}
	if req != nil {
		if len(req.Imp) > 0 {
			if tagID := strings.TrimSpace(req.Imp[0].TagID); tagID != "" {
				return tagID
			}
		}
		if req.App != nil && req.App.Publisher != nil {
			if publisherID := strings.TrimSpace(req.App.Publisher.ID); publisherID != "" {
				return publisherID
			}
		}
	}
	return strings.TrimSpace(fallback)
}

func decisionAuditBundle(req *openrtb.BidRequest, tag *SupplyTag) string {
	if req == nil || req.App == nil {
		if tag == nil {
			return ""
		}
		if bundle := openrtb.CanonicalBundleValue(tag.AppBundle); bundle != "" {
			return bundle
		}
		return openrtb.BundleFromStoreURL(tag.Domain)
	}

	if bundle := openrtb.CanonicalBundleValue(req.App.Bundle); bundle != "" {
		return bundle
	}
	if tag != nil {
		if bundle := openrtb.CanonicalBundleValue(tag.AppBundle); bundle != "" {
			return bundle
		}
	}
	if bundle := openrtb.BundleFromStoreURL(req.App.StoreURL); bundle != "" {
		return bundle
	}
	if bundle := openrtb.CanonicalBundleValue(req.App.ID); bundle != "" {
		return bundle
	}
	if tag != nil {
		return openrtb.BundleFromStoreURL(tag.Domain)
	}
	return ""
}

func decisionAuditRawBundle(req *openrtb.BidRequest) string {
	if req == nil || req.App == nil {
		return ""
	}
	if bundle := strings.TrimSpace(req.App.Bundle); bundle != "" {
		return bundle
	}
	return strings.TrimSpace(req.App.ID)
}

func (s *store) recordAdDecision(req *openrtb.BidRequest, winner *openrtb.Bid, winPrice float64, supplyID int, source, appBundle, demandEp string, campaignID int, campaignName, deliveryStatus string) {
	country := ""
	if req != nil && req.Device != nil && req.Device.Geo != nil {
		country = req.Device.Geo.Country
	}
	rawBundle := decisionAuditRawBundle(req)
	demandSource := strings.TrimSpace(demandEp)
	if demandSource == "" && winner != nil {
		demandSource = strings.TrimSpace(winner.DemandSrc)
		if demandSource == "" {
			demandSource = strings.TrimSpace(winner.Seat)
		}
	}
	adomain := ""
	if winner != nil && len(winner.ADomain) > 0 {
		adomain = winner.ADomain[0]
	}
	devType := "CTV"
	switch {
	case req == nil || req.Device == nil:
		devType = "CTV"
	case int(req.Device.DeviceType) == 1:
		devType = "Mobile"
	case int(req.Device.DeviceType) == 2:
		devType = "Desktop"
	case int(req.Device.DeviceType) == 4:
		devType = "Phone"
	case int(req.Device.DeviceType) == 5:
		devType = "Tablet"
	case int(req.Device.DeviceType) == 7:
		devType = "STB"
	}

	netPrice := 0.0
	if winner != nil {
		netPrice = winner.ReportingPrice(winPrice)
	}

	creativeID := ""
	seat := ""
	bidPrice := 0.0
	grossPrice := 0.0
	if winner != nil {
		creativeID = winner.CrID
		seat = winner.Seat
		bidPrice = winner.Price
		grossPrice = winPrice
	}

	s.decisionMu.Lock()
	s.adDecisions = append(s.adDecisions, AdDecision{
		Time: time.Now(), CampaignID: campaignID, Campaign: campaignName,
		CreativeID: creativeID, SupplyID: supplyID, Source: source,
		ADomain: adomain, Seat: seat,
		BidPrice: bidPrice, GrossPrice: grossPrice, NetPrice: netPrice,
		AdmType: "vast", AppBundle: appBundle, RawBundle: rawBundle, Country: country, DeviceType: devType,
		DemandEp: demandSource, Delivery: deliveryStatus,
	})
	if len(s.adDecisions) > 500 {
		s.adDecisions = s.adDecisions[len(s.adDecisions)-500:]
	}
	s.decisionMu.Unlock()

	s.mu.Lock()
	s.recordPersistedAnalyticsLocked(AdDecision{
		SupplyID:   supplyID,
		Source:     source,
		ADomain:    adomain,
		Seat:       seat,
		BidPrice:   bidPrice,
		GrossPrice: grossPrice,
		NetPrice:   netPrice,
		DemandEp:   demandSource,
		AppBundle:  appBundle,
		RawBundle:  rawBundle,
		CreativeID: creativeID,
	})
	s.stateGeneration.Add(1)
	s.mu.Unlock()
	s.scheduleDeferredStatePersist()
}

func normalizeDemandTimeoutMs(timeoutMs int) int {
	if timeoutMs <= 0 {
		return 500
	}
	return timeoutMs
}

func normalizeDemandIntegration(integration string) adapter.AdapterType {
	if strings.EqualFold(strings.TrimSpace(integration), string(adapter.TypeVAST)) {
		return adapter.TypeVAST
	}
	return adapter.TypeORTB
}

func buildDemandEndpointAdapterConfig(adapterID string, endpoint *DemandEndpoint) *adapter.AdapterConfig {
	if endpoint == nil {
		return nil
	}

	return &adapter.AdapterConfig{
		ID: adapterID, Name: endpoint.Name,
		Type:        normalizeDemandIntegration(endpoint.Integration),
		ORTBVersion: endpoint.OrtbVersion,
		Endpoint:    endpoint.URL, TimeoutMs: normalizeDemandTimeoutMs(endpoint.Timeout),
		Floor: endpoint.Floor, Margin: endpoint.Margin,
		QPSLimit: endpoint.QPS, Status: endpoint.Status,
		GZIPSupport:   endpoint.GZIPSupport,
		RemovePChain:  endpoint.RemovePChain,
		SChainEnabled: endpoint.SupplyChain,
		BAdv:          endpoint.BAdv,
		BCat:          endpoint.BCat,
	}
}

func buildDemandVASTAdapterConfig(adapterID string, tag *DemandVastTag) *adapter.AdapterConfig {
	if tag == nil {
		return nil
	}

	return &adapter.AdapterConfig{
		ID: adapterID, Name: tag.Name,
		Type:     adapter.TypeVAST,
		Endpoint: tag.URL, TimeoutMs: normalizeDemandTimeoutMs(0),
		Floor: tag.Floor, Margin: tag.Margin,
		Status: tag.Status,
	}
}

// ── Router ──

// EnterpriseDeps holds optional enterprise-grade pipeline dependencies.
// When non-nil, the pipeline handler is used instead of the legacy vastHandler.
type EnterpriseDeps struct {
	Pipeline    *pipeline.Pipeline
	Registry    *adapter.Registry
	FloorEngine *floor.Engine
	AQScanner   *adquality.Scanner
}

func NewRouterWithDeps(cfg *config.Config, metrics *monitor.Metrics, configPath string, eDeps *EnterpriseDeps) *fiber.App {
	app := fiber.New(fiber.Config{BodyLimit: 4 * 1024 * 1024})
	s := newStore()
	statePath := resolveSupplyDemandStatePath(configPath)
	if err := s.loadSupplyDemandState(statePath); err != nil {
		log.Printf("Warning: failed to load runtime state (%s): %v", statePath, err)
	}
	if eDeps != nil {
		s.registerPersistedDemandAdapters(eDeps.Registry)
	}

	// Professional SSP middleware stack
	app.Use(CORS())
	app.Use(SecurityHeaders())
	app.Use(RequestID())

	if metrics == nil {
		metrics = monitor.New()
	}
	dashPath := "dashboard.html"
	if cfg != nil {
		dashPath = cfg.Server.DashboardPath
	}

	// Set VAST builder base URL for tracking callbacks
	if cfg != nil && cfg.Server.PublicBaseURL != "" {
		vast.BaseURL = cfg.Server.PublicBaseURL
	} else {
		port := ":8080"
		if cfg != nil && cfg.Server.Port != "" {
			port = cfg.Server.Port
		}
		vast.BaseURL = "http://localhost" + port
	}

	// ─── Health ───
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "uptime": time.Since(metrics.StartTime).String()})
	})

	// ─── Dashboard ───
	app.Get("/", func(c *fiber.Ctx) error { return c.SendFile(dashPath) })
	app.Get("/dashboard", func(c *fiber.Ctx) error { return c.SendFile(dashPath) })
	app.Get("/favicon.ico", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusNoContent) })

	// ─── VAST Serving Endpoint ───
	if eDeps != nil && eDeps.Pipeline != nil {
		app.Get("/vast/:tag", pipelineHandler(eDeps.Pipeline, metrics, s))
		app.Get("/api/v1/vast/tag", pipelineHandler(eDeps.Pipeline, metrics, s))
		app.Get("/api/vast", supplyTagVastHandler(eDeps.Pipeline, metrics, s))
	}

	// ─── VAST Event Tracking Callbacks ───
	registerEventRoutes(app, metrics)

	// ─── Auth: Login & Password Management ───
	registerAuthRoutes(app, s)

	// ─── Admin: Campaigns CRUD ───
	auth := AdminAPIKey(s)
	registerCampaignRoutes(app, s, auth)

	// ─── Admin: Advertisers CRUD ───
	registerAdvertiserRoutes(app, s, auth)

	// ─── Admin: Targeting Rules ───
	registerTargetingRoutes(app, s, auth)

	// ─── Analytics & Reporting ───
	registerAnalyticsRoutes(app, s, metrics)

	// ─── Settings ───
	registerSettingsRoutes(app, cfg, configPath, auth)

	// ─── Supply-Demand Management ───
	registerSupplyDemandRoutes(app, s, eDeps, auth)

	// ─── Enterprise: Deal/Floor/AdQuality Management ───
	if eDeps != nil {
		registerFloorRoutes(app, eDeps.FloorEngine, auth)
		registerAdQualityRoutes(app, eDeps.AQScanner, auth)
		registerAdapterRoutes(app, eDeps.Registry, auth)
	}

	// ─── Metrics (Prometheus-style) ───
	app.Get("/metrics", func(c *fiber.Ctx) error {
		o := metrics.GetOverview()
		return c.Type("text").SendString(fmt.Sprintf(
			"# SSP Metrics\nssp_ad_requests_total %d\nssp_impressions_total %d\nssp_completions_total %d\nssp_spend_total %.2f\nssp_errors_total %d\nssp_adapter_errors_total %d\nssp_no_bids_total %d\nssp_wins_total %d\nssp_losses_total %d\nssp_vast_starts_total %d\nssp_vast_errors_total %d\nssp_avg_bid_latency_ms %.1f\n",
			o.AdRequests, o.Impressions, o.Completions, o.TotalSpend, o.Errors, o.AdapterErrors, o.NoBids,
			o.BidWins, o.BidLosses, o.VastStarts, o.VastErrors, o.AvgBidLatency,
		))
	})

	return app
}

// ── VAST Handler ──

// ── VAST Event Tracking Callbacks ──

func registerEventRoutes(app *fiber.App, metrics *monitor.Metrics) {
	evt := app.Group("/api/v1/event")

	type vastEvent struct {
		path      string
		eventType string
		recorder  func()
		detail    string // fmt template for Details; "" = use "bid=%s"
	}

	events := []vastEvent{
		{"/impression", "vast_impression", metrics.RecordImpression, "cmp=%s crid=%s price=%s"},
		{"/start", "vast_start", metrics.RecordVastStart, ""},
		{"/firstQuartile", "vast_q1", metrics.RecordVastQ1, ""},
		{"/midpoint", "vast_mid", metrics.RecordVastMid, ""},
		{"/thirdQuartile", "vast_q3", metrics.RecordVastQ3, ""},
		{"/complete", "vast_complete", metrics.RecordCompletion, ""},
		{"/skip", "vast_skip", metrics.RecordVastSkip, ""},
		{"/mute", "vast_mute", nil, ""},
		{"/unmute", "vast_unmute", nil, ""},
		{"/pause", "vast_pause", nil, ""},
		{"/resume", "vast_resume", nil, ""},
		{"/fullscreen", "vast_fullscreen", nil, ""},
		{"/error", "vast_error", metrics.RecordVastError, "code=%s"},
	}

	for _, e := range events {
		handler := e // capture for closure
		evt.Get(handler.path, func(c *fiber.Ctx) error {
			if handler.recorder != nil {
				handler.recorder()
			}

			if handler.eventType == "vast_impression" {
				bidID := strings.TrimSpace(c.Query("bid"))
				if bidID != "" {
					auction.FireBillingNoticeByBidID(bidID)
				}
			}
			var details string
			switch handler.detail {
			case "cmp=%s crid=%s price=%s":
				details = fmt.Sprintf("cmp=%s crid=%s price=%s", c.Query("cmp"), c.Query("crid"), c.Query("price"))
			case "code=%s":
				details = fmt.Sprintf("code=%s", c.Query("code"))
			default:
				details = fmt.Sprintf("bid=%s", c.Query("bid"))
			}

			env := c.Query("env", "ctv")
			metrics.AddTrafficEvent(monitor.TrafficEvent{
				Type:      handler.eventType,
				RequestID: c.Query("rid"),
				Env:       env,
				Details:   details,
				Campaign:  c.Query("cmp"),
				Creative:  c.Query("crid"),
				Country:   c.Query("ctry"),
				IP:        c.Query("ip"),
				Supply:    c.Query("sr"),
				Bundle:    openrtb.CleanBundleValue(c.Query("bndl"), "", ""),
				ADomain:   c.Query("adom"),
				Price:     c.Query("price"),
			})
			return c.SendStatus(204)
		})
	}
}

// ── Campaign Routes ──

func registerCampaignRoutes(app *fiber.App, s *store, auth fiber.Handler) {
	g := app.Group("/api/v1/admin/campaigns", auth)

	g.Get("/", func(c *fiber.Ctx) error {
		s.mu.RLock()
		s.deliveryMu.RLock()
		defer s.deliveryMu.RUnlock()
		defer s.mu.RUnlock()
		out := make([]Campaign, 0, len(s.campaigns))
		for _, camp := range s.campaigns {
			out = append(out, *camp)
		}
		return c.JSON(out)
	})

	g.Get("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.RLock()
		camp, ok := s.campaigns[id]
		if !ok {
			s.mu.RUnlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		s.deliveryMu.RLock()
		campSnapshot := *camp
		s.deliveryMu.RUnlock()
		s.mu.RUnlock()
		return c.JSON(campSnapshot)
	})

	g.Post("/", func(c *fiber.Ctx) error {
		var camp Campaign
		if err := c.BodyParser(&camp); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		camp.ID = s.nextCampaignID
		s.nextCampaignID++
		if camp.Status == 0 {
			camp.Status = 1
		}
		if camp.BudgetDaily > 0 && !camp.PacingEnabled {
			camp.PacingEnabled = true
		}
		if camp.FrequencyCap < 0 {
			camp.FrequencyCap = 0
		}
		s.campaigns[camp.ID] = &camp
		s.rebuildCampaignIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.Status(201).JSON(camp)
	})

	g.Put("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		var update Campaign
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		var present map[string]json.RawMessage
		_ = json.Unmarshal(c.Body(), &present)
		if _, ok := present["frequency_cap"]; ok && update.FrequencyCap < 0 {
			return c.Status(400).JSON(fiber.Map{"error": "frequency_cap must be >= 0"})
		}

		s.mu.Lock()
		camp, ok := s.campaigns[id]
		if !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		if update.Name != "" {
			camp.Name = update.Name
		}
		if update.Status != 0 {
			camp.Status = update.Status
		}
		if update.Bid != 0 {
			camp.Bid = update.Bid
		}
		if update.BidFloor != 0 {
			camp.BidFloor = update.BidFloor
		}
		if update.BudgetDaily != 0 {
			camp.BudgetDaily = update.BudgetDaily
		}
		if update.ADomain != "" {
			camp.ADomain = update.ADomain
		}
		if _, ok := present["budget_total"]; ok {
			camp.BudgetTotal = update.BudgetTotal
		}
		if _, ok := present["spent_today"]; ok {
			camp.SpentToday = update.SpentToday
		}
		if _, ok := present["spent_total"]; ok {
			camp.SpentTotal = update.SpentTotal
		}
		if _, ok := present["frequency_cap"]; ok {
			camp.FrequencyCap = update.FrequencyCap
		}
		if _, ok := present["pacing_enabled"]; ok {
			camp.PacingEnabled = update.PacingEnabled
		}

		s.rebuildCampaignIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		campSnapshot := *camp
		s.mu.Unlock()

		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(campSnapshot)
	})

	g.Patch("/:id/status", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		var body struct {
			Status int `json:"status"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		camp, ok := s.campaigns[id]
		if !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		camp.Status = body.Status
		s.rebuildCampaignIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		campSnapshot := *camp
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(campSnapshot)
	})

	g.Delete("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		if _, ok := s.campaigns[id]; !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.campaigns, id)
		s.rebuildCampaignIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(fiber.Map{"deleted": id})
	})
}

// ── Advertiser Routes ──

func registerAdvertiserRoutes(app *fiber.App, s *store, auth fiber.Handler) {
	g := app.Group("/api/v1/admin/advertisers", auth)

	g.Get("/", func(c *fiber.Ctx) error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]Advertiser, 0, len(s.advertisers))
		for _, a := range s.advertisers {
			out = append(out, *a)
		}
		return c.JSON(out)
	})

	g.Get("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		a, ok := s.advertisers[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		return c.JSON(a)
	})

	g.Post("/", func(c *fiber.Ctx) error {
		var a Advertiser
		if err := c.BodyParser(&a); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		a.ID = s.nextAdvertiserID
		s.nextAdvertiserID++
		if a.Status == 0 {
			a.Status = 1
		}
		s.advertisers[a.ID] = &a
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.Status(201).JSON(a)
	})

	g.Put("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		var update Advertiser
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		a, ok := s.advertisers[id]
		if !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		if update.Name != "" {
			a.Name = update.Name
		}
		if update.Company != "" {
			a.Company = update.Company
		}
		if update.Email != "" {
			a.Email = update.Email
		}
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		aSnapshot := *a
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(aSnapshot)
	})

	g.Delete("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		if _, ok := s.advertisers[id]; !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.advertisers, id)
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(fiber.Map{"deleted": id})
	})
}

// ── Targeting Routes ──

func registerTargetingRoutes(app *fiber.App, s *store, auth fiber.Handler) {
	g := app.Group("/api/v1/admin/campaigns", auth)

	g.Get("/:id/targeting", func(c *fiber.Ctx) error {
		campID, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		rules := make([]TargetingRule, 0)
		for _, r := range s.targetingRules {
			if r.CampaignID == campID {
				rules = append(rules, *r)
			}
		}
		return c.JSON(rules)
	})

	g.Post("/:id/targeting", func(c *fiber.Ctx) error {
		campID, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		var rule TargetingRule
		if err := c.BodyParser(&rule); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		rule.ID = s.nextRuleID
		s.nextRuleID++
		rule.CampaignID = campID
		s.targetingRules[rule.ID] = &rule
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.Status(201).JSON(rule)
	})

	g2 := app.Group("/api/v1/admin/targeting", auth)
	g2.Delete("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		if _, ok := s.targetingRules[id]; !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.targetingRules, id)
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(fiber.Map{"deleted": id})
	})
}

// ── Analytics & Reporting Routes ──

func registerAnalyticsRoutes(app *fiber.App, s *store, metrics *monitor.Metrics) {
	app.Get("/api/v1/analytics/overview", func(c *fiber.Ctx) error {
		return c.JSON(buildAnalyticsOverview(metrics.GetOverview()))
	})

	app.Get("/api/v1/analytics/campaigns", func(c *fiber.Ctx) error {
		s.mu.RLock()
		s.deliveryMu.RLock()
		defer s.deliveryMu.RUnlock()
		defer s.mu.RUnlock()
		out := make([]Campaign, 0, len(s.campaigns))
		for _, camp := range s.campaigns {
			out = append(out, *camp)
		}
		return c.JSON(out)
	})

	app.Get("/api/v1/analytics/campaign/:id/today", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		cm := metrics.GetCampaignMetric(id)
		cm.SpendMu.Lock()
		spend := cm.Spend
		cm.SpendMu.Unlock()
		return c.JSON(fiber.Map{
			"campaign_id": id, "requests": cm.Requests.Load(), "opportunities": cm.Opps.Load(),
			"impressions": cm.Impressions.Load(), "completions": cm.Completions.Load(),
			"clicks": cm.Clicks.Load(), "spend": spend,
		})
	})

	app.Get("/api/v1/analytics/campaign/:id/budget", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.RLock()
		s.deliveryMu.Lock()
		s.resetDailyDeliveryStateLocked()
		camp, ok := s.campaigns[id]
		var campSnapshot Campaign
		frequencyUsed := 0
		if ok {
			campSnapshot = *camp
			frequencyUsed = s.campaignFrequencyUsedLocked(id)
		}
		s.deliveryMu.Unlock()
		s.mu.RUnlock()
		if !ok {
			return c.JSON(fiber.Map{
				"budget_daily":      0,
				"budget_total":      0,
				"spent_today":       0,
				"spent_total":       0,
				"remaining_daily":   0,
				"remaining_total":   0,
				"frequency_cap":     0,
				"frequency_used":    0,
				"pacing_enabled":    false,
				"pacing_target_now": 0,
			})
		}

		remainingDaily := campSnapshot.BudgetDaily - campSnapshot.SpentToday
		if remainingDaily < 0 {
			remainingDaily = 0
		}
		remainingTotal := campSnapshot.BudgetTotal - campSnapshot.SpentTotal
		if remainingTotal < 0 {
			remainingTotal = 0
		}
		pacingTargetNow := 0.0
		if campSnapshot.PacingEnabled && campSnapshot.BudgetDaily > 0 {
			pacingTargetNow = (campSnapshot.BudgetDaily * utcDayProgress()) + (campSnapshot.BudgetDaily * 0.10)
		}

		return c.JSON(fiber.Map{
			"budget_daily":      campSnapshot.BudgetDaily,
			"budget_total":      campSnapshot.BudgetTotal,
			"spent_today":       campSnapshot.SpentToday,
			"spent_total":       campSnapshot.SpentTotal,
			"remaining_daily":   remainingDaily,
			"remaining_total":   remainingTotal,
			"frequency_cap":     campSnapshot.FrequencyCap,
			"frequency_used":    frequencyUsed,
			"pacing_enabled":    campSnapshot.PacingEnabled,
			"pacing_target_now": pacingTargetNow,
		})
	})

	app.Get("/api/v1/analytics/campaign/:id/realtime", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		cm := metrics.GetCampaignMetric(id)
		cm.SpendMu.Lock()
		spend := cm.Spend
		cm.SpendMu.Unlock()

		s.mu.RLock()
		s.deliveryMu.Lock()
		s.resetDailyDeliveryStateLocked()
		camp := s.campaigns[id]
		spentToday := 0.0
		spentTotal := 0.0
		if camp != nil {
			spentToday = camp.SpentToday
			spentTotal = camp.SpentTotal
		}
		frequencyUsed := s.campaignFrequencyUsedLocked(id)
		s.deliveryMu.Unlock()
		s.mu.RUnlock()

		return c.JSON(fiber.Map{
			"campaign_id": id, "requests": cm.Requests.Load(), "impressions": cm.Impressions.Load(),
			"completions": cm.Completions.Load(), "spend": spend,
			"spent_today": spentToday, "spent_total": spentTotal, "frequency_used": frequencyUsed,
		})
	})

	app.Post("/api/v1/analytics/flush", func(c *fiber.Ctx) error {
		if err := s.writeSupplyDemandState(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to flush runtime state"})
		}
		return c.JSON(fiber.Map{"flushed": true, "state_path": s.statePath})
	})

	app.Get("/api/v1/analytics/reports/demand", func(c *fiber.Ctx) error {
		return c.JSON(buildDemandReport(s.snapshotAnalyticsState()))
	})

	app.Get("/api/v1/analytics/reports/demand-totals", func(c *fiber.Ctx) error {
		return c.JSON(buildDemandTotalsReport(s.snapshotAnalyticsState()))
	})

	app.Get("/api/v1/analytics/reports/supply", func(c *fiber.Ctx) error {
		return c.JSON(buildSupplyReport(s.snapshotAnalyticsState()))
	})

	app.Get("/api/v1/analytics/reports/bundles", func(c *fiber.Ctx) error {
		return c.JSON(buildBundleReport(s.snapshotAnalyticsState()))
	})

	app.Get("/api/v1/analytics/reports/delivery-health", func(c *fiber.Ctx) error {
		h := metrics.GetDeliveryHealth()
		return c.JSON(fiber.Map{
			"impressions": h.Impressions,
			"starts":      h.Starts,
			"q1":          h.Q1,
			"mid":         h.Mid,
			"q3":          h.Q3,
			"completions": h.Completions,
			"skips":       h.Skips,
			"errors":      h.Errors,
			"start_rate":  h.StartRate,
			"vtr":         h.VTR,
			"skip_rate":   h.SkipRate,
			"error_rate":  h.ErrorRate,
			"by_campaign": make([]fiber.Map, 0),
		})
	})

	app.Get("/api/v1/analytics/reports/vast-errors", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"total_errors": metrics.VastErrors.Load(),
			"total_events": metrics.VastStarts.Load() + metrics.Completions.Load(),
			"by_campaign":  make([]fiber.Map, 0),
		})
	})

	app.Get("/api/v1/analytics/reports/error-reasons", func(c *fiber.Ctx) error {
		limit := c.QueryInt("limit", 20)
		return c.JSON(fiber.Map{
			"internal": metrics.ErrorReasonCounts(limit),
			"adapter":  metrics.AdapterErrorReasonCounts(limit),
		})
	})

	app.Get("/api/v1/analytics/reports/no-bid-reasons", func(c *fiber.Ctx) error {
		limit := c.QueryInt("limit", 20)
		return c.JSON(metrics.NoBidReasonCounts(limit))
	})

	app.Get("/api/v1/analytics/reports/creative", func(c *fiber.Ctx) error {
		return c.JSON(make([]fiber.Map, 0))
	})

	app.Get("/api/v1/analytics/reports/decisions", func(c *fiber.Ctx) error {
		s.decisionMu.RLock()
		limit := c.QueryInt("limit", 100)
		out := append([]AdDecision(nil), s.adDecisions...)
		s.decisionMu.RUnlock()
		if len(out) > limit {
			out = out[len(out)-limit:]
		}
		if out == nil {
			out = make([]AdDecision, 0)
		}
		for i := range out {
			out[i].AppBundle = openrtb.CanonicalBundleValue(out[i].AppBundle)
		}
		return c.JSON(out)
	})

	app.Get("/api/v1/analytics/traffic/live", func(c *fiber.Ctx) error {
		filterType := c.Query("type")
		events := metrics.GetTrafficEvents(filterType)
		if events == nil {
			events = make([]monitor.TrafficEvent, 0)
		}
		for i := range events {
			events[i].Bundle = openrtb.CanonicalBundleValue(events[i].Bundle)
		}
		return c.JSON(events)
	})
}

// ── Auth Routes ──

func registerAuthRoutes(app *fiber.App, s *store) {
	app.Get("/api/v1/auth/status", func(c *fiber.Ctx) error {
		required := adminAPIKeyRequired()
		authenticated := !required

		if required && s != nil {
			token := strings.TrimSpace(c.Cookies(dashboardSessionCookieName))
			if token != "" && s.validateDashboardSession(token) {
				authenticated = true
			}
		}
		if !authenticated && requestHasValidAdminAPIKey(c) {
			authenticated = true
		}

		return c.JSON(fiber.Map{
			"required":      required,
			"authenticated": authenticated,
		})
	})

	// Login — validates username + password, returns success/failure
	app.Post("/api/v1/auth/login", func(c *fiber.Ctx) error {
		var body struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.RLock()
		user := s.dashboardUser
		pass := s.dashboardPass
		s.mu.RUnlock()

		userOK := subtle.ConstantTimeCompare([]byte(body.Username), []byte(user)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(body.Password), []byte(pass)) == 1
		if !userOK || !passOK {
			return c.Status(401).JSON(fiber.Map{"error": "Invalid username or password"})
		}

		token, err := s.createDashboardSession()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to create dashboard session"})
		}

		secureCookie := strings.EqualFold(c.Protocol(), "https")
		c.Cookie(&fiber.Cookie{
			Name:     dashboardSessionCookieName,
			Value:    token,
			HTTPOnly: true,
			SameSite: "Lax",
			Secure:   secureCookie,
			Path:     "/",
			MaxAge:   86400,
			Expires:  time.Now().Add(24 * time.Hour),
		})

		return c.JSON(fiber.Map{"success": true, "user": user})
	})

	app.Post("/api/v1/auth/logout", func(c *fiber.Ctx) error {
		token := c.Cookies(dashboardSessionCookieName)
		s.clearDashboardSession(token)
		c.Cookie(&fiber.Cookie{
			Name:     dashboardSessionCookieName,
			Value:    "",
			HTTPOnly: true,
			SameSite: "Lax",
			Secure:   strings.EqualFold(c.Protocol(), "https"),
			Path:     "/",
			MaxAge:   -1,
			Expires:  time.Unix(0, 0),
		})
		return c.JSON(fiber.Map{"success": true})
	})

	// Change password — requires current password for verification
	passwordHandler := func(c *fiber.Ctx) error {
		var body struct {
			CurrentPassword string `json:"current_password"`
			NewUsername     string `json:"new_username"`
			NewPassword     string `json:"new_password"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		if body.NewPassword == "" {
			return c.Status(400).JSON(fiber.Map{"error": "New password is required"})
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		if subtle.ConstantTimeCompare([]byte(body.CurrentPassword), []byte(s.dashboardPass)) != 1 {
			return c.Status(401).JSON(fiber.Map{"error": "Current password is incorrect"})
		}

		if body.NewUsername != "" {
			s.dashboardUser = body.NewUsername
		}
		s.dashboardPass = body.NewPassword
		s.dashboardSessions = make(map[string]time.Time)
		return c.JSON(fiber.Map{"success": true, "message": "Credentials updated"})
	}

	app.Put("/api/v1/auth/password", passwordHandler)
	app.Post("/api/v1/auth/password", passwordHandler)
}

// ── Settings Routes ──

func registerSettingsRoutes(app *fiber.App, cfg *config.Config, configPath string, auth fiber.Handler) {
	g := app.Group("/api/v1/settings", auth)

	g.Get("/", func(c *fiber.Ctx) error {
		if cfg == nil {
			return c.JSON(fiber.Map{})
		}
		return c.JSON(cfg)
	})

	g.Put("/", func(c *fiber.Ctx) error {
		if cfg == nil {
			return c.Status(400).JSON(fiber.Map{"error": "No config loaded"})
		}
		var update config.Config
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		cfg.Server = update.Server
		if len(update.Adapters) > 0 {
			cfg.Adapters = update.Adapters
			cfg.Bidders = nil
		} else if len(update.Bidders) > 0 {
			cfg.Adapters = config.LegacyBiddersToAdapters(update.Bidders)
			cfg.Bidders = nil
		}
		if configPath != "" {
			if err := cfg.Save(configPath); err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Failed to save"})
			}
		}
		return c.JSON(cfg)
	})

	g.Post("/restart", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "restart_requested"})
	})
}

// ── Supply-Demand CRUD Routes ──

func registerSupplyDemandRoutes(app *fiber.App, s *store, eDeps *EnterpriseDeps, auth fiber.Handler) {
	sd := app.Group("/api/v1/supply-demand", auth)

	// Supply Tags
	sd.Get("/supply-tags", func(c *fiber.Ctx) error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]SupplyTag, 0, len(s.supplyTags))
		for _, t := range s.supplyTags {
			out = append(out, *t)
		}
		return c.JSON(out)
	})

	sd.Get("/supply-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.RLock()
		defer s.mu.RUnlock()
		t, ok := s.supplyTags[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		return c.JSON(t)
	})

	sd.Post("/supply-tags", func(c *fiber.Ctx) error {
		var t SupplyTag
		if err := c.BodyParser(&t); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		t.ID = s.nextSupplyTagID
		s.nextSupplyTagID++
		if t.Status == 0 {
			t.Status = 1
		}
		t.VastURL = generateVastTagURL(&t)
		s.supplyTags[t.ID] = &t
		s.rebuildSupplyIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.Status(201).JSON(t)
	})

	sd.Put("/supply-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		var update SupplyTag
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		t, ok := s.supplyTags[id]
		if !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		if update.Name != "" {
			t.Name = update.Name
		}
		if update.Status != 0 {
			t.Status = update.Status
		}
		if update.Floor != 0 {
			t.Floor = update.Floor
		}
		if update.SlotID != "" {
			t.SlotID = update.SlotID
		}
		if update.Integration != "" {
			t.Integration = update.Integration
		}
		if update.Pricing != "" {
			t.Pricing = update.Pricing
		}
		if update.Margin != 0 {
			t.Margin = update.Margin
		}
		if update.Env != "" {
			t.Env = update.Env
		}
		if update.MinDur != 0 {
			t.MinDur = update.MinDur
		}
		if update.MaxDur != 0 {
			t.MaxDur = update.MaxDur
		}
		if update.Width != 0 {
			t.Width = update.Width
		}
		if update.Height != 0 {
			t.Height = update.Height
		}
		if update.Channel != "" {
			t.Channel = update.Channel
		}
		if update.CountryCode != "" {
			t.CountryCode = update.CountryCode
		}
		if update.ContentGenre != "" {
			t.ContentGenre = update.ContentGenre
		}
		if update.ContentLang != "" {
			t.ContentLang = update.ContentLang
		}
		if update.DeviceType != 0 {
			t.DeviceType = update.DeviceType
		}
		if update.AppName != "" {
			t.AppName = update.AppName
		}
		if update.AppBundle != "" {
			t.AppBundle = update.AppBundle
		}
		if update.Domain != "" {
			t.Domain = update.Domain
		}
		t.Sensitive = update.Sensitive
		t.VastURL = generateVastTagURL(t)
		s.rebuildSupplyIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		tSnapshot := *t
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(tSnapshot)
	})

	sd.Delete("/supply-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		if _, ok := s.supplyTags[id]; !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.supplyTags, id)
		s.rebuildSupplyIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(fiber.Map{"deleted": id})
	})

	// Supply Tag VAST URL Generator
	sd.Get("/supply-tags/:id/vast-url", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.RLock()
		t, ok := s.supplyTags[id]
		s.mu.RUnlock()
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		w := t.Width
		if w == 0 {
			w = 1920
		}
		h := t.Height
		if h == 0 {
			h = 1080
		}
		minDur := t.MinDur
		if minDur == 0 {
			minDur = 5
		}
		maxDur := t.MaxDur
		if maxDur == 0 {
			maxDur = 30
		}
		// Use the request's own protocol+host so the URL matches the real domain
		relativeURL := generateVastTagURL(t)
		vastURL := c.BaseURL() + relativeURL

		return c.JSON(fiber.Map{
			"supply_tag_id":   t.ID,
			"supply_tag_name": t.Name,
			"vast_url":        vastURL,
			"width":           w,
			"height":          h,
			"min_duration":    minDur,
			"max_duration":    maxDur,
			"floor":           t.Floor,
			"environment":     t.Env,
		})
	})

	// Demand Endpoints
	sd.Get("/demand-endpoints", func(c *fiber.Ctx) error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]DemandEndpoint, 0, len(s.demandEndpoints))
		for _, e := range s.demandEndpoints {
			out = append(out, *e)
		}
		return c.JSON(out)
	})

	sd.Get("/demand-endpoints/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.RLock()
		defer s.mu.RUnlock()
		e, ok := s.demandEndpoints[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		return c.JSON(e)
	})

	sd.Post("/demand-endpoints", func(c *fiber.Ctx) error {
		var e DemandEndpoint
		if err := c.BodyParser(&e); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		if e.URL != "" {
			if err := httputil.ValidateDemandURL(e.URL); err != nil {
				return c.Status(400).JSON(fiber.Map{"error": "Invalid endpoint URL: " + err.Error()})
			}
		}
		e.Timeout = normalizeDemandTimeoutMs(e.Timeout)
		s.mu.Lock()
		e.ID = s.nextDemandEndpointID
		s.nextDemandEndpointID++
		if e.Status == 0 {
			e.Status = 1
		}
		s.demandEndpoints[e.ID] = &e
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}

		// Live-register as enterprise adapter for real-time parallel bidding
		if eDeps != nil && eDeps.Registry != nil && e.Status == 1 && e.URL != "" {
			adapterID := fmt.Sprintf("demand-ep-%d", e.ID)
			acfg := buildDemandEndpointAdapterConfig(adapterID, &e)
			if !adapter.RegisterFromConfig(eDeps.Registry, acfg) {
				log.Printf("Skipping demand endpoint %d (%s): unsupported integration %q", e.ID, e.Name, e.Integration)
			}
		}

		return c.Status(201).JSON(e)
	})

	sd.Put("/demand-endpoints/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		var update DemandEndpoint
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}

		if update.URL != "" {
			if err := httputil.ValidateDemandURL(update.URL); err != nil {
				return c.Status(400).JSON(fiber.Map{"error": "Invalid endpoint URL: " + err.Error()})
			}
		}

		s.mu.Lock()
		e, ok := s.demandEndpoints[id]
		if !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		if update.Name != "" {
			e.Name = update.Name
		}
		if update.URL != "" {
			e.URL = update.URL
		}
		if update.Status != 0 {
			e.Status = update.Status
		}
		if update.Floor != 0 {
			e.Floor = update.Floor
		}
		if update.Margin != 0 {
			e.Margin = update.Margin
		}
		if update.Timeout != 0 {
			e.Timeout = normalizeDemandTimeoutMs(update.Timeout)
		}
		if update.QPS != 0 {
			e.QPS = update.QPS
		}
		if update.Integration != "" {
			e.Integration = update.Integration
		}
		if update.OrtbVersion != "" {
			e.OrtbVersion = update.OrtbVersion
		}
		if update.AuctionType != "" {
			e.AuctionType = update.AuctionType
		}
		e.GZIPSupport = update.GZIPSupport
		e.RemovePChain = update.RemovePChain
		e.BAdv = update.BAdv
		e.BCat = update.BCat
		e.SupplyChain = update.SupplyChain
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		eSnapshot := *e
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}

		// Re-register adapter with updated config for live hot-reload
		if eDeps != nil && eDeps.Registry != nil && eSnapshot.URL != "" {
			adapterID := fmt.Sprintf("demand-ep-%d", id)
			acfg := buildDemandEndpointAdapterConfig(adapterID, &eSnapshot)
			if eSnapshot.Status == 1 {
				if !adapter.RegisterFromConfig(eDeps.Registry, acfg) {
					log.Printf("Skipping demand endpoint %d (%s): unsupported integration %q", id, eSnapshot.Name, eSnapshot.Integration)
				}
			} else {
				eDeps.Registry.Remove(adapterID)
			}
		}

		return c.JSON(eSnapshot)
	})

	sd.Delete("/demand-endpoints/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		if _, ok := s.demandEndpoints[id]; !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.demandEndpoints, id)
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		// Remove from live adapter registry
		if eDeps != nil && eDeps.Registry != nil {
			eDeps.Registry.Remove(fmt.Sprintf("demand-ep-%d", id))
		}
		return c.JSON(fiber.Map{"deleted": id})
	})

	// Demand VAST Tags
	sd.Get("/demand-vast-tags", func(c *fiber.Ctx) error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]DemandVastTag, 0, len(s.demandVastTags))
		for _, t := range s.demandVastTags {
			out = append(out, *t)
		}
		return c.JSON(out)
	})

	sd.Get("/demand-vast-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.RLock()
		defer s.mu.RUnlock()
		t, ok := s.demandVastTags[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		return c.JSON(t)
	})

	sd.Post("/demand-vast-tags", func(c *fiber.Ctx) error {
		var t DemandVastTag
		if err := c.BodyParser(&t); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		if t.URL != "" {
			if err := httputil.ValidateDemandURL(t.URL); err != nil {
				return c.Status(400).JSON(fiber.Map{"error": "Invalid VAST tag URL: " + err.Error()})
			}
		}
		s.mu.Lock()
		t.ID = s.nextDemandVastTagID
		s.nextDemandVastTagID++
		if t.Status == 0 {
			t.Status = 1
		}
		s.demandVastTags[t.ID] = &t
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}

		// Live-register VAST tag as enterprise adapter for real-time parallel bidding
		if eDeps != nil && eDeps.Registry != nil && t.Status == 1 && t.URL != "" {
			adapterID := fmt.Sprintf("demand-vast-%d", t.ID)
			acfg := buildDemandVASTAdapterConfig(adapterID, &t)
			if !adapter.RegisterFromConfig(eDeps.Registry, acfg) {
				log.Printf("Skipping demand VAST tag %d (%s): unsupported adapter type", t.ID, t.Name)
			}
		}

		return c.Status(201).JSON(t)
	})

	sd.Put("/demand-vast-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		var update DemandVastTag
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		if update.URL != "" {
			if err := httputil.ValidateDemandURL(update.URL); err != nil {
				return c.Status(400).JSON(fiber.Map{"error": "Invalid VAST tag URL: " + err.Error()})
			}
		}

		s.mu.Lock()
		t, ok := s.demandVastTags[id]
		if !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		if update.Name != "" {
			t.Name = update.Name
		}
		if update.URL != "" {
			t.URL = update.URL
		}
		if update.Status != 0 {
			t.Status = update.Status
		}
		if update.Floor != 0 {
			t.Floor = update.Floor
		}
		if update.Margin != 0 {
			t.Margin = update.Margin
		}
		if update.CPM != 0 {
			t.CPM = update.CPM
		}
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		tSnapshot := *t
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}

		// Re-register adapter with updated config for live hot-reload
		if eDeps != nil && eDeps.Registry != nil && tSnapshot.URL != "" {
			adapterID := fmt.Sprintf("demand-vast-%d", id)
			acfg := buildDemandVASTAdapterConfig(adapterID, &tSnapshot)
			if tSnapshot.Status == 1 {
				if !adapter.RegisterFromConfig(eDeps.Registry, acfg) {
					log.Printf("Skipping demand VAST tag %d (%s): unsupported adapter type", id, tSnapshot.Name)
				}
			} else {
				eDeps.Registry.Remove(adapterID)
			}
		}

		return c.JSON(tSnapshot)
	})

	sd.Delete("/demand-vast-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		if _, ok := s.demandVastTags[id]; !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.demandVastTags, id)
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		// Remove from live adapter registry
		if eDeps != nil && eDeps.Registry != nil {
			eDeps.Registry.Remove(fmt.Sprintf("demand-vast-%d", id))
		}
		return c.JSON(fiber.Map{"deleted": id})
	})

	// Supply-Demand Mappings
	sd.Get("/mappings", func(c *fiber.Ctx) error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]SDMapping, 0, len(s.mappings))
		for _, m := range s.mappings {
			out = append(out, *m)
		}
		return c.JSON(out)
	})

	sd.Post("/mappings", func(c *fiber.Ctx) error {
		var m SDMapping
		if err := c.BodyParser(&m); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		m.ID = s.nextMappingID
		s.nextMappingID++
		if m.Status == 0 {
			m.Status = 1
		}
		s.mappings[m.ID] = &m
		s.rebuildMappingIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.Status(201).JSON(m)
	})

	sd.Put("/mappings/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		var update SDMapping
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		s.mu.Lock()
		m, ok := s.mappings[id]
		if !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		if update.Priority != 0 {
			m.Priority = update.Priority
		}
		if update.Weight != 0 {
			m.Weight = update.Weight
		}
		if update.Status != 0 {
			m.Status = update.Status
		}
		s.rebuildMappingIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		mSnapshot := *m
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(mSnapshot)
	})

	sd.Delete("/mappings/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		if _, ok := s.mappings[id]; !ok {
			s.mu.Unlock()
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.mappings, id)
		s.rebuildMappingIndexLocked()
		stateWrite := s.prepareSupplyDemandStateWriteLocked()
		s.mu.Unlock()
		if err := stateWrite.Persist(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to persist runtime state"})
		}
		return c.JSON(fiber.Map{"deleted": id})
	})
}

// ── Pipeline Handler (Enterprise) ──

func handlePipelineServeResult(c *fiber.Ctx, p *pipeline.Pipeline, metrics *monitor.Metrics, s *store, req *openrtb.BidRequest, result *pipeline.Result, sourceTag *SupplyTag, decisionSource string) error {
	if result.Error != nil {
		return c.Status(500).JSON(fiber.Map{"error": result.Error.Error()})
	}

	auditSource := decisionAuditSource(req, sourceTag, decisionSource)
	auditBundle := decisionAuditBundle(req, sourceTag)
	auditSupplyID := 0
	if sourceTag != nil {
		auditSupplyID = sourceTag.ID
	}

	if result.NoBid || result.Winner == nil || strings.TrimSpace(result.VAST) == "" {
		return c.Type("xml").SendString(vast.BuildNoAd())
	}

	campaignID, campaignName, deliveryStatus, allowed := s.reserveCampaignDelivery(req, result.Winner, result.WinPrice)
	if !allowed {
		metrics.RecordNoBid()
		metrics.RecordNoBidReason("delivery_" + deliveryStatus)
		metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type:      "delivery_block",
			RequestID: req.ID,
			Env:       requestEnvironment(req),
			Details:   deliveryStatus,
			Campaign:  campaignName,
			Bundle:    auditBundle,
			ADomain:   winnerPrimaryDomain(result.Winner),
		})
		return c.Type("xml").SendString(vast.BuildNoAd())
	}

	if campaignID > 0 {
		cm := metrics.GetCampaignMetric(campaignID)
		cm.Opps.Add(1)
		cm.Impressions.Add(1)
		cm.SpendMu.Lock()
		cm.Spend += result.Winner.ReportingPrice(result.WinPrice) / 1000.0
		cm.SpendMu.Unlock()
	}

	metrics.RecordWin(result.WinPrice)
	metrics.RecordSpend(result.Winner.ReportingPrice(result.WinPrice))
	metrics.RecordGrossSpend(result.WinPrice)

	s.recordAdDecision(req, result.Winner, result.WinPrice, auditSupplyID, auditSource, auditBundle, result.AdapterID, campaignID, campaignName, deliveryStatus)
	p.FinalizeDelivery(result)

	return c.Type("xml").SendString(result.VAST)
}

// supplyTagVastHandler serves VAST ads via the /api/vast?sid=... endpoint.
// It looks up the supply tag by sid, enriches the bid request with the tag's
// config (floor, dimensions, duration), and runs it through the pipeline.
// generateVastTagURL builds the macro-templated VAST tag URL for a supply source.
func generateVastTagURL(t *SupplyTag) string {
	w := t.Width
	if w == 0 {
		w = 1920
	}
	h := t.Height
	if h == 0 {
		h = 1080
	}
	minDur := t.MinDur
	if minDur == 0 {
		minDur = 5
	}
	maxDur := t.MaxDur
	if maxDur == 0 {
		maxDur = 30
	}
	dt := t.DeviceType
	if dt == 0 {
		dt = 3
	}
	cc := t.CountryCode
	if cc == "" {
		cc = "US"
	}
	genre := t.ContentGenre
	if genre == "" {
		genre = "game,entertainment,family"
	}
	lang := t.ContentLang
	if lang == "" {
		lang = "en"
	}
	// Use relative path — the full URL is resolved at serve time using the request host
	return fmt.Sprintf("/api/vast?sid=%d&w=%d&h=%d&cb={cb}&ip={uip}&ua={ua}"+
		"&app_bundle={app_bundle}&app_name={app_name}&app_store_url={app_store_url}"+
		"&country_code=%s&max_dur=%d&min_dur=%d"+
		"&device_make={device_make}&device_model={device_model}&device_type=%d"+
		"&ct_genre=%s&ct_lang=%s&dnt=0&ifa={idfa}&os={device_os}"+
		"&us_privacy=1---",
		t.ID, w, h, cc, maxDur, minDur, dt, genre, lang)
}

func supplyTagVastHandler(p *pipeline.Pipeline, metrics *monitor.Metrics, s *store) fiber.Handler {
	return func(c *fiber.Ctx) error {
		sidStr := strings.TrimSpace(c.Query("sid"))
		if sidStr == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid VAST tag sid"})
		}

		sid, err := strconv.Atoi(sidStr)
		if err != nil || sid <= 0 {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid VAST tag sid"})
		}

		s.mu.RLock()
		tag := s.supplyTags[sid]
		s.mu.RUnlock()
		if tag == nil || tag.Status != 1 {
			return c.Status(404).JSON(fiber.Map{"error": "Invalid VAST tag sid"})
		}

		req := openrtb.BuildFromHTTP(c)
		if tag != nil {
			enrichFromSupplyTag(&req, tag)
		}

		if err := validate.Request(&req); err != nil {
			metrics.RecordError()
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		// Resolve supply-demand mappings: only send bid requests to
		// the demand sources explicitly mapped to this supply tag.
		var mappedAdapterIDs []string
		if sid > 0 {
			mappedAdapterIDs = s.mappedAdapterIDsForSupplyID(sid)
		}

		// Drop stale/unknown mapping targets. If nothing valid remains,
		// fall back to all active adapters instead of forcing empty fanout.
		if len(mappedAdapterIDs) > 0 && p.Registry != nil {
			validIDs := make([]string, 0, len(mappedAdapterIDs))
			for _, adapterID := range mappedAdapterIDs {
				cfg := p.Registry.GetConfig(adapterID)
				if cfg == nil || cfg.Status != 1 {
					continue
				}
				validIDs = append(validIDs, adapterID)
			}
			mappedAdapterIDs = validIDs
		}

		// If mappings exist, only those demand sources are called.
		// If no mappings, fan out to all active adapters (backward compat).
		var result *pipeline.Result
		if len(mappedAdapterIDs) > 0 {
			result = p.Execute(c.Context(), &req, c.BaseURL(), mappedAdapterIDs)
		} else {
			result = p.Execute(c.Context(), &req, c.BaseURL())
		}

		return handlePipelineServeResult(c, p, metrics, s, &req, result, tag, "supply_tag")
	}
}

func pipelineHandler(p *pipeline.Pipeline, metrics *monitor.Metrics, s *store) fiber.Handler {
	return func(c *fiber.Ctx) error {
		tagKey := strings.TrimSpace(c.Params("tag"))
		if tagKey == "" {
			tagKey = strings.TrimSpace(c.Query("tag"))
		}
		if tagKey == "" {
			return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source: missing tag"})
		}

		supplyTag := s.lookupActiveSupplyTagByKey(tagKey)
		if supplyTag == nil {
			return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source"})
		}

		req := openrtb.BuildFromHTTP(c)
		if supplyTag != nil {
			enrichFromSupplyTag(&req, supplyTag)
		}

		if err := validate.Request(&req); err != nil {
			metrics.RecordError()
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		result := p.Execute(c.Context(), &req, c.BaseURL())

		return handlePipelineServeResult(c, p, metrics, s, &req, result, supplyTag, "pipeline")
	}
}

// ── Enterprise: Floor Rule Management ──

func registerFloorRoutes(app *fiber.App, fe *floor.Engine, auth fiber.Handler) {
	g := app.Group("/api/v1/floors", auth)

	g.Get("/", func(c *fiber.Ctx) error {
		list := fe.ListRules()
		if list == nil {
			list = make([]*floor.Rule, 0)
		}
		return c.JSON(list)
	})

	g.Post("/", func(c *fiber.Ctx) error {
		var r floor.Rule
		if err := c.BodyParser(&r); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		if r.ID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Rule ID is required"})
		}
		if r.Status == 0 {
			r.Status = 1
		}
		fe.AddRule(&r)
		return c.Status(201).JSON(r)
	})

	g.Delete("/:id", func(c *fiber.Ctx) error {
		fe.RemoveRule(c.Params("id"))
		return c.JSON(fiber.Map{"deleted": c.Params("id")})
	})
}

// ── Enterprise: Ad Quality / Brand Safety ──

func registerAdQualityRoutes(app *fiber.App, sc *adquality.Scanner, auth fiber.Handler) {
	g := app.Group("/api/v1/adquality", auth)

	g.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(sc.GetConfig())
	})

	g.Put("/", func(c *fiber.Ctx) error {
		var cfg adquality.ScannerConfig
		if err := c.BodyParser(&cfg); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}
		sc.SetBlockedDomains(cfg.BlockedDomains)
		sc.SetBlockedCategories(cfg.BlockedCategories)
		sc.SetBlockedAttrs(cfg.BlockedAttrs)
		sc.SetBlockedAdvertisers(cfg.BlockedAdvertisers)
		sc.SetAllowedDomains(cfg.AllowedDomains)
		sc.SetAllowedCategories(cfg.AllowedCategories)
		return c.JSON(sc.GetConfig())
	})
}

// ── Enterprise: Adapter Management ──

func registerAdapterRoutes(app *fiber.App, reg *adapter.Registry, auth fiber.Handler) {
	g := app.Group("/api/v1/adapters", auth)

	g.Get("/", func(c *fiber.Ctx) error {
		list := reg.List()
		if list == nil {
			list = make([]adapter.AdapterInfo, 0)
		}
		return c.JSON(list)
	})

	g.Delete("/:id", func(c *fiber.Ctx) error {
		reg.Remove(c.Params("id"))
		return c.JSON(fiber.Map{"deleted": c.Params("id")})
	})
}
