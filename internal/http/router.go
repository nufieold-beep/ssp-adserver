package http

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"ssp/internal/adapter"
	"ssp/internal/adquality"
	"ssp/internal/auction"
	"ssp/internal/bidder"
	"ssp/internal/config"
	"ssp/internal/floor"
	"ssp/internal/httputil"
	"ssp/internal/monitor"
	"ssp/internal/openrtb"
	"ssp/internal/pipeline"
	"ssp/internal/validate"
	"ssp/internal/vast"

	"github.com/gofiber/fiber/v2"
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
	ID            int      `json:"id"`
	Name          string   `json:"name"`
	URL           string   `json:"endpoint_url"`
	Integration   string   `json:"integration"`
	OrtbVersion   string   `json:"ortb_version"`
	AuctionType   string   `json:"auction_type"`
	Floor         float64  `json:"floor"`
	Timeout       int      `json:"timeout_ms"`
	QPS           int      `json:"qps_limit"`
	Sensitive     bool     `json:"sensitive"`
	Margin        float64  `json:"margin"`
	Status        int      `json:"status"`
	GZIPSupport   bool     `json:"gzip_support"`
	RemovePChain  bool     `json:"remove_pchain"`
	BAdv          []string `json:"badv"`
	BCat          []string `json:"bcat"`
	SupplyChain   bool     `json:"schain_enabled"`
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
	CreativeID string    `json:"creative_id"`
	Source     string    `json:"source"`
	ADomain    string    `json:"adomain"`
	ADSource   string    `json:"ad_source"`
	BidPrice   float64   `json:"bid_price"`
	NetPrice   float64   `json:"net_price"`
	Seat       string    `json:"seat"`
	AdmType    string    `json:"adm_type"`
	DemandEp   string    `json:"demand_endpoint"`
	AppBundle  string    `json:"app_bundle"`
	Country    string    `json:"country"`
	DeviceType string    `json:"device_type"`
}

// store holds all in-memory state.
type store struct {
	mu sync.RWMutex

	campaigns      map[int]*Campaign
	nextCampaignID int

	advertisers      map[int]*Advertiser
	nextAdvertiserID int

	supplyTags      map[int]*SupplyTag
	nextSupplyTagID int
	supplyBySlotID  map[string]*SupplyTag
	supplyByName    map[string]*SupplyTag
	supplyByIDStr   map[string]*SupplyTag

	demandEndpoints      map[int]*DemandEndpoint
	nextDemandEndpointID int

	demandVastTags      map[int]*DemandVastTag
	nextDemandVastTagID int

	mappings      map[int]*SDMapping
	nextMappingID int
	mappingsBySID map[int][]*SDMapping

	targetingRules map[int]*TargetingRule
	nextRuleID     int

	adDecisions []AdDecision

	dashboardUser string
	dashboardPass string
}

func newStore() *store {
	return &store{
		campaigns:            map[int]*Campaign{1: {ID: 1, Name: "CTV Demo", Status: 1, Bid: 7.0, BidFloor: 5.0, BudgetDaily: 100, Env: "1"}, 2: {ID: 2, Name: "InApp Test", Status: 0, Bid: 5.0, BidFloor: 3.0, BudgetDaily: 50, Env: "2"}},
		nextCampaignID:       3,
		advertisers:          map[int]*Advertiser{1: {ID: 1, Name: "Demo Advertiser", Company: "Demo Inc", Status: 1}},
		nextAdvertiserID:     2,
		supplyTags:           make(map[int]*SupplyTag),
		nextSupplyTagID:      1,
		supplyBySlotID:       make(map[string]*SupplyTag),
		supplyByName:         make(map[string]*SupplyTag),
		supplyByIDStr:        make(map[string]*SupplyTag),
		demandEndpoints:      make(map[int]*DemandEndpoint),
		nextDemandEndpointID: 1,
		demandVastTags:       make(map[int]*DemandVastTag),
		nextDemandVastTagID:  1,
		mappings:             make(map[int]*SDMapping),
		nextMappingID:        1,
		mappingsBySID:        make(map[int][]*SDMapping),
		targetingRules:       make(map[int]*TargetingRule),
		nextRuleID:           1,
		dashboardUser:        "admin",
		dashboardPass:        "admin",
	}
}

// rebuildSupplyIndexLocked rebuilds active supply-tag lookup indexes.
// Caller must hold s.mu Lock/RLock as appropriate.
func (s *store) rebuildSupplyIndexLocked() {
	s.supplyBySlotID = make(map[string]*SupplyTag, len(s.supplyTags))
	s.supplyByName = make(map[string]*SupplyTag, len(s.supplyTags))
	s.supplyByIDStr = make(map[string]*SupplyTag, len(s.supplyTags))
	for _, t := range s.supplyTags {
		if t.Status != 1 {
			continue
		}
		if t.SlotID != "" {
			s.supplyBySlotID[t.SlotID] = t
		}
		if t.Name != "" {
			s.supplyByName[t.Name] = t
		}
		s.supplyByIDStr[strconv.Itoa(t.ID)] = t
	}
}

// rebuildMappingIndexLocked rebuilds active mapping indexes by supply id.
// Caller must hold s.mu Lock/RLock as appropriate.
func (s *store) rebuildMappingIndexLocked() {
	s.mappingsBySID = make(map[int][]*SDMapping)
	for _, m := range s.mappings {
		if m.Status != 1 {
			continue
		}
		s.mappingsBySID[m.SupplyID] = append(s.mappingsBySID[m.SupplyID], m)
	}
}

// lookupSupplyByTag checks whether a tag name/slot_id matches any registered supply source.
// Returns the matching SupplyTag or nil if unknown.
func (s *store) lookupSupplyByTag(tag string) *SupplyTag {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t, ok := s.supplyBySlotID[tag]; ok {
		return t
	}
	if t, ok := s.supplyByName[tag]; ok {
		return t
	}
	if t, ok := s.supplyByIDStr[tag]; ok {
		return t
	}
	return nil
}

// enrichFromSupplyTag overrides the BidRequest fields with the supply tag
// configuration set in the dashboard. The dashboard config is the source of
// truth — it always overrides the query-param defaults.
func enrichFromSupplyTag(req *openrtb.BidRequest, tag *SupplyTag) {
	if tag == nil || len(req.Imp) == 0 {
		return
	}
	// Floor: use tag floor (override default $5)
	if tag.Floor > 0 {
		req.Imp[0].BidFloor = tag.Floor
	}
	// Dimensions
	if tag.Width > 0 && req.Imp[0].Video != nil {
		req.Imp[0].Video.W = tag.Width
	}
	if tag.Height > 0 && req.Imp[0].Video != nil {
		req.Imp[0].Video.H = tag.Height
	}
	// Duration
	if tag.MinDur > 0 && req.Imp[0].Video != nil {
		req.Imp[0].Video.MinDuration = tag.MinDur
	}
	if tag.MaxDur > 0 && req.Imp[0].Video != nil {
		req.Imp[0].Video.MaxDuration = tag.MaxDur
	}
	// Slot ID
	if tag.SlotID != "" {
		req.Imp[0].TagID = tag.SlotID
	}
	// Device type from supply tag env
	if tag.DeviceType > 0 {
		req.Device.DeviceType = tag.DeviceType
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
			req.Device.Geo = &openrtb.Geo{Country: cc, Type: 2}
		}
	}
	// App fields: runtime values from the publisher's request take priority.
	// Supply tag config is used as a fallback when the publisher didn't send a value.
	if req.App != nil {
		if tag.AppBundle != "" && req.App.Bundle == "" {
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

func (s *store) recordAdDecision(req *openrtb.BidRequest, winner *openrtb.Bid, winPrice float64, source, demandEp string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	country := ""
	if req.Device.Geo != nil {
		country = req.Device.Geo.Country
	}
	appBundle := ""
	if req.App != nil {
		appBundle = req.App.Bundle
	}
	adomain := ""
	if len(winner.ADomain) > 0 {
		adomain = winner.ADomain[0]
	}
	devType := "CTV"
	switch req.Device.DeviceType {
	case 1:
		devType = "Mobile"
	case 2:
		devType = "Desktop"
	case 4:
		devType = "Phone"
	case 5:
		devType = "Tablet"
	case 7:
		devType = "STB"
	}

	s.adDecisions = append(s.adDecisions, AdDecision{
		Time: time.Now(), CreativeID: winner.CrID, Source: source,
		ADomain: adomain, Seat: winner.Seat,
		BidPrice: winner.Price, NetPrice: winPrice * 0.85,
		AdmType: "vast", AppBundle: appBundle, Country: country, DeviceType: devType,
		DemandEp: demandEp,
	})
	if len(s.adDecisions) > 500 {
		s.adDecisions = s.adDecisions[len(s.adDecisions)-500:]
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

func NewRouter() *fiber.App {
	return NewRouterWithDeps(nil, nil, nil, "", nil)
}

func NewRouterWithDeps(cfg *config.Config, mgr *bidder.Manager, metrics *monitor.Metrics, configPath string, eDeps *EnterpriseDeps) *fiber.App {
	app := fiber.New(fiber.Config{BodyLimit: 4 * 1024 * 1024})
	s := newStore()

	// Professional SSP middleware stack
	app.Use(CORS())
	app.Use(SecurityHeaders())
	app.Use(RequestID())

	if mgr == nil {
		mgr = bidder.NewManager()
	}
	if metrics == nil {
		metrics = monitor.New()
	}
	dashPath := "dashboard.html"
	auctionType := "first_price"
	if cfg != nil {
		dashPath = cfg.Server.DashboardPath
		if cfg.Server.AuctionType != "" {
			auctionType = cfg.Server.AuctionType
		}
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

	// ─── VAST Serving Endpoint ───
	// If enterprise pipeline is configured, use it; otherwise fall back to legacy handler.
	if eDeps != nil && eDeps.Pipeline != nil {
		app.Get("/vast/:tag", pipelineHandler(eDeps.Pipeline, metrics, s))
		app.Get("/api/v1/vast/tag", pipelineHandler(eDeps.Pipeline, metrics, s))
		app.Get("/api/vast", supplyTagVastHandler(eDeps.Pipeline, metrics, s))
	} else {
		app.Get("/vast/:tag", vastHandler(mgr, metrics, s, auctionType))
		app.Get("/api/v1/vast/tag", vastHandler(mgr, metrics, s, auctionType))
	}

	// ─── VAST Event Tracking Callbacks ───
	registerEventRoutes(app, metrics)

	// ─── Auth: Login & Password Management ───
	registerAuthRoutes(app, s)

	// ─── Admin: Campaigns CRUD ───
	auth := AdminAPIKey()
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
			"# SSP Metrics\nssp_ad_requests_total %d\nssp_impressions_total %d\nssp_completions_total %d\nssp_spend_total %.2f\nssp_errors_total %d\nssp_no_bids_total %d\nssp_wins_total %d\nssp_losses_total %d\nssp_vast_starts_total %d\nssp_vast_errors_total %d\nssp_avg_bid_latency_ms %.1f\n",
			o.AdRequests, o.Impressions, o.Completions, o.TotalSpend, o.Errors, o.NoBids,
			o.BidWins, o.BidLosses, o.VastStarts, o.VastErrors, o.AvgBidLatency,
		))
	})

	return app
}

// ── VAST Handler ──

func vastHandler(mgr *bidder.Manager, metrics *monitor.Metrics, s *store, auctionType string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Validate that the tag belongs to a registered supply source
		tag := c.Params("tag")
		if tag == "" {
			tag = c.Query("tag")
		}
		var supplyTag *SupplyTag
		if tag != "" {
			supplyTag = s.lookupSupplyByTag(tag)
			if supplyTag == nil {
				return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source"})
			}
		} else {
			// No tag provided — reject as unknown
			return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source"})
		}

		metrics.RecordAdRequest()
		metrics.RecordAdOpp()

		req := openrtb.BuildFromHTTP(c)

		// Enrich request with supply tag config (floors, dimensions, app info)
		enrichFromSupplyTag(&req, supplyTag)

		// Validate request per PDF spec section 6
		if err := validate.Request(&req); err != nil {
			metrics.RecordError()
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type: "ortb_request", RequestID: req.ID, Env: "CTV",
			Details: fmt.Sprintf("bundle=%s tag=%s", req.App.Bundle, req.Imp[0].TagID),
		})

		bidStart := time.Now()
		bids := mgr.CallAll(req)
		bidLatency := float64(time.Since(bidStart).Milliseconds())
		metrics.RecordBidLatency(bidLatency)

		if len(bids) == 0 {
			metrics.RecordNoBid()
			metrics.AddTrafficEvent(monitor.TrafficEvent{
				Type: "no_bid", RequestID: req.ID, Env: "CTV",
				Details: "no bids received from DSPs",
			})
			return c.Type("xml").SendString(vast.BuildNoAd())
		}

		// Run auction (first_price or second_price per config)
		result := auction.Run(bids, req.Imp[0].BidFloor, auctionType)
		if result.Winner == nil {
			metrics.RecordNoBid()
			return c.Type("xml").SendString(vast.BuildNoAd())
		}

		winner := result.Winner

		// Fire nurl (win notice) to winning DSP asynchronously
		auction.FireWinNotice(winner)

		// Fire lurl (loss notice) to all losing DSPs
		for i := range result.Losers {
			auction.FireLossNotice(&result.Losers[i])
			metrics.RecordLoss()
		}

		// Build VAST XML with real tracking URLs and burl as impression pixel
		xml := vast.Build(winner, &req, c.BaseURL())
		if xml == "" {
			metrics.RecordError()
			return c.Status(500).JSON(fiber.Map{"error": "Failed to build VAST"})
		}

		// Record win metrics (impression counted on client-side pixel fire)
		metrics.RecordWin(result.WinPrice)
		metrics.RecordSpend(result.WinPrice * 0.85) // Record Net Revenue
		metrics.RecordVastStart()

		metrics.AddTrafficEvent(monitor.TrafficEvent{
			Type: "ortb_response", RequestID: req.ID, Env: "CTV",
			Details: fmt.Sprintf("winner=%s price=%.2f clear=%.2f type=%s nurl=%v burl=%v",
				winner.ID, winner.Price, result.WinPrice, auctionType,
				winner.NURL != "", winner.BURL != ""),
		})

		// Record ad decision
		s.recordAdDecision(&req, winner, result.WinPrice, "ortb", winner.DemandSrc)

		return c.Type("xml").SendString(xml)
	}
}

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
				Bundle:    c.Query("bndl"),
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
		defer s.mu.RUnlock()
		camp, ok := s.campaigns[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		return c.JSON(camp)
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
		s.campaigns[camp.ID] = &camp
		s.mu.Unlock()
		return c.Status(201).JSON(camp)
	})

	g.Put("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		camp, ok := s.campaigns[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		var update Campaign
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
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
		return c.JSON(camp)
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
		defer s.mu.Unlock()
		camp, ok := s.campaigns[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		camp.Status = body.Status
		return c.JSON(camp)
	})

	g.Delete("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.campaigns[id]; !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.campaigns, id)
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
		s.mu.Unlock()
		return c.Status(201).JSON(a)
	})

	g.Put("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		a, ok := s.advertisers[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		var update Advertiser
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
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
		return c.JSON(a)
	})

	g.Delete("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.advertisers[id]; !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.advertisers, id)
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
		s.mu.Unlock()
		return c.Status(201).JSON(rule)
	})

	g2 := app.Group("/api/v1/admin/targeting", auth)
	g2.Delete("/:id", func(c *fiber.Ctx) error {
		id, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid ID"})
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.targetingRules[id]; !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.targetingRules, id)
		return c.JSON(fiber.Map{"deleted": id})
	})
}

// ── Analytics & Reporting Routes ──

func registerAnalyticsRoutes(app *fiber.App, s *store, metrics *monitor.Metrics) {
	app.Get("/api/v1/analytics/overview", func(c *fiber.Ctx) error {
		return c.JSON(metrics.GetOverview())
	})

	app.Get("/api/v1/analytics/campaigns", func(c *fiber.Ctx) error {
		s.mu.RLock()
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
		camp, ok := s.campaigns[id]
		s.mu.RUnlock()
		if !ok {
			return c.JSON(fiber.Map{"budget_daily": 0, "spent_today": 0, "pacing": 0})
		}
		return c.JSON(fiber.Map{"budget_daily": camp.BudgetDaily, "budget_total": camp.BudgetTotal, "spent_today": camp.SpentToday})
	})

	app.Get("/api/v1/analytics/campaign/:id/realtime", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		cm := metrics.GetCampaignMetric(id)
		cm.SpendMu.Lock()
		spend := cm.Spend
		cm.SpendMu.Unlock()
		return c.JSON(fiber.Map{
			"campaign_id": id, "requests": cm.Requests.Load(), "impressions": cm.Impressions.Load(),
			"completions": cm.Completions.Load(), "spend": spend,
		})
	})

	app.Post("/api/v1/analytics/flush", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"flushed": true})
	})

	app.Get("/api/v1/analytics/reports/demand", func(c *fiber.Ctx) error {
		s.mu.RLock()
		defer s.mu.RUnlock()

		type agg struct {
			DemandID   string
			ADomain    string
			CreativeID string
			Imps       int
			NetRev     float64
			GrossRev   float64
		}
		groups := make(map[string]*agg)
		for _, d := range s.adDecisions {
			key := d.DemandEp + "|" + d.ADomain + "|" + d.CreativeID
			if groups[key] == nil {
				groups[key] = &agg{DemandID: d.DemandEp, ADomain: d.ADomain, CreativeID: d.CreativeID}
			}
			groups[key].Imps++
			groups[key].NetRev += d.NetPrice / 1000.0
			groups[key].GrossRev += d.BidPrice / 1000.0
		}

		var rows []fiber.Map
		for _, g := range groups {
			ecpm := 0.0
			if g.Imps > 0 {
				ecpm = (g.NetRev / float64(g.Imps)) * 1000.0
			}
			gross := 0.0
			if g.Imps > 0 {
				gross = (g.GrossRev / float64(g.Imps)) * 1000.0
			}
			rows = append(rows, fiber.Map{
				"adomain": g.ADomain, "demand_id": g.DemandID, "creative_id": g.CreativeID,
				"impressions": g.Imps, "revenue": g.NetRev, "ecpm": ecpm,
				"gross_cpm": gross,
			})
		}
		return c.JSON(rows)
	})

	app.Get("/api/v1/analytics/reports/supply", func(c *fiber.Ctx) error {
		return c.JSON([]fiber.Map{})
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

	app.Get("/api/v1/analytics/reports/creative", func(c *fiber.Ctx) error {
		return c.JSON(make([]fiber.Map, 0))
	})

	app.Get("/api/v1/analytics/reports/decisions", func(c *fiber.Ctx) error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		limit := c.QueryInt("limit", 100)
		out := s.adDecisions
		if len(out) > limit {
			out = out[len(out)-limit:]
		}
		if out == nil {
			out = make([]AdDecision, 0)
		}
		return c.JSON(out)
	})

	app.Get("/api/v1/analytics/traffic/live", func(c *fiber.Ctx) error {
		filterType := c.Query("type")
		events := metrics.GetTrafficEvents(filterType)
		if events == nil {
			events = make([]monitor.TrafficEvent, 0)
		}
		return c.JSON(events)
	})
}

// ── Auth Routes ──

func registerAuthRoutes(app *fiber.App, s *store) {
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
		return c.JSON(fiber.Map{"success": true, "user": user})
	})

	// Change password — requires current password for verification
	app.Put("/api/v1/auth/password", func(c *fiber.Ctx) error {
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
		return c.JSON(fiber.Map{"success": true, "message": "Credentials updated"})
	})
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
		if len(update.Bidders) > 0 {
			cfg.Bidders = update.Bidders
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
		s.mu.Unlock()
		return c.Status(201).JSON(t)
	})

	sd.Put("/supply-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		t, ok := s.supplyTags[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		var update SupplyTag
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
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
		return c.JSON(t)
	})

	sd.Delete("/supply-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.supplyTags[id]; !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.supplyTags, id)
		s.rebuildSupplyIndexLocked()
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
		deviceType := "3"
		if t.Env == "STB" {
			deviceType = "7"
		}
		// Use the request's own protocol+host so the URL matches the real domain
		baseURL := c.BaseURL()
		url := fmt.Sprintf("%s/api/vast?sid=%d&w=%d&h=%d&cb={cb}&ip={uip}&ua={ua}"+
			"&app_bundle={app_bundle}&app_name={app_name}&app_store_url={app_store_url}"+
			"&country_code=US&max_dur=%d&min_dur=%d"+
			"&device_make={device_make}&device_model={device_model}&device_type=%s"+
			"&ct_genre=game,entertainment,family&ct_lang=en&dnt=0&ifa={idfa}&os={device_os}"+
			"&us_privacy=1---",
			baseURL, t.ID, w, h, maxDur, minDur, deviceType)

		return c.JSON(fiber.Map{
			"supply_tag_id":   t.ID,
			"supply_tag_name": t.Name,
			"vast_url":        url,
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
		s.mu.Lock()
		e.ID = s.nextDemandEndpointID
		s.nextDemandEndpointID++
		if e.Status == 0 {
			e.Status = 1
		}
		s.demandEndpoints[e.ID] = &e
		s.mu.Unlock()

		// Live-register as enterprise adapter for real-time parallel bidding
		if eDeps != nil && eDeps.Registry != nil && e.Status == 1 && e.URL != "" {
			adapterID := fmt.Sprintf("demand-ep-%d", e.ID)
			timeout := e.Timeout
			if timeout == 0 {
				timeout = 800
			}
			acfg := &adapter.AdapterConfig{
				ID: adapterID, Name: e.Name,
				Type:          adapter.AdapterType(e.Integration),
				Endpoint:      e.URL, TimeoutMs: timeout,
				Floor:         e.Floor, Margin: e.Margin,
				QPSLimit:      e.QPS, Status: 1,
				GZIPSupport:   e.GZIPSupport,
				RemovePChain:  e.RemovePChain,
				SChainEnabled: e.SupplyChain,
				BAdv:          e.BAdv,
				BCat:          e.BCat,
			}
			switch adapter.AdapterType(e.Integration) {
			case adapter.TypeVAST:
				eDeps.Registry.Register(adapter.NewVASTAdapter(acfg), acfg)
			default:
				eDeps.Registry.Register(adapter.NewORTBAdapter(acfg), acfg)
			}
		}

		return c.Status(201).JSON(e)
	})

	sd.Put("/demand-endpoints/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		e, ok := s.demandEndpoints[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		var update DemandEndpoint
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
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
			e.Timeout = update.Timeout
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

		// Re-register adapter with updated config for live hot-reload
		if eDeps != nil && eDeps.Registry != nil && e.URL != "" {
			adapterID := fmt.Sprintf("demand-ep-%d", id)
			timeout := e.Timeout
			if timeout == 0 {
				timeout = 800
			}
			acfg := &adapter.AdapterConfig{
				ID: adapterID, Name: e.Name,
				Type:          adapter.AdapterType(e.Integration),
				Endpoint:      e.URL, TimeoutMs: timeout,
				Floor:         e.Floor, Margin: e.Margin,
				QPSLimit:      e.QPS, Status: e.Status,
				GZIPSupport:   e.GZIPSupport,
				RemovePChain:  e.RemovePChain,
				SChainEnabled: e.SupplyChain,
				BAdv:          e.BAdv,
				BCat:          e.BCat,
			}
			if e.Status == 1 {
				switch adapter.AdapterType(e.Integration) {
				case adapter.TypeVAST:
					eDeps.Registry.Register(adapter.NewVASTAdapter(acfg), acfg)
				default:
					eDeps.Registry.Register(adapter.NewORTBAdapter(acfg), acfg)
				}
			} else {
				eDeps.Registry.Remove(adapterID)
			}
		}

		return c.JSON(e)
	})

	sd.Delete("/demand-endpoints/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.demandEndpoints[id]; !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.demandEndpoints, id)
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
		s.mu.Unlock()

		// Live-register VAST tag as enterprise adapter for real-time parallel bidding
		if eDeps != nil && eDeps.Registry != nil && t.Status == 1 && t.URL != "" {
			adapterID := fmt.Sprintf("demand-vast-%d", t.ID)
			acfg := &adapter.AdapterConfig{
				ID: adapterID, Name: t.Name,
				Type:     adapter.TypeVAST,
				Endpoint: t.URL, TimeoutMs: 800,
				Floor: t.Floor, Margin: t.Margin,
				Status: 1,
			}
			eDeps.Registry.Register(adapter.NewVASTAdapter(acfg), acfg)
		}

		return c.Status(201).JSON(t)
	})

	sd.Put("/demand-vast-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		t, ok := s.demandVastTags[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		var update DemandVastTag
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
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

		// Re-register adapter with updated config for live hot-reload
		if eDeps != nil && eDeps.Registry != nil && t.URL != "" {
			adapterID := fmt.Sprintf("demand-vast-%d", id)
			acfg := &adapter.AdapterConfig{
				ID: adapterID, Name: t.Name,
				Type:     adapter.TypeVAST,
				Endpoint: t.URL, TimeoutMs: 800,
				Floor: t.Floor, Margin: t.Margin,
				Status: t.Status,
			}
			if t.Status == 1 {
				eDeps.Registry.Register(adapter.NewVASTAdapter(acfg), acfg)
			} else {
				eDeps.Registry.Remove(adapterID)
			}
		}

		return c.JSON(t)
	})

	sd.Delete("/demand-vast-tags/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.demandVastTags[id]; !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.demandVastTags, id)
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
		s.mu.Unlock()
		return c.Status(201).JSON(m)
	})

	sd.Put("/mappings/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		m, ok := s.mappings[id]
		if !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		var update SDMapping
		if err := c.BodyParser(&update); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
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
		return c.JSON(m)
	})

	sd.Delete("/mappings/:id", func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.mappings[id]; !ok {
			return c.Status(404).JSON(fiber.Map{"error": "Not found"})
		}
		delete(s.mappings, id)
		s.rebuildMappingIndexLocked()
		return c.JSON(fiber.Map{"deleted": id})
	})
}

// ── Pipeline Handler (Enterprise) ──

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
		// Look up supply tag by sid — reject unknown/missing sources
		sidStr := c.Query("sid")
		if sidStr == "" {
			return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source: missing sid"})
		}
		sid, _ := strconv.Atoi(sidStr)
		s.mu.RLock()
		tag := s.supplyTags[sid]
		s.mu.RUnlock()
		if tag == nil || tag.Status != 1 {
			return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source"})
		}

		req := openrtb.BuildFromHTTP(c)
		enrichFromSupplyTag(&req, tag)

		if err := validate.Request(&req); err != nil {
			metrics.RecordError()
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		// Resolve supply-demand mappings: only send bid requests to
		// the demand sources explicitly mapped to this supply tag.
		var mappedAdapterIDs []string
		if sid > 0 {
			s.mu.RLock()
			for _, m := range s.mappingsBySID[sid] {
				switch m.Type {
				case "ortb":
					mappedAdapterIDs = append(mappedAdapterIDs, fmt.Sprintf("demand-ep-%d", m.DemandID))
				case "vast":
					mappedAdapterIDs = append(mappedAdapterIDs, fmt.Sprintf("demand-vast-%d", m.DemandID))
				}
			}
			s.mu.RUnlock()
		}

		// If mappings exist, only those demand sources are called.
		// If no mappings, fan out to all active adapters (backward compat).
		var result *pipeline.Result
		if len(mappedAdapterIDs) > 0 {
			result = p.Execute(c.Context(), &req, c.BaseURL(), mappedAdapterIDs)
		} else {
			result = p.Execute(c.Context(), &req, c.BaseURL())
		}

		if result.Error != nil {
			metrics.RecordError()
			return c.Status(500).JSON(fiber.Map{"error": result.Error.Error()})
		}

		if result.NoBid {
			return c.Type("xml").SendString(result.VAST)
		}

		// Record ad decision
		s.recordAdDecision(&req, result.Winner, result.WinPrice, "supply_tag", result.AdapterID)

		return c.Type("xml").SendString(result.VAST)
	}
}

func pipelineHandler(p *pipeline.Pipeline, metrics *monitor.Metrics, s *store) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Validate that the tag belongs to a registered supply source
		tag := c.Params("tag")
		if tag == "" {
			tag = c.Query("tag")
		}
		var supplyTag *SupplyTag
		if tag != "" {
			supplyTag = s.lookupSupplyByTag(tag)
			if supplyTag == nil {
				return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source"})
			}
		} else {
			return c.Status(403).JSON(fiber.Map{"error": "Unknown supply source"})
		}

		req := openrtb.BuildFromHTTP(c)
		enrichFromSupplyTag(&req, supplyTag)

		if err := validate.Request(&req); err != nil {
			metrics.RecordError()
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		result := p.Execute(c.Context(), &req, c.BaseURL())

		if result.Error != nil {
			metrics.RecordError()
			return c.Status(500).JSON(fiber.Map{"error": result.Error.Error()})
		}

		if result.NoBid {
			return c.Type("xml").SendString(result.VAST)
		}

		// Record ad decision
		s.recordAdDecision(&req, result.Winner, result.WinPrice, "pipeline", result.AdapterID)

		return c.Type("xml").SendString(result.VAST)
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
