package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ssp/internal/adapter"
	"ssp/internal/adquality"
	"ssp/internal/bidder"
	"ssp/internal/config"
	"ssp/internal/eventbus"
	"ssp/internal/floor"
	ssphttp "ssp/internal/http"
	"ssp/internal/monitor"
	"ssp/internal/openrtb"
	"ssp/internal/pipeline"
)

func main() {
	configPath := "configs/bidders.yaml"

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Printf("Warning: could not load config (%v), using defaults", err)
		cfg = &config.Config{
			Server: config.ServerConfig{
				Port:          ":8080",
				DashboardPath: "dashboard.html",
				ORTBBidFloor:  0.50,
				ORTBMinDur:    5,
				ORTBMaxDur:    30,
			},
		}
	}

	openrtb.ConfigureRequestDefaults(openrtb.RequestDefaults{
		BidFloor: cfg.Server.ORTBBidFloor,
		MinDur:   cfg.Server.ORTBMinDur,
		MaxDur:   cfg.Server.ORTBMaxDur,
	})

	// Legacy bidder manager (backward-compatible)
	mgr := bidder.NewManagerFromConfig(cfg)
	metrics := monitor.New()

	// ── Enterprise components ──
	bus := eventbus.New()
	reg := adapter.NewRegistry()
	floorEngine := floor.NewEngine()
	aqScanner := adquality.NewScanner()

	// Register adapters from config
	for _, ac := range cfg.Adapters {
		acfg := &adapter.AdapterConfig{
			ID:          ac.ID,
			Name:        ac.Name,
			Type:        adapter.AdapterType(ac.Type),
			Endpoint:    ac.Endpoint,
			TimeoutMs:   ac.TimeoutMs,
			Floor:       ac.Floor,
			Margin:      ac.Margin,
			QPSLimit:    ac.QPSLimit,
			AuctionType: ac.AuctionType,
			Status:      ac.Status,
			TargetGeos:  ac.TargetGeos,
			TargetOS:    ac.TargetOS,
			BlockedBcat: ac.BlockedBcat,
			AllowedMime: ac.AllowedMime,
		}
		switch adapter.AdapterType(ac.Type) {
		case adapter.TypeORTB:
			a := adapter.NewORTBAdapter(acfg)
			reg.Register(a, acfg)
		case adapter.TypeVAST:
			a := adapter.NewVASTAdapter(acfg)
			reg.Register(a, acfg)
		}
	}

	// Register legacy YAML bidders as enterprise adapters so ALL demand
	// sources (legacy + enterprise) compete in one parallel FanOut auction.
	for _, b := range mgr.Bidders() {
		la := adapter.NewLegacyAdapter(b)
		lcfg := &adapter.AdapterConfig{
			ID:     la.ID(),
			Name:   la.Name(),
			Type:   adapter.AdapterType(b.BidderType()),
			Status: 1,
		}
		reg.Register(la, lcfg)
	}

	// Load floor rules from config
	for _, fr := range cfg.FloorRules {
		floorEngine.AddRule(&floor.Rule{
			ID: fr.ID, Name: fr.Name, Priority: fr.Priority,
			FloorCPM: fr.FloorCPM, Geos: fr.Geos,
			DeviceTypes: fr.DeviceTypes, Hours: fr.Hours, Status: fr.Status,
		})
	}

	// Load ad quality rules from config
	if len(cfg.AdQuality.BlockedDomains) > 0 {
		aqScanner.SetBlockedDomains(cfg.AdQuality.BlockedDomains)
	}
	if len(cfg.AdQuality.BlockedCategories) > 0 {
		aqScanner.SetBlockedCategories(cfg.AdQuality.BlockedCategories)
	}
	if len(cfg.AdQuality.BlockedAttrs) > 0 {
		aqScanner.SetBlockedAttrs(cfg.AdQuality.BlockedAttrs)
	}
	if len(cfg.AdQuality.BlockedAdvertisers) > 0 {
		aqScanner.SetBlockedAdvertisers(cfg.AdQuality.BlockedAdvertisers)
	}

	// Build the enterprise pipeline
	pipe := &pipeline.Pipeline{
		Registry:    reg,
		FloorEngine: floorEngine,
		AQScanner:   aqScanner,
		Metrics:     metrics,
		Bus:         bus,
		AuctionType: cfg.Server.AuctionType,
		DefaultTMax: cfg.Server.DefaultTMax,
	}

	eDeps := &ssphttp.EnterpriseDeps{
		Pipeline:    pipe,
		Registry:    reg,
		FloorEngine: floorEngine,
		AQScanner:   aqScanner,
	}

	app := ssphttp.NewRouterWithDeps(cfg, mgr, metrics, configPath, eDeps)

	// Graceful shutdown on SIGINT/SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("Shutting down SSP server gracefully...")
		if err := app.ShutdownWithTimeout(30 * time.Second); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}
	}()

	log.Printf("SSP server starting on %s (legacy bidders: %d, total demand adapters: %d)",
		cfg.Server.Port, len(mgr.Bidders()), reg.Count())
	if err := app.Listen(cfg.Server.Port); err != nil {
		log.Printf("Server stopped: %v", err)
	}
}
