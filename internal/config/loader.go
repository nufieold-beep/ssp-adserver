package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type BidderConfig struct {
	Name     string  `yaml:"name" json:"name"`
	Type     string  `yaml:"type" json:"type"`
	Endpoint string  `yaml:"endpoint" json:"endpoint"`
	Timeout  int     `yaml:"timeout" json:"timeout"`
	Floor    float64 `yaml:"floor" json:"floor"`
	Margin   float64 `yaml:"margin" json:"margin"`
	Status   int     `yaml:"status" json:"status"` // 1=active, 0=inactive
}

type ServerConfig struct {
	Port          string  `yaml:"port" json:"port"`
	PublicBaseURL string  `yaml:"public_base_url" json:"public_base_url"`
	AuctionType   string  `yaml:"auction_type" json:"auction_type"` // first_price, second_price
	DefaultFloor  float64 `yaml:"default_floor" json:"default_floor"`
	DefaultTMax   int     `yaml:"default_tmax" json:"default_tmax"`
	DashboardPath string  `yaml:"dashboard_path" json:"dashboard_path"`
	ORTBBidFloor  float64 `yaml:"ortb_bid_floor" json:"ortb_bid_floor"`
	ORTBMinDur    int     `yaml:"ortb_min_duration" json:"ortb_min_duration"`
	ORTBMaxDur    int     `yaml:"ortb_max_duration" json:"ortb_max_duration"`
}

type Config struct {
	Server     ServerConfig      `yaml:"server" json:"server"`
	Bidders    []BidderConfig    `yaml:"bidders" json:"bidders"`
	Adapters   []AdapterConfig   `yaml:"adapters,omitempty" json:"adapters,omitempty"`
	FloorRules []FloorRuleConfig `yaml:"floor_rules,omitempty" json:"floor_rules,omitempty"`
	AdQuality  AdQualityConfig   `yaml:"ad_quality,omitempty" json:"ad_quality,omitempty"`
}

// AdapterConfig is the enterprise demand adapter config (replaces BidderConfig).
type AdapterConfig struct {
	ID          string   `yaml:"id" json:"id"`
	Name        string   `yaml:"name" json:"name"`
	Type        string   `yaml:"type" json:"type"` // "ortb" or "vast"
	Endpoint    string   `yaml:"endpoint" json:"endpoint"`
	TimeoutMs   int      `yaml:"timeout_ms" json:"timeout_ms"`
	Floor       float64  `yaml:"floor" json:"floor"`
	Margin      float64  `yaml:"margin" json:"margin"`
	QPSLimit    int      `yaml:"qps_limit" json:"qps_limit"`
	AuctionType string   `yaml:"auction_type" json:"auction_type"`
	Status      int      `yaml:"status" json:"status"`
	TargetGeos  []string `yaml:"target_geos" json:"target_geos"` // Targeting Configs
	TargetOS    []string `yaml:"target_os" json:"target_os"`
	BlockedBcat []string `yaml:"blocked_bcat" json:"blocked_bcat"`
	AllowedMime []string `yaml:"allowed_mime" json:"allowed_mime"`
}

// FloorRuleConfig represents a floor rule in the YAML config.
type FloorRuleConfig struct {
	ID          string   `yaml:"id" json:"id"`
	Name        string   `yaml:"name" json:"name"`
	Priority    int      `yaml:"priority" json:"priority"`
	FloorCPM    float64  `yaml:"floor_cpm" json:"floor_cpm"`
	Geos        []string `yaml:"geos" json:"geos"`
	DeviceTypes []int    `yaml:"device_types" json:"device_types"`
	Hours       []int    `yaml:"hours" json:"hours"`
	Status      int      `yaml:"status" json:"status"`
}

// AdQualityConfig holds block/allow lists for brand safety.
type AdQualityConfig struct {
	BlockedDomains     []string `yaml:"blocked_domains" json:"blocked_domains"`
	BlockedCategories  []string `yaml:"blocked_categories" json:"blocked_categories"`
	BlockedAttrs       []int    `yaml:"blocked_attrs" json:"blocked_attrs"`
	BlockedAdvertisers []string `yaml:"blocked_advertisers" json:"blocked_advertisers"`
	AllowedDomains     []string `yaml:"allowed_domains" json:"allowed_domains"`
	AllowedCategories  []string `yaml:"allowed_categories" json:"allowed_categories"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	// Defaults
	if cfg.Server.Port == "" {
		cfg.Server.Port = ":8080"
	}
	if cfg.Server.AuctionType == "" {
		cfg.Server.AuctionType = "first_price"
	}
	if cfg.Server.DefaultFloor == 0 {
		cfg.Server.DefaultFloor = 5.00
	}
	if cfg.Server.DefaultTMax == 0 {
		cfg.Server.DefaultTMax = 120
	}
	if cfg.Server.DashboardPath == "" {
		cfg.Server.DashboardPath = "dashboard.html"
	}
	if cfg.Server.ORTBBidFloor == 0 {
		cfg.Server.ORTBBidFloor = 0.50
	}
	if cfg.Server.ORTBMinDur == 0 {
		cfg.Server.ORTBMinDur = 5
	}
	if cfg.Server.ORTBMaxDur == 0 {
		cfg.Server.ORTBMaxDur = 30
	}

	return &cfg, nil
}

func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}
