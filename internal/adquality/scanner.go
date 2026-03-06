package adquality

import (
	"ssp/internal/openrtb"
	"strings"
	"sync"
)

// Scanner enforces brand safety and ad quality rules. Every enterprise SSP
// (Magnite, FreeWheel, GAM) runs post-bid ad quality filtering to protect
// publisher inventory from unwanted advertisers, categories, or creative types.
type Scanner struct {
	mu sync.RWMutex

	// Block lists (raw for API serialization)
	blockedDomains []string
	blockedCats    []string
	blockedAttrs   []int
	blockedAdvIDs  []string

	// Allow lists
	allowedDomains []string
	allowedCats    []string

	// O(1) lookup maps — pre-compiled on set, queried on filter
	blockedDomainsMap map[string]bool
	blockedCatsMap    map[string]bool
	blockedAttrsMap   map[int]bool
	blockedAdvIDsMap  map[string]bool
	allowedDomainsMap map[string]bool
	allowedCatsMap    map[string]bool
}

func NewScanner() *Scanner {
	return &Scanner{}
}

// --- Configuration methods ---

func (s *Scanner) SetBlockedDomains(domains []string) {
	s.mu.Lock()
	s.blockedDomains = domains
	s.blockedDomainsMap = make(map[string]bool, len(domains))
	for _, d := range domains {
		s.blockedDomainsMap[strings.ToLower(d)] = true
	}
	s.mu.Unlock()
}

func (s *Scanner) SetBlockedCategories(cats []string) {
	s.mu.Lock()
	s.blockedCats = cats
	s.blockedCatsMap = make(map[string]bool, len(cats))
	for _, c := range cats {
		s.blockedCatsMap[strings.ToUpper(c)] = true
	}
	s.mu.Unlock()
}

func (s *Scanner) SetBlockedAttrs(attrs []int) {
	s.mu.Lock()
	s.blockedAttrs = attrs
	s.blockedAttrsMap = make(map[int]bool, len(attrs))
	for _, a := range attrs {
		s.blockedAttrsMap[a] = true
	}
	s.mu.Unlock()
}

func (s *Scanner) SetBlockedAdvertisers(ids []string) {
	s.mu.Lock()
	s.blockedAdvIDs = ids
	s.blockedAdvIDsMap = make(map[string]bool, len(ids))
	for _, id := range ids {
		s.blockedAdvIDsMap[strings.ToLower(id)] = true
	}
	s.mu.Unlock()
}

func (s *Scanner) SetAllowedDomains(domains []string) {
	s.mu.Lock()
	s.allowedDomains = domains
	s.allowedDomainsMap = make(map[string]bool, len(domains))
	for _, d := range domains {
		s.allowedDomainsMap[strings.ToLower(d)] = true
	}
	s.mu.Unlock()
}

func (s *Scanner) SetAllowedCategories(cats []string) {
	s.mu.Lock()
	s.allowedCats = cats
	s.allowedCatsMap = make(map[string]bool, len(cats))
	for _, c := range cats {
		s.allowedCatsMap[strings.ToUpper(c)] = true
	}
	s.mu.Unlock()
}

// GetConfig returns current scanner configuration.
func (s *Scanner) GetConfig() ScannerConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return ScannerConfig{
		BlockedDomains:     append([]string{}, s.blockedDomains...),
		BlockedCategories:  append([]string{}, s.blockedCats...),
		BlockedAttrs:       append([]int{}, s.blockedAttrs...),
		BlockedAdvertisers: append([]string{}, s.blockedAdvIDs...),
		AllowedDomains:     append([]string{}, s.allowedDomains...),
		AllowedCategories:  append([]string{}, s.allowedCats...),
	}
}

// ScannerConfig is the serialisable representation of scanner rules.
type ScannerConfig struct {
	BlockedDomains     []string `json:"blocked_domains" yaml:"blocked_domains"`
	BlockedCategories  []string `json:"blocked_categories" yaml:"blocked_categories"`
	BlockedAttrs       []int    `json:"blocked_attrs" yaml:"blocked_attrs"`
	BlockedAdvertisers []string `json:"blocked_advertisers" yaml:"blocked_advertisers"`
	AllowedDomains     []string `json:"allowed_domains" yaml:"allowed_domains"`
	AllowedCategories  []string `json:"allowed_categories" yaml:"allowed_categories"`
}

// Filter removes bids that violate ad quality rules. Returns the clean subset.
// This runs after bid collection and before the auction, exactly how
// Magnite's "Quality" layer and FreeWheel's "Brand Safety" filter operate.
func (s *Scanner) Filter(bids []openrtb.Bid, req *openrtb.BidRequest) []openrtb.Bid {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clean := make([]openrtb.Bid, 0, len(bids))
	for _, bid := range bids {
		if s.isBlocked(&bid) {
			continue
		}
		clean = append(clean, bid)
	}
	return clean
}

func (s *Scanner) isBlocked(bid *openrtb.Bid) bool {
	// Check advertiser domain block list
	if len(bid.ADomain) > 0 {
		for _, dom := range bid.ADomain {
			d := strings.ToLower(dom)
			if s.domainBlocked(d) {
				return true
			}
			if !s.domainAllowed(d) {
				return true
			}
		}
	}

	// Check IAB category block list
	if len(bid.Cat) > 0 {
		for _, cat := range bid.Cat {
			c := strings.ToUpper(cat)
			if s.catBlocked(c) {
				return true
			}
			if !s.catAllowed(c) {
				return true
			}
		}
	}

	// Check creative attribute block list
	if len(bid.Attr) > 0 {
		for _, attr := range bid.Attr {
			if s.attrBlocked(attr) {
				return true
			}
		}
	}

	// Check blocked advertiser IDs (O(1) map lookup)
	if bid.AdvID != "" {
		if s.blockedAdvIDsMap[strings.ToLower(bid.AdvID)] {
			return true
		}
	}

	return false
}

func (s *Scanner) domainBlocked(d string) bool {
	if s.blockedDomainsMap[d] {
		return true
	}
	// Suffix match for subdomains
	for domain := range s.blockedDomainsMap {
		if strings.HasSuffix(d, "."+domain) {
			return true
		}
	}
	return false
}

func (s *Scanner) domainAllowed(d string) bool {
	if len(s.allowedDomainsMap) == 0 {
		return true
	}
	if s.allowedDomainsMap[d] {
		return true
	}
	for domain := range s.allowedDomainsMap {
		if strings.HasSuffix(d, "."+domain) {
			return true
		}
	}
	return false
}

func (s *Scanner) catBlocked(c string) bool {
	return s.blockedCatsMap[c]
}

func (s *Scanner) catAllowed(c string) bool {
	if len(s.allowedCatsMap) == 0 {
		return true
	}
	return s.allowedCatsMap[c]
}

func (s *Scanner) attrBlocked(attr int) bool {
	return s.blockedAttrsMap[attr]
}
