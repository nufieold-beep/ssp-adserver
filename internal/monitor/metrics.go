package monitor

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks real-time SSP metrics in memory.
type Metrics struct {
	mu sync.RWMutex

	// Global counters
	AdRequests   atomic.Int64
	AdOpps       atomic.Int64
	Impressions  atomic.Int64
	Completions  atomic.Int64
	Clicks       atomic.Int64
	NoBids       atomic.Int64
	Errors       atomic.Int64
	TotalSpendMu sync.Mutex
	TotalSpend   float64

	// VAST event counters
	VastStarts atomic.Int64
	VastQ1     atomic.Int64
	VastMid    atomic.Int64
	VastQ3     atomic.Int64
	VastSkips  atomic.Int64
	VastErrors atomic.Int64

	// Bid tracking
	BidWins     atomic.Int64
	BidLosses   atomic.Int64
	WinPricesMu sync.Mutex
	WinPrices   [1000]float64
	wpIdx       int
	wpCount     int

	// Bid latency tracking (milliseconds)
	BidLatenciesMu sync.Mutex
	BidLatencies   [1000]float64
	blIdx          int
	blCount        int

	// Per-campaign metrics
	CampaignMetrics sync.Map // map[int]*CampaignMetric

	// Traffic events ring buffer
	TrafficEvents []TrafficEvent
	trafficMu     sync.Mutex
	maxEvents     int

	StartTime time.Time
}

// CampaignMetric holds per-campaign counters.
type CampaignMetric struct {
	Requests    atomic.Int64
	Opps        atomic.Int64
	Impressions atomic.Int64
	Completions atomic.Int64
	Clicks      atomic.Int64
	SpendMu     sync.Mutex
	Spend       float64
}

// TrafficEvent records bidstream activity for the inspector.
type TrafficEvent struct {
	Time      time.Time `json:"time"`
	Type      string    `json:"type"`
	RequestID string    `json:"request_id"`
	Env       string    `json:"environment"`
	Details   string    `json:"details"`
	Campaign  string    `json:"campaign,omitempty"`
	Creative  string    `json:"creative_id,omitempty"`
	Country   string    `json:"country,omitempty"`
	IP        string    `json:"ip,omitempty"`
	Supply    string    `json:"supply,omitempty"`
	Bundle    string    `json:"bundle,omitempty"`
	ADomain   string    `json:"adomain,omitempty"`
	Price     string    `json:"price,omitempty"`
}

func New() *Metrics {
	return &Metrics{
		maxEvents: 200,
		StartTime: time.Now(),
	}
}

func (m *Metrics) RecordAdRequest()  { m.AdRequests.Add(1) }
func (m *Metrics) RecordAdOpp()      { m.AdOpps.Add(1) }
func (m *Metrics) RecordImpression() { m.Impressions.Add(1) }
func (m *Metrics) RecordCompletion() { m.Completions.Add(1) }
func (m *Metrics) RecordClick()      { m.Clicks.Add(1) }
func (m *Metrics) RecordNoBid()      { m.NoBids.Add(1) }
func (m *Metrics) RecordError()      { m.Errors.Add(1) }
func (m *Metrics) RecordVastStart()  { m.VastStarts.Add(1) }
func (m *Metrics) RecordVastQ1()     { m.VastQ1.Add(1) }
func (m *Metrics) RecordVastMid()    { m.VastMid.Add(1) }
func (m *Metrics) RecordVastQ3()     { m.VastQ3.Add(1) }
func (m *Metrics) RecordVastSkip()   { m.VastSkips.Add(1) }
func (m *Metrics) RecordVastError()  { m.VastErrors.Add(1) }

func (m *Metrics) RecordSpend(cpm float64) {
	m.TotalSpendMu.Lock()
	m.TotalSpend += cpm / 1000.0
	m.TotalSpendMu.Unlock()
}

func (m *Metrics) RecordWin(price float64) {
	m.BidWins.Add(1)
	m.WinPricesMu.Lock()
	m.WinPrices[m.wpIdx] = price
	m.wpIdx = (m.wpIdx + 1) % len(m.WinPrices)
	if m.wpCount < len(m.WinPrices) {
		m.wpCount++
	}
	m.WinPricesMu.Unlock()
}

func (m *Metrics) RecordLoss() { m.BidLosses.Add(1) }

func (m *Metrics) RecordBidLatency(ms float64) {
	m.BidLatenciesMu.Lock()
	m.BidLatencies[m.blIdx] = ms
	m.blIdx = (m.blIdx + 1) % len(m.BidLatencies)
	if m.blCount < len(m.BidLatencies) {
		m.blCount++
	}
	m.BidLatenciesMu.Unlock()
}

func (m *Metrics) AvgBidLatency() float64 {
	m.BidLatenciesMu.Lock()
	defer m.BidLatenciesMu.Unlock()
	if m.blCount == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < m.blCount; i++ {
		sum += m.BidLatencies[i]
	}
	return sum / float64(m.blCount)
}

func (m *Metrics) AddTrafficEvent(evt TrafficEvent) {
	evt.Time = time.Now()
	m.trafficMu.Lock()
	m.TrafficEvents = append(m.TrafficEvents, evt)
	if len(m.TrafficEvents) > m.maxEvents {
		m.TrafficEvents = m.TrafficEvents[len(m.TrafficEvents)-m.maxEvents:]
	}
	m.trafficMu.Unlock()
}

func (m *Metrics) GetTrafficEvents(filterType string) []TrafficEvent {
	m.trafficMu.Lock()
	defer m.trafficMu.Unlock()

	if filterType == "" {
		out := make([]TrafficEvent, len(m.TrafficEvents))
		copy(out, m.TrafficEvents)
		return out
	}

	var out []TrafficEvent
	for _, e := range m.TrafficEvents {
		if e.Type == filterType {
			out = append(out, e)
		}
	}
	return out
}

// GetCampaignMetric returns (or creates) metrics for a campaign.
func (m *Metrics) GetCampaignMetric(campaignID int) *CampaignMetric {
	val, ok := m.CampaignMetrics.Load(campaignID)
	if ok {
		return val.(*CampaignMetric)
	}
	cm := &CampaignMetric{}
	actual, _ := m.CampaignMetrics.LoadOrStore(campaignID, cm)
	return actual.(*CampaignMetric)
}

// Overview returns a snapshot of global metrics for the dashboard.
type Overview struct {
	AdRequests    int64   `json:"ad_requests"`
	AdOpps        int64   `json:"ad_opportunities"`
	Impressions   int64   `json:"impressions"`
	Completions   int64   `json:"completions"`
	Clicks        int64   `json:"clicks"`
	NoBids        int64   `json:"no_bids"`
	Errors        int64   `json:"errors"`
	TotalSpend    float64 `json:"total_spend"`
	Uptime        string  `json:"uptime"`
	AvgBidLatency float64 `json:"avg_bid_latency_ms"`
	BidWins       int64   `json:"bid_wins"`
	BidLosses     int64   `json:"bid_losses"`
	VastStarts    int64   `json:"vast_starts"`
	VastErrors    int64   `json:"vast_errors"`
}

func (m *Metrics) GetOverview() Overview {
	m.TotalSpendMu.Lock()
	spend := m.TotalSpend
	m.TotalSpendMu.Unlock()

	return Overview{
		AdRequests:    m.AdRequests.Load(),
		AdOpps:        m.AdOpps.Load(),
		Impressions:   m.Impressions.Load(),
		Completions:   m.Completions.Load(),
		Clicks:        m.Clicks.Load(),
		NoBids:        m.NoBids.Load(),
		Errors:        m.Errors.Load(),
		TotalSpend:    spend,
		Uptime:        time.Since(m.StartTime).Round(time.Second).String(),
		AvgBidLatency: m.AvgBidLatency(),
		BidWins:       m.BidWins.Load(),
		BidLosses:     m.BidLosses.Load(),
		VastStarts:    m.VastStarts.Load(),
		VastErrors:    m.VastErrors.Load(),
	}
}

// DeliveryHealth returns VAST delivery health metrics.
type DeliveryHealth struct {
	Impressions int64   `json:"impressions"`
	Starts      int64   `json:"starts"`
	Q1          int64   `json:"q1"`
	Mid         int64   `json:"mid"`
	Q3          int64   `json:"q3"`
	Completions int64   `json:"completions"`
	Skips       int64   `json:"skips"`
	Errors      int64   `json:"errors"`
	StartRate   float64 `json:"start_rate"`
	VTR         float64 `json:"vtr"`
	SkipRate    float64 `json:"skip_rate"`
	ErrorRate   float64 `json:"error_rate"`
}

func (m *Metrics) GetDeliveryHealth() DeliveryHealth {
	imps := m.Impressions.Load()
	starts := m.VastStarts.Load()
	completions := m.Completions.Load()
	skips := m.VastSkips.Load()
	errs := m.VastErrors.Load()

	h := DeliveryHealth{
		Impressions: imps,
		Starts:      starts,
		Q1:          m.VastQ1.Load(),
		Mid:         m.VastMid.Load(),
		Q3:          m.VastQ3.Load(),
		Completions: completions,
		Skips:       skips,
		Errors:      errs,
	}
	if imps > 0 {
		h.StartRate = float64(starts) / float64(imps) * 100
		h.VTR = float64(completions) / float64(imps) * 100
		h.SkipRate = float64(skips) / float64(imps) * 100
		h.ErrorRate = float64(errs) / float64(imps) * 100
	}
	return h
}
