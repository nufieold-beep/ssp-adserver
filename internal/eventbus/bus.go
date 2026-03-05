package eventbus

import "sync"

// Event represents a typed event flowing through the SSP pipeline.
// Every stage emits events; listeners (metrics, logging, analytics) consume them.
// This mirrors the event-driven architecture used by Magnite and FreeWheel
// to decouple ad-serving logic from observability.
type Event struct {
	Type string
	Data map[string]interface{}
}

const (
	EvtAdRequest    = "ad_request"
	EvtBidRequest   = "bid_request"
	EvtBidResponse  = "bid_response"
	EvtNoBid        = "no_bid"
	EvtBidTimeout   = "bid_timeout"
	EvtAuctionStart = "auction_start"
	EvtAuctionEnd   = "auction_end"
	EvtDealMatch    = "deal_match"
	EvtFloorApplied = "floor_applied"
	EvtAdQualBlock  = "adqual_block"
	EvtWinNotice    = "win_notice"
	EvtLossNotice   = "loss_notice"
	EvtImpression   = "impression"
	EvtBilling      = "billing"
	EvtVastEvent    = "vast_event"
	EvtError        = "error"
	EvtEnrichment   = "enrichment"
)

// Handler is a function that processes an event.
type Handler func(Event)

// Bus is an in-process event bus for decoupled SSP component communication.
// Magnite, GAM, and FreeWheel all use internal event buses to keep
// ad serving latency-critical paths clean of non-essential work.
type Bus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

func New() *Bus {
	return &Bus{handlers: make(map[string][]Handler)}
}

// Subscribe registers a handler for a specific event type.
func (b *Bus) Subscribe(eventType string, h Handler) {
	b.mu.Lock()
	b.handlers[eventType] = append(b.handlers[eventType], h)
	b.mu.Unlock()
}

// Publish sends an event to all subscribers asynchronously.
// Non-blocking: handlers run in goroutines to avoid slowing the serving path.
func (b *Bus) Publish(evt Event) {
	b.mu.RLock()
	handlers := b.handlers[evt.Type]
	b.mu.RUnlock()

	for _, h := range handlers {
		go h(evt)
	}
}

// PublishSync sends an event and waits for all handlers to complete.
// Used for critical events where ordering matters (e.g., auction result).
func (b *Bus) PublishSync(evt Event) {
	b.mu.RLock()
	handlers := b.handlers[evt.Type]
	b.mu.RUnlock()

	var wg sync.WaitGroup
	for _, h := range handlers {
		wg.Add(1)
		go func(fn Handler) {
			defer wg.Done()
			fn(evt)
		}(h)
	}
	wg.Wait()
}
