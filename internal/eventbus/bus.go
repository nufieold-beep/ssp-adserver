package eventbus

import (
	"sync"
	"sync/atomic"
)

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

type queuedEvent struct {
	evt     Event
	handler Handler
	done    *sync.WaitGroup
}

// Bus is an in-process event bus for decoupled SSP component communication.
// Magnite, GAM, and FreeWheel all use internal event buses to keep
// ad serving latency-critical paths clean of non-essential work.
type Bus struct {
	mu        sync.RWMutex
	handlers  map[string][]Handler
	queue     chan queuedEvent
	workerWG  sync.WaitGroup
	closed    atomic.Bool
	closeOnce sync.Once
}

const (
	defaultWorkerCount = 4
	defaultQueueSize   = 256
)

func New() *Bus {
	return newBus(defaultWorkerCount, defaultQueueSize)
}

func newBus(workerCount, queueSize int) *Bus {
	if workerCount <= 0 {
		workerCount = 1
	}
	if queueSize <= 0 {
		queueSize = workerCount * 32
	}
	b := &Bus{
		handlers: make(map[string][]Handler),
		queue:    make(chan queuedEvent, queueSize),
	}
	for i := 0; i < workerCount; i++ {
		b.workerWG.Add(1)
		go b.runWorker()
	}
	return b
}

// HasSubscribers reports whether an event type currently has at least one
// subscriber. Used on request hot paths to avoid building event payload maps
// when no consumer is listening.
func (b *Bus) HasSubscribers(eventType string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.handlers[eventType]) > 0
}

// Subscribe registers a handler for a specific event type.
func (b *Bus) Subscribe(eventType string, h Handler) {
	b.mu.Lock()
	b.handlers[eventType] = append(b.handlers[eventType], h)
	b.mu.Unlock()
}

// Publish sends an event to all subscribers asynchronously.
// The common path is queued to a bounded worker pool; when the queue is
// saturated, work falls back inline to avoid unbounded goroutine growth.
func (b *Bus) Publish(evt Event) {
	handlers := b.snapshotHandlers(evt.Type)

	for _, h := range handlers {
		b.dispatch(queuedEvent{evt: evt, handler: h}, false)
	}
}

// PublishSync sends an event and waits for all handlers to complete.
// Used for critical events where ordering matters (e.g., auction result).
func (b *Bus) PublishSync(evt Event) {
	handlers := b.snapshotHandlers(evt.Type)

	var wg sync.WaitGroup
	for _, h := range handlers {
		wg.Add(1)
		b.dispatch(queuedEvent{evt: evt, handler: h, done: &wg}, true)
	}
	wg.Wait()
}

func (b *Bus) Close() {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.queue)
		b.workerWG.Wait()
	})
}

func (b *Bus) snapshotHandlers(eventType string) []Handler {
	b.mu.RLock()
	defer b.mu.RUnlock()
	handlers := b.handlers[eventType]
	if len(handlers) == 0 {
		return nil
	}
	out := make([]Handler, len(handlers))
	copy(out, handlers)
	return out
}

func (b *Bus) dispatch(task queuedEvent, wait bool) {
	if b.closed.Load() {
		if task.done != nil {
			task.done.Done()
		}
		return
	}

	if wait {
		b.queue <- task
		return
	}

	select {
	case b.queue <- task:
	default:
		b.runTask(task)
	}
}

func (b *Bus) runWorker() {
	defer b.workerWG.Done()
	for task := range b.queue {
		b.runTask(task)
	}
}

func (b *Bus) runTask(task queuedEvent) {
	if task.done != nil {
		defer task.done.Done()
	}
	task.handler(task.evt)
}
