package eventbus

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishAndPublishSyncDispatchHandlers(t *testing.T) {
	b := newBus(1, 4)
	defer b.Close()

	var calls atomic.Int64
	done := make(chan struct{}, 2)
	b.Subscribe(EvtAdRequest, func(evt Event) {
		calls.Add(1)
		done <- struct{}{}
	})
	b.Subscribe(EvtAdRequest, func(evt Event) {
		calls.Add(1)
		done <- struct{}{}
	})

	b.Publish(Event{Type: EvtAdRequest})
	awaitSignal(t, done)
	awaitSignal(t, done)

	b.PublishSync(Event{Type: EvtAdRequest})
	if got := calls.Load(); got != 4 {
		t.Fatalf("expected 4 handler invocations, got %d", got)
	}
}

func awaitSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event handler")
	}
}
