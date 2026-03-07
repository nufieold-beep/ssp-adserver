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

func TestCloseWhilePublishSyncIsPending(t *testing.T) {
	b := newBus(1, 1)
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	b.Subscribe(EvtAdRequest, func(evt Event) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	})

	b.Publish(Event{Type: EvtAdRequest})
	awaitSignal(t, started)

	b.Publish(Event{Type: EvtAdRequest})

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		b.PublishSync(Event{Type: EvtAdRequest})
	}()

	time.Sleep(50 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		b.Close()
		close(closeDone)
	}()

	close(release)
	awaitSignal(t, publishDone)
	awaitSignal(t, closeDone)
}

func awaitSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event handler")
	}
}
