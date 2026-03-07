package http

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistSupplyDemandStateSkipsStaleSnapshots(t *testing.T) {
	s := newStore()
	s.statePath = filepath.Join(t.TempDir(), "runtime_state.json")

	s.mu.Lock()
	s.campaigns[1] = &Campaign{ID: 1, Name: "first"}
	s.nextCampaignID = 2
	write1 := s.prepareSupplyDemandStateWriteLocked()
	s.mu.Unlock()

	s.mu.Lock()
	s.campaigns[1].Name = "second"
	write2 := s.prepareSupplyDemandStateWriteLocked()
	s.mu.Unlock()

	if err := write2.Persist(); err != nil {
		t.Fatalf("persisting newer snapshot failed: %v", err)
	}
	if err := write1.Persist(); err != nil {
		t.Fatalf("persisting stale snapshot failed: %v", err)
	}

	data, err := os.ReadFile(s.statePath)
	if err != nil {
		t.Fatalf("reading persisted state failed: %v", err)
	}

	var state supplyDemandState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal persisted state failed: %v", err)
	}
	if len(state.Campaigns) != 1 {
		t.Fatalf("expected 1 persisted campaign, got %d", len(state.Campaigns))
	}
	if state.Campaigns[0].Name != "second" {
		t.Fatalf("expected latest campaign name %q, got %q", "second", state.Campaigns[0].Name)
	}
}
