package floor

import (
	"ssp/internal/openrtb"
	"testing"

	"github.com/prebid/openrtb/v20/adcom1"
)

func TestCalculateDecisionPrefersMatchingRuleOverBaseFloor(t *testing.T) {
	engine := NewEngine()
	engine.AddRule(&Rule{
		ID:          "geo-us-ctv",
		Name:        "US CTV",
		Priority:    1,
		FloorCPM:    3.25,
		Geos:        []string{"USA"},
		DeviceTypes: []int{3},
		Status:      1,
	})

	req := &openrtb.BidRequest{
		Imp: []openrtb.Imp{{BidFloor: 1.5}},
		Device: &openrtb.Device{
			DeviceType: adcom1.DeviceType(3),
			Geo:        &openrtb.Geo{Country: "USA"},
		},
	}

	decision := engine.CalculateDecision(req)
	if decision.Mode != "rule" {
		t.Fatalf("expected rule mode, got %q", decision.Mode)
	}
	if decision.MatchedRuleID != "geo-us-ctv" {
		t.Fatalf("expected matched rule id geo-us-ctv, got %q", decision.MatchedRuleID)
	}
	if decision.AppliedFloor != 3.25 {
		t.Fatalf("expected applied floor 3.25, got %.2f", decision.AppliedFloor)
	}
}

func TestObserveWinPriceFeedsAdaptiveFloor(t *testing.T) {
	engine := NewEngine()
	engine.ObserveWinPrice(10)

	decision := engine.CalculateDecision(&openrtb.BidRequest{})
	if decision.Mode != "adaptive" {
		t.Fatalf("expected adaptive mode, got %q", decision.Mode)
	}
	if decision.AdaptiveFloor != 7.0 {
		t.Fatalf("expected adaptive floor 7.0, got %.2f", decision.AdaptiveFloor)
	}
	if decision.AppliedFloor != 7.0 {
		t.Fatalf("expected applied floor 7.0, got %.2f", decision.AppliedFloor)
	}
}
