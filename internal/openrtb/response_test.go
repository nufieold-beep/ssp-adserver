package openrtb

import (
	"testing"

	openrtb2 "github.com/prebid/openrtb/v20/openrtb2"
)

func TestHasRenderableAdm(t *testing.T) {
	tests := []struct {
		name string
		adm  string
		want bool
	}{
		{name: "empty", adm: "", want: false},
		{name: "vast xml", adm: "  <VAST version=\"3.0\"></VAST>", want: true},
		{name: "scheme relative url", adm: "//cdn.example.com/video.mp4", want: true},
		{name: "https url", adm: "https://cdn.example.com/tag", want: true},
		{name: "invalid https host", adm: "https://", want: false},
		{name: "plain text", adm: "not-a-creative", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasRenderableAdm(tt.adm); got != tt.want {
				t.Fatalf("HasRenderableAdm(%q) = %v, want %v", tt.adm, got, tt.want)
			}
		})
	}
}

func TestIsRenderableBidIgnoresNoticeOnlyBids(t *testing.T) {
	bid := Bid{NURL: "https://example.com/win"}
	if IsRenderableBid(bid) {
		t.Fatal("expected notice-only bid to be non-renderable")
	}
}

func TestValidateBidResponseRejectsNonRenderableBids(t *testing.T) {
	req := &BidRequest{
		Imp: []Imp{{ID: "imp-1", BidFloor: 1.5}},
	}

	resp := &openrtb2.BidResponse{
		SeatBid: []openrtb2.SeatBid{{
			Seat: "seat-a",
			Bid: []openrtb2.Bid{
				{ID: "notice-only", ImpID: "imp-1", Price: 2.0, NURL: "https://example.com/win"},
				{ID: "invalid-adm", ImpID: "imp-1", Price: 2.0, AdM: "not-a-creative"},
				{ID: "below-floor", ImpID: "imp-1", Price: 1.0, AdM: "https://cdn.example.com/video.mp4"},
				{ID: "valid", ImpID: "imp-1", Price: 2.5, AdM: "https://cdn.example.com/video.mp4"},
			},
		}},
	}

	validated := ValidateBidResponse(resp, req)
	if len(validated) != 1 {
		t.Fatalf("expected 1 validated bid, got %d", len(validated))
	}
	if validated[0].ID != "valid" {
		t.Fatalf("expected surviving bid to be %q, got %q", "valid", validated[0].ID)
	}
}
