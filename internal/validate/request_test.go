package validate

import (
	"ssp/internal/openrtb"
	"testing"
)

func TestRequestPromotesFirstVideoImp(t *testing.T) {
	width := int64(1920)
	height := int64(1080)
	req := &openrtb.BidRequest{
		Imp: []openrtb.Imp{
			{ID: "banner-imp"},
			{
				ID: "video-imp",
				Video: &openrtb.Video{
					W:     &width,
					H:     &height,
					MIMEs: []string{"video/mp4"},
				},
			},
		},
		Device: &openrtb.Device{IP: "1.2.3.4", DeviceType: 3},
		App:    &openrtb.App{ID: "app-1", Bundle: "bundle-1"},
	}

	if err := Request(req); err != nil {
		t.Fatalf("expected request to validate, got %v", err)
	}
	if req.Imp[0].ID != "video-imp" {
		t.Fatalf("expected first impression to be normalized to video imp, got %q", req.Imp[0].ID)
	}
}
