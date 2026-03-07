package vast

import (
	"net/url"
	"strings"
	"testing"

	"ssp/internal/openrtb"

	"github.com/prebid/openrtb/v20/adcom1"
)

func TestImpressionBlockUsesCleanBundleInTrackingURL(t *testing.T) {
	bid := &openrtb.Bid{ID: "bid-1", Price: 1.25}
	req := &openrtb.BidRequest{
		ID:  "req-1",
		App: &openrtb.App{Bundle: "B00V3UTTPSernsp", StoreURL: "https://play.google.com/store/apps/details?id=com.clean.bundle"},
		Device: &openrtb.Device{
			DeviceType: adcom1.DeviceType(3),
		},
	}

	xml := impressionBlock("https://ads.example.com/api/v1/event", bid, req)
	if !strings.Contains(xml, "bndl=com.clean.bundle") {
		t.Fatalf("expected impression block to contain clean bundle, got %q", xml)
	}
	if strings.Contains(xml, "B00V3UTTPSernsp") {
		t.Fatalf("expected raw junk bundle to be omitted from impression block, got %q", xml)
	}
}

func TestImpressionBlockSuppressesJunkBundleWithoutFallback(t *testing.T) {
	bid := &openrtb.Bid{ID: "bid-2", Price: 1.25}
	req := &openrtb.BidRequest{
		ID:  "req-2",
		App: &openrtb.App{Bundle: "1022720soccha"},
		Device: &openrtb.Device{
			DeviceType: adcom1.DeviceType(3),
		},
	}

	xml := impressionBlock("https://ads.example.com/api/v1/event", bid, req)
	start := strings.Index(xml, "/impression?")
	if start < 0 {
		t.Fatalf("expected impression URL in xml, got %q", xml)
	}
	end := strings.Index(xml[start:], "]]")
	if end < 0 {
		t.Fatalf("expected CDATA end in xml, got %q", xml)
	}
	rawURL := xml[start+len("/impression?") : start+end]
	values, err := url.ParseQuery(rawURL)
	if err != nil {
		t.Fatalf("unexpected parse query error: %v", err)
	}
	if got := values.Get("bndl"); got != "" {
		t.Fatalf("expected junk bundle to be suppressed from tracking URL, got %q", got)
	}
}
