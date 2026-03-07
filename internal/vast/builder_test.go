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

func TestBuildWrapperUsesRequestedVAST41WithFallbackAndErrorHandling(t *testing.T) {
	bid := &openrtb.Bid{ID: "bid-1", Adm: "https://dsp.example.com/wrapper", Price: 3.5}
	req := &openrtb.BidRequest{Imp: []openrtb.Imp{{Video: &openrtb.Video{Protocols: []adcom1.MediaCreativeSubtype{11, 12}}}}}

	xml := Build(bid, req, "https://ads.example.com")
	if !strings.Contains(xml, `<VAST version="4.1">`) {
		t.Fatalf("expected VAST 4.1 wrapper, got %q", xml)
	}
	if !strings.Contains(xml, `<Wrapper fallbackOnNoAd="true" followAdditionalWrappers="true">`) {
		t.Fatalf("expected wrapper fallback attributes, got %q", xml)
	}
	if !strings.Contains(xml, `/api/v1/event/error?`) {
		t.Fatalf("expected wrapper error tracking, got %q", xml)
	}
}

func TestBuildInlineAddsViewableMeasurementForVAST41(t *testing.T) {
	bid := &openrtb.Bid{ID: "bid-2", CrID: "cr-2", Adm: "https://cdn.example.com/video.mp4", Price: 3.5}
	req := &openrtb.BidRequest{Imp: []openrtb.Imp{{Video: &openrtb.Video{Protocols: []adcom1.MediaCreativeSubtype{11}}}}}

	xml := Build(bid, req, "https://ads.example.com")
	if !strings.Contains(xml, `<ViewableImpression><Viewable><![CDATA[https://ads.example.com/api/v1/event/viewable?`) {
		t.Fatalf("expected viewable impression tracking, got %q", xml)
	}
	if !strings.Contains(xml, `<UniversalAdID idRegistry="ssp-creative"><![CDATA[cr-2]]></UniversalAdID>`) {
		t.Fatalf("expected UniversalAdID for modern VAST, got %q", xml)
	}
	if !strings.Contains(xml, `/api/v1/event/error?`) {
		t.Fatalf("expected inline error tracking, got %q", xml)
	}
}

func TestBuildNoAdForRequestUsesRequestedVASTVersion(t *testing.T) {
	req := &openrtb.BidRequest{Imp: []openrtb.Imp{{Video: &openrtb.Video{Protocols: []adcom1.MediaCreativeSubtype{2}}}}}
	xml := BuildNoAdForRequest(req)
	if !strings.Contains(xml, `<VAST version="2.0"/>`) {
		t.Fatalf("expected VAST 2.0 no-ad response, got %q", xml)
	}
}

func TestBuildNoAdForRequestUsesRequestedVAST40Version(t *testing.T) {
	req := &openrtb.BidRequest{Imp: []openrtb.Imp{{Video: &openrtb.Video{Protocols: []adcom1.MediaCreativeSubtype{7, 8}}}}}
	xml := BuildNoAdForRequest(req)
	if !strings.Contains(xml, `<VAST version="4.0"/>`) {
		t.Fatalf("expected VAST 4.0 no-ad response, got %q", xml)
	}
}

func TestBuildNoAdForRequestUsesRequestedVAST42Version(t *testing.T) {
	req := &openrtb.BidRequest{Imp: []openrtb.Imp{{Video: &openrtb.Video{Protocols: []adcom1.MediaCreativeSubtype{13}}}}}
	xml := BuildNoAdForRequest(req)
	if !strings.Contains(xml, `<VAST version="4.2"/>`) {
		t.Fatalf("expected VAST 4.2 no-ad response, got %q", xml)
	}
}
