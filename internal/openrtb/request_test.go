package openrtb

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/prebid/openrtb/v20/adcom1"
)

func TestBuildFromHTTPUsesCTVAppHeadersAsFallback(t *testing.T) {
	app := fiber.New()
	var got BidRequest

	app.Get("/", func(c *fiber.Ctx) error {
		got = BuildFromHTTP(c)
		return c.SendStatus(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Tag-ID", "slot-123")
	req.Header.Set("X-Device-User-Agent", "Roku/DVP-12.5")
	req.Header.Set("X-App-Bundle", "com.example.ctv")
	req.Header.Set("X-App-Name", "Example TV")
	req.Header.Set("X-App-Store-URL", "https://store.example.com/apps/example-tv")
	req.Header.Set("X-App-Version", "1.2.3")
	req.Header.Set("X-Device-IFA", "ifa-123")
	req.Header.Set("X-Device-IP", "203.0.113.10")
	req.Header.Set("X-Device-Type", "ctv")
	req.Header.Set("X-Device-Make", "Roku")
	req.Header.Set("X-Device-Model", "4800X")
	req.Header.Set("X-Device-OS", "Roku")
	req.Header.Set("X-Device-OS-Version", "12.5")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("CF-IPCountry", "US")

	if _, err := app.Test(req); err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}

	if got.Imp[0].TagID != "slot-123" {
		t.Fatalf("expected tag id from header, got %q", got.Imp[0].TagID)
	}
	if got.App == nil || got.App.Bundle != "com.example.ctv" {
		t.Fatalf("expected app bundle from header, got %#v", got.App)
	}
	if got.App.Name != "Example TV" {
		t.Fatalf("expected app name from header, got %q", got.App.Name)
	}
	if got.App.StoreURL != "https://store.example.com/apps/example-tv" {
		t.Fatalf("expected app store URL from header, got %q", got.App.StoreURL)
	}
	if got.App.Ver != "1.2.3" {
		t.Fatalf("expected app version from header, got %q", got.App.Ver)
	}
	if got.Device == nil {
		t.Fatal("expected device to be populated")
	}
	if got.Device.UA != "Roku/DVP-12.5" {
		t.Fatalf("expected device UA from header, got %q", got.Device.UA)
	}
	if got.Device.IP != "203.0.113.10" {
		t.Fatalf("expected device IP from header, got %q", got.Device.IP)
	}
	if got.Device.Make != "Roku" || got.Device.Model != "4800X" {
		t.Fatalf("expected device make/model from headers, got make=%q model=%q", got.Device.Make, got.Device.Model)
	}
	if got.Device.OS != "Roku" || got.Device.OSV != "12.5" {
		t.Fatalf("expected device os/osv from headers, got os=%q osv=%q", got.Device.OS, got.Device.OSV)
	}
	if got.Device.DeviceType != adcom1.DeviceType(3) {
		t.Fatalf("expected CTV device type 3, got %d", got.Device.DeviceType)
	}
	if got.Device.Language != "en-US" {
		t.Fatalf("expected language from Accept-Language header, got %q", got.Device.Language)
	}
	if got.Device.Geo == nil || got.Device.Geo.Country != "USA" {
		t.Fatalf("expected country from CF-IPCountry header, got %#v", got.Device.Geo)
	}
	if got.User == nil || got.User.ID != "ifa-123" {
		t.Fatalf("expected user id from IFA header, got %#v", got.User)
	}
	if got.Device.IFA != "ifa-123" {
		t.Fatalf("expected device IFA from header, got %q", got.Device.IFA)
	}
}

func TestBuildFromHTTPPrefersQueryParamsOverHeaders(t *testing.T) {
	app := fiber.New()
	var got BidRequest

	app.Get("/", func(c *fiber.Ctx) error {
		got = BuildFromHTTP(c)
		return c.SendStatus(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/?app_bundle=query.bundle&ua=query-ua&ip=198.51.100.20&device_type=7&country_code=CA&lang=fr", nil)
	req.Header.Set("X-App-Bundle", "header.bundle")
	req.Header.Set("X-Device-User-Agent", "header-ua")
	req.Header.Set("X-Device-IP", "203.0.113.10")
	req.Header.Set("X-Device-Type", "ctv")
	req.Header.Set("CF-IPCountry", "US")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	if _, err := app.Test(req); err != nil {
		t.Fatalf("unexpected fiber test error: %v", err)
	}

	if got.App == nil || got.App.Bundle != "query.bundle" {
		t.Fatalf("expected query bundle to win, got %#v", got.App)
	}
	if got.Device == nil {
		t.Fatal("expected device to be populated")
	}
	if got.Device.UA != "query-ua" {
		t.Fatalf("expected query UA to win, got %q", got.Device.UA)
	}
	if got.Device.IP != "198.51.100.20" {
		t.Fatalf("expected query IP to win, got %q", got.Device.IP)
	}
	if got.Device.DeviceType != adcom1.DeviceType(7) {
		t.Fatalf("expected query device type 7 to win, got %d", got.Device.DeviceType)
	}
	if got.Device.Language != "fr" {
		t.Fatalf("expected query language to win, got %q", got.Device.Language)
	}
	if got.Device.Geo == nil || got.Device.Geo.Country != "CAN" {
		t.Fatalf("expected query country to win, got %#v", got.Device.Geo)
	}
}

func TestCleanBundleValueDerivesCanonicalBundleFromStoreURL(t *testing.T) {
	got := CleanBundleValue("B00V3UTTPSernsp", "", "https://play.google.com/store/apps/details?id=com.frndlytv.channel")
	if got != "com.frndlytv.channel" {
		t.Fatalf("expected canonical bundle from store URL, got %q", got)
	}
}

func TestCleanBundleValueSuppressesNonCanonicalJunk(t *testing.T) {
	got := CleanBundleValue("1022720soccha", "", "")
	if got != "" {
		t.Fatalf("expected junk bundle to be suppressed, got %q", got)
	}
}

func TestBundleFromStoreURLUsesCustomDomainHost(t *testing.T) {
	got := BundleFromStoreURL("https://seed.verify.app/store")
	if got != "seed.verify.app" {
		t.Fatalf("expected custom domain host fallback, got %q", got)
	}
}

func TestCanonicalBundleValueRejectsLowConfidenceTwoSegmentBundle(t *testing.T) {
	got := CanonicalBundleValue("vizio.truliyt")
	if got != "" {
		t.Fatalf("expected low-confidence two-segment bundle to be rejected, got %q", got)
	}
}

func TestCanonicalBundleValueAcceptsKnownTwoSegmentBundleRoot(t *testing.T) {
	got := CanonicalBundleValue("tv.tubi")
	if got != "tv.tubi" {
		t.Fatalf("expected known two-segment bundle root to be accepted, got %q", got)
	}
}

func TestCanonicalBundleValueAcceptsThreeSegmentBundle(t *testing.T) {
	got := CanonicalBundleValue("com.xumo.historychannel")
	if got != "com.xumo.historychannel" {
		t.Fatalf("expected three-segment bundle to be accepted, got %q", got)
	}
}
