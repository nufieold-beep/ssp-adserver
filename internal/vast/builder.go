package vast

import (
	"fmt"
	"html"
	"net/url"
	"path"
	"ssp/internal/openrtb"
	"strings"
)

// BaseURL is set at startup to the server's publicly-reachable origin.
// e.g., "http://localhost:8080" — router sets this on init.
var BaseURL string

// AdmType classifies the content of a bid's Adm field.
type AdmType int

const (
	AdmInline      AdmType = iota // Media file URL → <InLine> with <MediaFile>
	AdmWrapper                    // VAST tag URL → <Wrapper> with <VASTAdTagURI>
	AdmPassthrough                // Complete VAST XML → inject our tracking pixels
)

// DetectAdmType inspects the Adm content and returns the appropriate type.
func DetectAdmType(adm string) AdmType {
	trimmed := strings.TrimSpace(adm)
	if trimmed == "" {
		return AdmInline
	}
	// Full VAST XML document — passthrough
	if strings.HasPrefix(trimmed, "<?xml") || strings.HasPrefix(trimmed, "<VAST") {
		return AdmPassthrough
	}
	// URL — check if it's a direct media file or a VAST wrapper redirect
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		ext := strings.ToLower(path.Ext(strings.SplitN(trimmed, "?", 2)[0]))
		switch ext {
		case ".mp4", ".webm", ".ogg", ".m3u8", ".mpd", ".mov", ".3gp":
			return AdmInline
		}
		return AdmWrapper
	}
	return AdmInline
}

// Build creates a VAST 3.0 XML response from a winning bid.
// baseURL is the publicly-reachable origin (e.g. "https://ads1.viadsmedia.com").
// req provides the full request context for enriched tracking pixels.
func Build(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	if baseURL == "" {
		baseURL = BaseURL
	}
	switch DetectAdmType(bid.Adm) {
	case AdmPassthrough:
		return buildPassthrough(bid, req, baseURL)
	case AdmWrapper:
		return buildWrapper(bid, req, baseURL)
	default:
		return buildInline(bid, req, baseURL)
	}
}

// impressionBlock returns the SSP + DSP impression pixel XML fragment.
// Includes full campaign/creative/geo/supply context for metrics tracking.
func impressionBlock(evtBase string, bid *openrtb.Bid, req *openrtb.BidRequest) string {
	var sb strings.Builder

	// Build enriched impression URL with all tracking params
	params := url.Values{}
	params.Set("rid", req.ID)
	params.Set("bid", bid.ID)
	params.Set("price", fmt.Sprintf("%.6f", bid.Price))

	// Creative
	if bid.CrID != "" {
		params.Set("crid", bid.CrID)
	}
	if len(bid.ADomain) > 0 {
		params.Set("adom", bid.ADomain[0])
	}
	if bid.Seat != "" {
		params.Set("cmp", bid.Seat)
	}

	// Geo / Device
	if req.Device.IP != "" {
		params.Set("ip", req.Device.IP)
	}
	if req.Device.Geo != nil && req.Device.Geo.Country != "" {
		params.Set("ctry", req.Device.Geo.Country)
	}

	// Environment
	env := "ctv"
	switch req.Device.DeviceType {
	case 1:
		env = "mobile"
	case 2:
		env = "desktop"
	case 4:
		env = "phone"
	case 5:
		env = "tablet"
	}
	params.Set("env", env)

	// Supply/App
	params.Set("sr", "viadsmedia.com")
	if req.App != nil && req.App.Bundle != "" {
		params.Set("bndl", req.App.Bundle)
	}

	fmt.Fprintf(&sb, "   <Impression><![CDATA[%s/impression?%s]]></Impression>\n",
		evtBase, params.Encode())

	if bid.BURL != "" {
		burl := bid.SubstituteMacros(bid.BURL)
		fmt.Fprintf(&sb, "   <Impression><![CDATA[%s]]></Impression>\n", burl)
	}
	return sb.String()
}

// buildInline creates a self-contained VAST InLine ad from a media file URL.
func buildInline(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	evtBase := fmt.Sprintf("%s/api/v1/event", baseURL)
	impressions := impressionBlock(evtBase, bid, req)

	w, h := bid.W, bid.H
	if w == 0 {
		w = 1920
	}
	if h == 0 {
		h = 1080
	}

	bidID := html.EscapeString(bid.ID)
	crID := html.EscapeString(bid.CrID)

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<VAST version="3.0">
 <Ad id="%s">
  <InLine>
   <AdSystem>viadsmedia SSP</AdSystem>
   <AdTitle>Ad %s</AdTitle>
%s   <Creatives>
    <Creative id="%s">
     <Linear>
      <Duration>00:00:30</Duration>
      <MediaFiles>
       <MediaFile type="video/mp4" width="%d" height="%d" delivery="progressive" bitrate="2000"><![CDATA[%s]]></MediaFile>
      </MediaFiles>
     </Linear>
    </Creative>
   </Creatives>
  </InLine>
 </Ad>
</VAST>`, bidID, bidID, impressions, crID, w, h, strings.TrimSpace(bid.Adm))
}

// buildWrapper creates a VAST Wrapper that redirects to the DSP's VAST tag URL.
// SSP tracking and impression pixels are injected so they fire alongside the
// downstream ad's own events.
func buildWrapper(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	evtBase := fmt.Sprintf("%s/api/v1/event", baseURL)
	impressions := impressionBlock(evtBase, bid, req)
	bidID := html.EscapeString(bid.ID)

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<VAST version="3.0">
 <Ad id="%s">
  <Wrapper>
   <AdSystem>viadsmedia SSP</AdSystem>
%s   <VASTAdTagURI><![CDATA[%s]]></VASTAdTagURI>
   <Creatives/>
  </Wrapper>
 </Ad>
</VAST>`, bidID, impressions, strings.TrimSpace(bid.Adm))
}

// buildPassthrough takes a complete VAST XML document from the DSP and
// injects SSP impression + tracking pixels into it.
func buildPassthrough(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	xml := html.UnescapeString(strings.TrimSpace(bid.Adm))
	evtBase := fmt.Sprintf("%s/api/v1/event", baseURL)
	impressions := impressionBlock(evtBase, bid, req)

	// Inject impression pixels after the first <Impression> block or after <InLine>/<Wrapper>
	injected := false
	for _, anchor := range []string{"</Impression>", "<Creatives>", "<InLine>", "<Wrapper>"} {
		idx := strings.Index(xml, anchor)
		if idx >= 0 {
			insertAt := idx + len(anchor)
			xml = xml[:insertAt] + "\n" + impressions + xml[insertAt:]
			injected = true
			break
		}
	}
	if !injected {
		// Fallback: insert after first <Ad...> tag
		if adIdx := strings.Index(xml, ">"); adIdx >= 0 {
			adEnd := strings.Index(xml[adIdx:], ">")
			if adEnd >= 0 {
				pos := adIdx + adEnd + 1
				xml = xml[:pos] + "\n" + impressions + xml[pos:]
			}
		}
	}

	return xml
}

// BuildNoAd returns an empty VAST response (no ad available).
func BuildNoAd() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<VAST version="3.0"/>`
}
