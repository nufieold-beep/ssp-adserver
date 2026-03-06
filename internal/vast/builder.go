package vast

import (
	"fmt"
	"html"
	"net/url"
	"path"
	"ssp/internal/openrtb"
	"strconv"
	"strings"
)

// BaseURL is set at startup to the server's publicly-reachable origin.
// e.g., "http://localhost:8080" — router sets this on init.
var BaseURL string

var alpha3To2Country = map[string]string{
	"USA": "US", "GBR": "GB", "CAN": "CA", "AUS": "AU", "DEU": "DE",
	"FRA": "FR", "JPN": "JP", "CHN": "CN", "IND": "IN", "BRA": "BR",
}

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

	if req == nil {
		req = &openrtb.BidRequest{}
	}

	rid := req.ID
	if req.User != nil && req.User.ID != "" {
		rid = req.User.ID
	} else if req.Device.IFA != "" {
		rid = req.Device.IFA
	}
	cmp := bid.Seat
	crid := bid.CrID
	ip := req.Device.IP
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
	ctry := ""
	if req.Device.Geo != nil {
		ctry = metricCountryCode(req.Device.Geo.Country)
	}
	sr := supplyRef(evtBase)
	bndl := ""
	if req.App != nil {
		bndl = req.App.Bundle
	}
	adom := ""
	if len(bid.ADomain) > 0 {
		adom = bid.ADomain[0]
	}
	price := strconv.FormatFloat(bid.Price, 'f', -1, 64)

	trackingURL := fmt.Sprintf(
		"%s/impression?rid=%s&cmp=%s&crid=%s&ctry=%s&ip=%s&env=%s&sr=%s&bndl=%s&adom=%s&price=%s",
		evtBase,
		url.QueryEscape(rid),
		url.QueryEscape(cmp),
		url.QueryEscape(crid),
		url.QueryEscape(ctry),
		url.QueryEscape(ip),
		url.QueryEscape(env),
		url.QueryEscape(sr),
		url.QueryEscape(bndl),
		url.QueryEscape(adom),
		url.QueryEscape(price),
	)

	fmt.Fprintf(&sb, "   <Impression><![CDATA[%s]]></Impression>\n", trackingURL)

	if bid.BURL != "" {
		burl := bid.SubstituteMacros(bid.BURL)
		fmt.Fprintf(&sb, "   <Impression><![CDATA[%s]]></Impression>\n", burl)
	}
	return sb.String()
}

func metricCountryCode(country string) string {
	code := strings.ToUpper(strings.TrimSpace(country))
	if v, ok := alpha3To2Country[code]; ok {
		return v
	}
	return code
}

func supplyRef(evtBase string) string {
	u, err := url.Parse(evtBase)
	if err != nil || u.Host == "" {
		return "viadsmedia.com"
	}
	if host := u.Hostname(); host != "" {
		if strings.HasSuffix(host, ".viadsmedia.com") || host == "viadsmedia.com" {
			return "viadsmedia.com"
		}
		return host
	}
	return "viadsmedia.com"
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

	admURL := strings.TrimSpace(bid.Adm)
	admURL = bid.SubstituteMacros(admURL) // Evaluate macros natively within direct media URLs
	
	mimeType := "video/mp4"
	ext := strings.ToLower(path.Ext(strings.SplitN(admURL, "?", 2)[0]))
	switch ext {
	case ".webm":
		mimeType = "video/webm"
	case ".ogg":
		mimeType = "video/ogg"
	case ".m3u8":
		mimeType = "application/x-mpegURL"
	case ".mpd":
		mimeType = "application/dash+xml"
	case ".mov":
		mimeType = "video/quicktime"
	case ".3gp":
		mimeType = "video/3gpp"
	}

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
       <MediaFile type="%s" width="%d" height="%d" delivery="progressive" bitrate="2000"><![CDATA[%s]]></MediaFile>
      </MediaFiles>
     </Linear>
    </Creative>
   </Creatives>
  </InLine>
 </Ad>
</VAST>`, bidID, bidID, impressions, crID, mimeType, w, h, admURL)
}

// buildWrapper creates a VAST Wrapper that redirects to the DSP's VAST tag URL.
// SSP tracking and impression pixels are injected so they fire alongside the
// downstream ad's own events.
func buildWrapper(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	evtBase := fmt.Sprintf("%s/api/v1/event", baseURL)
	impressions := impressionBlock(evtBase, bid, req)
	bidID := html.EscapeString(bid.ID)
	
	admURL := strings.TrimSpace(bid.Adm)
	admURL = bid.SubstituteMacros(admURL) // Resolve formatting inside the tag URI string

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<VAST version="3.0">
 <Ad id="%s">
  <Wrapper>
   <AdSystem>viadsmedia SSP</AdSystem>
   <VASTAdTagURI><![CDATA[%s]]></VASTAdTagURI>
%s   <Creatives></Creatives>
  </Wrapper>
 </Ad>
</VAST>`, bidID, admURL, impressions)
}

// buildPassthrough takes a complete VAST XML document from the DSP and
// injects SSP impression + tracking pixels into it.
func buildPassthrough(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	xml := strings.TrimSpace(bid.Adm)
	xml = bid.SubstituteMacros(xml) // Resolve ${AUCTION_PRICE} and others embedded natively in VAST XML
	
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
