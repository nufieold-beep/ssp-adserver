package vast

import (
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
	AdmInvalid     AdmType = iota
	AdmInline              // Media file URL → <InLine> with <MediaFile>
	AdmWrapper             // VAST tag URL → <Wrapper> with <VASTAdTagURI>
	AdmPassthrough         // Complete VAST XML → inject our tracking pixels
)

// mediaExtensions maps file extensions to their video MIME types.
var mediaExtensions = map[string]string{
	".mp4":  "video/mp4",
	".webm": "video/webm",
	".ogg":  "video/ogg",
	".m3u8": "application/x-mpegURL",
	".mpd":  "application/dash+xml",
	".mov":  "video/quicktime",
	".3gp":  "video/3gpp",
}

// mimeFromURL returns the MIME type for a media URL, defaulting to video/mp4.
func mediaPathExt(rawURL string) string {
	return strings.ToLower(path.Ext(strings.SplitN(rawURL, "?", 2)[0]))
}

// mimeFromURL returns the MIME type for a media URL, defaulting to video/mp4.
func mimeFromURL(rawURL string) string {
	ext := mediaPathExt(rawURL)
	if m, ok := mediaExtensions[ext]; ok {
		return m
	}
	return "video/mp4"
}

// isMediaExt returns true if the URL path has a known video file extension.
func isMediaExt(rawURL string) bool {
	ext := mediaPathExt(rawURL)
	_, ok := mediaExtensions[ext]
	return ok
}

// DetectAdmType inspects the Adm content and returns the appropriate type.
func DetectAdmType(adm string) AdmType {
	trimmed := strings.TrimSpace(adm)
	if trimmed == "" {
		return AdmInvalid
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "<?xml") || strings.HasPrefix(lower, "<vast") || strings.HasPrefix(lower, "<vmap") {
		return AdmPassthrough
	}
	if strings.HasPrefix(trimmed, "//") {
		if isMediaExt(trimmed) {
			return AdmInline
		}
		return AdmWrapper
	}
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		if isMediaExt(trimmed) {
			return AdmInline
		}
		return AdmWrapper
	}
	return AdmInvalid
}

// Build creates a VAST 3.0 XML response from a winning bid.
// baseURL is the publicly-reachable origin (e.g. "https://ads1.viadsmedia.com").
// req provides the full request context for enriched tracking pixels.
func Build(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	if bid == nil {
		return ""
	}
	if strings.TrimSpace(bid.Adm) == "" {
		return ""
	}
	if baseURL == "" {
		baseURL = BaseURL
	}
	switch DetectAdmType(bid.Adm) {
	case AdmPassthrough:
		return buildPassthrough(bid, req, baseURL)
	case AdmWrapper:
		return buildWrapper(bid, req, baseURL)
	case AdmInline:
		return buildInline(bid, req, baseURL)
	default:
		return ""
	}
}

// resolveRequestID picks the best identifier from the request context.
func resolveRequestID(req *openrtb.BidRequest) string {
	if req == nil {
		return ""
	}
	if req.User != nil && req.User.ID != "" {
		return req.User.ID
	}
	if req.Device != nil && req.Device.IFA != "" {
		return req.Device.IFA
	}
	return req.ID
}

// deviceEnv returns a short environment label for the device type.
func deviceEnv(dt int) string {
	switch dt {
	case 1:
		return "mobile"
	case 2:
		return "desktop"
	case 4:
		return "phone"
	case 5:
		return "tablet"
	default:
		return "ctv"
	}
}

// writeImpressionTag appends a single <Impression> CDATA element.
func writeImpressionTag(sb *strings.Builder, pixelURL string) {
	sb.WriteString("   <Impression><![CDATA[")
	sb.WriteString(pixelURL)
	sb.WriteString("]]></Impression>\n")
}

// Pre-allocated tracking event definitions — shared across all requests.
var trackingEvents = [...]struct{ name, path string }{
	{"creativeView", "/start"},
	{"start", "/start"},
	{"firstQuartile", "/firstQuartile"},
	{"midpoint", "/midpoint"},
	{"thirdQuartile", "/thirdQuartile"},
	{"complete", "/complete"},
	{"skip", "/skip"},
}

// trackingEventsBlock returns the <TrackingEvents> XML with SSP event pixels.
func trackingEventsBlock(evtBase string, bid *openrtb.Bid) string {
	var sb strings.Builder
	sb.Grow(1024)

	qs := url.Values{"bid": {bid.ID}, "crid": {bid.CrID}, "cmp": {bid.Seat}}.Encode()

	sb.WriteString("      <TrackingEvents>\n")
	for _, e := range trackingEvents {
		sb.WriteString("       <Tracking event=\"")
		sb.WriteString(e.name)
		sb.WriteString("\"><![CDATA[")
		sb.WriteString(evtBase)
		sb.WriteString(e.path)
		sb.WriteByte('?')
		sb.WriteString(qs)
		sb.WriteString("]]></Tracking>\n")
	}
	sb.WriteString("      </TrackingEvents>\n")

	return sb.String()
}

// impressionBlock returns the SSP + DSP impression pixel XML fragment.
func impressionBlock(evtBase string, bid *openrtb.Bid, req *openrtb.BidRequest) string {
	var sb strings.Builder
	sb.Grow(512)

	if req == nil {
		req = &openrtb.BidRequest{}
	}

	ctry := ""
	ip := ""
	deviceType := 0
	if req.Device != nil {
		ip = req.Device.IP
		deviceType = int(req.Device.DeviceType)
		if req.Device.Geo != nil {
			ctry = metricCountryCode(req.Device.Geo.Country)
		}
	}
	bndl := ""
	if req.App != nil {
		bndl = openrtb.CleanBundleValue(req.App.Bundle, req.App.ID, req.App.StoreURL)
	}
	adom := ""
	if len(bid.ADomain) > 0 {
		adom = bid.ADomain[0]
	}

	params := url.Values{
		"bid":  {bid.ID},
		"rid":  {resolveRequestID(req)},
		"cmp":  {bid.Seat},
		"crid": {bid.CrID},
		"ctry": {ctry},
		"ip":   {ip},
		"env":  {deviceEnv(deviceType)},
		"sr":   {supplyRef(evtBase)},
		"bndl": {bndl},
		"adom": {adom},
		"price": {strconv.FormatFloat(func() float64 {
			price := bid.WinPrice
			if price <= 0 {
				price = bid.Price
			}
			return price
		}(), 'f', -1, 64)},
	}
	writeImpressionTag(&sb, evtBase+"/impression?"+params.Encode())

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

// resolveAdm returns the Adm URL with macros substituted.
func resolveAdm(bid *openrtb.Bid) string {
	return bid.SubstituteMacrosRaw(strings.TrimSpace(bid.Adm))
}

// bidDimensions returns the bid's width and height with sensible defaults.
func bidDimensions(bid *openrtb.Bid) (int, int) {
	w, h := bid.W, bid.H
	if w == 0 {
		w = 1920
	}
	if h == 0 {
		h = 1080
	}
	return w, h
}

// buildInline creates a self-contained VAST InLine ad from a media file URL.
func buildInline(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	evtBase := baseURL + "/api/v1/event"
	impressions := impressionBlock(evtBase, bid, req)
	tracking := trackingEventsBlock(evtBase, bid)

	w, h := bidDimensions(bid)
	bidID := html.EscapeString(bid.ID)
	crID := html.EscapeString(bid.CrID)
	admURL := resolveAdm(bid)

	var sb strings.Builder
	sb.Grow(2048)
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<VAST version="3.0">
 <Ad id="`)
	sb.WriteString(bidID)
	sb.WriteString(`">
  <InLine>
   <AdSystem>viadsmedia SSP</AdSystem>
   <AdTitle>Ad `)
	sb.WriteString(bidID)
	sb.WriteString("</AdTitle>\n")
	sb.WriteString(impressions)
	sb.WriteString(`   <Creatives>
    <Creative id="`)
	sb.WriteString(crID)
	sb.WriteString(`">
     <Linear>
      <Duration>00:00:30</Duration>
`)
	sb.WriteString(tracking)
	sb.WriteString(`      <MediaFiles>
       <MediaFile type="`)
	sb.WriteString(mimeFromURL(admURL))
	sb.WriteString(`" width="`)
	sb.WriteString(strconv.Itoa(w))
	sb.WriteString(`" height="`)
	sb.WriteString(strconv.Itoa(h))
	sb.WriteString(`" delivery="progressive" bitrate="2000"><![CDATA[`)
	sb.WriteString(admURL)
	sb.WriteString(`]]></MediaFile>
      </MediaFiles>
     </Linear>
    </Creative>
   </Creatives>
  </InLine>
 </Ad>
</VAST>`)
	return sb.String()
}

// buildWrapper creates a VAST Wrapper that redirects to the DSP's VAST tag URL.
func buildWrapper(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	evtBase := baseURL + "/api/v1/event"
	impressions := impressionBlock(evtBase, bid, req)
	tracking := trackingEventsBlock(evtBase, bid)
	bidID := html.EscapeString(bid.ID)
	admURL := resolveAdm(bid)

	var sb strings.Builder
	sb.Grow(1536)
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<VAST version="3.0">
 <Ad id="`)
	sb.WriteString(bidID)
	sb.WriteString(`">
  <Wrapper>
   <AdSystem>viadsmedia SSP</AdSystem>
   <VASTAdTagURI><![CDATA[`)
	sb.WriteString(admURL)
	sb.WriteString("]]></VASTAdTagURI>\n")
	sb.WriteString(impressions)
	sb.WriteString(`   <Creatives>
    <Creative>
     <Linear>
`)
	sb.WriteString(tracking)
	sb.WriteString(`     </Linear>
    </Creative>
   </Creatives>
  </Wrapper>
 </Ad>
</VAST>`)
	return sb.String()
}

// buildPassthrough takes a complete VAST XML document from the DSP and
// injects SSP impression pixels only. The DSP's own TrackingEvents and
// creative structure are preserved untouched.
func buildPassthrough(bid *openrtb.Bid, req *openrtb.BidRequest, baseURL string) string {
	xml := bid.SubstituteMacrosRaw(strings.TrimSpace(bid.Adm))

	evtBase := baseURL + "/api/v1/event"
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
