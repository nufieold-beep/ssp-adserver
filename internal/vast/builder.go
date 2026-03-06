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

// ImpressionCtx carries request-level context for impression tracking URLs.
type ImpressionCtx struct {
	DemandID string // demand source / campaign ID
	Country  string // ISO country code
	IP       string
	Env      string // ctv, mobile, web
	Source   string // supply source domain
	Bundle   string // app bundle
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
// Auto-detects the Adm content type and generates InLine, Wrapper,
// or passthrough VAST accordingly, always injecting SSP tracking pixels.
func Build(bid *openrtb.Bid, requestID, baseURL string, ictx *ImpressionCtx) string {
	if baseURL == "" {
		baseURL = BaseURL
	}
	if ictx == nil {
		ictx = &ImpressionCtx{}
	}
	switch DetectAdmType(bid.Adm) {
	case AdmPassthrough:
		return buildPassthrough(bid, requestID, baseURL, ictx)
	case AdmWrapper:
		return buildWrapper(bid, requestID, baseURL, ictx)
	default:
		return buildInline(bid, requestID, baseURL, ictx)
	}
}

// impressionBlock returns the SSP + DSP impression pixel XML fragment.
func impressionBlock(evtBase, requestID string, bid *openrtb.Bid, ictx *ImpressionCtx) string {
	adom := ""
	if len(bid.ADomain) > 0 {
		adom = bid.ADomain[0]
	}
	params := url.Values{}
	params.Set("rid", requestID)
	params.Set("cmp", ictx.DemandID)
	params.Set("crid", bid.CrID)
	params.Set("ctry", ictx.Country)
	params.Set("ip", ictx.IP)
	params.Set("env", ictx.Env)
	params.Set("sr", ictx.Source)
	params.Set("bndl", ictx.Bundle)
	params.Set("adom", adom)
	params.Set("price", fmt.Sprintf("%.6f", bid.Price))

	var sb strings.Builder
	fmt.Fprintf(&sb, "   <Impression><![CDATA[%s/impression?%s]]></Impression>\n",
		evtBase, params.Encode())
	if bid.BURL != "" {
		burl := bid.SubstituteMacros(bid.BURL)
		fmt.Fprintf(&sb, "   <Impression><![CDATA[%s]]></Impression>\n", burl)
	}
	return sb.String()
}

// buildInline creates a self-contained VAST InLine ad from a media file URL.
func buildInline(bid *openrtb.Bid, requestID, baseURL string, ictx *ImpressionCtx) string {
	evtBase := fmt.Sprintf("%s/api/v1/event", baseURL)
	impressions := impressionBlock(evtBase, requestID, bid, ictx)

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
func buildWrapper(bid *openrtb.Bid, requestID, baseURL string, ictx *ImpressionCtx) string {
	evtBase := fmt.Sprintf("%s/api/v1/event", baseURL)
	impressions := impressionBlock(evtBase, requestID, bid, ictx)
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
func buildPassthrough(bid *openrtb.Bid, requestID, baseURL string, ictx *ImpressionCtx) string {
	xml := html.UnescapeString(strings.TrimSpace(bid.Adm))
	evtBase := fmt.Sprintf("%s/api/v1/event", baseURL)
	impressions := impressionBlock(evtBase, requestID, bid, ictx)

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
