package openrtb

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/prebid/openrtb/v20/adcom1"
	openrtb2 "github.com/prebid/openrtb/v20/openrtb2"
)

// RequestDefaults controls generated OpenRTB request defaults.
// Configured at startup via ConfigureRequestDefaults.
type RequestDefaults struct {
	BidFloor float64
	MinDur   int
	MaxDur   int
}

var requestDefaults = RequestDefaults{
	BidFloor: 0.50,
	MinDur:   5,
	MaxDur:   30,
}

// Pre-allocated shared slices for BuildFromHTTP — avoids heap allocation per request.
var (
	defaultCur       = []string{"USD"}
	defaultMimes     = []string{"video/mp4", "video/webm", "video/ogg", "application/x-mpegURL"}
	defaultProtocols = []adcom1.MediaCreativeSubtype{2, 3, 5, 6, 7, 8, 11, 12} // VAST 2-4 inline+wrapper
)

var alpha2To3Country = map[string]string{
	"US": "USA", "GB": "GBR", "CA": "CAN", "AU": "AUS", "DE": "DEU",
	"FR": "FRA", "JP": "JPN", "CN": "CHN", "IN": "IND", "BR": "BRA",
	"MX": "MEX", "RU": "RUS", "KR": "KOR", "IT": "ITA", "ES": "ESP",
	"NL": "NLD", "SE": "SWE", "NO": "NOR", "DK": "DNK", "FI": "FIN",
	"PL": "POL", "AT": "AUT", "CH": "CHE", "BE": "BEL", "IE": "IRL",
	"PT": "PRT", "NZ": "NZL", "SG": "SGP", "HK": "HKG", "TW": "TWN",
	"IL": "ISR", "AE": "ARE", "SA": "SAU", "ZA": "ZAF", "AR": "ARG",
	"CL": "CHL", "CO": "COL", "PH": "PHL", "TH": "THA", "MY": "MYS",
	"ID": "IDN", "VN": "VNM", "TR": "TUR", "EG": "EGY", "NG": "NGA",
	"KE": "KEN", "PK": "PAK", "BD": "BGD", "UA": "UKR", "RO": "ROU",
	"CZ": "CZE", "HU": "HUN", "GR": "GRC", "HR": "HRV", "BG": "BGR",
	"SK": "SVK", "SI": "SVN", "LT": "LTU", "LV": "LVA", "EE": "EST",
	"PE": "PER", "EC": "ECU", "VE": "VEN", "DO": "DOM", "PR": "PRI",
	"CR": "CRI", "PA": "PAN", "GT": "GTM", "CU": "CUB", "JM": "JAM",
}

// ConfigureRequestDefaults applies startup defaults for BuildFromHTTP.
func ConfigureRequestDefaults(d RequestDefaults) {
	if d.BidFloor > 0 {
		requestDefaults.BidFloor = d.BidFloor
	}
	if d.MinDur > 0 {
		requestDefaults.MinDur = d.MinDur
	}
	if d.MaxDur > 0 {
		requestDefaults.MaxDur = d.MaxDur
	}
}

// ── OpenRTB 2.6 BidRequest (CTV-focused, per spec) ──

type BidRequest = openrtb2.BidRequest
type Imp = openrtb2.Imp
type Video = openrtb2.Video
type Banner = openrtb2.Banner
type App = openrtb2.App
type Publisher = openrtb2.Publisher
type Content = openrtb2.Content
type Site = openrtb2.Site
type Geo = openrtb2.Geo
type Device = openrtb2.Device
type User = openrtb2.User
type Regs = openrtb2.Regs
type Source = openrtb2.Source
type SChain = openrtb2.SupplyChain
type SChainNode = openrtb2.SupplyChainNode
type UserAgent = openrtb2.UserAgent
type BrandVersion = openrtb2.BrandVersion

// defaultSChain is shared across all requests (immutable).
var defaultSChain = &Source{
	SChain: &SChain{
		Complete: 1,
		Ver:      "1.0",
		Nodes: []SChainNode{
			{ASI: "viadsmedia.com", SID: "pub-001", HP: int8Ptr(1)},
		},
	},
}

// BuildFromHTTP constructs a CTV/in-app video BidRequest from query params.
func BuildFromHTTP(c *fiber.Ctx) BidRequest {
	w := queryInt(c, "w", 1920)
	h := queryInt(c, "h", 1080)
	minDur := queryIntFallback(c, "min_dur", "minduration", requestDefaults.MinDur)
	maxDur := queryIntFallback(c, "max_dur", "maxduration", requestDefaults.MaxDur)

	skippable := 0
	if c.Query("skip") == "1" {
		skippable = 1
	}

	tagID := c.Query("sid", c.Query("tagid", c.Params("tag")))

	deviceType := 3 // CTV default
	if dt := c.Query("device_type", c.Query("devicetype")); dt != "" {
		deviceType, _ = strconv.Atoi(dt)
	}
	language := c.Query("ct_lang", c.Query("lang", "en"))

	dnt := queryInt(c, "dnt", 0)
	lmt := queryInt(c, "lmt", 0)
	ip := c.Query("ip", c.Query("uip", c.IP()))
	ua := c.Query("ua", c.Get("User-Agent"))
	ifa := c.Query("ifa")
	bundle := c.Query("app_bundle", c.Query("bundle"))
	if bundle == "" && tagID != "" {
		bundle = "supply." + normalizeBundleToken(tagID)
	}
	if bundle == "" {
		bundle = "app.unknown"
	}
	deviceOS := c.Query("os")
	deviceMake := c.Query("device_make")

	reqID := uuid.NewString()

	country := c.Query("country_code", c.Query("country"))
	if len(country) == 2 {
		country = ToAlpha3(country)
	}

	startDelay := queryInt(c, "startdelay", 0)
	placement := queryInt(c, "placement", 1)
	plcmt := queryInt(c, "plcmt", placement)
	playbackMethods := queryEnumIntList(c.Query("playmethod", c.Query("playbackmethod")))
	startDelayMode := adcom1.StartDelay(startDelay)
	placementSubtype := adcom1.VideoPlacementSubtype(placement)
	plcmtSubtype := adcom1.VideoPlcmtSubtype(plcmt)

	req := BidRequest{
		ID:      reqID,
		TMax:    int64(queryInt(c, "tmax", 0)),
		AT:      1,
		AllImps: 0,
		Cur:     defaultCur,
		Imp: []Imp{
			{
				ID:          reqID,
				BidFloor:    requestDefaults.BidFloor,
				BidFloorCur: "USD",
				Secure:      int8Ptr(0),
				TagID:       tagID,
				Video: &Video{
					MIMEs:          defaultMimes,
					Linearity:      adcom1.LinearityMode(1),
					MinDuration:    int64(minDur),
					MaxDuration:    int64(maxDur),
					Protocols:      defaultProtocols,
					W:              int64Ptr(int64(w)),
					H:              int64Ptr(int64(h)),
					Skip:           int8Ptr(skippable),
					Sequence:       1,
					BoxingAllowed:  int8Ptr(1),
					Placement:      placementSubtype,
					Plcmt:          plcmtSubtype,
					PlaybackMethod: playbackMethods,
					StartDelay:     &startDelayMode,
				},
			},
		},
		App: &App{
			ID:        bundle,
			Name:      c.Query("app_name"),
			Bundle:    bundle,
			StoreURL:  c.Query("app_store_url", c.Query("storeurl")),
			Ver:       c.Query("app_ver"),
			Publisher: &Publisher{ID: tagID},
			Content:   &Content{Language: language, LiveStream: int8Ptr(1)},
		},
		Device: &Device{
			DNT:        int8Ptr(dnt),
			UA:         ua,
			IP:         ip,
			Geo:        &Geo{Country: country, Region: c.Query("region"), City: c.Query("city"), ZIP: c.Query("zip"), Type: adcom1.LocationType(2)},
			Make:       deviceMake,
			Model:      c.Query("device_model"),
			OS:         deviceOS,
			OSV:        c.Query("osv"),
			DeviceType: adcom1.DeviceType(deviceType),
			IFA:        ifa,
			Lmt:        int8Ptr(lmt),
			W:          int64(w),
			H:          int64(h),
			Language:   language,
			SUA:        buildSUAFromUserAgent(ua, deviceType, deviceMake, deviceOS),
		},
		Regs: &Regs{
			COPPA:  0,
			GPPSID: []int8{0},
		},
		Source: cloneSource(defaultSChain),
	}

	if ifaType := detectIFAType(ua, deviceMake, deviceOS); ifaType != "" {
		req.Device.Ext = marshalRawJSON(map[string]string{"ifa_type": ifaType})
	}

	if ifa != "" {
		req.User = &User{ID: ifa}
	}

	if ct := c.Query("connectiontype"); ct != "" {
		if parsed, err := strconv.Atoi(ct); err == nil {
			connType := adcom1.ConnectionType(parsed)
			req.Device.ConnectionType = &connType
		}
	}

	if ctGenre := c.Query("ct_genre"); ctGenre != "" {
		cats := strings.Split(ctGenre, ",")
		req.App.Cat = cats
		if req.App.Content == nil {
			req.App.Content = &Content{}
		}
		req.App.Content.Genre = ctGenre
		req.App.Content.Cat = cats
	}

	if coppa := c.Query("coppa"); coppa != "" {
		if parsed, err := strconv.Atoi(coppa); err == nil {
			req.Regs.COPPA = int8(parsed)
		}
	}
	if usPriv := c.Query("us_privacy"); usPriv != "" {
		req.Regs.USPrivacy = usPriv
	}
	if gdpr := c.Query("gdpr"); gdpr != "" {
		if parsed, err := strconv.Atoi(gdpr); err == nil {
			gdprFlag := int8(parsed)
			req.Regs.GDPR = &gdprFlag
		}
	}

	return req
}

func cloneSource(src *Source) *Source {
	if src == nil {
		return nil
	}
	out := *src
	if src.SChain != nil {
		sChainCopy := *src.SChain
		if len(src.SChain.Nodes) > 0 {
			sChainCopy.Nodes = make([]SChainNode, len(src.SChain.Nodes))
			copy(sChainCopy.Nodes, src.SChain.Nodes)
		}
		out.SChain = &sChainCopy
	}
	return &out
}

func normalizeBundleToken(v string) string {
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(" ", ".", "/", ".", "\\", ".", "_", ".", ":", ".", "-", ".")
	v = replacer.Replace(v)
	for strings.Contains(v, "..") {
		v = strings.ReplaceAll(v, "..", ".")
	}
	v = strings.Trim(v, ".")
	if v == "" {
		return "unknown"
	}
	return v
}

// queryInt parses a query param as int with a default.
func queryInt(c *fiber.Ctx, key string, def int) int {
	v := c.Query(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// queryIntFallback tries primary key, then fallback key, then default.
func queryIntFallback(c *fiber.Ctx, primary, fallback string, def int) int {
	if v := c.Query(primary); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	if v := c.Query(fallback); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// queryEnumIntList parses comma/pipe/space separated integer enum values.
func queryEnumIntList(raw string) []adcom1.PlaybackMethod {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	tokens := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '|' || r == ' ' || r == '\t'
	})
	if len(tokens) == 0 {
		return nil
	}
	out := make([]adcom1.PlaybackMethod, 0, len(tokens))
	for _, token := range tokens {
		n, err := strconv.Atoi(strings.TrimSpace(token))
		if err != nil {
			continue
		}
		out = append(out, adcom1.PlaybackMethod(n))
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// buildSUAFromUserAgent builds device.sua from user-agent and device hints.
func buildSUAFromUserAgent(ua string, deviceType int, make, os string) *UserAgent {
	uaL := strings.ToLower(ua)
	platformBrand := detectPlatformBrand(uaL, make, os)
	browserBrand, browserVer := detectBrowserBrandVersionFromUA(ua, uaL, platformBrand, make, os)
	mobile := detectMobileFlag(uaL, deviceType)
	browser := BrandVersion{Brand: browserBrand}
	if browserVer != "" {
		browser.Version = []string{browserVer}
	}

	return &UserAgent{
		Browsers: []BrandVersion{browser},
		Platform: &BrandVersion{Brand: platformBrand},
		Mobile:   &mobile,
		Source:   adcom1.UserAgentSource(3),
	}
}

func detectMobileFlag(uaL string, deviceType int) int8 {
	switch deviceType {
	case 1, 4, 5:
		return 1
	case 3, 7:
		return 0
	}
	if strings.Contains(uaL, "mobile") && !strings.Contains(uaL, "tablet") && !strings.Contains(uaL, "tv") {
		return 1
	}
	return 0
}

func int8Ptr(v int) *int8 {
	x := int8(v)
	return &x
}

func int64Ptr(v int64) *int64 {
	x := v
	return &x
}

func marshalRawJSON(v interface{}) json.RawMessage {
	if v == nil {
		return nil
	}
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return encoded
}

func detectPlatformBrand(uaL, make, os string) string {
	switch {
	case strings.Contains(uaL, "aft") || strings.Contains(uaL, "fire tv") || strings.Contains(uaL, "amazon"):
		return "Android"
	case strings.Contains(uaL, "tizen"):
		return "Tizen"
	case strings.Contains(uaL, "webos"):
		return "webOS"
	case strings.Contains(uaL, "roku"):
		return "Roku"
	case strings.Contains(uaL, "android"):
		return "Android"
	case strings.Contains(uaL, "iphone") || strings.Contains(uaL, "ipad") || strings.Contains(uaL, "ios"):
		return "iOS"
	case strings.Contains(uaL, "windows"):
		return "Windows"
	case strings.Contains(uaL, "mac os x") || strings.Contains(uaL, "macintosh"):
		return "macOS"
	case strings.Contains(uaL, "linux"):
		return "Linux"
	}

	if os = strings.TrimSpace(os); os != "" {
		return os
	}
	if make = strings.TrimSpace(make); make != "" {
		return make
	}
	return "Android"
}

func detectBrowserBrandVersionFromUA(ua, uaL, platformBrand, make, os string) (string, string) {
	switch {
	case strings.Contains(uaL, "aft") || strings.Contains(uaL, "fire tv") || strings.Contains(uaL, "amazon"):
		if v := extractUATokenVersion(ua, "Silk/"); v != "" {
			return "AmazonFireStick", v
		}
		return "AmazonFireStick", ""
	case strings.Contains(uaL, "applecoremedia"):
		if v := extractUATokenVersion(ua, "AppleCoreMedia/"); v != "" {
			return "AppleTV", v
		}
		return "AppleTV", ""
	case strings.Contains(uaL, "roku"):
		if v := extractUATokenVersion(ua, "Roku/"); v != "" {
			return "Roku", v
		}
		return "Roku", ""
	case strings.Contains(uaL, "tizen") || (strings.Contains(uaL, "samsung") && strings.Contains(uaL, "tv")):
		if v := extractUATokenVersion(ua, "Tizen "); v != "" {
			return "SamsungTV", v
		}
		return "SamsungTV", ""
	case strings.Contains(uaL, "webos") || (strings.Contains(uaL, "lg") && strings.Contains(uaL, "tv")):
		if v := extractUATokenVersion(ua, "Web0S/"); v != "" {
			return "LGTV", v
		}
		if v := extractUATokenVersion(ua, "webOS/"); v != "" {
			return "LGTV", v
		}
		return "LGTV", ""
	}

	tokens := []struct {
		Token string
		Brand string
	}{
		{Token: "Edg/", Brand: "Edge"},
		{Token: "OPR/", Brand: "Opera"},
		{Token: "CriOS/", Brand: "Chrome"},
		{Token: "Chrome/", Brand: "Chrome"},
		{Token: "FxiOS/", Brand: "Firefox"},
		{Token: "Firefox/", Brand: "Firefox"},
		{Token: "Version/", Brand: "Safari"},
	}

	for _, t := range tokens {
		if v := extractUATokenVersion(ua, t.Token); v != "" {
			if t.Brand == "Safari" && !strings.Contains(uaL, "safari") {
				continue
			}
			return t.Brand, v
		}
	}

	// Fallback heuristics for SDK/app UAs without explicit browser tokens.
	switch strings.ToLower(platformBrand) {
	case "tvos", "ios", "macos":
		return "Safari", ""
	case "android":
		if strings.Contains(uaL, "fire") || strings.Contains(strings.ToLower(make), "amazon") {
			return "AmazonFireStick", ""
		}
		return "Chrome", ""
	case "tizen":
		return "SamsungTV", ""
	case "webos":
		return "LGTV", ""
	case "roku":
		return "Roku", ""
	}

	if strings.EqualFold(strings.TrimSpace(os), "tvOS") {
		return "Safari", ""
	}

	return "Unknown", ""
}

func extractUATokenVersion(ua, token string) string {
	idx := strings.Index(ua, token)
	if idx == -1 {
		return ""
	}
	rest := ua[idx+len(token):]
	if rest == "" {
		return ""
	}
	end := len(rest)
	for i, ch := range rest {
		if (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' {
			continue
		}
		end = i
		break
	}
	if end <= 0 {
		return ""
	}
	return strings.Trim(rest[:end], "._")
}

// detectIFAType returns the IFA type based on device OS, make, and user-agent.
func detectIFAType(ua, make, os string) string {
	osL := strings.ToLower(os)
	makeL := strings.ToLower(make)
	uaL := strings.ToLower(ua)

	switch {
	case osL == "ios" || strings.Contains(uaL, "iphone") || strings.Contains(uaL, "ipad") || strings.Contains(uaL, "apple"):
		return "idfa"
	case osL == "android" || strings.Contains(uaL, "android"):
		return "gaid"
	case strings.Contains(uaL, "tizen") || strings.Contains(makeL, "samsung"):
		return "tifa"
	case strings.Contains(uaL, "webos") || strings.Contains(makeL, "lg"):
		return "lgudid"
	case strings.Contains(makeL, "roku"):
		return "rida"
	case strings.Contains(makeL, "amazon") || strings.Contains(uaL, "fire"):
		return "afai"
	case strings.Contains(makeL, "vizio"):
		return "vtifa"
	}
	return ""
}

// ToAlpha3 converts ISO 3166-1 alpha-2 to alpha-3 country codes.
func ToAlpha3(code string) string {
	if v, ok := alpha2To3Country[strings.ToUpper(code)]; ok {
		return v
	}
	return strings.ToUpper(code)
}
