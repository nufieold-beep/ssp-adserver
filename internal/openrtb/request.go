package openrtb

import (
	"encoding/json"
	"net/url"
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
	defaultMimes     = []string{"video/mp4"}
	defaultProtocols = []adcom1.MediaCreativeSubtype{2, 3, 5, 6, 7, 8}
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

func buildDefaultSource(requestID string) *Source {
	if defaultSChain == nil {
		return nil
	}
	sourceCopy := *defaultSChain
	if defaultSChain.SChain != nil {
		schainCopy := *defaultSChain.SChain
		if len(defaultSChain.SChain.Nodes) > 0 {
			schainCopy.Nodes = append([]SChainNode(nil), defaultSChain.SChain.Nodes...)
		}
		sourceCopy.SChain = &schainCopy
	}
	sourceCopy.TID = requestID
	return &sourceCopy
}

// BuildFromHTTP constructs a CTV/in-app video BidRequest from query params
// with common app/CTV HTTP headers as fallback values.
func BuildFromHTTP(c *fiber.Ctx) BidRequest {
	queries := c.Queries()

	w := queryIntMap(queries, 1920, "w")
	h := queryIntMap(queries, 1080, "h")
	minDur := queryIntMap(queries, requestDefaults.MinDur, "min_dur", "minduration")
	maxDur := queryIntMap(queries, requestDefaults.MaxDur, "max_dur", "maxduration")

	skippable := 0
	if queryMap(queries, "skip") == "1" {
		skippable = 1
	}

	tagID := requestValue(c, queries, []string{"sid", "tagid"}, "X-Tag-ID", "Tag-ID", "X-Slot-ID", "Slot-ID", "X-Supply-ID", "Supply-ID")
	if tagID == "" {
		tagID = c.Params("tag")
	}

	deviceType := requestDeviceTypeValue(c, queries, 3) // CTV default
	language := requestLanguageValue(c, queries, "en")

	dnt := requestIntValue(c, queries, 0, []string{"dnt"}, "DNT", "X-Device-DNT")
	lmt := requestIntValue(c, queries, 0, []string{"lmt"}, "X-Device-LMT", "LMT")
	ip := requestValue(c, queries, []string{"ip", "uip"}, "X-Device-IP", "X-Forwarded-For", "X-Real-IP", "CF-Connecting-IP", "True-Client-IP")
	if ip == "" {
		ip = c.IP()
	}
	if strings.Contains(ip, ",") {
		ip = strings.TrimSpace(strings.SplitN(ip, ",", 2)[0])
	}
	ua := requestValue(c, queries, []string{"ua"}, "X-Device-User-Agent", "X-Original-User-Agent", "User-Agent")
	ifa := requestValue(c, queries, []string{"ifa"}, "X-Device-IFA", "X-IFA", "IFA")
	bundle := requestValue(c, queries, []string{"app_bundle", "bundle"}, "X-App-Bundle", "App-Bundle", "X-Bundle-ID", "Bundle-ID", "Bundle")
	if bundle == "" && tagID != "" {
		bundle = "supply." + normalizeBundleToken(tagID)
	}
	if bundle == "" {
		bundle = "app.unknown"
	}
	deviceOS := requestValue(c, queries, []string{"os"}, "X-Device-OS", "Device-OS")
	deviceMake := requestValue(c, queries, []string{"device_make"}, "X-Device-Make", "Device-Make")
	deviceModel := requestValue(c, queries, []string{"device_model"}, "X-Device-Model", "Device-Model")
	deviceOSV := requestValue(c, queries, []string{"osv"}, "X-Device-OSV", "X-Device-OS-Version", "Device-OSV", "Device-OS-Version")

	reqID := uuid.NewString()

	country := requestValue(c, queries, []string{"country_code", "country"}, "X-Country-Code", "Country-Code", "CF-IPCountry")
	if len(country) == 2 {
		country = ToAlpha3(country)
	}

	startDelay := queryIntMap(queries, 0, "startdelay")
	placement := queryIntMap(queries, 1, "placement")
	plcmt := queryIntMap(queries, placement, "plcmt")
	playbackMethods := queryEnumIntList(queryMap(queries, "playmethod", "playbackmethod"))
	startDelayMode := adcom1.StartDelay(startDelay)
	placementSubtype := adcom1.VideoPlacementSubtype(placement)
	plcmtSubtype := adcom1.VideoPlcmtSubtype(plcmt)

	req := BidRequest{
		ID:      reqID,
		TMax:    int64(queryIntMap(queries, 0, "tmax")),
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
			Name:      requestValue(c, queries, []string{"app_name"}, "X-App-Name", "App-Name"),
			Bundle:    bundle,
			StoreURL:  requestValue(c, queries, []string{"app_store_url", "storeurl"}, "X-App-Store-URL", "App-Store-URL", "X-Store-URL"),
			Ver:       requestValue(c, queries, []string{"app_ver"}, "X-App-Version", "App-Version"),
			Publisher: &Publisher{ID: tagID},
			Content:   &Content{Language: language, LiveStream: int8Ptr(1)},
		},
		Device: &Device{
			DNT:        int8Ptr(dnt),
			UA:         ua,
			IP:         ip,
			Geo:        &Geo{Country: country, Region: requestValue(c, queries, []string{"region"}, "X-Region", "Region"), City: requestValue(c, queries, []string{"city"}, "X-City", "City"), ZIP: requestValue(c, queries, []string{"zip"}, "X-Postal-Code", "Postal-Code", "X-ZIP", "ZIP"), Type: adcom1.LocationType(2)},
			Make:       deviceMake,
			Model:      deviceModel,
			OS:         deviceOS,
			OSV:        deviceOSV,
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
		Source: buildDefaultSource(reqID),
	}

	if ifaType := detectIFAType(ua, deviceMake, deviceOS); ifaType != "" {
		req.Device.Ext = buildIFATypeExt(ifaType)
	}

	if ifa != "" {
		req.User = &User{ID: ifa}
	}

	if ct := requestValue(c, queries, []string{"connectiontype"}, "X-Connection-Type", "Connection-Type"); ct != "" {
		if parsed, err := strconv.Atoi(ct); err == nil {
			connType := adcom1.ConnectionType(parsed)
			req.Device.ConnectionType = &connType
		}
	}

	if ctGenre := queryMap(queries, "ct_genre"); ctGenre != "" {
		cats := strings.Split(ctGenre, ",")
		req.App.Cat = cats
		if req.App.Content == nil {
			req.App.Content = &Content{}
		}
		req.App.Content.Genre = ctGenre
		req.App.Content.Cat = cats
	}

	if coppa := requestValue(c, queries, []string{"coppa"}, "X-COPPA", "COPPA"); coppa != "" {
		if parsed, err := strconv.Atoi(coppa); err == nil {
			req.Regs.COPPA = int8(parsed)
		}
	}
	if usPriv := requestValue(c, queries, []string{"us_privacy"}, "X-US-Privacy", "US-Privacy"); usPriv != "" {
		req.Regs.USPrivacy = usPriv
	}
	if gdpr := requestValue(c, queries, []string{"gdpr"}, "X-GDPR", "GDPR"); gdpr != "" {
		if parsed, err := strconv.Atoi(gdpr); err == nil {
			gdprFlag := int8(parsed)
			req.Regs.GDPR = &gdprFlag
		}
	}

	return req
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

// CanonicalBundleValue normalizes a bundle candidate and returns it only when it
// looks like a canonical bundle/package identifier.
func CanonicalBundleValue(value string) string {
	value = normalizeCleanBundleValue(value)
	if !looksCanonicalCleanBundle(value) {
		return ""
	}
	return value
}

// BundleFromStoreURL derives the cleanest bundle candidate available from a
// store URL or domain-like value.
func BundleFromStoreURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		return CanonicalBundleValue(raw)
	}

	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}

	if candidate := bundleFromPlatformStoreURL(u); candidate != "" {
		return candidate
	}

	for _, key := range []string{"id", "app_id", "appid", "bundle", "bundle_id", "package", "package_name", "pkg"} {
		if candidate := CanonicalBundleValue(u.Query().Get(key)); candidate != "" {
			return candidate
		}
	}

	for _, segment := range strings.Split(u.Path, "/") {
		if candidate := canonicalStorePathSegment(segment); candidate != "" {
			return candidate
		}
	}

	host := normalizeCleanBundleValue(trimCommonHostPrefix(u.Hostname()))
	if host == "" || isGenericStoreHost(host) || !looksCanonicalCleanBundle(host) {
		for _, segment := range strings.Split(u.Path, "/") {
			if candidate := CanonicalBundleValue(segment); candidate != "" {
				return candidate
			}
		}
		return ""
	}
	return host
}

// DecodeStoreURLValue decodes a fully encoded app store URL before it is
// forwarded to downstream ORTB endpoints.
func DecodeStoreURLValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || looksHTTPStoreURL(value) {
		return value
	}

	decoded := value
	for i := 0; i < 3; i++ {
		next, err := url.QueryUnescape(decoded)
		if err != nil {
			break
		}
		next = strings.TrimSpace(next)
		if next == "" || next == decoded {
			break
		}
		decoded = next
		if looksHTTPStoreURL(decoded) {
			return decoded
		}
	}

	return value
}

// CleanBundleValue returns the best clean bundle candidate available for
// analytics/reporting without mutating the original request payload.
func CleanBundleValue(bundle, appID, storeURL string) string {
	if candidate := CanonicalBundleValue(bundle); candidate != "" {
		return candidate
	}
	if candidate := BundleFromStoreURL(storeURL); candidate != "" {
		return candidate
	}
	if candidate := CanonicalBundleValue(appID); candidate != "" {
		return candidate
	}
	return ""
}

func normalizeCleanBundleValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	if looksEncodedURLLikeBundleValue(value) {
		return ""
	}

	hadScheme := false
	if idx := strings.Index(value, "://"); idx >= 0 {
		value = value[idx+3:]
		hadScheme = true
	}
	if idx := strings.IndexAny(value, "?#"); idx >= 0 {
		value = value[:idx]
	}
	if hadScheme {
		if idx := strings.Index(value, "/"); idx >= 0 {
			value = value[:idx]
		}
	}

	var b strings.Builder
	b.Grow(len(value))
	lastDot := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDot = false
		case r == '.' || r == '-' || r == '_' || r == ' ' || r == '/' || r == '\\' || r == ':':
			if b.Len() > 0 && !lastDot {
				b.WriteByte('.')
				lastDot = true
			}
		}
	}

	normalized := strings.Trim(b.String(), ".")
	if normalized == "" || isSyntheticGeneratedBundle(normalized) {
		return ""
	}
	return normalized
}

func looksCanonicalCleanBundle(value string) bool {
	if value == "" {
		return false
	}
	if !strings.Contains(value, ".") {
		return looksSingleTokenBundleValue(value)
	}

	parts := strings.Split(value, ".")
	if len(parts) < 2 {
		return false
	}

	alphaParts := 0
	for _, part := range parts {
		if part == "" {
			return false
		}
		if containsAlpha(part) {
			alphaParts++
		}
	}
	if alphaParts < 1 {
		return false
	}

	if len(parts) >= 3 {
		return true
	}

	return len(parts[0]) >= 2 && len(parts[1]) >= 2
}

func looksSingleTokenBundleValue(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return false
	}
	if isAllDigits(value) {
		return true
	}
	if len(value) == 10 && strings.HasPrefix(value, "b") && isAlphaNumeric(value) {
		return true
	}
	if len(value) >= 8 && strings.HasPrefix(value, "g") && isAllDigits(value[1:]) {
		return true
	}
	if len(value) == 32 && isHexToken(value) {
		return true
	}
	if isAlphaOnly(value) && len(value) >= 3 {
		return true
	}
	return false
}

func isSyntheticGeneratedBundle(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return true
	}
	return value == "app.unknown" || strings.HasPrefix(value, "supply.")
}

func containsAlpha(value string) bool {
	for _, r := range value {
		if r >= 'a' && r <= 'z' {
			return true
		}
	}
	return false
}

func isAlphaOnly(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}

func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isAlphaNumeric(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

func isHexToken(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func isKnownBundleRoot(value string) bool {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "amazon", "app", "co", "com", "dev", "io", "lgappstv", "me", "media", "net", "org", "roku", "samsung", "tv", "vizio":
		return true
	default:
		return false
	}
}

func bundleFromPlatformStoreURL(u *url.URL) string {
	if u == nil {
		return ""
	}

	host := trimCommonHostPrefix(u.Hostname())
	prefix := platformStoreBundlePrefix(host)
	if prefix == "" {
		return ""
	}

	for _, key := range []string{"appName", "appname", "app_name", "name", "title"} {
		if token := normalizeBundleToken(u.Query().Get(key)); token != "" && token != "unknown" {
			return prefix + "." + token
		}
	}

	return ""
}

func canonicalStorePathSegment(segment string) string {
	segment = strings.TrimSpace(strings.ToLower(segment))
	if segment == "" {
		return ""
	}
	if strings.HasPrefix(segment, "id") {
		candidate := strings.TrimPrefix(segment, "id")
		if isAllDigits(candidate) {
			return candidate
		}
	}
	return ""
}

func platformStoreBundlePrefix(host string) string {
	host = trimCommonHostPrefix(host)
	switch {
	case host == "vizio.com", strings.HasSuffix(host, ".vizio.com"):
		return "vizio"
	case host == "roku.com", strings.HasSuffix(host, ".roku.com"):
		return "roku"
	case host == "samsung.com", strings.HasSuffix(host, ".samsung.com"):
		return "samsung"
	case host == "amazon.com", strings.HasSuffix(host, ".amazon.com"):
		return "amazon"
	case host == "lgappstv.com", strings.HasSuffix(host, ".lgappstv.com"):
		return "lgappstv"
	default:
		return ""
	}
}

func looksEncodedURLLikeBundleValue(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return false
	}
	if strings.Contains(value, "://") || strings.Contains(value, "%3a%2f%2f") || strings.Contains(value, "3a2f2f") {
		return true
	}
	if strings.HasPrefix(value, "http") && (strings.Contains(value, "%2f") || strings.Contains(value, "2f")) {
		return true
	}
	for _, marker := range []string{".com2f", ".net2f", ".org2f", ".tv2f", "appname3d", "appid3d"} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func trimCommonHostPrefix(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	return strings.TrimPrefix(host, "www.")
}

func isGenericStoreHost(host string) bool {
	host = trimCommonHostPrefix(host)
	switch {
	case host == "play.google.com":
		return true
	case host == "apps.apple.com":
		return true
	case host == "itunes.apple.com":
		return true
	case host == "amazon.com", strings.HasSuffix(host, ".amazon.com"):
		return true
	case host == "roku.com", strings.HasSuffix(host, ".roku.com"):
		return true
	case host == "lgappstv.com", strings.HasSuffix(host, ".lgappstv.com"):
		return true
	case host == "samsung.com", strings.HasSuffix(host, ".samsung.com"):
		return true
	default:
		return false
	}
}

func looksHTTPStoreURL(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Scheme, "http") || strings.EqualFold(u.Scheme, "https")
}

func requestValue(c *fiber.Ctx, queries map[string]string, queryKeys []string, headerKeys ...string) string {
	if value := strings.TrimSpace(queryMap(queries, queryKeys...)); value != "" {
		return value
	}
	return headerValue(c, headerKeys...)
}

func requestIntValue(c *fiber.Ctx, queries map[string]string, def int, queryKeys []string, headerKeys ...string) int {
	if value, ok := parseIntValue(queryMap(queries, queryKeys...)); ok {
		return value
	}
	for _, key := range headerKeys {
		if value, ok := parseIntValue(headerValue(c, key)); ok {
			return value
		}
	}
	return def
}

func requestDeviceTypeValue(c *fiber.Ctx, queries map[string]string, def int) int {
	if value, ok := parseDeviceTypeValue(queryMap(queries, "device_type", "devicetype")); ok {
		return value
	}
	for _, key := range []string{"X-Device-Type", "Device-Type", "X-Inventory-Device-Type"} {
		if value, ok := parseDeviceTypeValue(headerValue(c, key)); ok {
			return value
		}
	}
	return def
}

func requestLanguageValue(c *fiber.Ctx, queries map[string]string, def string) string {
	if value := requestValue(c, queries, []string{"ct_lang", "lang"}, "X-Device-Language", "Device-Language", "Accept-Language", "Content-Language"); value != "" {
		return normalizeLanguageValue(value)
	}
	return def
}

func headerValue(c *fiber.Ctx, keys ...string) string {
	if c == nil {
		return ""
	}
	for _, key := range keys {
		if value := strings.TrimSpace(c.Get(key)); value != "" {
			return value
		}
	}
	return ""
}

func parseIntValue(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func parseDeviceTypeValue(raw string) (int, bool) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch raw {
	case "ctv", "connectedtv", "connected-tv", "connected_tv", "smarttv", "smart-tv", "smart_tv", "tv":
		return 3, true
	case "stb", "settopbox", "set-top-box", "set_top_box":
		return 7, true
	case "mobile", "phone", "smartphone":
		return 4, true
	case "tablet":
		return 5, true
	}
	return parseIntValue(raw)
}

func normalizeLanguageValue(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if idx := strings.IndexAny(raw, ",;"); idx >= 0 {
		raw = raw[:idx]
	}
	return strings.TrimSpace(raw)
}

func queryMap(queries map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := queries[key]; value != "" {
			return value
		}
	}
	return ""
}

func queryMapDefault(queries map[string]string, def string, keys ...string) string {
	if value := queryMap(queries, keys...); value != "" {
		return value
	}
	return def
}

func queryIntMap(queries map[string]string, def int, keys ...string) int {
	for _, key := range keys {
		value := queries[key]
		if value == "" {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			continue
		}
		return parsed
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

func buildIFATypeExt(ifaType string) json.RawMessage {
	if ifaType == "" {
		return nil
	}
	return json.RawMessage(`{"ifa_type":"` + ifaType + `"}`)
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
