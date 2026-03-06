package openrtb

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
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
	defaultProtocols = []int{2, 3, 5, 6, 7, 8, 11, 12} // VAST 2-4 inline+wrapper
	defaultAPI       = []int{1, 2, 7}                    // VPAID 1, VPAID 2, OMID 1
	defaultPlayback  = []int{1, 2, 6}                    // auto-sound, auto-mute, enter-viewport
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

type BidRequest struct {
	ID      string   `json:"id"`
	Imp     []Imp    `json:"imp"`
	App     *App     `json:"app,omitempty"`
	Site    *Site    `json:"site,omitempty"`
	Device  Device   `json:"device"`
	User    *User    `json:"user,omitempty"`
	Regs    *Regs    `json:"regs,omitempty"`
	At      int      `json:"at,omitempty"` // 1=first-price, 2=second-price
	TMax    int      `json:"tmax,omitempty"`
	AllImps int      `json:"allimps"`
	Cur     []string `json:"cur,omitempty"`
	BAdv    []string `json:"badv,omitempty"`
	BCat    []string `json:"bcat,omitempty"`
	Ext     *ReqExt  `json:"ext,omitempty"`
}

type Imp struct {
	ID          string  `json:"id"`
	Video       *Video  `json:"video,omitempty"`
	Instl       int     `json:"instl"`
	BidFloor    float64 `json:"bidfloor"`
	BidFloorCur string  `json:"bidfloorcur,omitempty"`
	Secure      int     `json:"secure"`
	TagID       string  `json:"tagid,omitempty"`
	Ext         *ImpExt `json:"ext,omitempty"`
}

type ImpExt struct {
	Skadn interface{} `json:"skadn,omitempty"`
}

type Video struct {
	Mimes          []string `json:"mimes"`
	Linearity      int      `json:"linearity,omitempty"`
	MinDuration    int      `json:"minduration,omitempty"`
	MaxDuration    int      `json:"maxduration,omitempty"`
	Protocols      []int    `json:"protocols,omitempty"`
	W              int      `json:"w"`
	H              int      `json:"h"`
	Skip           int      `json:"skip"`
	Sequence       int      `json:"sequence,omitempty"`
	BoxingAllowed  int      `json:"boxingallowed"`
	Placement      int      `json:"placement,omitempty"`
	PlaybackMethod []int    `json:"playbackmethod,omitempty"`
	SkipMin        int      `json:"skipmin,omitempty"`
	SkipAfter      int      `json:"skipafter,omitempty"`
	StartDelay     *int     `json:"startdelay,omitempty"`
	API            []int    `json:"api,omitempty"`
	MaxExtended    int      `json:"maxextended,omitempty"`
	Pos            int      `json:"pos,omitempty"`
	CompanionAd    []Banner `json:"companionad,omitempty"`
	CompanionType  []int    `json:"companiontype,omitempty"`
}

type Banner struct {
	W   int    `json:"w,omitempty"`
	H   int    `json:"h,omitempty"`
	ID  string `json:"id,omitempty"`
	Pos int    `json:"pos,omitempty"`
}

type App struct {
	ID       string   `json:"id,omitempty"`
	Name     string   `json:"name,omitempty"`
	Bundle   string   `json:"bundle"`
	StoreURL string   `json:"storeurl,omitempty"`
	Cat      []string `json:"cat,omitempty"`
	Ver      string   `json:"ver,omitempty"`
	Content  *Content `json:"content,omitempty"`
}

type Content struct {
	ID         string   `json:"id,omitempty"`
	Title      string   `json:"title,omitempty"`
	Genre      string   `json:"genre,omitempty"`
	Cat        []string `json:"cat,omitempty"`
	Language   string   `json:"language,omitempty"`
	Len        int      `json:"len,omitempty"`
	LiveStream int      `json:"livestream,omitempty"`
}

type Site struct {
	Domain string `json:"domain,omitempty"`
	Page   string `json:"page,omitempty"`
}

type Geo struct {
	Lat       float64 `json:"lat,omitempty"`
	Lon       float64 `json:"lon,omitempty"`
	Country   string  `json:"country,omitempty"`
	Region    string  `json:"region,omitempty"`
	Metro     string  `json:"metro,omitempty"`
	City      string  `json:"city,omitempty"`
	Zip       string  `json:"zip,omitempty"`
	Type      int     `json:"type,omitempty"`
	Accuracy  int     `json:"accuracy,omitempty"`
	IPService int     `json:"ipservice,omitempty"`
}

type Device struct {
	DNT            int        `json:"dnt"`
	UA             string     `json:"ua"`
	IP             string     `json:"ip"`
	Geo            *Geo       `json:"geo,omitempty"`
	Carrier        string     `json:"carrier,omitempty"`
	Make           string     `json:"make,omitempty"`
	Model          string     `json:"model,omitempty"`
	OS             string     `json:"os,omitempty"`
	OSv            string     `json:"osv,omitempty"`
	JS             int        `json:"js"`
	DeviceType     int        `json:"devicetype,omitempty"`
	IFA            string     `json:"ifa,omitempty"`
	LMT            int        `json:"lmt"`
	W              int        `json:"w,omitempty"`
	H              int        `json:"h,omitempty"`
	Language       string     `json:"language,omitempty"`
	ConnectionType int        `json:"connectiontype,omitempty"`
	Ext            *DeviceExt `json:"ext,omitempty"`
}

type DeviceExt struct {
	IFAType string `json:"ifa_type,omitempty"`
}

type User struct {
	ID       string   `json:"id,omitempty"`
	BuyerUID string   `json:"buyeruid,omitempty"`
	Gender   string   `json:"gender,omitempty"`
	YOB      int      `json:"yob,omitempty"`
	Ext      *UserExt `json:"ext,omitempty"`
}

type UserExt struct {
	Consent string `json:"consent,omitempty"`
}

type Regs struct {
	COPPA int      `json:"coppa,omitempty"`
	Ext   *RegsExt `json:"ext,omitempty"`
}

type RegsExt struct {
	GDPR   int    `json:"gdpr,omitempty"`
	USPriv string `json:"us_privacy,omitempty"`
}

type SChain struct {
	Complete int          `json:"complete"`
	Nodes    []SChainNode `json:"nodes"`
	Ver      string       `json:"ver,omitempty"`
}

type SChainNode struct {
	ASI    string `json:"asi"`
	SID    string `json:"sid"`
	HP     int    `json:"hp"`
	RID    string `json:"rid,omitempty"`
	Domain string `json:"domain,omitempty"`
	Name   string `json:"name,omitempty"`
}

type ReqExt struct {
	SChain *SChain `json:"schain,omitempty"`
}

// defaultSChain is shared across all requests (immutable).
var defaultSChain = &ReqExt{
	SChain: &SChain{
		Complete: 1,
		Ver:      "1.0",
		Nodes: []SChainNode{
			{ASI: "viadsmedia.com", SID: "pub-001", HP: 1},
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
	deviceOS := c.Query("os")
	deviceMake := c.Query("device_make")

	reqID := uuid.NewString()

	country := c.Query("country_code", c.Query("country"))
	if len(country) == 2 {
		country = ToAlpha3(country)
	}

	startDelay := queryInt(c, "startdelay", 0)
	placement := queryInt(c, "placement", 1)

	req := BidRequest{
		ID:      reqID,
		TMax:    800,
		At:      1,
		AllImps: 1,
		Cur:     defaultCur,
		Imp: []Imp{
			{
				ID:          reqID,
				BidFloor:    requestDefaults.BidFloor,
				BidFloorCur: "USD",
				Secure:      1,
				TagID:       tagID,
				Video: &Video{
					Mimes:          defaultMimes,
					Linearity:      1,
					MinDuration:    minDur,
					MaxDuration:    maxDur,
					Protocols:      defaultProtocols,
					W:              w,
					H:              h,
					Skip:           skippable,
					Sequence:       1,
					BoxingAllowed:  1,
					Placement:      placement,
					StartDelay:     &startDelay,
					API:            defaultAPI,
					PlaybackMethod: defaultPlayback,
					MaxExtended:    -1,
				},
			},
		},
		App: &App{
			ID:       bundle,
			Name:     c.Query("app_name"),
			Bundle:   bundle,
			StoreURL: c.Query("app_store_url", c.Query("storeurl")),
			Ver:      c.Query("app_ver"),
		},
		Device: Device{
			DNT:        dnt,
			UA:         ua,
			IP:         ip,
			Geo:        &Geo{Country: country, Region: c.Query("region"), City: c.Query("city"), Zip: c.Query("zip"), Type: 2},
			Make:       deviceMake,
			Model:      c.Query("device_model"),
			OS:         deviceOS,
			OSv:        c.Query("osv"),
			DeviceType: deviceType,
			IFA:        ifa,
			LMT:        lmt,
			W:          w,
			H:          h,
			Language:   language,
		},
		Regs: &Regs{
			Ext: &RegsExt{USPriv: c.Query("us_privacy", "1---")},
		},
		Ext: defaultSChain,
	}

	if ifaType := detectIFAType(ua, deviceMake, deviceOS); ifaType != "" {
		req.Device.Ext = &DeviceExt{IFAType: ifaType}
	}

	if ifa != "" {
		req.User = &User{ID: ifa, Ext: &UserExt{}}
	}

	if ct := c.Query("connectiontype"); ct != "" {
		req.Device.ConnectionType, _ = strconv.Atoi(ct)
	}

	if ctGenre := c.Query("ct_genre"); ctGenre != "" {
		cats := strings.Split(ctGenre, ",")
		req.App.Cat = cats
		req.App.Content = &Content{Genre: ctGenre, Cat: cats, Language: language}
	}

	if coppa := c.Query("coppa"); coppa != "" {
		req.Regs.COPPA, _ = strconv.Atoi(coppa)
	}
	if gdpr := c.Query("gdpr"); gdpr != "" {
		req.Regs.Ext.GDPR, _ = strconv.Atoi(gdpr)
	}

	return req
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
