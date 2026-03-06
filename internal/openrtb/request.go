package openrtb

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// RequestDefaults controls generated OpenRTB request defaults.
// It can be configured at startup via ConfigureRequestDefaults.
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
	Type      int     `json:"type,omitempty"` // 1=GPS, 2=IP
	Accuracy  int     `json:"accuracy,omitempty"`
	IPService int     `json:"ipservice,omitempty"` // 3=MaxMind
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
	DeviceType     int        `json:"devicetype,omitempty"` // 3=CTV, 7=set-top-box
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
	Consent string `json:"consent,omitempty"` // GDPR consent string
}

type Regs struct {
	COPPA int      `json:"coppa,omitempty"`
	Ext   *RegsExt `json:"ext,omitempty"`
}

type RegsExt struct {
	GDPR   int    `json:"gdpr,omitempty"`
	USPriv string `json:"us_privacy,omitempty"` // CCPA string
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

// BuildFromHTTP constructs a CTV/in-app video BidRequest from query params.
//
//	/api/vast?sid=1211&w=1920&h=1080&ip=...&ua=...&app_bundle=...
//	         &app_name=...&app_store_url=...&country_code=US&max_dur=60
//	         &min_dur=3&device_make=...&device_model=...&device_type=3
//	         &ct_genre=game,entertainment&dnt=0&ifa=...&os=...
//	         &us_privacy=1---&lmt=0
func BuildFromHTTP(c *fiber.Ctx) BidRequest {
	w, _ := strconv.Atoi(c.Query("w", "1920"))
	h, _ := strconv.Atoi(c.Query("h", "1080"))
	minDur, _ := strconv.Atoi(c.Query("min_dur", c.Query("minduration", strconv.Itoa(requestDefaults.MinDur))))
	maxDur, _ := strconv.Atoi(c.Query("max_dur", c.Query("maxduration", strconv.Itoa(requestDefaults.MaxDur))))

	skippable := 0
	if c.Query("skip") == "1" {
		skippable = 1
	}

	tagID := c.Query("sid", c.Query("tagid", c.Params("tag")))

	deviceType := 3
	if dt := c.Query("device_type", c.Query("devicetype")); dt != "" {
		deviceType, _ = strconv.Atoi(dt)
	}
	language := c.Query("ct_lang", c.Query("lang", "en"))

	dnt, _ := strconv.Atoi(c.Query("dnt", "0"))
	lmt, _ := strconv.Atoi(c.Query("lmt", "0"))
	ip := c.Query("ip", c.Query("uip", c.IP()))
	ua := c.Query("ua", c.Get("User-Agent"))
	ifa := c.Query("ifa")
	bundle := c.Query("app_bundle", c.Query("bundle"))
	deviceOS := c.Query("os")
	deviceMake := c.Query("device_make")

	reqID := uuid.New().String()

	// Country: accept alpha-2 or alpha-3
	country := c.Query("country_code", c.Query("country"))
	if len(country) == 2 {
		country = ToAlpha3(country)
	}

	req := BidRequest{
		ID:      reqID,
		TMax:    500,
		At:      1,
		AllImps: 0,
		Cur:     []string{"USD"},
		Imp: []Imp{
			{
				ID:          reqID,
				BidFloor:    requestDefaults.BidFloor,
				BidFloorCur: "USD",
				Secure:      0,
				Instl:       0,
				TagID:       tagID,
				Video: &Video{
					Mimes:         []string{"video/mp4", "video/webm"},
					Linearity:     1,
					MinDuration:   minDur,
					MaxDuration:   maxDur,
					Protocols:     []int{2, 3, 5, 6, 7, 8},
					W:             w,
					H:             h,
					Skip:          skippable,
					Sequence:      1,
					BoxingAllowed: 0,
					Placement:     1,
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
			JS:         0,
			DeviceType: deviceType,
			IFA:        ifa,
			LMT:        lmt,
			Language:   language,
		},
		Regs: &Regs{
			COPPA: 0,
			Ext:   &RegsExt{GDPR: 0, USPriv: c.Query("us_privacy", "1---")},
		},
	}

	// IFA type detection
	if ifaType := detectIFAType(ua, deviceMake, deviceOS); ifaType != "" {
		req.Device.Ext = &DeviceExt{IFAType: ifaType}
	}

	// User
	if ifa != "" {
		req.User = &User{ID: ifa, Ext: &UserExt{Consent: ""}}
	}

	// Connection type
	if ct := c.Query("connectiontype"); ct != "" {
		req.Device.ConnectionType, _ = strconv.Atoi(ct)
	}

	// Content categories
	if ctGenre := c.Query("ct_genre"); ctGenre != "" && req.App != nil {
		req.App.Cat = strings.Split(ctGenre, ",")
	}

	// Privacy overrides
	if coppa := c.Query("coppa"); coppa != "" {
		req.Regs.COPPA, _ = strconv.Atoi(coppa)
	}
	if gdpr := c.Query("gdpr"); gdpr != "" {
		req.Regs.Ext.GDPR, _ = strconv.Atoi(gdpr)
	}

	// Supply chain
	req.Ext = &ReqExt{
		SChain: &SChain{
			Complete: 1,
			Ver:      "1.0",
			Nodes: []SChainNode{
				{ASI: "viadsmedia.com", SID: "pub-001", HP: 1},
			},
		},
	}

	return req
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
	m := map[string]string{
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
	if v, ok := m[strings.ToUpper(code)]; ok {
		return v
	}
	return strings.ToUpper(code)
}
