package openrtb

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// ── OpenRTB 2.6 BidRequest (CTV-focused, per spec) ──

type BidRequest struct {
	ID      string   `json:"id"`
	Imp     []Imp    `json:"imp"`
	App     *App     `json:"app,omitempty"`
	Site    *Site    `json:"site,omitempty"`
	Device  Device   `json:"device"`
	Regs    *Regs    `json:"regs,omitempty"`
	User    *User    `json:"user,omitempty"`
	At      int      `json:"at,omitempty"`      // 1=first-price, 2=second-price
	TMax    int      `json:"tmax,omitempty"`
	AllImps int      `json:"allimps,omitempty"`
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
	Secure      *int    `json:"secure,omitempty"`
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
	Skip           *int     `json:"skip,omitempty"`
	Sequence       int      `json:"sequence,omitempty"`
	BoxingAllowed  *int     `json:"boxingallowed,omitempty"`
	Placement      int      `json:"placement,omitempty"`
	PlaybackMethod []int    `json:"playbackmethod,omitempty"`
	StartDelay     *int     `json:"startdelay,omitempty"`
	API            []int    `json:"api,omitempty"`
	SkipMin        int      `json:"skipmin,omitempty"`
	SkipAfter      int      `json:"skipafter,omitempty"`
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
	Type      int     `json:"type,omitempty"`      // 1=GPS, 2=IP
	Accuracy  int     `json:"accuracy,omitempty"`
	IPService int     `json:"ipservice,omitempty"` // 3=MaxMind
}

type Device struct {
	DNT            *int       `json:"dnt,omitempty"`
	UA             string     `json:"ua"`
	IP             string     `json:"ip"`
	Geo            *Geo       `json:"geo,omitempty"`
	Carrier        string     `json:"carrier,omitempty"`
	Make           string     `json:"make,omitempty"`
	Model          string     `json:"model,omitempty"`
	OS             string     `json:"os,omitempty"`
	OSv            string     `json:"osv,omitempty"`
	JS             int        `json:"js,omitempty"`
	DeviceType     int        `json:"devicetype,omitempty"` // 3=CTV, 7=set-top-box
	W              int        `json:"w,omitempty"`
	H              int        `json:"h,omitempty"`
	Language       string     `json:"language,omitempty"`
	IFA            string     `json:"ifa,omitempty"`
	LMT            *int       `json:"lmt,omitempty"`
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
	Consent string `json:"consent"` // GDPR consent string
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

// BuildFromHTTP constructs a CTV/in-app video BidRequest from an HTTP request.
// Query params come from the publisher's CTV player (macros replaced at runtime).
//
//	/api/vast?sid=1211&w=1920&h=1080&cb=...&ip=...&ua=...&app_bundle=...
//	         &app_name=...&app_store_url=...&country_code=US&max_dur=60
//	         &min_dur=3&device_make=...&device_model=...&device_type=3
//	         &ct_lang=en&dnt=0&lmt=0&ifa=...&os=...&us_privacy=1---
func BuildFromHTTP(c *fiber.Ctx) BidRequest {
	w, _ := strconv.Atoi(c.Query("w", "1920"))
	h, _ := strconv.Atoi(c.Query("h", "1080"))

	minDur, _ := strconv.Atoi(c.Query("min_dur", c.Query("minduration", "3")))
	maxDur, _ := strconv.Atoi(c.Query("max_dur", c.Query("maxduration", "60")))

	skippable := 0
	if c.Query("skip") == "1" {
		skippable = 1
	}

	// Secure based on actual protocol
	secure := 0
	if c.Protocol() == "https" {
		secure = 1
	}

	boxingAllowed := 0

	// Device type: default 3 (CTV)
	deviceType := 3
	if dt := c.Query("device_type", c.Query("devicetype")); dt != "" {
		deviceType, _ = strconv.Atoi(dt)
	}

	// DNT
	dntVal := 0
	if d := c.Query("dnt"); d != "" {
		dntVal, _ = strconv.Atoi(d)
	}

	// LMT
	lmtVal := 0
	if l := c.Query("lmt"); l != "" {
		lmtVal, _ = strconv.Atoi(l)
	}

	// IP: accept "ip" or "uip" (legacy)
	ip := c.Query("ip", c.Query("uip", c.IP()))

	// UA: accept "ua" query param or fall back to header
	ua := c.Query("ua", c.Get("User-Agent"))

	// Language
	lang := c.Query("ct_lang", c.Query("lang", "en"))

	// IFA (advertiser ID)
	ifa := c.Query("ifa", c.Query("idfa"))

	// App info
	appBundle := c.Query("app_bundle", c.Query("bundle"))
	appName := c.Query("app_name")
	appStoreURL := c.Query("app_store_url", c.Query("storeurl"))

	// Floor from query params, default 4.50
	bidFloor := 4.50
	if f := c.Query("floor", c.Query("bidfloor")); f != "" {
		if parsed, err := strconv.ParseFloat(f, 64); err == nil {
			bidFloor = parsed
		}
	}

	reqID := uuid.New().String()

	req := BidRequest{
		ID:      reqID,
		TMax:    500,
		At:      1,
		AllImps: 0,
		Cur:     []string{"USD"},
		Imp: []Imp{
			{
				ID:          reqID,
				Instl:       0,
				BidFloor:    bidFloor,
				BidFloorCur: "USD",
				Secure:      &secure,
				Video: &Video{
					Mimes:         []string{"video/mp4", "video/webm"},
					Linearity:     1,
					MinDuration:   minDur,
					MaxDuration:   maxDur,
					Protocols:     []int{2, 3, 5, 6, 7, 8},
					W:             w,
					H:             h,
					Skip:          &skippable,
					Sequence:      1,
					BoxingAllowed: &boxingAllowed,
					Placement:     1,
				},
			},
		},
		App: &App{
			ID:       appBundle,
			Name:     appName,
			Bundle:   appBundle,
			StoreURL: appStoreURL,
		},
		Device: Device{
			DNT:        &dntVal,
			UA:         ua,
			IP:         ip,
			Make:       c.Query("device_make"),
			Model:      c.Query("device_model"),
			OS:         c.Query("os", c.Query("device_os")),
			OSv:        c.Query("osv"),
			JS:         0,
			DeviceType: deviceType,
			W:          w,
			H:          h,
			Language:   lang,
			IFA:        ifa,
			LMT:        &lmtVal,
		},
		Ext: &ReqExt{},
	}

	// Connection type
	if ct := c.Query("connectiontype"); ct != "" {
		req.Device.ConnectionType, _ = strconv.Atoi(ct)
	}

	// IFA type extension
	if ifa != "" {
		req.Device.Ext = &DeviceExt{IFAType: "afai"}
	}

	// User from IFA
	req.User = &User{
		ID:  ifa,
		Ext: &UserExt{Consent: ""},
	}

	// Geo from query params (MaxMind fills missing fields later)
	country := c.Query("country_code", c.Query("country"))
	region := c.Query("region")
	city := c.Query("city")
	zip := c.Query("zip")
	metro := c.Query("metro")
	if country != "" || region != "" || city != "" {
		req.Device.Geo = &Geo{
			Country: country,
			Region:  region,
			Metro:   metro,
			City:    city,
			Zip:     zip,
			Type:    2,
		}
	}

	// Privacy / Regs (always present)
	coppa, _ := strconv.Atoi(c.Query("coppa", "0"))
	gdpr, _ := strconv.Atoi(c.Query("gdpr", "0"))
	usprivacy := c.Query("us_privacy", "1---")
	req.Regs = &Regs{
		COPPA: coppa,
		Ext:   &RegsExt{GDPR: gdpr, USPriv: usprivacy},
	}

	return req
}
