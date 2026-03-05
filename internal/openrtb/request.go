package openrtb

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// ── OpenRTB 2.6 BidRequest (CTV-focused, per spec) ──

type BidRequest struct {
	ID     string   `json:"id"`
	Imp    []Imp    `json:"imp"`
	App    *App     `json:"app,omitempty"`
	Site   *Site    `json:"site,omitempty"`
	Device Device   `json:"device"`
	User   *User    `json:"user,omitempty"`
	Regs   *Regs    `json:"regs,omitempty"`
	TMax   int      `json:"tmax,omitempty"`
	BAdv   []string `json:"badv,omitempty"`
	BCat   []string `json:"bcat,omitempty"`
	At     int      `json:"at,omitempty"` // 1=first-price, 2=second-price
	Ext    *ReqExt  `json:"ext,omitempty"`
}

type Imp struct {
	ID          string  `json:"id"`
	BidFloor    float64 `json:"bidfloor"`
	BidFloorCur string  `json:"bidfloorcur,omitempty"`
	Video       *Video  `json:"video,omitempty"`
	Secure      *int    `json:"secure,omitempty"` // 1=https required
	TagID       string  `json:"tagid,omitempty"`
	Ext         *ImpExt `json:"ext,omitempty"`
}

type ImpExt struct {
	Skadn interface{} `json:"skadn,omitempty"`
}

type Video struct {
	Mimes          []string `json:"mimes"`
	W              int      `json:"w"`
	H              int      `json:"h"`
	MinDuration    int      `json:"minduration,omitempty"`
	MaxDuration    int      `json:"maxduration,omitempty"`
	Protocols      []int    `json:"protocols,omitempty"`
	Placement      int      `json:"placement,omitempty"`
	PlaybackMethod []int    `json:"playbackmethod,omitempty"`
	Linearity      int      `json:"linearity,omitempty"`
	Skip           *int     `json:"skip,omitempty"`       // 0=no, 1=skippable
	SkipMin        int      `json:"skipmin,omitempty"`    // min seconds before skip
	SkipAfter      int      `json:"skipafter,omitempty"`  // seconds until skip allowed
	StartDelay     *int     `json:"startdelay,omitempty"` // 0=pre-roll, -1=mid, -2=post
	API            []int    `json:"api,omitempty"`        // API frameworks supported
	MaxExtended    int      `json:"maxextended,omitempty"`
	Pos            int      `json:"pos,omitempty"` // Ad position
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
	Bundle   string   `json:"bundle"`
	Name     string   `json:"name,omitempty"`
	Cat      []string `json:"cat,omitempty"`
	StoreURL string   `json:"storeurl,omitempty"`
	Ver      string   `json:"ver,omitempty"`
}

type Site struct {
	Domain string `json:"domain,omitempty"`
	Page   string `json:"page,omitempty"`
}

type Geo struct {
	Country string  `json:"country,omitempty"`
	Region  string  `json:"region,omitempty"`
	City    string  `json:"city,omitempty"`
	Zip     string  `json:"zip,omitempty"`
	Lat     float64 `json:"lat,omitempty"`
	Lon     float64 `json:"lon,omitempty"`
	Type    int     `json:"type,omitempty"` // 1=GPS, 2=IP
}

type Device struct {
	UA             string `json:"ua"`
	IP             string `json:"ip"`
	DeviceType     int    `json:"devicetype,omitempty"` // 3=CTV, 7=set-top-box
	Make           string `json:"make,omitempty"`
	Model          string `json:"model,omitempty"`
	OS             string `json:"os,omitempty"`
	OSv            string `json:"osv,omitempty"`
	W              int    `json:"w,omitempty"`
	H              int    `json:"h,omitempty"`
	Language       string `json:"language,omitempty"`
	IFA            string `json:"ifa,omitempty"`
	DNT            *int   `json:"dnt,omitempty"` // Do Not Track
	LMT            *int   `json:"lmt,omitempty"` // Limit Ad Tracking
	JS             int    `json:"js,omitempty"`
	ConnectionType int    `json:"connectiontype,omitempty"` // 1=ethernet, 2=wifi, etc.
	Geo            *Geo   `json:"geo,omitempty"`
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

// BuildFromHTTP constructs a CTV/in-app video BidRequest from an HTTP request.
// Supports both the legacy query params and the supply-tag VAST URL format:
//
//	/api/vast?sid=1211&w=1920&h=1080&cb=...&ip=...&ua=...&app_bundle=...
//	         &app_name=...&app_store_url=...&country_code=US&max_dur=30
//	         &min_dur=5&device_make=...&device_model=...&device_type=3
//	         &ct_genre=game,entertainment&ct_lang=en&dnt=0&ifa=...&os=...
//	         &us_privacy=1---
func BuildFromHTTP(c *fiber.Ctx) BidRequest {
	w, _ := strconv.Atoi(c.Query("w", "1920"))
	h, _ := strconv.Atoi(c.Query("h", "1080"))

	// Support both old (minduration/maxduration) and new (min_dur/max_dur) params
	minDur, _ := strconv.Atoi(c.Query("min_dur", c.Query("minduration", "5")))
	maxDur, _ := strconv.Atoi(c.Query("max_dur", c.Query("maxduration", "30")))

	skippable := 0
	if c.Query("skip") == "1" {
		skippable = 1
	}

	secure := 1
	startDelay := 0

	// sid (supply ID) or tagid or route param
	tagID := c.Query("sid", c.Query("tagid", c.Params("tag")))

	// Device type: new param "device_type", fallback "devicetype", default 3 (CTV)
	deviceType := 3
	if dt := c.Query("device_type", c.Query("devicetype")); dt != "" {
		deviceType, _ = strconv.Atoi(dt)
	}

	// DNT
	var dnt *int
	if d := c.Query("dnt"); d != "" {
		v, _ := strconv.Atoi(d)
		dnt = &v
	}

	// IP: accept "ip" (new) or "uip" (legacy)
	ip := c.Query("ip", c.Query("uip", c.IP()))

	// UA: accept "ua" query param or fall back to header
	ua := c.Query("ua", c.Get("User-Agent"))

	// Content genre/language for CTV content targeting
	ctGenre := c.Query("ct_genre")
	ctLang := c.Query("ct_lang", c.Query("lang", "en"))

	req := BidRequest{
		ID:   uuid.New().String(),
		TMax: 120,
		At:   1,
		Imp: []Imp{
			{
				ID:          "1",
				BidFloor:    5.00,
				BidFloorCur: "USD",
				Secure:      &secure,
				TagID:       tagID,
				Video: &Video{
					Mimes:          []string{"video/mp4", "video/webm"},
					W:              w,
					H:              h,
					MinDuration:    minDur,
					MaxDuration:    maxDur,
					Protocols:      []int{2, 3, 5, 6},
					Placement:      1,
					PlaybackMethod: []int{1, 3},
					Linearity:      1,
					Skip:           &skippable,
					StartDelay:     &startDelay,
					API:            []int{1, 2},
				},
			},
		},
		App: &App{
			Bundle:   c.Query("app_bundle", c.Query("bundle")),
			Name:     c.Query("app_name"),
			StoreURL: c.Query("app_store_url", c.Query("storeurl")),
			Ver:      c.Query("app_ver"),
		},
		Device: Device{
			UA:         ua,
			IP:         ip,
			DeviceType: deviceType,
			Make:       c.Query("device_make"),
			Model:      c.Query("device_model"),
			OS:         c.Query("os"),
			OSv:        c.Query("osv"),
			W:          w,
			H:          h,
			Language:   ctLang,
			IFA:        c.Query("ifa"),
			DNT:        dnt,
		},
	}

	// Connection type
	if ct := c.Query("connectiontype"); ct != "" {
		req.Device.ConnectionType, _ = strconv.Atoi(ct)
	}

	// Geo: accept "country_code" (new) or "country" (legacy)
	country := c.Query("country_code", c.Query("country"))
	region := c.Query("region")
	city := c.Query("city")
	if country != "" || region != "" || city != "" {
		req.Device.Geo = &Geo{Country: country, Region: region, City: city, Type: 2}
	}

	// CTV content categories from ct_genre (comma-separated IAB-style)
	if ctGenre != "" {
		cats := strings.Split(ctGenre, ",")
		if req.App != nil {
			req.App.Cat = cats
		}
	}

	// Privacy / Regs
	coppa, _ := strconv.Atoi(c.Query("coppa", "0"))
	gdpr, _ := strconv.Atoi(c.Query("gdpr", "0"))
	usprivacy := c.Query("us_privacy")
	if coppa != 0 || gdpr != 0 || usprivacy != "" {
		req.Regs = &Regs{
			COPPA: coppa,
			Ext:   &RegsExt{GDPR: gdpr, USPriv: usprivacy},
		}
	}

	// Supply chain transparency
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
