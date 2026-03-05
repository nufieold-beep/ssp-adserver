package openrtb

import (
	"strconv"
	"strings"

	"ssp/internal/geo"

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
	User    *User    `json:"user,omitempty"`
	Regs    *Regs    `json:"regs,omitempty"`
	At      int      `json:"at,omitempty"`      // 1=first-price, 2=second-price
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
	Type      int     `json:"type,omitempty"`      // 1=GPS, 2=IP
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

// BuildFromHTTP constructs a CTV/in-app video BidRequest from an HTTP request.
// Supports both the legacy query params and the supply-tag VAST URL format:
//
//	/api/vast?sid=1211&w=1920&h=1080&cb=...&ip=...&ua=...&app_bundle=...
//	         &app_name=...&app_store_url=...&country_code=US&max_dur=30
//	         &min_dur=5&device_make=...&device_model=...&device_type=3
//	         &ct_genre=game,entertainment&ct_lang=en&dnt=0&ifa=...&os=...
//	         &us_privacy=1---&lmt=0
func BuildFromHTTP(c *fiber.Ctx) BidRequest {
	w, _ := strconv.Atoi(c.Query("w", "1920"))
	h, _ := strconv.Atoi(c.Query("h", "1080"))

	minDur, _ := strconv.Atoi(c.Query("min_dur", c.Query("minduration", "3")))
	maxDur, _ := strconv.Atoi(c.Query("max_dur", c.Query("maxduration", "60")))

	skippable := 0
	if c.Query("skip") == "1" {
		skippable = 1
	}

	tagID := c.Query("sid", c.Query("tagid", c.Params("tag")))

	deviceType := 3
	if dt := c.Query("device_type", c.Query("devicetype")); dt != "" {
		deviceType, _ = strconv.Atoi(dt)
	}

	dnt, _ := strconv.Atoi(c.Query("dnt", "0"))
	lmt, _ := strconv.Atoi(c.Query("lmt", "0"))

	ip := c.Query("ip", c.Query("uip", c.IP()))
	ua := c.Query("ua", c.Get("User-Agent"))
	ifa := c.Query("ifa")
	bundle := c.Query("app_bundle", c.Query("bundle"))
	deviceOS := c.Query("os")
	deviceMake := c.Query("device_make")

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
				BidFloor:    4.50,
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
			Make:       deviceMake,
			Model:      c.Query("device_model"),
			OS:         deviceOS,
			OSv:        c.Query("osv"),
			JS:         0,
			DeviceType: deviceType,
			IFA:        ifa,
			LMT:        lmt,
		},
		Regs: &Regs{
			COPPA: 0,
			Ext:   &RegsExt{GDPR: 0, USPriv: c.Query("us_privacy", "1---")},
		},
	}

	// IFA type detection based on device OS/make/UA
	ifaType := geo.DetectIFAType(ua, deviceMake, deviceOS)
	if ifaType != "" {
		req.Device.Ext = &DeviceExt{IFAType: ifaType}
	}

	// User: id = IFA
	if ifa != "" {
		req.User = &User{ID: ifa, Ext: &UserExt{Consent: ""}}
	}

	// Geo from query params
	country := c.Query("country_code", c.Query("country"))
	region := c.Query("region")
	city := c.Query("city")
	zip := c.Query("zip")
	req.Device.Geo = &Geo{Country: country, Region: region, City: city, Zip: zip, Type: 2}

	// MaxMind fallback for geo and carrier
	if geoResult := geo.Lookup(ip); geoResult != nil {
		g := req.Device.Geo
		if g.Lat == 0 && g.Lon == 0 {
			g.Lat = geoResult.Lat
			g.Lon = geoResult.Lon
		}
		if g.Country == "" {
			g.Country = geoResult.Country
		}
		if g.Region == "" {
			g.Region = geoResult.Region
		}
		if g.Metro == "" {
			g.Metro = geoResult.Metro
		}
		if g.City == "" {
			g.City = geoResult.City
		}
		if g.Zip == "" {
			g.Zip = geoResult.Zip
		}
		if g.Accuracy == 0 {
			g.Accuracy = geoResult.Accuracy
		}
		g.IPService = 3 // MaxMind
		req.Device.Carrier = geoResult.Carrier
	}

	// Connection type
	if ct := c.Query("connectiontype"); ct != "" {
		req.Device.ConnectionType, _ = strconv.Atoi(ct)
	}

	// CTV content categories from ct_genre
	if ctGenre := c.Query("ct_genre"); ctGenre != "" {
		cats := strings.Split(ctGenre, ",")
		if req.App != nil {
			req.App.Cat = cats
		}
	}

	// Privacy overrides from query params
	if coppa := c.Query("coppa"); coppa != "" {
		req.Regs.COPPA, _ = strconv.Atoi(coppa)
	}
	if gdpr := c.Query("gdpr"); gdpr != "" {
		req.Regs.Ext.GDPR, _ = strconv.Atoi(gdpr)
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
