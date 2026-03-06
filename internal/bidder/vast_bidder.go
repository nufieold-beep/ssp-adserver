package bidder

import (
	"net/http"
	"net/url"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"strconv"
	"time"
)

type VASTBidder struct {
	name   string
	tag    string
	client *http.Client
	cpm    float64
}

func NewVASTBidder(name, tag string, timeoutMs int, cpm float64) *VASTBidder {
	t := time.Duration(timeoutMs) * time.Millisecond
	if t == 0 {
		t = 800 * time.Millisecond
	}
	if cpm == 0 {
		cpm = 1.0
	}
	return &VASTBidder{
		name:   name,
		tag:    tag,
		client: httputil.NewClient(t),
		cpm:    cpm,
	}
}

func (b *VASTBidder) Name() string       { return b.name }
func (b *VASTBidder) BidderType() string { return "vast" }

// enrichTagURL appends targeting signals from the bid request to the VAST tag
// URL so the DSP can make informed targeting and bid decisions. Parameters are
// only added when they have non-empty values and don't already exist in the URL.
func (b *VASTBidder) enrichTagURL(req openrtb.BidRequest) string {
	u, err := url.Parse(b.tag)
	if err != nil {
		return b.tag
	}

	q := u.Query()
	set := func(key, val string) {
		if val != "" && q.Get(key) == "" {
			q.Set(key, val)
		}
	}
	setInt := func(key string, val int) {
		if q.Get(key) == "" {
			q.Set(key, strconv.Itoa(val))
		}
	}

	// Device signals
	set("ip", req.Device.IP)
	set("ua", req.Device.UA)
	set("ifa", req.Device.IFA)
	set("os", req.Device.OS)
	set("make", req.Device.Make)
	set("model", req.Device.Model)
	setInt("devicetype", req.Device.DeviceType)
	setInt("dnt", req.Device.DNT)
	setInt("lmt", req.Device.LMT)
	set("lang", req.Device.Language)

	// Geo
	if req.Device.Geo != nil {
		set("country", req.Device.Geo.Country)
		set("region", req.Device.Geo.Region)
	}

	// App / Bundle
	if req.App != nil {
		set("app_bundle", req.App.Bundle)
		set("app_name", req.App.Name)
	}

	// Video dimensions & duration
	if len(req.Imp) > 0 && req.Imp[0].Video != nil {
		v := req.Imp[0].Video
		setInt("w", v.W)
		setInt("h", v.H)
		setInt("minduration", v.MinDuration)
		setInt("maxduration", v.MaxDuration)
	}

	u.RawQuery = q.Encode()
	return u.String()
}

func (b *VASTBidder) Request(req openrtb.BidRequest) ([]openrtb.Bid, error) {
	tagURL := b.enrichTagURL(req)
	resp, err := b.client.Get(tagURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := httputil.ReadResponseBody(resp)
	if err != nil {
		return nil, err
	}

	adm := string(body)
	if adm == "" {
		return nil, nil
	}

	// Match ImpID to the actual request imp so the bid passes validation
	impID := "1"
	if len(req.Imp) > 0 {
		impID = req.Imp[0].ID
	}

	bid := openrtb.Bid{
		ID:    "vast-" + b.name,
		ImpID: impID,
		Price: b.cpm,
		Adm:   adm,
		MType: 2,
	}

	return []openrtb.Bid{bid}, nil
}
