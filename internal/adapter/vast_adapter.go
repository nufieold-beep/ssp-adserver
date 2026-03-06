package adapter

import (
	"context"
	"net/http"
	"net/url"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"strconv"
	"time"
)

// VASTAdapter implements DemandAdapter for VAST tag demand sources.
// This handles ad networks that return VAST XML directly (no OpenRTB).
// Magnite and FreeWheel both support VAST tags as a demand type alongside
// programmatic ORTB, unified through a single adapter interface.
type VASTAdapter struct {
	id     string
	name   string
	tag    string
	client *http.Client
	cpm    float64
	margin float64
}

func NewVASTAdapter(cfg *AdapterConfig) *VASTAdapter {
	t := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if t == 0 {
		t = 800 * time.Millisecond
	}
	if cfg.Floor == 0 {
		cfg.Floor = 1.0
	}
	return &VASTAdapter{
		id: cfg.ID, name: cfg.Name, tag: cfg.Endpoint,
		cpm: cfg.Floor, margin: cfg.Margin,
		client: httputil.NewClient(t),
	}
}

func (a *VASTAdapter) ID() string                          { return a.id }
func (a *VASTAdapter) Name() string                        { return a.name }
func (a *VASTAdapter) Type() AdapterType                   { return TypeVAST }
func (a *VASTAdapter) Supports(_ *openrtb.BidRequest) bool { return true }

// enrichTagURL appends targeting signals from the bid request to the VAST tag
// URL so the DSP can make informed targeting and bid decisions.
func (a *VASTAdapter) enrichTagURL(req *openrtb.BidRequest) string {
	u, err := url.Parse(a.tag)
	if err != nil {
		return a.tag
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
	if req.Device.Geo != nil {
		set("country", req.Device.Geo.Country)
		set("region", req.Device.Geo.Region)
	}
	if req.App != nil {
		set("app_bundle", req.App.Bundle)
		set("app_name", req.App.Name)
	}
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

func (a *VASTAdapter) RequestBids(ctx context.Context, req *openrtb.BidRequest) (*BidResult, error) {
	tagURL := a.enrichTagURL(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, tagURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Do(httpReq)
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
		return &BidResult{AdapterID: a.id, NoBid: true}, nil
	}

	price := a.cpm
	if a.margin > 0 {
		price *= (1 - a.margin)
	}

	// Match ImpID to actual request imp for bid validation
	impID := "1"
	if len(req.Imp) > 0 {
		impID = req.Imp[0].ID
	}

	bid := openrtb.Bid{
		ID:     "vast-" + a.id,
		ImpID:  impID,
		Price:  price,
		Margin: a.margin,
		Adm:    adm,
		MType:  2,
	}

	return &BidResult{AdapterID: a.id, Bids: []openrtb.Bid{bid}}, nil
}
