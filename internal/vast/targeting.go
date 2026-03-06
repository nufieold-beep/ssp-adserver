package vast

import (
	"net/url"
	"ssp/internal/openrtb"
	"strconv"
)

// EnrichTagURL appends targeting signals from the bid request to a VAST tag
// URL so the DSP can make informed targeting and bid decisions. Parameters are
// only added when they have non-empty values and don't already exist in the URL.
func EnrichTagURL(tag string, req *openrtb.BidRequest) string {
	u, err := url.Parse(tag)
	if err != nil {
		return tag
	}
	if req == nil {
		return tag
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
	if req.Device != nil {
		set("ip", req.Device.IP)
		set("ua", req.Device.UA)
		set("ifa", req.Device.IFA)
		set("os", req.Device.OS)
		set("make", req.Device.Make)
		set("model", req.Device.Model)
		setInt("devicetype", int(req.Device.DeviceType))
		if req.Device.DNT != nil {
			setInt("dnt", int(*req.Device.DNT))
		}
		if req.Device.Lmt != nil {
			setInt("lmt", int(*req.Device.Lmt))
		}
		set("lang", req.Device.Language)
	}

	// Geo
	if req.Device != nil && req.Device.Geo != nil {
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
		if v.W != nil {
			setInt("w", int(*v.W))
		}
		if v.H != nil {
			setInt("h", int(*v.H))
		}
		setInt("minduration", int(v.MinDuration))
		setInt("maxduration", int(v.MaxDuration))
	}

	u.RawQuery = q.Encode()
	return u.String()
}
