package validate

import (
	"fmt"
	"strings"

	"ssp/internal/openrtb"
)

// Request validates a BidRequest per PDF spec section 6:
// - must have at least one imp
// - should include device.ip (best effort fallback when missing)
// - must include at least one video imp for CTV/in-app video
func Request(req *openrtb.BidRequest) error {
	if req == nil {
		return fmt.Errorf("missing bid request")
	}
	if len(req.Imp) == 0 {
		return fmt.Errorf("missing impressions")
	}
	imp := req.Imp[0]
	if imp.Video == nil {
		return fmt.Errorf("missing video impression")
	}
	if imp.Video.W == nil || imp.Video.H == nil || *imp.Video.W <= 0 || *imp.Video.H <= 0 {
		return fmt.Errorf("invalid video size")
	}
	if imp.Video.MaxDuration > 0 && imp.Video.MinDuration > imp.Video.MaxDuration {
		return fmt.Errorf("invalid video duration range")
	}
	if len(imp.Video.MIMEs) == 0 {
		return fmt.Errorf("missing video mimes")
	}
	if req.Device == nil {
		return fmt.Errorf("missing device")
	}

	if strings.TrimSpace(req.Device.IP) == "" {
		req.Device.IP = "0.0.0.0"
	}
	if req.Device.DeviceType == 0 {
		return fmt.Errorf("missing device type")
	}
	if req.App == nil && req.Site == nil {
		return fmt.Errorf("missing app/site context")
	}
	if req.App != nil {
		bundle := strings.TrimSpace(req.App.Bundle)
		appID := strings.TrimSpace(req.App.ID)
		if bundle == "" && appID == "" {
			return fmt.Errorf("missing app bundle/id")
		}
	}
	return nil
}
