package validate

import (
	"errors"
	"ssp/internal/openrtb"
)

// Request validates a BidRequest per PDF spec section 6:
// - must have at least one imp
// - must have device.ip
// - must include video specs for CTV
func Request(req *openrtb.BidRequest) error {
	if len(req.Imp) == 0 {
		return errors.New("must have at least one imp")
	}
	if req.Device.IP == "" {
		return errors.New("device.ip is required")
	}
	for _, imp := range req.Imp {
		if imp.Video == nil {
			return errors.New("video spec required for CTV impressions")
		}
		if len(imp.Video.Mimes) == 0 {
			return errors.New("video.mimes is required")
		}
	}
	return nil
}
