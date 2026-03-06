package validate

import (
	"errors"
	"ssp/internal/openrtb"
)

// Request validates a BidRequest per PDF spec section 6:
// - must have at least one imp
// - should include device.ip (best effort fallback when missing)
// - must include at least one video imp for CTV/in-app video
func Request(req *openrtb.BidRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if len(req.Imp) == 0 {
		return errors.New("must have at least one imp")
	}
	if req.Device == nil {
		req.Device = &openrtb.Device{}
	}

	// Some CTV integrations can omit IP at the edge (privacy/proxy setups).
	// Keep the request serviceable instead of hard-failing to no-fill.
	if req.Device.IP == "" {
		req.Device.IP = "0.0.0.0"
	}

	hasVideoImp := false
	for i := range req.Imp {
		if req.Imp[i].Video == nil {
			continue
		}
		hasVideoImp = true
		if len(req.Imp[i].Video.MIMEs) == 0 {
			req.Imp[i].Video.MIMEs = []string{"video/mp4", "video/webm", "application/x-mpegURL"}
		}
	}
	if !hasVideoImp {
		return errors.New("at least one video imp is required for CTV/in-app")
	}
	return nil
}
