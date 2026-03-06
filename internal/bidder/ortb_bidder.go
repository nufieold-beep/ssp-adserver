package bidder

import (
	"encoding/json"
	"net/http"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"time"
)

type ORTBBidder struct {
	name     string
	endpoint string
	client   *http.Client
}

func NewORTBBidder(name, url string, timeoutMs int) *ORTBBidder {
	t := time.Duration(timeoutMs) * time.Millisecond
	if t == 0 {
		t = 120 * time.Millisecond
	}
	return &ORTBBidder{
		name:     name,
		endpoint: url,
		client:   httputil.NewClient(t),
	}
}

func (b *ORTBBidder) Name() string       { return b.name }
func (b *ORTBBidder) BidderType() string { return "ortb" }

func (b *ORTBBidder) Request(req openrtb.BidRequest) ([]openrtb.Bid, error) {
	buf := httputil.GetBuffer()
	defer httputil.PutBuffer(buf)
	if err := json.NewEncoder(buf).Encode(req); err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest(http.MethodPost, b.endpoint, buf)
	if err != nil {
		return nil, err
	}
	httputil.SetORTBHeaders(httpReq, req.ID, req.Device.UA, req.Device.IP)

	resp, err := b.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return nil, nil
	}
	if resp.StatusCode != 200 {
		return nil, nil
	}

	reader, closeFn, gzErr := httputil.ResponseBodyReader(resp)
	if gzErr != nil {
		return nil, gzErr
	}
	defer closeFn()

	var bidResp openrtb.BidResponse
	if err := json.NewDecoder(reader).Decode(&bidResp); err != nil {
		return nil, err
	}

	// Validate response per PDF spec section 6
	valid := bidResp.Validate(&req)
	return valid, nil
}
