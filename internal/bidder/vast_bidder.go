package bidder

import (
	"html"
	"net/http"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
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
		t = 120 * time.Millisecond
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

func (b *VASTBidder) Request(req openrtb.BidRequest) ([]openrtb.Bid, error) {
	resp, err := b.client.Get(b.tag)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := httputil.ReadResponseBody(resp)
	if err != nil {
		return nil, err
	}

	adm := html.UnescapeString(string(body))
	if adm == "" {
		return nil, nil
	}

	bid := openrtb.Bid{
		ID:    "vast-" + b.name,
		ImpID: "1",
		Price: b.cpm,
		Adm:   adm,
		MType: "CREATIVE_MARKUP_VIDEO",
	}

	return []openrtb.Bid{bid}, nil
}
