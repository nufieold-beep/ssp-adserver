package openrtb

import (
	"fmt"
	"strings"
)

// ── OpenRTB 2.6 BidResponse ──

type BidResponse struct {
	ID      string    `json:"id"`
	SeatBid []SeatBid `json:"seatbid"`
	Cur     string    `json:"cur,omitempty"`
}

type SeatBid struct {
	Bid  []Bid  `json:"bid"`
	Seat string `json:"seat,omitempty"`
}

type Bid struct {
	ID       string   `json:"id"`
	ImpID    string   `json:"impid"`
	Price    float64  `json:"price"`
	Adm      string   `json:"adm"`
	NURL     string   `json:"nurl,omitempty"` // Win notice URL
	BURL     string   `json:"burl,omitempty"` // Billing URL (impression)
	LURL     string   `json:"lurl,omitempty"` // Loss notice URL
	CrID     string   `json:"crid,omitempty"`
	ADomain  []string `json:"adomain,omitempty"`
	Cat      []string `json:"cat,omitempty"` // Creative categories
	DealID   string   `json:"dealid,omitempty"`
	W        int      `json:"w,omitempty"`
	H        int      `json:"h,omitempty"`
	Attr     []int    `json:"attr,omitempty"`   // Creative attributes
	MType    any      `json:"mtype,omitempty"`  // OpenRTB 2.6: Creative markup type
	AdvID    string   `json:"adv_id,omitempty"` // Advertiser ID for quality checks
	Seat      string   `json:"-"`                // Populated from SeatBid.Seat
	WinPrice  float64  `json:"-"`                // Added internally for macro sub
	Margin    float64  `json:"-"`                // Margin applied (so we can reverse it for DSP gross)
	DemandSrc string   `json:"-"`                // Internal: adapter/demand source name
}

// SubstituteMacros replaces OpenRTB auction macros in a URL.
// Handles plain ${MACRO}, fully-encoded %24%7BMACRO%7D, and
// partially-encoded %24{MACRO} variants automatically.
func (b *Bid) SubstituteMacros(rawURL string) string {
	if rawURL == "" || (!strings.Contains(rawURL, "${") && !strings.Contains(rawURL, "%24")) {
		return rawURL
	}

	clearPrice := b.WinPrice
	if clearPrice == 0 {
		clearPrice = b.Price
	}
	if b.Margin > 0 && b.Margin < 1 {
		clearPrice = clearPrice / (1 - b.Margin)
	}

	// Canonical macro → value map.
	macros := map[string]string{
		"AUCTION_PRICE":    formatPrice(clearPrice),
		"AUCTION_ID":       b.ID,
		"AUCTION_BID_ID":   b.ID,
		"AUCTION_IMP_ID":   b.ImpID,
		"AUCTION_SEAT_ID":  b.Seat,
		"AUCTION_CURRENCY": "USD",
	}

	// Build replacer pairs covering all three encoding forms.
	pairs := make([]string, 0, len(macros)*6)
	for macro, val := range macros {
		pairs = append(pairs,
			"${"+macro+"}", val,                          // plain
			"%24%7B"+macro+"%7D", val,                    // fully encoded
			"%24{"+macro+"}", val,                        // partially encoded
		)
	}

	replaced := strings.NewReplacer(pairs...).Replace(rawURL)
	return ensureScheme(replaced)
}

// ensureScheme prepends https:// if the URL has no scheme.
func ensureScheme(u string) string {
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return u
	}
	if strings.HasPrefix(u, "//") {
		return "https:" + u
	}
	return "https://" + u
}

func formatPrice(p float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.6f", p), "0"), ".")
}

// Validate checks a BidResponse against a BidRequest per PDF spec section 6.
// Preserves nurl, burl, lurl and attaches seat from SeatBid.
func (resp *BidResponse) Validate(req *BidRequest) []Bid {
	impIDs := make(map[string]float64)
	for _, imp := range req.Imp {
		impIDs[imp.ID] = imp.BidFloor
	}

	var valid []Bid
	for _, sb := range resp.SeatBid {
		for _, bid := range sb.Bid {
			floor, ok := impIDs[bid.ImpID]
			if !ok {
				continue // impid must match an imp.id
			}
			if bid.Price < floor {
				continue // price must be >= floor
			}
			if bid.Adm == "" && bid.NURL == "" {
				continue // adm or nurl must be present
			}
			// Carry seat from SeatBid into Bid for macro substitution
			bid.Seat = sb.Seat
			valid = append(valid, bid)
		}
	}
	return valid
}
