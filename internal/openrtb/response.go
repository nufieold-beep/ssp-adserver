package openrtb

import (
	"strconv"
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
	ID        string   `json:"id"`
	ImpID     string   `json:"impid"`
	Price     float64  `json:"price"`
	Adm       string   `json:"adm"`
	NURL      string   `json:"nurl,omitempty"`
	BURL      string   `json:"burl,omitempty"`
	LURL      string   `json:"lurl,omitempty"`
	CrID      string   `json:"crid,omitempty"`
	ADomain   []string `json:"adomain,omitempty"`
	Cat       []string `json:"cat,omitempty"`
	DealID    string   `json:"dealid,omitempty"`
	W         int      `json:"w,omitempty"`
	H         int      `json:"h,omitempty"`
	Attr      []int    `json:"attr,omitempty"`
	MType     any      `json:"mtype,omitempty"`
	AdvID     string   `json:"adv_id,omitempty"`
	Seat      string   `json:"-"`
	WinPrice  float64  `json:"-"`
	Margin    float64  `json:"-"`
	DemandSrc string   `json:"-"`
}

// SubstituteMacros replaces OpenRTB auction macros in a URL.
// Handles plain ${MACRO}, fully-encoded %24%7BMACRO%7D, and
// partially-encoded %24{MACRO} variants.
func (b *Bid) SubstituteMacros(rawURL string) string {
	if rawURL == "" {
		return rawURL
	}
	// Fast path: no macros present
	if !strings.Contains(rawURL, "${") && !strings.Contains(rawURL, "%24") {
		return ensureScheme(rawURL)
	}

	clearPrice := b.WinPrice
	if clearPrice == 0 {
		clearPrice = b.Price
	}

	priceStr := formatPrice(clearPrice)

	// Direct string replacements — faster than building a Replacer for 6 macros × 3 encodings.
	r := rawURL
	r = replaceMacro(r, "AUCTION_PRICE", priceStr)
	r = replaceMacro(r, "AUCTION_ID", b.ID)
	r = replaceMacro(r, "AUCTION_BID_ID", b.ID)
	r = replaceMacro(r, "AUCTION_IMP_ID", b.ImpID)
	r = replaceMacro(r, "AUCTION_SEAT_ID", b.Seat)
	r = replaceMacro(r, "AUCTION_CURRENCY", "USD")

	return ensureScheme(r)
}

// ReportingPrice returns margin-adjusted price for internal billing/reporting.
// Auction ranking, floor checks, and outbound DSP macros should use gross price.
func (b *Bid) ReportingPrice(grossPrice float64) float64 {
	if b != nil && b.Margin > 0 && b.Margin < 1 {
		return grossPrice * (1 - b.Margin)
	}
	return grossPrice
}

// replaceMacro replaces all three encoding forms of a single macro in s.
func replaceMacro(s, macro, val string) string {
	// Plain: ${MACRO}
	if i := strings.Index(s, "${"+macro+"}"); i >= 0 {
		s = s[:i] + val + s[i+len(macro)+3:]
	}
	// Fully encoded: %24%7BMACRO%7D
	enc := "%24%7B" + macro + "%7D"
	if i := strings.Index(s, enc); i >= 0 {
		s = s[:i] + val + s[i+len(enc):]
	}
	// Partially encoded: %24{MACRO}
	partial := "%24{" + macro + "}"
	if i := strings.Index(s, partial); i >= 0 {
		s = s[:i] + val + s[i+len(partial):]
	}
	return s
}

// ensureScheme prepends https:// if the URL has no scheme.
func ensureScheme(u string) string {
	if len(u) >= 7 && (u[:7] == "http://" || u[:8] == "https://") {
		return u
	}
	if len(u) >= 2 && u[:2] == "//" {
		return "https:" + u
	}
	return "https://" + u
}

// formatPrice formats a float64 as a compact decimal string without trailing zeros.
func formatPrice(p float64) string {
	s := strconv.FormatFloat(p, 'f', 6, 64)
	// Trim trailing zeros after decimal point
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		end := len(s)
		for end > dot+1 && s[end-1] == '0' {
			end--
		}
		if end == dot+1 {
			end = dot // remove the dot too
		}
		s = s[:end]
	}
	return s
}

// Validate checks a BidResponse against a BidRequest per OpenRTB 2.6 spec.
// Returns only bids that match a request impression and meet the floor.
func (resp *BidResponse) Validate(req *BidRequest) []Bid {
	impIDs := make(map[string]float64, len(req.Imp))
	for _, imp := range req.Imp {
		impIDs[imp.ID] = imp.BidFloor
	}

	var valid []Bid
	for _, sb := range resp.SeatBid {
		for _, bid := range sb.Bid {
			floor, ok := impIDs[bid.ImpID]
			if !ok {
				continue
			}
			if bid.Price < floor {
				continue
			}
			if bid.Adm == "" && bid.NURL == "" {
				continue
			}
			bid.Seat = sb.Seat
			valid = append(valid, bid)
		}
	}
	return valid
}
