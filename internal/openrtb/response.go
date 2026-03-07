package openrtb

import (
	"net/url"
	"strconv"
	"strings"

	openrtb2 "github.com/prebid/openrtb/v20/openrtb2"
)

// ── OpenRTB 2.6 BidResponse ──

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

func bidFromPrebid(in openrtb2.Bid, seat string) Bid {
	out := Bid{
		ID:      in.ID,
		ImpID:   in.ImpID,
		Price:   in.Price,
		Adm:     in.AdM,
		NURL:    in.NURL,
		BURL:    in.BURL,
		LURL:    in.LURL,
		CrID:    in.CrID,
		ADomain: cloneStrings(in.ADomain),
		Cat:     cloneStrings(in.Cat),
		DealID:  in.DealID,
		W:       int(in.W),
		H:       int(in.H),
		AdvID:   in.AdID,
		MType:   int(in.MType),
		Seat:    seat,
	}

	if len(in.Attr) > 0 {
		out.Attr = make([]int, len(in.Attr))
		for i, attr := range in.Attr {
			out.Attr[i] = int(attr)
		}
	}

	return out
}

// IsRenderableBid returns true only if the bid has playable ad markup.
// nurl/burl/lurl are notices, not creatives, so they do NOT make a bid playable.
func IsRenderableBid(b Bid) bool {
	return HasRenderableAdm(b.Adm)
}

// HasRenderableAdm validates whether Adm is enough to build and serve a playable VAST.
func HasRenderableAdm(adm string) bool {
	trimmed := strings.TrimSpace(adm)
	if trimmed == "" {
		return false
	}

	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "<?xml") || strings.HasPrefix(lower, "<vast") || strings.HasPrefix(lower, "<vmap") {
		return true
	}

	if strings.HasPrefix(trimmed, "//") {
		return true
	}

	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		u, err := url.Parse(trimmed)
		if err != nil || u.Host == "" {
			return false
		}
		return true
	}

	return false
}

// SubstituteMacros replaces OpenRTB auction macros in a URL.
// Handles plain ${MACRO}, fully-encoded %24%7BMACRO%7D, and
// partially-encoded %24{MACRO} variants.
func (b *Bid) SubstituteMacros(rawURL string) string {
	return b.substituteMacros(rawURL, true)
}

// SubstituteMacrosRaw replaces OpenRTB macros without forcing URL normalization.
// Use this for non-URL payloads like VAST XML in bid.Adm.
func (b *Bid) SubstituteMacrosRaw(raw string) string {
	return b.substituteMacros(raw, false)
}

func (b *Bid) substituteMacros(raw string, enforceScheme bool) string {
	if raw == "" {
		return raw
	}
	// Fast path: no macros present
	if !strings.Contains(raw, "${") && !strings.Contains(raw, "%24") {
		if enforceScheme {
			return ensureScheme(raw)
		}
		return raw
	}

	clearPrice := b.WinPrice
	if clearPrice == 0 {
		clearPrice = b.Price
	}

	priceStr := formatPrice(clearPrice)

	// Direct string replacements — faster than building a Replacer for 6 macros × 3 encodings.
	r := raw
	r = replaceMacro(r, "AUCTION_PRICE", priceStr)
	r = replaceMacro(r, "AUCTION_ID", b.ID)
	r = replaceMacro(r, "AUCTION_BID_ID", b.ID)
	r = replaceMacro(r, "AUCTION_IMP_ID", b.ImpID)
	r = replaceMacro(r, "AUCTION_SEAT_ID", b.Seat)
	r = replaceMacro(r, "AUCTION_CURRENCY", "USD")

	if enforceScheme {
		return ensureScheme(r)
	}
	return r
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
	s = strings.ReplaceAll(s, "${"+macro+"}", val)
	// Fully encoded: %24%7BMACRO%7D
	enc := "%24%7B" + macro + "%7D"
	s = strings.ReplaceAll(s, enc, val)
	// Partially encoded: %24{MACRO}
	partial := "%24{" + macro + "}"
	s = strings.ReplaceAll(s, partial, val)
	return s
}

// ensureScheme prepends https:// if the URL has no scheme.
func ensureScheme(u string) string {
	lower := strings.ToLower(u)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return u
	}
	if strings.HasPrefix(u, "//") {
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

// ValidateBidResponse checks a BidResponse against a BidRequest per OpenRTB 2.6 spec.
// Returns only bids that match a request impression and meet the floor.
func ValidateBidResponse(resp *openrtb2.BidResponse, req *BidRequest) []Bid {
	if resp == nil || req == nil {
		return nil
	}

	impIDs := make(map[string]float64, len(req.Imp))
	for _, imp := range req.Imp {
		impIDs[imp.ID] = imp.BidFloor
	}

	var valid []Bid
	for _, sb := range resp.SeatBid {
		for _, rawBid := range sb.Bid {
			floor, ok := impIDs[rawBid.ImpID]
			if !ok {
				continue
			}
			if rawBid.Price < floor {
				continue
			}

			normalized := bidFromPrebid(rawBid, sb.Seat)
			if !IsRenderableBid(normalized) {
				continue
			}

			valid = append(valid, normalized)
		}
	}
	return valid
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
