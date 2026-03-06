package auction

import (
	"io"
	"log"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"time"
)

// AuctionResult contains the winner, final price, and all losing bids.
type AuctionResult struct {
	Winner   *openrtb.Bid
	WinPrice float64 // Final clearing price (may differ in second-price)
	Losers   []openrtb.Bid
}

// Run executes the full auction. auctionType is "first_price" or "second_price".
func Run(bids []openrtb.Bid, floor float64, auctionType string) *AuctionResult {
	result := &AuctionResult{}

	// Filter valid bids
	var eligibleBids []openrtb.Bid
	for i := range bids {
		bid := &bids[i]
		if bid.Price <= 0 {
			continue
		}

		// Keep floor checks on gross bid value when margin is configured.
		floorComparablePrice := bid.Price
		if bid.Margin > 0 && bid.Margin < 1 {
			floorComparablePrice = bid.Price / (1 - bid.Margin)
		}
		if floorComparablePrice < floor {
			continue
		}
		if bid.Adm == "" && bid.NURL == "" {
			continue
		}
		eligibleBids = append(eligibleBids, *bid)
	}

	if len(eligibleBids) == 0 {
		return result
	}

	// Sort: find highest and second-highest
	var winnerIndex int
	highestPrice := eligibleBids[0].Price
	secondHighestPrice := floor
	for i := 1; i < len(eligibleBids); i++ {
		if eligibleBids[i].Price > highestPrice {
			secondHighestPrice = highestPrice
			winnerIndex = i
			highestPrice = eligibleBids[i].Price
		} else if eligibleBids[i].Price > secondHighestPrice {
			secondHighestPrice = eligibleBids[i].Price
		}
	}

	result.Winner = &eligibleBids[winnerIndex]

	// Set clearing price based on auction type
	switch auctionType {
	case "second_price":
		result.WinPrice = secondHighestPrice + 0.01 // second price + penny
		if result.WinPrice > result.Winner.Price {
			result.WinPrice = result.Winner.Price
		}
	default: // first_price
		result.WinPrice = result.Winner.Price
	}
	result.Winner.WinPrice = result.WinPrice
	// Collect losers
	for i, bid := range eligibleBids {
		if i != winnerIndex {
			result.Losers = append(result.Losers, bid)
		}
	}

	return result
}

// SelectWinner is the legacy simple auction (first-price only).
func SelectWinner(bids []openrtb.Bid, floor float64) *openrtb.Bid {
	r := Run(bids, floor, "first_price")
	return r.Winner
}

// FireWinNotice sends the nurl (win notice) to the DSP asynchronously.
// Substitutes ${AUCTION_PRICE} and other macros before calling.
func FireWinNotice(bid *openrtb.Bid) {
	if bid.NURL == "" {
		return
	}
	url := bid.SubstituteMacros(bid.NURL)
	go fireURL(url)
}

// FireBillingNotice sends the burl (billing URL) to the DSP asynchronously.
// Called when a billable event occurs (e.g., ad impression rendered).
func FireBillingNotice(bid *openrtb.Bid) {
	if bid.BURL == "" {
		return
	}
	url := bid.SubstituteMacros(bid.BURL)
	go fireURL(url)
}

// FireLossNotice sends the lurl (loss notice) to losing DSPs asynchronously.
func FireLossNotice(bid *openrtb.Bid) {
	if bid.LURL == "" {
		return
	}
	url := bid.SubstituteMacros(bid.LURL)
	go fireURL(url)
}

// Shared HTTP client for notice firing — uses pooled transport.
var noticeClient = httputil.NewClient(5 * time.Second)

func fireURL(url string) {
	resp, err := noticeClient.Get(url)
	if err != nil {
		log.Printf("notice fire failed: %v", err)
		return
	}
	// Drain and close body to allow TCP connection reuse
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
