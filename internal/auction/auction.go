package auction

import (
	"io"
	"log"
	"ssp/internal/httputil"
	"ssp/internal/openrtb"
	"sync"
	"time"
)

// AuctionResult contains the winner, final price, and all losing bids.
type AuctionResult struct {
	Winner   *openrtb.Bid
	WinPrice float64 // Final clearing price (may differ in second-price)
	Losers   []openrtb.Bid
}

type billableNoticeEntry struct {
	url     string
	expires time.Time
}

var (
	billableNoticeMu sync.Mutex
	billableNotices  = make(map[string]billableNoticeEntry)
)

// Run executes the full auction. auctionType is "first_price" or "second_price".
func Run(bids []openrtb.Bid, floor float64, auctionType string) *AuctionResult {
	result := &AuctionResult{}

	var eligibleBids []openrtb.Bid
	for i := range bids {
		bid := &bids[i]
		if bid.Price <= 0 {
			continue
		}

		if bid.Price < floor {
			continue
		}
		if !openrtb.IsRenderableBid(*bid) {
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
	if bid == nil || bid.NURL == "" {
		return
	}
	url := bid.SubstituteMacros(bid.NURL)
	go fireURL(url)
}

// FireBillingNotice sends the burl (billing URL) to the DSP asynchronously.
// Called when a billable event occurs (e.g., ad impression rendered).
func FireBillingNotice(bid *openrtb.Bid) {
	if bid == nil || bid.BURL == "" {
		return
	}
	url := bid.SubstituteMacros(bid.BURL)
	go fireURL(url)
}

// RegisterBillableNotice stores a billable notice URL to be fired later
// on billable event callbacks (e.g., impression).
func RegisterBillableNotice(bid *openrtb.Bid) {
	if bid == nil || bid.ID == "" || bid.BURL == "" {
		return
	}
	entry := billableNoticeEntry{
		url:     bid.SubstituteMacros(bid.BURL),
		expires: time.Now().Add(30 * time.Minute),
	}

	billableNoticeMu.Lock()
	billableNotices[bid.ID] = entry
	billableNoticeMu.Unlock()
}

// FireBillingNoticeByBidID fires and removes the stored billable notice URL.
func FireBillingNoticeByBidID(bidID string) {
	if bidID == "" {
		return
	}

	billableNoticeMu.Lock()
	entry, ok := billableNotices[bidID]
	if ok {
		delete(billableNotices, bidID)
	}
	billableNoticeMu.Unlock()

	if !ok {
		return
	}
	if time.Now().After(entry.expires) {
		return
	}
	go fireURL(entry.url)
}

// FireLossNotice sends the lurl (loss notice) to losing DSPs asynchronously.
func FireLossNotice(bid *openrtb.Bid) {
	if bid == nil || bid.LURL == "" {
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
