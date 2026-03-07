package auction

import (
	"ssp/internal/openrtb"
	"testing"
	"time"
)

func TestRegisterBillableNoticePrunesExpiredEntries(t *testing.T) {
	billableNoticeMu.Lock()
	original := billableNotices
	billableNotices = map[string]billableNoticeEntry{
		"expired": {url: "https://expired.example.com", expires: time.Now().Add(-time.Minute)},
		"active":  {url: "https://active.example.com", expires: time.Now().Add(time.Minute)},
	}
	billableNoticeMu.Unlock()
	defer func() {
		billableNoticeMu.Lock()
		billableNotices = original
		billableNoticeMu.Unlock()
	}()

	RegisterBillableNotice(&openrtb.Bid{ID: "new-bid", BURL: "https://example.com/bill"})

	billableNoticeMu.Lock()
	defer billableNoticeMu.Unlock()
	if _, ok := billableNotices["expired"]; ok {
		t.Fatal("expected expired billable notice to be pruned")
	}
	if _, ok := billableNotices["active"]; !ok {
		t.Fatal("expected active billable notice to be preserved")
	}
	if _, ok := billableNotices["new-bid"]; !ok {
		t.Fatal("expected new billable notice to be registered")
	}
}

func TestFireBillingNoticeByBidIDDropsExpiredEntries(t *testing.T) {
	billableNoticeMu.Lock()
	original := billableNotices
	billableNotices = map[string]billableNoticeEntry{
		"expired": {url: "https://expired.example.com", expires: time.Now().Add(-time.Minute)},
	}
	billableNoticeMu.Unlock()
	defer func() {
		billableNoticeMu.Lock()
		billableNotices = original
		billableNoticeMu.Unlock()
	}()

	FireBillingNoticeByBidID("expired")

	billableNoticeMu.Lock()
	defer billableNoticeMu.Unlock()
	if _, ok := billableNotices["expired"]; ok {
		t.Fatal("expected expired billable notice to be removed during lookup")
	}
}

func TestRunRejectsNoticeOnlyBids(t *testing.T) {
	bids := []openrtb.Bid{
		{
			ID:    "notice-only",
			ImpID: "imp-1",
			Price: 10.0,
			NURL:  "https://example.com/win",
		},
		{
			ID:    "renderable",
			ImpID: "imp-1",
			Price: 2.0,
			Adm:   "https://cdn.example.com/video.mp4",
		},
	}

	result := Run(bids, 1.0, "first_price")
	if result.Winner == nil {
		t.Fatal("expected a renderable bid to win")
	}
	if result.Winner.ID != "renderable" {
		t.Fatalf("expected renderable bid to win, got %q", result.Winner.ID)
	}
	if result.WinPrice != 2.0 {
		t.Fatalf("expected clearing price 2.0, got %.2f", result.WinPrice)
	}
}
