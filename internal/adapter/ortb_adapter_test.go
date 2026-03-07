package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"ssp/internal/openrtb"

	"github.com/prebid/openrtb/v20/adcom1"
)

func TestORTBAdapterDowngradesRequestForORTB25(t *testing.T) {
	var (
		gotVersion string
		gotPayload map[string]interface{}
		handlerErr error
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		gotVersion = r.Header.Get("X-Openrtb-Version")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(body, &gotPayload); err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	startDelay := adcom1.StartDelay(0)
	request := &openrtb.BidRequest{
		ID: "req-ortb-25",
		Imp: []openrtb.Imp{{
			ID:       "imp-1",
			BidFloor: 1.0,
			Video: &openrtb.Video{
				Plcmt:      adcom1.VideoPlcmtSubtype(1),
				StartDelay: &startDelay,
			},
		}},
		Device: &openrtb.Device{
			UA:  "",
			SUA: &openrtb.UserAgent{Source: adcom1.UserAgentSource(3)},
		},
		Regs: &openrtb.Regs{GPPSID: []int8{2}},
	}

	adapter := NewORTBAdapter(&AdapterConfig{
		ID:          "ortb-25",
		Name:        "ORTB 2.5",
		Type:        TypeORTB,
		Endpoint:    server.URL,
		ORTBVersion: "2.5",
		Status:      1,
	})

	result, err := adapter.RequestBids(context.Background(), request)
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("unexpected request capture error: %v", handlerErr)
	}
	if result == nil || !result.NoBid {
		t.Fatalf("expected no-bid result from 204 response, got %#v", result)
	}
	if gotVersion != "2.5" {
		t.Fatalf("expected X-Openrtb-Version 2.5, got %q", gotVersion)
	}

	impList, ok := gotPayload["imp"].([]interface{})
	if !ok || len(impList) != 1 {
		t.Fatalf("expected one impression in outbound payload, got %#v", gotPayload["imp"])
	}
	video, ok := impList[0].(map[string]interface{})["video"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected video object in outbound payload, got %#v", impList[0])
	}
	if _, ok := video["plcmt"]; ok {
		t.Fatalf("expected ORTB 2.5 request to omit video.plcmt, got %#v", video["plcmt"])
	}

	device, ok := gotPayload["device"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected device object in outbound payload, got %#v", gotPayload["device"])
	}
	if _, ok := device["sua"]; ok {
		t.Fatalf("expected ORTB 2.5 request to omit device.sua, got %#v", device["sua"])
	}

	regs, ok := gotPayload["regs"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected regs object in outbound payload, got %#v", gotPayload["regs"])
	}
	if _, ok := regs["gppsid"]; ok {
		t.Fatalf("expected ORTB 2.5 request to omit regs.gppsid, got %#v", regs["gppsid"])
	}

	if request.Device == nil || request.Device.SUA == nil {
		t.Fatal("expected original request device.sua to remain intact")
	}
	if request.Regs == nil || len(request.Regs.GPPSID) != 1 {
		t.Fatalf("expected original request regs.gppsid to remain intact, got %#v", request.Regs)
	}
	if request.Imp[0].Video == nil || request.Imp[0].Video.Plcmt == 0 {
		t.Fatal("expected original request video.plcmt to remain intact")
	}
}

func TestORTBAdapterUsesCleanBundleInOutboundRequest(t *testing.T) {
	var (
		gotPayload map[string]interface{}
		handlerErr error
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(body, &gotPayload); err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	request := &openrtb.BidRequest{
		ID:  "req-clean-bundle",
		Imp: []openrtb.Imp{{ID: "imp-1"}},
		App: &openrtb.App{
			ID:       "B00V3UTTPSernsp",
			Bundle:   "B00V3UTTPSernsp",
			StoreURL: "https://play.google.com/store/apps/details?id=com.clean.bundle",
		},
	}

	adapter := NewORTBAdapter(&AdapterConfig{
		ID:          "ortb-clean-bundle",
		Name:        "ORTB Clean Bundle",
		Type:        TypeORTB,
		Endpoint:    server.URL,
		ORTBVersion: "2.6",
		Status:      1,
	})

	result, err := adapter.RequestBids(context.Background(), request)
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("unexpected request capture error: %v", handlerErr)
	}
	if result == nil || !result.NoBid {
		t.Fatalf("expected no-bid result from 204 response, got %#v", result)
	}

	app, ok := gotPayload["app"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected app object in outbound payload, got %#v", gotPayload["app"])
	}
	if got := app["bundle"]; got != "com.clean.bundle" {
		t.Fatalf("expected clean outbound app.bundle, got %#v", got)
	}
	if request.App == nil || request.App.Bundle != "B00V3UTTPSernsp" {
		t.Fatalf("expected original request bundle to remain unchanged, got %#v", request.App)
	}
	if request.App.StoreURL != "https://play.google.com/store/apps/details?id=com.clean.bundle" {
		t.Fatalf("expected original request store URL to remain unchanged, got %#v", request.App)
	}
	if request.App.ID != "B00V3UTTPSernsp" {
		t.Fatalf("expected original request app.id to remain unchanged, got %#v", request.App)
	}
	if got := app["id"]; got != "com.clean.bundle" {
		t.Fatalf("expected outbound app.id to be sanitized to the clean bundle, got %#v", got)
	}
}

func TestORTBAdapterSuppressesSyntheticBundleInOutboundRequest(t *testing.T) {
	var (
		gotPayload map[string]interface{}
		handlerErr error
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(body, &gotPayload); err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	request := &openrtb.BidRequest{
		ID:  "req-synthetic-bundle",
		Imp: []openrtb.Imp{{ID: "imp-1"}},
		App: &openrtb.App{
			ID:     "supply.42",
			Bundle: "supply.42",
		},
	}

	adapter := NewORTBAdapter(&AdapterConfig{
		ID:          "ortb-synthetic-bundle",
		Name:        "ORTB Synthetic Bundle",
		Type:        TypeORTB,
		Endpoint:    server.URL,
		ORTBVersion: "2.6",
		Status:      1,
	})

	result, err := adapter.RequestBids(context.Background(), request)
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("unexpected request capture error: %v", handlerErr)
	}
	if result == nil || !result.NoBid {
		t.Fatalf("expected no-bid result from 204 response, got %#v", result)
	}

	app, ok := gotPayload["app"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected app object in outbound payload, got %#v", gotPayload["app"])
	}
	if _, ok := app["bundle"]; ok {
		t.Fatalf("expected synthetic outbound app.bundle to be omitted, got %#v", app["bundle"])
	}
	if _, ok := app["id"]; ok {
		t.Fatalf("expected synthetic outbound app.id to be omitted, got %#v", app["id"])
	}
	if request.App == nil || request.App.Bundle != "supply.42" {
		t.Fatalf("expected original request bundle to remain unchanged, got %#v", request.App)
	}
	if request.App.ID != "supply.42" {
		t.Fatalf("expected original request app.id to remain unchanged, got %#v", request.App)
	}
}

func TestORTBAdapterPreservesCanonicalAppIDInOutboundRequest(t *testing.T) {
	var (
		gotPayload map[string]interface{}
		handlerErr error
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(body, &gotPayload); err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	request := &openrtb.BidRequest{
		ID:  "req-canonical-app-id",
		Imp: []openrtb.Imp{{ID: "imp-1"}},
		App: &openrtb.App{
			ID:     "tv.tubi",
			Bundle: "supply.42",
		},
	}

	adapter := NewORTBAdapter(&AdapterConfig{
		ID:          "ortb-canonical-app-id",
		Name:        "ORTB Canonical App ID",
		Type:        TypeORTB,
		Endpoint:    server.URL,
		ORTBVersion: "2.6",
		Status:      1,
	})

	result, err := adapter.RequestBids(context.Background(), request)
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}
	if handlerErr != nil {
		t.Fatalf("unexpected request capture error: %v", handlerErr)
	}
	if result == nil || !result.NoBid {
		t.Fatalf("expected no-bid result from 204 response, got %#v", result)
	}

	app, ok := gotPayload["app"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected app object in outbound payload, got %#v", gotPayload["app"])
	}
	if got := app["bundle"]; got != "tv.tubi" {
		t.Fatalf("expected outbound app.bundle to derive from canonical app.id, got %#v", got)
	}
	if got := app["id"]; got != "tv.tubi" {
		t.Fatalf("expected outbound canonical app.id to be preserved, got %#v", got)
	}
	if request.App == nil || request.App.Bundle != "supply.42" {
		t.Fatalf("expected original request bundle to remain unchanged, got %#v", request.App)
	}
	if request.App.ID != "tv.tubi" {
		t.Fatalf("expected original request app.id to remain unchanged, got %#v", request.App)
	}
}
