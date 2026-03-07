package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestORTBAdapterPreservesPublisherAppFieldsInOutboundRequest(t *testing.T) {
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
			Name:     "Example TV",
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
	if got := app["bundle"]; got != "B00V3UTTPSernsp" {
		t.Fatalf("expected outbound app.bundle to preserve publisher value, got %#v", got)
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
	if got := app["id"]; got != "B00V3UTTPSernsp" {
		t.Fatalf("expected outbound app.id to preserve publisher value, got %#v", got)
	}
	if got := app["name"]; got != "Example TV" {
		t.Fatalf("expected outbound app.name to preserve publisher value, got %#v", got)
	}
	if got := app["storeurl"]; got != "https://play.google.com/store/apps/details?id=com.clean.bundle" {
		t.Fatalf("expected outbound app.storeurl to preserve publisher value, got %#v", got)
	}
}

func TestORTBAdapterPreservesTransactionIDWhileRemovingOnlyConfiguredSupplyChainFields(t *testing.T) {
	var gotPayload map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotPayload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	request := &openrtb.BidRequest{
		ID:  "req-schain",
		Imp: []openrtb.Imp{{ID: "imp-1"}},
		Source: &openrtb.Source{
			TID:    "req-schain",
			PChain: "1!exchange.example,1!ssp.example",
			SChain: &openrtb.SChain{Complete: 1, Ver: "1.0", Nodes: []openrtb.SChainNode{{ASI: "viadsmedia.com", SID: "pub-001"}}},
		},
	}

	adapter := NewORTBAdapter(&AdapterConfig{
		ID:           "ortb-schain",
		Name:         "ORTB Supply Chain",
		Type:         TypeORTB,
		Endpoint:     server.URL,
		ORTBVersion:  "2.6",
		Status:       1,
		RemovePChain: true,
	})

	result, err := adapter.RequestBids(context.Background(), request)
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}
	if result == nil || !result.NoBid {
		t.Fatalf("expected no-bid result from 204 response, got %#v", result)
	}

	source, ok := gotPayload["source"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected source object in outbound payload, got %#v", gotPayload["source"])
	}
	if got := source["tid"]; got != "req-schain" {
		t.Fatalf("expected outbound source.tid to be preserved, got %#v", got)
	}
	if _, ok := source["schain"]; ok {
		t.Fatalf("expected schain to be removed when endpoint schain is disabled, got %#v", source["schain"])
	}
	if _, ok := source["pchain"]; ok {
		t.Fatalf("expected pchain to be removed when remove_pchain is enabled, got %#v", source["pchain"])
	}
	if request.Source == nil || request.Source.SChain == nil {
		t.Fatalf("expected original request source.schain to remain intact, got %#v", request.Source)
	}
}

func TestORTBAdapterClampsOutboundTMaxToClientBudget(t *testing.T) {
	var gotPayload map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotPayload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	request := &openrtb.BidRequest{ID: "req-tmax", TMax: 500, Imp: []openrtb.Imp{{ID: "imp-1"}}}
	adapter := NewORTBAdapter(&AdapterConfig{
		ID:        "ortb-timeout",
		Name:      "ORTB Timeout",
		Type:      TypeORTB,
		Endpoint:  server.URL,
		Status:    1,
		TimeoutMs: 120,
	})

	result, err := adapter.RequestBids(context.Background(), request)
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}
	if result == nil || !result.NoBid {
		t.Fatalf("expected no-bid result from 204 response, got %#v", result)
	}
	if got, ok := gotPayload["tmax"].(float64); !ok || int(got) != 70 {
		t.Fatalf("expected outbound tmax to be clamped to 70ms, got %#v", gotPayload["tmax"])
	}
}

func TestORTBAdapterClampsOutboundTMaxToRemainingContextBudget(t *testing.T) {
	var gotPayload map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotPayload)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	request := &openrtb.BidRequest{ID: "req-context-tmax", TMax: 500, Imp: []openrtb.Imp{{ID: "imp-1"}}}
	adapter := NewORTBAdapter(&AdapterConfig{
		ID:        "ortb-timeout-context",
		Name:      "ORTB Timeout Context",
		Type:      TypeORTB,
		Endpoint:  server.URL,
		Status:    1,
		TimeoutMs: 400,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	result, err := adapter.RequestBids(ctx, request)
	if err != nil {
		t.Fatalf("unexpected adapter error: %v", err)
	}
	if result == nil || !result.NoBid {
		t.Fatalf("expected no-bid result from 204 response, got %#v", result)
	}
	got, ok := gotPayload["tmax"].(float64)
	if !ok {
		t.Fatalf("expected outbound tmax in payload, got %#v", gotPayload["tmax"])
	}
	if got <= 0 || got > 150 {
		t.Fatalf("expected outbound tmax to respect remaining context budget, got %#v", got)
	}
	if got >= 350 {
		t.Fatalf("expected outbound tmax to be lower than the static client budget when request context is tighter, got %#v", got)
	}
}

func TestORTBAdapterPreservesPublisherBundleWhenInputBundleLooksEncoded(t *testing.T) {
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
		ID:  "req-vizio-bundle",
		Imp: []openrtb.Imp{{ID: "imp-1"}},
		App: &openrtb.App{
			ID:       "501481973_82926669",
			Bundle:   "https3a2f2fwww.vizio.com2fen2fsmart.tv.apps3fappname3ddantdm",
			StoreURL: "https://www.vizio.com/en/smart-tv-apps?appName=dantdm",
		},
	}

	adapter := NewORTBAdapter(&AdapterConfig{
		ID:          "ortb-vizio-bundle",
		Name:        "ORTB Vizio Bundle",
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
	if got := app["bundle"]; got != "https3a2f2fwww.vizio.com2fen2fsmart.tv.apps3fappname3ddantdm" {
		t.Fatalf("expected outbound app.bundle to preserve publisher value, got %#v", got)
	}
	if got := app["id"]; got != "501481973_82926669" {
		t.Fatalf("expected outbound app.id to preserve publisher value, got %#v", got)
	}
}

func TestORTBAdapterPreservesAppFieldsWithoutSanitizingSyntheticValues(t *testing.T) {
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
	if got := app["bundle"]; got != "supply.42" {
		t.Fatalf("expected outbound app.bundle to preserve request value, got %#v", got)
	}
	if got := app["id"]; got != "supply.42" {
		t.Fatalf("expected outbound app.id to preserve request value, got %#v", got)
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
	if got := app["bundle"]; got != "supply.42" {
		t.Fatalf("expected outbound app.bundle to preserve request value, got %#v", got)
	}
	if got := app["id"]; got != "tv.tubi" {
		t.Fatalf("expected outbound app.id to preserve request value, got %#v", got)
	}
	if request.App == nil || request.App.Bundle != "supply.42" {
		t.Fatalf("expected original request bundle to remain unchanged, got %#v", request.App)
	}
	if request.App.ID != "tv.tubi" {
		t.Fatalf("expected original request app.id to remain unchanged, got %#v", request.App)
	}
}

func TestORTBAdapterDecodesEncodedStoreURLBeforeForwarding(t *testing.T) {
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
		ID:  "req-encoded-store-url",
		Imp: []openrtb.Imp{{ID: "imp-1"}},
		App: &openrtb.App{
			ID:       "1089249069",
			Bundle:   "1089249069",
			StoreURL: "https%253A%252F%252Fapps.apple.com%252Fus%252Fapp%252Fexample-tv%252Fid1089249069",
		},
	}

	adapter := NewORTBAdapter(&AdapterConfig{
		ID:          "ortb-encoded-store-url",
		Name:        "ORTB Encoded Store URL",
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
	if got := app["storeurl"]; got != "https://apps.apple.com/us/app/example-tv/id1089249069" {
		t.Fatalf("expected outbound app.storeurl to be decoded, got %#v", got)
	}
	if request.App == nil || request.App.StoreURL != "https%253A%252F%252Fapps.apple.com%252Fus%252Fapp%252Fexample-tv%252Fid1089249069" {
		t.Fatalf("expected original request store URL to remain encoded, got %#v", request.App)
	}
}
