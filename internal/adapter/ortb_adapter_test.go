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
