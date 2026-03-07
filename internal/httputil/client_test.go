package httputil

import (
	"net/http"
	"testing"
)

func TestSetORTBHeadersAddsCTVAppCompatibleHeaders(t *testing.T) {
	httpReq, err := http.NewRequest(http.MethodPost, "https://dsp.example.com/openrtb2/auction", nil)
	if err != nil {
		t.Fatalf("unexpected request creation error: %v", err)
	}

	SetORTBHeaders(httpReq, "req-1", "Roku/DVP-12.5", "203.0.113.10", "2.6")

	if got := httpReq.Header.Get("Accept"); got != "application/json" {
		t.Fatalf("expected Accept header application/json, got %q", got)
	}
	if got := httpReq.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected Content-Type header application/json, got %q", got)
	}
	if got := httpReq.Header.Get("X-Openrtb-Version"); got != "2.6" {
		t.Fatalf("expected X-Openrtb-Version 2.6, got %q", got)
	}
	if got := httpReq.Header.Get("X-Request-ID"); got != "req-1" {
		t.Fatalf("expected X-Request-ID req-1, got %q", got)
	}
	if got := httpReq.Header.Get("User-Agent"); got != "Roku/DVP-12.5" {
		t.Fatalf("expected User-Agent Roku/DVP-12.5, got %q", got)
	}
	if got := httpReq.Header.Get("X-Device-User-Agent"); got != "Roku/DVP-12.5" {
		t.Fatalf("expected X-Device-User-Agent Roku/DVP-12.5, got %q", got)
	}
	if got := httpReq.Header.Get("X-Forwarded-For"); got != "203.0.113.10" {
		t.Fatalf("expected X-Forwarded-For 203.0.113.10, got %q", got)
	}
	if got := httpReq.Header.Get("X-Device-IP"); got != "203.0.113.10" {
		t.Fatalf("expected X-Device-IP 203.0.113.10, got %q", got)
	}
	if got := httpReq.Host; got != "dsp.example.com" {
		t.Fatalf("expected Host dsp.example.com, got %q", got)
	}
}
