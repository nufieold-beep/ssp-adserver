package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ssp/internal/openrtb"
)

func TestVASTAdapterRejectsInvalidPayloads(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "non vast payload", body: "<html>no ad</html>", wantErr: "non-VAST payload"},
		{name: "invalid xml", body: "<VAST version=\"3.0\"><Ad></VAST>", wantErr: "invalid XML"},
	}

	req := &openrtb.BidRequest{ID: "req-1", Imp: []openrtb.Imp{{ID: "imp-1"}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			adapter := NewVASTAdapter(&AdapterConfig{
				ID:       "vast-test",
				Name:     "VAST Test",
				Type:     TypeVAST,
				Endpoint: server.URL,
				Floor:    1.25,
			})

			_, err := adapter.RequestBids(context.Background(), req)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}
